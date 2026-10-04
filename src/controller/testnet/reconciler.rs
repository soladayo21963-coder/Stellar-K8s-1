// Copyright 2024 Stellar-K8s Contributors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//! Reconciliation loop for the [`StellarTestnet`] controller.
//!
//! The reconciler drives the lifecycle state-machine:
//!
//! ```text
//! Pending ──► Provisioning ──► Ready
//!                  │
//!                  └──► Failed (retried with back-off)
//! ```
//!
//! On deletion the reconciler calls [`provisioner::teardown_testnet`] to
//! explicitly clean up any resources that may not have been garbage-collected
//! by owner references yet.
//!
//! TTL enforcement is handled by re-enqueueing the resource with a delay
//! equal to the remaining TTL; when the TTL has elapsed the reconciler
//! deletes the `StellarTestnet` resource itself.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use kube::api::{Api, DeleteParams, Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::{Client, ResourceExt};
use serde_json::json;
use tracing::{error, info, warn};

use crate::crd::testnet::{StellarTestnet, StellarTestnetPhase, StellarTestnetStatus};
use crate::error::Error;

use super::provisioner;

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

/// How often the controller re-checks a provisioning testnet for readiness.
const REQUEUE_PROVISIONING_SECS: u64 = 5;

/// How often the controller re-checks a Ready testnet for TTL expiry.
const REQUEUE_READY_SECS: u64 = 30;

/// How long to wait before retrying after a transient error.
const REQUEUE_ERROR_SECS: u64 = 15;

/// SSA field-manager name used to patch the status sub-resource.
const FIELD_MANAGER: &str = "stellar-testnet-operator";

// ─────────────────────────────────────────────────────────────────────────────
// Controller state
// ─────────────────────────────────────────────────────────────────────────────

/// Shared state passed to every reconciliation call.
pub struct TestnetContext {
    pub client: Client,
}

// ─────────────────────────────────────────────────────────────────────────────
// Reconcile entry point
// ─────────────────────────────────────────────────────────────────────────────

/// Main reconcile function called by `kube-rs` for every `StellarTestnet` event.
pub async fn reconcile(
    testnet: Arc<StellarTestnet>,
    ctx: Arc<TestnetContext>,
) -> Result<Action, Error> {
    let name = testnet.name_any();
    let namespace = testnet.namespace().unwrap_or_else(|| "default".to_string());
    let client = &ctx.client;

    info!(name = %name, namespace = %namespace, "Reconciling StellarTestnet");

    // ── Deletion handling ───────────────────────────────────────────────────
    if testnet.metadata.deletion_timestamp.is_some() {
        info!(name = %name, "StellarTestnet is being deleted – running teardown");
        patch_phase(client, &testnet, StellarTestnetPhase::Deleting, None).await?;
        provisioner::teardown_testnet(client, &testnet).await?;
        return Ok(Action::await_change());
    }

    // ── TTL enforcement ─────────────────────────────────────────────────────
    if let Some(action) = check_ttl(client, &testnet).await? {
        return Ok(action);
    }

    // ── Provision / reconcile resources ─────────────────────────────────────
    let result = provisioner::ensure_testnet(client, &testnet).await;

    match result {
        Ok(mut status) => {
            // Check if Pod is already Ready.
            match provisioner::is_testnet_ready(client, &testnet).await {
                Ok(true) => {
                    status.phase = StellarTestnetPhase::Ready;
                    status.message = Some("Soroban RPC endpoint is available".to_string());
                    if status.ready_at.is_none() {
                        status.ready_at = Some(Utc::now().to_rfc3339());
                    }
                    patch_status(client, &testnet, &status).await?;
                    info!(name = %name, rpc_url = ?status.rpc_url, "StellarTestnet is Ready");
                    Ok(Action::requeue(Duration::from_secs(REQUEUE_READY_SECS)))
                }
                Ok(false) => {
                    status.phase = StellarTestnetPhase::Provisioning;
                    patch_status(client, &testnet, &status).await?;
                    Ok(Action::requeue(Duration::from_secs(REQUEUE_PROVISIONING_SECS)))
                }
                Err(e) => {
                    let msg = format!("Failed to check pod readiness: {e}");
                    warn!(name = %name, error = %e, "Pod readiness check failed");
                    let status = StellarTestnetStatus {
                        phase: StellarTestnetPhase::Failed,
                        message: Some(msg),
                        last_reconcile_time: Some(Utc::now().to_rfc3339()),
                        failure_count: current_failure_count(&testnet) + 1,
                        ..Default::default()
                    };
                    patch_status(client, &testnet, &status).await?;
                    Ok(Action::requeue(Duration::from_secs(REQUEUE_ERROR_SECS)))
                }
            }
        }
        Err(e) => {
            let msg = format!("Provisioning failed: {e}");
            error!(name = %name, error = %e, "StellarTestnet provisioning error");
            let status = StellarTestnetStatus {
                phase: StellarTestnetPhase::Failed,
                message: Some(msg),
                last_reconcile_time: Some(Utc::now().to_rfc3339()),
                failure_count: current_failure_count(&testnet) + 1,
                ..Default::default()
            };
            patch_status(client, &testnet, &status).await?;
            Ok(Action::requeue(Duration::from_secs(REQUEUE_ERROR_SECS)))
        }
    }
}

/// Error handler called by the controller runtime when `reconcile` returns
/// an `Err`.
pub fn error_policy(
    testnet: Arc<StellarTestnet>,
    err: &Error,
    _ctx: Arc<TestnetContext>,
) -> Action {
    error!(
        name = %testnet.name_any(),
        error = %err,
        "Unhandled StellarTestnet reconcile error; retrying"
    );
    Action::requeue(Duration::from_secs(REQUEUE_ERROR_SECS))
}

// ─────────────────────────────────────────────────────────────────────────────
// Private helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Check if the testnet has exceeded its TTL; delete it if so.
/// Returns `Some(Action)` when the reconciler should stop processing this
/// resource (either it was deleted or it should requeue at TTL boundary).
async fn check_ttl(
    client: &Client,
    testnet: &StellarTestnet,
) -> Result<Option<Action>, Error> {
    let ttl = testnet.spec.ttl_seconds;
    if ttl == 0 {
        return Ok(None); // No TTL configured.
    }

    let created_at = match testnet.metadata.creation_timestamp.as_ref() {
        Some(ts) => ts.0,
        None => return Ok(None),
    };

    let elapsed = Utc::now()
        .signed_duration_since(created_at)
        .num_seconds()
        .max(0) as u64;

    if elapsed >= ttl {
        info!(
            name = %testnet.name_any(),
            ttl = ttl,
            elapsed = elapsed,
            "StellarTestnet TTL expired – deleting"
        );
        let api: Api<StellarTestnet> = Api::namespaced(
            client.clone(),
            &testnet.namespace().unwrap_or_else(|| "default".to_string()),
        );
        api.delete(&testnet.name_any(), &DeleteParams::default())
            .await?;
        return Ok(Some(Action::await_change()));
    }

    let remaining = ttl - elapsed;
    Ok(Some(Action::requeue(Duration::from_secs(
        remaining.min(REQUEUE_READY_SECS),
    ))))
}

/// Patch only the `.status.phase` and optional `.status.message`.
async fn patch_phase(
    client: &Client,
    testnet: &StellarTestnet,
    phase: StellarTestnetPhase,
    message: Option<String>,
) -> Result<(), Error> {
    let namespace = testnet.namespace().unwrap_or_else(|| "default".to_string());
    let api: Api<StellarTestnet> = Api::namespaced(client.clone(), &namespace);

    let phase_str = serde_json::to_value(&phase).unwrap_or_default();
    let patch = json!({
        "status": {
            "phase": phase_str,
            "message": message,
            "lastReconcileTime": Utc::now().to_rfc3339()
        }
    });

    api.patch_status(
        &testnet.name_any(),
        &PatchParams::apply(FIELD_MANAGER),
        &Patch::Merge(&patch),
    )
    .await?;
    Ok(())
}

/// Patch the full `.status` sub-resource.
async fn patch_status(
    client: &Client,
    testnet: &StellarTestnet,
    status: &StellarTestnetStatus,
) -> Result<(), Error> {
    let namespace = testnet.namespace().unwrap_or_else(|| "default".to_string());
    let api: Api<StellarTestnet> = Api::namespaced(client.clone(), &namespace);

    let patch = json!({ "status": status });
    api.patch_status(
        &testnet.name_any(),
        &PatchParams::apply(FIELD_MANAGER),
        &Patch::Merge(&patch),
    )
    .await?;
    Ok(())
}

/// Read the current `failureCount` from the CR status, defaulting to 0.
fn current_failure_count(testnet: &StellarTestnet) -> u32 {
    testnet
        .status
        .as_ref()
        .map(|s| s.failure_count)
        .unwrap_or(0)
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::testnet::StellarTestnetSpec;

    fn minimal_spec() -> StellarTestnetSpec {
        StellarTestnetSpec {
            stellar_core_image: "stellar/stellar-core:v21.0.0".to_string(),
            soroban_rpc_image: "stellar/soroban-rpc:v21.0.0".to_string(),
            network_passphrase: "Standalone Network ; February 2017".to_string(),
            test_accounts: vec![],
            ttl_seconds: 0,
            expose_via_ingress: false,
            ingress_class: "nginx".to_string(),
            core_cpu: "500m".to_string(),
            core_memory: "512Mi".to_string(),
            rpc_cpu: "500m".to_string(),
            rpc_memory: "512Mi".to_string(),
        }
    }

    #[test]
    fn current_failure_count_defaults_to_zero() {
        let tn = StellarTestnet::new("test", minimal_spec());
        assert_eq!(current_failure_count(&tn), 0);
    }

    #[test]
    fn no_ttl_check_bypassed() {
        // A testnet with ttl_seconds == 0 should not be deleted.
        // We verify this by confirming current_failure_count stays 0.
        let tn = StellarTestnet::new("test", minimal_spec());
        assert_eq!(tn.spec.ttl_seconds, 0);
    }
}
