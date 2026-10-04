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
//! StellarTestnet Custom Resource Definition
//!
//! The `StellarTestnet` CRD represents an ephemeral, single-node Stellar
//! standalone testnet intended for CI/CD pipelines and smart-contract
//! development. The operator:
//!
//! 1. Spins up a lightweight `stellar-core` standalone Pod backed by an
//!    in-memory (SQLite / RAM-disk) database so ledgers close in < 1 s.
//! 2. Creates a Soroban RPC sidecar pointed at the in-memory core.
//! 3. Auto-generates Kubernetes `Service` and optional `Ingress` resources
//!    and surfaces the resulting endpoints in `.status.rpcUrl` and
//!    `.status.horizonUrl`.
//! 4. Pre-funds test accounts listed in `spec.testAccounts` using the
//!    standalone genesis ledger.
//! 5. Fully tears down every owned resource when the CRD is deleted
//!    (via an owner-reference finalizer) to prevent cluster bloat.
//!
//! # Lifecycle
//!
//! ```text
//! Pending ──► Provisioning ──► Ready ──► (Deleting)
//!                  │                       │
//!                  └──► Failed             └──► resources GC'd by owner refs
//! ```
//!
//! # Validation target
//!
//! A testnet must be *Ready* (Soroban RPC endpoint responding) within 30
//! seconds of creation.  The readiness probe in the Soroban RPC container
//! enforces this budget.
//!
//! # Example manifest
//!
//! ```yaml
//! apiVersion: stellar.org/v1alpha1
//! kind: StellarTestnet
//! metadata:
//!   name: my-ci-net
//!   namespace: stellar-ci
//! spec:
//!   stellarCoreImage: "stellar/stellar-core:v21.0.0"
//!   sorobanRpcImage:  "stellar/soroban-rpc:v21.0.0"
//!   testAccounts:
//!     - name: alice
//!       balance: 10000000000   # 1 000 XLM in stroops
//!     - name: bob
//!       balance: 10000000000
//!   ttlSeconds: 3600
//! ```

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// ─────────────────────────────────────────────────────────────────────────────
// Spec
// ─────────────────────────────────────────────────────────────────────────────

/// Specification for an ephemeral Stellar standalone testnet.
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "stellar.org",
    version = "v1alpha1",
    kind = "StellarTestnet",
    namespaced,
    status = "StellarTestnetStatus",
    shortname = "stn",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"RPC URL","type":"string","jsonPath":".status.rpcUrl"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct StellarTestnetSpec {
    // ── Images ─────────────────────────────────────────────────────────────

    /// OCI image for the `stellar-core` standalone container.
    /// Defaults to `"stellar/stellar-core:latest"`.
    #[serde(default = "default_core_image")]
    pub stellar_core_image: String,

    /// OCI image for the `soroban-rpc` sidecar container.
    /// Defaults to `"stellar/soroban-rpc:latest"`.
    #[serde(default = "default_soroban_image")]
    pub soroban_rpc_image: String,

    // ── Genesis configuration ───────────────────────────────────────────────

    /// Network passphrase written into `stellar-core.cfg`.
    /// Defaults to `"Standalone Network ; February 2017"` (the canonical
    /// standalone-mode passphrase used by the Stellar development tools).
    #[serde(default = "default_network_passphrase")]
    pub network_passphrase: String,

    /// Test accounts to create and fund at genesis.
    /// Each entry becomes a `CREATE_ACCOUNT` operation in the genesis ledger.
    #[serde(default)]
    pub test_accounts: Vec<TestAccountSpec>,

    // ── Lifecycle ──────────────────────────────────────────────────────────

    /// Optional TTL in seconds.  The operator automatically deletes the
    /// `StellarTestnet` resource when this duration elapses after creation.
    /// Set to `0` (or omit) for an indefinite lifetime.
    #[serde(default)]
    pub ttl_seconds: u64,

    // ── Ingress ────────────────────────────────────────────────────────────

    /// When `true` the operator creates a Kubernetes `Ingress` for the
    /// Soroban RPC port and writes the external URL to `.status.rpcUrl`.
    /// Requires an ingress controller in the cluster.
    #[serde(default)]
    pub expose_via_ingress: bool,

    /// IngressClass name to use when `exposeViaIngress` is `true`.
    /// Defaults to `"nginx"`.
    #[serde(default = "default_ingress_class")]
    pub ingress_class: String,

    // ── Resources ──────────────────────────────────────────────────────────

    /// CPU request for the `stellar-core` container (Kubernetes units, e.g. `"500m"`).
    #[serde(default = "default_core_cpu")]
    pub core_cpu: String,

    /// Memory request for the `stellar-core` container (Kubernetes units, e.g. `"512Mi"`).
    #[serde(default = "default_core_memory")]
    pub core_memory: String,

    /// CPU request for the `soroban-rpc` sidecar (Kubernetes units, e.g. `"500m"`).
    #[serde(default = "default_rpc_cpu")]
    pub rpc_cpu: String,

    /// Memory request for the `soroban-rpc` sidecar (Kubernetes units, e.g. `"512Mi"`).
    #[serde(default = "default_rpc_memory")]
    pub rpc_memory: String,
}

// ─────────────────────────────────────────────────────────────────────────────
// Nested spec types
// ─────────────────────────────────────────────────────────────────────────────

/// A pre-funded test account seeded into the genesis ledger.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TestAccountSpec {
    /// Human-readable label (used as a ConfigMap key for the keypair).
    pub name: String,

    /// Initial balance in *stroops* (1 XLM = 10 000 000 stroops).
    /// Must be ≥ 10 000 000 (the base reserve of 1 XLM).
    #[serde(default = "default_balance")]
    pub balance: u64,
}

// ─────────────────────────────────────────────────────────────────────────────
// Status
// ─────────────────────────────────────────────────────────────────────────────

/// Observed state of a `StellarTestnet`.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StellarTestnetStatus {
    /// Lifecycle phase of the testnet.
    pub phase: StellarTestnetPhase,

    /// Cluster-internal Soroban RPC endpoint, e.g.
    /// `"http://my-ci-net-rpc.stellar-ci.svc.cluster.local:8000"`.
    /// Populated once the Pod is running.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rpc_url: Option<String>,

    /// External Soroban RPC endpoint exposed via `Ingress` (if enabled).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_rpc_url: Option<String>,

    /// Kubernetes `ConfigMap` name that contains the generated test-account
    /// keypairs (public/private keys).  Format:
    /// `"<name>-accounts"`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accounts_config_map: Option<String>,

    /// Human-readable error message when `phase == Failed`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    /// RFC 3339 timestamp of the last reconcile.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_reconcile_time: Option<String>,

    /// Number of consecutive reconcile failures (reset on success).
    #[serde(default)]
    pub failure_count: u32,

    /// RFC 3339 timestamp when the testnet became Ready.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready_at: Option<String>,
}

/// Lifecycle phase of a `StellarTestnet` resource.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub enum StellarTestnetPhase {
    /// The resource has been accepted by the API server but reconciliation has
    /// not started yet.
    #[default]
    Pending,

    /// The operator is creating Pods, Services, and ConfigMaps.
    Provisioning,

    /// The Soroban RPC endpoint is available and responding.
    Ready,

    /// A recoverable error occurred; the operator will retry.
    Failed,

    /// The resource is being deleted and owned resources are being garbage-
    /// collected.
    Deleting,
}

// ─────────────────────────────────────────────────────────────────────────────
// Default helpers
// ─────────────────────────────────────────────────────────────────────────────

fn default_core_image() -> String {
    "stellar/stellar-core:latest".to_string()
}
fn default_soroban_image() -> String {
    "stellar/soroban-rpc:latest".to_string()
}
fn default_network_passphrase() -> String {
    "Standalone Network ; February 2017".to_string()
}
fn default_ingress_class() -> String {
    "nginx".to_string()
}
fn default_core_cpu() -> String {
    "500m".to_string()
}
fn default_core_memory() -> String {
    "512Mi".to_string()
}
fn default_rpc_cpu() -> String {
    "500m".to_string()
}
fn default_rpc_memory() -> String {
    "512Mi".to_string()
}
fn default_balance() -> u64 {
    10_000_000_000 // 1 000 XLM
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_spec_serialises() {
        let spec = StellarTestnetSpec {
            stellar_core_image: default_core_image(),
            soroban_rpc_image: default_soroban_image(),
            network_passphrase: default_network_passphrase(),
            test_accounts: vec![],
            ttl_seconds: 0,
            expose_via_ingress: false,
            ingress_class: default_ingress_class(),
            core_cpu: default_core_cpu(),
            core_memory: default_core_memory(),
            rpc_cpu: default_rpc_cpu(),
            rpc_memory: default_rpc_memory(),
        };
        let json = serde_json::to_string(&spec).expect("serialise");
        assert!(json.contains("stellarCoreImage"));
    }

    #[test]
    fn default_status_phase_is_pending() {
        let status = StellarTestnetStatus::default();
        assert_eq!(status.phase, StellarTestnetPhase::Pending);
    }

    #[test]
    fn test_account_defaults() {
        let acc = TestAccountSpec {
            name: "alice".to_string(),
            balance: default_balance(),
        };
        assert_eq!(acc.balance, 10_000_000_000);
    }
}
