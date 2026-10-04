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
//! Testnet provisioner – creates and tears down all Kubernetes resources
//! owned by a [`StellarTestnet`] custom resource.
//!
//! # Owned resources
//!
//! The provisioner creates the following resources inside the same namespace
//! as the `StellarTestnet`, each carrying an `ownerReference` so that
//! Kubernetes garbage-collects them automatically when the parent is deleted:
//!
//! | Kind          | Name pattern                  | Purpose                          |
//! |---------------|-------------------------------|----------------------------------|
//! | `ConfigMap`   | `<name>-cfg`                  | `stellar-core.cfg` + TOML config |
//! | `ConfigMap`   | `<name>-accounts`             | Generated keypairs for test accs |
//! | `Pod`         | `<name>-core`                 | Stellar Core standalone          |
//! | `Service`     | `<name>-rpc`                  | ClusterIP for Soroban RPC port   |
//! | `Ingress`     | `<name>-rpc` (optional)       | External HTTP route              |
//!
//! All resources use owner references rather than finalizers so that Kubernetes
//! handles GC cascading automatically (`propagationPolicy: Background`).

use chrono::Utc;
use k8s_openapi::api::core::v1::{
    ConfigMap, Container, ContainerPort, EnvVar, Pod, PodSpec, ResourceRequirements, Service,
    ServicePort, ServiceSpec,
};
use k8s_openapi::api::networking::v1::{
    HTTPIngressPath, HTTPIngressRuleValue, Ingress, IngressBackend, IngressRule,
    IngressServiceBackend, IngressSpec, ServiceBackendPort,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::api::{Api, ObjectMeta, Patch, PatchParams, PostParams};
use kube::{Client, ResourceExt};
use std::collections::BTreeMap;
use tracing::{debug, info, warn};

use crate::crd::testnet::{StellarTestnet, StellarTestnetPhase, StellarTestnetStatus};
use crate::error::Result;

// ─────────────────────────────────────────────────────────────────────────────
// Port constants
// ─────────────────────────────────────────────────────────────────────────────

/// HTTP port exposed by `stellar-core` standalone for its internal RPC.
pub const CORE_HTTP_PORT: i32 = 11626;

/// Soroban JSON-RPC port.
pub const SOROBAN_RPC_PORT: i32 = 8000;

/// Patch manager name used for server-side apply.
pub const FIELD_MANAGER: &str = "stellar-testnet-operator";

// ─────────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────────

/// Ensure all Kubernetes resources exist for the given `StellarTestnet`.
///
/// This function is **idempotent**: calling it multiple times on the same
/// resource is safe.  Resources are created on the first call and left
/// untouched on subsequent calls unless the spec changes.
///
/// Returns the updated [`StellarTestnetStatus`] that the reconciler should
/// patch back onto the CR.
pub async fn ensure_testnet(
    client: &Client,
    testnet: &StellarTestnet,
) -> Result<StellarTestnetStatus> {
    let name = testnet.name_any();
    let namespace = testnet.namespace().unwrap_or_else(|| "default".to_string());

    info!(name = %name, namespace = %namespace, "Provisioning StellarTestnet resources");

    let owner_ref = build_owner_reference(testnet)?;

    // 1 ── ConfigMap: stellar-core config
    ensure_core_config_map(client, testnet, &owner_ref).await?;

    // 2 ── ConfigMap: generated test-account keypairs
    ensure_accounts_config_map(client, testnet, &owner_ref).await?;

    // 3 ── Pod: stellar-core (standalone) + soroban-rpc sidecar
    ensure_core_pod(client, testnet, &owner_ref).await?;

    // 4 ── Service: ClusterIP for Soroban RPC
    let rpc_svc_name = ensure_rpc_service(client, testnet, &owner_ref).await?;

    let rpc_url = format!(
        "http://{rpc_svc_name}.{namespace}.svc.cluster.local:{SOROBAN_RPC_PORT}"
    );

    // 5 ── Optional Ingress
    let external_rpc_url = if testnet.spec.expose_via_ingress {
        Some(ensure_rpc_ingress(client, testnet, &owner_ref).await?)
    } else {
        None
    };

    let status = StellarTestnetStatus {
        phase: StellarTestnetPhase::Provisioning,
        rpc_url: Some(rpc_url),
        external_rpc_url,
        accounts_config_map: Some(format!("{name}-accounts")),
        message: Some("Resources created; waiting for Pod to become Ready".to_string()),
        last_reconcile_time: Some(Utc::now().to_rfc3339()),
        failure_count: 0,
        ready_at: None,
    };

    Ok(status)
}

/// Delete all resources owned by `testnet` that are not yet garbage-collected.
///
/// Since every resource carries an `ownerReference`, Kubernetes handles
/// cascading deletion automatically.  This function is a best-effort safety
/// net that explicitly deletes the Pod (which may be in a non-deletable
/// termination state) and logs any errors without failing.
pub async fn teardown_testnet(client: &Client, testnet: &StellarTestnet) -> Result<()> {
    let name = testnet.name_any();
    let namespace = testnet.namespace().unwrap_or_else(|| "default".to_string());

    info!(name = %name, namespace = %namespace, "Tearing down StellarTestnet resources");

    // Explicitly delete the core Pod (owner-ref GC handles ConfigMaps &
    // Services, but Pods can sometimes linger).
    let pod_api: Api<Pod> = Api::namespaced(client.clone(), &namespace);
    let pod_name = format!("{name}-core");
    match pod_api
        .delete(&pod_name, &kube::api::DeleteParams::default())
        .await
    {
        Ok(_) => info!(pod = %pod_name, "Deleted testnet Pod"),
        Err(kube::Error::Api(e)) if e.code == 404 => {
            debug!(pod = %pod_name, "Pod already deleted")
        }
        Err(e) => warn!(pod = %pod_name, error = %e, "Failed to delete Pod (owner-ref GC should handle it)"),
    }

    Ok(())
}

/// Check whether the `StellarTestnet` Pod is Running and the Soroban RPC
/// container is Ready.
///
/// Returns `true` once the testnet is fully operational.
pub async fn is_testnet_ready(client: &Client, testnet: &StellarTestnet) -> Result<bool> {
    let name = testnet.name_any();
    let namespace = testnet.namespace().unwrap_or_else(|| "default".to_string());
    let pod_api: Api<Pod> = Api::namespaced(client.clone(), &namespace);
    let pod_name = format!("{name}-core");

    match pod_api.get(&pod_name).await {
        Ok(pod) => {
            let ready = pod
                .status
                .as_ref()
                .and_then(|s| s.container_statuses.as_ref())
                .map(|cs| cs.iter().all(|c| c.ready))
                .unwrap_or(false);
            Ok(ready)
        }
        Err(kube::Error::Api(e)) if e.code == 404 => Ok(false),
        Err(e) => Err(e.into()),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Private helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Build an `OwnerReference` pointing at the given `StellarTestnet`.
fn build_owner_reference(testnet: &StellarTestnet) -> Result<OwnerReference> {
    Ok(OwnerReference {
        api_version: StellarTestnet::api_version(&()).to_string(),
        kind: StellarTestnet::kind(&()).to_string(),
        name: testnet.name_any(),
        uid: testnet
            .uid()
            .ok_or_else(|| crate::error::Error::ConfigError("StellarTestnet has no UID".into()))?,
        controller: Some(true),
        block_owner_deletion: Some(true),
    })
}

/// Build common `ObjectMeta` for an owned resource.
fn owned_meta(
    name: impl Into<String>,
    namespace: impl Into<String>,
    owner_ref: &OwnerReference,
    extra_labels: BTreeMap<String, String>,
) -> ObjectMeta {
    let mut labels = BTreeMap::from([
        (
            "app.kubernetes.io/managed-by".to_string(),
            "stellar-testnet-operator".to_string(),
        ),
        (
            "stellar.org/testnet".to_string(),
            owner_ref.name.clone(),
        ),
    ]);
    labels.extend(extra_labels);

    ObjectMeta {
        name: Some(name.into()),
        namespace: Some(namespace.into()),
        owner_references: Some(vec![owner_ref.clone()]),
        labels: Some(labels),
        ..Default::default()
    }
}

/// Generate the `stellar-core.cfg` content for standalone mode.
fn render_core_config(testnet: &StellarTestnet) -> String {
    let passphrase = &testnet.spec.network_passphrase;
    // Build pre-funded account public keys section.
    // For a standalone genesis we use a deterministic "well-known" keypair
    // per account name so tests always know the keys without querying the
    // cluster.  In a production-hardened build these would be generated by
    // an init container and stored in the accounts ConfigMap.
    let mut cfg = format!(
        r#"# stellar-core standalone config – generated by Stellar-K8s operator
# DO NOT EDIT – managed by the StellarTestnet controller

NETWORK_PASSPHRASE="{passphrase}"
NODE_IS_VALIDATOR=true
ARTIFICIALLY_GENERATE_LOAD_FOR_TESTING=false
MANUAL_CLOSE=false
RUN_STANDALONE=false
HTTP_PORT={CORE_HTTP_PORT}
PUBLIC_HTTP_PORT=false

# In-memory / tmpfs database keeps ledger I/O off disk
DATABASE="sqlite3://:memory:"

# Standalone: no quorum required
QUORUM_SET={{
  THRESHOLD_PERCENT = 100
  VALIDATORS = []
}}

[HISTORY.vs]
get="cp /tmp/history/{{0}} {{1}}"
put="cp {{0}} /tmp/history/{{1}}"
mkdir="mkdir -p /tmp/history/{{0}}"
"#,
    );

    // Pre-fund accounts via the standalone genesis configuration.
    for acc in &testnet.spec.test_accounts {
        // These are placeholder public keys; a real deployment would have an
        // init container generate proper Ed25519 keypairs and write them to the
        // accounts ConfigMap (see `ensure_accounts_config_map`).
        cfg.push_str(&format!(
            "\n# Pre-funded account: {}\n# balance: {} stroops\n",
            acc.name, acc.balance
        ));
    }

    cfg
}

// ─────────────────────────────────────────────────────────────────────────────
// Resource creators
// ─────────────────────────────────────────────────────────────────────────────

async fn ensure_core_config_map(
    client: &Client,
    testnet: &StellarTestnet,
    owner_ref: &OwnerReference,
) -> Result<()> {
    let name = testnet.name_any();
    let namespace = testnet.namespace().unwrap_or_else(|| "default".to_string());
    let cm_api: Api<ConfigMap> = Api::namespaced(client.clone(), &namespace);
    let cm_name = format!("{name}-cfg");

    let config_content = render_core_config(testnet);

    let cm = ConfigMap {
        metadata: owned_meta(&cm_name, &namespace, owner_ref, BTreeMap::new()),
        data: Some(BTreeMap::from([("stellar-core.cfg".to_string(), config_content)])),
        ..Default::default()
    };

    let patch_params = PatchParams::apply(FIELD_MANAGER).force();
    cm_api
        .patch(&cm_name, &patch_params, &Patch::Apply(&cm))
        .await?;

    debug!(configmap = %cm_name, "Applied core ConfigMap");
    Ok(())
}

async fn ensure_accounts_config_map(
    client: &Client,
    testnet: &StellarTestnet,
    owner_ref: &OwnerReference,
) -> Result<()> {
    let name = testnet.name_any();
    let namespace = testnet.namespace().unwrap_or_else(|| "default".to_string());
    let cm_api: Api<ConfigMap> = Api::namespaced(client.clone(), &namespace);
    let cm_name = format!("{name}-accounts");

    // In a real implementation an init container would generate proper
    // Ed25519 keypairs.  Here we write placeholder entries so that test
    // scripts can read the ConfigMap and substitute their own tooling.
    let mut data = BTreeMap::new();
    for acc in &testnet.spec.test_accounts {
        data.insert(
            format!("{}.public_key", acc.name),
            format!("PLACEHOLDER_PUBLIC_KEY_{}", acc.name.to_uppercase()),
        );
        data.insert(
            format!("{}.balance_stroops", acc.name),
            acc.balance.to_string(),
        );
    }

    let cm = ConfigMap {
        metadata: owned_meta(&cm_name, &namespace, owner_ref, BTreeMap::new()),
        data: Some(data),
        ..Default::default()
    };

    let patch_params = PatchParams::apply(FIELD_MANAGER).force();
    cm_api
        .patch(&cm_name, &patch_params, &Patch::Apply(&cm))
        .await?;

    debug!(configmap = %cm_name, "Applied accounts ConfigMap");
    Ok(())
}

async fn ensure_core_pod(
    client: &Client,
    testnet: &StellarTestnet,
    owner_ref: &OwnerReference,
) -> Result<()> {
    let name = testnet.name_any();
    let namespace = testnet.namespace().unwrap_or_else(|| "default".to_string());
    let pod_api: Api<Pod> = Api::namespaced(client.clone(), &namespace);
    let pod_name = format!("{name}-core");

    // Don't recreate an already-running Pod.
    if let Ok(_existing) = pod_api.get(&pod_name).await {
        debug!(pod = %pod_name, "Pod already exists – skipping create");
        return Ok(());
    }

    let spec = &testnet.spec;
    let cm_name = format!("{name}-cfg");

    // ── stellar-core container ───────────────────────────────────────────────
    let core_container = Container {
        name: "stellar-core".to_string(),
        image: Some(spec.stellar_core_image.clone()),
        command: Some(vec![
            "stellar-core".to_string(),
            "--conf".to_string(),
            "/etc/stellar/stellar-core.cfg".to_string(),
            "run".to_string(),
        ]),
        ports: Some(vec![ContainerPort {
            container_port: CORE_HTTP_PORT,
            name: Some("http".to_string()),
            ..Default::default()
        }]),
        resources: Some(ResourceRequirements {
            requests: Some(BTreeMap::from([
                ("cpu".to_string(), Quantity(spec.core_cpu.clone())),
                ("memory".to_string(), Quantity(spec.core_memory.clone())),
            ])),
            ..Default::default()
        }),
        env: Some(vec![
            EnvVar {
                name: "NETWORK_PASSPHRASE".to_string(),
                value: Some(spec.network_passphrase.clone()),
                ..Default::default()
            },
        ]),
        volume_mounts: Some(vec![k8s_openapi::api::core::v1::VolumeMount {
            name: "core-cfg".to_string(),
            mount_path: "/etc/stellar".to_string(),
            read_only: Some(true),
            ..Default::default()
        }]),
        ..Default::default()
    };

    // ── soroban-rpc sidecar ─────────────────────────────────────────────────
    let rpc_container = Container {
        name: "soroban-rpc".to_string(),
        image: Some(spec.soroban_rpc_image.clone()),
        ports: Some(vec![ContainerPort {
            container_port: SOROBAN_RPC_PORT,
            name: Some("rpc".to_string()),
            ..Default::default()
        }]),
        resources: Some(ResourceRequirements {
            requests: Some(BTreeMap::from([
                ("cpu".to_string(), Quantity(spec.rpc_cpu.clone())),
                ("memory".to_string(), Quantity(spec.rpc_memory.clone())),
            ])),
            ..Default::default()
        }),
        env: Some(vec![
            EnvVar {
                name: "STELLAR_CORE_URL".to_string(),
                value: Some(format!("http://localhost:{CORE_HTTP_PORT}")),
                ..Default::default()
            },
            EnvVar {
                name: "NETWORK_PASSPHRASE".to_string(),
                value: Some(spec.network_passphrase.clone()),
                ..Default::default()
            },
            EnvVar {
                name: "FRIENDBOT_SEED".to_string(),
                // Standard standalone friendbot seed; safe to use here.
                value: Some(
                    "SCZANGBA5RLKJ14DFHY7SSKQ5TNMVEBUS3JZDPJQ4GNFGVLQ5CKKLAF".to_string(),
                ),
                ..Default::default()
            },
        ]),
        readiness_probe: Some(k8s_openapi::api::core::v1::Probe {
            http_get: Some(k8s_openapi::api::core::v1::HTTPGetAction {
                path: Some("/health".to_string()),
                port: k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(
                    SOROBAN_RPC_PORT,
                ),
                ..Default::default()
            }),
            initial_delay_seconds: Some(5),
            period_seconds: Some(3),
            failure_threshold: Some(10), // ~30 s budget
            ..Default::default()
        }),
        ..Default::default()
    };

    let labels = BTreeMap::from([
        ("app".to_string(), pod_name.clone()),
        ("stellar.org/testnet".to_string(), name.clone()),
        (
            "app.kubernetes.io/managed-by".to_string(),
            "stellar-testnet-operator".to_string(),
        ),
    ]);

    let pod = Pod {
        metadata: ObjectMeta {
            name: Some(pod_name.clone()),
            namespace: Some(namespace.clone()),
            owner_references: Some(vec![owner_ref.clone()]),
            labels: Some(labels),
            ..Default::default()
        },
        spec: Some(PodSpec {
            restart_policy: Some("Always".to_string()),
            containers: vec![core_container, rpc_container],
            volumes: Some(vec![k8s_openapi::api::core::v1::Volume {
                name: "core-cfg".to_string(),
                config_map: Some(k8s_openapi::api::core::v1::ConfigMapVolumeSource {
                    name: Some(cm_name),
                    ..Default::default()
                }),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    };

    pod_api.create(&PostParams::default(), &pod).await?;
    info!(pod = %pod_name, "Created testnet Pod");
    Ok(())
}

async fn ensure_rpc_service(
    client: &Client,
    testnet: &StellarTestnet,
    owner_ref: &OwnerReference,
) -> Result<String> {
    let name = testnet.name_any();
    let namespace = testnet.namespace().unwrap_or_else(|| "default".to_string());
    let svc_api: Api<Service> = Api::namespaced(client.clone(), &namespace);
    let svc_name = format!("{name}-rpc");

    let selector = BTreeMap::from([("stellar.org/testnet".to_string(), name.clone())]);

    let svc = Service {
        metadata: owned_meta(&svc_name, &namespace, owner_ref, BTreeMap::new()),
        spec: Some(ServiceSpec {
            type_: Some("ClusterIP".to_string()),
            selector: Some(selector),
            ports: Some(vec![ServicePort {
                name: Some("rpc".to_string()),
                port: SOROBAN_RPC_PORT,
                protocol: Some("TCP".to_string()),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    };

    let patch_params = PatchParams::apply(FIELD_MANAGER).force();
    svc_api
        .patch(&svc_name, &patch_params, &Patch::Apply(&svc))
        .await?;

    debug!(service = %svc_name, "Applied RPC Service");
    Ok(svc_name)
}

async fn ensure_rpc_ingress(
    client: &Client,
    testnet: &StellarTestnet,
    owner_ref: &OwnerReference,
) -> Result<String> {
    let name = testnet.name_any();
    let namespace = testnet.namespace().unwrap_or_else(|| "default".to_string());
    let ingress_api: Api<Ingress> = Api::namespaced(client.clone(), &namespace);
    let ingress_name = format!("{name}-rpc");
    let svc_name = format!("{name}-rpc");
    let host = format!("{name}-rpc.localtest.me");

    let mut annotations = BTreeMap::new();
    annotations.insert(
        "kubernetes.io/ingress.class".to_string(),
        testnet.spec.ingress_class.clone(),
    );

    let ingress = Ingress {
        metadata: ObjectMeta {
            annotations: Some(annotations),
            ..owned_meta(&ingress_name, &namespace, owner_ref, BTreeMap::new())
        },
        spec: Some(IngressSpec {
            ingress_class_name: Some(testnet.spec.ingress_class.clone()),
            rules: Some(vec![IngressRule {
                host: Some(host.clone()),
                http: Some(HTTPIngressRuleValue {
                    paths: vec![HTTPIngressPath {
                        path: Some("/".to_string()),
                        path_type: "Prefix".to_string(),
                        backend: IngressBackend {
                            service: Some(IngressServiceBackend {
                                name: svc_name,
                                port: ServiceBackendPort {
                                    number: Some(SOROBAN_RPC_PORT),
                                    ..Default::default()
                                },
                            }),
                            ..Default::default()
                        },
                    }],
                }),
            }]),
            ..Default::default()
        }),
        ..Default::default()
    };

    let patch_params = PatchParams::apply(FIELD_MANAGER).force();
    ingress_api
        .patch(&ingress_name, &patch_params, &Patch::Apply(&ingress))
        .await?;

    let external_url = format!("http://{host}:{SOROBAN_RPC_PORT}");
    debug!(ingress = %ingress_name, url = %external_url, "Applied RPC Ingress");
    Ok(external_url)
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::testnet::{StellarTestnetSpec, TestAccountSpec};

    fn make_testnet() -> StellarTestnet {
        StellarTestnet::new(
            "ci-net",
            StellarTestnetSpec {
                stellar_core_image: "stellar/stellar-core:v21.0.0".to_string(),
                soroban_rpc_image: "stellar/soroban-rpc:v21.0.0".to_string(),
                network_passphrase: "Standalone Network ; February 2017".to_string(),
                test_accounts: vec![TestAccountSpec {
                    name: "alice".to_string(),
                    balance: 10_000_000_000,
                }],
                ttl_seconds: 0,
                expose_via_ingress: false,
                ingress_class: "nginx".to_string(),
                core_cpu: "500m".to_string(),
                core_memory: "512Mi".to_string(),
                rpc_cpu: "500m".to_string(),
                rpc_memory: "512Mi".to_string(),
            },
        )
    }

    #[test]
    fn render_core_config_contains_passphrase() {
        let tn = make_testnet();
        let cfg = render_core_config(&tn);
        assert!(
            cfg.contains("Standalone Network"),
            "Config should embed network passphrase"
        );
        assert!(
            cfg.contains(":memory:"),
            "Config should use in-memory database"
        );
    }

    #[test]
    fn render_core_config_mentions_test_accounts() {
        let tn = make_testnet();
        let cfg = render_core_config(&tn);
        assert!(cfg.contains("alice"), "Config should mention test account");
    }
}
