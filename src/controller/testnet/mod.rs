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
//! `StellarTestnet` controller – ephemeral single-node testnet provisioner.
//!
//! # Modules
//!
//! - [`provisioner`]: Creates and tears down all Kubernetes resources owned by
//!   a `StellarTestnet` CR (ConfigMaps, Pod, Service, optional Ingress).
//! - [`reconciler`]: The reconcile + error-policy functions registered with
//!   the `kube-rs` controller runtime.
//!
//! # Starting the controller
//!
//! Call [`run_testnet_controller`] from the operator binary
//! (`src/commands/operator.rs`) to spawn the controller as a background
//! `tokio` task:
//!
//! ```rust,ignore
//! tokio::spawn(async move {
//!     if let Err(e) = controller::testnet::run_testnet_controller(client).await {
//!         tracing::error!("Testnet controller error: {e:?}");
//!     }
//! });
//! ```

pub mod provisioner;
pub mod reconciler;

use std::sync::Arc;

use futures::StreamExt;
use kube::runtime::controller::Controller;
use kube::runtime::watcher::Config as WatcherConfig;
use kube::{Api, Client};
use tracing::info;

use crate::crd::testnet::StellarTestnet;
use crate::error::Error;
use reconciler::TestnetContext;

/// Start the `StellarTestnet` controller and drive it to completion.
///
/// This function runs indefinitely; the caller should spawn it as a `tokio`
/// task.
pub async fn run_testnet_controller(client: Client) -> Result<(), Error> {
    info!("Starting StellarTestnet controller");

    let testnet_api: Api<StellarTestnet> = Api::all(client.clone());

    let ctx = Arc::new(TestnetContext {
        client: client.clone(),
    });

    Controller::new(testnet_api, WatcherConfig::default())
        .shutdown_on_signal()
        .run(reconciler::reconcile, reconciler::error_policy, ctx)
        .for_each(|result| async move {
            match result {
                Ok(obj) => {
                    tracing::debug!(
                        name = %obj.0.name,
                        namespace = ?obj.0.namespace,
                        "Reconciled StellarTestnet"
                    );
                }
                Err(e) => {
                    tracing::warn!(error = %e, "StellarTestnet reconcile error");
                }
            }
        })
        .await;

    Ok(())
}
