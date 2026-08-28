use crate::epoch_manager::types::EpochManager;
use rayls_consensus_network::{
    error::NetworkError, types::NetworkHandle, ConsensusNetwork, NetworkMetrics, RLMessage,
};
use rayls_consensus_primary::{network::PrimaryNetworkHandle, ConsensusBus};
use rayls_consensus_state_sync::prime_consensus;
use rayls_consensus_worker::WorkerNetworkHandle;
use rayls_infrastructure_config::{ConsensusConfig, NetworkConfig, RaylsDirs};
use rayls_infrastructure_types::{
    BlsPublicKey, Database as ReDatabase, Multiaddr, NetworkPublicKey, NodeMode, TaskSpawner,
};
use std::{sync::Arc, time::Duration};
use tracing::{debug, error, info, warn};

/// Determine the node mode from current state signals.
///
/// Priority (highest to lowest):
/// 1. Not in committee -> Observer
/// 2. Observer flag set -> Observer
/// 3. Explicit mode-transition target pending -> adopt it
/// 4. Prior mode preservation on respawn -> keep current mode (except Observer promotes to
///    CvvInactive when now in-committee without observer_flag; see arm below)
/// 5. Local consensus history exists -> CvvInactive (catch up)
/// 6. No history, first boot -> CvvActive (fresh genesis)
///
/// The sole-committee-member path (single-validator dev chain) is handled before
/// this function is called, via a feature-gated early return in `identify_node_mode`.
pub(crate) fn decide_node_mode(
    in_committee: bool,
    observer_flag: bool,
    explicit_target: Option<NodeMode>,
    initial_epoch: bool,
    prior_mode: NodeMode,
    has_local_history: bool,
) -> (NodeMode, &'static str) {
    if !in_committee {
        return (NodeMode::Observer, "not-in-committee");
    }
    if observer_flag {
        return (NodeMode::Observer, "observer-flag");
    }
    if let Some(target) = explicit_target {
        return (target, "explicit-mode-transition");
    }
    if !initial_epoch {
        return match prior_mode {
            NodeMode::CvvActive => (NodeMode::CvvActive, "prior-mode-active"),
            NodeMode::CvvInactive => (NodeMode::CvvInactive, "prior-mode-inactive"),
            // Promote a dynamic observer just admitted to the committee, instead of leaving it
            // Observer forever. Reaching here means in_committee, !observer_flag, !initial_epoch,
            // prior==Observer - the only path is "was not-in-committee last epoch, just staked in."
            // Join as CvvInactive to catch up on the boundary without proposing/voting; the bridge
            // subscriber requests CvvActive once synced. (Staying Observer would leave a silent
            // committee member - counted toward quorum but never certifying - stalling consensus.)
            NodeMode::Observer => (NodeMode::CvvInactive, "joined-committee"),
        };
    }
    if has_local_history {
        (NodeMode::CvvInactive, "has-local-history")
    } else {
        (NodeMode::CvvActive, "fresh-genesis")
    }
}

/// Returns whether the mode write must be skipped because the node is an explicitly-configured
/// observer being promoted away from Observer.
///
/// Stickiness is an operator contract (`--observer`), not a property of the current mode: a
/// staked node that merely booted into Observer (e.g. while catching up) must stay promotable,
/// or it can never rejoin the committee.
pub(crate) fn is_observer_sticky(
    observer_flag: bool,
    prior_mode: NodeMode,
    target_mode: NodeMode,
) -> bool {
    observer_flag && prior_mode == NodeMode::Observer && target_mode != NodeMode::Observer
}

/// Returns whether the node has executed any consensus output in the chain's history.
///
/// Chain-wide by design, not per-epoch. `committed_round` is the wrong source: `reset_for_epoch`
/// and `prime_consensus`'s cross-epoch reset both zero it, so a node restarting on an epoch
/// boundary would look fresh and boot `CvvActive` (charging into the new epoch and forking past the
/// prior epoch's unexecuted closing output) instead of catching up. The execution anchor's number
/// is never reset per epoch, so it records the true history.
pub(crate) fn node_has_local_history(consensus_bus: &ConsensusBus) -> bool {
    consensus_bus.executed_anchor().borrow().number > 0
}

impl<P, DB> EpochManager<P, DB>
where
    P: RaylsDirs + Clone + 'static,
    DB: ReDatabase,
{
    /// Startup for the node. This creates all components on startup before starting the first
    /// epoch.
    ///
    /// This will create the long-running primary/worker [ConsensusNetwork]s for p2p swarm.
    pub(super) fn spawn_node_networks(
        &mut self,
        node_task_spawner: TaskSpawner,
        network_config: &NetworkConfig,
        network_metrics: Arc<NetworkMetrics>,
    ) -> eyre::Result<()> {
        self.spawn_primary_node_network(
            node_task_spawner.clone(),
            network_config,
            network_metrics.clone(),
        )?;
        self.spawn_worker_node_network(node_task_spawner, network_config, network_metrics)?;

        Ok(())
    }

    pub(super) fn spawn_primary_node_network(
        &mut self,
        node_task_spawner: TaskSpawner,
        network_config: &NetworkConfig,
        network_metrics: Arc<NetworkMetrics>,
    ) -> eyre::Result<()> {
        // create long-running network task for primary
        let primary_network = ConsensusNetwork::new_for_primary(
            network_config,
            self.consensus_bus.primary_network_events_cloned(),
            self.key_config.clone(),
            self.consensus_db.clone(),
            node_task_spawner.clone(),
            self.builder.rayls_infrastructure_config.node_info.primary_network_address().clone(),
            network_metrics,
        )?;
        let primary_network_handle = primary_network.network_handle();
        let node_shutdown = self.node_shutdown.subscribe();

        // spawn long-running primary network task
        node_task_spawner.spawn_critical_task("Primary Network", async move {
            tokio::select!(
                _ = &node_shutdown => {
                    Ok(())
                },
                res = primary_network.run() => {
                    warn!(target: "epoch-manager", ?res, "primary network stopped");
                    res
                },
            )
        });

        // primary network handle
        self.primary_network_handle = Some(PrimaryNetworkHandle::new(primary_network_handle));

        Ok(())
    }

    pub(super) fn spawn_worker_node_network(
        &mut self,
        node_task_spawner: TaskSpawner,
        network_config: &NetworkConfig,
        network_metrics: Arc<NetworkMetrics>,
    ) -> eyre::Result<()> {
        // create long-running network task for worker
        let worker_network = ConsensusNetwork::new_for_worker(
            network_config,
            self.worker_event_stream.clone(),
            self.key_config.clone(),
            self.consensus_db.clone(),
            node_task_spawner.clone(),
            self.builder.rayls_infrastructure_config.node_info.worker_network_address().clone(),
            network_metrics,
        )?;
        let worker_network_handle = worker_network.network_handle();
        let node_shutdown = self.node_shutdown.subscribe();

        // spawn long-running primary network task
        node_task_spawner.spawn_critical_task("Worker Network", async move {
            tokio::select!(
                _ = &node_shutdown => {
                    Ok(())
                }
                res = worker_network.run() => {
                    warn!(target: "epoch-manager", ?res, "worker network stopped");
                    res
                }
            )
        });

        // set temporary task spawner - this is updated with each epoch
        self.worker_network_handle = Some(WorkerNetworkHandle::new(
            worker_network_handle,
            node_task_spawner.clone(),
            network_config.libp2p_config().max_rpc_message_size,
        ));

        Ok(())
    }

    /// Helper method to identify the node's mode:
    /// - "Committee-voting Validator" (CVV)
    /// - "Committee-voting Validator Inactive" (CVVInactive - syncing to rejoin)
    /// - "Observer"
    ///
    /// This method also updates the `ConsensusBus::node_mode()`.
    pub(super) async fn identify_node_mode(
        &self,
        consensus_config: &ConsensusConfig<DB>,
    ) -> eyre::Result<NodeMode> {
        let initial_epoch = self.initial_epoch;
        debug!(target: "epoch-manager", authority_id=?consensus_config.authority_id(), "identifying node mode..." );
        let in_committee = consensus_config
            .authority_id()
            .map(|id| consensus_config.in_committee(&id))
            .unwrap_or(false);

        // prime watch channels before consumers spawn
        prime_consensus(&self.consensus_bus, consensus_config);

        // prior_mode default is unreliable on first run; trust only on respawn
        let committed_round = *self.consensus_bus.committed_round_updates().borrow();
        let has_local_history = node_has_local_history(&self.consensus_bus);
        let observer_flag = self.builder.rayls_infrastructure_config.observer;
        let prior_mode = *self.consensus_bus.node_mode().borrow();
        let explicit_target = *self.consensus_bus.mode_transition().borrow();

        // Single-validator dev chain: the sole member is always the canonical source
        // of truth - it can never be "behind" with no peers to catch up from.
        // Resolve CvvActive directly without going through decide_node_mode, which
        // would otherwise apply the has-local-history -> CvvInactive branch and hang.
        // An explicitly-configured observer is still honored (Observer is sticky)  -
        // decide_node_mode handles that below.
        #[cfg(feature = "dev-single-node-setup")]
        if in_committee && consensus_config.committee().size() == 1 && !observer_flag {
            let mode = NodeMode::CvvActive;
            let reason = "sole-committee-member";
            info!(
                target: "epoch-manager",
                authority_id = ?consensus_config.authority_id(),
                ?mode,
                reason,
                "identify_node_mode: sole committee member (dev) - boot active"
            );
            self.consensus_bus.node_mode().send_modify(|v| *v = mode);
            return Ok(mode);
        }

        let (mode, reason) = decide_node_mode(
            in_committee,
            observer_flag,
            explicit_target,
            initial_epoch,
            prior_mode,
            has_local_history,
        );

        info!(
            target: "epoch-manager",
            authority_id = ?consensus_config.authority_id(),
            ?mode,
            ?prior_mode,
            ?explicit_target,
            reason,
            in_committee,
            observer_flag,
            has_local_history,
            committed_round,
            "identify_node_mode: decided"
        );
        self.consensus_bus.node_mode().send_modify(|v| *v = mode);

        Ok(mode)
    }

    /// Dial peer.
    pub(super) fn dial_peer_bls<Req: RLMessage, Res: RLMessage>(
        handle: NetworkHandle<Req, Res>,
        bls_pubkey: BlsPublicKey,
        node_task_spawner: TaskSpawner,
    ) {
        // spawn dials on long-running task manager
        let task_name = format!("DialPeer {bls_pubkey}");
        node_task_spawner.spawn_task(task_name, async move {
            let mut backoff = 1;
            let mut retries = 0;

            debug!(target: "epoch-manager", ?bls_pubkey, "dialing peer");
            while let Err(e) = handle.dial_by_bls(bls_pubkey).await {
                // ignore errors for peers that are already connected or being dialed
                if matches!(e, NetworkError::AlreadyConnected(_))
                    || matches!(e, NetworkError::AlreadyDialing(_))
                {
                    return;
                }
                retries += 1;

                warn!(target: "epoch-manager", "failed to dial {bls_pubkey}: {e}");
                tokio::time::sleep(Duration::from_secs(backoff)).await;
                if backoff < 120 {
                    backoff += backoff;
                }
                let peers = handle.connected_peer_count().await.unwrap_or(0);
                // We have been trying for a while (at least two max backoffs at 120 secs), if we
                // have any other peers give up.
                if retries > 10 && peers > 0 {
                    error!(target = "dial_peer", "failed to reach peer {bls_pubkey}, giving up");
                    return;
                }
            }
        });
    }

    /// Helper method for parsing provided env var with fallback [Multiaddr]. This is useful to
    /// override the primary/worker swarm listner address for cloud deployments.
    pub(super) fn parse_listener_address_for_swarm(
        env_var: &str,
        network_pubkey: NetworkPublicKey,
        fallback: Multiaddr,
    ) -> eyre::Result<Multiaddr> {
        std::env::var(env_var)
            .map(|addr| {
                addr.parse()
                    .map_err(|e| {
                        eyre::eyre!(
                            "Failed to parse listener multiaddr from env {env_var} ({addr})\n{e}"
                        )
                    })
                    // add Protocol::P2p to multiaddr to maintain consistency with
                    // bin/rayls-network/src/keytool/generate.rs
                    .and_then(|multi: Multiaddr| {
                        multi.with_p2p(network_pubkey.into()).map_err(|_| {
                            eyre::eyre!(
                                "{env_var} multiaddr contains a different P2P protocol {:?}",
                                std::env::var(env_var)
                            )
                        })
                    })
            })
            .unwrap_or(Ok(fallback))
    }
}
