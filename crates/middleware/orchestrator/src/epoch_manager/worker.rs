use crate::{
    engine::ExecutionNode, epoch_manager::types::EpochManager, types::EngineToPrimaryRpc,
    worker::WorkerNode,
};
use eyre::OptionExt;
use rayls_consensus_worker::{WorkerNetwork, WorkerNetworkHandle};
use rayls_execution_evm::chainspec::{RaylsChainHardforks, RaylsHardforks};
use rayls_infrastructure_config::{ConsensusConfig, LibP2pConfig, RaylsDirs};
use rayls_infrastructure_types::{
    gas_accumulator::GasAccumulator, BatchValidation, BlsPublicKey, Database as ReDatabase,
    RaylsSender, TaskSpawner,
};
use std::{collections::HashSet, sync::Arc};
use tracing::{debug, info};

impl<P, DB> EpochManager<P, DB>
where
    P: RaylsDirs + Clone + 'static,
    DB: ReDatabase,
{
    /// Create a [WorkerNode].
    pub(super) async fn spawn_worker_node_components(
        &mut self,
        consensus_config: &ConsensusConfig<DB>,
        engine: &ExecutionNode,
        epoch_task_spawner: TaskSpawner,
        engine_to_primary: EngineToPrimaryRpc<DB>,
        gas_accumulator: GasAccumulator,
    ) -> eyre::Result<WorkerNode<DB>> {
        let initial_epoch = self.initial_epoch;
        // only support one worker for now (with id 0) - otherwise, loop here
        let worker_id = 0;
        let base_fee = gas_accumulator.base_fee(worker_id);

        // update the network handle's task spawner for reporting batches in the epoch
        {
            let network_handle = self
                .worker_network_handle
                .as_mut()
                .ok_or_eyre("worker network handle missing from epoch manager")?;

            network_handle.update_task_spawner(epoch_task_spawner.clone());
            // initialize worker components on startup
            // This will use the new epoch_task_spawner on network_handle.
            if initial_epoch {
                engine
                    .initialize_worker_components(
                        worker_id,
                        network_handle.clone(),
                        engine_to_primary,
                    )
                    .await?;
            } else {
                // We updated our epoch task spawner so make sure worker network tasks are
                // restarted.
                engine.respawn_worker_network_tasks(network_handle.clone()).await;
            }
        }

        let network_handle = self
            .worker_network_handle
            .as_ref()
            .ok_or_eyre("worker network handle missing from epoch manager")?
            .clone();

        let validator = engine
            .new_batch_validator(&worker_id, base_fee, consensus_config.committee().epoch())
            .await;
        self.spawn_worker_network_for_epoch(
            consensus_config,
            &worker_id,
            validator.clone(),
            epoch_task_spawner,
            &network_handle,
        )
        .await?;

        let worker =
            WorkerNode::new(worker_id, consensus_config.clone(), network_handle.clone(), validator);

        Ok(worker)
    }

    /// Create the worker network.
    pub(super) async fn spawn_worker_network_for_epoch(
        &mut self,
        consensus_config: &ConsensusConfig<DB>,
        worker_id: &u16,
        validator: Arc<dyn BatchValidation>,
        epoch_task_spawner: TaskSpawner,
        network_handle: &WorkerNetworkHandle,
    ) -> eyre::Result<()> {
        let initial_epoch = self.initial_epoch;
        // get event streams for the worker network handler
        let rx_event_stream = self.worker_event_stream.subscribe();
        debug!(target: "epoch-manager", "spawning worker network for epoch");

        let committee_keys: HashSet<BlsPublicKey> = consensus_config
            .committee()
            .authorities()
            .into_iter()
            .map(|a| *a.protocol_key())
            .collect();

        // start listening if the network needs to be initialized
        if initial_epoch {
            let worker_address = Self::parse_listener_address_for_swarm(
                "WORKER_LISTENER_MULTIADDR",
                consensus_config.primary_networkkey(),
                consensus_config.worker_address(),
            )?;
            // Explicit relay reservations, using the worker's own network key for the circuit
            // listen addresses (see the primary's equivalent block).
            let relay_reservations = Self::relay_listen_addresses(
                "WORKER_RELAY_MULTIADDRS",
                consensus_config.worker_networkkey(),
            )?;
            super::network::start_swarm_listeners(
                network_handle.inner_handle(),
                worker_address,
                relay_reservations,
                "WORKER_RELAY_MULTIADDRS",
            )
            .await?;
        }

        // Rayls: Always rebuild identity mappings (known_peers, known_peerids) every epoch.
        // This fixes the race condition where gossip arrives before identity mappings exist,
        // causing committee members to be incorrectly banned after restart.
        // Previously only called on initial_epoch, but needed for all epochs to handle restarts.
        network_handle
            .inner_handle()
            .add_bootstrap_peers(
                consensus_config
                    .committee()
                    .bootstrap_servers()
                    .iter()
                    .map(|(k, v)| (*k, v.worker.clone()))
                    .collect(),
            )
            .await?;

        network_handle.inner_handle().new_epoch(committee_keys.clone()).await?;

        let worker_address = consensus_config.worker_address();

        // always attempt to dial peers for the new epoch
        // the network's peer manager will intercept dial attempts for peers that are already
        // connected
        debug!(target: "epoch-manager", ?worker_address, "spawning worker network for epoch");
        for (_, peer) in consensus_config
            .committee()
            .others_primaries_by_id(consensus_config.authority().as_ref().map(|a| a.id()).as_ref())
        {
            Self::dial_peer_bls(
                network_handle.inner_handle().clone(),
                peer,
                epoch_task_spawner.clone(),
            );
        }

        // refresh gossipsub mesh for worker topics to ensure peers are grafted
        // after restart/recovery (unsubscribe + subscribe forces gossipsub to
        // re-announce the subscription to connected peers)
        let txn_topic = LibP2pConfig::worker_txn_topic();
        info!(target: "epoch-manager::gossipsub", ?txn_topic, "subscribing to worker txn topic");
        network_handle.inner_handle().subscribe(txn_topic.clone()).await?;

        // Get gossip from committee members about batches.
        // Useful for non-CVVs to prefetch and harmless for CVVs.
        let batch_topic = LibP2pConfig::worker_batch_topic();
        let committee_keys = committee_keys.into_iter().collect::<HashSet<_>>();

        info!(target: "epoch-manager", batch_topic=?batch_topic, committee=?committee_keys, "subscribing to gossip about batches");
        network_handle
            .inner_handle()
            .subscribe_with_publishers(batch_topic.clone(), committee_keys)
            .await?;

        // log worker gossipsub mesh state after subscribing
        let worker_connected =
            network_handle.inner_handle().connected_peer_count().await.unwrap_or(0);
        let mesh_txn =
            network_handle.inner_handle().mesh_peers(txn_topic).await.map(|p| p.len()).unwrap_or(0);
        let mesh_batch = network_handle
            .inner_handle()
            .mesh_peers(batch_topic)
            .await
            .map(|p| p.len())
            .unwrap_or(0);
        info!(target: "epoch-manager::gossipsub",
            worker_connected,
            mesh_txn,
            mesh_batch,
            "worker gossipsub mesh state after subscriptions"
        );

        // spawn worker network
        WorkerNetwork::new(
            rx_event_stream,
            network_handle.clone(),
            consensus_config.clone(),
            *worker_id,
            validator,
        )
        .spawn(&epoch_task_spawner);

        Ok(())
    }

    /// Use accumulated gas information to set each workers base fee for the epoch.
    ///
    /// After the EIP-1559 per-block fork: no-op (each block self-adjusts via payload builder).
    /// Before the fork: currently a no-op stub (unchanged from main).
    pub(super) fn adjust_base_fees(&self, gas_accumulator: &GasAccumulator, block_number: u64) {
        let network = self.builder.rayls_infrastructure_config.parameters.network;
        let hardforks = RaylsChainHardforks::for_network(network);
        if hardforks.is_eip1559_active_at_block(block_number) {
            // per-block EIP-1559 active — base fee is updated per-block by the payload builder
            return;
        }
        for worker_id in 0..gas_accumulator.num_workers() {
            let worker_id = worker_id as u16;
            let (_blocks, _gas_used) = gas_accumulator.get_values(worker_id);
            // Change this base fee to update base fee in batches workers create.
            let _base_fee = gas_accumulator.base_fee(worker_id);
        }
    }
}
