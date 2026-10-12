use crate::{epoch_manager::types::EpochManager, primary::PrimaryNode};
use eyre::OptionExt;
use rayls_consensus_primary::{
    network::{PrimaryNetwork, PrimaryNetworkHandle},
    StateSynchronizer,
};
use rayls_infrastructure_config::{ConsensusConfig, LibP2pConfig, RaylsDirs};
use rayls_infrastructure_types::{
    quorum_threshold, BlsPublicKey, Database as ReDatabase, RaylsSender, TaskSpawner,
};
use std::{collections::HashSet, time::Duration};
use tracing::{debug, info, warn};

impl<P, DB> EpochManager<P, DB>
where
    P: RaylsDirs + Clone + 'static,
    DB: ReDatabase,
{
    /// Create a [PrimaryNode].
    ///
    /// This also creates the [PrimaryNetwork].
    pub(super) async fn create_primary_node_components(
        &mut self,
        consensus_config: &ConsensusConfig<DB>,
        epoch_task_spawner: TaskSpawner,
    ) -> eyre::Result<PrimaryNode<DB>> {
        let rayls_consensus_state_sync = StateSynchronizer::new(
            consensus_config.clone(),
            self.consensus_bus.clone(),
            epoch_task_spawner.clone(),
        );
        let network_handle = self
            .primary_network_handle
            .as_ref()
            .ok_or_eyre("primary network handle missing from epoch manager")?
            .clone();

        // create the epoch-specific `PrimaryNetwork`
        self.spawn_primary_network_for_epoch(
            consensus_config,
            rayls_consensus_state_sync.clone(),
            epoch_task_spawner.clone(),
            &network_handle,
        )
        .await?;

        // spawn primary - create node and spawn network
        let primary = PrimaryNode::new(
            consensus_config.clone(),
            self.consensus_bus.clone(),
            network_handle,
            rayls_consensus_state_sync,
        );

        Ok(primary)
    }

    /// Create the primary network for the specific epoch.
    ///
    /// This is not the swarm level, but the [PrimaryNetwork] interface.
    pub(super) async fn spawn_primary_network_for_epoch(
        &mut self,
        consensus_config: &ConsensusConfig<DB>,
        rayls_consensus_state_sync: StateSynchronizer<DB>,
        epoch_task_spawner: TaskSpawner,
        network_handle: &PrimaryNetworkHandle,
    ) -> eyre::Result<()> {
        let initial_epoch = self.initial_epoch;
        // get event streams for the primary network handler
        let event_stream = self.consensus_bus.primary_network_events().clone();
        let rx_event_stream = event_stream.subscribe();

        // set committee for network to prevent banning
        debug!(target: "epoch-manager", auth=?consensus_config.authority_id(), "spawning primary network for epoch");
        let committee_keys: HashSet<BlsPublicKey> = consensus_config
            .committee()
            .authorities()
            .into_iter()
            .map(|a| *a.protocol_key())
            .collect();

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
                    .map(|(k, v)| (*k, v.primary.clone()))
                    .collect(),
            )
            .await?;

        network_handle.inner_handle().new_epoch(committee_keys.clone()).await?;
        debug!(target: "epoch-manager", auth=?consensus_config.authority_id(), "event stream updated!");

        // start listening if the network needs to be initialized
        if initial_epoch {
            // start listening for p2p messages
            let primary_address = Self::parse_listener_address_for_swarm(
                "PRIMARY_LISTENER_MULTIADDR",
                consensus_config.primary_networkkey(),
                consensus_config.primary_address(),
            )?;
            // Explicit relay reservations (comma-separated relay base multiaddrs): an alternate
            // leg of the advertised relay (e.g. a dual-homed DMZ relay's inside address) or
            // additional relays for failover.
            let relay_reservations = Self::relay_listen_addresses(
                "PRIMARY_RELAY_MULTIADDRS",
                consensus_config.primary_networkkey(),
            )?;
            super::network::start_swarm_listeners(
                network_handle.inner_handle(),
                primary_address,
                relay_reservations,
                "PRIMARY_RELAY_MULTIADDRS",
            )
            .await?;
        }

        let mut peers = network_handle.connected_peers_count().await.unwrap_or(0);
        if peers == 0 || self.consensus_bus.node_mode().borrow().is_cvv() {
            // always dial peers for the new epoch
            // do this if a CVV (may need to connect to the other CVVs) or if we don't have any
            // peers if we are not a committee member and have peers then do not pester
            // the committee
            for (_authority_id, bls_pubkey) in consensus_config
                .committee()
                .others_primaries_by_id(consensus_config.authority_id().as_ref())
            {
                Self::dial_peer_bls(
                    network_handle.inner_handle().clone(),
                    bls_pubkey,
                    epoch_task_spawner.clone(),
                );
            }
        }

        // update the authorized publishers for gossip every epoch
        let committee_keys = committee_keys.into_iter().collect::<HashSet<_>>();
        let topic = LibP2pConfig::primary_topic();
        info!(target: "epoch-manager", topic=?topic, committee=?committee_keys, "updating authorized publishers for gossip");

        let result = network_handle
            .inner_handle()
            .subscribe_with_publishers(topic.clone(), committee_keys.into_iter().collect())
            .await?;
        info!(target: "epoch-manager", topic=?topic, result=?result, "subscribed to gossip");

        // quorum for active CVVs, any 1 peer for observers
        let is_active_cvv = self.consensus_bus.node_mode().borrow().is_active_cvv();
        let committee_size = consensus_config.committee().size() as u64;
        let min_peers = if is_active_cvv {
            let quorum = quorum_threshold(committee_size) as usize;
            quorum.saturating_sub(1) // exclude ourselves
        } else {
            1
        };

        let mut retries = 0;
        while peers < min_peers {
            retries += 1;
            if retries > 240 {
                if is_active_cvv {
                    return Err(eyre::eyre!(
                        "Unable to join rayls network, can not connect to enough peers! \
                         Connected: {}, required: {} (quorum for committee size {})",
                        peers,
                        min_peers,
                        committee_size
                    ));
                }
                warn!(target: "epoch-manager",
                    peers,
                    "no peers connected after 2 minutes, proceeding as observer — \
                     gossipsub will connect peers in the background"
                );
                break;
            }
            if retries % 10 == 0 {
                warn!(target: "epoch-manager",
                    peers = peers,
                    required = min_peers,
                    committee_size = committee_size,
                    "waiting for peers before starting consensus"
                );
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
            peers = network_handle.connected_peers_count().await.unwrap_or(0);
            debug!(target: "epoch-manager", "Waiting for peers: {}/{} connected", peers, min_peers);
        }

        // log gossipsub mesh state after quorum reached
        let consensus_output_topic = LibP2pConfig::consensus_output_topic();
        let primary_topic = LibP2pConfig::primary_topic();
        let epoch_vote_topic = LibP2pConfig::epoch_vote_topic();

        let mesh_consensus = network_handle
            .inner_handle()
            .mesh_peers(consensus_output_topic.clone())
            .await
            .unwrap_or_default();
        let mesh_primary = network_handle
            .inner_handle()
            .mesh_peers(primary_topic.clone())
            .await
            .unwrap_or_default();
        let mesh_epoch_vote =
            network_handle.inner_handle().mesh_peers(epoch_vote_topic).await.unwrap_or_default();
        let all_peers = network_handle.inner_handle().all_peers().await.unwrap_or_default();

        info!(target: "epoch-manager::gossipsub",
            peers = peers,
            required = min_peers,
            committee_size = committee_size,
            mesh_consensus_output = mesh_consensus.len(),
            ?mesh_consensus,
            mesh_primary = mesh_primary.len(),
            ?mesh_primary,
            mesh_epoch_vote = mesh_epoch_vote.len(),
            ?mesh_epoch_vote,
            all_peers_count = all_peers.len(),
            "quorum peers connected - gossipsub mesh state before starting consensus"
        );

        // log each peer's subscribed topics for debugging
        for (peer_id, topics) in &all_peers {
            let topic_names: Vec<_> = topics.iter().map(|t| t.to_string()).collect();
            debug!(target: "epoch-manager::gossipsub",
                ?peer_id,
                ?topic_names,
                "peer topic subscriptions"
            );
        }

        // spawn primary network
        PrimaryNetwork::new(
            rx_event_stream,
            network_handle.clone(),
            consensus_config.clone(),
            self.consensus_bus.clone(),
            rayls_consensus_state_sync,
            epoch_task_spawner.clone(), // tasks should abort with epoch
        )
        .spawn(&epoch_task_spawner);

        Ok(())
    }
}
