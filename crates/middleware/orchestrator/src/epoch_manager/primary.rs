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

        network_handle
            .inner_handle()
            .new_epoch(committee_keys.clone(), self.registered_validators.clone())
            .await?;
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

        // Update the authorized publishers for gossip every epoch.
        //
        // Consensus output and epoch votes are published only by committee members (the CVV
        // subscriber and the epoch-vote collector). Without a list, `verify_gossip` accepted and
        // relayed any peer's message, and every node the publisher had connected to paid for
        // it: a BLS verify for an output, a parked task until the next epoch close for a vote.
        // Their list is the registered validators (registry, committee and lookahead) rather
        // than this epoch's committee: a syncing node behind a committee rotation still has to
        // accept the network head from the new committee, votes for an epoch are published and
        // retried after its committee has rotated out, and an unlisted publisher's message is
        // neither forwarded nor processed. The list is necessary, not sufficient: `verify_gossip`
        // also needs the author's peer id bound to its BLS key. Configured bindings cover the
        // genesis committee only, so a member that joined later is bound on the nodes it
        // connected to directly; a node behind the rotation that is not among them stops
        // relaying that member's output, where before it forwarded blind what it could not
        // process. One more edge: a member that left the registry while an epoch it voted in
        // is still uncertified has its retried votes ignored once it is out of the departing
        // committee too. That needs a stall longer than an epoch and an exit in the same
        // window, and the vote fetch covers that case, so it is accepted here.
        for (topic, publishers) in
            gossip_publisher_lists(committee_keys, &self.registered_validators)
        {
            info!(target: "epoch-manager", topic=?topic, publishers = publishers.len(), "updating authorized publishers for gossip");
            debug!(target: "epoch-manager", topic=?topic, ?publishers, "authorized publishers");

            let result = network_handle
                .inner_handle()
                .subscribe_with_publishers(topic.clone(), publishers)
                .await?;
            info!(target: "epoch-manager", topic=?topic, result=?result, "subscribed to gossip");
        }

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

/// The gossip topics the primary subscribes to each epoch, each with the set of BLS keys
/// allowed to publish on it. Certificates come from the current committee; consensus output
/// and epoch votes come from any registered validator (see the caller for why the wider set).
///
/// Kept as one function so the wiring is testable: a topic missing from here is subscribed
/// open, and nothing else would notice.
pub(crate) fn gossip_publisher_lists(
    committee_keys: HashSet<BlsPublicKey>,
    registered_validators: &HashSet<BlsPublicKey>,
) -> [(String, HashSet<BlsPublicKey>); 3] {
    [
        (LibP2pConfig::primary_topic(), committee_keys),
        (LibP2pConfig::consensus_output_topic(), registered_validators.clone()),
        (LibP2pConfig::epoch_vote_topic(), registered_validators.clone()),
    ]
}
