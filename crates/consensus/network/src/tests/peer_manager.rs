//! Unit tests for peer manager

use super::*;
use crate::common::{create_multiaddr, random_ip_addr};
use assert_matches::assert_matches;
use libp2p::swarm::{ConnectionId, NetworkBehaviour as _};
use rand::{rngs::StdRng, SeedableRng as _};
use rayls_infrastructure_config::{KeyConfig, NetworkConfig, ScoreConfig};
use rayls_infrastructure_storage::mem_db::MemDatabase;
use rayls_infrastructure_types::{now, BlsKeypair, NetworkKeypair, NetworkPublicKey};
use rayls_testing_test_utils::CommitteeFixture;
use std::{
    collections::{HashMap, HashSet},
    net::{Ipv4Addr, Ipv6Addr},
    time::Duration,
};
use tokio::time::{sleep, timeout};

fn create_test_peer_manager(network_config: Option<NetworkConfig>) -> PeerManager {
    let network_config = network_config.unwrap_or_default();
    let all_nodes =
        CommitteeFixture::builder(MemDatabase::default).with_network_config(network_config).build();
    let mut authorities = all_nodes.authorities();
    let authority_1 = authorities.next().expect("first authority");
    let config = authority_1.consensus_config();
    PeerManager::new(config.network_config().peer_config())
}

/// Helper function to extract events of a certain type
fn extract_events<'a>(
    events: &'a [PeerEvent],
    event_type: fn(&'a PeerEvent) -> bool,
) -> Vec<&'a PeerEvent> {
    events.iter().filter(|e| event_type(e)).collect()
}

/// Helper to get all events from the peer manager
fn collect_all_events(peer_manager: &mut PeerManager) -> Vec<PeerEvent> {
    let mut events = Vec::new();
    while let Some(event) = peer_manager.poll_events() {
        events.push(event);
    }
    events
}

/// Register a peer connection and return its PeerId
fn register_peer(peer_manager: &mut PeerManager, multiaddr: Option<Multiaddr>) -> PeerId {
    let peer_id = PeerId::random();
    let multiaddr = multiaddr.unwrap_or_else(|| create_multiaddr(None));
    let connection = ConnectionType::IncomingConnection { multiaddr };
    assert!(peer_manager.register_peer_connection(&peer_id, connection));
    peer_id
}

#[tokio::test]
async fn test_register_disconnected_basic() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = PeerId::random();
    let multiaddr = create_multiaddr(None);

    // register connection
    let connection = ConnectionType::IncomingConnection { multiaddr };
    assert!(peer_manager.register_peer_connection(&peer_id, connection));

    // register disconnection
    peer_manager.register_disconnected(&peer_id);

    // assert peer is no longer connected
    assert!(!peer_manager.is_connected(&peer_id));

    // assert no events from disconnect without ban
    assert!(peer_manager.poll_events().is_none());
}

#[tokio::test]
async fn test_register_disconnected_with_banned_peer() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = PeerId::random();
    let multiaddr = create_multiaddr(None);

    // register connection
    let connection = ConnectionType::IncomingConnection { multiaddr };
    assert!(peer_manager.register_peer_connection(&peer_id, connection));

    // Apply a severe penalty to trigger ban
    peer_manager.process_penalty(peer_id, Penalty::Fatal);

    // clear events from reported penalty
    let mut disconnect_events = Vec::new();
    while let Some(event) = peer_manager.poll_events() {
        disconnect_events.push(event);
    }

    // assert peer is set for disconnect
    let disconnect_event =
        extract_events(&disconnect_events, |e| matches!(e, PeerEvent::DisconnectPeer(_))).len();
    assert!(disconnect_event == 1, "Expect one disconnect event");
    assert_matches!(
        disconnect_events.first().unwrap(),
        PeerEvent::DisconnectPeer(id) if *id == peer_id
    );

    // register disconnection
    peer_manager.register_disconnected(&peer_id);

    // There should be no additional ban events since the peer is already banned
    let mut banned_events = Vec::new();
    while let Some(event) = peer_manager.poll_events() {
        banned_events.push(event);
    }

    let banned_event = extract_events(&banned_events, |e| matches!(e, PeerEvent::Banned(_))).len();
    assert!(banned_event == 1, "Expect one banned event");
    assert_matches!(
        banned_events.first().unwrap(),
        PeerEvent::Banned(id) if *id == peer_id
    );

    // assert peer is still banned after disconnection
    assert!(peer_manager.peer_banned(&peer_id), "Peer should remain banned after disconnection");
}

#[tokio::test]
async fn test_add_trusted_peer() {
    let config = ScoreConfig::default();
    let mut peer_manager = create_test_peer_manager(None);
    let keys = KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::from_os_rng()));
    let peer_bls = keys.primary_public_key();
    let peer_netkey = keys.primary_network_public_key();
    let peer_id: PeerId = peer_netkey.clone().into();
    let multiaddr = create_multiaddr(None);

    // Create a oneshot channel to simulate the reply channel
    let (sender, _receiver) = oneshot::channel();

    // Add trusted peer
    peer_manager.add_trusted_peer_and_dial(
        peer_bls,
        NetworkInfo { pubkey: peer_netkey, multiaddrs: vec![multiaddr.clone()], timestamp: now() },
        sender,
    );

    let score = peer_manager.peer_score(&peer_id).unwrap();
    assert_eq!(score, config.max_score);

    // Verify a dial request was created
    let dial_request = peer_manager.next_dial_request().unwrap();
    assert_eq!(dial_request.peer_id, peer_id);
    assert_eq!(dial_request.multiaddrs, vec![multiaddr]);

    // assert penalty doesn't affect trusted peer
    peer_manager.process_penalty(peer_id, Penalty::Fatal);
    assert!(!peer_manager.peer_banned(&peer_id));
    let score = peer_manager.peer_score(&peer_id).unwrap();
    assert_eq!(score, config.max_score);
}

#[tokio::test]
async fn test_dial_peer_success() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = PeerId::random();
    let multiaddr = create_multiaddr(None);

    // Create a oneshot channel to simulate the reply channel
    let (sender, receiver) = oneshot::channel();

    // Dial peer
    peer_manager.dial_peer(peer_id, vec![multiaddr.clone()], Some(sender));

    // Verify a dial request was created
    let dial_request = peer_manager.next_dial_request();
    assert!(dial_request.is_some());
    let request = dial_request.unwrap();
    assert_eq!(request.peer_id, peer_id);
    assert_eq!(request.multiaddrs, vec![multiaddr.clone()]);

    // Register the dial attempt
    peer_manager.register_dial_attempt(peer_id, request.reply);

    // update connection status to trigger dial result
    assert!(peer_manager
        .register_peer_connection(&peer_id, ConnectionType::IncomingConnection { multiaddr }));

    let result = timeout(Duration::from_millis(500), receiver).await.unwrap().unwrap();
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_dial_peer_already_dialing_error() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = PeerId::random();
    let multiaddr = create_multiaddr(None);

    // Create a oneshot channel to simulate the reply channel
    let (sender, _receiver) = oneshot::channel();

    // Dial peer for the first time
    peer_manager.dial_peer(peer_id, vec![multiaddr.clone()], Some(sender));

    // Verify a dial request was created
    let dial_request = peer_manager.next_dial_request().unwrap();

    // Register the dial attempt
    peer_manager.register_dial_attempt(peer_id, dial_request.reply);

    // Create another oneshot channel
    let (sender2, receiver2) = oneshot::channel();

    // Try to dial the same peer again
    peer_manager.dial_peer(peer_id, vec![multiaddr.clone()], Some(sender2));

    // Verify no new dial request was created (since we already dialing)
    assert!(peer_manager.next_dial_request().is_none());

    // Verify the error from the oneshot channel
    let result = timeout(Duration::from_millis(500), receiver2).await.unwrap().unwrap();
    assert!(result.is_err());
}

#[tokio::test]
async fn test_dial_peer_already_connected() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = PeerId::random();
    let multiaddr = create_multiaddr(None);

    // Register a connected peer
    let connection = ConnectionType::IncomingConnection { multiaddr: multiaddr.clone() };
    assert!(peer_manager.register_peer_connection(&peer_id, connection));
    assert!(peer_manager.is_connected(&peer_id));

    // Create a oneshot channel
    let (sender, receiver) = oneshot::channel();

    // Try to dial the already connected peer
    peer_manager.dial_peer(peer_id, vec![multiaddr.clone()], Some(sender));

    // Verify no dial request was created
    assert!(peer_manager.next_dial_request().is_none());

    // Verify the error from the oneshot channel
    let result = timeout(Duration::from_millis(500), receiver).await;
    let channel_result = result.unwrap().unwrap();
    assert!(channel_result.is_err()); // Dial should have failed with an error
}

#[tokio::test]
async fn test_process_penalty_mild() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = register_peer(&mut peer_manager, None);

    // Apply multiple mild penalties
    for _ in 0..5 {
        peer_manager.process_penalty(peer_id, Penalty::Mild);
    }

    let config = ScoreConfig::default();
    let score_after_penalty = peer_manager.peer_score(&peer_id).unwrap();
    assert!(score_after_penalty < config.default_score);

    let events = collect_all_events(&mut peer_manager);
    assert!(events.is_empty());

    // Verify peer is not banned after mild penalties
    assert!(!peer_manager.peer_banned(&peer_id));
}

#[tokio::test]
async fn test_process_penalty_medium() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = register_peer(&mut peer_manager, None);

    // Apply medium penalties
    for _ in 0..3 {
        peer_manager.process_penalty(peer_id, Penalty::Medium);
    }

    // Apply one more medium penalty which should trigger disconnection
    peer_manager.process_penalty(peer_id, Penalty::Medium);

    // Get events
    let events = collect_all_events(&mut peer_manager);

    // There should be a disconnect event
    let disconnect_events = extract_events(&events, |e| matches!(e, PeerEvent::DisconnectPeer(_)));
    assert!(
        matches!(
            disconnect_events.first().unwrap(), PeerEvent::DisconnectPeer(id) if *id == peer_id
        ),
        "Expected disconnect event after medium penalties"
    );

    let config = ScoreConfig::default();
    let score_after_penalty = peer_manager.peer_score(&peer_id).unwrap();
    assert!(score_after_penalty <= config.min_score_before_disconnect);
}

#[tokio::test]
async fn test_process_penalty_severe() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = register_peer(&mut peer_manager, None);

    // Apply severe penalties
    peer_manager.process_penalty(peer_id, Penalty::Severe);

    // Clear events
    collect_all_events(&mut peer_manager);

    // Apply one more severe penalty which should trigger disconnection
    peer_manager.process_penalty(peer_id, Penalty::Severe);

    // Get events
    let events = collect_all_events(&mut peer_manager);

    // There should be a disconnect event
    let disconnect_events = extract_events(&events, |e| matches!(e, PeerEvent::DisconnectPeer(_)));
    assert!(
        matches!(
            disconnect_events.first().unwrap(), PeerEvent::DisconnectPeer(id) if *id == peer_id
        ),
        "Expected disconnect event after severe penalties"
    );

    let config = ScoreConfig::default();
    let score_after_penalty = peer_manager.peer_score(&peer_id).unwrap();
    assert!(score_after_penalty <= config.min_score_before_disconnect);
}

#[tokio::test]
async fn test_process_penalty_fatal() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = register_peer(&mut peer_manager, None);

    // Apply a fatal penalty
    peer_manager.process_penalty(peer_id, Penalty::Fatal);

    // Get events
    let events = collect_all_events(&mut peer_manager);

    // There should be a disconnect event
    let disconnect_events = extract_events(&events, |e| matches!(e, PeerEvent::DisconnectPeer(_)));
    assert!(
        matches!(
            disconnect_events.first().unwrap(), PeerEvent::DisconnectPeer(id) if *id == peer_id
        ),
        "Expected disconnect event after fatal penalty"
    );

    let config = ScoreConfig::default();
    let score_after_penalty = peer_manager.peer_score(&peer_id).unwrap();
    assert!(score_after_penalty <= config.min_score_before_disconnect);

    // Register disconnection
    peer_manager.register_disconnected(&peer_id);

    // Get events
    let events = collect_all_events(&mut peer_manager);

    // There should be a banned event
    let banned_events = extract_events(&events, |e| matches!(e, PeerEvent::Banned(_)));
    assert!(
        matches!(
            banned_events.first().unwrap(), PeerEvent::Banned(id) if *id == peer_id
        ),
        "Expected banned event after fatal penalty"
    );

    // Verify peer is banned
    assert!(peer_manager.peer_banned(&peer_id));
    let banned_score = peer_manager.peer_score(&peer_id).unwrap();
    assert!(banned_score <= config.min_score_before_ban);
}

#[tokio::test]
async fn test_heartbeat_maintenance() {
    // custom network config with short heartbeat interval for peer manager
    let mut network_config = NetworkConfig::default();
    network_config.peer_config_mut().score_config.score_halflife = 0.5;
    let default_score = network_config.peer_config().score_config.default_score;

    let mut peer_manager = create_test_peer_manager(Some(network_config));
    let peer_id = register_peer(&mut peer_manager, None);

    // Apply a mild penalty
    peer_manager.process_penalty(peer_id, Penalty::Mild);
    let score_after_penalty = peer_manager.peer_score(&peer_id).unwrap();
    assert!(score_after_penalty < default_score);

    // Clear events
    collect_all_events(&mut peer_manager);

    // halflife set to 0.5
    sleep(Duration::from_secs(1)).await;

    // trigger heartbeat for update
    peer_manager.heartbeat();

    // Verify the peer score increases after heartbeats (penalties decay)
    let score_after_heartbeat = peer_manager.peer_score(&peer_id).unwrap();
    assert!(score_after_heartbeat > score_after_penalty, "Score should increase after heartbeats");
}

#[tokio::test]
async fn test_temporarily_banned_peer() {
    let mut network_config = NetworkConfig::default();
    // make the temp ban very short
    let temp_ban_duration = Duration::from_millis(10);
    network_config.peer_config_mut().excess_peers_reconnection_timeout = temp_ban_duration;

    let mut peer_manager = create_test_peer_manager(Some(network_config));
    let peer_id = register_peer(&mut peer_manager, None);

    // Disconnect the peer with PX (this should temp ban the peer)
    peer_manager.disconnect_peer(peer_id, true);

    // Get events
    let events = collect_all_events(&mut peer_manager);

    // Verify there's a disconnect with PX event
    let disconnect_px_events =
        extract_events(&events, |e| matches!(e, PeerEvent::DisconnectPeerX(_, _)));
    assert!(
        matches!(
            disconnect_px_events.first().unwrap(), PeerEvent::DisconnectPeerX(id, _) if *id == peer_id
        ),
        "Expected disconnect event with peer exchange"
    );

    // Verify peer is temporarily banned
    assert!(peer_manager.peer_banned(&peer_id), "Peer should be temporarily banned");

    // sleep for temp ban duration
    let _ = sleep(temp_ban_duration * 2).await;

    // Run heartbeat to clear temporary bans
    peer_manager.heartbeat();

    // Get events
    let events = collect_all_events(&mut peer_manager);

    // Verify there's an unbanned event
    let unbanned_events = extract_events(&events, |e| matches!(e, PeerEvent::Unbanned(_)));
    assert!(
        matches!(
            unbanned_events.first().unwrap(), PeerEvent::Unbanned(id) if *id == peer_id
        ),
        "Expected peer is unbanned"
    );

    // Verify peer is no longer banned
    assert!(!peer_manager.peer_banned(&peer_id), "Peer should not be banned after heartbeat");
}

#[tokio::test]
async fn test_process_peer_exchange() {
    let mut peer_manager = create_test_peer_manager(None);

    // Create peer exchange data
    let multiaddr1 = create_multiaddr(None);
    let multiaddr2 = create_multiaddr(None);

    let mut exchange_map = HashMap::new();
    let multiaddrs1 = HashSet::from([multiaddr1]);
    let multiaddrs2 = HashSet::from([multiaddr2]);
    let mut rng = StdRng::from_seed([0; 32]);
    let bls1 = *BlsKeypair::generate(&mut rng).public();
    let bls2 = *BlsKeypair::generate(&mut rng).public();
    let net1: NetworkPublicKey = NetworkKeypair::generate_ed25519().public().clone().into();
    let net2: NetworkPublicKey = NetworkKeypair::generate_ed25519().public().clone().into();
    let peer_id1 = net1.clone().into();
    let peer_id2 = net2.clone().into();
    exchange_map.insert(bls1, (net1, multiaddrs1));
    exchange_map.insert(bls2, (net2, multiaddrs2));

    let exchange = PeerExchangeMap::from(exchange_map);

    // Process the peer exchange
    peer_manager.process_peer_exchange(exchange);

    // verify peers were added to discovery
    assert!(peer_manager.discovery_peers.contains_key(&peer_id1));
    assert!(peer_manager.discovery_peers.contains_key(&peer_id2));

    // discovery heartbeat to initiate dial requests
    peer_manager.discovery_heartbeat();

    // verify dial requests were created for both peers
    let _ = peer_manager.next_dial_request().expect("peer 1 exchange");
    let _ = peer_manager.next_dial_request().expect("peer 2 exchange");

    // verify no more dial requests
    assert!(peer_manager.next_dial_request().is_none());
}

#[tokio::test]
async fn test_prune_connected_peers() {
    let mut peer_manager = create_test_peer_manager(None);

    // Register many peers
    let max_peers = peer_manager.config.max_peers();
    let mut peer_ids = Vec::new();
    for _ in 0..(max_peers * 2) {
        let peer_id = register_peer(&mut peer_manager, None);
        peer_ids.push(peer_id);
    }

    // Force pruning via heartbeat
    peer_manager.heartbeat();

    // Get events
    let events = collect_all_events(&mut peer_manager);

    // Verify there are disconnect events (from pruning)
    let disconnect_events = extract_events(&events, |e| {
        matches!(e, PeerEvent::DisconnectPeerX(_, _)) || matches!(e, PeerEvent::DisconnectPeer(_))
    });

    assert!(!disconnect_events.is_empty(), "Expected disconnect events from pruning");
}

#[tokio::test]
async fn test_is_peer_connected_or_disconnecting() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = register_peer(&mut peer_manager, None);

    // Verify peer is considered connected
    assert!(peer_manager.is_peer_connected_or_disconnecting(&peer_id));

    // Disconnect the peer (but don't register disconnection yet)
    peer_manager.disconnect_peer(peer_id, false);

    // Peer should still be considered as connected or disconnecting
    assert!(peer_manager.is_peer_connected_or_disconnecting(&peer_id));

    // Register disconnection
    peer_manager.register_disconnected(&peer_id);

    // Peer should no longer be considered connected or disconnecting
    assert!(!peer_manager.is_peer_connected_or_disconnecting(&peer_id));
}

#[tokio::test]
async fn test_is_validator() {
    let all_nodes = CommitteeFixture::builder(MemDatabase::default).build();
    let mut authorities = all_nodes.authorities();
    let authority_1 = authorities.next().expect("first authority");
    let config = authority_1.consensus_config();
    let mut peer_manager = PeerManager::new(config.network_config().peer_config());
    let validator = *authority_1.authority().protocol_key();
    let random_peer_id = PeerId::random();

    let info = NetworkInfo {
        pubkey: config.key_config().primary_network_public_key(),
        multiaddrs: vec![config.primary_address()],
        timestamp: now(),
    };
    peer_manager.add_known_peer(validator, info);

    // update epoch with random multiaddr
    let committee = config.committee_pub_keys();
    peer_manager.new_epoch(committee);

    // Verify random peer is not a validator
    assert!(!peer_manager.is_peer_validator(&random_peer_id));

    // Verify the committee member this node had already resolved IS a validator. Without this the
    // test passes even if `new_epoch` recognizes nobody at all.
    let resolved: PeerId = config.key_config().primary_network_public_key().into();
    assert!(peer_manager.is_peer_validator(&resolved));
}

/// A committee member this node resolves after the epoch boundary must be absolved from that
/// moment, not from the next boundary. Committee membership is decided by stake; whether local
/// discovery has caught up is this node's problem, not the validator's.
///
/// The node genuinely cannot exempt a peer before it resolves the key-to-peer mapping, so the
/// invariant starts at resolution.
#[tokio::test]
async fn test_committee_member_resolved_mid_epoch_is_absolved_immediately() {
    let all_nodes = CommitteeFixture::builder(MemDatabase::default).build();
    let mut authorities = all_nodes.authorities();
    let authority_1 = authorities.next().expect("first authority");
    let authority_2 = authorities.next().expect("second authority");
    let config = authority_1.consensus_config();
    let mut peer_manager = PeerManager::new(config.network_config().peer_config());

    // this node resolved itself but not authority_2, the ordinary state on a fresh start or a
    // restart before discovery has caught up
    peer_manager.add_known_peer(
        *authority_1.authority().protocol_key(),
        NetworkInfo {
            pubkey: config.key_config().primary_network_public_key(),
            multiaddrs: vec![config.primary_address()],
            timestamp: now(),
        },
    );
    peer_manager.new_epoch(config.committee_pub_keys());

    // authority_2 connects before its record arrives, so it is tracked as an ordinary peer
    let config_2 = authority_2.consensus_config();
    let unresolved: PeerId = config_2.key_config().primary_network_public_key().into();
    assert!(peer_manager.register_peer_connection(
        &unresolved,
        ConnectionType::IncomingConnection { multiaddr: create_multiaddr(None) }
    ));

    // discovery completes and the record resolves mid-epoch
    peer_manager.add_known_peer(
        *authority_2.authority().protocol_key(),
        NetworkInfo {
            pubkey: config_2.key_config().primary_network_public_key(),
            multiaddrs: vec![config_2.primary_address()],
            timestamp: now(),
        },
    );

    // INVARIANT: resolution grants the exemption, so penalties from this point are absolved.
    // CURRENT: `new_epoch` rebuilds `current_committee_keys` only from members it could already
    // resolve, so `upsert_peer` sees an unknown key and leaves the peer untrusted for the epoch.
    assert!(
        peer_manager.is_peer_validator(&unresolved),
        "a committee member resolved mid-epoch was not recognized as a validator"
    );
    peer_manager.process_penalty(unresolved, Penalty::Fatal);
    assert!(!peer_manager.peer_banned(&unresolved), "a committee member was banned by a penalty");
}

/// A ban charged against a committee member before its record resolved is released when discovery
/// trusts it. A lingering charge can evict an innocent peer under the cap or push a shared IP over
/// the block threshold.
#[tokio::test]
async fn resolving_a_banned_committee_member_releases_its_ban() {
    let all_nodes = CommitteeFixture::builder(MemDatabase::default).build();
    let mut authorities = all_nodes.authorities();
    let authority_1 = authorities.next().expect("first authority");
    let authority_2 = authorities.next().expect("second authority");
    let config = authority_1.consensus_config();
    let mut peer_manager = PeerManager::new(config.network_config().peer_config());

    // this node resolves itself, then installs a committee whose second member it cannot yet
    // resolve
    peer_manager.add_known_peer(
        *authority_1.authority().protocol_key(),
        NetworkInfo {
            pubkey: config.key_config().primary_network_public_key(),
            multiaddrs: vec![config.primary_address()],
            timestamp: now(),
        },
    );
    peer_manager.new_epoch(config.committee_pub_keys());

    // authority_2 connects and is banned while still an ordinary, unresolved peer
    let config_2 = authority_2.consensus_config();
    let unresolved: PeerId = config_2.key_config().primary_network_public_key().into();
    assert!(peer_manager.register_peer_connection(
        &unresolved,
        ConnectionType::IncomingConnection { multiaddr: create_multiaddr(None) }
    ));
    peer_manager.process_penalty(unresolved, Penalty::Fatal);
    peer_manager.register_disconnected(&unresolved);
    assert!(peer_manager.peer_banned(&unresolved), "precondition: banned before resolution");
    let _ = collect_all_events(&mut peer_manager);

    // discovery resolves the record mid-epoch and trusts the member
    peer_manager.add_known_peer(
        *authority_2.authority().protocol_key(),
        NetworkInfo {
            pubkey: config_2.key_config().primary_network_public_key(),
            multiaddrs: vec![config_2.primary_address()],
            timestamp: now(),
        },
    );

    // INVARIANT: resolution releases the ban, surfacing an unban that repairs the gossipsub
    // blacklist and the kad routing entry.
    // PRE-FIX: `upsert_peer` only trusts the resolved member; the `Banned` status and IP charge
    // survive discovery.
    let events = collect_all_events(&mut peer_manager);
    assert!(
        events.iter().any(|e| matches!(e, PeerEvent::Unbanned(p) if *p == unresolved)),
        "resolving a banned committee member must release its ban"
    );
}

/// The admission check and the registration check must use the same ban predicate. If the swarm
/// admits a connection that `register_peer_connection` then refuses, the node holds an open
/// connection it does not track.
///
/// This is the path that emits `error ... "connected with banned peer"` in production.
#[tokio::test]
async fn test_admitted_connection_is_registered_under_the_same_ban_predicate() {
    let all_nodes = CommitteeFixture::builder(MemDatabase::default).build();
    let mut authorities = all_nodes.authorities();
    let authority_1 = authorities.next().expect("first authority");
    let config = authority_1.consensus_config();
    let mut peer_manager = PeerManager::new(config.network_config().peer_config());

    let blocklisted = create_multiaddr(Some(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 200))));
    let clean = create_multiaddr(Some(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 201))));

    // the validator is connected, so its record exists and `upsert_peer` will take the
    // `update_net` branch rather than filing addresses under `listening_addrs`
    let validator: PeerId = config.key_config().primary_network_public_key().into();
    assert!(peer_manager.register_peer_connection(
        &validator,
        ConnectionType::IncomingConnection { multiaddr: clean.clone() }
    ));

    // the validator advertises a second address in its record, and joins the committee
    peer_manager.add_known_peer(
        *authority_1.authority().protocol_key(),
        NetworkInfo {
            pubkey: config.key_config().primary_network_public_key(),
            multiaddrs: vec![blocklisted.clone()],
            timestamp: now(),
        },
    );
    peer_manager.new_epoch(config.committee_pub_keys());
    assert!(peer_manager.is_peer_validator(&validator), "precondition: validator is in committee");

    // two unrelated peers are banned at the advertised address, blocklisting it
    for _ in 0..2 {
        let other = PeerId::random();
        assert!(peer_manager.register_peer_connection(
            &other,
            ConnectionType::IncomingConnection { multiaddr: blocklisted.clone() }
        ));
        peer_manager.process_penalty(other, Penalty::Fatal);
        peer_manager.register_disconnected(&other);
    }

    // the validator reconnects from its clean address. `sanitize_ip_addr` passes (that address is
    // not blocklisted) and `PeerManager::peer_banned` exempts it as a committee member, so the
    // swarm admits the connection.
    assert!(
        !peer_manager.peer_banned(&validator),
        "precondition: the swarm admits this connection"
    );

    // INVARIANT: a connection the admission checks accepted is registered.
    // CURRENT: `register_peer_connection` gates on `AllPeers::peer_banned`, which has no committee
    // exemption, so it refuses and logs `error "connected with banned peer"` - while
    // `on_connection_established` discards the `false` and still emits `PeerEvent::PeerConnected`.
    assert!(
        peer_manager.register_peer_connection(
            &validator,
            ConnectionType::IncomingConnection { multiaddr: clean }
        ),
        "the swarm admitted this connection but the peer manager refused to register it"
    );
}

#[tokio::test]
async fn test_register_outgoing_connection() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = PeerId::random();
    let multiaddr = create_multiaddr(None);

    // Create a oneshot channel
    let (sender, receiver) = oneshot::channel();

    // Register dial attempt
    peer_manager.register_dial_attempt(peer_id, Some(sender));

    // Register outgoing connection
    let connection = ConnectionType::OutgoingConnection { multiaddr: multiaddr.clone() };
    assert!(peer_manager.register_peer_connection(&peer_id, connection));

    // Verify dial success was sent
    let result = timeout(Duration::from_millis(500), receiver).await.unwrap().unwrap();
    assert!(result.is_ok());

    // Verify peer is connected
    assert!(peer_manager.is_connected(&peer_id));
}

#[tokio::test]
async fn test_peer_limit_reached() {
    let mut peer_manager = create_test_peer_manager(None);

    // Create many connected peers to reach the limit
    let mut peer_ids = Vec::new();
    for _ in 0..50 {
        let peer_id = register_peer(&mut peer_manager, None);
        peer_ids.push(peer_id);
    }

    // Create endpoint for inbound connection
    let multiaddr = create_multiaddr(None);
    let endpoint = ConnectedPoint::Listener {
        local_addr: multiaddr.clone(),
        send_back_addr: multiaddr.clone(),
    };

    // Check if peer limit is reached
    assert!(peer_manager.peer_limit_reached(&endpoint), "Peer limit should be reached");
}

#[tokio::test]
async fn test_peers_for_exchange() {
    let mut peer_manager = create_test_peer_manager(None);

    // Register some peers
    for i in 0..5 {
        let network_key: NetworkPublicKey = NetworkKeypair::generate_ed25519().public().into();
        let peer_id: PeerId = network_key.clone().into();
        let addr = create_multiaddr(None);

        let multiaddr = addr.clone();
        let connection = ConnectionType::IncomingConnection { multiaddr };
        assert!(peer_manager.register_peer_connection(&peer_id, connection));
        let mut rng = StdRng::from_seed([i; 32]);
        let bls = *BlsKeypair::generate(&mut rng).public();
        peer_manager.add_known_peer(
            bls,
            NetworkInfo { pubkey: network_key, multiaddrs: vec![addr], timestamp: now() },
        );
    }

    // Get peers for exchange
    let exchange = peer_manager.peers_for_exchange();

    // Verify we have peers in the exchange
    assert!(!exchange.0.is_empty(), "Should have peers for exchange");

    // Each peer should have their multiaddr in the exchange
    for (_, (_, addrs)) in exchange.into_iter() {
        assert!(!addrs.is_empty(), "Each peer should have at least one multiaddr");
    }
}

#[tokio::test]
async fn test_banned_peer_dial_fails_and_ip_ban() {
    let mut peer_manager = create_test_peer_manager(None);
    let ip = random_ip_addr();
    let multiaddr = create_multiaddr(Some(ip));
    let peer_id = register_peer(&mut peer_manager, Some(multiaddr.clone()));

    // Initially IP is not banned
    assert!(!peer_manager.is_ip_banned(&ip));

    // Apply fatal penalty
    peer_manager.process_penalty(peer_id, Penalty::Fatal);

    // Clear events
    collect_all_events(&mut peer_manager);

    // Register disconnection to finalize the first ban for ip
    peer_manager.register_disconnected(&peer_id);
    assert!(!peer_manager.is_ip_banned(&ip));

    // Clear events
    let events = collect_all_events(&mut peer_manager);

    // there should be a banned event
    let banned_events = extract_events(&events, |e| matches!(e, PeerEvent::Banned(_)));
    assert!(
        matches!(
            banned_events.first().unwrap(), PeerEvent::Banned(id) if *id == peer_id
        ),
        "Expected banned event after fatal penalty"
    );

    // assert behavior stops connection
    let connection_id = ConnectionId::new_unchecked(0);
    let local = create_multiaddr(None);

    // handle banned peer id trying to reconnect
    let reconnect_attempt = peer_manager.handle_established_inbound_connection(
        connection_id,
        peer_id,
        &local,
        &multiaddr,
    );

    // assert inbound connection fails
    assert!(reconnect_attempt.is_err());

    // register different malicious peer id from same ip
    let peer_id = PeerId::random();
    let connection = ConnectionType::IncomingConnection { multiaddr: multiaddr.clone() };
    assert!(peer_manager.register_peer_connection(&peer_id, connection));

    // apply fatal penalty
    peer_manager.process_penalty(peer_id, Penalty::Fatal);

    // Register disconnection to finalize the first ban for ip
    peer_manager.register_disconnected(&peer_id);

    // verify IP is now banned after second peer banned from ip
    assert!(peer_manager.is_ip_banned(&ip));

    // assert pending dial attempt fails
    let dial_attempt =
        peer_manager.handle_pending_inbound_connection(connection_id, &local, &multiaddr);
    assert!(dial_attempt.is_err());
}

#[test]
fn test_extract_ip_from_multiaddr() {
    // test ipv4
    let ipv4 = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1));
    let addr = Multiaddr::empty().with(ipv4.into()).with(Protocol::Tcp(8080));
    assert_eq!(PeerManager::extract_ip_from_multiaddr(&addr), Some(ipv4));

    // test ipv6
    let ipv6 = IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1));
    let addr = Multiaddr::empty().with(ipv6.into()).with(Protocol::Tcp(8080));
    assert_eq!(PeerManager::extract_ip_from_multiaddr(&addr), Some(ipv6));

    // test no ip (e.g., only tcp)
    let addr = Multiaddr::empty().with(Protocol::Tcp(8080));
    assert_eq!(PeerManager::extract_ip_from_multiaddr(&addr), None);

    // test unsupported protocol
    let addr = Multiaddr::empty().with(Protocol::Dns("example.com".into()));
    assert_eq!(PeerManager::extract_ip_from_multiaddr(&addr), None);
}

#[tokio::test]
async fn test_has_valid_unbanned_ips_with_valid_ips() {
    let peer_manager = create_test_peer_manager(None);
    let addr = create_multiaddr(None);
    assert!(peer_manager.has_valid_unbanned_ips(&[addr]));
}

#[tokio::test]
async fn test_has_valid_unbanned_ips_with_no_valid_ips() {
    let peer_manager = create_test_peer_manager(None);
    let addr = Multiaddr::empty().with(Protocol::Tcp(8080)); // No IP

    assert!(!peer_manager.has_valid_unbanned_ips(&[addr]));
}

#[tokio::test]
async fn test_has_valid_unbanned_ips_with_banned_ip() {
    let mut peer_manager = create_test_peer_manager(None);
    let addr = create_multiaddr(None);
    let peer_id = PeerId::random();

    // connect and ban the peer to ban the ip
    let connection = ConnectionType::IncomingConnection { multiaddr: addr.clone() };
    peer_manager.register_peer_connection(&peer_id, connection);
    peer_manager.process_penalty(peer_id, Penalty::Fatal);
    peer_manager.register_disconnected(&peer_id);

    // create another peer with same ip (need 2 peers from same ip for ip ban)
    let peer_id2 = PeerId::random();
    let connection2 = ConnectionType::IncomingConnection { multiaddr: addr.clone() };
    peer_manager.register_peer_connection(&peer_id2, connection2);
    peer_manager.process_penalty(peer_id2, Penalty::Fatal);
    peer_manager.register_disconnected(&peer_id2);

    // IP should be banned
    assert!(!peer_manager.has_valid_unbanned_ips(&[addr]));
}

#[tokio::test]
async fn test_has_valid_unbanned_ips_early_return_on_banned() {
    let mut peer_manager = create_test_peer_manager(None);
    let banned_ip = random_ip_addr();
    let valid_ip = random_ip_addr();

    // ban the first ip
    let peer_id = PeerId::random();
    let addr1 = create_multiaddr(Some(banned_ip));
    let connection = ConnectionType::IncomingConnection { multiaddr: addr1.clone() };
    peer_manager.register_peer_connection(&peer_id, connection);
    peer_manager.process_penalty(peer_id, Penalty::Fatal);
    peer_manager.register_disconnected(&peer_id);

    let peer_id2 = PeerId::random();
    let connection2 = ConnectionType::IncomingConnection { multiaddr: addr1.clone() };
    peer_manager.register_peer_connection(&peer_id2, connection2);
    peer_manager.process_penalty(peer_id2, Penalty::Fatal);
    peer_manager.register_disconnected(&peer_id2);

    // create multiaddrs with banned ip first, valid ip second
    let addr2 = create_multiaddr(Some(valid_ip));

    // should return false on first banned ip (early return)
    assert!(!peer_manager.has_valid_unbanned_ips(&[addr1, addr2]));
}

#[tokio::test]
async fn test_process_peers_for_discovery_filters_duplicates() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = PeerId::random();
    let addr = create_multiaddr(None);
    let peer_info = PeerInfo { peer_id, addrs: vec![addr.clone()] };

    // add same peer twice
    peer_manager.process_peers_for_discovery(vec![peer_info.clone()]);
    peer_manager.process_peers_for_discovery(vec![peer_info.clone()]);

    // should only have one entry
    assert_eq!(peer_manager.discovery_peers.len(), 1);
    assert_eq!(peer_manager.discovery_peers.remove(&peer_id), Some(vec![addr]));
}

#[tokio::test]
async fn test_process_peers_for_discovery_filters_no_valid_ip() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = PeerId::random();
    let invalid_addr = Multiaddr::empty().with(Protocol::Tcp(8080)); // No IP
    let peer_info = PeerInfo { peer_id, addrs: vec![invalid_addr] };

    peer_manager.process_peers_for_discovery(vec![peer_info]);

    // should not be added
    assert_eq!(peer_manager.discovery_peers.len(), 0);
}

#[tokio::test]
async fn test_process_peers_for_discovery_filters_banned_ip() {
    let mut peer_manager = create_test_peer_manager(None);
    let addr = create_multiaddr(None);

    // ban the ip first (need 2 peers from same ip)
    let banned_peer1 = PeerId::random();
    let connection1 = ConnectionType::IncomingConnection { multiaddr: addr.clone() };
    peer_manager.register_peer_connection(&banned_peer1, connection1);
    peer_manager.process_penalty(banned_peer1, Penalty::Fatal);
    peer_manager.register_disconnected(&banned_peer1);

    let banned_peer2 = PeerId::random();
    let connection2 = ConnectionType::IncomingConnection { multiaddr: addr.clone() };
    peer_manager.register_peer_connection(&banned_peer2, connection2);
    peer_manager.process_penalty(banned_peer2, Penalty::Fatal);
    peer_manager.register_disconnected(&banned_peer2);

    // try to add a new peer with banned ip
    let new_peer = PeerId::random();
    let peer_info = PeerInfo { peer_id: new_peer, addrs: vec![addr] };

    peer_manager.process_peers_for_discovery(vec![peer_info]);

    // should not be added
    assert_eq!(peer_manager.discovery_peers.len(), 0);
}

#[tokio::test]
async fn test_process_peers_for_discovery_filters_connected_peers() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = PeerId::random();
    let addr = create_multiaddr(None);

    // connect the peer first
    let connection = ConnectionType::IncomingConnection { multiaddr: addr.clone() };
    peer_manager.register_peer_connection(&peer_id, connection);

    // try to add to discovery
    let peer_info = PeerInfo { peer_id, addrs: vec![addr] };

    peer_manager.process_peers_for_discovery(vec![peer_info]);

    // should not be added (can't dial connected peers)
    assert_eq!(peer_manager.discovery_peers.len(), 0);
}

#[tokio::test]
async fn test_process_peers_for_discovery_filters_dialing_peers() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = PeerId::random();
    let addr = create_multiaddr(Some(random_ip_addr()));

    // register dial attempt
    peer_manager.register_dial_attempt(peer_id, None);

    // try to add to discovery
    let peer_info = PeerInfo { peer_id, addrs: vec![addr] };

    peer_manager.process_peers_for_discovery(vec![peer_info]);

    // should not be added (already dialing)
    assert_eq!(peer_manager.discovery_peers.len(), 0);
}

#[tokio::test]
async fn test_process_peers_for_discovery_accepts_valid_peers() {
    let mut peer_manager = create_test_peer_manager(None);

    // Create multiple valid peers
    let peer1 = PeerInfo {
        peer_id: PeerId::random(),
        addrs: vec![create_multiaddr(Some(random_ip_addr()))],
    };
    let peer2 = PeerInfo {
        peer_id: PeerId::random(),
        addrs: vec![create_multiaddr(Some(random_ip_addr()))],
    };
    let peer3 = PeerInfo {
        peer_id: PeerId::random(),
        addrs: vec![create_multiaddr(Some(random_ip_addr()))],
    };

    peer_manager.process_peers_for_discovery(vec![peer1, peer2, peer3]);

    // all should be added
    assert_eq!(peer_manager.discovery_peers.len(), 3);
}

#[tokio::test]
async fn test_process_peers_for_discovery_mixed_valid_invalid() {
    let mut peer_manager = create_test_peer_manager(None);

    let valid_peer = PeerInfo {
        peer_id: PeerId::random(),
        addrs: vec![create_multiaddr(Some(random_ip_addr()))],
    };

    let invalid_peer_no_ip = PeerInfo {
        peer_id: PeerId::random(),
        addrs: vec![Multiaddr::empty().with(Protocol::Tcp(8080))],
    };

    let connected_peer_id = PeerId::random();
    let addr = create_multiaddr(Some(random_ip_addr()));
    peer_manager.register_peer_connection(
        &connected_peer_id,
        ConnectionType::IncomingConnection { multiaddr: addr.clone() },
    );
    let connected_peer = PeerInfo { peer_id: connected_peer_id, addrs: vec![addr] };

    peer_manager.process_peers_for_discovery(vec![valid_peer, invalid_peer_no_ip, connected_peer]);

    // only the valid peer should be added
    assert_eq!(peer_manager.discovery_peers.len(), 1);
}

#[tokio::test]
async fn test_discovery_heartbeat_filters_ineligible_peers() {
    let mut peer_manager = create_test_peer_manager(None);

    // add a valid peer to discovery
    let valid_peer = PeerId::random();
    let valid_addr = create_multiaddr(None);
    peer_manager.discovery_peers.insert(valid_peer, vec![valid_addr.clone()]);

    // add a peer with no valid ip
    let invalid_peer = PeerId::random();
    let invalid_addr = Multiaddr::empty().with(Protocol::Tcp(8080));
    peer_manager.discovery_peers.insert(invalid_peer, vec![invalid_addr]);

    // add a connected peer (ineligible)
    let connected_peer = PeerId::random();
    let connected_addr = create_multiaddr(None);
    peer_manager.register_peer_connection(
        &connected_peer,
        ConnectionType::IncomingConnection { multiaddr: connected_addr.clone() },
    );
    peer_manager.discovery_peers.insert(connected_peer, vec![connected_addr]);

    // run heartbeat
    peer_manager.discovery_heartbeat();

    // assert valid peer is dialing
    let valid_peer_dial_request =
        peer_manager.dial_requests.pop_front().expect("valid peer dial request");

    assert_matches!(
        valid_peer_dial_request,
        DialRequest { peer_id, multiaddrs, .. }
        if peer_id == valid_peer && multiaddrs == vec![valid_addr]
    );
    // assert all discoveries are empty
    assert!(!peer_manager.discovery_peers.contains_key(&valid_peer));
    assert!(!peer_manager.discovery_peers.contains_key(&invalid_peer));
    assert!(!peer_manager.discovery_peers.contains_key(&connected_peer));
    // assert discovery event emitted
    let event = peer_manager.events.pop_front().expect("discovery event");
    assert_matches!(event, PeerEvent::Discovery);
}

#[tokio::test]
async fn test_discovery_heartbeat_respects_target_peers() {
    let mut peer_manager = create_test_peer_manager(None);

    // connect enough peers to meet target
    let target = peer_manager.config.target_num_peers;
    for _ in 0..target {
        let peer_id = PeerId::random();
        let addr = create_multiaddr(None);
        peer_manager.register_peer_connection(
            &peer_id,
            ConnectionType::IncomingConnection { multiaddr: addr },
        );
    }

    // add max discovery peers
    let target = peer_manager.config.max_discovery_peers();
    for _ in 0..target {
        let discovery_peer = PeerId::random();
        peer_manager.discovery_peers.insert(discovery_peer, vec![create_multiaddr(None)]);
    }

    let initial_discovery_count = peer_manager.discovery_peers.len();

    // run heartbeat
    peer_manager.discovery_heartbeat();

    // should not dial any peers (target reached)
    assert_eq!(peer_manager.dial_requests.len(), 0);
    // do not emit discovery event
    assert_eq!(peer_manager.events.len(), 0);
    // discovery peers should still be in the pool
    assert_eq!(peer_manager.discovery_peers.len(), initial_discovery_count);
}

#[tokio::test]
async fn test_discovery_heartbeat_prunes_excess_peers() {
    let mut peer_manager = create_test_peer_manager(None);
    let max_discovery = peer_manager.config.max_discovery_peers();

    // Add more than max discovery peers
    let excess_count = max_discovery + 10;
    for _ in 0..excess_count {
        let peer_id = PeerId::random();
        peer_manager.discovery_peers.insert(peer_id, vec![create_multiaddr(None)]);
    }

    // too many before heartbeat
    assert!(peer_manager.discovery_peers.len() > max_discovery);

    // run heartbeat
    peer_manager.discovery_heartbeat();

    // should prune down to max (or slightly less if some were dialed)
    assert!(peer_manager.discovery_peers.len() <= max_discovery);
}

#[tokio::test]
async fn test_discovery_heartbeat_removes_banned_ip_peers() {
    let mut peer_manager = create_test_peer_manager(None);
    let addr = create_multiaddr(None);

    // Ban the IP by banning 2 peers from that IP
    let banned_peer1 = PeerId::random();
    peer_manager.register_peer_connection(
        &banned_peer1,
        ConnectionType::IncomingConnection { multiaddr: addr.clone() },
    );
    peer_manager.process_penalty(banned_peer1, Penalty::Fatal);
    peer_manager.register_disconnected(&banned_peer1);

    let banned_peer2 = PeerId::random();
    peer_manager.register_peer_connection(
        &banned_peer2,
        ConnectionType::IncomingConnection { multiaddr: addr.clone() },
    );
    peer_manager.process_penalty(banned_peer2, Penalty::Fatal);
    peer_manager.register_disconnected(&banned_peer2);

    // add a discovery peer with the banned ip
    let discovery_peer = PeerId::random();
    peer_manager.discovery_peers.insert(discovery_peer, vec![addr]);

    // run heartbeat
    peer_manager.discovery_heartbeat();

    // discovery peer with banned ip should be removed
    assert!(!peer_manager.discovery_peers.contains_key(&discovery_peer));
}

/// A connection this node refuses to track must be closed, not announced upward. Announcing it
/// leaves the application routing consensus traffic to a peer the manager does not manage.
#[tokio::test]
async fn test_refused_registration_disconnects_instead_of_announcing_the_peer() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = register_peer(&mut peer_manager, None);

    peer_manager.process_penalty(peer_id, Penalty::Fatal);
    peer_manager.register_disconnected(&peer_id);
    collect_all_events(&mut peer_manager);
    assert!(peer_manager.peer_banned(&peer_id), "precondition: the peer is banned");

    // the ban landed between the admission hook and this callback, which is the only way a banned
    // peer reaches registration now that both gates share one predicate
    let registered = peer_manager.register_peer_connection(
        &peer_id,
        ConnectionType::IncomingConnection { multiaddr: create_multiaddr(None) },
    );
    assert!(!registered, "precondition: registration refuses a banned peer");

    let events = collect_all_events(&mut peer_manager);
    assert!(
        !events.iter().any(|e| matches!(e, PeerEvent::PeerConnected(id, _) if *id == peer_id)),
        "announced a peer it refused to track"
    );
    assert!(
        events.iter().any(|e| matches!(e, PeerEvent::DisconnectPeer(id) if *id == peer_id)),
        "refused registration left the connection open"
    );
}
