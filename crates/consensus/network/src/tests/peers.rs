//! Unit tests for `AllPeers`

use super::*;
use crate::common::{create_multiaddr, random_ip_addr};
use libp2p::PeerId;
use rand::{rngs::StdRng, SeedableRng as _};
use rayls_infrastructure_config::{PeerConfig, ScoreConfig};
use rayls_infrastructure_types::{now, BlsKeypair, NetworkKeypair};
use std::{
    net::IpAddr,
    time::{Duration, Instant},
};

/// Helper function to create a test AllPeers instance
fn create_all_peers(peer_config: Option<PeerConfig>) -> AllPeers {
    let config = peer_config.unwrap_or_default();
    let dial_timeout = Duration::from_secs(5);
    AllPeers::new(
        dial_timeout,
        config.max_banned_peers,
        config.max_disconnected_peers,
        config.score_config,
    )
}

#[test]
fn test_add_trusted_peer() {
    let config = ScoreConfig::default();
    let mut all_peers = create_all_peers(None);
    let addr = create_multiaddr(None);

    let mut rng = StdRng::from_seed([0; 32]);
    let bls = *BlsKeypair::generate(&mut rng).public();
    let net: NetworkPublicKey = NetworkKeypair::generate_ed25519().public().into();
    let peer_id: PeerId = net.clone().into();
    all_peers.add_trusted_peer(bls, net, vec![addr.clone()]);

    assert!(all_peers.peers.contains_key(&peer_id));
    let peer = all_peers.peers.get_mut(&peer_id).unwrap();
    assert_eq!(peer.reputation(), Reputation::Trusted);
    assert_eq!(peer.score().aggregate_score(), config.max_score);

    // assert that peer exchange doesn't include address for disconnected
    assert!(!peer.exchange_info().unwrap().1.iter().any(|a| a == &addr));

    // update connection and assert exchange info
    peer.register_outgoing(addr.clone());
    assert!(peer.exchange_info().unwrap().1.iter().any(|a| a == &addr));
}

#[test]
fn test_poc_stale_trust_persists_for_rotated_out_committee_member() {
    let config = ScoreConfig::default();
    let mut all_peers = create_all_peers(None);

    // B: Byzantine node — committee member in epoch N, rotated out in epoch N+1.
    let mut rng_b = StdRng::from_seed([0xBBu8; 32]);
    let bls_b = *BlsKeypair::generate(&mut rng_b).public();
    let net_b: NetworkPublicKey = NetworkKeypair::generate_ed25519().public().into();
    let peer_id_b: PeerId = net_b.clone().into();
    let addr_b = create_multiaddr(None);

    // C: Honest committee member in epoch N+1 (B is NOT included).
    let mut rng_c = StdRng::from_seed([0xCCu8; 32]);
    let bls_c = *BlsKeypair::generate(&mut rng_c).public();
    let net_c: NetworkPublicKey = NetworkKeypair::generate_ed25519().public().into();
    let addr_c = create_multiaddr(None);

    // create epoch N committee with B and C as members
    all_peers.new_epoch(vec![
        (
            bls_c,
            Some(NetworkInfo {
                pubkey: net_c.clone(),
                multiaddrs: vec![addr_c.clone()],
                timestamp: now(),
            }),
        ),
        (
            bls_b,
            Some(NetworkInfo { pubkey: net_b, multiaddrs: vec![addr_b.clone()], timestamp: now() }),
        ),
    ]);

    assert!(all_peers.is_peer_validator(&peer_id_b), "B must be a validator");

    // Rotate to epoch N+1 committee (excluding B).
    all_peers.new_epoch(vec![(
        bls_c,
        Some(NetworkInfo { pubkey: net_c, multiaddrs: vec![addr_c], timestamp: now() }),
    )]);

    assert!(!all_peers.is_peer_validator(&peer_id_b), "B must not be a validator after rotation");
    let b_after_rotation = all_peers.get_peer(&peer_id_b).unwrap();
    assert!(
        !b_after_rotation.is_trusted(),
        "B should lose trusted status after rotation out of committee"
    );
    assert_eq!(
        b_after_rotation.score().aggregate_score(),
        config.default_score,
        "switches to default score after rotation"
    );

    let penalty_action = all_peers.process_penalty(&peer_id_b, Penalty::Fatal);
    assert!(penalty_action.is_ban(), "Fatal penalty against stale-trusted B is a no-op");
    assert_eq!(
        all_peers.get_peer(&peer_id_b).unwrap().score().aggregate_score(),
        config.min_score,
        "B should be at min score after fatal penalty, even if disconnect action is a no-op"
    );
}

#[test]
fn test_process_penalty() {
    let config = ScoreConfig::default();
    let mut all_peers = create_all_peers(None);
    let peer_id = PeerId::random();

    // add connected peer first and set as this node dialed
    let mut peer = Peer::default_for_test();
    peer.set_connection_status(ConnectionStatus::Connected { num_in: 0, num_out: 1 });
    all_peers.peers.insert(peer_id, peer);

    // test penalty that doesn't change reputation
    let action = all_peers.process_penalty(&peer_id, Penalty::Mild);
    assert!(matches!(action, PeerAction::NoAction));

    let mut action = PeerAction::NoAction;
    // test penalty that causes disconnection
    while all_peers.get_peer(&peer_id).map(|p| p.score().aggregate_score()).unwrap()
        > config.min_score_before_disconnect
    {
        action = all_peers.process_penalty(&peer_id, Penalty::Severe);
    }
    assert!(matches!(action, PeerAction::Disconnect));

    // test penalty that causes ban
    let action = all_peers.process_penalty(&peer_id, Penalty::Fatal);
    // assert no action for disconnecting peer
    assert!(matches!(action, PeerAction::NoAction));
    // assert ban applied after disconnect
    let action = all_peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);
    assert!(matches!(action, PeerAction::Ban(_)));
    let score = all_peers.get_peer(&peer_id).map(|p| p.score().aggregate_score());
    assert_eq!(score, Some(config.min_score));

    // test penalty for unknown peer
    let unknown_peer_id = PeerId::random();
    let action = all_peers.process_penalty(&unknown_peer_id, Penalty::Fatal);
    assert!(matches!(action, PeerAction::NoAction));
}

#[test]
fn test_ensure_peer_exists() {
    let mut all_peers = create_all_peers(None);
    let peer_id = PeerId::random();

    // Unknown peer, valid initial state
    let status = all_peers.ensure_peer_exists(&peer_id, &NewConnectionStatus::Dialing);
    assert!(matches!(status, ConnectionStatus::Unknown));
    assert!(all_peers.peers.contains_key(&peer_id));

    // now peer exists with default status
    all_peers.peers.clear();
    let status = all_peers.ensure_peer_exists(&peer_id, &NewConnectionStatus::Banned);
    assert!(matches!(status, ConnectionStatus::Unknown));

    // Check that peer is banned when new status is Banned
    let peer = all_peers.peers.get(&peer_id).unwrap();
    assert_eq!(peer.reputation(), Reputation::Banned);
}

#[test]
fn test_peer_exchange() {
    let mut all_peers = create_all_peers(None);

    // Add some connected peers
    for i in 1..4 {
        let network_key: NetworkPublicKey = NetworkKeypair::generate_ed25519().public().into();
        let peer_id: PeerId = network_key.clone().into();
        let addr = create_multiaddr(None);

        all_peers.update_connection_status(
            &peer_id,
            NewConnectionStatus::Connected {
                multiaddr: addr.clone(),
                direction: ConnectionDirection::Incoming,
            },
        );
        let mut rng = StdRng::from_seed([i; 32]);
        let bls = *BlsKeypair::generate(&mut rng).public();
        all_peers.upsert_peer(bls, network_key, vec![addr]);
    }

    // Add a disconnected peer
    let disc_peer_id = PeerId::random();
    all_peers.update_connection_status(&disc_peer_id, NewConnectionStatus::Disconnected);

    let exchange = all_peers.peer_exchange();
    assert_eq!(exchange.0.len(), 3);
    let mut rng = StdRng::from_seed([0; 32]);
    let bls = *BlsKeypair::generate(&mut rng).public();
    assert!(!exchange.0.contains_key(&bls));
}

#[test]
fn test_connected_peers_by_score() {
    let mut all_peers = create_all_peers(None);

    // Add some connected peers
    let mut first_peer_id = PeerId::random();
    for i in 1..4 {
        let new_peer_id = PeerId::random();
        let addr = create_multiaddr(None);

        all_peers.update_connection_status(
            &new_peer_id,
            NewConnectionStatus::Connected {
                multiaddr: addr,
                direction: ConnectionDirection::Incoming,
            },
        );
        // track first peer id
        if i == 1 {
            first_peer_id = new_peer_id;
        }
    }

    // update last peer id as routable
    all_peers.update_routing_for_peer(&first_peer_id, true);

    let peers_by_score = all_peers.connected_peers_by_score_and_routability();
    assert_eq!(peers_by_score.len(), 3);

    // Ensure they're sorted by score
    for i in 1..peers_by_score.len() {
        assert!(peers_by_score[i - 1].1.score() <= peers_by_score[i].1.score());
    }

    // assert routable peer is last
    assert!(peers_by_score.last().map(|peer| peer.1.is_routable()).unwrap())
}

#[test]
fn test_heartbeat_maintenance() {
    let mut all_peers = create_all_peers(None);
    let peer_id = PeerId::random();

    // Add a dialing peer
    all_peers.update_connection_status(&peer_id, NewConnectionStatus::Dialing);

    // Manually set the dialing time to be older than the timeout
    if let Some(peer) = all_peers.peers.get_mut(&peer_id) {
        if peer.connection_status().is_dialing() {
            peer.set_connection_status(ConnectionStatus::Dialing {
                instant: Instant::now() - Duration::from_secs(10),
            }); // Older than the 5s timeout
        }
    }

    // Run heartbeat maintenance
    let _ = all_peers.heartbeat_maintenance();

    // The peer should now be disconnected
    let peer = all_peers.peers.get(&peer_id).unwrap();
    assert!(matches!(peer.connection_status(), ConnectionStatus::Disconnected { .. }));
    assert_eq!(all_peers.disconnected_peers, 1);
}

#[test]
fn test_pruning_logic() {
    let config = PeerConfig::default();
    let mut all_peers = create_all_peers(Some(config));

    // Add many disconnected peers
    let peer_num = 101;
    for i in 1..=peer_num {
        let peer_id = PeerId::random();

        all_peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);

        // Manually set disconnection time to be older for first peers
        if let Some(peer) = all_peers.peers.get_mut(&peer_id) {
            let disconnected =
                matches!(peer.connection_status(), ConnectionStatus::Disconnected { .. });
            if disconnected {
                // set a deterministic order - earlier IDs are older
                peer.set_connection_status(ConnectionStatus::Disconnected {
                    instant: Instant::now() - Duration::from_secs(i as u64),
                });
            }
        }
    }

    assert_eq!(all_peers.disconnected_peers, peer_num);

    // Pruning happens in register_disconnected
    all_peers.register_disconnected(&PeerId::random());

    // Should be pruned to MAX_DISCONNECTED_PEERS (1000)
    assert_eq!(all_peers.disconnected_peers, config.max_disconnected_peers);
    assert_eq!(all_peers.peers.len(), config.max_disconnected_peers);

    // Now test banned peers pruning
    let mut all_peers = create_all_peers(None);

    // Add many banned peers
    for i in 1..=peer_num {
        let peer_id = PeerId::random();

        // Need to go through disconnecting first
        all_peers.update_connection_status(
            &peer_id,
            NewConnectionStatus::Disconnecting { reason: DisconnectReason::Banned },
        );
        all_peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);
        all_peers.update_connection_status(&peer_id, NewConnectionStatus::Banned);

        // Manually set banned time to be older for first peers
        if let Some(peer) = all_peers.peers.get_mut(&peer_id) {
            let banned = matches!(peer.connection_status(), ConnectionStatus::Banned { .. });
            if banned {
                // set a deterministic order - earlier IDs are older
                peer.set_connection_status(ConnectionStatus::Banned {
                    instant: Instant::now() - Duration::from_secs(i as u64),
                });
            }
        }
    }

    assert_eq!(all_peers.banned_peers.total(), peer_num);

    // Trigger pruning
    let (_, pruned) = all_peers.register_disconnected(&PeerId::random());

    // Should be pruned to MAX_BANNED_PEERS (1000)
    assert_eq!(all_peers.banned_peers.total(), config.max_banned_peers);
    let expected = peer_num - config.max_banned_peers;
    assert_eq!(pruned.len(), expected); // 1 peer should be pruned
}

#[test]
fn test_is_validator() {
    let validator_id = PeerId::random();
    let mut all_peers = AllPeers::new(Duration::from_secs(5), 10, 10, ScoreConfig::default());
    all_peers.current_committee.insert(validator_id);
    assert!(all_peers.is_peer_validator(&validator_id));
    assert!(!all_peers.is_peer_validator(&PeerId::random()));
}

#[test]
fn test_ip_and_peer_banned() {
    let mut all_peers = create_all_peers(None);
    let peer_id = PeerId::random();
    let ip = IpAddr::V4("52.3.3.3".parse().unwrap());
    let addr = create_multiaddr(Some(ip));

    // Add a peer and ban it
    all_peers.update_connection_status(
        &peer_id,
        NewConnectionStatus::Connected {
            multiaddr: addr.clone(),
            direction: ConnectionDirection::Incoming,
        },
    );
    all_peers.update_connection_status(
        &peer_id,
        NewConnectionStatus::Disconnecting { reason: DisconnectReason::Banned },
    );
    let action = all_peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);
    assert!(matches!(action, PeerAction::Ban(_)));
    let banned = all_peers.score_or_ip_banned(&peer_id);
    assert!(!banned);

    // check if IP is banned
    assert!(!all_peers.ip_banned(&ip));
    assert!(!all_peers.ip_banned(&random_ip_addr()));

    // new peer connects from same IP
    let new_peer = PeerId::random();
    all_peers.update_connection_status(
        &new_peer,
        NewConnectionStatus::Connected {
            multiaddr: addr,
            direction: ConnectionDirection::Incoming,
        },
    );

    all_peers.update_connection_status(
        &new_peer,
        NewConnectionStatus::Disconnecting { reason: DisconnectReason::Banned },
    );
    let action = all_peers.update_connection_status(&new_peer, NewConnectionStatus::Disconnected);
    assert!(matches!(action, PeerAction::Ban(_)));
    let banned = all_peers.score_or_ip_banned(&new_peer);
    assert!(banned);

    // Check if peer is banned
    assert!(all_peers.score_or_ip_banned(&peer_id));
    assert!(!all_peers.score_or_ip_banned(&PeerId::random()));
}

#[test]
fn test_connected_peer_methods() {
    let mut all_peers = create_all_peers(None);

    // Add some connected peers
    let connected_peer_id = PeerId::random();
    let dialing_peer_id = PeerId::random();
    let disconnected_peer_id = PeerId::random();
    let disconnecting_peer_id = PeerId::random();

    all_peers.update_connection_status(
        &connected_peer_id,
        NewConnectionStatus::Connected {
            multiaddr: create_multiaddr(None),
            direction: ConnectionDirection::Incoming,
        },
    );

    all_peers.update_connection_status(&dialing_peer_id, NewConnectionStatus::Dialing);

    all_peers.update_connection_status(&disconnected_peer_id, NewConnectionStatus::Disconnected);

    all_peers.update_connection_status(
        &disconnecting_peer_id,
        NewConnectionStatus::Disconnecting { reason: DisconnectReason::ExcessPeers },
    );

    // Test connected_peer_ids
    let connected_ids: Vec<_> = all_peers.connected_peer_ids().collect();
    assert_eq!(connected_ids.len(), 1);
    assert!(connected_ids.contains(&&connected_peer_id));

    // Test connected_or_dialing_peers
    let connected_or_dialing: Vec<_> = all_peers.connected_or_dialing_peers();
    assert_eq!(connected_or_dialing.len(), 2);
    assert!(connected_or_dialing.contains(&connected_peer_id));
    assert!(connected_or_dialing.contains(&dialing_peer_id));

    // Test is_peer_connected_or_disconnecting
    assert!(all_peers.is_peer_connected_or_disconnecting(&connected_peer_id));
    assert!(all_peers.is_peer_connected_or_disconnecting(&disconnecting_peer_id));
    assert!(!all_peers.is_peer_connected_or_disconnecting(&dialing_peer_id));
    assert!(!all_peers.is_peer_connected_or_disconnecting(&disconnected_peer_id));
}

#[test]
fn test_unknown_to_connected_transition() {
    let mut all_peers = create_all_peers(None);
    let peer_id = PeerId::random();
    let addr = create_multiaddr(None);

    // Test Unknown -> Connected (incoming)
    let action = all_peers.update_connection_status(
        &peer_id,
        NewConnectionStatus::Connected {
            multiaddr: addr.clone(),
            direction: ConnectionDirection::Incoming,
        },
    );

    assert!(matches!(action, PeerAction::NoAction));
    let peer = all_peers.get_peer(&peer_id).unwrap();
    assert!(matches!(peer.connection_status(), ConnectionStatus::Connected { num_in, .. }
            if *num_in == 1));
}

#[test]
fn test_connected_to_disconnecting_transition() {
    let mut all_peers = create_all_peers(None);
    let peer_id = PeerId::random();
    let addr = create_multiaddr(None);

    // Setup: Unknown -> Connected
    all_peers.update_connection_status(
        &peer_id,
        NewConnectionStatus::Connected {
            multiaddr: addr,
            direction: ConnectionDirection::Incoming,
        },
    );

    // Test Connected -> Disconnecting
    let action = all_peers.update_connection_status(
        &peer_id,
        NewConnectionStatus::Disconnecting { reason: DisconnectReason::ExcessPeers },
    );

    assert!(matches!(action, PeerAction::DisconnectWithPX));
    let peer = all_peers.get_peer(&peer_id).unwrap();
    assert!(matches!(peer.connection_status(), ConnectionStatus::Disconnecting { reason }
            if !reason.bans_peer()));
}

#[test]
fn test_disconnecting_to_disconnected_transition() {
    let mut all_peers = create_all_peers(None);
    let peer_id = PeerId::random();
    let addr = create_multiaddr(None);

    // Setup: Unknown -> Connected -> Disconnecting
    all_peers.update_connection_status(
        &peer_id,
        NewConnectionStatus::Connected {
            multiaddr: addr,
            direction: ConnectionDirection::Incoming,
        },
    );
    let action = all_peers.update_connection_status(
        &peer_id,
        NewConnectionStatus::Disconnecting { reason: DisconnectReason::ExcessPeers },
    );
    assert!(matches!(action, PeerAction::DisconnectWithPX));

    // Test Disconnecting -> Disconnected
    let action = all_peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);

    assert!(matches!(action, PeerAction::NoAction));
    assert_eq!(all_peers.disconnected_peers, 1);
    let peer = all_peers.get_peer(&peer_id).unwrap();
    assert!(matches!(peer.connection_status(), ConnectionStatus::Disconnected { .. }));
}

#[test]
fn test_disconnected_to_dialing_transition() {
    let mut all_peers = create_all_peers(None);
    let peer_id = PeerId::random();

    // set to Disconnected
    all_peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);

    assert_eq!(all_peers.disconnected_peers, 1);

    // Test Disconnected -> Dialing
    let action = all_peers.update_connection_status(&peer_id, NewConnectionStatus::Dialing);

    assert!(matches!(action, PeerAction::NoAction));
    assert_eq!(all_peers.disconnected_peers, 0); // Counter should be decremented
    let peer = all_peers.get_peer(&peer_id).unwrap();
    assert!(matches!(peer.connection_status(), ConnectionStatus::Dialing { .. }));
}

#[test]
fn test_dialing_to_connected_transition() {
    let mut all_peers = create_all_peers(None);
    let peer_id = PeerId::random();
    let addr = create_multiaddr(None);

    // Setup: Unknown -> Dialing
    all_peers.update_connection_status(&peer_id, NewConnectionStatus::Dialing);

    // Test Dialing -> Connected
    let action = all_peers.update_connection_status(
        &peer_id,
        NewConnectionStatus::Connected {
            multiaddr: addr,
            direction: ConnectionDirection::Outgoing,
        },
    );

    assert!(matches!(action, PeerAction::NoAction));
    let peer = all_peers.get_peer(&peer_id).unwrap();
    assert!(matches!(peer.connection_status(), ConnectionStatus::Connected { num_out, .. }
            if *num_out == 1));
}

#[test]
fn test_disconnected_to_banned_transition() {
    let mut all_peers = create_all_peers(None);
    let peer_id = PeerId::random();

    // Setup: Set to Disconnected
    all_peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);

    // Test Disconnected -> Banned
    let action = all_peers.update_connection_status(&peer_id, NewConnectionStatus::Banned);
    assert!(matches!(action, PeerAction::Ban(_)));
    assert_eq!(all_peers.disconnected_peers, 0); // counter should be decremented

    let peer = all_peers.get_peer(&peer_id).unwrap();
    assert!(matches!(peer.connection_status(), ConnectionStatus::Banned { .. }));
}

#[test]
fn test_banned_to_unbanned_transition() {
    let mut all_peers = create_all_peers(None);
    let peer_id = PeerId::random();

    // Setup: Disconnected -> Banned
    all_peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);
    all_peers.update_connection_status(&peer_id, NewConnectionStatus::Banned);

    // Test Banned -> Unbanned
    let action = all_peers.update_connection_status(&peer_id, NewConnectionStatus::Unbanned);

    assert!(matches!(action, PeerAction::Unban(_)));
    assert_eq!(all_peers.disconnected_peers, 1); // Counter should be incremented
    let peer = all_peers.get_peer(&peer_id).unwrap();
    assert!(matches!(peer.connection_status(), ConnectionStatus::Disconnected { .. }));
}

#[test]
fn test_connected_to_banned_transition() {
    let mut all_peers = create_all_peers(None);
    let peer_id = PeerId::random();
    let addr = create_multiaddr(None);

    // Setup: Unknown -> Connected
    all_peers.update_connection_status(
        &peer_id,
        NewConnectionStatus::Connected {
            multiaddr: addr,
            direction: ConnectionDirection::Incoming,
        },
    );

    // Test Connected -> Banned (should go through Disconnecting)
    let action = all_peers.update_connection_status(&peer_id, NewConnectionStatus::Banned);

    assert!(matches!(action, PeerAction::Disconnect));
    let peer = all_peers.get_peer(&peer_id).unwrap();
    assert!(matches!(peer.connection_status(), ConnectionStatus::Disconnecting { reason }
            if reason.bans_peer()));
}

#[test]
fn test_ban_action_returns_only_unbanned_ips() {
    let mut all_peers = create_all_peers(None);

    // create IPs
    let ip1 = IpAddr::V4("192.168.1.1".parse().unwrap());
    let ip2 = IpAddr::V4("192.168.1.2".parse().unwrap());

    // first peer with ip1
    let peer1 = PeerId::random();
    let addr1 = create_multiaddr(Some(ip1));
    all_peers.update_connection_status(
        &peer1,
        NewConnectionStatus::Connected {
            multiaddr: addr1.clone(),
            direction: ConnectionDirection::Incoming,
        },
    );
    all_peers.update_connection_status(
        &peer1,
        NewConnectionStatus::Disconnecting { reason: DisconnectReason::Banned },
    );
    all_peers.update_connection_status(&peer1, NewConnectionStatus::Disconnected);

    // second peer also from ip1 - this should trigger IP ban
    let peer2 = PeerId::random();
    all_peers.update_connection_status(
        &peer2,
        NewConnectionStatus::Connected {
            multiaddr: addr1.clone(),
            direction: ConnectionDirection::Incoming,
        },
    );
    all_peers.update_connection_status(
        &peer2,
        NewConnectionStatus::Disconnecting { reason: DisconnectReason::Banned },
    );
    all_peers.update_connection_status(&peer2, NewConnectionStatus::Disconnected);

    // at this point, ip1 should be banned (2 peers banned from this IP)
    assert!(all_peers.ip_banned(&ip1), "ip1 should be IP-banned after 2 peers banned");

    // now create a NEW peer that connects from ip2 but also has ip1 in its known addresses
    // NOTE: this should not happen in production, but this test is to ensure only new IP addresses
    // are returned from ban list
    let peer3 = PeerId::random();

    // connect from ip2
    let addr2 = create_multiaddr(Some(ip2));
    all_peers.update_connection_status(
        &peer3,
        NewConnectionStatus::Connected {
            multiaddr: addr2.clone(),
            direction: ConnectionDirection::Incoming,
        },
    );

    // disconnect and reconnect from ip1 to add it to known addresses
    all_peers.update_connection_status(
        &peer3,
        NewConnectionStatus::Disconnecting { reason: DisconnectReason::ExcessPeers },
    );
    all_peers.update_connection_status(&peer3, NewConnectionStatus::Disconnected);
    all_peers.update_connection_status(
        &peer3,
        NewConnectionStatus::Connected {
            multiaddr: addr1.clone(), // connect from the already-banned IP
            direction: ConnectionDirection::Incoming,
        },
    );

    // now peer3 has both ip1 (banned) and ip2 (not banned) in its history
    // ban peer3
    all_peers.update_connection_status(
        &peer3,
        NewConnectionStatus::Disconnecting { reason: DisconnectReason::Banned },
    );
    let action = all_peers.update_connection_status(&peer3, NewConnectionStatus::Disconnected);

    if let PeerAction::Ban(ips) = action {
        assert_eq!(ips.len(), 1, "Should only ban ip2 since ip1 is already IP-banned");
        assert!(ips.contains(&ip2), "Should ban ip2");
        assert!(!ips.contains(&ip1), "Should NOT include ip1 as it's already IP-banned");
    } else {
        panic!("Expected Ban action for peer3");
    }
}
