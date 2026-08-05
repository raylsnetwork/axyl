//! Invariant tests for `Reputation` and `ConnectionStatus`.
//!
//! Each test states one proposition that must hold for any sequence of penalties and status
//! transitions. Tests are written against the public seams of `AllPeers` - `process_penalty`,
//! `update_connection_status` and `heartbeat_maintenance` - never against a hand-set internal
//! state, so that a refactor of the transition table cannot make them vacuous.
//!
//! Where a test currently fails, the invariant it states is the intended behavior and the failure
//! is the bug. Such tests carry an `INVARIANT`/`CURRENT` comment pair naming both.

use super::*;
use crate::common::{create_multiaddr, ensure_score_config};
use libp2p::PeerId;
use rand::{rngs::StdRng, SeedableRng as _};
use rayls_infrastructure_config::{PeerConfig, ScoreConfig};
use rayls_infrastructure_types::{BlsKeypair, NetworkKeypair};
use std::net::{IpAddr, Ipv4Addr};

/// Build an `AllPeers` from an operator config, installing its `ScoreConfig` for this test.
///
/// The configuration is process-global but overwritten per test, so under `cargo test
/// --test-threads 1` each case runs against the config it installs here.
fn all_peers_with(config: PeerConfig) -> AllPeers {
    ensure_score_config(Some(config.score_config));
    AllPeers::new(Duration::from_secs(5), config.max_banned_peers, config.max_disconnected_peers)
}

/// Build an `AllPeers` with the default operator config.
fn all_peers() -> AllPeers {
    all_peers_with(PeerConfig::default())
}

/// A stable IPv4 address, so a test can assert against the exact IP it charged.
fn ip(last_octet: u8) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(203, 0, 113, last_octet))
}

/// A deterministic BLS key paired with a fresh network key, as `upsert_peer` expects.
fn random_keys(seed: u8) -> (BlsPublicKey, NetworkPublicKey) {
    let mut rng = StdRng::from_seed([seed; 32]);
    let bls = *BlsKeypair::generate(&mut rng).public();
    let network_key: NetworkPublicKey = NetworkKeypair::generate_ed25519().public().into();
    (bls, network_key)
}

/// Register an inbound connection from `addr`, creating the peer at the default score.
fn connect_from(all_peers: &mut AllPeers, addr: IpAddr) -> PeerId {
    let peer_id = PeerId::random();
    all_peers.update_connection_status(
        &peer_id,
        NewConnectionStatus::Connected {
            multiaddr: create_multiaddr(Some(addr)),
            direction: ConnectionDirection::Incoming,
        },
    );
    peer_id
}

/// The current aggregate score for a known peer.
fn score_of(all_peers: &AllPeers, peer_id: &PeerId) -> f64 {
    all_peers.get_peer(peer_id).expect("peer is known").score().aggregate_score()
}

/// Apply `Medium` penalties until the peer's score reaches the disconnect threshold, returning the
/// last action. Deliberately stops short of the ban threshold.
fn penalize_to_disconnect_threshold(all_peers: &mut AllPeers, peer_id: &PeerId) -> PeerAction {
    let config = ScoreConfig::default();
    let mut action = PeerAction::NoAction;
    while score_of(all_peers, peer_id) > config.min_score_before_disconnect {
        action = all_peers.process_penalty(peer_id, Penalty::Medium);
    }
    assert!(
        score_of(all_peers, peer_id) > config.min_score_before_ban,
        "helper must stop between the disconnect and ban thresholds"
    );
    action
}

/// Number of peers currently held in `ConnectionStatus::Banned`.
fn peers_in_banned_status(all_peers: &AllPeers) -> usize {
    all_peers.peers.values().filter(|peer| peer.connection_status().is_banned()).count()
}

/// The two conservation laws of the ban accounting, checkable after any mutation:
///
/// 1. `BannedPeers::total` equals the number of peers in `ConnectionStatus::Banned`.
/// 2. Every per-IP charge is backed by a peer in `ConnectionStatus::Banned` holding that IP.
///
/// Law 2 is checked in the direction that matters for admission: an IP the node refuses
/// connections from must be an IP that at least two currently-banned peers hold.
fn assert_ban_accounting_conserved(all_peers: &AllPeers, context: &str) {
    assert_eq!(
        all_peers.banned_peers.total(),
        peers_in_banned_status(all_peers),
        "{context}: banned total drifted from the set of peers in ConnectionStatus::Banned"
    );

    for ip in all_peers.banned_peers.banned_ips() {
        let holders = all_peers
            .peers
            .values()
            .filter(|peer| {
                peer.connection_status().is_banned() && peer.known_ip_addresses().any(|i| i == ip)
            })
            .count();
        assert!(
            holders > 1,
            "{context}: {ip} is blocklisted but only {holders} banned peer(s) hold it"
        );
    }
}

// Threshold arithmetic: what crossing each threshold is allowed to do

/// The two score thresholds must produce two distinct outcomes: crossing the disconnect threshold
/// disconnects, crossing the ban threshold bans.
#[test]
fn crossing_disconnect_threshold_does_not_ban() {
    let mut peers = all_peers();
    let peer_id = connect_from(&mut peers, ip(1));

    let action = penalize_to_disconnect_threshold(&mut peers, &peer_id);
    assert!(matches!(action, PeerAction::Disconnect | PeerAction::DisconnectWithPX));

    // the swarm reports the connection closed
    peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);

    // guards the `DisconnectReason::Penalized` arm: a disconnect earned at this threshold must
    // not carry the ban flag through to `handle_disconnected_and_banned`
    let status = *peers.get_peer(&peer_id).expect("peer is known").connection_status();
    assert!(
        matches!(status, ConnectionStatus::Disconnected { .. }),
        "peer between the thresholds ended in {status:?}, expected Disconnected"
    );
    assert_eq!(peers.banned_peers.total(), 0, "no ban was earned, so nothing may be charged");
}

/// Charging the IP ban table is a consequence of a ban. A peer that was never banned must leave the
/// table untouched, otherwise two such peers blocklist an IP that no banned peer ever used.
#[test]
fn crossing_disconnect_threshold_does_not_charge_the_ip_table() {
    let mut peers = all_peers();
    let shared = ip(2);

    for _ in 0..2 {
        let peer_id = connect_from(&mut peers, shared);
        penalize_to_disconnect_threshold(&mut peers, &peer_id);
        peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);
    }

    // two peers is the blocklist threshold, so this is the smallest case that would trip it
    assert!(
        !peers.ip_banned(&shared),
        "an IP was blocklisted by two peers that never reached the ban threshold"
    );
}

/// `Reputation` is the authority on whether a peer is banned. Every store that records a ban must
/// agree with it, otherwise an admission check reads one store and an eviction check reads another.
#[test]
fn ban_stores_agree_after_crossing_disconnect_threshold() {
    let mut peers = all_peers();
    let peer_id = connect_from(&mut peers, ip(3));

    penalize_to_disconnect_threshold(&mut peers, &peer_id);
    peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);

    let peer = peers.get_peer(&peer_id).expect("peer is known");
    let reputation_says_banned = peer.reputation().banned();
    let status_says_banned = peer.connection_status().is_banned();

    // the two stores are read by different call sites, so they must not disagree about one peer
    assert_eq!(
        reputation_says_banned, status_says_banned,
        "reputation and connection status disagree about whether the peer is banned"
    );
}

/// A score-driven ban must be releasable by the same mechanism that applied it: score decay. If the
/// release path keys on a state the peer never occupied, the ban outlives the score that caused it.
#[test]
fn a_score_driven_ban_is_released_by_score_decay() {
    // a short halflife so a single heartbeat decays the score above both thresholds
    let config = PeerConfig {
        score_config: ScoreConfig { score_halflife: 0.001, ..Default::default() },
        ..Default::default()
    };
    let mut peers = all_peers_with(config);

    let peer_id = connect_from(&mut peers, ip(4));
    penalize_to_disconnect_threshold(&mut peers, &peer_id);
    peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);

    // decay the score back into the tolerable range
    std::thread::sleep(Duration::from_millis(1100));
    let actions = peers.heartbeat_maintenance();

    assert!(
        score_of(&peers, &peer_id) > config.score_config.min_score_before_disconnect,
        "precondition: the score must have decayed above the disconnect threshold"
    );

    // the release path keys on `Reputation::Banned`, so a peer that only ever sat between the
    // thresholds must never have been marked banned in the first place
    let status = *peers.get_peer(&peer_id).expect("peer is known").connection_status();
    assert!(
        !status.is_banned(),
        "score recovered to {} but the peer is still {status:?} (actions: {actions:?})",
        score_of(&peers, &peer_id)
    );
    assert_eq!(peers.banned_peers.total(), 0, "IP charge outlived the score that caused it");
}

// Conservation: the ban accounting must match the set of banned peers

/// `BannedPeers::total` is the input to `prune_banned_peers`. It must equal the number of peers
/// actually held in `ConnectionStatus::Banned`, or pruning evicts the wrong number of peers.
#[test]
fn banned_total_equals_the_number_of_banned_peers() {
    let mut peers = all_peers();

    let banned = connect_from(&mut peers, ip(10));
    peers.process_penalty(&banned, Penalty::Fatal);
    peers.update_connection_status(&banned, NewConnectionStatus::Disconnected);

    let healthy = connect_from(&mut peers, ip(11));
    peers.update_connection_status(
        &healthy,
        NewConnectionStatus::Disconnecting { reason: DisconnectReason::ExcessPeers },
    );
    peers.update_connection_status(&healthy, NewConnectionStatus::Disconnected);

    assert_eq!(
        peers.banned_peers.total(),
        peers_in_banned_status(&peers),
        "banned total drifted from the set of peers in ConnectionStatus::Banned"
    );
}

/// Every increment of the ban accounting has exactly one matching decrement. Unbanning a peer that
/// was never banned must be a no-op, otherwise `total` underflows below the real count.
#[test]
fn unbanning_a_never_banned_peer_does_not_decrement_the_total() {
    let mut peers = all_peers();

    let banned = connect_from(&mut peers, ip(12));
    peers.process_penalty(&banned, Penalty::Fatal);
    peers.update_connection_status(&banned, NewConnectionStatus::Disconnected);
    assert_eq!(peers.banned_peers.total(), 1, "precondition: one banned peer");

    // a peer that was disconnected normally is asked to unban
    let healthy = connect_from(&mut peers, ip(13));
    peers.update_connection_status(
        &healthy,
        NewConnectionStatus::Disconnecting { reason: DisconnectReason::ExcessPeers },
    );
    peers.update_connection_status(&healthy, NewConnectionStatus::Disconnected);
    peers.update_connection_status(&healthy, NewConnectionStatus::Unbanned);

    // INVARIANT: only a banned peer's release may decrement the total.
    // CURRENT: `BannedPeers::remove_banned_peer` decrements unconditionally.
    assert_eq!(
        peers.banned_peers.total(),
        peers_in_banned_status(&peers),
        "unbanning a never-banned peer corrupted the banned total"
    );
}

/// Ban and unban must round-trip the per-IP counters, so an IP is not blocklisted by a charge that
/// no longer corresponds to a banned peer.
#[test]
fn ban_then_unban_round_trips_the_per_ip_counters() {
    let mut peers = all_peers();
    let shared = ip(14);

    let ids: Vec<_> = (0..2)
        .map(|_| {
            let peer_id = connect_from(&mut peers, shared);
            peers.process_penalty(&peer_id, Penalty::Fatal);
            peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);
            peer_id
        })
        .collect();

    assert!(peers.ip_banned(&shared), "precondition: two banned peers blocklist the shared IP");

    for peer_id in &ids {
        peers.update_connection_status(peer_id, NewConnectionStatus::Unbanned);
    }

    assert!(!peers.ip_banned(&shared), "IP stayed blocklisted after every ban was released");
    assert_eq!(peers.banned_peers.total(), 0, "banned total did not return to zero");
}

// Score semantics

/// `Penalty::Fatal` assigns the floor rather than subtracting, so applying it twice must leave the
/// score where the first one put it.
#[test]
fn repeated_fatal_penalties_are_idempotent() {
    let config = ScoreConfig::default();
    let mut peers = all_peers();
    let peer_id = connect_from(&mut peers, ip(20));

    peers.process_penalty(&peer_id, Penalty::Fatal);
    let after_first = score_of(&peers, &peer_id);
    peers.process_penalty(&peer_id, Penalty::Fatal);

    assert_eq!(after_first, config.min_score, "Fatal must assign the configured floor");
    assert_eq!(score_of(&peers, &peer_id), after_first, "a second Fatal moved the score");
}

/// Penalties clamp at the configured floor rather than accumulating below it, so a peer cannot be
/// driven arbitrarily negative and outlast the decay schedule.
#[test]
fn penalties_clamp_at_the_configured_floor() {
    let config = ScoreConfig::default();
    let mut peers = all_peers();
    let peer_id = connect_from(&mut peers, ip(21));

    for _ in 0..40 {
        peers.process_penalty(&peer_id, Penalty::Severe);
    }

    assert_eq!(score_of(&peers, &peer_id), config.min_score, "score escaped the configured floor");
}

/// The reputation ladder is read at exactly two points. A peer sitting exactly on a threshold must
/// land on the harsher side consistently, and one point above it must not.
#[test]
fn reputation_boundaries_are_inclusive_at_the_threshold() {
    let config = ScoreConfig::default();
    let mut peers = all_peers();

    // exactly on the disconnect threshold
    let at_threshold = connect_from(&mut peers, ip(22));
    while score_of(&peers, &at_threshold) > config.min_score_before_disconnect {
        peers.process_penalty(&at_threshold, Penalty::Medium);
    }
    assert_eq!(score_of(&peers, &at_threshold), config.min_score_before_disconnect);
    assert_eq!(
        peers.get_peer(&at_threshold).expect("known").reputation(),
        Reputation::Disconnected,
        "a score exactly at the disconnect threshold must count as Disconnected"
    );

    // one Mild above it
    let above = connect_from(&mut peers, ip(23));
    while score_of(&peers, &above) > config.min_score_before_disconnect + 1.0 {
        peers.process_penalty(&above, Penalty::Mild);
    }
    assert_eq!(
        peers.get_peer(&above).expect("known").reputation(),
        Reputation::Trusted,
        "a score above the disconnect threshold must still count as Trusted"
    );
}

// Absolution

/// A current-committee member is immune to penalties, so its score must not move at all.
#[test]
fn committee_members_are_immune_to_penalties() {
    let mut peers = all_peers();
    let peer_id = connect_from(&mut peers, ip(30));
    peers.current_committee.insert(peer_id);

    let before = score_of(&peers, &peer_id);
    let action = peers.process_penalty(&peer_id, Penalty::Fatal);

    assert!(matches!(action, PeerAction::NoAction));
    assert_eq!(score_of(&peers, &peer_id), before, "a committee member's score was penalized");
}

// Ban-table capacity and trust promotion

/// The ban table is bounded for memory, not for amnesty. Eviction ages out the oldest bans, so a
/// flood of new identities cannot flush the bans this node most recently earned.
///
/// Three bans against a bound of one forces `excess == 2`. A selection that is correct only for a
/// single eviction passes the one-peer case and still picks the wrong pair here.
#[test]
fn ban_table_overflow_retires_the_oldest_bans() {
    let mut peers = all_peers_with(PeerConfig { max_banned_peers: 1, ..Default::default() });

    // ban three peers without pruning in between, so the bound is exceeded by two at once
    let banned: Vec<_> = (70..73u8)
        .map(|octet| {
            let peer_id = connect_from(&mut peers, ip(octet));
            peers.process_penalty(&peer_id, Penalty::Fatal);
            peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);
            std::thread::sleep(Duration::from_millis(2));
            peer_id
        })
        .collect();
    assert_eq!(peers.banned_peers.total(), 3, "precondition: three bans, bound of one");

    // any disconnect registration triggers the capacity sweep
    let (_, pruned) = peers.register_disconnected(&banned[2]);
    let retired: Vec<_> = pruned.iter().map(|(id, _)| *id).collect();

    assert_eq!(retired.len(), 2, "a bound of one against three bans retires two");
    assert!(retired.contains(&banned[0]), "the oldest ban survived eviction");
    assert!(retired.contains(&banned[1]), "the second-oldest ban survived eviction");
    assert!(!retired.contains(&banned[2]), "eviction retired the ban applied most recently");
}

/// Disconnected-peer eviction ages out the oldest records, like ban eviction. Both callers share
/// `collect_excess_peers`, so both inherit whichever ordering it implements.
#[test]
fn disconnected_peer_eviction_retires_the_oldest_record() {
    let mut peers = all_peers_with(PeerConfig { max_disconnected_peers: 1, ..Default::default() });

    let first = connect_from(&mut peers, ip(95));
    peers.update_connection_status(
        &first,
        NewConnectionStatus::Disconnecting { reason: DisconnectReason::ExcessPeers },
    );
    peers.register_disconnected(&first);

    let second = connect_from(&mut peers, ip(96));
    peers.update_connection_status(
        &second,
        NewConnectionStatus::Disconnecting { reason: DisconnectReason::ExcessPeers },
    );
    peers.register_disconnected(&second);

    // INVARIANT: the older record is the one evicted.
    // CURRENT: `collect_excess_peers` converges on the newest entries, so the record just created
    // is dropped and the stale one is retained.
    assert!(
        peers.get_peer(&second).is_some(),
        "eviction dropped the newest disconnected record instead of the oldest"
    );
    assert!(peers.get_peer(&first).is_none(), "the oldest disconnected record survived eviction");
}

// Leaving the Banned state: every exit settles the accounting

/// Promoting a peer to trusted is documented to unban its IPs. It must actually release every
/// charge that peer holds, or the charge outlives the peer record entirely.
#[test]
fn promoting_a_banned_peer_to_trusted_releases_its_ip_charges() {
    let mut peers = all_peers();
    let shared = ip(72);

    let mut identities = Vec::new();
    for seed in 10..12u8 {
        let (bls, network_key) = random_keys(seed);
        let peer_id: PeerId = network_key.clone().into();
        peers.update_connection_status(
            &peer_id,
            NewConnectionStatus::Connected {
                multiaddr: create_multiaddr(Some(shared)),
                direction: ConnectionDirection::Incoming,
            },
        );
        peers.process_penalty(&peer_id, Penalty::Fatal);
        peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);
        identities.push((bls, network_key));
    }
    assert!(peers.ip_banned(&shared), "precondition: the shared IP is blocklisted");

    for (bls, network_key) in identities {
        peers.add_trusted_peer(bls, network_key, vec![create_multiaddr(Some(shared))]);
    }

    // INVARIANT: after both peers are trusted, nothing is still charged against their IP.
    // CURRENT: `Peer::new_trusted` files the address under `listening_addrs`, so
    // `known_ip_addresses()` on the fresh value is empty and `remove_banned_peer` cleans nothing.
    assert!(!peers.ip_banned(&shared), "IP stayed blocklisted after every holder became trusted");
}

/// Disconnecting a peer that is already banned must not lose its ban accounting. If the counters
/// stay charged while no peer is in `ConnectionStatus::Banned`, the next ban double-charges.
#[test]
fn disconnecting_an_already_banned_peer_settles_the_accounting() {
    let mut peers = all_peers();
    let peer_id = connect_from(&mut peers, ip(90));

    peers.process_penalty(&peer_id, Penalty::Fatal);
    peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);
    assert_ban_accounting_conserved(&peers, "after the ban");

    // the manager disconnects a peer it already banned; `disconnect_peer` hardcodes `banned: false`
    peers.update_connection_status(
        &peer_id,
        NewConnectionStatus::Disconnecting { reason: DisconnectReason::ExcessPeers },
    );
    peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);

    // INVARIANT: the peer is no longer banned, so nothing is still charged for it.
    // CURRENT: `handle_disconnecting_transition` sets the new status before inspecting the old
    // one, and its `Banned` arm only logs - `remove_banned_peer` is never called.
    assert_ban_accounting_conserved(&peers, "after disconnecting an already-banned peer");
}
/// Re-banning a peer whose earlier ban was never settled must not charge it twice, or the ban
/// table grows a permanent surplus for a single peer.
#[test]
fn re_banning_a_peer_does_not_double_charge_it() {
    let mut peers = all_peers();
    let peer_id = connect_from(&mut peers, ip(91));

    peers.process_penalty(&peer_id, Penalty::Fatal);
    peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);
    peers.update_connection_status(
        &peer_id,
        NewConnectionStatus::Disconnecting { reason: DisconnectReason::ExcessPeers },
    );
    peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);

    // the same peer is banned again from the Disconnected state
    peers.update_connection_status(&peer_id, NewConnectionStatus::Banned);

    // INVARIANT: one banned peer is worth exactly one charge.
    // CURRENT: the first charge was never released, so `add_banned_peer` takes the total to 2.
    assert_ban_accounting_conserved(&peers, "after re-banning the same peer");
}
/// A peer's ban must charge only its own IPs. If the charge is released against whatever addresses
/// the peer holds at release time, one peer's unban can cancel another peer's ban.
#[test]
fn releasing_a_ban_cannot_cancel_another_peers_ban() {
    let mut peers = all_peers();
    let shared = ip(93);

    // two peers banned from the shared address, enough to blocklist it
    for seed in 30..32u8 {
        let (bls, network_key) = random_keys(seed);
        let peer_id: PeerId = network_key.clone().into();
        peers.update_connection_status(
            &peer_id,
            NewConnectionStatus::Connected {
                multiaddr: create_multiaddr(Some(shared)),
                direction: ConnectionDirection::Incoming,
            },
        );
        peers.upsert_peer(bls, network_key, vec![create_multiaddr(Some(shared))]);
        peers.process_penalty(&peer_id, Penalty::Fatal);
        peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);
    }
    assert!(peers.ip_banned(&shared), "precondition: the shared address is blocklisted");

    // an unrelated peer is banned from its own address, then learns the shared one from a record
    let (bls, network_key) = random_keys(32);
    let outsider: PeerId = network_key.clone().into();
    peers.update_connection_status(
        &outsider,
        NewConnectionStatus::Connected {
            multiaddr: create_multiaddr(Some(ip(94))),
            direction: ConnectionDirection::Incoming,
        },
    );
    peers.process_penalty(&outsider, Penalty::Fatal);
    peers.update_connection_status(&outsider, NewConnectionStatus::Disconnected);
    peers.upsert_peer(bls, network_key, vec![create_multiaddr(Some(shared))]);

    // releasing the outsider's own ban
    peers.update_connection_status(&outsider, NewConnectionStatus::Unbanned);

    // INVARIANT: two peers are still banned at the shared address, so it stays blocklisted.
    // CURRENT: `remove_banned_peer` decrements every IP the peer holds *at release time*, which
    // now includes an address it never charged.
    assert!(
        peers.ip_banned(&shared),
        "one peer's unban cancelled a charge belonging to two other banned peers"
    );
}
/// Promoting an unrelated peer to trusted must not touch the ban accounting at all.
#[test]
fn promoting_an_unrelated_peer_to_trusted_leaves_the_ban_total_alone() {
    let mut peers = all_peers();

    let banned = connect_from(&mut peers, ip(97));
    peers.process_penalty(&banned, Penalty::Fatal);
    peers.update_connection_status(&banned, NewConnectionStatus::Disconnected);
    assert_eq!(peers.banned_peers.total(), 1, "precondition: exactly one banned peer");

    let (bls, network_key) = random_keys(40);
    peers.add_trusted_peer(bls, network_key, vec![create_multiaddr(Some(ip(98)))]);

    // INVARIANT: adding a trusted peer that was never banned changes nothing.
    // CURRENT: `add_trusted_peer` calls `remove_banned_peer`, whose unconditional
    // `total.saturating_sub(1)` runs even though the IP iterator it is handed is always empty.
    assert_ban_accounting_conserved(&peers, "after promoting an unrelated peer to trusted");
}

// Absolution

/// Decay is a function of elapsed wall time. Running the heartbeat more often must not slow it
/// down, or a node with a fast heartbeat never forgives anything.
#[test]
fn decay_depends_on_elapsed_time_not_on_heartbeat_frequency() {
    let config = PeerConfig {
        score_config: ScoreConfig { score_halflife: 1.0, ..Default::default() },
        ..Default::default()
    };
    let mut peers = all_peers_with(config);

    let peer_id = connect_from(&mut peers, ip(40));
    peers.process_penalty(&peer_id, Penalty::Medium);
    let after_penalty = score_of(&peers, &peer_id);

    // one halflife of wall time, sampled every 100ms
    for _ in 0..10 {
        std::thread::sleep(Duration::from_millis(100));
        peers.heartbeat_maintenance();
    }

    // INVARIANT: after one halflife the score has moved roughly halfway back to zero.
    // CURRENT: `Score::update_at` truncates elapsed to whole seconds but advances `last_updated`
    // unconditionally, so a sub-second sampling cadence discards every remainder.
    assert!(
        score_of(&peers, &peer_id) > after_penalty / 2.0,
        "score did not decay across one halflife sampled at 100ms: {} vs {after_penalty}",
        score_of(&peers, &peer_id)
    );
}

// Heartbeat verdicts

/// Every verdict the heartbeat reaches must be acted on. A verdict that is computed and then
/// dropped is a control that reads as present and is not.
#[test]
fn the_heartbeat_acts_on_a_disconnect_verdict() {
    let mut peers = all_peers();
    let peer_id = connect_from(&mut peers, ip(60));

    // drive the peer clearly below the disconnect threshold, then let it reconnect with its score
    // intact. One more penalty past the threshold keeps the verdict stable across the sub-second
    // decay the heartbeat applies before reading the reputation back.
    penalize_to_disconnect_threshold(&mut peers, &peer_id);
    peers.process_penalty(&peer_id, Penalty::Medium);
    peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);
    peers.update_connection_status(
        &peer_id,
        NewConnectionStatus::Connected {
            multiaddr: create_multiaddr(Some(ip(60))),
            direction: ConnectionDirection::Incoming,
        },
    );
    assert_eq!(
        peers.get_peer(&peer_id).expect("known").reputation(),
        Reputation::Disconnected,
        "precondition: a connected peer sitting below the disconnect threshold"
    );

    let actions = peers.heartbeat_maintenance();

    // a verdict the heartbeat computes and drops is a control that reads as present and is not,
    // and it restates itself at error level on every beat for as long as the score stays down
    assert!(
        actions.iter().any(|(id, action)| *id == peer_id
            && matches!(action, PeerAction::Disconnect | PeerAction::DisconnectWithPX)),
        "heartbeat reached a disconnect verdict but returned no action: {actions:?}"
    );
}

// Provenance: what an IP charge is allowed to be derived from

/// The IP ban table is a Sybil defense keyed on where a peer actually connected from. Charging it
/// with addresses a peer merely claims lets that peer blocklist an address it does not control.
#[test]
fn only_observed_addresses_charge_the_ip_ban_table() {
    let mut peers = all_peers();
    let victim = ip(50);

    // two throwaway identities, each connecting from its own address and then advertising the
    // victim's address as one of its own in a peer record
    for seed in 0..2u8 {
        let (bls, network_key) = random_keys(seed);
        let peer_id: PeerId = network_key.clone().into();
        peers.update_connection_status(
            &peer_id,
            NewConnectionStatus::Connected {
                multiaddr: create_multiaddr(Some(ip(51 + seed))),
                direction: ConnectionDirection::Incoming,
            },
        );
        peers.upsert_peer(bls, network_key, vec![create_multiaddr(Some(victim))]);

        // each identity earns its own ban
        peers.process_penalty(&peer_id, Penalty::Fatal);
        peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);
    }

    // INVARIANT: only the address this node observed the connection on is charged.
    // CURRENT: `Peer::known_ip_addresses` reads `multiaddrs`, which `update_net` extends from the
    // peer's own record, so two peers can blocklist any address they name.
    assert!(
        !peers.ip_banned(&victim),
        "two peers blocklisted an address neither of them ever connected from"
    );
}

/// Clearing a peer's ban is a decision other stores must hear about - the gossipsub blacklist and
/// the kad routing table are repaired only by `PeerAction::Unban`.
#[test]
fn clearing_a_ban_on_dial_emits_an_unban_action() {
    let mut peers = all_peers();
    let peer_id = connect_from(&mut peers, ip(92));

    peers.process_penalty(&peer_id, Penalty::Fatal);
    peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);
    assert_eq!(peers.banned_peers.total(), 1, "precondition: the peer is banned and charged");

    let action = peers.update_connection_status(&peer_id, NewConnectionStatus::Dialing);

    // INVARIANT: a transition that clears the ban stores announces it.
    // CURRENT: `handle_dialing_transition`'s `Banned` arm calls `remove_banned_peer` and then
    // returns `NoAction`, so the peer is silently un-charged while the gossipsub blacklist and the
    // kad routing entry stay as the ban left them.
    assert!(
        matches!(action, PeerAction::Unban(_)),
        "ban cleared on dial without an Unban action: {action:?}"
    );
}

/// The same law on the connect path. Accepting a connection from a banned peer releases the ban,
/// so it carries the same obligation to announce it.
#[test]
fn clearing_a_ban_on_connect_emits_an_unban_action() {
    let mut peers = all_peers();
    let peer_id = connect_from(&mut peers, ip(99));

    peers.process_penalty(&peer_id, Penalty::Fatal);
    peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);
    assert_eq!(peers.banned_peers.total(), 1, "precondition: the peer is banned and charged");

    let action = peers.update_connection_status(
        &peer_id,
        NewConnectionStatus::Connected {
            multiaddr: create_multiaddr(Some(ip(99))),
            direction: ConnectionDirection::Incoming,
        },
    );

    assert!(
        matches!(action, PeerAction::Unban(_)),
        "ban cleared on connect without an Unban action: {action:?}"
    );
}

// Configured thresholds

/// The decay freeze and the ban verdict must agree about which peers are banned. A peer the freeze
/// holds but the verdict calls merely disconnected has a score nothing can move and a state
/// nothing can release, because every release path keys on the verdict.
#[test]
fn the_decay_freeze_agrees_with_the_ban_verdict() {
    // an operator hardens the ban threshold
    let config = PeerConfig {
        score_config: ScoreConfig { min_score_before_ban: -15.0, ..Default::default() },
        ..Default::default()
    };
    let mut peers = all_peers_with(config);

    // one peer either side of the configured threshold
    for (octet, penalties) in [(41u8, 2usize), (43, 4)] {
        let peer_id = connect_from(&mut peers, ip(octet));
        for _ in 0..penalties {
            peers.process_penalty(&peer_id, Penalty::Medium);
        }

        let peer = peers.get_peer(&peer_id).expect("known");
        assert_eq!(
            peer.score().is_banned(),
            peer.reputation().banned(),
            "the freeze and the verdict disagree at score {}",
            peer.score().aggregate_score()
        );
    }
}

/// The operator's configured thresholds are the ones enforced. A security control that reads as
/// configured but is not is worse than no knob at all, because it converts an available mitigation
/// into a false belief that the mitigation is applied.
#[test]
fn operator_configured_thresholds_are_enforced() {
    // an operator relaxes the thresholds to stop false positives
    let config = PeerConfig {
        score_config: ScoreConfig {
            min_score_before_disconnect: -60.0,
            min_score_before_ban: -80.0,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut peers = all_peers_with(config);

    let peer_id = connect_from(&mut peers, ip(42));
    for _ in 0..4 {
        peers.process_penalty(&peer_id, Penalty::Medium);
    }
    assert_eq!(score_of(&peers, &peer_id), -20.0, "precondition: the peer sits at -20");

    assert_eq!(
        peers.get_peer(&peer_id).expect("known").reputation(),
        Reputation::Trusted,
        "the disconnect threshold was configured to -60 but a peer at -20 was disconnected"
    );
}
