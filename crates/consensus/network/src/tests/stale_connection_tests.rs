//! A peer that dies silently and comes back with the same identity before its old QUIC
//! connection idles out (30 s) must get working gossip and request-response at once.
//!
//! The old instance reaches the target through an in-process UDP forwarder; dropping the
//! forwarder makes the old connection go silent exactly like a killed process (no QUIC close
//! ever reaches the target), so the target keeps it as a second, dead connection when the new
//! instance connects.

use super::*;
use crate::{
    common::{TestWorkerRequest, TestWorkerResponse, TEST_HEARTBEAT_INTERVAL},
    kad::KadStoreType,
    types::NetworkHandle,
    NetworkMetrics,
};
use libp2p::{gossipsub::TopicHash, Multiaddr, PeerId};
use rayls_infrastructure_config::{ConsensusConfig, NetworkConfig};
use rayls_infrastructure_storage::mem_db::MemDatabase;
use rayls_infrastructure_types::TaskManager;
use rayls_testing_test_utils::CommitteeFixture;
use std::{
    collections::HashSet,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{net::UdpSocket, sync::mpsc, task::JoinHandle, time::timeout};

type Req = TestWorkerRequest;
type Res = TestWorkerResponse;
type Events = mpsc::Receiver<NetworkEvent<Req, Res>>;

const TOPIC: &str = "stale-connection-topic";
/// Well under the 30 s idle timeout, so the dead connection is still open on the target.
const DEADLINE: Duration = Duration::from_secs(10);

/// Build and spawn one network instance for `config`; returns its handle and event stream.
fn spawn_network(
    config: &ConsensusConfig<MemDatabase>,
    task_manager: &TaskManager,
) -> (NetworkHandle<Req, Res>, Events, JoinHandle<()>) {
    let (tx, events) = mpsc::channel(100);
    let network =
        ConsensusNetwork::<Req, Res, MemDatabase, mpsc::Sender<NetworkEvent<Req, Res>>>::new(
            config.network_config(),
            tx,
            config.key_config().clone(),
            config.key_config().primary_network_keypair().clone(),
            MemDatabase::default(),
            task_manager.get_spawner(),
            KadStoreType::Primary,
            config.primary_address(),
            Arc::new(NetworkMetrics::default()),
        )
        .expect("network created");
    let handle = network.network_handle();
    let task = tokio::spawn(async move {
        let _ = network.run().await;
    });
    (handle, events, task)
}

/// Forward UDP datagrams between one client and `target`; returns the address clients dial.
/// While `drop_down` is set, datagrams from `target` to the client are dropped (one direction of
/// the link is cut; the other still flows).
async fn spawn_forwarder(
    target: SocketAddr,
    drop_down: Arc<AtomicBool>,
) -> (SocketAddr, JoinHandle<()>) {
    let front = UdpSocket::bind("127.0.0.1:0").await.expect("front binds");
    let back = UdpSocket::bind("127.0.0.1:0").await.expect("back binds");
    let front_addr = front.local_addr().expect("front addr");
    let task = tokio::spawn(async move {
        let mut client: Option<SocketAddr> = None;
        let mut up = vec![0u8; 65_536];
        let mut down = vec![0u8; 65_536];
        loop {
            tokio::select! {
                res = front.recv_from(&mut up) => {
                    if let Ok((n, from)) = res {
                        client = Some(from);
                        let _ = back.send_to(&up[..n], target).await;
                    }
                }
                res = back.recv_from(&mut down) => {
                    if let (Ok((n, _)), Some(client)) = (res, client) {
                        if drop_down.load(Ordering::Relaxed) {
                            continue;
                        }
                        let _ = front.send_to(&down[..n], client).await;
                    }
                }
            }
        }
    });
    (front_addr, task)
}

fn socket_addr(addr: &Multiaddr) -> SocketAddr {
    use libp2p::multiaddr::Protocol;
    let mut ip = None;
    let mut port = None;
    for p in addr.iter() {
        match p {
            Protocol::Ip4(v4) => ip = Some(std::net::IpAddr::V4(v4)),
            Protocol::Udp(p) => port = Some(p),
            _ => {}
        }
    }
    SocketAddr::new(ip.expect("ip"), port.expect("udp port"))
}

/// The target (validator 1) and a restarted peer (validator 2, new process, same identity)
/// while the target still holds the old instance's dead connection.
struct Restarted {
    target: NetworkHandle<Req, Res>,
    target_events: Events,
    target_config: ConsensusConfig<MemDatabase>,
    peer: NetworkHandle<Req, Res>,
    peer_events: Events,
    peer_config: ConsensusConfig<MemDatabase>,
    _task_manager: TaskManager,
}

async fn restart_behind_stale_connection() -> eyre::Result<Restarted> {
    let mut network_config = NetworkConfig::default();
    network_config.peer_config_mut().heartbeat_interval = TEST_HEARTBEAT_INTERVAL;
    let fixture =
        CommitteeFixture::builder(MemDatabase::default).with_network_config(network_config).build();
    let mut authorities = fixture.authorities();
    let target_config = authorities.next().expect("first authority").consensus_config();
    let peer_config = authorities.next().expect("second authority").consensus_config();
    let committee: HashSet<_> = [
        target_config.key_config().primary_public_key(),
        peer_config.key_config().primary_public_key(),
    ]
    .into_iter()
    .collect();
    let task_manager = TaskManager::default();

    // target listens directly and knows where the peer listens
    let (target, target_events, _) = spawn_network(&target_config, &task_manager);
    target.start_listening(target_config.primary_address()).await?;
    let target_addr = target_config.primary_address();
    target.subscribe_with_publishers(TOPIC.into(), target_config.committee_pub_keys()).await?;
    target
        .add_explicit_peer(
            peer_config.key_config().primary_public_key(),
            peer_config.primary_networkkey(),
            peer_config.primary_address(),
        )
        .await?;
    target.new_epoch(committee.clone()).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // old instance of the peer reaches the target only through the forwarder
    let (front, forwarder) =
        spawn_forwarder(socket_addr(&target_addr), Arc::new(AtomicBool::new(false))).await;
    let (old, _old_events, old_task) = spawn_network(&peer_config, &task_manager);
    old.subscribe_with_publishers(TOPIC.into(), peer_config.committee_pub_keys()).await?;
    let via_forwarder: Multiaddr =
        format!("/ip4/127.0.0.1/udp/{}/quic-v1", front.port()).parse()?;
    old.add_trusted_peer_and_dial(
        target_config.key_config().primary_public_key(),
        target_config.key_config().primary_network_public_key(),
        via_forwarder,
    )
    .await?;
    let target_peer: PeerId = target_config.primary_networkkey().into();
    let topic = TopicHash::from_raw(TOPIC);
    timeout(DEADLINE, async {
        loop {
            let knows = old.all_peers().await.ok().and_then(|p| p.get(&target_peer).cloned());
            if knows.is_some_and(|topics| topics.contains(&topic)) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await?;

    // the old instance stays up a few seconds, as a real node does before it crashes
    tokio::time::sleep(Duration::from_secs(3)).await;

    // the old instance dies silently: nothing more reaches the target over its connection
    forwarder.abort();
    old_task.abort();
    drop(old);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // the new instance comes back with the same identity and dials the target directly
    let (peer, peer_events, _) = spawn_network(&peer_config, &task_manager);
    peer.start_listening(peer_config.primary_address()).await?;
    peer.subscribe_with_publishers(TOPIC.into(), peer_config.committee_pub_keys()).await?;
    peer.new_epoch(committee).await?;
    peer.add_trusted_peer_and_dial(
        target_config.key_config().primary_public_key(),
        target_config.key_config().primary_network_public_key(),
        target_addr,
    )
    .await?;

    Ok(Restarted {
        target,
        target_events,
        target_config,
        peer,
        peer_events,
        peer_config,
        _task_manager: task_manager,
    })
}

/// The restarted peer learns the target's subscriptions again and its publishes reach it.
#[tokio::test(flavor = "multi_thread")]
async fn test_restarted_peer_relearns_subscriptions() -> eyre::Result<()> {
    let Restarted { target, mut target_events, target_config, peer, .. } =
        restart_behind_stale_connection().await?;
    let target_peer: PeerId = target_config.primary_networkkey().into();
    let topic = TopicHash::from_raw(TOPIC);
    let payload = b"after restart".to_vec();

    let outcome = timeout(DEADLINE, async {
        loop {
            let knows = peer.all_peers().await.ok().and_then(|p| p.get(&target_peer).cloned());
            if knows.is_some_and(|topics| topics.contains(&topic))
                && peer.publish(TOPIC.into(), payload.clone()).await.is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await;
    let known = peer.all_peers().await?.get(&target_peer).cloned();
    assert!(
        outcome.is_ok(),
        "restarted peer never relearned the target's subscriptions within {DEADLINE:?}: \
         target's topics as seen by the peer = {known:?}"
    );

    // the publish reaches the target
    let received = timeout(DEADLINE, async {
        while let Some(event) = target_events.recv().await {
            if matches!(&event, NetworkEvent::Gossip(msg, _) if msg.data == payload) {
                return true;
            }
        }
        false
    })
    .await;
    assert_eq!(received, Ok(true), "target never received the restarted peer's publish");
    drop(target);
    Ok(())
}

/// Every request from the target to the restarted peer is answered promptly: none is routed
/// over the dead connection.
#[tokio::test(flavor = "multi_thread")]
async fn test_requests_to_restarted_peer_avoid_dead_connection() -> eyre::Result<()> {
    let Restarted { target, peer, mut peer_events, peer_config, .. } =
        restart_behind_stale_connection().await?;
    let responder = peer.clone();
    tokio::spawn(async move {
        while let Some(event) = peer_events.recv().await {
            if let NetworkEvent::Request { channel, .. } = event {
                let _ = responder
                    .send_response(TestWorkerResponse::MissingBatches { batches: vec![] }, channel)
                    .await;
            }
        }
    });
    let peer_key = peer_config.key_config().primary_public_key();

    // let the reconnect settle (and, with the fix, the reset and redial complete)
    timeout(DEADLINE, async {
        while target.connected_peer_count().await.unwrap_or(0) == 0 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await?;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut answered = 0;
    let mut hung = 0;
    for _ in 0..8 {
        let reply =
            target.send_request(TestWorkerRequest::MissingBatches(vec![]), peer_key).await?;
        match timeout(Duration::from_secs(3), reply).await {
            Ok(Ok(Ok(_))) => answered += 1,
            _ => hung += 1,
        }
    }
    assert_eq!(hung, 0, "{hung} of {} requests to the restarted peer hung", answered + hung);
    Ok(())
}

/// Whether `peer` knows that `target` subscribes to `topic`.
async fn knows_topic(peer: &NetworkHandle<Req, Res>, target: &PeerId, topic: &TopicHash) -> bool {
    peer.all_peers()
        .await
        .ok()
        .and_then(|p| p.get(target).cloned())
        .is_some_and(|topics| topics.contains(topic))
}

/// A network attacker on the path drops one direction of a live link (target to peer) for a little
/// over the idle timeout, with no crash and no restart. The peer times out and redials; the target
/// still holds the old connection, because the peer's keep-alives kept reaching it. After the drop
/// ends, the peer must see the target's subscriptions again and requests must not hang.
///
/// Ignored because it waits out the real 30 s QUIC idle timeout (about 50 s per run); run it with
/// `--ignored`.
#[ignore = "waits out the 30 s QUIC idle timeout (about 50 s); run with --ignored"]
#[tokio::test(flavor = "multi_thread")]
async fn test_one_way_drop_relearns_subscriptions() -> eyre::Result<()> {
    let mut network_config = NetworkConfig::default();
    network_config.peer_config_mut().heartbeat_interval = TEST_HEARTBEAT_INTERVAL;
    let fixture =
        CommitteeFixture::builder(MemDatabase::default).with_network_config(network_config).build();
    let mut authorities = fixture.authorities();
    let target_config = authorities.next().expect("first authority").consensus_config();
    let peer_config = authorities.next().expect("second authority").consensus_config();
    let committee: HashSet<_> = [
        target_config.key_config().primary_public_key(),
        peer_config.key_config().primary_public_key(),
    ]
    .into_iter()
    .collect();
    let task_manager = TaskManager::default();
    let target_peer: PeerId = target_config.primary_networkkey().into();
    let topic = TopicHash::from_raw(TOPIC);

    let (target, _target_events, _) = spawn_network(&target_config, &task_manager);
    target.start_listening(target_config.primary_address()).await?;
    target.subscribe_with_publishers(TOPIC.into(), target_config.committee_pub_keys()).await?;
    target.new_epoch(committee.clone()).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // the peer reaches the target only through the forwarder, where the attacker sits
    let drop_down = Arc::new(AtomicBool::new(false));
    let (front, _forwarder) =
        spawn_forwarder(socket_addr(&target_config.primary_address()), drop_down.clone()).await;
    let (peer, mut peer_events, _) = spawn_network(&peer_config, &task_manager);
    peer.subscribe_with_publishers(TOPIC.into(), peer_config.committee_pub_keys()).await?;
    peer.new_epoch(committee).await?;
    let via_forwarder: Multiaddr =
        format!("/ip4/127.0.0.1/udp/{}/quic-v1", front.port()).parse()?;
    peer.add_trusted_peer_and_dial(
        target_config.key_config().primary_public_key(),
        target_config.key_config().primary_network_public_key(),
        via_forwarder,
    )
    .await?;
    let responder = peer.clone();
    tokio::spawn(async move {
        while let Some(event) = peer_events.recv().await {
            if let NetworkEvent::Request { channel, .. } = event {
                let _ = responder
                    .send_response(TestWorkerResponse::MissingBatches { batches: vec![] }, channel)
                    .await;
            }
        }
    });
    timeout(DEADLINE, async {
        while !knows_topic(&peer, &target_peer, &topic).await {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await?;
    tokio::time::sleep(Duration::from_secs(3)).await;

    // cut target -> peer for a little over the 30 s idle timeout
    drop_down.store(true, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_secs(35)).await;
    drop_down.store(false, Ordering::Relaxed);

    // the peer redials on its heartbeat; it must relearn the target's subscriptions
    let payload = b"after one-way drop".to_vec();
    let relearned = timeout(Duration::from_secs(20), async {
        loop {
            if knows_topic(&peer, &target_peer, &topic).await
                && peer.publish(TOPIC.into(), payload.clone()).await.is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await;
    let known = peer.all_peers().await?.get(&target_peer).cloned();
    assert!(
        relearned.is_ok(),
        "peer never relearned the target's subscriptions after the one-way drop: \
         target's topics as seen by the peer = {known:?}"
    );

    // and the target's requests to the peer are all answered
    let peer_key = peer_config.key_config().primary_public_key();
    let mut hung = 0;
    for _ in 0..8 {
        let reply =
            target.send_request(TestWorkerRequest::MissingBatches(vec![]), peer_key).await?;
        if !matches!(timeout(Duration::from_secs(3), reply).await, Ok(Ok(Ok(_)))) {
            hung += 1;
        }
    }
    assert_eq!(hung, 0, "{hung} of 8 requests to the peer hung after the one-way drop");
    Ok(())
}
