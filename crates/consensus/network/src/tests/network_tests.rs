//! Tests networking using libp2p between peers.

use super::*;
use crate::{
    common::{
        create_multiaddr, TestPrimaryRequest, TestPrimaryResponse, TestWorkerRequest,
        TestWorkerResponse, TEST_HEARTBEAT_INTERVAL,
    },
    error::NetworkError,
    kad::KadStoreType,
    types::NetworkHandle,
    NetworkMetrics, Penalty,
};
use assert_matches::assert_matches;
use eyre::eyre;
use futures::StreamExt as _;
use libp2p::{
    core::transport::ListenerId,
    gossipsub::{Message as GossipMessage, TopicHash},
    kad::{self, store::RecordStore, ProviderRecord, RecordKey},
    multiaddr::Protocol,
    swarm::SwarmEvent,
    Multiaddr, PeerId,
};
use rayls_execution_evm::test_utils::fixture_batch_with_transactions;
use rayls_infrastructure_config::{ConsensusConfig, NetworkConfig};
use rayls_infrastructure_storage::mem_db::MemDatabase;
use rayls_infrastructure_types::{encode, now, BlsSigner, Certificate, Header, TaskManager};
use rayls_testing_test_utils::CommitteeFixture;
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use tokio::{sync::mpsc, time::timeout};
use tracing::debug;

/// Test topic for gossip.
const TEST_TOPIC: &str = "test-topic";

/// Helper function to create peers.
fn create_test_peers<Req: RLMessage, Res: RLMessage>(
    num_peers: NonZeroUsize,
    network_config: Option<NetworkConfig>,
) -> (TestPeer<Req, Res>, Vec<TestPeer<Req, Res>>, TaskManager) {
    let network_config = network_config.unwrap_or_default();

    let all_nodes = CommitteeFixture::builder(MemDatabase::default)
        .committee_size(num_peers)
        .with_network_config(network_config)
        .build();
    let authorities = all_nodes.authorities();
    let task_manager = TaskManager::default();
    let mut peers: Vec<_> = authorities
        .map(|a| {
            let config = a.consensus_config();
            let (tx, network_events) = mpsc::channel(10);
            let network_key = config.key_config().primary_network_keypair().clone();
            let db = MemDatabase::default();
            let metrics = Arc::new(NetworkMetrics::default());
            let network = ConsensusNetwork::<
                Req,
                Res,
                MemDatabase,
                mpsc::Sender<NetworkEvent<Req, Res>>,
            >::new(
                config.network_config(),
                tx,
                config.key_config().clone(),
                network_key,
                db,
                task_manager.get_spawner(),
                KadStoreType::Primary,
                config.primary_address(),
                metrics.clone(),
            )
            .expect("peer1 network created");

            let network_handle = network.network_handle();
            TestPeer {
                config,
                _network_events: network_events,
                network_handle,
                network: Some(network),
                network_metrics: metrics,
            }
        })
        .collect();

    let target = peers.remove(0);

    // return task manager to prevent drop
    (target, peers, task_manager)
}

/// A peer on RL
struct TestPeer<Req, Res, DB = MemDatabase>
where
    Req: RLMessage,
    Res: RLMessage,
{
    /// Peer's node config.
    config: ConsensusConfig<DB>,
    /// Receiver for network events.
    _network_events: mpsc::Receiver<NetworkEvent<Req, Res>>,
    /// Network handle to send commands.
    network_handle: NetworkHandle<Req, Res>,
    /// The network task.
    #[allow(clippy::type_complexity)]
    network: Option<ConsensusNetwork<Req, Res, MemDatabase, mpsc::Sender<NetworkEvent<Req, Res>>>>,
    /// Network metrics shared with the spawned task.
    network_metrics: Arc<NetworkMetrics>,
}
/// A peer on RL
struct NetworkPeer<Req, Res, DB = MemDatabase>
where
    Req: RLMessage,
    Res: RLMessage,
{
    /// Peer's node config.
    config: ConsensusConfig<DB>,
    /// Receiver for network events.
    network_events: mpsc::Receiver<NetworkEvent<Req, Res>>,
    /// Network handle to send commands.
    network_handle: NetworkHandle<Req, Res>,
    /// The network task.
    network: ConsensusNetwork<Req, Res, MemDatabase, mpsc::Sender<NetworkEvent<Req, Res>>>,
    /// Network metrics shared with the spawned task.
    network_metrics: Arc<NetworkMetrics>,
}

/// The type for holding testng components.
struct TestTypes<Req, Res, DB = MemDatabase>
where
    Req: RLMessage,
    Res: RLMessage,
{
    /// The first authority in the committee.
    peer1: NetworkPeer<Req, Res, DB>,
    /// The second authority in the committee.
    peer2: NetworkPeer<Req, Res, DB>,
    /// The owned task manager to prevent dropping.
    _task_manager: TaskManager,
}

/// Helper function to create an instance of [RequestHandler] for the first authority in the
/// committee.
fn create_test_types<Req, Res>() -> TestTypes<Req, Res>
where
    Req: RLMessage,
    Res: RLMessage,
{
    // custom network config with short heartbeat interval for peer manager
    let mut network_config = NetworkConfig::default();
    network_config.peer_config_mut().heartbeat_interval = TEST_HEARTBEAT_INTERVAL;

    let all_nodes =
        CommitteeFixture::builder(MemDatabase::default).with_network_config(network_config).build();
    let mut authorities = all_nodes.authorities();
    let authority_1 = authorities.next().expect("first authority");
    let authority_2 = authorities.next().expect("second authority");
    let config_1 = authority_1.consensus_config();
    let config_2 = authority_2.consensus_config();
    let (tx1, network_events_1) = mpsc::channel(10);
    let (tx2, network_events_2) = mpsc::channel(10);
    let task_manager = TaskManager::default();
    let metrics_1 = Arc::new(NetworkMetrics::default());
    let metrics_2 = Arc::new(NetworkMetrics::default());

    // peer1
    let network_key_1 = config_1.key_config().primary_network_keypair().clone();
    let peer1_network =
        ConsensusNetwork::<Req, Res, MemDatabase, mpsc::Sender<NetworkEvent<Req, Res>>>::new(
            config_1.network_config(),
            tx1,
            config_1.key_config().clone(),
            network_key_1,
            MemDatabase::default(),
            task_manager.get_spawner(),
            KadStoreType::Primary,
            config_1.primary_address(),
            metrics_1.clone(),
        )
        .expect("peer1 network created");
    let network_handle_1 = peer1_network.network_handle();
    let peer1 = NetworkPeer {
        config: config_1,
        network_events: network_events_1,
        network_handle: network_handle_1,
        network: peer1_network,
        network_metrics: metrics_1,
    };

    // peer2
    let network_key_2 = config_2.key_config().primary_network_keypair().clone();
    let peer2_network =
        ConsensusNetwork::<Req, Res, MemDatabase, mpsc::Sender<NetworkEvent<Req, Res>>>::new(
            config_2.network_config(),
            tx2,
            config_2.key_config().clone(),
            network_key_2,
            MemDatabase::default(),
            task_manager.get_spawner(),
            KadStoreType::Primary,
            config_2.primary_address(),
            metrics_2.clone(),
        )
        .expect("peer2 network created");
    let network_handle_2 = peer2_network.network_handle();
    let peer2 = NetworkPeer {
        config: config_2,
        network_events: network_events_2,
        network_handle: network_handle_2,
        network: peer2_network,
        network_metrics: metrics_2,
    };

    TestTypes { peer1, peer2, _task_manager: task_manager }
}

#[tokio::test]
async fn test_valid_req_restt() -> eyre::Result<()> {
    // start honest peer1 network
    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let NetworkPeer { config: config_1, network_handle: peer1, network, .. } = peer1;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // start honest peer2 network
    let NetworkPeer {
        config: config_2,
        network_handle: peer2,
        network_events: mut network_events_2,
        network,
        ..
    } = peer2;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // start swarm listening on default any address
    peer1.start_listening(config_1.primary_address()).await?;
    peer2.start_listening(config_2.primary_address()).await?;

    let missing_block = fixture_batch_with_transactions(3).seal_slow();
    let digests = vec![missing_block.digest()];
    let batch_req = TestWorkerRequest::MissingBatches(digests);
    let batch_res = TestWorkerResponse::MissingBatches { batches: vec![missing_block] };

    // dial peer2
    peer1
        .add_explicit_peer(
            config_2.key_config().primary_public_key(),
            config_2.primary_networkkey(),
            config_2.primary_address(),
        )
        .await?;
    peer1.dial_by_bls(config_2.key_config().primary_public_key()).await?;

    // Wait a beat for peer2 to recieve peer1 bls key.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // send request and wait for response
    let max_time = Duration::from_secs(5);
    let response_from_peer =
        peer1.send_request(batch_req.clone(), config_2.key_config().primary_public_key()).await?;
    let event =
        timeout(max_time, network_events_2.recv()).await?.expect("first network event received");

    // expect network event
    if let NetworkEvent::Request { request, channel, .. } = event {
        assert_eq!(request, batch_req);

        // send response
        peer2.send_response(batch_res.clone(), channel).await?;
    } else {
        panic!("unexpected network event received");
    }

    // expect response
    let response = timeout(max_time, response_from_peer).await?.expect("outbound id recv")?;
    assert_eq!(response, batch_res);

    Ok(())
}

#[tokio::test]
async fn test_valid_req_res_connection_closed_cleanup() -> eyre::Result<()> {
    // start honest peer1 network
    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let NetworkPeer { config: config_1, network_handle: peer1, network, .. } = peer1;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // start honest peer2 network
    let NetworkPeer { config: config_2, network_handle: peer2, network, .. } = peer2;
    let peer2_network_task = tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // start swarm listening on default any address
    peer1.start_listening(config_1.primary_address()).await?;
    peer2.start_listening(config_2.primary_address()).await?;
    let peer2_id = peer2.local_peer_id().await?;

    let missing_block = fixture_batch_with_transactions(3).seal_slow();
    let digests = vec![missing_block.digest()];
    let batch_req = TestWorkerRequest::MissingBatches(digests);

    // dial peer2
    peer1
        .add_explicit_peer(
            config_2.key_config().primary_public_key(),
            config_2.primary_networkkey(),
            config_2.primary_address(),
        )
        .await?;
    peer1.dial_by_bls(config_2.key_config().primary_public_key()).await?;

    // expect no pending requests yet
    let count = peer1.get_pending_request_count().await?;
    assert_eq!(count, 0);

    // send request and wait for response
    let _reply = peer1.send_request_direct(batch_req.clone(), peer2_id).await?;

    // peer1 has a pending_request now
    let count = peer1.get_pending_request_count().await?;
    assert_eq!(count, 1);

    // another sanity check
    let connected_peers = peer1.connected_peer_ids().await?;
    assert_eq!(connected_peers.len(), 1);

    // simulate crashed peer 2
    peer2_network_task.abort();
    assert!(peer2_network_task.await.unwrap_err().is_cancelled());

    // allow peer1 to process disconnect
    tokio::time::sleep(Duration::from_millis(500)).await;

    // peer1 removes pending requests
    let count = peer1.get_pending_request_count().await?;
    assert_eq!(count, 0);

    Ok(())
}

#[tokio::test]
async fn test_valid_req_res_inbound_failure() -> eyre::Result<()> {
    // start honest peer1 network
    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let NetworkPeer { config: config_1, network_handle: peer1, network, .. } = peer1;
    let peer1_network_task = tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // start honest peer2 network
    let NetworkPeer {
        config: config_2,
        network_handle: peer2,
        network_events: mut network_events_2,
        network,
        ..
    } = peer2;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // start swarm listening on default any address
    peer1.start_listening(config_1.primary_address()).await?;
    peer2.start_listening(config_2.primary_address()).await?;
    let peer2_id = peer2.local_peer_id().await?;

    let missing_block = fixture_batch_with_transactions(3).seal_slow();
    let digests = vec![missing_block.digest()];
    let batch_req = TestWorkerRequest::MissingBatches(digests);

    // dial peer2
    peer1
        .add_explicit_peer(
            config_2.key_config().primary_public_key(),
            config_2.primary_networkkey(),
            config_2.primary_address(),
        )
        .await?;
    peer1.dial_by_bls(config_2.key_config().primary_public_key()).await?;

    // expect no pending requests yet
    let count = peer1.get_pending_request_count().await?;
    assert_eq!(count, 0);

    // send request and wait for response
    let max_time = Duration::from_secs(5);
    let _response = peer1.send_request_direct(batch_req.clone(), peer2_id).await?;

    // peer1 has a pending_request now
    let count = peer1.get_pending_request_count().await?;
    assert_eq!(count, 1);

    // another sanity check
    let connected_peers = peer1.connected_peer_ids().await?;
    assert_eq!(connected_peers.len(), 1);

    // wait for peer2 to receive req
    let event =
        timeout(max_time, network_events_2.recv()).await?.expect("first network event received");

    // expect network event
    if let NetworkEvent::Request { request, cancel, .. } = event {
        assert_eq!(request, batch_req);

        // peer 1 crashes after making request
        peer1_network_task.abort();
        assert!(peer1_network_task.await.unwrap_err().is_cancelled());

        tokio::task::yield_now().await;
        timeout(Duration::from_secs(2), cancel).await?.expect("first network event received");
        assert_matches!((), ());
    } else {
        panic!("unexpected network event received");
    }

    // InboundFailure::Io(Kind(UnexpectedEof))
    Ok(())
}

#[tokio::test]
async fn test_outbound_failure_malicious_request() -> eyre::Result<()> {
    // start malicious peer1 network
    //
    // although these are valid req/res types, they are incorrect for the honest peer's
    // "worker" network
    let TestTypes { peer1, .. } = create_test_types::<TestPrimaryRequest, TestPrimaryResponse>();
    let NetworkPeer { config: config_1, network_handle: malicious_peer, network, .. } = peer1;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // start honest peer2 network
    let TestTypes { peer2, .. } = create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let NetworkPeer { config: config_2, network_handle: honest_peer, network, .. } = peer2;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // start swarm listening on default any address
    malicious_peer.start_listening(config_1.primary_address()).await?;
    honest_peer.start_listening(config_2.primary_address()).await?;

    let malicious_peer_id = malicious_peer.local_peer_id().await?;
    let honest_peer_id = honest_peer.local_peer_id().await?;
    let honest_peer_addr = config_2.primary_address();
    let honest_peer_net = config_2.primary_networkkey();

    // this type already impl `RLMessage` but this could be incorrect message type
    let malicious_msg = TestPrimaryRequest::Vote {
        header: Header::default(),
        parents: vec![Certificate::default()],
    };

    // dial honest peer
    let honest_bls = config_2.key_config().primary_public_key();
    malicious_peer.add_explicit_peer(honest_bls, honest_peer_net, honest_peer_addr.clone()).await?;
    malicious_peer.dial_by_bls(honest_bls).await?;

    // sleep for heartbeat
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL)).await;

    let peer_score_before_msg = honest_peer.peer_score(malicious_peer_id).await?.unwrap();

    // honest peer returns `OutboundFailure` error
    let response_from_peer = malicious_peer.send_request(malicious_msg, honest_bls).await?;
    let res = timeout(Duration::from_secs(2), response_from_peer)
        .await?
        .expect("first network event received");

    assert_matches!(res, Err(NetworkError::Outbound(_)));

    // Allow time for penalty to be applied
    tokio::time::sleep(Duration::from_millis(500)).await;

    // TODO: the honest peer penalize the malicious requestor. see Issue #250
    //
    // assert honest peer's score is lower - penalties are applied immediately
    // however, it should be the case that honest peer penalizes the malicious peer
    let peer_score_after_msg = malicious_peer.peer_score(honest_peer_id).await?.unwrap();
    assert!(peer_score_before_msg > peer_score_after_msg);

    Ok(())
}

#[tokio::test]
async fn test_outbound_failure_malicious_response() -> eyre::Result<()> {
    // honest peer 1
    let TestTypes { peer1, .. } = create_test_types::<TestPrimaryRequest, TestPrimaryResponse>();
    let NetworkPeer { config: config_1, network_handle: honest_peer, network, .. } = peer1;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // malicious peer2
    //
    // although these are honest req/res types, they are incorrect for the honest peer's
    // "primary" network this allows the network to receive "correct" messages and
    // respond with bad messages
    let TestTypes { peer2, .. } = create_test_types::<TestPrimaryRequest, TestWorkerResponse>();
    let NetworkPeer {
        config: config_2,
        network_handle: malicious_peer,
        network,
        network_events: mut network_events_2,
        ..
    } = peer2;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // start swarm listening on default any address
    honest_peer.start_listening(config_1.primary_address()).await?;
    malicious_peer.start_listening(config_2.primary_address()).await?;
    let malicious_peer_id = malicious_peer.local_peer_id().await?;
    let malicious_peer_addr =
        malicious_peer.listeners().await?.first().expect("malicious_peer listen addr").clone();

    // dial malicious_peer
    let mal_bls = config_2.key_config().primary_public_key();
    honest_peer
        .add_explicit_peer(mal_bls, config_2.primary_networkkey(), malicious_peer_addr.clone())
        .await?;
    honest_peer.dial_by_bls(mal_bls).await?;
    // Wait a beat for malicious to recieve honest's bls key.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // send request and wait for malicious response
    let max_time = Duration::from_secs(2);
    let honest_req = TestPrimaryRequest::Vote {
        header: Header::default(),
        parents: vec![Certificate::default()],
    };
    let response_from_peer =
        honest_peer.send_request_direct(honest_req.clone(), malicious_peer_id).await?;
    let event =
        timeout(max_time, network_events_2.recv()).await?.expect("first network event received");

    // expect network event
    if let NetworkEvent::Request { request, channel, .. } = event {
        assert_eq!(request, honest_req);
        // send response
        let block = fixture_batch_with_transactions(1).seal_slow();
        let malicious_reply = TestWorkerResponse::MissingBatches { batches: vec![block] };
        malicious_peer.send_response(malicious_reply, channel).await?;
    } else {
        panic!("unexpected network event received");
    }

    // expect response
    let res = timeout(max_time, response_from_peer).await?.expect("response received within time");

    // OutboundFailure::Io(Custom { kind: Other, error: Custom("Invalid value was given to the
    // function") })
    assert_matches!(res, Err(NetworkError::Outbound(_)));

    Ok(())
}

#[tokio::test]
async fn test_publish_to_one_peer() -> eyre::Result<()> {
    // start honest cvv network
    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let NetworkPeer { config: config_1, network_handle: cvv, network, .. } = peer1;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // start honest nvv network
    let NetworkPeer {
        config: config_2,
        network_handle: nvv,
        network_events: mut nvv_network_events,
        network,
        ..
    } = peer2;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // start swarm listening on default any address
    cvv.start_listening(config_1.primary_address()).await?;
    nvv.start_listening(config_2.primary_address()).await?;
    let cvv_addr = cvv.listeners().await?.first().expect("peer2 listen addr").clone();

    // subscribe
    nvv.subscribe_with_publishers(TEST_TOPIC.into(), config_1.committee_pub_keys()).await?;

    // dial cvv
    nvv.add_trusted_peer_and_dial(
        config_1.key_config().primary_public_key(),
        config_1.key_config().primary_network_public_key(),
        cvv_addr,
    )
    .await?;

    // publish random block
    let random_block = fixture_batch_with_transactions(10);
    let sealed_block = random_block.seal_slow();
    let expected_result = Vec::from(&sealed_block);

    // sleep for gossip connection time lapse
    tokio::time::sleep(Duration::from_millis(500)).await;

    // publish on wrong topic - no peers
    let expected_failure = cvv.publish("WRONG_TOPIC".into(), expected_result.clone()).await;
    assert!(expected_failure.is_err());

    // publish correct message and wait to receive
    let _message_id = cvv.publish(TEST_TOPIC.into(), expected_result.clone()).await?;
    let event =
        timeout(Duration::from_secs(2), nvv_network_events.recv()).await?.expect("batch received");

    // assert gossip message
    if let NetworkEvent::Gossip(msg, _) = event {
        assert_eq!(msg.data, expected_result);
    } else {
        panic!("unexpected network event received");
    }

    Ok(())
}

#[tokio::test]
async fn test_msg_verification_ignores_unauthorized_publisher() -> eyre::Result<()> {
    // start honest cvv network
    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let NetworkPeer { config: config_1, network_handle: cvv, network, .. } = peer1;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // start honest nvv network
    let NetworkPeer {
        config: config_2,
        network_handle: nvv,
        network_events: mut nvv_network_events,
        network,
        ..
    } = peer2;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // start swarm listening on default any address
    cvv.start_listening(config_1.primary_address()).await?;
    nvv.start_listening(config_2.primary_address()).await?;

    let target_peer_bls = config_1.key_config().primary_public_key();
    let target_peer_net = config_1.primary_networkkey();
    let cvv_id: PeerId = target_peer_net.clone().into();
    let target_addr = config_1.primary_address();
    nvv.add_explicit_peer(target_peer_bls, target_peer_net, target_addr).await?;
    // subscribe
    nvv.subscribe_with_publishers(TEST_TOPIC.into(), config_1.committee_pub_keys()).await?;

    // dial cvv
    nvv.dial_by_bls(target_peer_bls).await?;

    // publish random block
    let random_block = fixture_batch_with_transactions(10);
    let sealed_block = random_block.seal_slow();
    let expected_result = Vec::from(&sealed_block);

    // sleep for gossip connection time lapse
    tokio::time::sleep(Duration::from_millis(500)).await;

    // publish correct message and wait to receive
    let _message_id = cvv.publish(TEST_TOPIC.into(), expected_result.clone()).await?;
    let event =
        timeout(Duration::from_secs(2), nvv_network_events.recv()).await?.expect("batch received");

    // assert gossip message
    if let NetworkEvent::Gossip(msg, _) = event {
        assert_eq!(msg.data, expected_result);
    } else {
        panic!("unexpected network event received");
    }

    // remove cvv from whitelist and try to publish again
    nvv.update_authorized_publishers(HashMap::new()).await?;

    let random_block = fixture_batch_with_transactions(10);
    let sealed_block = random_block.seal_slow();
    let expected_result = Vec::from(&sealed_block);
    let _message_id = cvv.publish(TEST_TOPIC.into(), expected_result.clone()).await?;

    // message should never be forwarded
    let timeout = timeout(Duration::from_secs(2), nvv_network_events.recv()).await;
    assert!(timeout.is_err());

    // assert fatal score
    let score = nvv.peer_score(cvv_id).await?;
    assert_eq!(score, Some(config_2.network_config().peer_config().score_config.min_score));

    Ok(())
}

/// Test peer exchanges when too many peers connect
// #[tokio::test]
// async fn test_peer_exchange_with_excess_peers() -> eyre::Result<()> {
//     rayls_infrastructure_types::test_utils::init_test_tracing();
//     // Create a custom config with very low peer limits for testing
//     let target = NonZeroUsize::new(5).unwrap();
//     let mut network_config = NetworkConfig::default();
//     network_config.peer_config_mut().target_num_peers = 5; // entire committee + 1
//     network_config.peer_config_mut().peer_excess_factor = 0.1;
//     network_config.peer_config_mut().excess_peers_reconnection_timeout = Duration::from_secs(10);
//     network_config.peer_config_mut().heartbeat_interval = TEST_HEARTBEAT_INTERVAL;
//     network_config.libp2p_config_mut().k_bucket_size = target;

//     // Set up peers with the custom config
//     let (mut target_peer, mut other_peers, _) = create_test_peers::<
//         TestWorkerRequest,
//         TestWorkerResponse,
//     >(target, Some(network_config.clone()));

//     // spawn target network
//     let target_network = target_peer.network.take().expect("target network is some");
//     let target_id = target_peer.config.authority().as_ref().expect("authority").id();
//     tokio::spawn(async move {
//         let res = target_network.run().await;
//         debug!(target: "network", ?target_id, ?res, "target network shutdown");
//     });

//     // Start target peer listening
//     target_peer.network_handle.start_listening(target_peer.config.primary_address()).await?;
//     let target_addr = target_peer.config.primary_address();
//     let target_peer_id = target_peer.network_handle.local_peer_id().await?;
//     let target_peer_bls = target_peer.config.key_config().primary_public_key();
//     let target_peer_net = target_peer.config.primary_networkkey();

//     debug!(target: "network", ?target_peer_id, ?target_peer_bls, "target peer started");

//     // start and connect the first few peers (more than target_num_peers)
//     for peer in other_peers.iter_mut() {
//         // spawn peer network
//         let peer_network = peer.network.take().expect("peer network is some");
//         let id = peer.config.authority().as_ref().expect("authority").id();
//         tokio::spawn(async move {
//             let res = peer_network.run().await;
//             debug!(target: "network", ?id, ?res, "network shutdown");
//         });

//         peer.network_handle.start_listening(peer.config.primary_address()).await?;

//         // No kademilia so need to add peers on both side explicitly.
//         peer.network_handle
//             .add_explicit_peer(target_peer_bls, target_peer_net.clone(), target_addr.clone())
//             .await?;
//         target_peer
//             .network_handle
//             .add_explicit_peer(
//                 peer.config.key_config().primary_public_key(),
//                 peer.config.primary_networkkey(),
//                 peer.config.primary_address(),
//             )
//             .await?;

//         // subscribe to topic
//         peer.network_handle
//             .subscribe_with_publishers(TEST_TOPIC.into(), peer.config.committee_pub_keys())
//             .await?;

//         // connect to target
//         peer.network_handle.dial_by_bls(target_peer_bls).await?;

//         // give time for connection to establish
//         tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL * 2)).await;
//     }

//     // allow heartbeat to trigger peer pruning
//     tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL)).await;

//     // check that target has limited peers
//     let connected_peers = target_peer.network_handle.connected_peer_ids().await?;
//     debug!(target: "network", count=connected_peers.len(), "target connected peers after
// pruning");

//     // Should have at most max_peers connected
//     let max_peers = network_config.peer_config().max_peers();
//     assert!(
//         connected_peers.len() <= max_peers,
//         "Target should have at most {} peers, has {}",
//         max_peers,
//         connected_peers.len()
//     );

//     // create a new non-validator peer
//     let TestTypes { peer1: nvv_peer, peer2, .. } =
//         create_test_types::<TestWorkerRequest, TestWorkerResponse>();

//     let NetworkPeer {
//         config: peer2_config,
//         network_handle: peer2,
//         network: peer2_network,
//         network_events: _,
//     } = peer2;

//     // connect peer2
//     tokio::spawn(async move {
//         peer2_network.run().await.expect("nvv network run failed!");
//     });

//     peer2.start_listening(peer2_config.primary_address()).await?;

//     // add peers to each other's known peers
//     // add target as a bootstrap nodes
//     peer2.add_explicit_peer(target_peer_bls, target_peer_net.clone(),
// target_addr.clone()).await?;

//     // subscribe to topic for gossip
//     peer2
//         .subscribe_with_publishers(TEST_TOPIC.into(),
// vec![target_peer_bls].into_iter().collect())         .await?;

//     // connect to target
//     peer2.dial_by_bls(target_peer_bls).await?;

//     // give time for connection to establish
//     // target should be at max capacity
//     tokio::time::sleep(Duration::from_millis(200)).await;

//     // spawn nvv that goes through px
//     let NetworkPeer {
//         config: nvv_config,
//         network_handle: nvv,
//         network,
//         network_events: mut nvv_events,
//     } = nvv_peer;

//     tokio::spawn(async move {
//         network.run().await.expect("nvv network run failed!");
//     });

//     nvv.start_listening(nvv_config.primary_address()).await?;
//     let nvv_peer_id = nvv.local_peer_id().await?;
//     let nvv_peer_bls = nvv_config.key_config().primary_public_key();

//     debug!(target: "network", ?nvv_peer_id, ?nvv_peer_bls, "nvv peer started");

//     // add target as bootstrap peer
//     nvv.add_explicit_peer(target_peer_bls, target_peer_net, target_addr.clone()).await?;
//     // subscribe with target as authorized publisher

//     nvv.subscribe_with_publishers(TEST_TOPIC.into(), vec![target_peer_bls].into_iter().collect())
//         .await?;

//     // connect nvv to target (which already has too many peers)
//     nvv.dial_by_bls(target_peer_bls).await?;

//     // allow time for kademlia records to propagate
//     tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL * 10)).await;

//     // assert target is disconnected from nvv
//     assert!(!target_peer
//         .network_handle
//         .connected_peers()
//         .await?
//         .contains(nvv_config.config().primary_bls_key()));

//     // assert nvv is disconnected from target
//     let connected = nvv.connected_peer_ids().await?;
//     error!(target: "network", ?connected, "nvv connected peers");
//     assert!(!connected.contains(&target_peer_id));

//     // publish from target
//     let random_block = fixture_batch_with_transactions(10);
//     let sealed_block = random_block.seal_slow();
//     let expected_msg = Vec::from(&sealed_block);

//     target_peer.network_handle.publish(TEST_TOPIC.into(), expected_msg.clone()).await?;

//     // check if nvv receives the gossip (either directly or through mesh)
//     let mut received = false;
//     let timeout = Duration::from_secs(10);
//     let start = tokio::time::Instant::now();

//     while !received && start.elapsed() < timeout {
//         match tokio::time::timeout(Duration::from_millis(2500), nvv_events.recv()).await {
//             Ok(Some(NetworkEvent::Gossip(msg, from))) => {
//                 assert_eq!(msg.data, expected_msg, "Gossip message data mismatch");
//                 debug!(target: "network", ?from, "nvv received gossip from peer");
//                 received = true;
//             }
//             Ok(Some(NetworkEvent::Request {
//                 request: TestWorkerRequest::PeerExchange(_), ..
//             })) => {
//                 // Ignore additional peer exchanges
//                 continue;
//             }
//             Ok(Some(other)) => {
//                 debug!(target: "network", ?other, "nvv received other event");
//             }
//             _ => {}
//         }
//     }

//     assert!(received, "nvv MUST receive gossip message through mesh propagation");

//     Ok(())
// }

#[tokio::test(flavor = "multi_thread")]
async fn test_score_decay_and_reconnection() -> eyre::Result<()> {
    // Create a custom config with short halflife for quicker testing
    let mut network_config = NetworkConfig::default();
    network_config.peer_config_mut().score_config.score_halflife = 0.1;
    network_config.peer_config_mut().heartbeat_interval = TEST_HEARTBEAT_INTERVAL;
    let default_score = network_config.peer_config_mut().score_config.default_score;

    // Set up multiple peers with the custom config
    let (peer1, mut other_peers, _) = create_test_peers::<TestWorkerRequest, TestWorkerResponse>(
        NonZeroUsize::new(4).unwrap(),
        Some(network_config.clone()),
    );

    let peer2 = other_peers.remove(0);
    let TestPeer { config: config_1, network_handle: peer1, network, .. } = peer1;
    tokio::spawn(async move {
        network.expect("peer1 network available").run().await.expect("network run failed!");
    });

    let TestPeer { config: config_2, network_handle: peer2, network, .. } = peer2;
    tokio::spawn(async move {
        network.expect("peer2 network available").run().await.expect("network run failed!");
    });

    // Start listeners and establish connection
    peer1.start_listening(config_1.primary_address()).await?;
    peer2.start_listening(config_2.primary_address()).await?;

    let peer2_id: PeerId = config_2.primary_networkkey().into();
    let peer2_bls = config_2.key_config().primary_public_key();

    peer1
        .add_explicit_peer(peer2_bls, config_2.primary_networkkey(), config_2.primary_address())
        .await?;

    // Connect peers
    peer1.dial_by_bls(peer2_bls).await?;

    // Wait a beat for peer2 to recieve peer1 bls key.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify connection established
    let connected_peers = peer1.connected_peer_ids().await?;
    assert!(connected_peers.contains(&peer2_id), "Peer2 should be connected");

    // Apply medium penalties to lower score but not ban
    for _ in 0..3 {
        peer1.report_penalty(peer2_bls, Penalty::Medium).await;
    }

    // Check peer2's score is lower but still connected
    let score_after_penalty = peer1.peer_score(peer2_id).await?.unwrap();
    assert!(
        score_after_penalty < default_score,
        "{score_after_penalty} not less than {default_score}"
    );

    // Wait for scores to recover through heartbeats
    tokio::time::sleep(Duration::from_secs(5 * TEST_HEARTBEAT_INTERVAL)).await;

    // Check score improved
    let score_after_decay = peer1.peer_score(peer2_id).await?.unwrap();
    assert!(score_after_decay > score_after_penalty);

    // Peer should still be connected
    let connected_peers = peer1.connected_peer_ids().await?;
    assert!(
        connected_peers.contains(&peer2_id),
        "Peer2 should still be connected after score recovery"
    );

    Ok(())
}

#[tokio::test]
async fn test_banned_peer_reconnection_attempt() -> eyre::Result<()> {
    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();

    let NetworkPeer { config: config_1, network_handle: honest_peer, network, .. } = peer1;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    let NetworkPeer { config: config_2, network_handle: malicious_peer, network, .. } = peer2;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // Start listeners
    honest_peer.start_listening(config_1.primary_address()).await?;
    malicious_peer.start_listening(config_2.primary_address()).await?;

    let malicious_id: PeerId = config_2.primary_networkkey().into();
    let malicious_bls = config_2.key_config().primary_public_key();

    let malicious_addr = config_2.primary_address();

    // Connect malicious to honest
    malicious_peer
        .add_explicit_peer(
            config_1.key_config().primary_public_key(),
            config_1.primary_networkkey(),
            config_1.primary_address(),
        )
        .await?;
    malicious_peer.dial_by_bls(config_1.key_config().primary_public_key()).await?;

    // Wait for connection to establish
    tokio::time::sleep(Duration::from_millis(100)).await;

    debug!(target: "peer-manager", ?malicious_id, ?malicious_bls, "assessing fatal penalty!!");
    // Report fatal penalty for malicious peer
    honest_peer.report_penalty(malicious_bls, Penalty::Fatal).await;

    // Wait for ban to take effect and disconnect
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL)).await;

    // Verify malicious peer is disconnected
    let connected_peers = honest_peer.connected_peer_ids().await?;
    assert!(!connected_peers.contains(&malicious_id), "Malicious peer should be disconnected");

    // Verify peer is banned
    let score = honest_peer.peer_score(malicious_id).await?.unwrap();
    let min_score = config_1.network_config().peer_config().score_config.min_score_before_ban;
    assert!(score <= min_score, "Peer should have ban-level score");

    // Now try to reconnect from malicious peer
    let honest_bls = config_1.key_config().primary_public_key();
    malicious_peer
        .add_explicit_peer(honest_bls, config_1.primary_networkkey(), config_1.primary_address())
        .await?;
    let dial_result = malicious_peer.dial_by_bls(honest_bls).await;

    // The dial command should succeed at the API level (the swarm will try to dial)
    assert!(dial_result.is_ok());

    // Wait a moment to see if connection is rejected
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify connection is rejected
    let connected_peers = honest_peer.connected_peer_ids().await?;
    assert!(
        !connected_peers.contains(&malicious_id),
        "Banned peer should not be allowed to reconnect"
    );

    // Try direct connection from honest to malicious (should also be refused due to banned state)
    assert!(honest_peer.dial(malicious_id, malicious_addr).await.is_err());

    // Wait a moment
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify connection still not established
    let connected_peers = honest_peer.connected_peer_ids().await?;
    assert!(
        !connected_peers.contains(&malicious_id),
        "Honest peer should not connect to banned peer"
    );

    Ok(())
}

#[tokio::test]
async fn test_dial_timeout_behavior() -> eyre::Result<()> {
    let mut network_config = NetworkConfig::default();
    network_config.peer_config_mut().dial_timeout = Duration::from_millis(100);

    let (mut peer1, _others, _) = create_test_peers::<TestWorkerRequest, TestWorkerResponse>(
        NonZeroUsize::new(4).unwrap(),
        None,
    );
    let network = peer1.network.take().unwrap();
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // Start listener
    peer1.network_handle.start_listening(peer1.config.primary_address()).await?;

    // Create a peer ID that doesn't exist
    let nonexistent_peer = PeerId::random();

    // Create a valid but unreachable multiaddr (use a random high port)
    let unreachable_addr = create_multiaddr(None);

    let (tx, rx) = oneshot::channel();
    let handle = peer1.network_handle.clone();
    tokio::spawn(async move {
        // Start dial attempt
        let dial_result = handle.dial(nonexistent_peer, unreachable_addr).await;
        let _ = tx.send(dial_result);
    });

    // Wait for dial timeout
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL * 2)).await;

    assert!(rx.await.unwrap().is_err());

    // Verify dialing peer has been cleaned up
    let connected_peers = peer1.network_handle.connected_peer_ids().await?;
    assert!(!connected_peers.contains(&nonexistent_peer), "Failed dial should be cleaned up");

    Ok(())
}

#[tokio::test]
async fn test_multi_peer_mesh_formation() -> eyre::Result<()> {
    // Create multiple peers but with more realistic constraints
    let num_peers = NonZeroUsize::new(4).unwrap();
    let mut network_config = NetworkConfig::default();

    // Use default peer limits but with a faster heartbeat for testing
    network_config.peer_config_mut().heartbeat_interval = TEST_HEARTBEAT_INTERVAL;

    // committee network
    let (mut target_peer, _committee, _) = create_test_peers::<TestWorkerRequest, TestWorkerResponse>(
        num_peers,
        Some(network_config.clone()),
    );
    // create other nvvs
    let (_, mut other_peers, _) =
        create_test_peers::<TestWorkerRequest, TestWorkerResponse>(num_peers, Some(network_config));

    // Start target peer
    let target_network = target_peer.network.take().expect("target network is some");
    tokio::spawn(async move {
        let _ = target_network.run().await;
    });

    // Start target peer listening
    target_peer.network_handle.start_listening(target_peer.config.primary_address()).await?;

    let target_bls = *target_peer.config.config().primary_bls_key();
    let target_addr = target_peer.config.primary_address();
    let target_net_key = target_peer.config.primary_networkkey();

    // Subscribe target to test topic
    target_peer
        .network_handle
        .subscribe_with_publishers(
            TEST_TOPIC.into(),
            other_peers.first().unwrap().config.committee_pub_keys(),
        )
        .await?;

    // Start other peers and connect them all to the target (star topology)
    for peer in other_peers.iter_mut() {
        // Start peer network
        let peer_network = peer.network.take().expect("peer network is some");
        tokio::spawn(async move {
            let _ = peer_network.run().await;
        });

        // Start listener
        peer.network_handle.start_listening(peer.config.primary_address()).await?;

        // Connect to target peer
        peer.network_handle
            .add_explicit_peer(target_bls, target_net_key.clone(), target_addr.clone())
            .await?;
        peer.network_handle.dial_by_bls(target_bls).await?;

        // Give time for connection to establish
        tokio::time::sleep(Duration::from_millis(50)).await;

        // subscribe to test topic with target peer as authorized publisher
        peer.network_handle
            .subscribe_with_publishers(TEST_TOPIC.into(), vec![target_bls].into_iter().collect())
            .await?;
    }

    // Wait for connections to stabilize
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL * 2)).await;

    // Verify all peers are connected to target
    let connected_peers = target_peer.network_handle.connected_peer_ids().await?;
    assert_eq!(connected_peers.len(), other_peers.len(), "All peers should be connected to target");

    // Check gossipsub mesh formation
    let mesh_peers = target_peer.network_handle.mesh_peers(TEST_TOPIC.into()).await?;
    debug!(target: "network", "Target has {} peers in its gossipsub mesh", mesh_peers.len());

    // Mesh formation takes time, we should have at least some peers in the mesh
    // Note: We can't guarantee all peers will be in the mesh due to gossipsub's internal behavior
    assert!(!mesh_peers.is_empty(), "Target should have at least some peers in its gossipsub mesh");

    // Test message propagation
    let test_data = Vec::from("test data for propagation".as_bytes());
    target_peer.network_handle.publish(TEST_TOPIC.into(), test_data.clone()).await?;

    // Wait for message propagation
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Check mesh connectivity through gossipsub stats
    let gossip_peers = target_peer.network_handle.all_peers().await?;
    debug!(target: "network", "Gossipsub knows about {} peers", gossip_peers.len());
    assert!(!gossip_peers.is_empty(), "Target's gossipsub should know about its peers");

    // For each peer, check if they're subscribed to the topic
    for (peer_id, topics) in gossip_peers {
        assert!(
            topics.iter().any(|t| *t == TopicHash::from_raw(TEST_TOPIC)),
            "Peer {peer_id:?} should be subscribed to test topic"
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_new_epoch_unbans_committee_members() -> eyre::Result<()> {
    // Start with two peers
    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let NetworkPeer { config: config_1, network_handle: peer1, network, .. } = peer1;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    let NetworkPeer { config: config_2, network_handle: peer2, network, .. } = peer2;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // Start swarm listening
    peer1.start_listening(config_1.primary_address()).await?;
    peer2.start_listening(config_2.primary_address()).await?;

    let peer2_id = peer2.local_peer_id().await?;
    let peer2_addr = peer2.listeners().await?.first().expect("peer2 listen addr").clone();

    // Connect peers
    peer1
        .add_explicit_peer(
            config_2.key_config().primary_public_key(),
            config_2.key_config().primary_network_public_key(),
            peer2_addr.clone(),
        )
        .await?;
    peer1.dial_by_bls(config_2.key_config().primary_public_key()).await?;

    // Wait for connection to establish
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify connection established
    let connected_peers = peer1.connected_peer_ids().await?;
    assert!(connected_peers.contains(&peer2_id), "Peer2 should be connected initially");

    // Apply fatal penalty to peer2 - should ban it
    peer1.report_penalty(config_2.key_config().primary_public_key(), Penalty::Fatal).await;

    // Wait for ban to take effect
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL)).await;

    // Verify peer2 is disconnected and banned
    let connected_peers = peer1.connected_peer_ids().await?;
    assert!(!connected_peers.contains(&peer2_id), "Peer2 should be disconnected after ban");

    let score = peer1.peer_score(peer2_id).await?.unwrap();
    let min_score = config_1.network_config().peer_config().score_config.min_score;
    assert_eq!(score, min_score, "Peer2 should have ban-level score");

    // Now simulate a new epoch where peer2 is in the committee
    let committee = vec![*config_2.authority().as_ref().expect("authority").protocol_key()]
        .into_iter()
        .collect();

    // Send NewEpoch command to peer1
    let handle = peer1.clone();
    tokio::spawn(async move {
        handle.new_epoch(committee).await.expect("Failed to send NewEpoch command");
    })
    .await?;

    // Wait for unban to take effect
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL)).await;

    // Verify peer2's score has improved and is no longer banned
    let score_after_epoch = peer1.peer_score(peer2_id).await?.unwrap();
    assert_eq!(
        score_after_epoch,
        config_1.network_config().peer_config().score_config.max_score,
        "Peer2 should have improved score after new epoch"
    );

    // peer2 should dial peer1 - but try dial to reconnecting peer2 and ignore `AlreadyConnectedErr`
    let _ = peer1.dial_by_bls(config_2.key_config().primary_public_key()).await;

    // Wait for connection to reestablish
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify connection reestablished
    let connected_peers_after = peer1.connected_peer_ids().await?;
    assert!(connected_peers_after.contains(&peer2_id), "Peer2 should be reconnected after unban");

    Ok(())
}

#[tokio::test]
async fn test_new_epoch_unbans_committee_member_ip() -> eyre::Result<()> {
    // Create multiple peers for this test
    let num_peers = NonZeroUsize::new(4).unwrap();
    let (mut target_peer, _, _) =
        create_test_peers::<TestWorkerRequest, TestWorkerResponse>(num_peers, None);

    // create new committee
    let (_, mut other_peers, _) =
        create_test_peers::<TestWorkerRequest, TestWorkerResponse>(num_peers, None);

    // Start target peer network
    let target_network = target_peer.network.take().expect("target network is some");
    tokio::spawn(async move {
        target_network.run().await.expect("network run failed!");
    });

    // Start listening
    target_peer.network_handle.start_listening(target_peer.config.primary_address()).await?;

    // Take peer1 and peer2 from other_peers
    let mut peer1 = other_peers.remove(0);
    let mut peer2 = other_peers.remove(0);

    // Start peer1 network
    let peer1_network = peer1.network.take().expect("peer1 network is some");
    let peer1_network_task = tokio::spawn(async move {
        peer1_network.run().await.expect("network run failed!");
    });

    peer1.network_handle.start_listening(peer1.config.primary_address()).await?;
    let peer1_id = peer1.network_handle.local_peer_id().await?;
    let peer1_addr = peer1.config.primary_address();

    // Start peer2 network - this will be our future committee member
    // Use the SAME multiaddr as peer1 to simulate same IP
    let peer2_network = peer2.network.take().expect("peer2 network is some");
    tokio::spawn(async move {
        peer2_network.run().await.expect("network run failed!");
    });

    // For peer2, multiaddr has the same IP as peer1 (127.0.0.1)
    let peer2_addr = peer2.config.primary_address();
    let peer2_id = peer2.network_handle.local_peer_id().await?;
    // join network so kad record is available
    peer2.network_handle.start_listening(peer2_addr.clone()).await?;
    peer2.network_handle.dial(peer1_id, peer1_addr.clone()).await?;

    // Connect target to peer1
    target_peer
        .network_handle
        .add_explicit_peer(
            peer1.config.key_config().primary_public_key(),
            peer1.config.primary_networkkey(),
            peer1.config.primary_address(),
        )
        .await?;
    target_peer.network_handle.dial_by_bls(peer1.config.key_config().primary_public_key()).await?;

    // Wait for connection to establish
    tokio::time::sleep(Duration::from_millis(TEST_HEARTBEAT_INTERVAL)).await;

    // Apply fatal penalty to peer1 - should ban it and its IP
    target_peer
        .network_handle
        .report_penalty(peer1.config.key_config().primary_public_key(), Penalty::Fatal)
        .await;

    // Wait for ban to take effect
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL)).await;

    // Verify peer1 is disconnected and banned
    let connected_peers = target_peer.network_handle.connected_peer_ids().await?;
    assert!(!connected_peers.contains(&peer1_id), "Peer1 should be disconnected after ban");

    // shutdown peer1 network
    peer1_network_task.abort();

    // allow os to make port available again
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL)).await;

    // Now simulate a new epoch where peer2 is in the committee with the same IP as banned peer1
    let committee = vec![*peer2.config.authority().as_ref().expect("authority").protocol_key()]
        .into_iter()
        .collect();

    target_peer.network_handle.new_epoch(committee).await?;

    // wait for connection to establish
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL)).await;

    // verify connection established with peer2
    let connected_peers_after = target_peer.network_handle.connected_peer_ids().await?;
    assert!(
        connected_peers_after.contains(&peer2_id),
        "Peer2 should be connected despite sharing IP with banned peer1"
    );

    Ok(())
}

#[tokio::test]
async fn test_new_epoch_handles_disconnecting_pending_ban() -> eyre::Result<()> {
    // Start with two peers
    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let NetworkPeer { config: config_1, network_handle: peer1, network, .. } = peer1;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    let NetworkPeer { config: config_2, network_handle: peer2, network, .. } = peer2;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // Start swarm listening
    peer1.start_listening(config_1.primary_address()).await?;
    peer2.start_listening(config_2.primary_address()).await?;

    let peer2_id = peer2.local_peer_id().await?;

    let peer2_bls = config_2.key_config().primary_public_key();
    peer1
        .add_explicit_peer(
            peer2_bls,
            config_2.key_config().primary_network_public_key(),
            config_2.primary_address(),
        )
        .await?;
    // Connect peers
    peer1.dial_by_bls(peer2_bls).await?;

    // Wait for connection to establish
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify connection established
    let connected_peers = peer1.connected_peer_ids().await?;
    assert!(connected_peers.contains(&peer2_id), "Peer2 should be connected initially");

    // Apply severe penalties to put peer in a disconnecting state pending ban
    // We need to apply penalties but not enough to cause immediate ban
    // First apply medium penalties
    for _ in 0..3 {
        peer1.report_penalty(peer2_bls, Penalty::Medium).await;
    }

    // Then apply a severe penalty - should trigger disconnect pending ban
    peer1.report_penalty(peer2_bls, Penalty::Severe).await;

    // Wait for disconnect to begin but not complete
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Now simulate a new epoch where peer2 is in the committee
    let committee = vec![*config_2.authority().as_ref().expect("authority").protocol_key()]
        .into_iter()
        .collect();

    // Send NewEpoch command to peer1
    let handle = peer1.clone();
    tokio::spawn(async move {
        handle.new_epoch(committee).await.expect("Failed to send NewEpoch command");
    })
    .await?;

    // Wait for epoch processing
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL)).await;

    // Verify peer2's score has improved and is trusted
    let score_after_epoch = peer1.peer_score(peer2_id).await?.unwrap();
    assert!(score_after_epoch > 0.0, "Peer2 should have a positive score after new epoch");

    // Try reconnecting peer2 if it was disconnected during the process
    if !peer1.connected_peer_ids().await?.contains(&peer2_id) {
        let dial_result = peer1.dial_by_bls(peer2_bls).await;
        assert!(dial_result.is_ok(), "Should be able to reconnect to peer2 after new epoch");

        // Wait for connection to reestablish
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Verify connection is established
    let connected_peers_after = peer1.connected_peer_ids().await?;
    assert!(connected_peers_after.contains(&peer2_id), "Peer2 should be connected after new epoch");

    Ok(())
}

/// Test kad records available to new node joining the network.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn test_get_kad_records() -> eyre::Result<()> {
    // used later
    let num_network_peers = 5;

    // Set up multiple peers with the custom config
    let (mut target_peer, mut committee, _) =
        create_test_peers::<TestWorkerRequest, TestWorkerResponse>(
            NonZeroUsize::new(num_network_peers).unwrap(),
            None,
        );

    // spawn target network
    let target_network = target_peer.network.take().expect("target network is some");
    let id = target_peer.config.authority().as_ref().expect("authority").id();
    let target_peer_bls = target_peer.config.key_config().primary_public_key();
    let target_peer_net = target_peer.config.primary_networkkey();
    tokio::spawn(async move {
        let res = target_network.run().await;
        debug!(target: "network", ?id, ?res, "network shutdown");
    });

    // Start target peer listening
    let target_addr = target_peer.config.primary_address();
    target_peer.network_handle.start_listening(target_addr.clone()).await?;
    let target_peer_id: PeerId =
        target_peer.config.config().node_info.primary_network_key().clone().into();

    let mut peer_mapping = vec![(target_peer_bls, target_peer_net.clone(), target_addr.clone())];
    // Start other peers and connect them one by one to the target
    for peer in committee.iter_mut() {
        // spawn peer network
        let peer_network = peer.network.take().expect("peer network is some");
        let id = peer.config.authority().as_ref().expect("authority").id();
        tokio::spawn(async move {
            let res = peer_network.run().await;
            debug!(target: "network", ?id, ?res, "network shutdown");
        });

        let peer_addr = peer.config.primary_address();
        peer.network_handle.start_listening(peer_addr).await?;

        peer_mapping.push((
            peer.config.key_config().primary_public_key(),
            peer.config.key_config().primary_network_public_key(),
            peer.config.config().node_info.primary_network_address().clone(),
        ));

        // Connect to target
        peer.network_handle
            .add_trusted_peer_and_dial(
                target_peer_bls,
                target_peer_net.clone(),
                target_addr.clone(),
            )
            .await?;

        // Give time for connection to establish
        tokio::time::sleep(Duration::from_millis(100)).await;

        peer.network_handle
            .subscribe_with_publishers(TEST_TOPIC.into(), peer.config.committee_pub_keys())
            .await?;
    }

    // Allow time for heartbeats to happen
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL)).await;

    // Check connected peers on target - should be limited based on config
    let connected_peers = target_peer.network_handle.connected_peer_ids().await?;

    // assert all peers connected (minus this node)
    assert_eq!(connected_peers.len(), num_network_peers - 1);

    // create non-validator peer
    let TestTypes { peer1, .. } = create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let NetworkPeer {
        config: nvv_config,
        network_handle: nvv,
        network,
        network_events: mut nvv_events,
        ..
    } = peer1;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    nvv.start_listening(nvv_config.primary_address()).await?;
    // give time for listener to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // connect to target
    nvv.add_trusted_peer_and_dial(target_peer_bls, target_peer_net.clone(), target_addr.clone())
        .await?;
    // subscribe to topic
    // add target peer as authorized publisher
    nvv.subscribe_with_publishers(
        TEST_TOPIC.into(),
        vec![*target_peer.config.authority().as_ref().expect("authority").protocol_key()]
            .into_iter()
            .collect(),
    )
    .await?;

    // give time for connection to establish
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL)).await;

    // find other committee members through kad
    let authorities: Vec<BlsPublicKey> =
        committee.iter().map(|peer| peer.config.key_config().primary_public_key()).collect();
    nvv.find_authorities(authorities.clone()).await?;

    // allow dial attempts to be made
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL * 5)).await;

    // assert nvv is connected with other peers
    let connected = nvv.connected_peer_ids().await?;
    debug!(target: "network", ?connected, "nvv connected peers");
    assert!(connected.contains(&target_peer_id));
    for peer in committee.iter() {
        let id = peer.network_handle.local_peer_id().await?;
        debug!(target: "network", ?id, "checking connection for peer");
        assert!(connected.contains(&id));
    }

    // publish random batch
    let random_block = fixture_batch_with_transactions(10);
    let sealed_block = random_block.seal_slow();
    let expected_msg = Vec::from(&sealed_block);

    // assert gossip from disconnected target peer is received by nvv
    target_peer.network_handle.publish(TEST_TOPIC.into(), expected_msg.clone()).await?;

    // wait for gossip from disconnected peer
    match timeout(Duration::from_secs(5), nvv_events.recv()).await {
        Ok(Some(NetworkEvent::Gossip(msg, _))) => {
            let GossipMessage { source, data, .. } = msg;
            assert_eq!(source, Some(target_peer_id));
            assert_eq!(data, expected_msg);
        }
        Ok(None) => return Err(eyre!("Channel closed without receiving event")),
        Err(_) => return Err(eyre!("Timeout waiting for peer exchange event")),
        e => return Err(eyre!("wrong event type: {:?}", e)),
    }

    Ok(())
}

#[tokio::test]
async fn test_node_record_validation() {
    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let network = peer1.network;

    // Create a kad::Record with correct publisher
    let mut peer_record = network.get_peer_record();
    assert!(network.peer_record_valid(&peer_record).is_some());
    assert!(peer2.network.peer_record_valid(&peer_record).is_some());

    // assert no publisher fails
    peer_record.publisher = None;
    // assert invalid peer record rejected with no publisher
    assert!(network.peer_record_valid(&peer_record).is_none());

    // assert publisher mismatch fails
    peer_record.publisher = Some(*peer2.network.swarm.local_peer_id());
    assert!(network.peer_record_valid(&peer_record).is_none());
}

#[tokio::test]
async fn test_newer_kad_record_replaced() -> eyre::Result<()> {
    let TestTypes { peer1, mut peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let mut network = peer1.network;
    let peer2_new_record = peer2.network.get_peer_record();
    // create valid peer2 record with old timestamp
    let mut peer2_info = peer2.network.node_record.info.clone();
    // timestamp in the past
    peer2_info.timestamp = now() - 10_000;
    // sign record
    let signature = peer2.config.key_config().request_signature_direct(&encode(&peer2_info));
    let old_record = NodeRecord { info: peer2_info, signature };
    // assert old record is valid
    let peer2_pubkey = peer2.config.key_config().primary_public_key();
    assert!(old_record.clone().verify(&peer2_pubkey).is_ok());
    // store with peer2 to generate old kad record
    peer2.network.node_record = old_record;
    let old_kad_record = peer2.network.get_peer_record();
    // put old record in store
    network.swarm.behaviour_mut().kademlia.store_mut().put(old_kad_record.clone())?;
    // assert kad store is old
    let store_record = network
        .swarm
        .behaviour_mut()
        .kademlia
        .store_mut()
        .get(&peer2_new_record.key)
        .expect("peer2 record in local kad store");
    assert_eq!(old_kad_record, *store_record);

    // process new put request with newer record
    network.process_kad_put_request(*peer2.network.swarm.local_peer_id(), peer2_new_record.clone());
    // assert kad store is updated
    let store_record = network
        .swarm
        .behaviour_mut()
        .kademlia
        .store_mut()
        .get(&peer2_new_record.key)
        .expect("peer2 record in local kad store");
    assert_eq!(*store_record, peer2_new_record);

    Ok(())
}

/// FIND-025 Path A: AddProvider store exhaustion must not propagate a fatal error.
///
/// Pre-fill the provider table to capacity, then verify that an overflow AddProvider
/// event returns Ok, does not store the record, and does not crash the node.
#[tokio::test]
async fn test_kad_provider_store_exhaustion_does_not_propagate_error() -> eyre::Result<()> {
    use libp2p::kad::store::MemoryStoreConfig;

    let TestTypes { peer1, .. } = create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let mut network = peer1.network;

    let max_provided_keys = MemoryStoreConfig::default().max_provided_keys;

    // fill provider store to capacity with unique keys
    let store = network.swarm.behaviour_mut().kademlia.store_mut();
    for i in 0..max_provided_keys {
        let key = RecordKey::new(&i.to_be_bytes());
        let record =
            ProviderRecord { key, provider: PeerId::random(), expires: None, addresses: vec![] };
        store.add_provider(record)?;
    }

    // build an overflow AddProvider event
    let overflow_key = RecordKey::new(&max_provided_keys.to_be_bytes());
    let overflow_peer = PeerId::random();
    let overflow_record = ProviderRecord {
        key: overflow_key.clone(),
        provider: overflow_peer,
        expires: None,
        addresses: vec![],
    };
    let event = kad::Event::InboundRequest {
        request: kad::InboundRequest::AddProvider { record: Some(overflow_record) },
    };

    // before the fix this returned Err(StoreKademliaRecord) causing node shutdown.
    // the fix catches the store error locally - must return Ok.
    let result = network.process_kad_event(event);
    assert!(result.is_ok(), "provider store overflow must not propagate fatal error");

    // verify the overflow record was not added
    let providers = network.swarm.behaviour_mut().kademlia.store_mut().providers(&overflow_key);
    assert!(providers.is_empty(), "overflow provider record must not be stored");

    Ok(())
}

/// A foreign inbound AddProvider record must not be stored.
///
/// Rayls discovers peers with `get_record` on the BLS key and never calls `get_providers`, so a
/// stored provider record is never read - it only grows an unbounded, never-queried table that
/// takes unsigned writes from any non-banned peer. The handler drops foreign provider records
/// rather than persist them.
#[tokio::test]
async fn test_kad_foreign_provider_record_is_not_stored() -> eyre::Result<()> {
    let TestTypes { peer1, .. } = create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let mut network = peer1.network;

    let key = RecordKey::new(b"foreign-provided-key");
    let record = ProviderRecord {
        key: key.clone(),
        provider: PeerId::random(),
        expires: None,
        addresses: vec![],
    };

    let event = kad::Event::InboundRequest {
        request: kad::InboundRequest::AddProvider { record: Some(record) },
    };

    let result = network.process_kad_event(event);
    assert!(result.is_ok(), "processing an inbound provider record must not error");

    let providers = network.swarm.behaviour_mut().kademlia.store_mut().providers(&key);
    assert!(providers.is_empty(), "foreign provider record must not be stored");

    Ok(())
}

/// A peer-exchange disconnect whose request fails must not orphan its tracked ack.
///
/// A PX disconnect is recorded in both `pending_px_disconnects` and `outbound_requests`. The
/// expected outcome is an `OutboundFailure` (the node is dropping the peer), and that arm must
/// clear the entry from BOTH maps - otherwise the `outbound_requests` ack lingers until the
/// periodic sweep, one leaked entry per PX-disconnect.
#[tokio::test]
async fn test_px_disconnect_failure_clears_outbound_request() -> eyre::Result<()> {
    use crate::peers::{PeerEvent, PeerExchangeMap};
    use libp2p::{
        request_response::{Event as ReqResEvent, OutboundFailure},
        swarm::ConnectionId,
    };

    let TestTypes { peer1, .. } = create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let mut network = peer1.network;

    let peer_id = PeerId::random();

    // drive a PX disconnect: inserts into pending_px_disconnects AND outbound_requests
    network.process_peer_manager_event(PeerEvent::DisconnectPeerX(
        peer_id,
        PeerExchangeMap(std::collections::HashMap::new()),
    ))?;

    let request_id = *network
        .pending_px_disconnects
        .keys()
        .next()
        .expect("px disconnect registered a pending request");
    assert!(
        network.outbound_requests.contains_key(&(peer_id, request_id)),
        "precondition: px request is tracked in outbound_requests"
    );

    // the px request fails - the expected path, since the node is dropping the peer
    let event = ReqResEvent::OutboundFailure {
        peer: peer_id,
        request_id,
        error: OutboundFailure::ConnectionClosed,
        connection_id: ConnectionId::new_unchecked(0),
    };
    network.process_reqres_event(event)?;

    assert!(
        !network.outbound_requests.contains_key(&(peer_id, request_id)),
        "px outbound failure must clear the tracked ack, not orphan it"
    );

    Ok(())
}

/// FIND-025 Path B: PutRecord store exhaustion must not propagate a fatal error.
///
/// Pre-fill the record store to capacity, then submit a valid BLS-signed PutRecord.
/// Verify the event returns Ok, the record is not stored, and the peer is not added
/// as a known peer.
#[tokio::test]
async fn test_kad_record_store_exhaustion_does_not_propagate_error() -> eyre::Result<()> {
    use libp2p::kad::store::MemoryStoreConfig;

    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let mut network = peer1.network;

    let max_records = MemoryStoreConfig::default().max_records;

    // fill record store to capacity with unique records
    let store = network.swarm.behaviour_mut().kademlia.store_mut();
    for i in 0..max_records {
        let record = kad::Record {
            key: RecordKey::new(&i.to_be_bytes()),
            value: vec![0u8; 8],
            publisher: Some(PeerId::random()),
            expires: None,
        };
        store.put(record)?;
    }

    // create a valid BLS-signed record from peer2
    let peer2_record = peer2.network.get_peer_record();
    let peer2_peer_id = *peer2.network.swarm.local_peer_id();
    let peer2_bls_key = peer2.config.key_config().primary_public_key();

    // deliver the put request via process_kad_put_request directly
    // (process_kad_event wraps this but requires a ConnectionId we can't construct)
    network.process_kad_put_request(peer2_peer_id, peer2_record.clone());

    // verify the overflow record was not stored
    let stored = network.swarm.behaviour_mut().kademlia.store_mut().get(&peer2_record.key);
    assert!(stored.is_none(), "overflow record must not be stored");

    // verify the peer was NOT added as a known peer (fall-through bug fix)
    let known = network.swarm.behaviour().peer_manager.auth_to_peer(peer2_bls_key);
    assert!(known.is_none(), "peer must not be added as known when store is full");

    Ok(())
}

/// `load_known_peers_from_kad_store` rehydrates the in-memory BLS map from persistent
/// records at startup, so a restarted node does not need to wait for peer re-PUTs to
/// route gossip. Pins the load-bearing fix for the observer-restart bug.
#[tokio::test]
async fn test_load_known_peers_from_kad_store_rehydrates_after_restart() -> eyre::Result<()> {
    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let mut network = peer1.network;

    let peer2_record = peer2.network.get_peer_record();
    let peer2_bls_key = peer2.config.key_config().primary_public_key();

    // seed peer2's signed record directly into the kad store, simulating the persistent
    // state inherited from a prior boot. Bypass process_kad_put_request so peer_manager
    // never learns the mapping.
    network.swarm.behaviour_mut().kademlia.store_mut().put(peer2_record.clone())?;
    assert!(
        network.swarm.behaviour().peer_manager.auth_to_peer(peer2_bls_key).is_none(),
        "peer2 should be cold in known_peers before rehydration"
    );

    network.load_known_peers_from_kad_store();

    let known = network.swarm.behaviour().peer_manager.auth_to_peer(peer2_bls_key);
    assert!(known.is_some(), "peer2 mapping must be present after rehydration");

    Ok(())
}

/// `load_known_peers_from_kad_store` must skip the local node's own record so the node
/// never registers itself as a peer.
#[tokio::test]
async fn test_load_known_peers_from_kad_store_skips_self() -> eyre::Result<()> {
    let TestTypes { peer1, .. } = create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let mut network = peer1.network;

    let self_record = network.get_peer_record();
    let self_bls_key = peer1.config.key_config().primary_public_key();
    network.swarm.behaviour_mut().kademlia.store_mut().put(self_record)?;

    network.load_known_peers_from_kad_store();

    let known = network.swarm.behaviour().peer_manager.auth_to_peer(self_bls_key);
    assert!(known.is_none(), "rehydration must not add the local node to its own known_peers");

    Ok(())
}

/// A re-PUT of an already-stored record (same or older timestamp) is classified as
/// `OldRecord` by the freshness check; the refresh branch must still populate
/// `known_peerids` so a restarted node recovers BLS routing without waiting for peers
/// to bump their timestamps.
#[tokio::test]
async fn test_kad_put_request_refreshes_known_peers_on_duplicate() -> eyre::Result<()> {
    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let mut network = peer1.network;

    let peer2_record = peer2.network.get_peer_record();
    let peer2_peer_id = *peer2.network.swarm.local_peer_id();
    let peer2_bls_key = peer2.config.key_config().primary_public_key();

    // simulate a restart: kad store carries peer2's record from a prior boot,
    // but known_peerids is cold (process_kad_put_request was never called).
    network.swarm.behaviour_mut().kademlia.store_mut().put(peer2_record.clone())?;
    assert!(
        network.swarm.behaviour().peer_manager.auth_to_peer(peer2_bls_key).is_none(),
        "known_peers must be cold before the OldRecord refresh"
    );

    // peer2 re-publishes the record. Same timestamp -> freshness check fails -> the
    // OldRecord refresh branch is the only path that can repopulate the mapping.
    network.process_kad_put_request(peer2_peer_id, peer2_record.clone());

    let known = network.swarm.behaviour().peer_manager.auth_to_peer(peer2_bls_key);
    assert!(known.is_some(), "OldRecord branch must refresh known_peers after restart");

    Ok(())
}

#[tokio::test]
async fn test_connected_peers_count_double_decrement() -> eyre::Result<()> {
    // Demonstrates the connected_peers_count gauge double-decrement bug:
    //
    // When the peer manager initiates a disconnect (via DisconnectPeer/DisconnectPeerX event),
    // the gauge is decremented in the event handler. Then the swarm closes the connection,
    // which triggers PeerDisconnected, decrementing the gauge again. For fatal penalties,
    // a third Banned event also decrements the gauge.
    //
    // Expected (symmetric): inc on connect, dec on disconnect -> gauge returns to 0.
    // Actual (buggy):       inc on connect, 2-3 decs on disconnect -> gauge goes negative.

    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestPrimaryRequest, TestPrimaryResponse>();

    let peer1_metrics = peer1.network_metrics.clone();
    let NetworkPeer {
        config: config_1, network_handle: peer1_handle, network: peer1_network, ..
    } = peer1;
    tokio::spawn(async move {
        peer1_network.run().await.expect("peer1 network run failed!");
    });

    let NetworkPeer {
        config: config_2, network_handle: peer2_handle, network: peer2_network, ..
    } = peer2;
    tokio::spawn(async move {
        peer2_network.run().await.expect("peer2 network run failed!");
    });

    // Start listening
    peer1_handle.start_listening(config_1.primary_address()).await?;
    peer2_handle.start_listening(config_2.primary_address()).await?;

    let peer2_id = peer2_handle.local_peer_id().await?;
    let peer2_addr = peer2_handle.listeners().await?.first().expect("peer2 listen addr").clone();

    // Connect peer1 -> peer2
    peer1_handle
        .add_explicit_peer(
            config_2.key_config().primary_public_key(),
            config_2.key_config().primary_network_public_key(),
            peer2_addr.clone(),
        )
        .await?;
    peer1_handle.dial_by_bls(config_2.key_config().primary_public_key()).await?;

    // Wait for connection to establish
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify connection
    let connected = peer1_handle.connected_peer_ids().await?;
    assert!(connected.contains(&peer2_id), "peer2 should be connected to peer1");

    // Check gauge after connection: should be 1
    let gauge = peer1_metrics.connected_peers_count.with_label_values(&["primary"]);
    assert_eq!(gauge.get(), 1, "gauge should be 1 after one peer connects");

    // Apply fatal penalty - this triggers:
    //   PeerManager -> DisconnectPeer event
    //   ConnectionClosed -> PeerDisconnected event
    //   AllPeers.register_disconnected -> Ban -> Banned event
    peer1_handle.report_penalty(config_2.key_config().primary_public_key(), Penalty::Fatal).await;

    // Wait for all disconnect events to propagate
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL * 2)).await;

    // Verify peer is disconnected
    let connected = peer1_handle.connected_peer_ids().await?;
    assert!(!connected.contains(&peer2_id), "peer2 should be disconnected after fatal penalty");

    // The gauge should be 0 — exactly one peer disconnected.
    assert_eq!(
        gauge.get(),
        0,
        "connected_peers_count should be 0 after one peer disconnects, \
         but asymmetric inc/dec in DisconnectPeer/PeerDisconnected/Banned handlers \
         causes the gauge to drift negative"
    );

    Ok(())
}

/// A swarm with zero listeners must shut down only when nothing can re-create a listener: with
/// relay reservations desired, `retry_relay_reservations` re-issues them, so an all-relays-down
/// window (a boot race, a simultaneous relay outage) is retried instead of shutting the network
/// down.
#[tokio::test]
async fn test_zero_listeners_retried_when_relay_reservations_desired() -> eyre::Result<()> {
    let TestTypes { peer1, .. } = create_test_types::<TestPrimaryRequest, TestPrimaryResponse>();
    let mut network = peer1.network;

    // no listeners were started: a closed listener with no desired relay reservations is fatal
    assert_matches!(
        network.handle_listener_closed(ListenerId::next(), &[]),
        Err(NetworkError::AllListenersClosed)
    );

    // with a desired relay reservation the same state is a retried outage, not a shutdown
    let circuit: Multiaddr =
        "/ip4/127.0.0.1/udp/4001/quic-v1/p2p/12D3KooWK99VoVxNE7XzyBwXEzW7xhK7Gpv85r9F3V3fyKSUKPH5/p2p-circuit/p2p/12D3KooWJWoaqZhDaoEFshF7Rh1bpY9ohihFhzcW6d69Lr2NASuq"
            .parse()
            .expect("valid circuit multiaddr");
    network.relay_reservations.insert(circuit, None);
    assert!(network.handle_listener_closed(ListenerId::next(), &[]).is_ok());

    Ok(())
}

/// Spawns an in-process circuit-relay-v2 server on localhost QUIC, returning its dialable
/// `/ip4/127.0.0.1/udp/<port>/quic-v1/p2p/<relay-id>` address.
async fn spawn_relay_server() -> eyre::Result<Multiaddr> {
    let mut relay_swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_quic()
        .with_behaviour(|keypair| {
            libp2p::relay::Behaviour::new(keypair.public().to_peer_id(), Default::default())
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();
    let relay_id = *relay_swarm.local_peer_id();
    relay_swarm.listen_on("/ip4/127.0.0.1/udp/0/quic-v1".parse()?)?;
    let listen_addr = timeout(Duration::from_secs(5), async {
        loop {
            if let SwarmEvent::NewListenAddr { address, .. } = relay_swarm.select_next_some().await
            {
                break address;
            }
        }
    })
    .await?;
    let relay_addr = listen_addr.with(Protocol::P2p(relay_id));
    // reservation vouchers advertise the relay's external addresses
    relay_swarm.add_external_address(relay_addr.clone());
    tokio::spawn(async move {
        loop {
            relay_swarm.select_next_some().await;
        }
    });
    Ok(relay_addr)
}

/// A request-response round trip between two nodes whose only path to each other is a
/// circuit-relay-v2 server, on a host where direct connectivity IS physically possible. Every
/// connection either end establishes must classify as a relay leg or a circuit - the
/// `direct_nonrelay` count stays 0 - proving the relayed-only topology is a property of the
/// node's dial/listen behavior, not of the network isolation (firewalls, VPC routing) around it.
#[tokio::test]
async fn test_reqres_via_relay_classifies_every_connection_relayed() -> eyre::Result<()> {
    let relay_addr = spawn_relay_server().await?;

    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();

    // peer1 is the destination behind the relay
    let NetworkPeer {
        config: config_1,
        network_handle: peer1,
        network_events: mut network_events_1,
        network,
        network_metrics: metrics_1,
    } = peer1;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // peer2 reaches peer1 through the relay
    let NetworkPeer {
        config: config_2,
        network_handle: peer2,
        network,
        network_metrics: metrics_2,
        ..
    } = peer2;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // peer1's ONLY listener is a reservation on the relay - it never listens on a direct address
    let circuit_listen = relay_addr.clone().with(Protocol::P2pCircuit);
    peer1.start_listening(circuit_listen.clone()).await?;
    // the circuit address is reported as a listener once the relay accepts the reservation
    timeout(Duration::from_secs(10), async {
        loop {
            let listeners = peer1.listeners().await.unwrap_or_default();
            if listeners.iter().any(|a| a.iter().any(|p| matches!(p, Protocol::P2pCircuit))) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await?;

    // peer2 knows peer1 ONLY by its circuit address
    let peer1_bls = config_1.key_config().primary_public_key();
    let peer1_id: PeerId = config_1.primary_networkkey().into();
    let peer1_circuit = circuit_listen.with(Protocol::P2p(peer1_id));
    peer2.add_explicit_peer(peer1_bls, config_1.primary_networkkey(), peer1_circuit).await?;
    peer2.dial_by_bls(peer1_bls).await?;

    // peer1 must learn peer2's bls key (kad record published on connect) to accept its request
    let peer2_bls = config_2.key_config().primary_public_key();
    timeout(Duration::from_secs(10), async {
        loop {
            if peer1.connected_peers().await.unwrap_or_default().contains(&peer2_bls) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await?;

    // full request-response round trip through the circuit
    let missing_block = fixture_batch_with_transactions(3).seal_slow();
    let digests = vec![missing_block.digest()];
    let batch_req = TestWorkerRequest::MissingBatches(digests);
    let batch_res = TestWorkerResponse::MissingBatches { batches: vec![missing_block] };
    let response = peer2.send_request(batch_req.clone(), peer1_bls).await?;
    let event = timeout(Duration::from_secs(5), network_events_1.recv())
        .await?
        .expect("network event received");
    let NetworkEvent::Request { request, channel, .. } = event else {
        panic!("unexpected network event received");
    };
    assert_eq!(request, batch_req);
    peer1.send_response(batch_res.clone(), channel).await?;
    assert_eq!(
        timeout(Duration::from_secs(5), response).await?.expect("response received")?,
        batch_res
    );

    // the topology proof: messages flowed, yet neither node ever established a direct connection
    // to anything but the relay
    for (label, metrics) in [("peer1", &metrics_1), ("peer2", &metrics_2)] {
        let count =
            |path: &str| metrics.connections_by_path.with_label_values(&[path, "primary"]).get();
        assert_eq!(
            count("direct_nonrelay"),
            0,
            "{label} opened a direct connection to a non-relay peer"
        );
        assert!(count("circuit") >= 1, "{label} should hold at least one circuit");
        assert!(count("relay_direct") >= 1, "{label} should hold its relay leg");
    }

    Ok(())
}

/// The path classifier is not vacuously green: two nodes connected directly, with no relay
/// involved, classify their connections as `direct_nonrelay` on both ends and never as circuits.
#[tokio::test]
async fn test_direct_connection_classifies_direct_nonrelay() -> eyre::Result<()> {
    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let NetworkPeer {
        config: config_1,
        network_handle: peer1,
        network,
        network_metrics: metrics_1,
        ..
    } = peer1;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });
    let NetworkPeer {
        config: config_2,
        network_handle: peer2,
        network,
        network_metrics: metrics_2,
        ..
    } = peer2;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    peer1.start_listening(config_1.primary_address()).await?;
    peer2.start_listening(config_2.primary_address()).await?;

    peer1
        .add_explicit_peer(
            config_2.key_config().primary_public_key(),
            config_2.primary_networkkey(),
            config_2.primary_address(),
        )
        .await?;
    peer1.dial_by_bls(config_2.key_config().primary_public_key()).await?;

    // the dial reply can resolve before the network task processes the establishment event, so
    // poll the counters instead of asserting immediately
    let direct = |m: &Arc<NetworkMetrics>| {
        m.connections_by_path.with_label_values(&["direct_nonrelay", "primary"]).get()
    };
    timeout(Duration::from_secs(10), async {
        loop {
            if direct(&metrics_1) >= 1 && direct(&metrics_2) >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await?;

    for (label, metrics) in [("peer1", &metrics_1), ("peer2", &metrics_2)] {
        let count =
            |path: &str| metrics.connections_by_path.with_label_values(&[path, "primary"]).get();
        assert_eq!(count("circuit"), 0, "{label} has no relay, so no circuit connections");
        assert_eq!(count("relay_direct"), 0, "{label} has no relay, so no relay legs");
    }

    Ok(())
}
