use super::{
    health::{ValidatorHealth, BREAKER_COOLDOWN, BREAKER_NOT_INCLUDED_TRIP, PROBE_EXPIRY},
    *,
};
use rand::{rngs::StdRng, SeedableRng};
use rayls_consensus_network::{
    error::NetworkError,
    types::{NetworkCommand, NetworkHandle, NetworkResult},
};
use rayls_consensus_worker::{SubmitRejection, WorkerRPCError, WorkerRequest, WorkerResponse};
use rayls_execution_evm::{
    recover_pooled_transaction, reth_env::RethEnv, test_utils::TransactionFactory,
    PoolTransaction as _, RethChainSpec,
};
use rayls_infrastructure_types::{
    test_genesis, Address, BlsKeypair, GenesisAccount, TaskManager, U256,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{mpsc, oneshot};

/// A distinct, deterministic key per test id.
fn key(id: u64) -> BlsPublicKey {
    *BlsKeypair::generate(&mut StdRng::seed_from_u64(id)).public()
}

/// Candidates are every connected committee member in ring order from the owner's slot, and none
/// are dropped; a disconnected member is the only one skipped.
#[test]
fn candidates_are_all_connected_in_ring_order() {
    let c: Vec<_> = (0..4).map(key).collect();
    let health = ValidatorHealth::new();
    let now = Instant::now();
    assert_eq!(
        health.candidates(1, &c, &c, now),
        vec![c[1], c[2], c[3], c[0]],
        "ring order from the owner's slot, full list"
    );
    assert_eq!(
        health.candidates(0, &c, &[c[0], c[3]], now),
        vec![c[0], c[3]],
        "a disconnected member is skipped; the connected ones stay in ring order"
    );
}

/// A mode reject trips at once, transport failures trip on the third consecutive one, an ack
/// closes the breaker, and a lapsed cooldown returns the validator to the walk as a last resort so
/// a real send is its probe.
#[test]
fn breaker_holds_a_validator_out_until_its_cooldown_lapses() {
    let c: Vec<_> = (0..3).map(key).collect();
    let mut health = ValidatorHealth::new();
    let t0 = Instant::now();
    let mode_reject = || SubmitError::Rejected(SubmitRejection::NotBatchProducing);
    let transport = || SubmitError::Network(NetworkError::Timeout);

    assert!(health.on_failure(&c[0], &mode_reject(), t0), "one mode reject trips");
    assert!(!health.on_failure(&c[0], &mode_reject(), t0), "re-tripping is not a trip");
    assert_eq!(health.candidates(0, &c, &c, t0), vec![c[1], c[2], c[0]], "open breaker walks last");

    assert!(!health.on_failure(&c[1], &transport(), t0));
    assert!(!health.on_failure(&c[1], &transport(), t0));
    assert!(
        !health.on_success(&c[1], []),
        "clearing a counting-but-closed breaker is not a recovery"
    );
    assert!(!health.on_failure(&c[1], &transport(), t0), "success reset the count");
    assert!(!health.on_failure(&c[1], &transport(), t0));
    assert!(health.on_failure(&c[1], &transport(), t0), "third consecutive trips");
    assert_eq!(health.candidates(0, &c, &c, t0), vec![c[2], c[0], c[1]]);

    health.on_failure(&c[2], &mode_reject(), t0);
    assert_eq!(health.candidates(1, &c, &c, t0), vec![c[1], c[2], c[0]], "all open: last resort");

    assert!(
        health.on_success(&c[2], []),
        "an ack on a tripped breaker reports the validator eligible"
    );
    assert_eq!(
        health.candidates(0, &c, &c, t0),
        vec![c[2], c[0], c[1]],
        "an ack closes the breaker"
    );

    let lapsed = t0 + BREAKER_COOLDOWN;
    assert_eq!(health.candidates(0, &c, &c, lapsed), c, "lapsed breakers return to ring order");
    assert!(
        health.on_failure(&c[0], &transport(), lapsed),
        "one failed probe re-trips a lapsed breaker, whatever opened it"
    );
    assert_eq!(health.candidates(0, &c, &c, lapsed), vec![c[1], c[2], c[0]]);
}

/// A backlog shed is the validator's own report that it cannot admit more right now, so it counts
/// like a transport failure: three consecutive sheds trip the breaker and hold the validator out
/// of routing for a cooldown, mixed with transport failures toward the same count; an ack closes
/// it. One shed alone is a burst, not saturation, and does not trip.
#[test]
fn backlog_sheds_trip_the_breaker_like_transport_failures() {
    let c: Vec<_> = (0..2).map(key).collect();
    let mut health = ValidatorHealth::new();
    let t0 = Instant::now();
    let shed = || SubmitError::Rejected(SubmitRejection::Backlog);
    let transport = || SubmitError::Network(NetworkError::Timeout);

    assert!(!health.on_failure(&c[0], &shed(), t0), "one shed is a burst");
    assert!(!health.on_failure(&c[0], &shed(), t0));
    assert!(health.on_failure(&c[0], &shed(), t0), "third consecutive shed trips");
    assert!(health.holds(&c[0], t0), "a tripped busy validator is held out of routing");
    assert!(!health.holds(&c[0], t0 + BREAKER_COOLDOWN), "the hold lapses after one cooldown");

    assert!(!health.on_failure(&c[1], &shed(), t0));
    assert!(!health.on_failure(&c[1], &transport(), t0));
    assert!(health.on_failure(&c[1], &shed(), t0), "sheds and transport failures share the count");
    assert!(health.on_success(&c[1], []), "an ack proves the validator admits again and closes it");
    assert!(!health.holds(&c[1], t0));
}

/// The common recovery is a lapsed breaker: its cooldown expired, so the validator is back at its
/// natural rank and the next normal send probes it. That ack must still report the recovery, even
/// though the breaker no longer holds at the moment of the ack.
#[test]
fn an_ack_on_a_lapsed_breaker_still_reports_recovery() {
    let c: Vec<_> = (0..1).map(key).collect();
    let mut health = ValidatorHealth::new();
    let t0 = Instant::now();
    health.on_failure(&c[0], &SubmitError::Rejected(SubmitRejection::NotBatchProducing), t0);

    let lapsed = t0 + BREAKER_COOLDOWN;
    assert_eq!(
        health.candidates(0, &c, &c, lapsed),
        c,
        "a lapsed breaker probes at its natural rank"
    );
    assert!(
        health.on_success(&c[0], []),
        "the probe's ack clears the lapsed breaker and reports recovery"
    );
}

#[test]
fn is_caught_up_gates_on_local_lag() {
    assert!(TxnForwarder::is_caught_up(100, 100), "level with the anchor is caught up");
    assert!(TxnForwarder::is_caught_up(120, 100), "20 behind is at the bound, still caught up");
    assert!(!TxnForwarder::is_caught_up(121, 100), "21 behind exceeds the bound");
    assert!(
        !TxnForwarder::is_caught_up(99, 100),
        "seen below anchor (epoch reset) is not caught up"
    );
}

#[test]
fn should_send_flows_first_sends_but_gates_resends() {
    let first_due = ForwardProbe { forwarded: false, due: true };
    let resend_due = ForwardProbe { forwarded: true, due: true };
    let not_due = ForwardProbe { forwarded: true, due: false };

    // a first send flows whether or not re-sends are currently allowed
    assert!(TxnForwarder::should_send(first_due, false));
    assert!(TxnForwarder::should_send(first_due, true));
    // a re-send additionally needs the caught-up gate
    assert!(!TxnForwarder::should_send(resend_due, false));
    assert!(TxnForwarder::should_send(resend_due, true));
    // nothing sends when the mark is not due
    assert!(!TxnForwarder::should_send(not_due, true));
}

/// A forwarded frontier is due for re-send once local execution passes its send head by a small
/// correctness margin, not only after the far larger advance that couples re-send latency to
/// unrelated traffic. On a caught-up observer the send head is the tip, so that larger advance
/// only accrues from other senders' blocks and strands the frontier for ~one such window while the
/// backlog behind it piles into the validators' queued subpool. The wall-clock `after` budget is
/// the deliberate WAN latency/jitter buffer and still gates re-send frequency; here it is satisfied
/// so the anchor margin is the sole variable under test.
#[test]
fn a_forwarded_frontier_is_due_once_execution_passes_its_send_head() {
    use rayls_execution_evm::in_flight::InFlightTracker;
    let marks = InFlightTracker::new().arm_forwarding(FORWARD_POLICY);
    let hash = TxHash::repeat_byte(1);
    let t0 = Instant::now();
    let send_head = 100;
    marks.mark_forwarded([hash], t0, send_head);

    // The re-send time budget has elapsed, isolating the anchor gate from the wall-clock gate.
    let now = t0 + FORWARD_POLICY.after + Duration::from_millis(1);

    // Local execution has passed the send head by two blocks: a still-pending forward is provably
    // not-included (inclusion would have advanced the account nonce and pruned it), so it is due.
    assert!(
        marks.probe(&hash, now, send_head + 2).due,
        "a forward two blocks past its send head is due for re-send"
    );
    // Execution has not yet passed the send head: inclusion could still be landing across the WAN,
    // so the forward must not re-send and risk flooding a validator that will include it.
    assert!(
        !marks.probe(&hash, now, send_head).due,
        "a forward not yet passed by local execution must not re-send"
    );
}

/// A stranded frontier re-sends within the WAN latency/jitter budget, not only after the far longer
/// window that let the validators' queued backlog balloon. The budget still holds long enough that
/// a transaction merely in flight across a geo link (committed by the owner but not yet propagated
/// and executed here) is not re-sent under it.
#[test]
fn a_stranded_frontier_re_sends_within_the_wan_budget() {
    use rayls_execution_evm::in_flight::InFlightTracker;
    let marks = InFlightTracker::new().arm_forwarding(FORWARD_POLICY);
    let hash = TxHash::repeat_byte(1);
    let t0 = Instant::now();
    let send_head = 100;
    marks.mark_forwarded([hash], t0, send_head);

    // Anchor has passed the send head, so the correctness gate is open and time is the variable.
    let anchor = send_head + 2;

    // Within the budget: a still-propagating inclusion must not be re-sent (WAN jitter headroom).
    assert!(
        !marks.probe(&hash, t0 + Duration::from_millis(2500), anchor).due,
        "a forward within the WAN latency budget must not re-send"
    );
    // Past the budget: a still-pending frontier is stranded and must re-send promptly.
    assert!(
        marks.probe(&hash, t0 + Duration::from_millis(3500), anchor).due,
        "a forward past the WAN latency budget must re-send"
    );
}

/// Re-forwarding a still-pending frontier does not back it off out of the WAN budget. Backoff on
/// this path counts successful re-sends of a transaction the owner acked but has not yet included
/// (rotated away, killed pre-commit, or merely slow), so it throttles the very transaction that
/// most needs a fast re-send. A bounded dampener is kept (the re-send still spaces out), but the
/// tail stays within a small multiple of the base rather than the far larger window that let the
/// backlog balloon.
#[test]
fn a_repeatedly_re_forwarded_frontier_stays_within_a_bounded_backoff() {
    use rayls_execution_evm::in_flight::InFlightTracker;
    let marks = InFlightTracker::new().arm_forwarding(FORWARD_POLICY);
    let hash = TxHash::repeat_byte(1);
    let t0 = Instant::now();
    let send_head = 100;
    let anchor = send_head + 2;

    // Four forwards (one insert, three re-sends) bring the mark to three recorded attempts.
    for _ in 0..4 {
        marks.mark_forwarded([hash], t0, send_head);
    }

    // The dampener still holds within the base budget: no re-send under ~one budget.
    assert!(
        !marks.probe(&hash, t0 + Duration::from_millis(2500), anchor).due,
        "even a re-forwarded frontier keeps a base dampener"
    );
    // But the backoff tail cannot push a stranded frontier past a small multiple of the base: at
    // 7s it is due, where the uncapped doubling (3s x 2^3 = 24s) would still gate it.
    assert!(
        marks.probe(&hash, t0 + Duration::from_millis(7000), anchor).due,
        "a repeatedly re-forwarded frontier re-sends within a bounded backoff, not the 16x tail"
    );
}

#[test]
fn validate_stale_keeps_only_hashes_this_node_sent() {
    let sent = [TxHash::repeat_byte(1), TxHash::repeat_byte(2)];
    // the peer claims a hash we sent plus one we never sent
    let claimed = vec![TxHash::repeat_byte(2), TxHash::repeat_byte(9)];
    let stale = TxnForwarder::validate_stale(&sent, claimed);
    assert_eq!(stale.len(), 1);
    assert!(stale.contains(&TxHash::repeat_byte(2)));
    assert!(!stale.contains(&TxHash::repeat_byte(9)), "a peer cannot suppress an unsent hash");
}

#[tokio::test(flavor = "multi_thread")]
async fn chunk_under_budget_splits_by_count_and_budget() {
    let task_manager = TaskManager::default();
    let mut fixture = PoolFixture::new(&task_manager).await;
    fixture.add_next().await;
    fixture.add_next().await;
    let txns: Vec<(TxHash, Arc<PoolTxn>)> =
        fixture.pool.pending_transactions().into_iter().map(|txn| (*txn.hash(), txn)).collect();
    assert_eq!(txns.len(), 3, "three pooled transactions to split");

    // A count cap breaks the stream every max_count under an unbounded byte budget.
    let by_count = TxnForwarder::chunk_under_budget(txns.clone(), usize::MAX, 2);
    assert_eq!(by_count.iter().map(Vec::len).collect::<Vec<_>>(), vec![2, 1]);

    // A byte budget below any single transaction still ships each alone rather than dropping it.
    let by_budget = TxnForwarder::chunk_under_budget(txns, 1, usize::MAX);
    assert_eq!(by_budget.iter().map(Vec::len).collect::<Vec<_>>(), vec![1, 1, 1]);

    // The empty stream produces no messages.
    assert!(TxnForwarder::chunk_under_budget(Vec::new(), usize::MAX, 2).is_empty());
}

/// Spawn a mock worker network reporting `connected` and answering each direct submit through
/// `on_submit`, registered on `task_manager` so the test tears it down with the forwarder.
fn spawn_mock_network(
    task_manager: &TaskManager,
    connected: Vec<BlsPublicKey>,
    mut on_submit: impl FnMut(BlsPublicKey, Vec<Bytes>, oneshot::Sender<NetworkResult<WorkerResponse>>)
        + Send
        + 'static,
) -> WorkerNetworkHandle {
    let (tx, mut commands) = mpsc::channel::<NetworkCommand<WorkerRequest, WorkerResponse>>(64);
    task_manager.get_spawner().spawn_task("mock worker network", async move {
        while let Some(cmd) = commands.recv().await {
            match cmd {
                NetworkCommand::ConnectedPeers { reply } => {
                    let _ = reply.send(connected.clone());
                }
                NetworkCommand::SendRequest {
                    peer,
                    request: WorkerRequest::SubmitTxns { transactions },
                    reply,
                } => on_submit(peer, transactions, reply),
                _ => {}
            }
        }
    });
    WorkerNetworkHandle::new(NetworkHandle::new(tx), task_manager.get_spawner(), 1 << 20)
}

/// The ack every honest mock validator sends.
fn ack() -> NetworkResult<WorkerResponse> {
    Ok(WorkerResponse::SubmitTxns { stale: vec![] })
}

/// The hashes of encoded transactions.
fn hashes_of(txns: &[Bytes]) -> Vec<TxHash> {
    txns.iter().map(|txn| *recover_pooled_transaction(txn).unwrap().hash()).collect()
}

/// One delivery the mock network saw: the peer and how many transactions it was sent.
type Delivered = (BlsPublicKey, usize);

/// The next delivery, within the bound every live test allows.
async fn next(delivered: &mut mpsc::Receiver<Delivered>) -> Delivered {
    tokio::time::timeout(Duration::from_secs(5), delivered.recv())
        .await
        .expect("a delivery within the bound")
        .expect("the mock network outlives the test")
}

/// Deliveries until `txns` transactions have been seen.
async fn next_txns(delivered: &mut mpsc::Receiver<Delivered>, txns: usize) -> Vec<Delivered> {
    let mut seen = Vec::new();
    while seen.iter().map(|(_, count)| count).sum::<usize>() < txns {
        seen.push(next(delivered).await);
    }
    seen
}

/// Move local execution and the seen header to `number` together: still caught up.
fn advance(
    executed: &watch::Sender<ConsensusHeader>,
    seen: &watch::Sender<ConsensusHeader>,
    number: u64,
) {
    let header = ConsensusHeader { number, ..Default::default() };
    executed.send(header.clone()).unwrap();
    seen.send(header).unwrap();
}

/// A pool over a temp chain with funded senders whose transactions the tests forward.
struct PoolFixture {
    pool: WorkerTxPool,
    factories: Vec<TransactionFactory>,
    chain: Arc<RethChainSpec>,
    gas_price: u128,
    _tmp_dir: tempfile::TempDir,
}

impl PoolFixture {
    /// A fixture holding the default sender's first transaction pending.
    async fn new(task_manager: &TaskManager) -> Self {
        let mut fixture = Self::with_senders(task_manager, vec![TransactionFactory::new()]).await;
        fixture.add_next().await;
        fixture
    }

    /// A fixture funding every given sender at genesis, with nothing pending yet.
    async fn with_senders(task_manager: &TaskManager, factories: Vec<TransactionFactory>) -> Self {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let funded = factories
            .iter()
            .map(|factory| (factory.address(), GenesisAccount::default().with_balance(U256::MAX)));
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().extend_accounts(funded).into());
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), task_manager, None)
                .await
                .unwrap();
        let pool = reth_env.init_txn_pool().unwrap();
        let gas_price = reth_env.get_gas_price().unwrap();
        Self { pool, factories, chain, gas_price, _tmp_dir: tmp_dir }
    }

    /// The default sender whose transactions the fixture pools.
    fn sender(&self) -> Address {
        self.factories[0].address()
    }

    /// Pool the default sender's next transaction.
    async fn add_next(&mut self) {
        self.add_next_for(0).await;
    }

    /// Pool the next transaction of the `index`-th sender.
    async fn add_next_for(&mut self, index: usize) {
        let txn = self.factories[index].create_eip1559_encoded(
            self.chain.clone(),
            None,
            self.gas_price,
            Some(Address::ZERO),
            U256::from(1),
            Default::default(),
        );
        self.pool.add_transaction_local(recover_pooled_transaction(&txn).unwrap()).await.unwrap();
    }
}

/// `count` distinct senders whose owner slot on `committee` is `slot`, found by seed.
fn senders_on_slot(count: usize, slot: u64, committee: &[BlsPublicKey]) -> Vec<TransactionFactory> {
    (0u64..)
        .map(|seed| TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(seed)))
        .filter(|factory| TxnForwarder::owner_slot(factory.address(), 0, committee) == slot)
        .take(count)
        .collect()
}

/// A resend policy short enough to drive a live test through re-sends in wall-clock milliseconds.
const FAST_POLICY: DuePolicy =
    DuePolicy { after: Duration::from_millis(100), backoff_shift_cap: 0, min_anchor_advance: 1 };

/// A direct-submit forwarder over `network_handle` and the fixture's pool.
fn forwarder_over(
    fixture: &PoolFixture,
    network_handle: WorkerNetworkHandle,
    committee: Vec<BlsPublicKey>,
    executed: watch::Receiver<ConsensusHeader>,
    seen: watch::Receiver<ConsensusHeader>,
    policy: DuePolicy,
) -> TxnForwarder {
    // The tests keep the executed anchor below one rotation window, so every send is owner window
    // 0.
    TxnForwarder::new(
        fixture.pool.clone(),
        network_handle,
        executed,
        seen,
        committee,
        Box::new(|| true),
        1 << 20,
        policy,
    )
}

/// A forwarder whose progress signals never move, under the production policy.
fn idle_forwarder_over(
    fixture: &PoolFixture,
    network_handle: WorkerNetworkHandle,
    committee: Vec<BlsPublicKey>,
) -> TxnForwarder {
    let (executed, seen) = (
        watch::channel(ConsensusHeader::default()).1,
        watch::channel(ConsensusHeader::default()).1,
    );
    forwarder_over(fixture, network_handle, committee, executed, seen, FORWARD_POLICY)
}

/// A validator that answers a submit with the not-batch-producing reply trips its breaker on the
/// spot: the sender's next message goes straight to the live validator without touching it again.
#[tokio::test(flavor = "multi_thread")]
async fn mode_rejecting_validator_is_skipped_while_its_breaker_holds() {
    let task_manager = TaskManager::default();
    let mut fixture = PoolFixture::new(&task_manager).await;
    // Seat the rejecting validator on the sender's owner slot so every walk starts there.
    let committee: Vec<_> = (1..=2).map(key).collect();
    let owner = TxnForwarder::owner_slot(fixture.sender(), 0, &committee) as usize;
    let (rejecting, live) = (committee[owner], committee[1 - owner]);

    let rejections = Arc::new(AtomicUsize::new(0));
    let (delivered_tx, mut delivered) = mpsc::channel::<Delivered>(4);
    let counted = rejections.clone();
    let network_handle =
        spawn_mock_network(&task_manager, committee.clone(), move |peer, txns, reply| {
            if peer == rejecting {
                counted.fetch_add(1, Ordering::Relaxed);
                // A gate reply rides the error response variant, as the receiver sends it.
                let reply_text = SubmitRejection::NotBatchProducing.to_string();
                let _ = reply.send(Ok(WorkerResponse::Error(WorkerRPCError(reply_text))));
            } else {
                let _ = delivered_tx.try_send((peer, txns.len()));
                let _ = reply.send(ack());
            }
        });
    let forwarder = idle_forwarder_over(&fixture, network_handle, committee);
    task_manager
        .get_spawner()
        .spawn_task("txn forwarder", forwarder.run(Duration::from_millis(50)));

    assert_eq!(next(&mut delivered).await.0, live, "the first message reaches live");
    assert_eq!(rejections.load(Ordering::Relaxed), 1, "the owner was tried once and rejected");

    fixture.add_next().await;
    assert_eq!(next(&mut delivered).await.0, live, "the second message reaches live");
    assert_eq!(rejections.load(Ordering::Relaxed), 1, "the open breaker kept the walk off it");
}

/// A sender's owner is fixed for one anchor window and re-drawn at the next: a censoring owner
/// holds a sender for at most `1 << OWNER_ROTATION_SHIFT` blocks, and re-sends within the window
/// return to the same owner so the breaker, not the walk, is what escapes it sooner.
#[test]
fn owner_is_fixed_within_an_anchor_window_and_rotates_at_the_next() {
    let window = 1u64 << OWNER_ROTATION_SHIFT;
    let c: Vec<_> = (0..4).map(key).collect();
    let senders: Vec<Address> = (0..64u8).map(Address::with_last_byte).collect();
    for &sender in &senders {
        let owner = TxnForwarder::owner_slot(sender, 0, &c);
        for anchor in [1, window / 2, window - 1] {
            assert_eq!(
                TxnForwarder::owner_slot(sender, anchor, &c),
                owner,
                "fixed within a window"
            );
        }
        assert_eq!(
            TxnForwarder::owner_slot(sender, window, &c),
            TxnForwarder::owner_slot(sender, 2 * window - 1, &c),
            "fixed within the next window"
        );
    }
    let rotated = senders
        .iter()
        .filter(|&&s| TxnForwarder::owner_slot(s, window, &c) != TxnForwarder::owner_slot(s, 0, &c))
        .count();
    assert!(rotated >= 32, "a new window re-draws owners; only {rotated} of 64 senders moved");
}

/// Rendezvous routing is a pure function of the sender, the window and the eligible set: a
/// sender keeps its owner in every eligible subset that still contains it, and when its owner
/// leaves the set the sender moves to its next-best validator, so one validator's senders spread
/// over the rest instead of piling onto a neighbour.
#[test]
fn rendezvous_owner_is_stable_under_eligibility_changes_and_spreads_failover() {
    let c: Vec<_> = (0..4).map(key).collect();
    let senders: Vec<Address> = (0..64u8).map(Address::with_last_byte).collect();
    let owner = |s, eligible: &dyn Fn(u64) -> bool| {
        TxnForwarder::rendezvous_owner(s, 7, &c, |slot| eligible(slot)).expect("an eligible slot")
    };
    let mut fallback_of = [[0usize; 4]; 4];
    for &sender in &senders {
        let full = owner(sender, &|_| true);
        for gone in 0..4u64 {
            let without = owner(sender, &|slot| slot != gone);
            if gone == full {
                assert_ne!(without, gone);
                fallback_of[gone as usize][without as usize] += 1;
            } else {
                assert_eq!(without, full, "an unrelated validator leaving does not move a sender");
            }
        }
    }
    for (gone, spread) in fallback_of.iter().enumerate() {
        let receivers = spread.iter().filter(|&&n| n > 0).count();
        assert!(receivers >= 2, "slot {gone}'s senders must spread over the rest, got {spread:?}");
    }
    assert!(TxnForwarder::rendezvous_owner(senders[0], 7, &c, |_| false).is_none());
}

/// An owner that acks but does not include never fails a send, so its breaker must count
/// non-inclusion on its own: three re-sent groups trip it, and an ack (a censor acks everything)
/// neither resets the count nor clears the trip. Only a delivered group leaving the pool does.
#[test]
fn acked_but_not_included_trips_the_breaker_until_a_delivery_is_included() {
    let c: Vec<_> = (0..3).map(key).collect();
    let mut health = ValidatorHealth::new();
    let t0 = Instant::now();
    let (sender, last) = (Address::random(), TxHash::random());
    assert!(!health.on_not_included(&c[0], sender, t0));
    assert!(!health.on_success(&c[0], []), "an ack on a closed breaker is not a recovery");
    assert!(!health.on_not_included(&c[0], sender, t0), "and did not reset the count");
    assert!(health.on_not_included(&c[0], sender, t0), "the third non-inclusion trips");
    assert_eq!(health.candidates(0, &c, &c, t0), vec![c[1], c[2], c[0]]);

    assert!(!health.on_success(&c[0], [(sender, last)]), "the probe ack does not clear the trip");
    assert!(health.credit_included(|_| true).is_empty(), "nor does the delivery staying pending");
    assert_eq!(health.candidates(0, &c, &c, t0), vec![c[1], c[2], c[0]]);
    assert_eq!(health.credit_included(|_| false), vec![c[0]], "the delivery leaving the pool does");
    assert_eq!(health.candidates(0, &c, &c, t0), c, "with a clean count");
}

/// An inclusion is the evidence a non-inclusion count is missing, so it resets a closed breaker's
/// count the way an ack resets transport failures: blame accrued over a lifetime must not trip an
/// honest validator that has included since.
#[test]
fn inclusion_resets_a_closed_breakers_non_inclusion_count() {
    let c: Vec<_> = (0..3).map(key).collect();
    let mut health = ValidatorHealth::new();
    let t0 = Instant::now();
    let (sender, last) = (Address::random(), TxHash::random());
    assert!(!health.on_not_included(&c[0], sender, t0));
    assert!(!health.on_not_included(&c[0], sender, t0));
    assert!(!health.on_success(&c[0], [(sender, last)]));
    assert!(health.credit_included(|_| false).is_empty(), "a closed breaker has no trip to close");
    assert!(!health.on_not_included(&c[0], sender, t0), "the count restarted at the inclusion");
    assert!(!health.on_not_included(&c[0], sender, t0));
    assert!(health.on_not_included(&c[0], sender, t0), "and trips on three fresh blames");
}

/// A lapsed breaker admits one first-send group as its probe and holds the rest of its slot's
/// traffic off until that probe resolves; a closed breaker admits everything.
#[test]
fn lapsed_breaker_admits_one_probe_at_a_time() {
    let peer = key(0);
    let (probe, other) = (Address::random(), Address::random());
    let mut health = ValidatorHealth::new();
    let t0 = Instant::now();
    assert!(health.admit(&peer, Some(probe), t0), "a clean record admits");
    for _ in 0..3 {
        health.on_not_included(&peer, probe, t0);
    }
    assert!(!health.admit(&peer, Some(probe), t0), "a holding breaker admits nothing");
    let lapsed = t0 + BREAKER_COOLDOWN;
    assert!(!health.admit(&peer, None, lapsed), "a re-send cannot probe: its verdict is too slow");
    assert!(
        health.admit(&peer, Some(probe), lapsed),
        "the first send after the lapse is the probe"
    );
    assert!(
        !health.admit(&peer, Some(other), lapsed),
        "the next is not, while the probe is outstanding"
    );
    assert!(
        !health.admit(&peer, Some(other), lapsed + BREAKER_COOLDOWN),
        "a probe's verdict is its re-send, one resend window out, so it outlives a cooldown"
    );
    assert!(health.admit(&peer, Some(other), lapsed + PROBE_EXPIRY), "an unresolved probe expires");
}

/// A probe that comes back as a re-send re-trips its breaker with a doubled hold, so a validator
/// that never includes is probed less and less often; a blame for any other sender (a group that
/// predates the trip) neither resolves the probe nor re-trips. An inclusion still closes outright.
#[test]
fn a_failed_probe_re_trips_with_a_longer_hold() {
    let c = vec![key(0), key(1)];
    let (probe, other) = (Address::random(), Address::random());
    let mut health = ValidatorHealth::new();
    let t0 = Instant::now();
    for _ in 0..3 {
        health.on_not_included(&c[0], other, t0);
    }
    let lapsed = t0 + BREAKER_COOLDOWN;
    assert!(health.admit(&c[0], Some(probe), lapsed));
    assert!(!health.on_not_included(&c[0], other, lapsed), "a pre-trip blame is not a re-trip");
    assert!(!health.admit(&c[0], Some(other), lapsed), "and leaves the probe outstanding");

    assert!(health.on_not_included(&c[0], probe, lapsed), "the probe's own blame re-trips");
    let once = lapsed + BREAKER_COOLDOWN;
    assert_eq!(health.candidates(0, &c, &c, once), vec![c[1], c[0]], "held past one cooldown");
    assert!(!health.admit(&c[0], Some(other), once));
    let twice = lapsed + BREAKER_COOLDOWN * 2;
    assert!(
        health.admit(&c[0], Some(other), twice),
        "the doubled hold lapses and admits a new probe"
    );

    health.on_success(&c[0], [(other, TxHash::random())]);
    assert_eq!(health.credit_included(|_| false), vec![c[0]], "an inclusion closes it outright");
    assert!(health.admit(&c[0], Some(probe), twice), "with a clean record");
}

/// A re-sent group blames the validator that acked its prior send: the recorded delivery when
/// there is one, else the group's owner. First sends blame no one.
#[test]
fn re_sent_groups_blame_the_acking_validator_else_the_owner() {
    let c: Vec<_> = (0..3).map(key).collect();
    let sender = Address::random();
    let affinity = TxnForwarder::owner_slot(sender, 0, &c);
    let mut health = ValidatorHealth::new();
    let t0 = Instant::now();
    let group = |resent| [(sender, affinity, resent)];
    for _ in 0..3 {
        assert!(health.blame_non_inclusion(&c, group(false), t0).is_empty());
    }
    assert_eq!(health.candidates(affinity, &c, &c, t0)[0], c[affinity as usize]);
    for _ in 0..2 {
        assert!(health.blame_non_inclusion(&c, group(true), t0).is_empty());
    }
    let blamed = c[affinity as usize];
    assert_eq!(health.blame_non_inclusion(&c, group(true), t0), vec![blamed]);
    assert_eq!(health.candidates(affinity, &c, &c, t0).last(), Some(&blamed));

    // A recorded delivery names the acking validator, whoever the owner is.
    let acked_by = c[((affinity + 2) % 3) as usize];
    health.on_success(&acked_by, [(sender, TxHash::random())]);
    for _ in 0..2 {
        assert!(health.blame_non_inclusion(&c, group(true), t0).is_empty());
    }
    assert_eq!(health.blame_non_inclusion(&c, group(true), t0), vec![acked_by]);
}

/// The evidence-driven escape end to end: an owner that acks every send as stale and includes
/// nothing keeps receiving the re-sends until the non-inclusion threshold of them blamed it, then
/// the same transactions land on the live validator. Fails against a terminal stale ack, which
/// silenced the hash for the node's lifetime.
#[tokio::test(flavor = "multi_thread")]
async fn censoring_owner_is_escaped_once_its_breaker_trips() {
    let task_manager = TaskManager::default();
    let fixture = PoolFixture::new(&task_manager).await;
    let committee: Vec<_> = (1..=2).map(key).collect();
    let owner = TxnForwarder::owner_slot(fixture.sender(), 0, &committee) as usize;
    let (censor, live) = (committee[owner], committee[1 - owner]);

    let (delivered_tx, mut delivered) = mpsc::channel::<Delivered>(8);
    let network_handle =
        spawn_mock_network(&task_manager, committee.clone(), move |peer, txns, reply| {
            let _ = delivered_tx.try_send((peer, txns.len()));
            // The censor lies: every hash it is sent is "already executed".
            let stale = if peer == censor { hashes_of(&txns) } else { vec![] };
            let _ = reply.send(Ok(WorkerResponse::SubmitTxns { stale }));
        });
    let (executed_tx, executed_rx) = watch::channel(ConsensusHeader::default());
    let (seen_tx, seen_rx) = watch::channel(ConsensusHeader::default());
    let forwarder =
        forwarder_over(&fixture, network_handle, committee, executed_rx, seen_rx, FAST_POLICY);
    task_manager
        .get_spawner()
        .spawn_task("txn forwarder", forwarder.run(Duration::from_millis(20)));

    let (first, txns) = next(&mut delivered).await;
    assert_eq!(first, censor, "the first send lands on the affinity owner");
    // Execution moves on without the transaction, one block per re-send, still caught up; every
    // re-send short of the trip returns to the owner.
    for resend in 1..BREAKER_NOT_INCLUDED_TRIP {
        advance(&executed_tx, &seen_tx, u64::from(resend));
        let (to, resent) = next(&mut delivered).await;
        assert_eq!(to, censor, "re-send {resend} returns to the owner");
        assert_eq!(resent, txns, "carrying the same transactions");
    }
    advance(&executed_tx, &seen_tx, u64::from(BREAKER_NOT_INCLUDED_TRIP));

    let (escaped, resent) = next(&mut delivered).await;
    assert_eq!(escaped, live, "the tripping re-send walks on to the live validator");
    assert_eq!(resent, txns, "carrying the same transactions");
}

/// The breaker's steady-state claim: a censor that acks and drops is held out after three re-sent
/// groups, and once its cooldown lapses it gets one probe, not the slot's traffic. Every fresh
/// sender first-sent after the lapse that the censor captures pays a full resend window.
#[tokio::test(flavor = "multi_thread")]
async fn lapsed_censor_breaker_admits_at_most_a_probe() {
    let task_manager = TaskManager::default();
    let (censor, live) = (key(1), key(2));
    // Every sender's owner is the censor, with one transaction each so transaction counts are
    // group counts; slot 0 keeps the committee order simple.
    let committee = vec![censor, live];
    let senders = senders_on_slot(14, 0, &committee);
    let mut fixture = PoolFixture::with_senders(&task_manager, senders).await;

    let (delivered_tx, mut delivered) = mpsc::channel::<Delivered>(64);
    let pool = fixture.pool.clone();
    let network_handle =
        spawn_mock_network(&task_manager, committee.clone(), move |peer, txns, reply| {
            // The live validator includes: the pool prunes what it was sent. The censor acks
            // and drops.
            if peer == live {
                pool.remove_transactions(hashes_of(&txns));
            }
            let _ = delivered_tx.try_send((peer, txns.len()));
            let _ = reply.send(ack());
        });
    let (executed_tx, executed_rx) = watch::channel(ConsensusHeader::default());
    let (seen_tx, seen_rx) = watch::channel(ConsensusHeader::default());
    let forwarder =
        forwarder_over(&fixture, network_handle, committee, executed_rx, seen_rx, FAST_POLICY);
    task_manager
        .get_spawner()
        .spawn_task("txn forwarder", forwarder.run(Duration::from_millis(20)));

    for index in 0..3 {
        fixture.add_next_for(index).await;
        assert_eq!(next(&mut delivered).await.0, censor, "first sends land on the owner");
    }
    advance(&executed_tx, &seen_tx, 1);
    // Re-sends return to the owner until the third blamed group trips its breaker.
    next_txns(&mut delivered, 3).await;
    fixture.add_next_for(3).await;
    assert_eq!(next(&mut delivered).await.0, live, "a held censor takes no sender");

    tokio::time::sleep(BREAKER_COOLDOWN + Duration::from_millis(100)).await;
    for index in 4..14 {
        fixture.add_next_for(index).await;
    }
    let captured: usize = next_txns(&mut delivered, 10)
        .await
        .iter()
        .filter(|(to, _)| *to == censor)
        .map(|(_, count)| count)
        .sum();
    assert!(
        captured <= 1,
        "a lapsed breaker gets one probe; the censor captured {captured} of 10 fresh senders"
    );
}

/// Owner streams are independent: an owner that is slow to ack must not delay another owner's
/// first send by more than a tick or two.
#[tokio::test(flavor = "multi_thread")]
async fn slow_acking_owner_does_not_stall_other_owners() {
    let task_manager = TaskManager::default();
    let (slow, fast) = (key(1), key(2));
    let committee = vec![slow, fast];
    let mut senders = senders_on_slot(1, 0, &committee);
    senders.extend(senders_on_slot(1, 1, &committee));
    let mut fixture = PoolFixture::with_senders(&task_manager, senders).await;
    let ack_delay = Duration::from_secs(1);

    let (delivered_tx, mut delivered) = mpsc::channel::<Delivered>(8);
    let network_handle =
        spawn_mock_network(&task_manager, committee.clone(), move |peer, txns, reply| {
            let _ = delivered_tx.try_send((peer, txns.len()));
            if peer == slow {
                // Reply off the mock loop so the delay stalls only this request.
                tokio::spawn(async move {
                    tokio::time::sleep(ack_delay).await;
                    let _ = reply.send(ack());
                });
            } else {
                let _ = reply.send(ack());
            }
        });
    let forwarder = idle_forwarder_over(&fixture, network_handle, committee);
    let tick = Duration::from_millis(20);
    task_manager.get_spawner().spawn_task("txn forwarder", forwarder.run(tick));

    fixture.add_next_for(0).await;
    assert_eq!(next(&mut delivered).await.0, slow);
    let added = Instant::now();
    fixture.add_next_for(1).await;
    assert_eq!(next(&mut delivered).await.0, fast);
    let waited = added.elapsed();
    assert!(
        waited < tick * 10,
        "the fast owner's first send waited {waited:?} behind the slow owner's ack"
    );
}

/// A first send made while this node lags must be stamped with the network head at send time, not
/// the local execution anchor. The validator includes it at the head; once local execution comes
/// within the re-send bound of that head, an anchor-stamped mark is long "due" by the anchor
/// margin although the block that included it is not executed locally yet, so every such sender
/// re-sends in one tick and one tick's blame trips every live validator at once.
#[tokio::test(flavor = "multi_thread")]
async fn first_send_while_lagging_is_not_resent_before_execution_passes_the_send_head() {
    let task_manager = TaskManager::default();
    let committee: Vec<_> = (1..=2).map(key).collect();
    let senders = senders_on_slot(3, 0, &committee);
    let mut fixture = PoolFixture::with_senders(&task_manager, senders).await;

    let (delivered_tx, mut delivered) = mpsc::channel::<Delivered>(64);
    let network_handle =
        spawn_mock_network(&task_manager, committee.clone(), move |peer, txns, reply| {
            // Every validator acks and includes at the network head; the local pool prunes only
            // once local execution reaches that block, which the test never lets happen.
            let _ = delivered_tx.try_send((peer, txns.len()));
            let _ = reply.send(ack());
        });
    let (executed_tx, executed_rx) = watch::channel(ConsensusHeader::default());
    let (seen_tx, seen_rx) = watch::channel(ConsensusHeader::default());
    // Lagging: the network is at 100 and local execution at 0, so first sends flow while re-sends
    // are gated.
    seen_tx.send(ConsensusHeader { number: 100, ..Default::default() }).unwrap();
    let forwarder =
        forwarder_over(&fixture, network_handle, committee, executed_rx, seen_rx, FAST_POLICY);
    task_manager
        .get_spawner()
        .spawn_task("txn forwarder", forwarder.run(Duration::from_millis(20)));

    for index in 0..3 {
        fixture.add_next_for(index).await;
        assert!(next(&mut delivered).await.1 > 0, "first sends flow while lagging");
    }

    // The lag closes to within the re-send bound, but the including block (at the head, 100) is
    // still ahead of local execution: nothing may be re-sent, and so nothing blamed.
    executed_tx.send(ConsensusHeader { number: 90, ..Default::default() }).unwrap();
    let resent = tokio::time::timeout(Duration::from_millis(500), next(&mut delivered)).await;
    assert!(
        resent.is_err(),
        "re-sent {resent:?} before local execution passed the head the sends were made at"
    );
}

/// Every observer routes by the same rule, and a validator that drops out of the eligible set
/// hands its senders to the rest by their own rendezvous scores: after the owner's breaker trips
/// on a mode reject, each further sender it owned lands on the validator production computes for
/// it, and those fallbacks cover more than one validator.
#[tokio::test(flavor = "multi_thread")]
async fn a_held_owner_hands_its_senders_to_the_rest_by_rendezvous() {
    let task_manager = TaskManager::default();
    let committee: Vec<_> = (0..3).map(key).collect();
    let senders = senders_on_slot(8, 0, &committee);
    let fallback: Vec<u64> = senders
        .iter()
        .map(|factory| {
            TxnForwarder::rendezvous_owner(factory.address(), 0, &committee, |slot| slot != 0)
                .expect("two validators remain")
        })
        .collect();
    assert!(
        fallback[1..].contains(&1) && fallback[1..].contains(&2),
        "the senders' fallbacks must cover both remaining validators: {fallback:?}"
    );
    let mut fixture = PoolFixture::with_senders(&task_manager, senders).await;

    let rejecting = committee[0];
    let (delivered_tx, mut delivered) = mpsc::channel::<Delivered>(64);
    let network_handle =
        spawn_mock_network(&task_manager, committee.clone(), move |peer, txns, reply| {
            if peer == rejecting {
                let reply_text = SubmitRejection::NotBatchProducing.to_string();
                let _ = reply.send(Ok(WorkerResponse::Error(WorkerRPCError(reply_text))));
            } else {
                let _ = delivered_tx.try_send((peer, txns.len()));
                let _ = reply.send(ack());
            }
        });
    let forwarder = idle_forwarder_over(&fixture, network_handle, committee.clone());
    task_manager
        .get_spawner()
        .spawn_task("txn forwarder", forwarder.run(Duration::from_millis(20)));
    let slot_of =
        |peer: BlsPublicKey| committee.iter().position(|p| *p == peer).expect("member") as u64;

    // The first send hits the owner, trips its breaker, and fails over along the ring.
    fixture.add_next_for(0).await;
    assert_eq!(slot_of(next(&mut delivered).await.0), 1, "ring failover of the tripping send");
    // With the owner held, every later sender goes straight to its own rendezvous fallback.
    for index in 1..8 {
        fixture.add_next_for(index).await;
        assert_eq!(slot_of(next(&mut delivered).await.0), fallback[index], "sender {index}");
    }
}

/// The rendezvous score must rank the validators uniformly across senders: a weak mixer lets the
/// fixed validator keys dominate the sender prefix, so one validator wins almost always and
/// another almost never, whatever the sender count. Over 4096 senders every slot must land within
/// 15% of its fair share, in the same window and across windows.
#[test]
fn rendezvous_owner_spreads_senders_uniformly() {
    let c: Vec<_> = (0..4).map(key).collect();
    let senders = (0..4096u64).map(|i| {
        let mut bytes = [0u8; 20];
        bytes[..8].copy_from_slice(&i.to_le_bytes());
        Address::from(bytes)
    });
    let mut load = [0usize; 4];
    for sender in senders {
        load[TxnForwarder::owner_slot(sender, 0, &c) as usize] += 1;
    }
    let fair = 4096 / 4;
    for (slot, n) in load.iter().enumerate() {
        assert!(
            (fair * 85 / 100..=fair * 115 / 100).contains(n),
            "slot {slot} owns {n} of 4096 senders, fair share {fair}: {load:?}"
        );
    }
}
