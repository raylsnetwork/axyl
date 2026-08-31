//! Per-authority batch sequence ordering with parking for out-of-order batches.

use std::sync::Arc;

use std::{cmp::Ordering, collections::BTreeMap};

use parking_lot::Mutex;
use rayls_infrastructure_storage::{
    tables::{Batches, ConsensusBlocks},
    BatchOrderingStore,
};
use rayls_infrastructure_types::{
    batch_ordering::{AuthoritySeqState, BatchOrderingState, MAX_PARKED_PER_AUTHORITY},
    batch_tracker::BatchTracker,
    executed_batch_registry::ExecutedBatchRegistry,
    leader_epoch_and_batch_digests, AcceptResult, Address, Database, DbTx, Epoch, PreparedBatch,
};
use tracing::{debug, info, warn};

#[derive(Debug)]
struct BatchOrderingInner<DB: BatchOrderingStore> {
    batch_ordering_state: Mutex<BatchOrderingState>,
    batch_ordering_store: DB,
}

/// Per-authority batch ordering with parking and epoch-reset.
#[derive(Debug, Clone)]
pub struct BatchOrdering<DB: BatchOrderingStore> {
    inner: Arc<BatchOrderingInner<DB>>,
}

impl<DB: Database> BatchOrdering<DB> {
    /// Create an ordering over `store` seeded with `batch_ordering_state`.
    pub fn new(store: DB, batch_ordering_state: BatchOrderingState) -> Self {
        Self {
            inner: Arc::new(BatchOrderingInner {
                batch_ordering_state: Mutex::new(batch_ordering_state),
                batch_ordering_store: store,
            }),
        }
    }

    /// Create an ordering over `store` with no recorded authorities.
    pub fn new_with_empty_state(store: DB) -> Self {
        Self::new(store, Default::default())
    }

    /// Try to accept a batch for the given authority.
    ///
    /// `seq == 0` means a pre-upgrade node - skip ordering, return [`AcceptResult::InOrder`].
    ///
    /// `seq_normalization` is the OutputSeqNormalization activation at the block being built: it
    /// gates the overflow prune, which changes replicated output, so every node must flip it at
    /// the same block.
    pub fn try_accept(
        &self,
        authority: Address,
        batch_seq: u64,
        prepared: PreparedBatch,
        seq_normalization: bool,
    ) -> AcceptResult {
        if batch_seq == 0 {
            return AcceptResult::InOrder(prepared);
        }

        let mut state = self.inner.batch_ordering_state.lock();
        let auth = state.authorities.entry(authority).or_default();

        if let Some(last_seq) = auth.last_executed_seq {
            if batch_seq <= last_seq {
                // stale re-proposal: parking would land under an already-executed seq where
                // drain_consecutive never looks; the dedup registry absorbs it instead
                info!(
                    target: "engine",
                    ?authority,
                    batch_seq,
                    last_seq,
                    batch_digest = ?prepared.batch_digest,
                    "stale-seq batch, deferring to dedup registry"
                );
                return AcceptResult::InOrder(prepared);
            }
            if batch_seq != last_seq + 1 {
                if auth.parked.len() >= MAX_PARKED_PER_AUTHORITY {
                    warn!(
                        target: "engine",
                        ?authority,
                        batch_seq,
                        parked_count = auth.parked.len(),
                        batch_digest = ?prepared.batch_digest,
                        "parking limit reached, executing batch out of order"
                    );
                    auth.last_executed_seq = Some(batch_seq);
                    // Drop the parked entries the forced jump abandoned: drain_consecutive only
                    // looks at last_executed_seq + 1, so they can never drain, and leaving them
                    // pins parked.len() at the limit (every later gap force-executes forever).
                    // Pre-fork the stranded entries still force-drain as blocks at the epoch
                    // boundary, so pruning them on only some validators would fork the chain
                    // there.
                    if seq_normalization {
                        auth.parked = auth.parked.split_off(&(batch_seq + 1));
                    }
                    return AcceptResult::OverflowForced(prepared);
                }

                warn!(
                    target: "engine",
                    ?authority,
                    batch_seq,
                    last_seq,
                    expected_seq = last_seq + 1,
                    batch_digest = ?prepared.batch_digest,
                    "parking out-of-order batch"
                );
                // Not registered in the dedup guard yet: the batch has not executed. Registration
                // happens when drain_consecutive releases it.
                auth.parked.insert(batch_seq, prepared);
                return AcceptResult::Parked;
            }
        }

        auth.last_executed_seq = Some(batch_seq);
        AcceptResult::InOrder(prepared)
    }

    /// Builds the ordering for `current_epoch`, preferring persisted state over a history reseed.
    ///
    /// Persisted state one epoch behind is kept, not reseeded: a restart between the boundary block
    /// and the next epoch's first output leaves the closing epoch's parked batches undrained (so
    /// `persisted.epoch` lags by one), and reseeding would drop them and fork the chain short.
    /// State further behind is stale and reseeded.
    pub fn from_history(store: DB, current_epoch: Epoch) -> Self {
        let mut state = store
            .read_batch_ordering_state()
            .expect("BatchOrdering: initial DB read failed")
            .unwrap_or_default();

        // `epoch + 1 == current_epoch` rather than `current_epoch - 1` to avoid genesis underflow.
        let valid_frame = state.epoch == current_epoch || state.epoch + 1 == current_epoch;
        if state.authorities.is_empty() || !valid_frame {
            let recovered = recover_authorities_from_history(&store, current_epoch);
            info!(
                target: "engine",
                current_epoch,
                persisted_epoch = state.epoch,
                authorities = recovered.len(),
                "BatchOrdering reseeding per-authority seqs from consensus history"
            );
            state.authorities = recovered;
            state.epoch = current_epoch;
        }
        Self::new(store, state)
    }

    /// Drains consecutive parked batches starting from the next expected seq into `collected`.
    ///
    /// With `check_dedup` each drained batch is registered in the dedup guard and skipped if
    /// already executed; without it the batches were registered at park time.
    pub fn drain_consecutive(
        &self,
        authority: Address,
        collected: &mut Vec<PreparedBatch>,
        executed_batch_registry: &ExecutedBatchRegistry,
        batch_tracker: Option<&Arc<BatchTracker>>,
        check_dedup: bool,
    ) {
        loop {
            let parked = {
                let mut state = self.inner.batch_ordering_state.lock();
                let Some(auth) = state.authorities.get_mut(&authority) else {
                    break;
                };
                let Some(last_seq) = auth.last_executed_seq else { break };
                let next_seq = last_seq + 1;
                match auth.parked.remove(&next_seq) {
                    Some(parked) => {
                        auth.last_executed_seq = Some(next_seq);
                        parked
                    }
                    None => break,
                }
            };

            if check_dedup {
                if !executed_batch_registry.try_register(parked.batch_digest, parked.output_digest)
                {
                    if let Some(tracker) = batch_tracker {
                        tracker.batch_deduped(parked.batch_digest);
                    }
                    continue;
                }
            }

            info!(
                target: "engine",
                ?authority,
                seq = parked.batch.seq,
                batch_digest = ?parked.batch_digest,
                "collecting previously parked batch for execution"
            );

            let mut drained = parked;
            drained.drained = true;
            collected.push(drained);
        }
    }

    /// Drains every parked batch on an epoch change, sorted by `(beneficiary, seq)` so all
    /// validators execute them in the same order.
    ///
    /// Returns an empty vec when the epoch has not advanced.
    pub fn drain_epoch(&self, new_epoch: Epoch) -> Vec<PreparedBatch> {
        let mut state = self.inner.batch_ordering_state.lock();
        match new_epoch.cmp(&state.epoch) {
            Ordering::Equal => return Vec::new(),
            // Never rewind: the epoch is a monotonic position. A crash after finalize but before
            // the next output can replay an already-passed epoch's output; draining and moving the
            // epoch backward here would double-drain the parked set and desync the frame.
            Ordering::Less => {
                warn!(
                    target: "engine",
                    current_epoch = state.epoch,
                    replayed_epoch = new_epoch,
                    "ignoring a batch-ordering drain for an already-passed epoch"
                );
                return Vec::new();
            }
            Ordering::Greater => {}
        }

        let total_parked: usize = state.authorities.values().map(|a| a.parked.len()).sum();
        let mut drained: Vec<PreparedBatch> = Vec::with_capacity(total_parked);
        for (authority, auth_state) in std::mem::take(&mut state.authorities) {
            for (seq, parked) in auth_state.parked {
                warn!(
                    target: "engine",
                    ?authority,
                    seq,
                    batch_digest = ?parked.batch_digest,
                    tx_count = parked.batch.transactions.len(),
                    "draining parked batch from previous epoch"
                );
                drained.push(parked);
            }
        }

        drained
            .sort_by(|a, b| a.beneficiary.cmp(&b.beneficiary).then(a.batch.seq.cmp(&b.batch.seq)));

        if !drained.is_empty() {
            warn!(
                target: "engine",
                old_epoch = state.epoch,
                new_epoch,
                drained_count = drained.len(),
                "epoch changed, drained parked batches from previous epoch"
            );
        } else {
            info!(
                target: "engine",
                old_epoch = state.epoch,
                new_epoch,
                "resetting batch ordering state for new epoch"
            );
        }
        state.epoch = new_epoch;
        drained
    }

    /// Write the current ordering state to the store.
    pub fn persist(&self) {
        // Snapshot under the lock, write outside it: holding the state lock across the DB write
        // stalls every try_accept/drain for the write's duration (the persist-starvation class).
        let state = self.inner.batch_ordering_state.lock().clone();
        self.inner
            .batch_ordering_store
            .write_batch_ordering_state(&state)
            .expect("DB write failed");
    }

    /// Returns the highest seq executed (or recovered) for `authority`, or `None` when no batch
    /// has been observed for it in the current epoch.
    pub fn last_executed_seq(&self, authority: Address) -> Option<u64> {
        self.inner.batch_ordering_state.lock().authorities.get(&authority)?.last_executed_seq
    }

    /// Returns the number of authorities tracked in the current epoch.
    pub fn tracked_authorities(&self) -> usize {
        self.inner.batch_ordering_state.lock().authorities.len()
    }

    /// Returns the number of parked batches for `authority`.
    pub fn parked_count(&self, authority: Address) -> usize {
        self.inner
            .batch_ordering_state
            .lock()
            .authorities
            .get(&authority)
            .map(|a| a.parked.len())
            .unwrap_or(0)
    }
}

/// Walks `ConsensusBlocks` in reverse, accumulating the per-beneficiary max `batch.seq` for
/// `current_epoch`; stops at the first block of an earlier epoch. Returns an empty map when the
/// store holds no blocks for `current_epoch`.
fn recover_authorities_from_history<DB: Database>(
    store: &DB,
    current_epoch: Epoch,
) -> BTreeMap<Address, AuthoritySeqState> {
    let by_addr = store
        .with_read_txn(|txn| {
            // The walk covers a whole epoch of ConsensusBlocks on a cold boot; under the default
            // long-read cap a slow disk aborts the txn mid-walk. Opt out like the catch-up
            // accumulator does: this runs once at boot before any writer contends.
            txn.disable_long_read_safety();
            let mut by_addr: BTreeMap<Address, u64> = BTreeMap::new();
            for (_key, value) in txn.reverse_raw_iter::<ConsensusBlocks>() {
                let (leader_epoch, batch_digests) = leader_epoch_and_batch_digests(&value)?;
                if leader_epoch < current_epoch {
                    break;
                }
                for batch_digest in batch_digests {
                    let Ok(Some(batch)) = txn.get::<Batches>(&batch_digest) else { continue };
                    // batch.beneficiary must equal the live path's key,
                    // authority_execution_address(cert.origin()). If they diverge, a restart
                    // seeds watermarks under addresses the live path never looks up, silently
                    // disabling gap detection.
                    by_addr
                        .entry(batch.beneficiary)
                        .and_modify(|s| *s = (*s).max(batch.seq))
                        .or_insert(batch.seq);
                }
            }
            Ok(by_addr)
        })
        // Fail closed: with the cap lifted a failure here is a real DB error, and booting with
        // empty watermarks silently disables gap detection for the epoch.
        .expect("BatchOrdering: consensus-history read failed on boot");

    debug!(
        target: "engine",
        current_epoch,
        authorities = by_addr.len(),
        "BatchOrdering history walk complete"
    );

    by_addr
        .into_iter()
        .map(|(addr, seq)| {
            (addr, AuthoritySeqState { last_executed_seq: Some(seq), parked: BTreeMap::new() })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexmap::IndexMap;
    use rayls_infrastructure_storage::{
        mem_db::MemDatabase, tables::ConsensusBlocks as ConsensusBlocksTable,
    };
    use rayls_infrastructure_types::{
        Batch, BlockHash, Certificate, CommittedSubDag, ConsensusHeader, ReputationScores, WorkerId,
    };

    fn make_batch(beneficiary: Address, seq: u64) -> Batch {
        Batch {
            transactions: vec![],
            epoch: 0,
            beneficiary,
            base_fee_per_gas: 7,
            worker_id: 0,
            seq,
            received_at: None,
        }
    }

    fn write_consensus_block(
        store: &MemDatabase,
        block_num: u64,
        epoch: Epoch,
        beneficiary: Address,
        seq: u64,
    ) {
        let batch = make_batch(beneficiary, seq);
        let digest = batch.digest();
        store.insert::<Batches>(&digest, &batch).expect("insert batch");

        let mut leader = Certificate::default();
        leader.header.epoch = epoch;
        leader.header.round = 1;
        let mut payload: IndexMap<BlockHash, WorkerId> = IndexMap::new();
        payload.insert(digest, 0);
        leader.header_mut_for_test().update_payload_for_test(payload);

        let sub_dag = CommittedSubDag::new(
            vec![leader.clone()],
            leader,
            block_num,
            ReputationScores::default(),
            None,
        );
        let header = ConsensusHeader {
            parent_hash: BlockHash::default(),
            sub_dag,
            number: block_num,
            extra: BlockHash::default(),
        };
        store.insert::<ConsensusBlocksTable>(&block_num, &header).expect("insert block");
    }

    #[test]
    fn recover_walker_picks_up_max_seq_per_beneficiary_in_current_epoch() {
        // reverse-iter visits blocks 4, 3, 2, 1; block 1 (epoch 4) trips the
        // epoch-boundary break so prior-epoch seqs stay out of the result.
        let store = MemDatabase::default();
        let auth_a = Address::from([1u8; 20]);
        let auth_b = Address::from([2u8; 20]);

        write_consensus_block(&store, 1, 4, auth_a, 99); // prior epoch - ignored
        write_consensus_block(&store, 2, 5, auth_a, 10);
        write_consensus_block(&store, 3, 5, auth_a, 11);
        write_consensus_block(&store, 4, 5, auth_b, 42);

        let recovered = recover_authorities_from_history(&store, 5);
        assert_eq!(recovered.len(), 2, "two distinct beneficiaries in epoch 5");
        assert_eq!(recovered.get(&auth_a).unwrap().last_executed_seq, Some(11));
        assert_eq!(recovered.get(&auth_b).unwrap().last_executed_seq, Some(42));
    }

    #[test]
    fn recover_walker_empty_store_returns_empty() {
        let store = MemDatabase::default();
        let recovered = recover_authorities_from_history(&store, 5);
        assert!(recovered.is_empty());
    }

    #[test]
    fn new_with_history_recovery_seeds_state_when_persisted_missing() {
        let store = MemDatabase::default();
        let auth = Address::from([7u8; 20]);
        write_consensus_block(&store, 1, 3, auth, 100);

        let ord = BatchOrdering::from_history(store, 3);
        let state = ord.inner.batch_ordering_state.lock();
        assert_eq!(state.epoch, 3);
        assert_eq!(state.authorities.get(&auth).unwrap().last_executed_seq, Some(100));
    }

    #[test]
    fn new_with_history_recovery_preserves_persisted_when_present() {
        let store = MemDatabase::default();
        let auth = Address::from([7u8; 20]);

        let mut existing = BatchOrderingState::default();
        existing.epoch = 3;
        existing.authorities.insert(
            auth,
            AuthoritySeqState { last_executed_seq: Some(50), parked: BTreeMap::new() },
        );
        store.write_batch_ordering_state(&existing).expect("seed");

        write_consensus_block(&store, 1, 3, auth, 999);

        let ord = BatchOrdering::from_history(store, 3);
        let state = ord.inner.batch_ordering_state.lock();
        assert_eq!(state.authorities.get(&auth).unwrap().last_executed_seq, Some(50));
    }

    fn make_parked(beneficiary: Address, seq: u64) -> PreparedBatch {
        let batch = make_batch(beneficiary, seq);
        let batch_digest = batch.digest();
        PreparedBatch {
            batch: Arc::new(batch),
            batch_digest,
            beneficiary,
            output_digest: BlockHash::default(),
            output_nonce: 0,
            timestamp: 0,
            epoch: 87,
            worker_id: 0,
            batch_index: 0,
            drained: false,
            gas_limit: 30_000_000,
        }
    }

    #[test]
    fn from_history_keeps_closing_epoch_parked_batches_across_mid_transition_restart() {
        // Mid-transition restart: tip at closing+1 but the closing epoch's parked batches are still
        // undrained, so persisted.epoch (87) lags current_epoch (88). Recovery must keep them, not
        // reseed - dropping them shortens the chain below the network and wedges catch-up.
        let store = MemDatabase::default();
        let auth = Address::from([3u8; 20]);

        let mut persisted = BatchOrderingState { epoch: 87, ..Default::default() };
        let parked: BTreeMap<u64, PreparedBatch> =
            (1436..=1440u64).map(|seq| (seq, make_parked(auth, seq))).collect();
        // Production precondition: every committed batch's body row exists in the store the
        // ordering blob lives in; the by-digest persistence reloads bodies from those rows.
        for prepared in parked.values() {
            store.insert::<Batches>(&prepared.batch_digest, &prepared.batch).expect("seed body");
        }
        persisted
            .authorities
            .insert(auth, AuthoritySeqState { last_executed_seq: Some(1434), parked });
        store.write_batch_ordering_state(&persisted).expect("seed persisted state");

        // A history reseed against the post-boundary epoch would rebuild empty parked.
        write_consensus_block(&store, 1, 88, auth, 1441);

        let ord = BatchOrdering::from_history(store, 88);

        {
            let state = ord.inner.batch_ordering_state.lock();
            assert_eq!(state.epoch, 87, "must keep the closing-epoch frame, not reseed to 88");
            let parked = &state.authorities.get(&auth).expect("authority preserved").parked;
            assert_eq!(parked.len(), 5, "all five parked batches must survive the restart");
        }

        // the boundary drain on the first new-epoch output flushes them as their own blocks
        let drained = ord.drain_epoch(88);
        assert_eq!(drained.len(), 5, "parked batches drain into the new epoch");
    }

    #[test]
    fn overflow_forced_drops_the_stranded_parked_entries() {
        // Fill the parking budget with a gap, then force one batch far past it. The forced jump
        // abandons every parked seq below it; leaving them in the map orphans them forever
        // (drain_consecutive only looks at last_executed_seq + 1) and pins parked.len() at the
        // limit, so ordering stays off for the authority for the rest of the epoch.
        let store = MemDatabase::default();
        let ord = BatchOrdering::new_with_empty_state(store);
        let auth = Address::from([1u8; 20]);

        // seq 1 executes; the following seqs park behind the missing seq 2 (exactly the limit).
        assert!(matches!(
            ord.try_accept(auth, 1, make_parked(auth, 1), false),
            AcceptResult::InOrder(_)
        ));
        for seq in 3..=(2 + MAX_PARKED_PER_AUTHORITY as u64) {
            assert!(matches!(
                ord.try_accept(auth, seq, make_parked(auth, seq), false),
                AcceptResult::Parked
            ));
        }
        assert_eq!(ord.parked_count(auth), MAX_PARKED_PER_AUTHORITY);

        // the next gapped batch trips the overflow and forces execution at seq 100.
        assert!(matches!(
            ord.try_accept(auth, 100, make_parked(auth, 100), true),
            AcceptResult::OverflowForced(_)
        ));
        assert_eq!(ord.last_executed_seq(auth), Some(100));

        // the invariant "parked only holds seqs > last_executed_seq" must hold: the abandoned
        // sub-100 entries are gone, not stranded.
        assert_eq!(ord.parked_count(auth), 0, "stranded parked entries must be dropped");

        // and ordering is live again: a fresh gap parks rather than force-executing.
        assert!(matches!(
            ord.try_accept(auth, 200, make_parked(auth, 200), true),
            AcceptResult::Parked
        ));
    }

    #[test]
    fn overflow_forced_keeps_stranded_entries_pre_fork() {
        // Pre-fork pin: un-upgraded peers still boundary-force-drain the stranded entries as
        // blocks, so the prune must not fire until OutputSeqNormalization activates.
        let store = MemDatabase::default();
        let ord = BatchOrdering::new_with_empty_state(store);
        let auth = Address::from([1u8; 20]);

        assert!(matches!(
            ord.try_accept(auth, 1, make_parked(auth, 1), false),
            AcceptResult::InOrder(_)
        ));
        for seq in 3..=(2 + MAX_PARKED_PER_AUTHORITY as u64) {
            assert!(matches!(
                ord.try_accept(auth, seq, make_parked(auth, seq), false),
                AcceptResult::Parked
            ));
        }
        assert!(matches!(
            ord.try_accept(auth, 100, make_parked(auth, 100), false),
            AcceptResult::OverflowForced(_)
        ));
        assert_eq!(
            ord.parked_count(auth),
            MAX_PARKED_PER_AUTHORITY,
            "pre-fork the stranded entries stay parked for the boundary force-drain"
        );
    }

    #[test]
    fn drain_epoch_never_rewinds_to_an_already_passed_epoch() {
        // A crash after finalize but before the next output can replay an older epoch's output.
        // drain_epoch for that stale epoch must be a no-op: it must not drain the current parked
        // set nor move the epoch backward (the epoch is a monotonic position).
        let store = MemDatabase::default();
        let ord = BatchOrdering::new_with_empty_state(store);
        let auth = Address::from([1u8; 20]);

        // advance to epoch 88 with one parked batch belonging to it
        assert_eq!(ord.drain_epoch(88).len(), 0);
        assert!(matches!(
            ord.try_accept(auth, 1, make_parked(auth, 1), false),
            AcceptResult::InOrder(_)
        ));
        assert!(matches!(
            ord.try_accept(auth, 3, make_parked(auth, 3), false),
            AcceptResult::Parked
        ));

        // replay of an already-passed epoch: no drain, no rewind
        let drained = ord.drain_epoch(87);
        assert!(drained.is_empty(), "a stale-epoch drain must not drain the current parked set");
        assert_eq!(ord.parked_count(auth), 1, "the current epoch's parked batch must survive");
        assert_eq!(
            ord.inner.batch_ordering_state.lock().epoch,
            88,
            "the epoch must not move backward"
        );
    }
}
