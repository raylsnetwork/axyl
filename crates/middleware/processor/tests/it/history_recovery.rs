//! Integration tests for `BatchOrdering::from_history`.
//!
//! Pre-populates `ConsensusBlocks` + `Batches`, then verifies seeding,
//! ordering enforcement, epoch filtering, and gap-batch parking.

#![allow(dead_code, unreachable_pub)]

use std::time::Instant;

use indexmap::IndexMap;
use rayls_infrastructure_storage::{
    open_db,
    tables::{Batches, ConsensusBlocks as ConsensusBlocksTable},
    DatabaseType,
};
use rayls_infrastructure_types::{
    AcceptResult, Address, Batch, BlockHash, Certificate, CommittedSubDag, ConsensusHeader,
    Database, DbTxMut, Epoch, PreparedBatch, ReputationScores, WorkerId, B256,
};
use rayls_middleware_processor::batch::BatchOrdering;
use tempfile::TempDir;

/// Production-equivalent epoch duration: 24h at ~1 block/sec.
const EPOCH_BLOCKS: u64 = 24 * 60 * 60;

/// Pre-populated `ConsensusBlocks` + `Batches` fixture backed by the production
/// layered DB (in-memory cache over MDBX).
///
/// Each [`Self::append_block`] writes one synthetic `ConsensusHeader` whose
/// sub_dag carries one certificate per `(beneficiary, seqs)` entry. Each seq
/// gets a real `Batch` row in the `Batches` CF so the walker can resolve the
/// payload digests back to batches.
struct ConsensusHistoryFixture {
    _tmp_dir: TempDir,
    db: DatabaseType,
    next_block_num: u64,
}

impl ConsensusHistoryFixture {
    fn new() -> Self {
        let tmp_dir = TempDir::new().expect("create tempdir");
        let db = open_db(tmp_dir.path());
        Self { _tmp_dir: tmp_dir, db, next_block_num: 0 }
    }

    fn db(&self) -> &DatabaseType {
        &self.db
    }

    /// Bulk-append `count` blocks with a deterministic but scattered author
    /// distribution (seeded LCG). Per-author seqs still advance monotonically
    /// for whichever author each block picks, but cluster sizes and gaps
    /// vary, simulating realistic best/worst-case access patterns for the
    /// recovery walker.
    ///
    /// Returns the final per-author seq.
    fn append_scattered_blocks(
        &mut self,
        epoch: Epoch,
        count: u64,
        authors: &[Address],
        seed: u64,
    ) -> Vec<u64> {
        assert!(!authors.is_empty(), "need at least one author");
        let mut rng = seed;
        let mut seqs = vec![0u64; authors.len()];
        let start_block_num = self.next_block_num;
        self.db
            .with_write_txn(|txn| {
                for i in 0..count {
                    rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                    let idx = ((rng >> 33) as usize) % authors.len();
                    seqs[idx] += 1;
                    let block_num = start_block_num + i;
                    let beneficiary = authors[idx];
                    let seq = seqs[idx];

                    let batch = Batch {
                        transactions: vec![],
                        epoch,
                        beneficiary,
                        base_fee_per_gas: 7,
                        worker_id: 0,
                        seq,
                        received_at: None,
                    };
                    let digest = batch.digest();
                    txn.insert::<Batches>(&digest, &batch)?;

                    let mut payload: IndexMap<BlockHash, WorkerId> = IndexMap::new();
                    payload.insert(digest, 0);
                    let mut cert = Certificate::default();
                    cert.header.epoch = epoch;
                    cert.header.round = (block_num as u32) + 1;
                    cert.header_mut_for_test().update_payload_for_test(payload);
                    let leader = cert.clone();
                    let sub_dag = CommittedSubDag::new(
                        vec![cert],
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
                    txn.insert::<ConsensusBlocksTable>(&block_num, &header)?;
                }
                Ok(())
            })
            .expect("bulk write txn for scattered blocks");
        self.next_block_num = start_block_num + count;
        self.db.sync_persist().expect("persist");
        seqs
    }

    /// Append one `ConsensusHeader` at `epoch`, with one cert per beneficiary
    /// and one `Batch` per seq.
    fn append_block(&mut self, epoch: Epoch, certs: Vec<(Address, Vec<u64>)>) -> u64 {
        let block_num = self.next_block_num;
        self.next_block_num += 1;

        let mut sub_dag_certificates = Vec::with_capacity(certs.len());
        for (beneficiary, seqs) in certs {
            let mut payload: IndexMap<BlockHash, WorkerId> = IndexMap::new();
            for seq in seqs {
                let batch = Batch {
                    transactions: vec![],
                    epoch,
                    beneficiary,
                    base_fee_per_gas: 7,
                    worker_id: 0,
                    seq,
                    received_at: None,
                };
                let digest = batch.digest();
                self.db.insert::<Batches>(&digest, &batch).expect("insert batch");
                payload.insert(digest, 0);
            }
            let mut cert = Certificate::default();
            cert.header.epoch = epoch;
            cert.header.round = (block_num as u32) + 1;
            cert.header_mut_for_test().update_payload_for_test(payload);
            sub_dag_certificates.push(cert);
        }

        let leader = sub_dag_certificates.first().cloned().unwrap_or_default();
        let sub_dag = CommittedSubDag::new(
            sub_dag_certificates,
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
        self.db.insert::<ConsensusBlocksTable>(&block_num, &header).expect("insert block");
        block_num
    }
}

fn make_prepared(beneficiary: Address, seq: u64) -> PreparedBatch {
    PreparedBatch {
        batch: std::sync::Arc::new(Batch {
            transactions: vec![],
            epoch: 5,
            beneficiary,
            base_fee_per_gas: 7,
            worker_id: 0,
            seq,
            received_at: None,
        }),
        batch_digest: B256::from([seq as u8; 32]),
        beneficiary,
        output_digest: B256::from([0x42; 32]),
        output_nonce: 0,
        timestamp: 0,
        epoch: 5,
        worker_id: 0,
        batch_index: 0,
        drained: false,
        gas_limit: 30_000_000,
    }
}

const AUTH_A: Address = Address::new([0xAA; 20]);
const AUTH_B: Address = Address::new([0xBB; 20]);
const EPOCH: Epoch = 5;

/// Verify recovery seeds state and `try_accept` enforces ordering end-to-end.
///
/// Builds a history of 3 blocks in epoch 5 carrying batches from two
/// beneficiaries. Recovery should produce `A -> 3` and `B -> 2`. Then drives
/// `try_accept` through every branch (in-order, gap-park, stale-seq dedup,
/// drain-after-fill) and checks the outcome.
#[tokio::test]
async fn recovery_seeds_state_and_enforces_ordering() {
    let mut fixture = ConsensusHistoryFixture::new();
    fixture.append_block(EPOCH, vec![(AUTH_A, vec![1]), (AUTH_B, vec![1])]);
    fixture.append_block(EPOCH, vec![(AUTH_A, vec![2])]);
    fixture.append_block(EPOCH, vec![(AUTH_A, vec![3]), (AUTH_B, vec![2])]);

    let ord = BatchOrdering::from_history(fixture.db().clone(), EPOCH);

    // in-order accept for AUTH_A at seq=4 (last=3 -> next=4)
    let r = ord.try_accept(AUTH_A, 4, make_prepared(AUTH_A, 4));
    assert!(matches!(r, AcceptResult::InOrder(_)), "expected InOrder, got {:?}", r);

    // gap: AUTH_A at seq=6 (last is now 4, gap of 1)
    let r = ord.try_accept(AUTH_A, 6, make_prepared(AUTH_A, 6));
    assert!(matches!(r, AcceptResult::Parked), "expected Parked, got {:?}", r);

    // stale: AUTH_A at seq=2 (already executed pre-recovery)
    let r = ord.try_accept(AUTH_A, 2, make_prepared(AUTH_A, 2));
    assert!(
        matches!(r, AcceptResult::InOrder(_)),
        "stale-seq must defer to dedup (returns InOrder), got {:?}",
        r
    );

    // AUTH_B independent: last=2 -> seq=3 in-order
    let r = ord.try_accept(AUTH_B, 3, make_prepared(AUTH_B, 3));
    assert!(matches!(r, AcceptResult::InOrder(_)), "expected InOrder, got {:?}", r);

    // fill gap for AUTH_A: seq=5 is the missing one (last=4)
    let r = ord.try_accept(AUTH_A, 5, make_prepared(AUTH_A, 5));
    assert!(matches!(r, AcceptResult::InOrder(_)), "expected InOrder, got {:?}", r);

    // drain_consecutive should pick up the parked seq=6
    let mut drained: Vec<PreparedBatch> = Vec::new();
    let registry =
        rayls_infrastructure_types::executed_batch_registry::ExecutedBatchRegistry::default();
    ord.drain_consecutive(AUTH_A, &mut drained, &registry, None, true);
    assert_eq!(drained.len(), 1, "expected the parked seq=6 to drain after seq=5");
    assert_eq!(drained[0].batch.seq, 6);
}

/// Filter out prior-epoch seqs when seeding current-epoch state.
#[tokio::test]
async fn recovery_filters_prior_epoch_blocks() {
    let mut fixture = ConsensusHistoryFixture::new();
    fixture.append_block(EPOCH - 1, vec![(AUTH_A, vec![99])]); // prior epoch - ignored
    fixture.append_block(EPOCH, vec![(AUTH_A, vec![10])]);

    let ord = BatchOrdering::from_history(fixture.db().clone(), EPOCH);

    // last for AUTH_A should be 10 (from current epoch), not 99 (prior)
    let r = ord.try_accept(AUTH_A, 11, make_prepared(AUTH_A, 11));
    assert!(
        matches!(r, AcceptResult::InOrder(_)),
        "seq=11 with last=10 must be in-order, got {:?}",
        r
    );

    // seq=12 with last=11 still in-order
    let r = ord.try_accept(AUTH_A, 12, make_prepared(AUTH_A, 12));
    assert!(matches!(r, AcceptResult::InOrder(_)));

    // seq=100 (huge gap from 12) must park, NOT be accepted in-order
    let r = ord.try_accept(AUTH_A, 100, make_prepared(AUTH_A, 100));
    assert!(
        matches!(r, AcceptResult::Parked),
        "huge gap must park; if InOrder we recovered the prior-epoch seq incorrectly, got {:?}",
        r
    );
}

/// Treat the first batch as inaugural when persisted state and history are empty.
///
/// Matches the existing first-batch semantics for an empty authorities map.
#[tokio::test]
async fn recovery_empty_history_inaugural_first_batch() {
    let fixture = ConsensusHistoryFixture::new();

    let ord = BatchOrdering::from_history(fixture.db().clone(), EPOCH);

    // no prior state for AUTH_A; first batch at any seq is the inaugural one
    let r = ord.try_accept(AUTH_A, 5, make_prepared(AUTH_A, 5));
    assert!(matches!(r, AcceptResult::InOrder(_)));

    // subsequent seq=4 is below last=5 -> stale (deferred to dedup)
    let r = ord.try_accept(AUTH_A, 4, make_prepared(AUTH_A, 4));
    assert!(matches!(r, AcceptResult::InOrder(_)), "stale-seq must defer to dedup, got {:?}", r);
}

/// Verify gap-batch parking when no persisted state exists on restart.
///
/// Restarts mid-epoch with no persisted ordering state, then receives a
/// quorum-certified batch at `seq=N+3` from an authority that committed up
/// to `seq=N` on the chain.
///
/// Without recovery, the post-restart engine would see `last_executed_seq=None`
/// and accept the gap batch in-order, jumping the counter and forking.
/// With recovery wired, the walker reconstructs `last_executed_seq=N` from
/// `ConsensusBlocks`, and the gap batch parks correctly.
#[tokio::test]
async fn recovery_parks_gap_batch_after_state_loss() {
    let mut fixture = ConsensusHistoryFixture::new();
    // chain has committed seqs 1..=5 from AUTH_A in the current epoch
    for seq in 1..=5 {
        fixture.append_block(EPOCH, vec![(AUTH_A, vec![seq])]);
    }

    // recovery seeds last_executed_seq[AUTH_A] = 5
    let ord = BatchOrdering::from_history(fixture.db().clone(), EPOCH);

    // inject the gap batch: seq jumps from None/5 to N+3=8
    let r = ord.try_accept(AUTH_A, 8, make_prepared(AUTH_A, 8));

    // without recovery: InOrder, jumping last to 8; with recovery: Parked
    assert!(
        matches!(r, AcceptResult::Parked),
        "recovery must park the gap batch (last=5, seq=8); got {:?}",
        r
    );
}

/// Walk a full epoch (~86,400 blocks at 1 block/sec) and verify recovery
/// returns correct per-authority seqs at production scale.
///
/// Verifies post-recovery ordering enforcement still works at boundary seqs.
#[tokio::test]
async fn recovery_at_full_epoch_scale_86400_blocks() {
    let mut fixture = ConsensusHistoryFixture::new();

    let auth_a = Address::new([0x0A; 20]);
    let auth_b = Address::new([0x0B; 20]);
    let auth_c = Address::new([0x0C; 20]);
    let auth_d = Address::new([0x0D; 20]);
    let authors = [auth_a, auth_b, auth_c, auth_d];

    let setup_start = Instant::now();
    let final_seqs = fixture.append_scattered_blocks(EPOCH, EPOCH_BLOCKS, &authors, 0xDEAD_BEEF);
    let setup_elapsed = setup_start.elapsed();
    println!(
        "\nsetup: {} blocks scattered across {} authors in {:.2}s ({:.0} blocks/sec)",
        EPOCH_BLOCKS,
        authors.len(),
        setup_elapsed.as_secs_f64(),
        EPOCH_BLOCKS as f64 / setup_elapsed.as_secs_f64()
    );
    let total: u64 = final_seqs.iter().sum();
    for (i, seq) in final_seqs.iter().enumerate() {
        let pct = (*seq as f64 / total as f64) * 100.0;
        println!("  author[{i}] final seq = {seq:>6} ({pct:>5.1}% of blocks)");
    }

    let recover_start = Instant::now();
    let ord = BatchOrdering::from_history(fixture.db().clone(), EPOCH);
    let recover_elapsed = recover_start.elapsed();
    println!(
        "recovery: walked ~{} blocks in {:.3}s ({:.0} blocks/sec)",
        EPOCH_BLOCKS,
        recover_elapsed.as_secs_f64(),
        EPOCH_BLOCKS as f64 / recover_elapsed.as_secs_f64()
    );

    // direct credibility checks: walker output must match what the fixture wrote
    assert_eq!(
        ord.tracked_authorities(),
        authors.len(),
        "walker should track exactly {} authorities; tracking {}",
        authors.len(),
        ord.tracked_authorities()
    );
    for (i, (auth, expected_last)) in authors.iter().zip(final_seqs.iter()).enumerate() {
        let observed = ord.last_executed_seq(*auth);
        assert_eq!(
            observed,
            Some(*expected_last),
            "author[{i}] last_executed_seq mismatch: walker returned {observed:?}, fixture wrote {expected_last}"
        );
        assert_eq!(
            ord.parked_count(*auth),
            0,
            "author[{i}] should have zero parked batches after recovery"
        );
    }
    println!(
        "credibility: walker output matches fixture exactly for all {} authors",
        authors.len()
    );

    for (auth, expected_last) in authors.iter().zip(final_seqs.iter()) {
        // next consecutive seq must be in-order (last + 1)
        let next = expected_last + 1;
        let r = ord.try_accept(*auth, next, make_prepared(*auth, next));
        assert!(
            matches!(r, AcceptResult::InOrder(_)),
            "auth at last={expected_last}, seq={next} must be in-order; got {r:?}"
        );

        // after the accept above, last advanced to `next`; a gap of 2 must park
        let gap = next + 2;
        let r = ord.try_accept(*auth, gap, make_prepared(*auth, gap));
        assert!(
            matches!(r, AcceptResult::Parked),
            "auth at last={next}, seq={gap} must park; got {r:?}"
        );

        // stale-seq below the boundary defers to dedup
        let stale = expected_last / 2;
        let r = ord.try_accept(*auth, stale, make_prepared(*auth, stale));
        assert!(
            matches!(r, AcceptResult::InOrder(_)),
            "auth at last={next}, seq={stale} stale-seq must defer to dedup (returns InOrder); got {r:?}"
        );
    }
}
