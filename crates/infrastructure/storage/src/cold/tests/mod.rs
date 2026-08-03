//! Cold-tier regression tests, built against the node's `LayeredDatabase<MdbxDatabase>` shape
//! (cold layer attached) and so MDBX-only.

#![cfg(feature = "reth-libmdbx")]

use std::{collections::BTreeSet, sync::Arc};

use rayls_infrastructure_types::{
    encode, Batch, BlockHash, Certificate, CommittedSubDag, ConsensusHeader, Database, DbTx,
    DbTxMut, Epoch, Header, ReputationScores,
};

#[allow(unused_imports)]
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};
use tempfile::TempDir;

// The fn is imported through its module path so the name lands in the value namespace only;
// `use crate::cold::reconcile` would also import the module and collide with `mod reconcile`
// below.
use crate::{
    cold::{
        archive_below_epoch, reconcile::reconcile, ArchiveStats, ColdArchiver, ColdConfig,
        ColdError, ColdLocation, ColdStore, SealOutcome, ARCHIVE_HIGH_WATER_MARK_KEY,
    },
    layered_db::LayeredDatabase,
    mdbx::MdbxDatabase,
    open_default_tables,
    tables::{Batches, ColdArchiveHighWaterMark, ColdBatchLocations, ConsensusBlocks},
};

mod archive;
mod finalize;
mod layered;
mod reconcile;
mod seal;
mod tx;

/// Number of epochs the fixture spans, including the recent epoch that must stay hot.
const EPOCHS: Epoch = 4;

/// Number of consensus blocks (and one batch each) per epoch.
const BLOCKS_PER_EPOCH: u64 = 6;

/// The node's `DatabaseType` shape: the layered database with the cold layer attached.
type TestDb = LayeredDatabase<MdbxDatabase>;

/// The hot-only view of [`TestDb`] (what the producer reads and writes through).
type HotDb = LayeredDatabase<MdbxDatabase>;

/// One synthetic consensus block plus the single batch its sub-dag references.
struct Fixture {
    /// Dense consensus-block number, also the `ConsensusBlocks` key.
    number: u64,
    /// Epoch the block belongs to, carried by the sub-dag leader.
    epoch: Epoch,
    /// Digest of the referenced batch, also the `Batches` key.
    digest: BlockHash,
    /// Header archived to the consensus_blocks segment.
    header: ConsensusHeader,
    /// Batch archived to the batches segment.
    batch: Batch,
}

/// Builds a synthetic batch with deterministic, content-addressed contents.
fn batch_for(number: u64, epoch: Epoch) -> Batch {
    Batch {
        // Distinct per block so the byte-identical readback assertion is meaningful.
        transactions: vec![vec![number as u8; 1 + (number as usize % 7)]],
        epoch,
        worker_id: 0,
        seq: number,
        ..Default::default()
    }
}

/// Builds a consensus header whose sub-dag leader fixes the epoch and whose certificate payload
/// references `digest`, mirroring how the producer recovers an epoch and its batch digests.
fn header_for(number: u64, epoch: Epoch, digest: BlockHash) -> ConsensusHeader {
    // The payload field is an IndexMap; collect into it via the struct field's declared type so the
    // test needs no direct indexmap dependency.
    let payload = std::iter::once((digest, 0u16)).collect();

    // The producer reads the epoch from the sub-dag leader and the batch digests from each
    // certificate's header payload, so both the leader and a referencing certificate carry it.
    // `Certificate`/`CommittedSubDag` keep private signature fields, so build them through
    // `Default` plus the public `header` field and the `CommittedSubDag::new` constructor.
    let mut leader = Certificate::default();
    leader.header = Header { epoch, ..Default::default() };
    let mut cert = Certificate::default();
    cert.header = Header { epoch, payload, ..Default::default() };

    let sub_dag = CommittedSubDag::new(vec![cert], leader, 0, ReputationScores::default(), None);
    ConsensusHeader { sub_dag, number, ..Default::default() }
}

/// Builds the per-epoch block/batch fixtures, dense and gapless by number.
fn build_fixtures() -> Vec<Fixture> {
    let mut out = Vec::with_capacity((EPOCHS as u64 * BLOCKS_PER_EPOCH) as usize);
    let mut number = 0u64;
    for epoch in 0..EPOCHS {
        for _ in 0..BLOCKS_PER_EPOCH {
            // Content-address the digest so equal blocks never collide across epochs.
            let digest = BlockHash::from(digest_seed(number, epoch));
            let header = header_for(number, epoch, digest);
            let batch = batch_for(number, epoch);
            out.push(Fixture { number, epoch, digest, header, batch });
            number += 1;
        }
    }
    out
}

/// Produces a collision-free 32-byte digest seed from a (number, epoch) pair.
fn digest_seed(number: u64, epoch: Epoch) -> [u8; 32] {
    let mut seed = [0u8; 32];
    seed[..8].copy_from_slice(&number.to_be_bytes());
    seed[8..12].copy_from_slice(&epoch.to_be_bytes());
    // A non-zero tail keeps the digest distinct from the all-zero sentinel.
    seed[31] = 0xA5;
    seed
}

/// Opens a fresh cold-attached `LayeredDatabase<MdbxDatabase>` rooted under `tmp`.
///
/// Returns the tiered handle (what the node reads through) alongside its hot-only view. The
/// producer reads and writes through the hot view so its reads never fall through to cold; both
/// handles share the same `Arc<ColdStore>` and the single `db_run` writer, so newly sealed jars
/// are visible through the cold fall-through within the process.
fn open_test_db(tmp: &TempDir) -> (TestDb, HotDb) {
    let mdbx = MdbxDatabase::open(tmp.path().join("hot")).expect("open mdbx");
    let cold = ColdStore::open(&ColdConfig { dir: tmp.path().join("cold") }).expect("open cold");
    let mut db = LayeredDatabase::open(mdbx).with_cold(Arc::new(cold));
    open_default_tables(&mut db).expect("open tables");
    let hot = db.without_cold();
    (db, hot)
}

/// Inserts every fixture's batch and consensus block into the hot tier.
fn seed_hot(hot: &HotDb, fixtures: &[Fixture]) {
    hot.with_write_txn(|txn| {
        for f in fixtures {
            txn.insert::<Batches>(&f.digest, &f.batch)?;
            txn.insert::<ConsensusBlocks>(&f.number, &f.header)?;
        }
        Ok(())
    })
    .expect("seed hot");
    hot.sync_persist();
}

/// Counts the rows of a table in the bare MDBX whose key satisfies `keep`.
///
/// Probes the raw [`MdbxDatabase`] (`hot.inner()`) directly rather than the layered or cold stack:
/// boundedness is a statement about hot occupancy, so it must read the persistent layer only, not a
/// merged or cold-chained view. The caller flushes the layered write queue with `sync_persist`
/// first.
fn count_hot_rows<T, F>(mdbx: &MdbxDatabase, keep: F) -> usize
where
    T: rayls_infrastructure_types::Table,
    F: Fn(&T::Key) -> bool,
{
    mdbx.iter::<T>().filter(|(k, _)| keep(k)).count()
}

/// Digests shared by the chunked-seal tests; [`DIGEST_A`] recurs across a chunk boundary.
const DIGEST_A: BlockHash = BlockHash::repeat_byte(0xA1);
/// Second chunked-seal fixture digest.
const DIGEST_B: BlockHash = BlockHash::repeat_byte(0xB2);
/// Third chunked-seal fixture digest, appended after the cross-chunk dedup skip.
const DIGEST_C: BlockHash = BlockHash::repeat_byte(0xC3);

/// Seeds the hot tier with single-digest `blocks` and `batches` as `(digest, batch number)`.
fn seed_chunk_blocks(hot: &HotDb, blocks: &[(u64, Vec<BlockHash>)], batches: &[(BlockHash, u64)]) {
    hot.with_write_txn(|txn| {
        for (digest, number) in batches {
            txn.insert::<Batches>(digest, &batch_for(*number, 0))?;
        }
        for (number, digests) in blocks {
            txn.insert::<ConsensusBlocks>(number, &header_for(*number, 0, digests[0]))?;
        }
        Ok(())
    })
    .expect("seed hot");
    hot.sync_persist();
}
