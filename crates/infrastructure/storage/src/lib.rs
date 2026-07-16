// SPDX-License-Identifier: BUSL-1.1
//! Persistent storage types

mod stores;
use layered_db::LayeredDatabase;
#[cfg(feature = "reth-libmdbx")]
use mdbx::MdbxDatabase;
use rayls_infrastructure_types::Database;
pub use stores::*;
// Always build redb, we use it as the default for persistant consensus data.
pub use redb::database::ReDB;
use tables::{
    BatchOrderingState, BatchSeqCounter, Batches, CertificateDigestByOrigin,
    CertificateDigestByRound, Certificates, ConsensusBlockNumbersByDigest, ConsensusBlocks,
    ConsensusBlocksCache, EpochCerts, EpochRecords, EpochRecordsIndex, EpochTransitionCheckpoints,
    KadProviderRecords, KadRecords, KadWorkerProviderRecords, KadWorkerRecords, LastProposed,
    LastProposedByAuthority, NodeBatchesCache, NodeIdentity, Payload, Votes,
};

// Always build redb, we use it as the default for persistant consensus data.
pub mod layered_db;
#[cfg(feature = "reth-libmdbx")]
pub mod mdbx;
pub mod mem_db;
pub mod redb;

pub use rayls_infrastructure_types::{error::StoreError, ReadTimeout};

use crate::mdbx::MdbxConfig;

pub type ProposerKey = u32;
// A type alias marking the "payload" tokens sent by workers to their primary as batch
// acknowledgements
pub type PayloadToken = u8;

/// Convenience type to propagate store errors.
/// Use eyre- just YOLO these errors for now...
pub type StoreResult<T> = eyre::Result<T>;

/// The datastore column family names.
const LAST_PROPOSED_CF: &str = "last_proposed";
const LAST_PROPOSED_BY_AUTHORITY_CF: &str = "last_proposed_by_authority";
const VOTES_CF: &str = "votes";
const CERTIFICATES_CF: &str = "certificates";
const CERTIFICATE_DIGEST_BY_ROUND_CF: &str = "certificate_digest_by_round";
const CERTIFICATE_DIGEST_BY_ORIGIN_CF: &str = "certificate_digest_by_origin";
const PAYLOAD_CF: &str = "payload";
const BATCHES_CF: &str = "batches";
const CONSENSUS_BLOCK_CF: &str = "consensus_block";
const CONSENSUS_BLOCK_NUMBER_BY_DIGEST_CF: &str = "consensus_block_number_by_digest";
const CONSENSUS_BLOCK_CACHE_CF: &str = "consensus_block_cache";
const NODE_BATCHES_CACHE_CF: &str = "node_batches_cache";
const EPOCH_RECORDS_CF: &str = "epoch_record_by_number";
const EPOCH_CERTS_CF: &str = "epoch_cert_by_number";
const EPOCH_RECORDS_INDEX_CF: &str = "epoch_records_index";
const KAD_RECORD_CF: &str = "kad_record";
const KAD_PROVIDER_RECORD_CF: &str = "kad_provider_record";
const KAD_WORKER_RECORD_CF: &str = "kad_worker_record";
const KAD_WORKER_PROVIDER_RECORD_CF: &str = "kad_worker_provider_record";
const EPOCH_TRANSITION_CHECKPOINTS_CF: &str = "epoch_transition_checkpoints";
const BATCH_SEQ_COUNTER_CF: &str = "batch_seq_counter";
const NODE_IDENTITY_CF: &str = "node_identity";
const BATCH_ORDERING_STATE_CF: &str = "batch_ordering_state";

macro_rules! tables {
    ( $($table:ident;$name:expr;<$K:ty, $V:ty>),*) => {
            $(
                #[derive(Debug)]
                pub struct $table {}
                impl rayls_infrastructure_types::Table for $table {
                    type Key = $K;
                    type Value = $V;

                    const NAME: &'static str = $name;
                }
            )*
    };
}

pub mod tables {
    use super::{PayloadToken, ProposerKey};
    use rayls_infrastructure_types::{
        batch_ordering::BatchOrderingState as TypeBatchOrderingState, AuthorityIdentifier, Batch,
        BlockHash, Certificate, CertificateDigest, ConsensusHeader, Epoch, EpochCertificate,
        EpochRecord, EpochTransitionCheckpoint, Header, Round, VoteInfo, WorkerId, B256,
    };

    tables!(
        LastProposed;crate::LAST_PROPOSED_CF;<ProposerKey, Header>, // Cleared every epoch
        LastProposedByAuthority;crate::LAST_PROPOSED_BY_AUTHORITY_CF;<AuthorityIdentifier, Header>,
        Votes;crate::VOTES_CF;<AuthorityIdentifier, VoteInfo>, // Cleared every epoch
        Certificates;crate::CERTIFICATES_CF;<CertificateDigest, Certificate>, // Cleared every epoch
        CertificateDigestByRound;crate::CERTIFICATE_DIGEST_BY_ROUND_CF;<(Round, AuthorityIdentifier), CertificateDigest>, // Cleared every epoch
        CertificateDigestByOrigin;crate::CERTIFICATE_DIGEST_BY_ORIGIN_CF;<(AuthorityIdentifier, Round), CertificateDigest>, // Cleared every epoch
        Payload;crate::PAYLOAD_CF;<(BlockHash, WorkerId), PayloadToken>, // Cleared every epoch
        // Table is used for "normal" consensus as well as for the consensus chain.
        Batches;crate::BATCHES_CF;<BlockHash, Batch>, // Long lived
        // These tables are for the consensus chain not the normal consensus.
        ConsensusBlocks;crate::CONSENSUS_BLOCK_CF;<u64, ConsensusHeader>,
        // This can contain mappings for confirmed but not executed blocks (block might be ConsensusBlocks OR ConsensusBlocksCache).
        ConsensusBlockNumbersByDigest;crate::CONSENSUS_BLOCK_NUMBER_BY_DIGEST_CF;<BlockHash, u64>,
        // This is a cache to store verified but unprocessed consensus headers, remove once processed.
        ConsensusBlocksCache;crate::CONSENSUS_BLOCK_CACHE_CF;<u64, ConsensusHeader>,
        // This is a cache to store this nodes batches before consensus, remove once in a ConsensusHeader.
        NodeBatchesCache;crate::NODE_BATCHES_CACHE_CF;<BlockHash, Batch>,
        // These tables are for the epoch chain not the normal consensus.
        EpochRecords;crate::EPOCH_RECORDS_CF;<Epoch, EpochRecord>,
        EpochCerts;crate::EPOCH_CERTS_CF;<B256, EpochCertificate>,
        EpochRecordsIndex;crate::EPOCH_RECORDS_INDEX_CF;<B256, Epoch>,
        // Epoch transition checkpoint for crash recovery. Keyed by epoch, stores at most one entry.
        EpochTransitionCheckpoints;crate::EPOCH_TRANSITION_CHECKPOINTS_CF;<Epoch, EpochTransitionCheckpoint>,
        // These are used for network storage and separate from consensus
        KadRecords;crate::KAD_RECORD_CF;<BlockHash, Vec<u8>>,
        KadProviderRecords;crate::KAD_PROVIDER_RECORD_CF;<BlockHash, Vec<u8>>,
        KadWorkerRecords;crate::KAD_WORKER_RECORD_CF;<BlockHash, Vec<u8>>,
        KadWorkerProviderRecords;crate::KAD_WORKER_PROVIDER_RECORD_CF;<BlockHash, Vec<u8>>,
        // Per-worker batch sequence counter, persists across epochs and restarts.
        BatchSeqCounter;crate::BATCH_SEQ_COUNTER_CF;<WorkerId, u64>,
        // Node identity: stores this validator's AuthorityIdentifier for foreign DB detection.
        NodeIdentity;crate::NODE_IDENTITY_CF;<u8, AuthorityIdentifier>,
        // Batch ordering state for the current epoch.
        BatchOrderingState;crate::BATCH_ORDERING_STATE_CF;<u8, TypeBatchOrderingState>
    );
}

// mdbx is  the default, if redb is set then is used (so priority is mdbx -> redb)
#[cfg(all(feature = "reth-libmdbx", not(feature = "redb")))]
pub type DatabaseType = LayeredDatabase<MdbxDatabase>;
#[cfg(feature = "redb")]
pub type DatabaseType = LayeredDatabase<ReDB>;

/// Open the configured DB with the required tables.
/// This will return a concrete type for the currently configured Database.
#[allow(unreachable_code)] // Need this so it compiles cleanly with redb.
pub fn open_db<Path: AsRef<std::path::Path> + Send>(store_path: Path) -> DatabaseType {
    open_db_with_consensus_config(store_path, &MdbxConfig::default())
}

/// Open the configured DB with the required tables.
/// This will return a concrete type for the currently configured Database.
#[allow(unreachable_code)] // Need this so it compiles cleanly with redb.
pub fn open_db_with_consensus_config<Path: AsRef<std::path::Path> + Send>(
    store_path: Path,
    consensus_db_config: &MdbxConfig,
) -> DatabaseType {
    // Open the right DB based on feature flags.  The default is MDBX unless the redb flag is
    // set.
    #[cfg(all(feature = "reth-libmdbx", not(feature = "redb")))]
    return _open_mdbx(store_path, consensus_db_config);
    #[cfg(feature = "redb")]
    return _open_redb(store_path);
    panic!("No DB configured!")
}

// The open functions below are the way they are so we can use if cfg!... on open_db.

/// Open or reopen all the storage of the node backed by MDBX.
#[cfg(feature = "reth-libmdbx")]
fn _open_mdbx<P: AsRef<std::path::Path> + Send>(
    store_path: P,
    consensus_db_config: &MdbxConfig,
) -> LayeredDatabase<MdbxDatabase> {
    let persistent_db = MdbxDatabase::open_with_config(store_path, consensus_db_config.clone())
        .expect("Cannot open database");
    // Don't forget to add a new table to MemDatabase...
    let mut db = LayeredDatabase::open(persistent_db);

    open_default_tables(&mut db).expect("failed to open table!");

    db
}

/// Open or reopen all the storage of the node backed by ReDB.
#[cfg(feature = "redb")]
fn _open_redb<P: AsRef<std::path::Path> + Send>(store_path: P) -> LayeredDatabase<ReDB> {
    let re_db = ReDB::open(store_path).expect("Cannot open database");

    let mut db = LayeredDatabase::open(re_db);

    open_default_tables(&mut db).expect("failed to open table!");

    db
}

fn open_default_tables<DB: Database>(db: &mut DB) -> eyre::Result<()> {
    db.open_table::<LastProposed>()
        .map_err(|e| eyre::eyre!("failed to open LastProposed table: {e}"))?;
    db.open_table::<LastProposedByAuthority>()
        .map_err(|e| eyre::eyre!("failed to open LastProposedByAuthority table: {e}"))?;
    db.open_table::<Votes>().map_err(|e| eyre::eyre!("failed to open Votes table: {e}"))?;
    db.open_table::<Certificates>()
        .map_err(|e| eyre::eyre!("failed to open Certificates table: {e}"))?;
    db.open_table::<CertificateDigestByRound>()
        .map_err(|e| eyre::eyre!("failed to open CertificateDigestByRound table: {e}"))?;
    db.open_table::<CertificateDigestByOrigin>()
        .map_err(|e| eyre::eyre!("failed to open CertificateDigestByOrigin table: {e}"))?;
    db.open_table::<Payload>().map_err(|e| eyre::eyre!("failed to open Payload table: {e}"))?;
    db.open_table::<Batches>().map_err(|e| eyre::eyre!("failed to open Batches table: {e}"))?;
    db.open_table::<ConsensusBlocks>()
        .map_err(|e| eyre::eyre!("failed to open ConsensusBlocks table: {e}"))?;
    db.open_table::<ConsensusBlockNumbersByDigest>()
        .map_err(|e| eyre::eyre!("failed to open ConsensusBlockNumbersByDigest table: {e}"))?;
    db.open_table::<ConsensusBlocksCache>()
        .map_err(|e| eyre::eyre!("failed to open ConsensusBlocksCache table: {e}"))?;
    db.open_table::<NodeBatchesCache>()
        .map_err(|e| eyre::eyre!("failed to open NodeBatchesCache table: {e}"))?;
    db.open_table::<EpochRecords>()
        .map_err(|e| eyre::eyre!("failed to open EpochRecords table: {e}"))?;
    db.open_table::<EpochCerts>()
        .map_err(|e| eyre::eyre!("failed to open EpochCerts table: {e}"))?;
    db.open_table::<EpochRecordsIndex>()
        .map_err(|e| eyre::eyre!("failed to open EpochRecordsIndex table: {e}"))?;
    db.open_table::<EpochTransitionCheckpoints>()
        .map_err(|e| eyre::eyre!("failed to open EpochTransitionCheckpoints table: {e}"))?;
    db.open_table::<KadRecords>()
        .map_err(|e| eyre::eyre!("failed to open KadRecords table: {e}"))?;
    db.open_table::<KadProviderRecords>()
        .map_err(|e| eyre::eyre!("failed to open KadProviderRecords table: {e}"))?;
    db.open_table::<KadWorkerRecords>()
        .map_err(|e| eyre::eyre!("failed to open KadWorkerRecords table: {e}"))?;
    db.open_table::<KadWorkerProviderRecords>()
        .map_err(|e| eyre::eyre!("failed to open KadWorkerProviderRecords table: {e}"))?;
    db.open_table::<BatchSeqCounter>()
        .map_err(|e| eyre::eyre!("failed to open BatchSeqCounter table: {e}"))?;
    db.open_table::<NodeIdentity>()
        .map_err(|e| eyre::eyre!("failed to open NodeIdentity table: {e}"))?;
    db.open_table::<BatchOrderingState>()
        .map_err(|e| eyre::eyre!("failed to open BatchOrdering table: {e}"))?;

    Ok(())
}

#[cfg(test)]
mod test {
    use rayls_infrastructure_types::{Database, DbTxMut};

    #[derive(Debug)]
    pub(crate) struct TestTable {}
    impl rayls_infrastructure_types::Table for TestTable {
        type Key = u64;
        type Value = String;

        const NAME: &'static str = "TestTable";
    }

    /// Runs a simple bench/test for the provided DB.  Can use it for larger dataset tests as well
    /// as comparing backends. For example run ```cargo test dbsimpbench --features redb --
    /// --nocapture --test-threads 1``` to run each backend through the bench one at a time.
    pub(crate) fn db_simp_bench<DB: Database>(db: DB, name: &str) {
        use rayls_infrastructure_types::{DbTx, DbTxMut};

        println!("\nDBBENCH [{name}] starting simpdbbench");
        // the layered database cache is currently 20k, so limit to this.
        // if bigger, the cache starts to be emptied before the actual commit
        // and there is a small time window where some values are not found anywhere
        let max = 20_000;

        let total = std::time::Instant::now();
        let start = std::time::Instant::now();
        db.with_write_txn(|txn| {
            for (key, value) in (0..max).map(|i| (i, i.to_string())) {
                txn.insert::<TestTable>(&key, &value).unwrap();
            }

            Ok(())
        })
        .unwrap();
        db.sync_persist();

        println!("DBBENCH [{name}] insert {max}: {}", start.elapsed().as_secs_f64());
        let startc = std::time::Instant::now();

        println!(
            "DBBENCH [{name}] commit {max}: {}, total insert/commit: {}",
            startc.elapsed().as_secs_f64(),
            start.elapsed().as_secs_f64()
        );

        //check if all values are present
        for (key, value) in (0..max).map(|i| (i, i.to_string())) {
            let val = db.get::<TestTable>(&key).unwrap().unwrap();
            assert_eq!(value, val);
        }

        let start = std::time::Instant::now();
        let mut _i = 0;
        #[allow(clippy::explicit_counter_loop)]
        for (_k, _v) in db.iter::<TestTable>() {
            // assert_eq!(k, i); //no need to check keys here, we check them above, because keys are
            // not necessarily in order of insertion assert_eq!(v, i.to_string());
            _i += 1;
        }
        println!("DBBENCH [{name}] iterate {max}: {}", start.elapsed().as_secs_f64());

        let start = std::time::Instant::now();
        let mut _i = max;
        for (_k, _v) in db.reverse_iter::<TestTable>() {
            _i -= 1;
            // assert_eq!(k, i);
            // assert_eq!(v, i.to_string());
        }
        println!("DBBENCH [{name}] iterate reverse {max}: {}", start.elapsed().as_secs_f64());

        let start = std::time::Instant::now();
        for (key, value) in (0..max).rev().map(|i| (i, i.to_string())) {
            let val = db.get::<TestTable>(&key).unwrap().unwrap();
            assert_eq!(value, val);
        }
        println!("DBBENCH [{name}] loop reverse, no txn {max}: {}", start.elapsed().as_secs_f64());

        let start = std::time::Instant::now();
        db.with_read_txn(|txn| {
            for (key, value) in (0..max).rev().map(|i| (i, i.to_string())) {
                let val = txn.get::<TestTable>(&key).unwrap().unwrap();
                assert_eq!(value, val);
            }

            Ok(())
        })
        .unwrap();

        println!("DBBENCH [{name}] loop reverse, {max}: {}", start.elapsed().as_secs_f64());

        let start = std::time::Instant::now();
        db.with_read_txn(|txn| {
            for (key, value) in (0..(max / 2)).map(|i| (i, i.to_string())) {
                let key2 = max - key - 1;
                let value2 = key2.to_string();
                let val = txn.get::<TestTable>(&key).unwrap().unwrap();
                assert_eq!(value, val);
                let val = txn.get::<TestTable>(&key2).unwrap().unwrap();
                assert_eq!(value2, val);
            }
            Ok(())
        })
        .unwrap();

        println!("DBBENCH [{name}] loop two way, {max}: {}", start.elapsed().as_secs_f64());

        let start = std::time::Instant::now();
        db.with_write_txn(|txn| {
            txn.clear_table::<TestTable>().unwrap();
            Ok(())
        })
        .unwrap();
        println!("DBBENCH [{name}] clear_table {max}: {}", start.elapsed().as_secs_f64());

        let start = std::time::Instant::now();
        db.with_write_txn(|txn| {
            for (key, value) in (0..max).map(|i| (i, i.to_string())) {
                txn.insert::<TestTable>(&key, &value).unwrap();
            }

            Ok(())
        })
        .unwrap();
        println!("DBBENCH [{name}] insert post clear {max}: {}", start.elapsed().as_secs_f64());

        println!("DBBENCH [{name}] Total pre drop: {}", total.elapsed().as_secs_f64());
        let start = std::time::Instant::now();

        println!("DBBENCH [{name}] drop DB: {}", start.elapsed().as_secs_f64());
        println!("DBBENCH [{name}] Total Runtime: {}", total.elapsed().as_secs_f64());
    }

    pub(crate) fn test_contains_key<DB: Database>(db: DB) {
        db.insert::<TestTable>(&123456789, &"123456789".to_string()).expect("Failed to insert");
        assert!(db.contains_key::<TestTable>(&123456789).expect("Failed to call contains key"));
        assert!(!db.contains_key::<TestTable>(&000000000).expect("Failed to call contains key"));
    }

    pub(crate) fn test_get<DB: Database>(db: DB) {
        db.insert::<TestTable>(&123456789, &"123456789".to_string()).expect("Failed to insert");
        assert_eq!(
            Some("123456789".to_string()),
            db.get::<TestTable>(&123456789).expect("Failed to get")
        );
        assert_eq!(None, db.get::<TestTable>(&000000000).expect("Failed to get"));
    }

    pub(crate) fn test_multi_get<DB: Database>(db: DB) {
        db.insert::<TestTable>(&123, &"123".to_string()).expect("Failed to insert");
        db.insert::<TestTable>(&456, &"456".to_string()).expect("Failed to insert");

        let result = db.multi_get::<TestTable>([&123, &456, &789]).expect("Failed to multi get");

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], Some("123".to_string()));
        assert_eq!(result[1], Some("456".to_string()));
        assert_eq!(result[2], None);
    }

    pub(crate) fn test_skip<DB: Database>(db: DB) {
        db.insert::<TestTable>(&123, &"123".to_string()).expect("Failed to insert");
        db.insert::<TestTable>(&456, &"456".to_string()).expect("Failed to insert");
        db.insert::<TestTable>(&789, &"789".to_string()).expect("Failed to insert");
        db.sync_persist(); // Either a no-op or a chance for write ops to catch up.

        // Skip all smaller
        let key_vals: Vec<_> = db.skip_to::<TestTable>(&456).expect("Seek failed").collect();
        assert_eq!(key_vals.len(), 2);
        assert_eq!(key_vals[0], (456, "456".to_string()));
        assert_eq!(key_vals[1], (789, "789".to_string()));

        // Skip to the end
        assert_eq!(db.skip_to::<TestTable>(&999).expect("Seek failed").count(), 0);

        // Skip to last
        assert_eq!(db.last_record::<TestTable>(), Some((789, "789".to_string())));

        // Skip to successor of first value
        assert_eq!(db.skip_to::<TestTable>(&000).expect("Skip failed").count(), 3);
    }

    pub(crate) fn test_skip_to_previous_simple<DB: Database>(db: DB) {
        let mut txn = db.write_txn().unwrap();
        txn.insert::<TestTable>(&123, &"123".to_string()).expect("Failed to insert");
        txn.insert::<TestTable>(&456, &"456".to_string()).expect("Failed to insert");
        txn.insert::<TestTable>(&789, &"789".to_string()).expect("Failed to insert");
        txn.commit().unwrap();
        db.sync_persist(); // Either a no-op or a chance for write ops to catch up.

        // Skip to the one before the end
        let key_val = db.record_prior_to::<TestTable>(&999).expect("Seek failed");
        assert_eq!(key_val, (789, "789".to_string()));

        // Skip to prior of first value
        // Note: returns an empty iterator!
        assert!(db.record_prior_to::<TestTable>(&000).is_none());
    }

    pub(crate) fn test_iter_skip_to_previous_gap<DB: Database>(db: DB) {
        let mut txn = db.write_txn().unwrap();
        for i in 1..100 {
            if i != 50 {
                txn.insert::<TestTable>(&i, &i.to_string()).unwrap();
            }
        }
        txn.commit().unwrap();
        db.sync_persist(); // Either a no-op or a chance for write ops to catch up.
                           // Skip prior to will return an iterator starting with an "unexpected" key if the sought one
                           // is not in the table
        let val = db.record_prior_to::<TestTable>(&50).map(|(k, _)| k).unwrap();
        assert_eq!(49, val);
    }

    pub(crate) fn test_remove<DB: Database>(db: DB) {
        db.insert::<TestTable>(&123456789, &"123456789".to_string()).expect("Failed to insert");
        // we to await for the value to be inserted in the inner DVB as well
        // without it we ensure it is only present in the mem db
        // so in the remove check we may get a false positive that the value is missing, when it is
        // still in the db
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert!(db.get::<TestTable>(&123456789).expect("Failed to get").is_some());

        db.remove::<TestTable>(&123456789).expect("Failed to remove");
        assert!(db.get::<TestTable>(&123456789).expect("Failed to get").is_none());
    }

    pub(crate) fn test_remove_then_insert_new<DB: Database>(db: DB) {
        db.insert::<TestTable>(&123456789, &"123456789".to_string()).expect("Failed to insert");
        // we to await for the value to be inserted in the inner DVB as well
        // without it we ensure it is only present in the mem db
        // so in the remove check we may get a false positive that the value is missing, when it is
        // still in the db
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert!(db.get::<TestTable>(&123456789).expect("Failed to get").is_some());

        db.remove::<TestTable>(&123456789).expect("Failed to remove");
        assert!(db.get::<TestTable>(&123456789).expect("Failed to get").is_none());

        db.insert::<TestTable>(&123456789, &"NEW_VALUE".to_string())
            .expect("Failed to insert new value");
        assert_eq!(
            Some("NEW_VALUE".to_string()),
            db.get::<TestTable>(&123456789).expect("Failed to get")
        );
    }

    pub(crate) fn test_iter<DB: Database>(db: DB) {
        db.insert::<TestTable>(&123456789, &"123456789".to_string()).expect("Failed to insert");

        // Note that inserts "show up" immediadly but there could be a race where they are
        // iterated over twice while being persisted.
        let mut iter = db.iter::<TestTable>();
        assert_eq!(Some((123456789, "123456789".to_string())), iter.next());
        assert_eq!(None, iter.next());
    }

    pub(crate) fn test_iter_reverse<DB: Database>(db: DB) {
        db.insert::<TestTable>(&1, &"1".to_string()).expect("Failed to insert");
        db.insert::<TestTable>(&2, &"2".to_string()).expect("Failed to insert");
        db.insert::<TestTable>(&3, &"3".to_string()).expect("Failed to insert");
        // Note that inserts "show up" immediadly but there could be a race where they are
        // iterated over twice while being persisted.
        let mut iter = db.iter::<TestTable>();

        assert_eq!(Some((1, "1".to_string())), iter.next());
        assert_eq!(Some((2, "2".to_string())), iter.next());
        assert_eq!(Some((3, "3".to_string())), iter.next());
        assert_eq!(None, iter.next());
    }

    pub(crate) fn test_clear<DB: Database>(db: DB) {
        // Test clear of empty map
        let _ = db.clear_table::<TestTable>();

        let mut txn = db.write_txn().unwrap();
        for (key, val) in (0..101).map(|i| (i, i.to_string())) {
            txn.insert::<TestTable>(&key, &val).expect("Failed to batch insert");
        }
        txn.commit().unwrap();
        db.sync_persist(); // Either a no-op or a chance for write ops to catch up.

        // Check we have multiple entries
        assert!(db.iter::<TestTable>().count() > 1);
        let _ = db.clear_table::<TestTable>();
        db.sync_persist(); // Either a no-op or a chance for write ops to catch up.
        assert_eq!(db.iter::<TestTable>().count(), 0);
        // Clear again to ensure safety when clearing empty map
        let _ = db.clear_table::<TestTable>();
        assert_eq!(db.iter::<TestTable>().count(), 0);
        // Clear with one item
        let _ = db.insert::<TestTable>(&1, &"e".to_string());
        assert_eq!(db.iter::<TestTable>().count(), 1);
        let _ = db.clear_table::<TestTable>();
        db.sync_persist(); // Either a no-op or a chance for write ops to catch up.
        assert_eq!(db.iter::<TestTable>().count(), 0);
    }

    pub(crate) fn test_is_empty<DB: Database>(db: DB) {
        // Test empty map is truly empty
        assert!(db.is_empty::<TestTable>());
        let _ = db.clear_table::<TestTable>();
        assert!(db.is_empty::<TestTable>());

        let mut txn = db.write_txn().unwrap();
        for (key, val) in (0..101).map(|i| (i, i.to_string())) {
            txn.insert::<TestTable>(&key, &val).expect("Failed to batch insert");
        }
        txn.commit().unwrap();

        // Check we have multiple entries and not empty
        assert!(db.iter::<TestTable>().count() > 1);
        assert!(!db.is_empty::<TestTable>());

        // Clear again to ensure empty works after clearing
        let _ = db.clear_table::<TestTable>();
        db.sync_persist(); // Either a no-op or a chance for write ops to catch up.
        assert_eq!(db.iter::<TestTable>().count(), 0);
        assert!(db.is_empty::<TestTable>());
    }

    pub(crate) fn test_multi_insert<DB: Database>(db: DB) {
        let mut txn = db.write_txn().unwrap();
        for (key, val) in (0..101).map(|i| (i, i.to_string())) {
            txn.insert::<TestTable>(&key, &val).expect("Failed to batch insert");
        }
        txn.commit().unwrap();

        for (k, v) in (0..101).map(|i| (i, i.to_string())) {
            let val = db.get::<TestTable>(&k).expect("Failed to get inserted key");
            assert_eq!(Some(v), val);
        }
    }

    pub(crate) fn test_multi_remove<DB: Database>(db: DB) {
        // Create kv pairs
        let mut txn = db.write_txn().unwrap();
        for (key, val) in (0..101).map(|i| (i, i.to_string())) {
            txn.insert::<TestTable>(&key, &val).expect("Failed to batch insert");
        }
        txn.commit().unwrap();

        // Check insertion
        for (k, v) in (0..101).map(|i| (i, i.to_string())) {
            let val = db.get::<TestTable>(&k).expect("Failed to get inserted key");
            assert_eq!(Some(v), val);
        }

        // Remove 50 items
        let mut txn = db.write_txn().unwrap();
        for (key, _val) in (0..101).map(|i| (i, i.to_string())).take(50) {
            txn.remove::<TestTable>(&key).expect("Failed to batch remove");
        }
        txn.commit().unwrap();
        db.sync_persist(); // Either a no-op or a chance for write ops to catch up.
                           // Rayls: Asserting against fetched items, due to chaining of in memory and persistent db,
                           // resulting in double iter size.
        for (k, _) in (0..101).map(|i| (i, i.to_string())).take(50) {
            let val = db.get::<TestTable>(&k).expect("Failed to get removed key");
            assert_eq!(None, val);
        }

        // Check that the remaining are present
        for (k, v) in (0..101).map(|i| (i, i.to_string())).skip(50) {
            let val = db.get::<TestTable>(&k).expect("Failed to get inserted key");
            assert_eq!(Some(v), val);
        }
    }
}
