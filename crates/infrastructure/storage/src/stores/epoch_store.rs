//! Trait and helpers for accessing Epoch data in the consensus DB.
use rayls_infrastructure_types::{
    BlockHash, BlsPublicKey, Database, DbTx, DbTxMut, Epoch, EpochCertificate, EpochRecord,
};

use crate::{
    tables::{EpochCerts, EpochRecords, EpochRecordsIndex, PendingEpochRecord},
    StoreResult,
};

/// Helpers for Epoch DB access.
pub trait EpochStore {
    /// Retrieve the committee keys for epoch if available in the DB.
    fn get_committee_keys(&self, epoch: Epoch) -> Option<Vec<BlsPublicKey>>;

    /// Save an epoch record *without* a certificate.
    ///
    /// Only for the epoch-0 unsigned dummy record written at startup. Every real
    /// record must be written together with its cert via
    /// [`EpochStore::save_epoch_record_with_cert`] to avoid an unrecoverable
    /// record-without-cert half-state on disk.
    fn save_epoch_record(&self, epoch_rec: &EpochRecord) -> StoreResult<()>;

    /// Save an epoch record with its certificate.
    fn save_epoch_record_with_cert(
        &self,
        epoch_rec: &EpochRecord,
        cert: &EpochCertificate,
    ) -> StoreResult<()>;

    /// Retrieve the epoch record and certificate (if available) by number.
    fn get_epoch_by_number(&self, epoch: Epoch) -> Option<(EpochRecord, Option<EpochCertificate>)>;

    /// Retrieve the epoch record and certificate (if available) by hash.
    fn get_epoch_by_hash(&self, hash: BlockHash)
        -> Option<(EpochRecord, Option<EpochCertificate>)>;

    /// Persist the record for a just-closed epoch before it is certified, as a resume hint for
    /// bootstrap/retry. Overwrites any previously pending record - only the newest closed epoch
    /// still awaiting a cert is ever meaningful. Never read as a substitute for a certified
    /// record: callers still only trust [`EpochStore::get_epoch_by_number`]'s cert-bearing result
    /// for `parent_hash`/committee derivation.
    fn save_pending_epoch_record(&self, epoch_rec: &EpochRecord) -> StoreResult<()>;

    /// Retrieve the pending (not yet certified) epoch record, if one is stored.
    fn get_pending_epoch_record(&self) -> Option<EpochRecord>;

    /// Remove the pending epoch record, but only if it is still the one for `epoch`.
    ///
    /// A no-op if the stored pending entry belongs to a different (newer) epoch, so a stale
    /// caller can never clobber a more recent pending write.
    fn clear_pending_epoch_record(&self, epoch: Epoch) -> StoreResult<()>;
}

impl<DB: Database> EpochStore for DB {
    fn get_committee_keys(&self, epoch: Epoch) -> Option<Vec<BlsPublicKey>> {
        if let Ok(Some(rec)) = self.get::<EpochRecords>(&epoch) {
            Some(rec.committee)
        } else if let Ok(Some(rec)) = self.get::<EpochRecords>(&(epoch.saturating_sub(1))) {
            Some(rec.next_committee)
        } else {
            None
        }
    }

    fn save_epoch_record(&self, epoch_rec: &EpochRecord) -> StoreResult<()> {
        let epoch_hash = epoch_rec.digest();
        let epoch = epoch_rec.epoch;

        self.with_write_txn(|tx| {
            if epoch_rec.epoch == 0 {
                // Should have a "dummy" epoch 0 record, remove just in case the backend has a
                // dumb insert or something.
                tx.remove::<EpochRecords>(&epoch)?;
            }
            tx.insert::<EpochRecordsIndex>(&epoch_hash, &epoch)?;
            tx.insert::<EpochRecords>(&epoch, epoch_rec)?;
            Ok(())
        })
    }

    fn save_epoch_record_with_cert(
        &self,
        epoch_rec: &EpochRecord,
        cert: &EpochCertificate,
    ) -> StoreResult<()> {
        let epoch_hash = epoch_rec.digest();
        let epoch = epoch_rec.epoch;

        self.with_write_txn(|tx| {
            tx.insert::<EpochRecordsIndex>(&epoch_hash, &epoch)?;
            tx.insert::<EpochRecords>(&epoch, epoch_rec)?;
            tx.insert::<EpochCerts>(&epoch_hash, cert)?;
            Ok(())
        })
    }

    fn get_epoch_by_number(&self, epoch: Epoch) -> Option<(EpochRecord, Option<EpochCertificate>)> {
        self.with_read_txn(|txn| {
            let record = txn.get::<EpochRecords>(&epoch)?;
            if let Some(record) = record {
                let digest = record.digest();
                let epoch_cert = txn.get::<EpochCerts>(&digest)?;
                return Ok((record, epoch_cert));
            }

            Err(eyre::eyre!("No epoch record found"))
        })
        .ok()
    }

    fn get_epoch_by_hash(
        &self,
        hash: BlockHash,
    ) -> Option<(EpochRecord, Option<EpochCertificate>)> {
        self.with_read_txn(|txn| {
            let epoch = txn.get::<EpochRecordsIndex>(&hash)?;
            if let Some(epoch) = epoch {
                if let Some(record) = txn.get::<EpochRecords>(&epoch)? {
                    let digest = record.digest();
                    let epoch_cert = txn.get::<EpochCerts>(&digest)?;

                    return Ok((record, epoch_cert));
                }
            }

            Err(eyre::eyre!("No epoch record found"))
        })
        .ok()
    }

    fn save_pending_epoch_record(&self, epoch_rec: &EpochRecord) -> StoreResult<()> {
        let epoch = epoch_rec.epoch;
        self.with_write_txn(|tx| {
            // At most one entry ever: clear before inserting so a re-run (recovery replaying
            // write_epoch_record for the same epoch, or a later epoch superseding an older
            // pending one) can never leave more than the newest behind.
            tx.clear_table::<PendingEpochRecord>()?;
            tx.insert::<PendingEpochRecord>(&epoch, epoch_rec)?;
            Ok(())
        })
    }

    fn get_pending_epoch_record(&self) -> Option<EpochRecord> {
        self.iter::<PendingEpochRecord>().next().map(|(_, rec)| rec)
    }

    fn clear_pending_epoch_record(&self, epoch: Epoch) -> StoreResult<()> {
        self.with_write_txn(|tx| {
            // Only remove if the stored entry is still the one for `epoch` - a stale caller
            // (e.g. a slow backfill for an epoch superseded by a newer close) must never clobber
            // a more recent pending write.
            if tx.get::<PendingEpochRecord>(&epoch)?.is_some() {
                tx.remove::<PendingEpochRecord>(&epoch)?;
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod pending_epoch_record_tests {
    use super::*;
    use crate::mem_db::MemDatabase;

    fn record(epoch: Epoch) -> EpochRecord {
        EpochRecord { epoch, ..Default::default() }
    }

    #[test]
    fn round_trips_a_pending_record() {
        let db = MemDatabase::default();
        assert!(db.get_pending_epoch_record().is_none());

        db.save_pending_epoch_record(&record(5)).unwrap();

        assert_eq!(db.get_pending_epoch_record().unwrap().epoch, 5);
    }

    #[test]
    fn a_newer_pending_record_replaces_the_older_one() {
        let db = MemDatabase::default();
        db.save_pending_epoch_record(&record(5)).unwrap();
        db.save_pending_epoch_record(&record(6)).unwrap();

        // At most one entry ever - the newer closed epoch is the only one that can still matter.
        assert_eq!(db.get_pending_epoch_record().unwrap().epoch, 6);
    }

    #[test]
    fn clear_removes_a_matching_pending_record() {
        let db = MemDatabase::default();
        db.save_pending_epoch_record(&record(5)).unwrap();

        db.clear_pending_epoch_record(5).unwrap();

        assert!(db.get_pending_epoch_record().is_none());
    }

    #[test]
    fn clear_is_a_no_op_against_a_different_epochs_pending_record() {
        let db = MemDatabase::default();
        db.save_pending_epoch_record(&record(6)).unwrap();

        // A stale caller for an epoch a newer close already superseded must not clobber it.
        db.clear_pending_epoch_record(5).unwrap();

        assert_eq!(db.get_pending_epoch_record().unwrap().epoch, 6);
    }

    #[test]
    fn clear_against_an_empty_table_is_a_no_op() {
        let db = MemDatabase::default();

        db.clear_pending_epoch_record(5).unwrap();

        assert!(db.get_pending_epoch_record().is_none());
    }

    #[test]
    fn pending_and_certified_records_are_independent_tables() {
        let db = MemDatabase::default();
        let rec = record(5);
        db.save_pending_epoch_record(&rec).unwrap();

        // Certifying elsewhere does not implicitly clear the pending hint - callers are
        // responsible for calling clear_pending_epoch_record alongside every
        // save_epoch_record_with_cert call site (see core.rs / epoch.rs).
        assert!(db.get_pending_epoch_record().is_some());
        assert!(db.get_epoch_by_number(5).is_none());
    }
}
