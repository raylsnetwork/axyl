//! Trait and helpers for accessing Epoch data in the consensus DB.
use rayls_infrastructure_types::{
    BlockHash, BlsPublicKey, Database, DbTx, DbTxMut, Epoch, EpochCertificate, EpochRecord,
};

use crate::{
    tables::{EpochCerts, EpochRecords, EpochRecordsIndex, PendingEpochRecord},
    StoreResult,
};
use tracing::{debug, info};

/// Log target for the lifecycle of `PendingEpochRecord` rows: saved at an epoch close, removed
/// with the certificate or by a sweep, resumed or adopted. Transitions log at `info` (a handful
/// per epoch); the raw table listing logs at `debug`. `RUST_LOG=epoch-manager::pending-record=info`
/// shows only these.
pub const PENDING_RECORD_LOG_TARGET: &str = "epoch-manager::pending-record";

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

    /// Save an epoch record with its certificate. Also removes the epoch's
    /// [`PendingEpochRecord`] row, if any, in the same transaction.
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
    /// bootstrap/retry. Keyed by epoch: a later close never displaces an earlier epoch that is
    /// still awaiting its cert, since the chain can advance past an uncertified epoch (#142) and
    /// each one must be resumable independently. Re-saving the same epoch overwrites in place.
    /// Never read as a substitute for a certified record: callers still only trust
    /// [`EpochStore::get_epoch_by_number`]'s cert-bearing result for `parent_hash`/committee
    /// derivation.
    fn save_pending_epoch_record(&self, epoch_rec: &EpochRecord) -> StoreResult<()>;

    /// Retrieve every pending (not yet certified) epoch record, oldest epoch first.
    fn pending_epoch_records(&self) -> Vec<EpochRecord>;

    /// Remove the pending epoch record for `epoch`. A no-op if none is stored; other epochs'
    /// pending entries are never touched.
    ///
    /// Normally unnecessary: [`EpochStore::save_epoch_record_with_cert`] removes the row itself.
    /// Kept for the defensive sweep of rows whose epoch turns out to be certified already.
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
            // A certified epoch is by definition no longer pending. Removing the resume hint in
            // the same txn as the cert means a pending row can never outlive its cert, whichever
            // path produced the cert (own quorum, peer fetch, state-sync backfill). Removing a
            // missing key is a no-op.
            tx.remove::<PendingEpochRecord>(&epoch)?;
            Ok(())
        })?;
        info!(
            target: PENDING_RECORD_LOG_TARGET,
            epoch,
            digest = %epoch_hash,
            "epoch certified: its pending record, if any, was removed with the certificate"
        );
        Ok(())
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
            // Keyed insert: a re-run for the same epoch (recovery replaying write_epoch_record)
            // overwrites in place; a later epoch's close adds a row rather than evicting an
            // older epoch that is still waiting on its cert.
            tx.insert::<PendingEpochRecord>(&epoch, epoch_rec)?;
            Ok(())
        })?;
        info!(
            target: PENDING_RECORD_LOG_TARGET,
            epoch,
            digest = %epoch_rec.digest(),
            parent_hash = %epoch_rec.parent_hash,
            committee = epoch_rec.committee.len(),
            "pending epoch record saved: epoch closed here, certificate not on disk yet"
        );
        Ok(())
    }

    fn pending_epoch_records(&self) -> Vec<EpochRecord> {
        // Sort explicitly rather than relying on the backend's key order, which depends on the
        // key encoding.
        let mut recs: Vec<EpochRecord> =
            self.iter::<PendingEpochRecord>().map(|(_, rec)| rec).collect();
        recs.sort_by_key(|rec| rec.epoch);
        debug!(
            target: PENDING_RECORD_LOG_TARGET,
            epochs = ?recs.iter().map(|rec| rec.epoch).collect::<Vec<_>>(),
            "pending epoch records listed"
        );
        recs
    }

    fn clear_pending_epoch_record(&self, epoch: Epoch) -> StoreResult<()> {
        // Keyed removal, not read-then-remove: DbTx::get() is not callable on a write
        // transaction on the layered backend (panics - "DbTx get() should not be called on a
        // DbTxMut!"). Removing a non-existent key is already a safe no-op (see
        // save_epoch_record's own defensive `tx.remove` above), and since the key IS the epoch
        // number, this only ever touches this epoch's entry.
        self.with_write_txn(|tx| {
            tx.remove::<PendingEpochRecord>(&epoch)?;
            Ok(())
        })?;
        info!(
            target: PENDING_RECORD_LOG_TARGET,
            epoch,
            "pending epoch record cleared without a certificate write (already certified elsewhere)"
        );
        Ok(())
    }
}

#[cfg(test)]
mod pending_epoch_record_tests {
    use super::*;
    use crate::mem_db::MemDatabase;

    fn record(epoch: Epoch) -> EpochRecord {
        EpochRecord { epoch, ..Default::default() }
    }

    fn pending_epochs(db: &MemDatabase) -> Vec<Epoch> {
        db.pending_epoch_records().into_iter().map(|rec| rec.epoch).collect()
    }

    /// An unsigned cert for `rec`: the store does not verify signatures, only the digest link.
    fn cert_for(rec: &EpochRecord) -> EpochCertificate {
        EpochCertificate {
            epoch_hash: rec.digest(),
            signature: Default::default(),
            signed_authorities: Default::default(),
        }
    }

    #[test]
    fn round_trips_a_pending_record() {
        let db = MemDatabase::default();
        assert!(db.pending_epoch_records().is_empty());

        db.save_pending_epoch_record(&record(5)).unwrap();

        assert_eq!(pending_epochs(&db), vec![5]);
    }

    #[test]
    fn a_later_close_keeps_an_earlier_uncertified_epoch_pending() {
        let db = MemDatabase::default();
        db.save_pending_epoch_record(&record(6)).unwrap();
        db.save_pending_epoch_record(&record(5)).unwrap();

        // The chain can advance past an uncertified epoch (#142); both must stay resumable,
        // and the listing is ordered by epoch regardless of insertion order.
        assert_eq!(pending_epochs(&db), vec![5, 6]);
    }

    #[test]
    fn re_saving_the_same_epoch_overwrites_in_place() {
        let db = MemDatabase::default();
        db.save_pending_epoch_record(&record(5)).unwrap();
        db.save_pending_epoch_record(&record(5)).unwrap();

        assert_eq!(pending_epochs(&db), vec![5]);
    }

    #[test]
    fn clear_removes_only_the_matching_pending_record() {
        let db = MemDatabase::default();
        db.save_pending_epoch_record(&record(5)).unwrap();
        db.save_pending_epoch_record(&record(6)).unwrap();

        db.clear_pending_epoch_record(5).unwrap();

        assert_eq!(pending_epochs(&db), vec![6]);
    }

    #[test]
    fn clear_is_a_no_op_against_a_different_epochs_pending_record() {
        let db = MemDatabase::default();
        db.save_pending_epoch_record(&record(6)).unwrap();

        db.clear_pending_epoch_record(5).unwrap();

        assert_eq!(pending_epochs(&db), vec![6]);
    }

    #[test]
    fn clear_against_an_empty_table_is_a_no_op() {
        let db = MemDatabase::default();

        db.clear_pending_epoch_record(5).unwrap();

        assert!(db.pending_epoch_records().is_empty());
    }

    #[test]
    fn a_pending_record_does_not_count_as_a_certified_one() {
        let db = MemDatabase::default();
        db.save_pending_epoch_record(&record(5)).unwrap();

        assert_eq!(pending_epochs(&db), vec![5]);
        assert!(db.get_epoch_by_number(5).is_none());
    }

    #[test]
    fn saving_a_cert_removes_only_that_epochs_pending_record() {
        let db = MemDatabase::default();
        db.save_pending_epoch_record(&record(5)).unwrap();
        db.save_pending_epoch_record(&record(6)).unwrap();

        db.save_epoch_record_with_cert(&record(5), &cert_for(&record(5))).unwrap();

        // The invariant the table relies on: a row exists iff the epoch has no cert on disk.
        assert_eq!(pending_epochs(&db), vec![6]);
        assert!(db.get_epoch_by_number(5).is_some_and(|(_, cert)| cert.is_some()));
    }

    #[test]
    fn saving_a_cert_with_no_pending_record_is_fine() {
        let db = MemDatabase::default();

        db.save_epoch_record_with_cert(&record(5), &cert_for(&record(5))).unwrap();

        assert!(db.pending_epoch_records().is_empty());
    }
}
