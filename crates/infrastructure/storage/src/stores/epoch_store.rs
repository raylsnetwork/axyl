//! Trait and helpers for accessing Epoch data in the consensus DB.
use rayls_infrastructure_types::{
    dkg::DkgOutcome, Address, BlockHash, BlsPublicKey, Database, DbTx, DbTxMut, Epoch,
    EpochCertificate, EpochRecord, RecordBinding, B256,
};

use crate::{
    tables::{
        DkgDealings, DkgOutcomes, EpochCerts, EpochRecordBindings, EpochRecords, EpochRecordsIndex,
        PendingEpochRecord,
    },
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

    /// Save an epoch record with its certificate and what the certificate signs besides the
    /// record. Also removes the epoch's [`PendingEpochRecord`] row, if any, in the same
    /// transaction.
    fn save_epoch_record_with_cert(
        &self,
        epoch_rec: &EpochRecord,
        cert: &EpochCertificate,
        binding: &RecordBinding,
    ) -> StoreResult<()>;

    /// What the certificate of the record with digest `digest` signs besides the record
    /// (`Legacy` when nothing is stored).
    fn get_record_binding(&self, digest: B256) -> RecordBinding;

    /// The group key of `epoch` as this node knows it: `Some(None)` for aggregate certificates,
    /// `None` when unknown. From the DKG outcome when this node computed it, else from the
    /// certified record of `epoch - 1`: the next key it signs, or aggregate when it signs none
    /// (that epoch did not deal); the two must agree.
    fn epoch_group_key(&self, epoch: Epoch) -> Option<Option<BlsPublicKey>>;

    /// Remove an epoch record, its digest index entry, and its certificate atomically. No-op if
    /// absent.
    fn remove_epoch_record(&self, epoch: Epoch) -> StoreResult<()>;

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

    /// The DKG outcome for `epoch` (the epoch the key is for), if this node computed it.
    fn get_dkg_outcome(&self, epoch: Epoch) -> Option<DkgOutcome>;

    /// Persist the DKG outcome for `epoch`.
    fn save_dkg_outcome(&self, epoch: Epoch, outcome: &DkgOutcome) -> StoreResult<()>;

    /// Keep accepted dealings, each as the epoch whose key it deals, its hash, its dealer and
    /// its bytes.
    fn save_dkg_dealings(&self, dealings: &[(Epoch, B256, Address, Vec<u8>)]) -> StoreResult<()>;

    /// The kept dealing of the key of `epoch` whose hash is `hash`, with its dealer.
    fn get_dkg_dealing(&self, epoch: Epoch, hash: B256) -> Option<(Address, Vec<u8>)>;

    /// Drop the kept dealings of the keys of epochs before `epoch`.
    fn prune_dkg_dealings(&self, epoch: Epoch) -> StoreResult<()>;
}

impl<DB: Database> EpochStore for DB {
    fn get_dkg_outcome(&self, epoch: Epoch) -> Option<DkgOutcome> {
        self.get::<DkgOutcomes>(&epoch).ok().flatten()
    }

    fn get_record_binding(&self, digest: B256) -> RecordBinding {
        self.get::<EpochRecordBindings>(&digest).ok().flatten().unwrap_or(RecordBinding::Legacy)
    }

    fn epoch_group_key(&self, epoch: Epoch) -> Option<Option<BlsPublicKey>> {
        let from_outcome = self.get_dkg_outcome(epoch).map(|outcome| match outcome {
            DkgOutcome::Key(key) => Some(key.group_key()),
            DkgOutcome::Fallback => None,
        });
        let from_record = epoch.checked_sub(1).and_then(|prev| {
            let (record, cert) = self.get_epoch_by_number(prev)?;
            cert?;
            Some(self.get_record_binding(record.digest()).next_key())
        });
        match (from_outcome, from_record) {
            (Some(outcome), Some(record)) if outcome != record => {
                tracing::error!(
                    target: "epoch-manager",
                    epoch,
                    "the certified record of the previous epoch signs another key than this node computed for the epoch: trusting neither"
                );
                None
            }
            (Some(known), _) | (None, Some(known)) => Some(known),
            (None, None) => None,
        }
    }

    fn save_dkg_outcome(&self, epoch: Epoch, outcome: &DkgOutcome) -> StoreResult<()> {
        self.with_write_txn(|tx| tx.insert::<DkgOutcomes>(&epoch, outcome))
    }

    fn save_dkg_dealings(&self, dealings: &[(Epoch, B256, Address, Vec<u8>)]) -> StoreResult<()> {
        if dealings.is_empty() {
            return Ok(());
        }
        self.with_write_txn(|tx| {
            for (epoch, hash, dealer, bytes) in dealings {
                tx.insert::<DkgDealings>(&(*epoch, *hash), &(*dealer, bytes.clone()))?;
            }
            Ok(())
        })
    }

    fn get_dkg_dealing(&self, epoch: Epoch, hash: B256) -> Option<(Address, Vec<u8>)> {
        self.get::<DkgDealings>(&(epoch, hash)).ok().flatten()
    }

    fn prune_dkg_dealings(&self, epoch: Epoch) -> StoreResult<()> {
        // keys are not in epoch order on disk (the epoch encodes little-endian): scan them all,
        // a few epochs' worth at most
        let stale: Vec<(Epoch, B256)> =
            self.iter::<DkgDealings>().map(|(key, _)| key).filter(|(e, _)| *e < epoch).collect();
        if stale.is_empty() {
            return Ok(());
        }
        self.with_write_txn(|tx| {
            for key in &stale {
                tx.remove::<DkgDealings>(key)?;
            }
            Ok(())
        })
    }

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
        binding: &RecordBinding,
    ) -> StoreResult<()> {
        let epoch_hash = epoch_rec.digest();
        let epoch = epoch_rec.epoch;

        self.with_write_txn(|tx| {
            tx.insert::<EpochRecordsIndex>(&epoch_hash, &epoch)?;
            tx.insert::<EpochRecords>(&epoch, epoch_rec)?;
            tx.insert::<EpochCerts>(&epoch_hash, cert)?;
            match binding {
                RecordBinding::Legacy => tx.remove::<EpochRecordBindings>(&epoch_hash)?,
                RecordBinding::NextKey(_) => {
                    tx.insert::<EpochRecordBindings>(&epoch_hash, binding)?
                }
            }
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

    fn remove_epoch_record(&self, epoch: Epoch) -> StoreResult<()> {
        // Read the digest outside the write txn: get() panics inside one.
        let epoch_hash = self.get::<EpochRecords>(&epoch).ok().flatten().map(|rec| rec.digest());
        self.with_write_txn(|tx| {
            if let Some(hash) = epoch_hash {
                tx.remove::<EpochCerts>(&hash)?;
                tx.remove::<EpochRecordsIndex>(&hash)?;
                tx.remove::<EpochRecordBindings>(&hash)?;
            }
            tx.remove::<EpochRecords>(&epoch)?;
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

    /// An epoch's group key: from the certified record before it (a legacy record means
    /// aggregate certificates, a keyed one names the key), from the node's own outcome, and from
    /// neither when the two disagree.
    #[test]
    fn epoch_group_key_rules() {
        let db = MemDatabase::default();
        let key = BlsPublicKey::default();
        assert_eq!(db.epoch_group_key(5), None, "nothing known");

        db.save_epoch_record_with_cert(&record(4), &cert_for(&record(4)), &RecordBinding::Legacy)
            .unwrap();
        assert_eq!(db.epoch_group_key(5), Some(None), "a legacy record: epoch 5 is aggregate");

        let keyed = RecordBinding::NextKey(Some(key));
        db.save_epoch_record_with_cert(&record(4), &cert_for(&record(4)), &keyed).unwrap();
        assert_eq!(db.get_record_binding(record(4).digest()), keyed);
        assert_eq!(db.epoch_group_key(5), Some(Some(key)));

        db.save_dkg_outcome(5, &DkgOutcome::Fallback).unwrap();
        assert_eq!(db.epoch_group_key(5), None, "outcome and record disagree: trust neither");

        db.remove_epoch_record(4).unwrap();
        assert_eq!(db.get_record_binding(record(4).digest()), RecordBinding::Legacy);
        assert_eq!(db.epoch_group_key(5), Some(None), "the outcome alone");
    }

    /// Kept dealings are found by epoch and hash, and pruning drops earlier epochs only.
    #[test]
    fn keeps_and_prunes_dkg_dealings() {
        let db = MemDatabase::default();
        let dealer = Address::repeat_byte(7);
        let dealings: Vec<(Epoch, B256, Address, Vec<u8>)> = (1..=4)
            .map(|epoch| (epoch, B256::repeat_byte(epoch as u8), dealer, vec![epoch as u8; 3]))
            .collect();
        db.save_dkg_dealings(&dealings).unwrap();
        assert_eq!(db.get_dkg_dealing(2, B256::repeat_byte(2)), Some((dealer, vec![2; 3])));
        assert_eq!(db.get_dkg_dealing(2, B256::repeat_byte(3)), None, "hash of another epoch");

        db.prune_dkg_dealings(3).unwrap();
        assert_eq!(db.get_dkg_dealing(1, B256::repeat_byte(1)), None);
        assert_eq!(db.get_dkg_dealing(2, B256::repeat_byte(2)), None);
        assert!(db.get_dkg_dealing(3, B256::repeat_byte(3)).is_some());
        assert!(db.get_dkg_dealing(4, B256::repeat_byte(4)).is_some());
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

        db.save_epoch_record_with_cert(&record(5), &cert_for(&record(5)), &RecordBinding::Legacy)
            .unwrap();

        // The invariant the table relies on: a row exists iff the epoch has no cert on disk.
        assert_eq!(pending_epochs(&db), vec![6]);
        assert!(db.get_epoch_by_number(5).is_some_and(|(_, cert)| cert.is_some()));
    }

    #[test]
    fn saving_a_cert_with_no_pending_record_is_fine() {
        let db = MemDatabase::default();

        db.save_epoch_record_with_cert(&record(5), &cert_for(&record(5)), &RecordBinding::Legacy)
            .unwrap();

        assert!(db.pending_epoch_records().is_empty());
    }
}
