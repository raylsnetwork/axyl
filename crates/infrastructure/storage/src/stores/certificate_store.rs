//! NOTE: tests for this module are in test-utils storage_tests.rs to avoid circular dependancies.

use std::{cmp::Ordering, collections::BTreeMap, future::Future, sync::LazyLock};

use crate::{
    tables::{
        CertificateDigestByOrigin, CertificateDigestByRound, Certificates,
        PrevEpochCertificateDigestByOrigin, PrevEpochCertificates,
    },
    StoreResult,
};
use rayls_infrastructure_types::{
    AuthorityIdentifier, Certificate, CertificateDigest, Database, DbTx, DbTxMut, Hash,
    ReadTimeout, Round,
};
use rayls_infrastructure_utils::NotifyRead;

static NOTIFY_SUBSCRIBERS: LazyLock<NotifyRead<CertificateDigest, Certificate>> =
    LazyLock::new(NotifyRead::new);

/// Which epoch's certificate tables a read targets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CertScope {
    /// The live tables of the current epoch.
    Current,
    /// The tail of the previous epoch kept across the transition clear, see
    /// [`copy_tail_to_prev_epoch`].
    PrevEpoch,
}

/// Rounds of the closing epoch kept in the previous-epoch tail tables across the transition
/// clear. The tail must hold the round after the closing leader, whose certificates are the
/// leader's commit support and are in no committed sub-dag, plus the lag of a validator still
/// finishing that epoch. Fifty rounds is a few seconds of DAG and a few hundred certificates.
pub const PREV_EPOCH_TAIL_ROUNDS: Round = 50;

/// One certificate of the tail kept across the epoch clear: its round, origin, digest and body.
pub type PrevEpochTailEntry = (Round, AuthorityIdentifier, CertificateDigest, Certificate);

/// Read the last `keep_rounds` rounds of the live certificate tables, highest round first.
///
/// Read through the database, not inside the transition's write transaction: the layered
/// database's write transaction refuses every read. Call it right before the clear transaction
/// and hand the result to [`write_prev_epoch_tail`]; nothing writes certificates at that point of
/// the transition, so the two see the same tables.
pub fn live_tail<DB: Database>(
    db: &DB,
    keep_rounds: Round,
) -> StoreResult<Vec<PrevEpochTailEntry>> {
    if keep_rounds == 0 {
        return Ok(Vec::new());
    }
    let Some(((highest, _), _)) = db.last_record::<CertificateDigestByRound>() else {
        return Ok(Vec::new());
    };
    // keep rounds in [from, highest]
    let from = highest.saturating_sub(keep_rounds - 1);
    let index: Vec<_> = db
        .reverse_iter::<CertificateDigestByRound>()
        .take_while(|((round, _), _)| *round >= from)
        .collect();
    let mut tail = Vec::with_capacity(index.len());
    for ((round, origin), digest) in index {
        match db.get::<Certificates>(&digest)? {
            Some(cert) => tail.push((round, origin, digest, cert)),
            // The round index and the certificate bodies are written together, so this is an
            // invariant breach. Surface it here, where it costs a peer a certificate it needs.
            None => tracing::warn!(
                target: "certificate-store",
                round,
                ?origin,
                ?digest,
                "round index entry without a certificate body; left out of the previous-epoch tail"
            ),
        }
    }
    Ok(tail)
}

/// Replace the previous-epoch tail tables with `tail` (from [`live_tail`]). Meant to run inside
/// the transition's write transaction, before the live tables are cleared, so the tail and the
/// clear commit together and a recovery that re-runs the clear cannot lose it.
pub fn write_prev_epoch_tail<TX: DbTxMut>(
    txn: &mut TX,
    tail: &[PrevEpochTailEntry],
) -> StoreResult<()> {
    txn.clear_table::<PrevEpochCertificates>()?;
    txn.clear_table::<PrevEpochCertificateDigestByOrigin>()?;
    for (round, origin, digest, cert) in tail {
        txn.insert::<PrevEpochCertificates>(digest, cert)?;
        txn.insert::<PrevEpochCertificateDigestByOrigin>(&(origin.clone(), *round), digest)?;
    }
    Ok(())
}

/// Certificate persistence with round-based indexing and pub/sub on writes.
///
/// Cert store growth is bounded by epoch length; all tables are cleared
/// at epoch transition via `clear_consensus_db_for_next_epoch`.
/// Tables:
/// - Certificates: The basic digest to certificate store.
/// - CertificateDigestByRound: A secondary index that keeps the certificate digest ids by the
///   certificate rounds. Certificate origin is used to produce unique keys. This helps us to
///   perform range requests based on rounds. We avoid storing again the certificate here to not
///   waste space. To dereference we use the certificates_by_id storage.
/// - CertificateDigestByOrigin: A secondary index that keeps the certificate digest ids by the
///   certificate origins. Certificate rounds are used to produce unique keys. This helps us to
///   perform range requests based on rounds. We avoid storing again the certificate here to not
///   waste space. To dereference we use the certificates_by_id storage.
pub trait CertificateStore {
    /// Inserts a certificate to the store
    fn write(&self, certificate: Certificate) -> StoreResult<()>;

    /// Inserts multiple certificates in the storage. This is an atomic operation.
    /// In the end it notifies any subscribers that are waiting to hear for the
    /// value.
    fn write_all(&self, certificates: impl IntoIterator<Item = Certificate>) -> StoreResult<()>;

    /// Retrieves a certificate from the store. If not found
    /// then None is returned as result.
    fn read(&self, id: CertificateDigest) -> StoreResult<Option<Certificate>>;

    /// Retrieves a certificate from the store by round and authority.
    /// If not found, None is returned as result.
    fn read_by_index(
        &self,
        origin: &AuthorityIdentifier,
        round: Round,
    ) -> StoreResult<Option<Certificate>>;

    /// [`CertificateStore::read_by_index`] against the tables selected by `scope`.
    fn read_by_index_in(
        &self,
        scope: CertScope,
        origin: &AuthorityIdentifier,
        round: Round,
    ) -> StoreResult<Option<Certificate>>;

    /// [`CertificateStore::next_round_number`] against the tables selected by `scope`.
    fn next_round_number_in(
        &self,
        scope: CertScope,
        origin: &AuthorityIdentifier,
        round: Round,
    ) -> StoreResult<Option<Round>>;

    /// Check database for certificate.
    fn contains(&self, digest: &CertificateDigest) -> StoreResult<bool>;

    /// True only when the certificate is present in the memory tier; `false` means "not known
    /// without a persistent-tier read", not "absent". For hot paths that skip work on a hit.
    fn contains_in_memory(&self, digest: &CertificateDigest) -> bool;

    /// Check database for multiple certificates.
    fn multi_contains<'a>(
        &self,
        digests: impl Iterator<Item = &'a CertificateDigest>,
    ) -> StoreResult<Vec<bool>>;

    /// Retrieves multiple certificates by their provided ids. The results
    /// are returned in the same sequence as the provided keys.
    fn read_all(
        &self,
        ids: impl IntoIterator<Item = CertificateDigest>,
    ) -> StoreResult<Vec<Option<Certificate>>>;

    /// Waits to get notified until the requested certificate becomes available
    // Use de-sugared async fn to specify trait bounds and avoid clippy warnings.
    fn notify_read(&self, id: CertificateDigest) -> impl Future<Output = StoreResult<Certificate>>;

    /// Deletes a single certificate by its digest.
    fn delete(&self, id: CertificateDigest) -> StoreResult<()>;

    /// Retrieves all the certificates with round >= the provided round.
    /// The result is returned with certificates sorted in round asc order.
    ///
    /// `timeout` controls whether this (potentially large) scan is subject to the read-transaction
    /// timeout; recovery-path callers pass [`ReadTimeout::Exempt`].
    fn after_round(&self, round: Round, timeout: ReadTimeout) -> StoreResult<Vec<Certificate>>;

    /// Retrieves origins with certificates in each round >= the provided round.
    ///
    /// `timeout` controls whether this scan is subject to the read-transaction
    /// timeout; recovery-path callers pass [`ReadTimeout::Exempt`].
    fn origins_after_round(
        &self,
        round: Round,
        timeout: ReadTimeout,
    ) -> StoreResult<BTreeMap<Round, Vec<AuthorityIdentifier>>>;

    /// Retrieves the certificates of the last round and the round before that
    fn last_two_rounds_certs(&self) -> StoreResult<Vec<Certificate>>;

    /// Retrieves the last certificate of the given origin.
    /// Returns None if there is no certificate for the origin.
    fn last_round(&self, origin: &AuthorityIdentifier) -> StoreResult<Option<Certificate>>;

    /// Retrieves the highest round number in the store.
    /// Returns 0 if there is no certificate in the store.
    fn highest_round_number(&self) -> Round;

    /// Retrieves the last round number of the given origin.
    /// Returns None if there is no certificate for the origin.
    fn last_round_number(&self, origin: &AuthorityIdentifier) -> StoreResult<Option<Round>>;

    /// Retrieves the next round number bigger than the given round for the origin.
    /// Returns None if there is no more local certificate from the origin with bigger round.
    fn next_round_number(
        &self,
        origin: &AuthorityIdentifier,
        round: Round,
    ) -> StoreResult<Option<Round>>;

    /// Clears both the main storage of the certificates and the secondary index
    fn clear(&self) -> StoreResult<()>;

    /// Checks whether the storage is empty. The main storage is
    /// being used to determine this.
    fn is_empty_certs(&self) -> bool;
}

/// Save a cert using an open txn.
fn save_cert<TX: DbTxMut>(
    txn: &mut TX,
    digest: CertificateDigest,
    certificate: Certificate,
) -> StoreResult<()> {
    txn.insert::<Certificates>(&digest, &certificate)?;

    // write the certificates id by their rounds
    let key = (certificate.round(), certificate.origin().clone());
    txn.insert::<CertificateDigestByRound>(&key, &digest)?;

    // write the certificates id by their origins
    let key = (certificate.origin().clone(), certificate.round());
    txn.insert::<CertificateDigestByOrigin>(&key, &digest)?;

    NOTIFY_SUBSCRIBERS.notify(&digest, &certificate);

    Ok(())
}

impl<DB: Database> CertificateStore for DB {
    /// Inserts a certificate to the store
    fn write(&self, certificate: Certificate) -> StoreResult<()> {
        let id = certificate.digest();
        self.with_write_txn(|txn| {
            save_cert(txn, id, certificate)?;
            Ok(())
        })
    }

    /// Inserts multiple certificates in the storage. This is an atomic operation.
    /// In the end it notifies any subscribers that are waiting to hear for the
    /// value.
    fn write_all(&self, certificates: impl IntoIterator<Item = Certificate>) -> StoreResult<()> {
        self.with_write_txn(|txn| {
            for certificate in certificates {
                let digest = certificate.digest();
                if let Err(e) = save_cert(txn, digest, certificate) {
                    tracing::error!("Failed to write certificate for {digest} due to error {e}.");
                    return Err(e);
                }
            }
            Ok(())
        })
    }

    /// Retrieves a certificate from the store. If not found
    /// then None is returned as result.
    fn read(&self, id: CertificateDigest) -> StoreResult<Option<Certificate>> {
        self.get::<Certificates>(&id)
    }

    /// Retrieves a certificate from the store by round and authority.
    /// If not found, None is returned as result.
    fn read_by_index(
        &self,
        origin: &AuthorityIdentifier,
        round: Round,
    ) -> StoreResult<Option<Certificate>> {
        match self.get::<CertificateDigestByOrigin>(&(origin.clone(), round))? {
            Some(d) => self.read(d),
            None => Ok(None),
        }
    }

    fn read_by_index_in(
        &self,
        scope: CertScope,
        origin: &AuthorityIdentifier,
        round: Round,
    ) -> StoreResult<Option<Certificate>> {
        match scope {
            CertScope::Current => self.read_by_index(origin, round),
            CertScope::PrevEpoch => {
                match self.get::<PrevEpochCertificateDigestByOrigin>(&(origin.clone(), round))? {
                    Some(d) => self.get::<PrevEpochCertificates>(&d),
                    None => Ok(None),
                }
            }
        }
    }

    fn next_round_number_in(
        &self,
        scope: CertScope,
        origin: &AuthorityIdentifier,
        round: Round,
    ) -> StoreResult<Option<Round>> {
        match scope {
            CertScope::Current => self.next_round_number(origin, round),
            CertScope::PrevEpoch => self.with_read_txn(|txn| {
                // same guard as `next_round_number`: the round can come from a peer's request
                let Some(next) = round.checked_add(1) else {
                    return Ok(None);
                };
                let key = (origin.clone(), next);
                if let Some(((name, round), _)) =
                    txn.skip_to::<PrevEpochCertificateDigestByOrigin>(&key)?.next()
                {
                    if &name == origin {
                        return Ok(Some(round));
                    }
                }
                Ok(None)
            }),
        }
    }

    fn contains(&self, digest: &CertificateDigest) -> StoreResult<bool> {
        self.contains_key::<Certificates>(digest)
    }

    fn contains_in_memory(&self, digest: &CertificateDigest) -> bool {
        self.contains_key_in_memory::<Certificates>(digest)
    }

    fn multi_contains<'a>(
        &self,
        digests: impl Iterator<Item = &'a CertificateDigest>,
    ) -> StoreResult<Vec<bool>> {
        digests.map(|digest| self.contains_key::<Certificates>(digest)).collect()
    }

    /// Retrieves multiple certificates by their provided ids. The results
    /// are returned in the same sequence as the provided keys.
    fn read_all(
        &self,
        ids: impl IntoIterator<Item = CertificateDigest>,
    ) -> StoreResult<Vec<Option<Certificate>>> {
        ids.into_iter().map(|digest| self.get::<Certificates>(&digest)).collect()
    }

    /// Waits to get notified until the requested certificate becomes available
    async fn notify_read(&self, id: CertificateDigest) -> StoreResult<Certificate> {
        // we register our interest to be notified with the value
        let receiver = NOTIFY_SUBSCRIBERS.register_one(&id);

        // let's read the value because we might have missed the opportunity
        // to get notified about it
        if let Ok(Some(cert)) = self.read(id) {
            // notify any obligations - and remove the entries
            NOTIFY_SUBSCRIBERS.notify(&id, &cert);

            // reply directly
            return Ok(cert);
        }

        // now wait to hear back the result
        let result = receiver.await;

        Ok(result)
    }

    /// Deletes a single certificate by its digest.
    fn delete(&self, id: CertificateDigest) -> StoreResult<()> {
        let cert = match self.read(id)? {
            Some(cert) => cert,
            None => return Ok(()), // Already deleted or never existed - safe no-op
        };

        self.with_write_txn(|txn| {
            txn.remove::<CertificateDigestByRound>(&(cert.round(), cert.origin().clone()))?;
            txn.remove::<CertificateDigestByOrigin>(&(cert.origin().clone(), cert.round()))?;
            txn.remove::<Certificates>(&id)?;
            Ok(())
        })
    }

    /// Retrieves all the certificates with round >= the provided round.
    /// The result is returned with certificates sorted in round asc order
    fn after_round(&self, round: Round, timeout: ReadTimeout) -> StoreResult<Vec<Certificate>> {
        // Collect digests within a properly scoped read transaction
        // to ensure MDBX can reclaim dirty pages after the iterator completes
        self.with_read_txn(|txn| {
            if timeout == ReadTimeout::Exempt {
                // Bounded one-shot recovery scan: opt out of the read-txn cap so a transient I/O
                // stall can't force-reset the txn mid-recovery and fail the caller.
                txn.disable_long_read_safety();
            }
            let iter = if round > 0 {
                txn.skip_to::<CertificateDigestByRound>(
                    &(round - 1, AuthorityIdentifier::default()),
                )?
            } else {
                txn.iter::<CertificateDigestByRound>()
            };

            let digests: Vec<_> = iter
                .filter_map(|((r, _), d)| match r.cmp(&round) {
                    Ordering::Equal | Ordering::Greater => Some(d),
                    Ordering::Less => None,
                })
                .collect();

            let certs_opt = self.multi_get_with_tx::<Certificates>(txn, &digests)?;

            let mut certs = Vec::with_capacity(digests.len());
            for (digest, cert_opt) in digests.into_iter().zip(certs_opt) {
                if let Some(cert) = cert_opt {
                    certs.push(cert);
                } else {
                    return Err(eyre::Report::msg(format!(
                        "Certificate with some digests not found, CertificateStore invariant violation: {digest}"
                    )));
                }
            }

            Ok(certs)
        })
    }

    /// Retrieves origins with certificates in each round >= the provided round.
    fn origins_after_round(
        &self,
        round: Round,
        timeout: ReadTimeout,
    ) -> StoreResult<BTreeMap<Round, Vec<AuthorityIdentifier>>> {
        // Collect results within a properly scoped read transaction
        // to ensure MDBX can reclaim dirty pages after the iterator completes
        self.with_read_txn(|txn| {
            if timeout == ReadTimeout::Exempt {
                // Bounded recovery scan: opt out of the read-txn cap so a transient I/O
                // stall can't force-reset the txn mid-recovery and silently truncate the
                // origin set, causing the cert_fetcher to re-fetch certs it already has.
                txn.disable_long_read_safety();
            }
            // Skip to a row at or before the requested round.
            let iter = if round > 0 {
                txn.skip_to::<CertificateDigestByRound>(&(
                    round - 1,
                    AuthorityIdentifier::default(),
                ))?
            } else {
                txn.iter::<CertificateDigestByRound>()
            };

            let mut result = BTreeMap::<Round, Vec<AuthorityIdentifier>>::new();
            for ((r, origin), _) in iter {
                if r < round {
                    continue;
                }
                result.entry(r).or_default().push(origin);
            }
            Ok(result)
        })
    }

    /// Retrieves the certificates of the last round and the round before that
    fn last_two_rounds_certs(&self) -> StoreResult<Vec<Certificate>> {
        // Collect digests within a properly scoped read transaction
        // to ensure MDBX can reclaim dirty pages after the iterator completes
        self.with_read_txn(|txn| {
            let certificates_reverse = txn.reverse_iter::<CertificateDigestByRound>();

            let mut round = 0;
            let mut digests = Vec::new();

            for (key, digest) in certificates_reverse {
                let (certificate_round, _certificate_origin) = key;

                // We treat zero as special value (round unset) in order to
                // capture the last certificate's round.
                // We are now in a round less than the previous so we want to
                // stop consuming
                if round == 0 {
                    round = certificate_round;
                } else if certificate_round < round - 1 {
                    break;
                }

                digests.push(digest);
            }
            let certs_opt = self.multi_get_with_tx::<Certificates>(txn, &digests)?;

            let mut certificates = Vec::with_capacity(digests.len());
            for (digest, cert_opt) in digests.into_iter().zip(certs_opt) {
                let certificate = cert_opt.ok_or_else(|| {
                    eyre::Report::msg(format!(
                        "Certificate with id {digest} not found in main storage although it should"
                    ))
                })?;
                certificates.push(certificate);
            }

            Ok(certificates)
        })
    }

    /// Retrieves the last certificate of the given origin.
    /// Returns None if there is no certificate for the origin.
    fn last_round(&self, origin: &AuthorityIdentifier) -> StoreResult<Option<Certificate>> {
        self.with_read_txn(|txn| {
            let key = (origin.clone(), Round::MAX);
            if let Some(((name, _round), digest)) =
                txn.record_prior_to::<CertificateDigestByOrigin>(&key)
            {
                if &name == origin {
                    return txn.get::<Certificates>(&digest);
                }
            }
            Ok(None)
        })
    }

    /// Retrieves the highest round number in the store.
    /// Returns 0 if there is no certificate in the store.
    fn highest_round_number(&self) -> Round {
        // Use last_record which is already properly scoped via with_read_txn
        // to ensure MDBX can reclaim dirty pages after completion
        if let Some(((round, _), _)) = self.last_record::<CertificateDigestByRound>() {
            round
        } else {
            0
        }
    }

    /// Retrieves the last round number of the given origin.
    /// Returns None if there is no certificate for the origin.
    fn last_round_number(&self, origin: &AuthorityIdentifier) -> StoreResult<Option<Round>> {
        let key = (origin.clone(), Round::MAX);
        if let Some(((name, round), _)) = self.record_prior_to::<CertificateDigestByOrigin>(&key) {
            if &name == origin {
                return Ok(Some(round));
            }
        }
        Ok(None)
    }

    /// Retrieves the next round number bigger than the given round for the origin.
    /// Returns None if there is no more local certificate from the origin with bigger round.
    fn next_round_number(
        &self,
        origin: &AuthorityIdentifier,
        round: Round,
    ) -> StoreResult<Option<Round>> {
        // `round` can come from a peer's fetch request: at the top of the range there is nothing
        // above, and a plain `+ 1` would abort the node (overflow checks are on in release).
        let Some(next) = round.checked_add(1) else {
            return Ok(None);
        };
        // Use with_read_txn to ensure MDBX can reclaim dirty pages after the iterator completes
        self.with_read_txn(|txn| {
            let key = (origin.clone(), next);
            if let Some(((name, round), _)) = txn.skip_to::<CertificateDigestByOrigin>(&key)?.next()
            {
                if &name == origin {
                    return Ok(Some(round));
                }
            }
            Ok(None)
        })
    }

    /// Clears both the main storage of the certificates and the secondary index
    fn clear(&self) -> StoreResult<()> {
        self.with_write_txn(|txn| {
            txn.clear_table::<CertificateDigestByRound>()?;
            txn.clear_table::<CertificateDigestByOrigin>()?;
            txn.clear_table::<Certificates>()?;
            Ok(())
        })
    }

    /// Checks whether the storage is empty. The main storage is
    /// being used to determine this.
    fn is_empty_certs(&self) -> bool {
        self.is_empty::<Certificates>()
    }
}

// NOTE: tests for this module are in primary/tests/it/storage_tests.rs to avoid circular
// dependancies.

/// Previous-epoch tail tests run here, against the layered database, because its write
/// transaction refuses reads while [`crate::mem_db::MemDatabase`] allows them: a tail step that
/// reads inside the clear transaction passes on the memory database and panics on a node.
#[cfg(test)]
mod prev_epoch_tail_tests {
    use super::*;
    use crate::{layered_db::LayeredDatabase, mem_db::MemDatabase, open_default_tables};
    use rayls_infrastructure_types::Header;

    fn cert(round: Round, author: u8) -> Certificate {
        let mut cert = Certificate::default();
        cert.header = Header {
            round,
            author: AuthorityIdentifier::dummy_for_test(author),
            ..Default::default()
        };
        cert
    }

    #[test]
    fn tail_survives_the_epoch_clear_on_the_layered_database() {
        let mut db = LayeredDatabase::open(MemDatabase::new());
        open_default_tables(&mut db).expect("open tables");
        for round in 1..=4 {
            for author in 0..4u8 {
                db.write(cert(round, author)).expect("write certificate");
            }
        }

        // what the transition does: read the last two rounds through the database, then write
        // them as the tail and clear the live tables in one write transaction
        let tail = live_tail(&db, 2).expect("read tail");
        assert_eq!(tail.len(), 8, "two rounds of four authorities");
        db.with_write_txn(|txn| {
            write_prev_epoch_tail(txn, &tail)?;
            txn.clear_table::<Certificates>()?;
            txn.clear_table::<CertificateDigestByRound>()?;
            txn.clear_table::<CertificateDigestByOrigin>()
        })
        .expect("write tail and clear live tables");

        let a0 = AuthorityIdentifier::dummy_for_test(0);
        assert!(db.read_by_index(&a0, 4).expect("read").is_none(), "live tables are cleared");
        assert!(
            db.read_by_index_in(CertScope::PrevEpoch, &a0, 2).expect("read").is_none(),
            "round 2 is below the kept tail"
        );
        let kept = db.read_by_index_in(CertScope::PrevEpoch, &a0, 3).expect("read");
        assert_eq!(kept.map(|c| c.header.round), Some(3));
        assert_eq!(db.next_round_number_in(CertScope::PrevEpoch, &a0, 2).expect("next"), Some(3));
        assert_eq!(db.next_round_number_in(CertScope::PrevEpoch, &a0, 3).expect("next"), Some(4));
        assert_eq!(db.next_round_number_in(CertScope::PrevEpoch, &a0, 4).expect("next"), None);

        // a walk starting at the top of the round range (a crafted request can send it) must
        // answer "nothing above" instead of overflowing
        assert_eq!(
            db.next_round_number_in(CertScope::PrevEpoch, &a0, Round::MAX).expect("next"),
            None
        );
        assert_eq!(
            db.next_round_number_in(CertScope::Current, &a0, Round::MAX).expect("next"),
            None
        );

        // the next transition replaces the tail
        db.with_write_txn(|txn| write_prev_epoch_tail(txn, &[])).expect("replace tail");
        assert!(db.read_by_index_in(CertScope::PrevEpoch, &a0, 3).expect("read").is_none());
    }
}
