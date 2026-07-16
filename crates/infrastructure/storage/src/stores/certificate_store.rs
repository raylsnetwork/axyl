//! NOTE: tests for this module are in test-utils storage_tests.rs to avoid circular dependancies.

use std::{cmp::Ordering, collections::BTreeMap, future::Future, sync::LazyLock};

use crate::{
    tables::{CertificateDigestByOrigin, CertificateDigestByRound, Certificates},
    StoreResult,
};
use rayls_infrastructure_types::{
    AuthorityIdentifier, Certificate, CertificateDigest, Database, DbTx, DbTxMut, Hash,
    ReadTimeout, Round,
};
use rayls_infrastructure_utils::NotifyRead;

static NOTIFY_SUBSCRIBERS: LazyLock<NotifyRead<CertificateDigest, Certificate>> =
    LazyLock::new(NotifyRead::new);

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

    /// Check database for certificate.
    fn contains(&self, digest: &CertificateDigest) -> StoreResult<bool>;

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
    fn origins_after_round(
        &self,
        round: Round,
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

    fn contains(&self, digest: &CertificateDigest) -> StoreResult<bool> {
        self.contains_key::<Certificates>(digest)
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
    ) -> StoreResult<BTreeMap<Round, Vec<AuthorityIdentifier>>> {
        // Collect results within a properly scoped read transaction
        // to ensure MDBX can reclaim dirty pages after the iterator completes
        self.with_read_txn(|txn| {
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
        // Use with_read_txn to ensure MDBX can reclaim dirty pages after the iterator completes
        self.with_read_txn(|txn| {
            let key = (origin.clone(), round + 1);
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
