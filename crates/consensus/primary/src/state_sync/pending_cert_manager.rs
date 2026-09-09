//! Manage pending certificates.
//!
//! Pending certificates are waiting to be accepted due to missing parents.
//! This mod manages and tracks pending certificates for rounds of consensus.

use crate::{
    error::{CertManagerError, CertManagerResult},
    ConsensusBus,
};
use rayls_infrastructure_types::{
    Certificate, CertificateDigest, CertificateDigestMap, CertificateDigestSet, Hash as _, Round,
};
use std::collections::{BTreeMap, VecDeque};
use tracing::{debug, warn};

/// Warn when pending certificates exceed this threshold.
const PENDING_WARN_THRESHOLD: usize = 10_000;

/// A certificate that is missing parents and pending approval.
///
/// All pending certificates must be verified before adding.
#[derive(Debug, Clone)]
struct PendingCertificate {
    /// The pending certificate.
    certificate: Certificate,
    /// The certificate's missing parents that must be retrieved before the pending certificate is
    /// accepted.
    missing_parent_digests: CertificateDigestSet,
}

impl PendingCertificate {
    /// Create a new instance of Self.
    fn new(certificate: Certificate, missing_parents: CertificateDigestSet) -> Self {
        Self { certificate, missing_parent_digests: missing_parents }
    }
}

/// Manages certificate dependencies and tracks their readiness status.
///
/// Certificates are only accepted after their parents. If a certificate's parents are missing,
/// the certificate is kept here until its parents become available.
///
/// NOTE: all pending certificates must be verified.
#[derive(Debug)]
pub(super) struct PendingCertificateManager {
    /// Pending certificates that cannot be accepted yet.
    ///
    /// Each entry tracks both the certificate itself and its dependency state.
    pending: CertificateDigestMap<PendingCertificate>,
    /// Map of the missing certificate digests and the pending certificates that are blocked by
    /// them.
    ///
    /// The keys are (round, digest) to enable garbage collection by round.
    missing_for_pending: BTreeMap<(Round, CertificateDigest), CertificateDigestSet>,
    /// Consensus channels.
    consensus_bus: ConsensusBus,
}

impl PendingCertificateManager {
    /// Create a new instance of Self.
    pub(super) fn new(consensus_bus: ConsensusBus) -> Self {
        Self { pending: Default::default(), missing_for_pending: Default::default(), consensus_bus }
    }

    /// Insert a pending certificate with its missing parent digests.
    pub(super) fn insert_pending(
        &mut self,
        certificate: Certificate,
        missing_parents: CertificateDigestSet,
    ) -> CertManagerResult<()> {
        let digest = certificate.digest();
        let parent_round = certificate.round().saturating_sub(1);
        debug!(target: "primary::pending_certs", ?digest, "Processing certificate with missing parents");

        self.consensus_bus
            .primary_metrics()
            .node_metrics
            .certificates_suspended
            .with_label_values(&["missing_parents"])
            .inc();

        // track pending certificate
        let pending = PendingCertificate::new(certificate, missing_parents.clone());
        if let Some(existing) = self.pending.insert(digest, pending) {
            if existing.missing_parent_digests != missing_parents {
                return Err(CertManagerError::PendingParentsMismatch(digest));
            }
        }

        let pending_count = self.pending.len();
        if pending_count >= PENDING_WARN_THRESHOLD && pending_count % 1000 == 0 {
            warn!(
                target: "primary::pending_certs",
                pending_count,
                missing_parents_count = self.missing_for_pending.len(),
                "pending certificates exceeds threshold"
            );
        }

        // insert missing parents
        for parent in missing_parents {
            self.missing_for_pending.entry((parent_round, parent)).or_default().insert(digest);
        }

        self.publish_count();

        Ok(())
    }

    /// Publishes the pending count to the proposer's backpressure watch and the metrics mirror.
    ///
    /// `send_replace`, not `send`: nobody holds a receiver (the proposer borrows the sender), and
    /// a `send` with no receivers discards the value instead of storing it.
    fn publish_count(&self) {
        self.consensus_bus.suspended_cert_count().send_replace(self.pending.len());
        // metrics mirror only - decisions read the watch above
        self.consensus_bus
            .primary_metrics()
            .node_metrics
            .certificates_currently_suspended
            .set(self.pending.len() as i64);
    }

    /// When a certificate is accepted, returns all of its children that are now ready to be
    /// verified.
    // TODO: remove after tests
    // synchronizer::state::accept_children
    pub(super) fn update_pending(
        &mut self,
        round: Round,
        digest: CertificateDigest,
    ) -> CertManagerResult<VecDeque<Certificate>> {
        let mut ready_certificates = VecDeque::new();
        let mut certificates_to_process = VecDeque::new();
        certificates_to_process.push_back((round, digest));

        // Process certificates in a cascading manner
        while let Some((next_round, next_digest)) = certificates_to_process.pop_front() {
            // get pending certificates with missing parents
            let Some(pending_digests) = self.missing_for_pending.remove(&(next_round, next_digest))
            else {
                continue;
            };

            // remove missing parents from pending certs and process if ready
            for pending_digest in &pending_digests {
                // get pending cert
                let pending_cert = self
                    .pending
                    .get_mut(pending_digest)
                    .ok_or(CertManagerError::PendingCertificateNotFound(*pending_digest))?;

                // remove parent
                pending_cert.missing_parent_digests.remove(&next_digest);

                // try to accept if no more missing parents
                if pending_cert.missing_parent_digests.is_empty() {
                    // remove from pending
                    let ready = self
                        .pending
                        .remove(pending_digest)
                        .ok_or(CertManagerError::PendingCertificateNotFound(*pending_digest))?;

                    // update any pending certificates waiting for this certificate
                    certificates_to_process.push_back((ready.certificate.round(), *pending_digest));

                    // return this certificate as ready for verification
                    ready_certificates.push_back(ready.certificate);
                }
            }
        }

        // the drain must publish too, or the proposer brake stays stuck at the last suspension
        self.publish_count();
        Ok(ready_certificates)
    }

    /// Return the first key/value in the pending BTreeMap (sorted) that matches the gc round.
    ///
    /// This is useful for iterating through all missing certificates that are blocking pending
    /// certificates from being accepted.
    pub(super) fn next_for_gc_round(
        &mut self,
        gc_round: Round,
    ) -> Option<(Round, CertificateDigest)> {
        let (round, digest) = self
            .missing_for_pending
            .first_key_value()
            .map(|((round, digest), _children)| (*round, *digest))?;

        // check if all gc rounds are processed
        if round > gc_round {
            return None;
        }

        // remove missing parents from gc round
        //
        // NOTE: this digest is returned and will be used to update pending by caller
        if let Some(pending) = self.pending.get_mut(&digest) {
            pending.missing_parent_digests.clear();
        }

        Some((round, digest))
    }

    /// Returns whether a certificate is being tracked.
    pub(super) fn is_pending(&self, digest: &CertificateDigest) -> bool {
        self.pending.contains_key(digest)
    }

    /// Returns the number of pending certificates.
    pub(super) fn num_pending(&self) -> usize {
        self.pending.len()
    }

    /// Filter parents that are pending in place.
    ///
    /// This is used when voting for headers.
    pub(super) fn filter_unknown_digests(&self, unknown: &mut Vec<CertificateDigest>) {
        unknown.retain(|digest| !self.pending.contains_key(digest));
    }
}
