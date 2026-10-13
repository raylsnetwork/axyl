//! Test-only Byzantine certificate-variant forge for the fetched-variant-overwrite reproduction.
//!
//! Compiled only with the `byzantine-test-hooks` feature, never in a production build. On a target
//! round (named by environment) the proposing node collects quorum + 1 votes for its own header and
//! builds a *second, valid* certificate for the SAME header from a different signer subset: same
//! certificate digest, different 2f+1 signer set and aggregate signature. The certifier then pushes
//! that variant through the production *fetch* accept path
//! (`ProcessVerifiedCertificates` -> `CertificateManager::process_verified_certificates`), which,
//! unlike the gossip path, has no "already stored" guard and overwrites the canonical copy
//! last-writer-wins. See `etc/test/fetched-variant-overwrite/` for the live harness.
//!
//! Armed per process by environment, so one node in a test network forges while the rest stay
//! honest:
//!
//!   RL_FVO_ROUND=<round>   header round (epoch >= 1) to forge a variant for (arms the hook)

use crate::state_sync::CertificateManagerCommand;
use crate::ConsensusBus;
use rayls_infrastructure_storage::CertificateStore;
use rayls_infrastructure_types::{
    AuthorityIdentifier, BlsSignature, Certificate, Committee, Database, Hash as _, Header,
    RaylsSender,
};
use tracing::warn;

/// Collects raw votes during a proposal so a valid variant certificate can be built afterwards.
pub(super) struct VariantForge {
    active: bool,
    quorum: usize,
    votes: Vec<(AuthorityIdentifier, BlsSignature)>,
}

impl VariantForge {
    /// Arm for `header` when `RL_FVO_ROUND` names its round (epoch >= 1); otherwise inert.
    pub(super) fn arm(committee: &Committee, header: &Header) -> Self {
        let target: Option<u32> = std::env::var("RL_FVO_ROUND").ok().and_then(|v| v.parse().ok());
        let active = target == Some(header.round()) && header.epoch() >= 1;
        VariantForge { active, quorum: committee.quorum_threshold() as usize, votes: Vec::new() }
    }

    /// Whether the proposal should keep collecting votes past plain quorum (quorum + 1 total), so a
    /// disjoint signer subset is available for the variant.
    pub(super) fn wants_more_votes(&self) -> bool {
        self.active && self.votes.len() <= self.quorum
    }

    /// Record a raw vote (author + signature) for later variant assembly.
    pub(super) fn record(&mut self, author: AuthorityIdentifier, signature: BlsSignature) {
        if self.active {
            self.votes.push((author, signature));
        }
    }

    /// Build a valid variant certificate for `header` from a different signer subset than
    /// `canonical` (drop the first collected signer, take the next `quorum`). Returns `None` unless
    /// a genuinely different, fully verified quorum was formed.
    pub(super) fn build(
        &self,
        committee: &Committee,
        header: &Header,
        canonical: &Certificate,
    ) -> Option<Certificate> {
        if !self.active || self.votes.len() <= self.quorum {
            return None;
        }
        let subset: Vec<_> = self.votes[1..=self.quorum].to_vec();
        match Certificate::new_unverified(committee, header.clone(), subset) {
            Ok(unverified) => match unverified.validate_and_verify(committee) {
                Ok(variant) if variant.signed_authorities() != canonical.signed_authorities() => {
                    warn!(
                        target: "primary::byzantine",
                        round = canonical.round(),
                        canonical_signers = ?canonical.signed_authorities(),
                        variant_signers = ?variant.signed_authorities(),
                        "RL_FVO built a valid variant certificate (same digest, different signers)"
                    );
                    Some(variant)
                }
                Ok(_) => {
                    warn!(target: "primary::byzantine", "RL_FVO variant had the same signer set; skipping");
                    None
                }
                Err(e) => {
                    warn!(target: "primary::byzantine", ?e, "RL_FVO variant failed verification");
                    None
                }
            },
            Err(e) => {
                warn!(target: "primary::byzantine", ?e, "RL_FVO failed to build variant");
                None
            }
        }
    }
}

/// Push `variant` through the production fetch accept path: the same `ProcessVerifiedCertificates`
/// command a node answering a catch-up request feeds the certificate manager. That path has no
/// stored-digest guard, so this overwrites the canonical certificate already in `certificate_store`.
/// Logs the stored signer set before and after, which is the observable overwrite.
pub(super) async fn inject_via_fetch_path<DB: Database>(
    consensus_bus: &ConsensusBus,
    certificate_store: &DB,
    variant: Certificate,
) {
    let signers = |store: &DB| {
        store
            .read(variant.digest())
            .ok()
            .flatten()
            .map(|c| format!("{:?}", c.signed_authorities()))
    };
    let before = signers(certificate_store);
    let (reply, res) = tokio::sync::oneshot::channel();
    if consensus_bus
        .certificate_manager()
        .send(CertificateManagerCommand::ProcessVerifiedCertificates {
            certificates: vec![variant.clone()],
            reply,
        })
        .await
        .is_ok()
    {
        let accept = res.await;
        let after = signers(certificate_store);
        warn!(
            target: "primary::byzantine",
            round = variant.round(),
            digest = ?variant.digest(),
            ?accept,
            stored_before = ?before,
            stored_after = ?after,
            variant_signers = ?variant.signed_authorities(),
            "RL_FVO injected variant through the fetch accept path"
        );
    }
}
