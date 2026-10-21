//! Test-only poison override for missing-certificates (catch-up) fetch replies.
//!
//! Compiled only with the `merged-fetch-poison` feature, never in a production build. It lets a
//! committee member answer a catching-up validator's missing-certificates request with a single
//! unusable certificate instead of its real certificates, which is the reproduction lever for the
//! merged-fetch-poison issue: the victim merges every peer's answer into one list and discards the
//! whole list, including honest certificates, when any one certificate fails validation.
//!
//! It is armed per process by environment, so a single node in a test network poisons replies while
//! the rest stay honest:
//!
//!   RL_POISON_CERT_FETCH=1   answer every missing-certificates request with one bad certificate
//!
//! The bad certificate carries a default header at round `exclusive_lower_bound + 1` of the
//! request, so it sits above the victim's garbage-collection round and reaches signature
//! verification instead of being skipped as too old, and an `Unverified` zero signature, so that
//! verification fails and the victim discards the merged batch.

use rayls_infrastructure_types::{
    BlsSignature, Certificate, Epoch, Round, SignatureVerificationState,
};
use tracing::warn;

/// Returns a poisoned certificate list to serve back for a missing-certificates request, or None to
/// serve the honestly collected certificates. `exclusive_lower_bound` is the request's lower bound,
/// which is the requester's garbage-collection round. `our_epoch` is the committee epoch the
/// response is served from. `honest_len` is the number of real certificates the honest path would
/// have returned, for the log line only.
pub(super) fn override_missing_certs(
    exclusive_lower_bound: Round,
    our_epoch: Epoch,
    honest_len: usize,
) -> Option<Vec<Certificate>> {
    if std::env::var("RL_POISON_CERT_FETCH").is_err() {
        return None;
    }
    // One round above the requester's garbage-collection round, so the victim does not skip it as
    // too old and instead runs signature verification on it, which fails.
    let poison_round = exclusive_lower_bound.saturating_add(1);
    let mut cert = Certificate::default();
    cert.header.round = poison_round;
    // The victim filters out any certificate outside its current epoch before verification, so the
    // poison certificate must carry the victim's epoch to reach signature verification at all.
    cert.header.epoch = our_epoch;
    cert.set_signature_verification_state(SignatureVerificationState::Unverified(
        BlsSignature::default(),
    ));
    warn!(
        target: "primary::merged_fetch_poison",
        poison_round,
        our_epoch,
        honest_len,
        "POISON CERT-FETCH: replacing honest certificates with one unverifiable certificate"
    );
    Some(vec![cert])
}
