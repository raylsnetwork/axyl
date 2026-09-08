// SPDX-License-Identifier: BUSL-1.1
//! Serializable, human-oriented projections of consensus types.
//!
//! The consensus types themselves are not fit for JSON output: `ConsensusHeader` fails to
//! serialize (its reputation map is keyed by a non-string type) and digests print inconsistently
//! (some hex, some truncated base58). Everything here renders bytes as full `0x` hex.

use rayls_infrastructure_types::{
    encode, AuthorityIdentifier, BlockHash, BlsPublicKey, BlsSignature, Certificate,
    CertificateDigest, EpochTransitionCheckpoint, Hash as _, SignatureVerificationState, WorkerId,
    B256,
};
use serde::Serialize;

/// `0x`-prefixed lowercase hex.
pub fn hex(bytes: impl AsRef<[u8]>) -> String {
    const_hex::encode_prefixed(bytes)
}

/// Hex of a 32-byte hash.
pub fn b256(hash: &B256) -> String {
    hex(hash.as_slice())
}

/// Hex of an authority identifier (its 32 raw bytes).
pub fn authority(id: &AuthorityIdentifier) -> String {
    hex(encode(id))
}

/// Hex of a BLS public key (96 compressed bytes).
pub fn pubkey(key: &BlsPublicKey) -> String {
    hex(key.as_ref())
}

/// Hex of a BLS signature (96 compressed bytes).
pub fn signature(sig: &BlsSignature) -> String {
    hex(sig.to_bytes())
}

/// Hex of a DAG certificate digest.
pub fn cert_digest(digest: CertificateDigest) -> String {
    b256(&BlockHash::from(digest))
}

/// Compact identification of a DAG certificate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CertificateSummary {
    pub digest: String,
    pub author: String,
    pub round: u32,
    pub epoch: u32,
}

impl CertificateSummary {
    pub fn of(cert: &Certificate) -> Self {
        Self {
            digest: cert_digest(cert.digest()),
            author: authority(cert.origin()),
            round: cert.round(),
            epoch: cert.epoch(),
        }
    }
}

/// Full view of a DAG certificate, including the parts consensus does not hash: the signer set
/// and aggregate signature. Two nodes can hold certificates with equal digests but different
/// signers; comparing these fields is how that is detected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CertificateView {
    #[serde(flatten)]
    pub summary: CertificateSummary,
    pub header_digest: String,
    pub created_at: u64,
    pub signers: Vec<u32>,
    pub signer_count: u64,
    pub signature: Option<String>,
    pub verification_state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parents: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Vec<PayloadEntry>>,
}

/// One batch reference in a header's payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PayloadEntry {
    pub batch: String,
    pub worker: WorkerId,
}

impl CertificateView {
    pub fn of(cert: &Certificate, verbose: bool) -> Self {
        let header = cert.header();
        Self {
            summary: CertificateSummary::of(cert),
            header_digest: hex(header.digest()),
            created_at: *cert.created_at(),
            signers: cert.signed_authorities().iter().collect(),
            signer_count: cert.signed_authorities().len(),
            signature: cert.aggregated_signature().map(|s| signature(&s)),
            verification_state: verification_state_name(cert.signature_verification_state()),
            parents: verbose.then(|| header.parents().iter().map(|d| cert_digest(*d)).collect()),
            payload: verbose.then(|| {
                header
                    .payload()
                    .iter()
                    .map(|(batch, worker)| PayloadEntry { batch: b256(batch), worker: *worker })
                    .collect()
            }),
        }
    }

    /// The fields that must agree for two nodes to hold the *same* certificate, signatures
    /// included.
    pub fn signature_identity(&self) -> (&[u32], Option<&str>) {
        (&self.signers, self.signature.as_deref())
    }
}

#[allow(deprecated)]
fn verification_state_name(state: &SignatureVerificationState) -> &'static str {
    match state {
        SignatureVerificationState::Unsigned(_) => "unsigned",
        SignatureVerificationState::Unverified(_) => "unverified",
        SignatureVerificationState::VerifiedDirectly(_) => "verified-directly",
        SignatureVerificationState::VerifiedIndirectly(_) => "verified-indirectly",
        SignatureVerificationState::Genesis => "genesis",
    }
}

/// A leftover epoch-transition checkpoint: proof of an interrupted transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CheckpointView {
    pub epoch: u32,
    pub completed_phase: String,
    pub target_hash: String,
    pub timestamp: u64,
}

impl CheckpointView {
    pub fn of(cp: &EpochTransitionCheckpoint) -> Self {
        Self {
            epoch: cp.epoch,
            completed_phase: format!("{:?}", cp.completed_phase),
            target_hash: b256(&cp.target_hash),
            timestamp: cp.timestamp,
        }
    }
}
