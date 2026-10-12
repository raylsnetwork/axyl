// SPDX-License-Identifier: BUSL-1.1
//! Serializable, human-oriented projections of consensus types.
//!
//! The reports keep their own views for two reasons: they carry facts the wire types do not
//! (verification results, storage tiers, link checks), and they render every byte string as full
//! `0x` hex, where the wire types' own JSON mixes hex (`B256`) with base58 (digests, keys,
//! signatures, authority identifiers). Verbose reports embed the wire type itself as `raw`, which
//! for a consensus header is the object the `rayls_latestHeader` RPC returns.

use alloy::{
    consensus::transaction::SignerRecoverable as _,
    eips::{eip2718::Decodable2718 as _, Typed2718 as _},
};
use rayls_infrastructure_types::{
    encode, keccak256, AuthorityIdentifier, BlockHash, BlsPublicKey, BlsSignature, Certificate,
    CertificateDigest, ConsensusHeader, EpochTransitionCheckpoint, Hash as _,
    SignatureVerificationState, TransactionSigned, TransactionTrait as _, TxKind, B256,
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

/// What the tool derives from a DAG certificate, including the parts consensus does not hash:
/// the signer set and aggregate signature. Two nodes can hold certificates with equal digests but
/// different signers; comparing these fields is how that is detected.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CertificateView {
    #[serde(flatten)]
    pub summary: CertificateSummary,
    pub header_digest: String,
    pub created_at: u64,
    pub signers: Vec<u32>,
    pub signer_count: u64,
    pub signature: Option<String>,
    pub verification_state: &'static str,
    /// The certificate as stored, in the wire type's own JSON form (`-v`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw: Option<Certificate>,
}

impl CertificateView {
    pub fn of(cert: &Certificate, verbose: bool) -> Self {
        Self {
            summary: CertificateSummary::of(cert),
            header_digest: hex(cert.header().digest()),
            created_at: *cert.created_at(),
            signers: cert.signed_authorities().iter().collect(),
            signer_count: cert.signed_authorities().len(),
            signature: cert.aggregated_signature().map(|s| signature(&s)),
            verification_state: verification_state_name(cert.signature_verification_state()),
            raw: verbose.then(|| cert.clone()),
        }
    }

    /// The fields that must agree for two nodes to hold the *same* certificate, signatures
    /// included.
    pub fn signature_identity(&self) -> (&[u32], Option<&str>) {
        (&self.signers, self.signature.as_deref())
    }
}

/// Compact identification of a stored consensus header: its own fields and its sub-dag's counts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HeaderSummary {
    pub number: u64,
    pub digest: String,
    pub parent_hash: String,
    pub leader: CertificateSummary,
    pub certificate_count: usize,
    pub batch_count: usize,
    pub commit_timestamp: u64,
}

impl HeaderSummary {
    pub fn of(header: &ConsensusHeader) -> Self {
        let sub_dag = &header.sub_dag;
        Self {
            number: header.number,
            digest: b256(&header.digest()),
            parent_hash: b256(&header.parent_hash),
            leader: CertificateSummary::of(&sub_dag.leader),
            certificate_count: sub_dag.certificates.len(),
            batch_count: sub_dag.certificates.iter().map(|c| c.header().payload().len()).sum(),
            commit_timestamp: sub_dag.commit_timestamp(),
        }
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

/// One transaction of a batch, decoded from its stored EIP-2718 bytes.
///
/// The hash is keccak256 of the stored bytes: the transaction hash for every transaction type,
/// because the node only stores exact envelopes (peer batches are validated with an exact decode).
/// It is shown even when the bytes do not decode, since that is what the table is keyed by.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct TransactionView {
    /// Position within the batch.
    pub index: usize,
    pub hash: String,
    pub bytes: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_type: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chain_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nonce: Option<u64>,
    /// Recovered sender; absent when the signature does not recover.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    /// Recipient, or `create` for contract creation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<String>,
    /// Value in wei, decimal.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gas_limit: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_fee_per_gas: Option<u128>,
    /// Why the bytes did not decode as a signed transaction.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl TransactionView {
    pub fn of(index: usize, raw: &[u8]) -> Self {
        let mut view =
            Self { index, hash: b256(&keccak256(raw)), bytes: raw.len(), ..Self::default() };
        match TransactionSigned::decode_2718_exact(raw) {
            Ok(tx) => {
                view.tx_type = Some(tx.ty());
                view.chain_id = tx.chain_id();
                view.nonce = Some(tx.nonce());
                view.from = tx.recover_signer().ok().map(|a| a.to_string());
                view.to = Some(match tx.kind() {
                    TxKind::Call(to) => to.to_string(),
                    TxKind::Create => "create".to_owned(),
                });
                view.value = Some(tx.value().to_string());
                view.gas_limit = Some(tx.gas_limit());
                view.max_fee_per_gas = Some(tx.max_fee_per_gas());
            }
            Err(err) => view.error = Some(err.to_string()),
        }
        view
    }
}
