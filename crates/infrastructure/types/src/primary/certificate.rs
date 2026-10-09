//! Certificates and their digests.
//!
//! Certificates are issued by Primaries once their proposed headers are verified by a quorum (2f+1)
//! of peers.

use crate::{
    bcs_layout::{skip_bytes, BcsCursor, BcsLayout, BcsLayoutError},
    crypto::{
        self, to_intent_message, BlsAggregateSignature, BlsPublicKey, BlsSignature,
        ValidatorAggregateSignature,
    },
    ensure,
    error::{CertificateError, CertificateResult, DagError, DagResult, HeaderError},
    now, quorum_threshold,
    serde::RoaringBitmapSerde,
    Authority, AuthorityIdentifier, BlockHash, Committee, Digest, Epoch, Hash, Header, Round,
    TimestampSec, VotingPower,
};
use alloy::primitives::map::FbBuildHasher;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::{collections::BTreeMap, fmt};

/// A header signed by a quorum (2f+1) of the committee.
#[serde_as]
#[derive(Default, Clone, Serialize, Deserialize)]
pub struct Certificate {
    /// Certificate's header.
    pub header: Header,
    /// The aggregate signature over the header and how far it has been verified.
    pub signature_verification_state: SignatureVerificationState,
    /// Bitmap that indicates which authorities from committee signed this certificate.
    #[serde_as(as = "RoaringBitmapSerde")]
    signed_authorities: roaring::RoaringBitmap,
    /// Timestamp for certificate creation.
    ///
    /// This is only used for performance metrics. Consensus relies on the header's timestamp.
    created_at: TimestampSec,
}

impl Certificate {
    /// Creates one empty round-0 certificate per authority.
    pub fn genesis(committee: &Committee) -> Vec<Self> {
        committee
            .authorities()
            .iter()
            .map(|authority| Self {
                header: Header {
                    author: authority.id(),
                    epoch: committee.epoch(),
                    ..Default::default()
                },
                ..Self::default()
            })
            .collect()
    }

    /// Builds a certificate from votes, requiring quorum stake but not verifying signatures.
    pub fn new_unverified(
        committee: &Committee,
        header: Header,
        votes: Vec<(AuthorityIdentifier, BlsSignature)>,
    ) -> DagResult<Certificate> {
        Self::new_internal(
            committee,
            header,
            votes
                .into_iter()
                .filter_map(|(a, sig)| committee.authority(&a).map(|a| (*a.protocol_key(), sig)))
                .collect(),
            true,
        )
    }

    /// Builds a certificate from votes without a stake check, for tests.
    pub fn new_unsigned_for_test(
        committee: &Committee,
        header: Header,
        votes: Vec<(AuthorityIdentifier, BlsSignature)>,
    ) -> DagResult<Certificate> {
        Self::new_internal(
            committee,
            header,
            votes
                .into_iter()
                .filter_map(|(a, sig)| committee.authority(&a).map(|a| (*a.protocol_key(), sig)))
                .collect(),
            false,
        )
    }

    /// Aggregates the votes into a certificate without verifying authority signatures.
    fn new_internal(
        committee: &Committee,
        header: Header,
        // We need votes to be a BTreeMap to force authorities to be in the expected order.
        mut votes: BTreeMap<BlsPublicKey, BlsSignature>,
        check_stake: bool,
    ) -> DagResult<Certificate> {
        let mut weight = 0;
        let mut sigs = Vec::new();

        let auths: BTreeMap<BlsPublicKey, Authority> =
            committee.authorities().into_iter().map(|a| (*a.protocol_key(), a)).collect();
        let filtered_votes = auths
            .iter()
            .enumerate()
            .filter(|(_, (key, authority))| {
                if !votes.is_empty() && *key == votes.first_key_value().expect("votes not empty").0
                {
                    sigs.push(votes.pop_first().expect("votes not empty"));
                    weight += authority.voting_power();
                    let sig_last = sigs.last().expect("sigs not empty");
                    // If there are repeats, also remove them
                    while !votes.is_empty()
                        && votes.first_key_value().expect("votes not empty")
                            == (&sig_last.0, &sig_last.1)
                    {
                        votes.pop_first().expect("votes not empty");
                    }
                    return true;
                }
                false
            })
            .map(|(index, _)| index as u32);

        let signed_authorities= roaring::RoaringBitmap::from_sorted_iter(filtered_votes)
            .map_err(|_| DagError::InvalidBitmap("Failed to convert votes into a bitmap of authority keys. Something is likely very wrong...".to_string()))?;

        // Ensure that all authorities in the set of votes are known
        ensure!(
            votes.is_empty(),
            DagError::UnknownAuthority(
                votes.first_key_value().expect("votes not empty").0.to_string()
            )
        );

        // Ensure that the authorities have enough weight
        ensure!(
            !check_stake || weight >= committee.quorum_threshold(),
            DagError::CertificateRequiresQuorum
        );

        let sigs: Vec<BlsSignature> = sigs.iter().map(|(_, sig)| *sig).collect();
        let bls_signature = if sigs.is_empty() {
            BlsSignature::default()
        } else {
            let aggregated_signature = BlsAggregateSignature::aggregate(&sigs[..], true)
                .map_err(|_| DagError::InvalidSignature)?;

            aggregated_signature.to_signature()
        };

        let signature_verification_state = if !check_stake {
            SignatureVerificationState::Unsigned(bls_signature)
        } else {
            SignatureVerificationState::Unverified(bls_signature)
        };

        Ok(Certificate {
            header,
            signature_verification_state,
            signed_authorities,
            created_at: now(),
        })
    }

    /// Returns the keys of the authorities that signed this certificate.
    ///
    /// The certificate must belong to `committee`'s epoch.
    pub fn signed_authorities_with_committee(&self, committee: &Committee) -> Vec<BlsPublicKey> {
        assert_eq!(committee.epoch(), self.epoch());
        let (_stake, pks) = self.signed_by(&committee.bls_keys());
        pks
    }

    /// Returns the signing weight and the signers' keys, given the committee keys in committee
    /// order.
    pub fn signed_by(&self, committee: &[BlsPublicKey]) -> (VotingPower, Vec<BlsPublicKey>) {
        // Ensure the certificate has a quorum.
        let mut weight = 0;

        let auth_indexes = self.signed_authorities.iter().collect::<Vec<_>>();
        let mut auth_iter = 0;
        let pks = committee
            .iter()
            .enumerate()
            .filter_map(|(i, key)| match auth_indexes.get(auth_iter) {
                Some(index) if *index == i as u32 => {
                    weight += 1; // All validators have a voting weight of 1.
                    auth_iter += 1;
                    Some(*key)
                }
                _ => None,
            })
            .collect();
        (weight, pks)
    }

    /// Validates the certificate against the committee and verifies its aggregate signature,
    /// returning it in verified state. See [`SignatureVerificationState`] for why the state and
    /// the signature bytes travel together.
    pub fn validate_and_verify(self, committee: &Committee) -> CertificateResult<Certificate> {
        // ensure the header is from the correct epoch
        ensure!(
            self.epoch() == committee.epoch(),
            CertificateError::from(HeaderError::InvalidEpoch {
                theirs: self.epoch(),
                ours: committee.epoch()
            })
        );

        // Genesis certificates are always valid.
        if self.round() == 0 && Self::genesis(committee).contains(&self) {
            return Ok(self);
        }

        // Save signature verifications when the header is invalid.
        self.header.validate(committee)?;

        let (weight, pks) = self.signed_by(&committee.bls_keys());

        let threshold = committee.quorum_threshold();
        ensure!(weight >= threshold, CertificateError::Inquorate { stake: weight, threshold });

        let verified_cert = self.verify_signature(pks)?;

        Ok(verified_cert)
    }

    /// Verifies the aggregate signature against `committee`, resetting the verification state
    /// first so an already-verified certificate is re-checked.
    pub fn verify_cert(mut self, committee: &[BlsPublicKey]) -> CertificateResult<Certificate> {
        self = self.validate_received()?;
        let (weight, pks) = self.signed_by(committee);

        // All validator have a vote weight of 1.
        let threshold = quorum_threshold(committee.len() as u64);
        ensure!(weight >= threshold, CertificateError::Inquorate { stake: weight, threshold });

        let verified_cert = self.verify_signature(pks)?;

        Ok(verified_cert)
    }

    /// Verifies the signature unless the state already says it was verified.
    #[allow(deprecated)]
    fn verify_signature(mut self, pks: Vec<BlsPublicKey>) -> CertificateResult<Certificate> {
        // get signature from verification state
        let signature = match self.signature_verification_state {
            SignatureVerificationState::VerifiedDirectly(_)
            | SignatureVerificationState::VerifiedIndirectly(_)
            | SignatureVerificationState::Genesis => return Ok(self),
            SignatureVerificationState::Unverified(ref sig) => sig,
            SignatureVerificationState::Unsigned(_) => {
                return Err(CertificateError::Unsigned);
            }
        };

        // Verify the signatures
        let certificate_digest = self.digest();
        let aggregate_signature = BlsAggregateSignature::from_signature(signature);
        if !aggregate_signature.verify_secure(&to_intent_message(certificate_digest), &pks[..]) {
            return Err(CertificateError::InvalidSignature);
        }

        self.signature_verification_state =
            SignatureVerificationState::VerifiedDirectly(*signature);

        Ok(self)
    }

    /// Marks a received certificate as unverified so it must pass signature verification.
    pub fn validate_received(mut self) -> CertificateResult<Self> {
        self.set_signature_verification_state(SignatureVerificationState::Unverified(
            self.aggregated_signature()
                .ok_or(CertificateError::RecoverBlsAggregateSignatureBytes)?,
        ));
        Ok(self)
    }

    /// The certificate's round.
    pub fn round(&self) -> Round {
        self.header.round()
    }

    /// The certificate's epoch.
    pub fn epoch(&self) -> Epoch {
        self.header.epoch()
    }

    /// The nonce of this certificate's header.
    pub fn nonce(&self) -> u64 {
        self.header.nonce()
    }

    /// The author of the certificate.
    pub fn origin(&self) -> &AuthorityIdentifier {
        self.header.author()
    }

    /// The header for the certificate.
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// The aggregate signature, absent for genesis.
    #[allow(deprecated)]
    pub fn aggregated_signature(&self) -> Option<BlsSignature> {
        match &self.signature_verification_state {
            SignatureVerificationState::VerifiedDirectly(sig)
            | SignatureVerificationState::VerifiedIndirectly(sig)
            | SignatureVerificationState::Unverified(sig)
            | SignatureVerificationState::Unsigned(sig) => Some(*sig),
            SignatureVerificationState::Genesis => None,
        }
    }

    /// The signature verification state.
    pub fn signature_verification_state(&self) -> &SignatureVerificationState {
        &self.signature_verification_state
    }

    /// The time (sec) when the certificate was created.
    ///
    /// This is only used for performance metrics. Consensus relies on the header's timestamp.
    pub fn created_at(&self) -> &TimestampSec {
        &self.created_at
    }

    /// Sets the signature verification state.
    pub fn set_signature_verification_state(&mut self, state: SignatureVerificationState) {
        self.signature_verification_state = state;
    }

    /// The bitmap of authorities, in committee order, whose votes form the aggregate signature.
    pub fn signed_authorities(&self) -> &roaring::RoaringBitmap {
        &self.signed_authorities
    }

    /// Returns true if the signature has been verified or the certificate is genesis.
    #[allow(deprecated)]
    pub fn is_verified(&self) -> bool {
        matches!(
            self.signature_verification_state,
            SignatureVerificationState::VerifiedDirectly(_)
                | SignatureVerificationState::VerifiedIndirectly(_)
                | SignatureVerificationState::Genesis
        )
    }

    /// Replaces the certificate's header. Test-only.
    pub fn update_header_for_test(&mut self, header: Header) {
        self.header = header;
    }

    /// Returns the header mutably. Test-only.
    pub fn header_mut_for_test(&mut self) -> &mut Header {
        &mut self.header
    }

    /// Replaces the creation timestamp. Test-only.
    pub fn update_created_at_for_test(&mut self, timestamp: TimestampSec) {
        self.created_at = timestamp;
    }
}

impl From<&[u8]> for Certificate {
    fn from(value: &[u8]) -> Self {
        crate::decode(value)
    }
}

impl From<&Certificate> for Vec<u8> {
    fn from(value: &Certificate) -> Self {
        crate::encode(value)
    }
}

impl Certificate {
    /// Skips one BCS-encoded certificate, returning the embedded header's byte span (the range
    /// [`Header::digest`] hashes, and therefore the certificate's digest preimage).
    pub fn skip_with_header_span<'a>(c: &mut BcsCursor<'a>) -> Result<&'a [u8], BcsLayoutError> {
        let header = c.take_span::<Header>()?;
        c.skip::<SignatureVerificationState>()?;
        skip_bytes(c)?;
        c.skip::<TimestampSec>()?;
        Ok(header)
    }
}

/// BCS layout: `header, signature_verification_state, signed_authorities,
/// created_at`. `signed_authorities` is a roaring bitmap, on the wire as
/// ULEB128(L) + L bytes. Keep in lockstep with the struct.
impl BcsLayout for Certificate {
    fn skip(c: &mut BcsCursor<'_>) -> Result<(), BcsLayoutError> {
        Self::skip_with_header_span(c).map(drop)
    }
}

/// Verification status of a certificate's aggregate signature, carrying the signature bytes.
///
/// The bytes live inside the state rather than beside it so a status can only be reached by
/// operating on the exact bytes it describes: a verified state cannot hold bytes other than the
/// ones that were verified, and an unsigned state cannot hold a quorum signature.
#[derive(Copy, Clone, Serialize, Deserialize, Debug)]
pub enum SignatureVerificationState {
    /// The certificate has not yet received a quorum of signatures.
    Unsigned(BlsSignature),
    /// The certificate arrived from the network and has not been verified.
    Unverified(BlsSignature),
    /// The aggregate signature was verified on this node.
    VerifiedDirectly(BlsSignature),
    /// Kept for BCS variant index stability with existing consensus DBs.
    #[deprecated(note = "not assigned, kept for serialization index stability")]
    VerifiedIndirectly(BlsSignature),
    /// A genesis certificate, which carries no signature and needs no verification.
    Genesis,
}

impl Default for SignatureVerificationState {
    fn default() -> Self {
        SignatureVerificationState::Unsigned(BlsSignature::default())
    }
}

/// BCS layout: ULEB128 variant tag, then the payload. Tags MUST match the enum
/// order: 0 Unsigned, 1 Unverified, 2 VerifiedDirectly, 3 VerifiedIndirectly,
/// 4 Genesis.
impl BcsLayout for SignatureVerificationState {
    fn skip(c: &mut BcsCursor<'_>) -> Result<(), BcsLayoutError> {
        let tag = c.read_uleb128()?;
        match tag {
            0..=3 => c.skip::<BlsSignature>().map(drop),
            4 => Ok(()),
            _ => {
                Err(BcsLayoutError::UnknownVariant { tag, type_name: "SignatureVerificationState" })
            }
        }
    }
}

/// Marks a received certificate as unverified so it must pass signature verification.
pub fn validate_received_certificate(
    mut certificate: Certificate,
) -> CertificateResult<Certificate> {
    certificate.set_signature_verification_state(SignatureVerificationState::Unverified(
        certificate
            .aggregated_signature()
            .ok_or(CertificateError::RecoverBlsAggregateSignatureBytes)?,
    ));
    Ok(certificate)
}

/// Digest of a certificate, equal to the digest of its header.
#[derive(
    Clone, Copy, Default, PartialEq, Eq, std::hash::Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct CertificateDigest(Digest<{ crypto::DIGEST_LENGTH }>);

/// Hash map keyed by [`CertificateDigest`].
///
/// The key is already a uniformly distributed 32-byte hash, so the hasher consumes its bytes
/// directly instead of running SipHash over them on every lookup. Sound because the derived
/// `Hash` writes exactly the digest's 32 bytes: array length prefixes route through
/// `write_usize`, which this hasher discards.
pub type CertificateDigestMap<V> =
    std::collections::HashMap<CertificateDigest, V, FbBuildHasher<32>>;

/// Hash set of [`CertificateDigest`]; see [`CertificateDigestMap`] for why the hasher is sound.
pub type CertificateDigestSet = std::collections::HashSet<CertificateDigest, FbBuildHasher<32>>;

impl CertificateDigest {
    /// Creates a digest from its raw bytes.
    pub fn new(digest: [u8; crypto::DIGEST_LENGTH]) -> Self {
        CertificateDigest(Digest { digest })
    }
}

/// BCS layout: delegates to inner `Digest<32>` (ULEB128(32) + 32 raw bytes).
impl BcsLayout for CertificateDigest {
    #[inline]
    fn skip(c: &mut BcsCursor<'_>) -> Result<(), BcsLayoutError> {
        c.skip::<Digest<{ crypto::DIGEST_LENGTH }>>().map(drop)
    }
}

impl AsRef<[u8]> for CertificateDigest {
    fn as_ref(&self) -> &[u8] {
        &self.0.digest
    }
}

impl From<CertificateDigest> for Digest<{ crypto::DIGEST_LENGTH }> {
    fn from(hd: CertificateDigest) -> Self {
        hd.0
    }
}

impl fmt::Debug for CertificateDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for CertificateDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0.to_string().get(0..16).ok_or(fmt::Error)?)
    }
}

impl Hash<{ crypto::DIGEST_LENGTH }> for Certificate {
    type TypedDigest = CertificateDigest;

    fn digest(&self) -> CertificateDigest {
        CertificateDigest(Digest { digest: self.header.digest().into() })
    }
}

impl From<CertificateDigest> for BlockHash {
    fn from(value: CertificateDigest) -> Self {
        Self::from(value.0.digest)
    }
}

impl fmt::Debug for Certificate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: C{}({}, {}, E{})",
            self.digest(),
            self.round(),
            self.origin(),
            self.header.digest(),
            self.epoch()
        )
    }
}

impl PartialEq for Certificate {
    fn eq(&self, other: &Self) -> bool {
        let mut ret = self.header().digest() == other.header().digest();
        ret &= self.round() == other.round();
        ret &= self.epoch() == other.epoch();
        ret &= self.origin() == other.origin();
        ret
    }
}

#[cfg(test)]
mod tests {

    /// The fixed-bytes hasher requires every key to hash as exactly 32 bytes and enforces it
    /// with a debug assertion, so this exercises real map operations to pin that
    /// `CertificateDigest`'s derived `Hash` satisfies the contract. A key that also wrote a
    /// length prefix would trip the assertion here.
    #[test]
    fn certificate_digest_map_hashes_exactly_the_digest_bytes() {
        let mut map = super::CertificateDigestMap::default();
        let mut set = super::CertificateDigestSet::default();
        for byte in 0..8u8 {
            let digest = CertificateDigest::new([byte; crypto::DIGEST_LENGTH]);
            map.insert(digest, byte);
            set.insert(digest);
        }
        let probe = CertificateDigest::new([3u8; crypto::DIGEST_LENGTH]);
        assert_eq!(map.get(&probe), Some(&3));
        assert!(set.contains(&probe));
        assert_eq!(map.len(), 8);
    }

    use super::*;

    /// Creates a minimal certificate for serialization tests.
    fn make_test_certificate(state: SignatureVerificationState) -> Certificate {
        Certificate {
            header: Header::default(),
            signature_verification_state: state,
            signed_authorities: roaring::RoaringBitmap::from_iter([0u32, 1, 2]),
            created_at: 1000,
        }
    }

    /// Verifies the BCS roundtrip for every SignatureVerificationState variant.
    /// Removing or reordering variants breaks persisted certificate deserialization.
    #[test]
    #[allow(deprecated)]
    fn bcs_roundtrip_all_signature_verification_states() {
        let sig = BlsSignature::default();

        let variants = [
            ("Unsigned", SignatureVerificationState::Unsigned(sig)),
            ("Unverified", SignatureVerificationState::Unverified(sig)),
            ("VerifiedDirectly", SignatureVerificationState::VerifiedDirectly(sig)),
            ("VerifiedIndirectly", SignatureVerificationState::VerifiedIndirectly(sig)),
            ("Genesis", SignatureVerificationState::Genesis),
        ];

        for (name, state) in variants {
            let cert = make_test_certificate(state);
            let bytes: Vec<u8> = (&cert).into();
            let decoded = Certificate::from(bytes.as_slice());

            assert_eq!(
                cert.header().digest(),
                decoded.header().digest(),
                "{name}: header mismatch"
            );
            assert_eq!(cert.round(), decoded.round(), "{name}: round mismatch");
            assert_eq!(cert.epoch(), decoded.epoch(), "{name}: epoch mismatch");
            assert_eq!(
                cert.signed_authorities(),
                decoded.signed_authorities(),
                "{name}: signed_authorities mismatch"
            );

            // re-encode to verify variant index stability
            let re_encoded: Vec<u8> = (&decoded).into();
            assert_eq!(
                bytes, re_encoded,
                "{name}: re-encoded bytes differ - variant index may have shifted"
            );
        }
    }

    /// Pins BCS variant indices to expected positions.
    /// BCS uses ULEB128 variant indices; shifting breaks DB compatibility.
    #[test]
    #[allow(deprecated)]
    fn bcs_variant_index_stability() {
        let sig = BlsSignature::default();

        let expected_indices: &[(SignatureVerificationState, u8)] = &[
            (SignatureVerificationState::Unsigned(sig), 0),
            (SignatureVerificationState::Unverified(sig), 1),
            (SignatureVerificationState::VerifiedDirectly(sig), 2),
            (SignatureVerificationState::VerifiedIndirectly(sig), 3),
            (SignatureVerificationState::Genesis, 4),
        ];

        for (state, expected_idx) in expected_indices {
            let bytes = crate::encode(state);
            assert_eq!(
                bytes[0], *expected_idx,
                "BCS variant index for {:?} shifted from {} to {} - this breaks DB compatibility",
                state, expected_idx, bytes[0]
            );
        }
    }
}
