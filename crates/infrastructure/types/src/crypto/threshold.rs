//! Threshold BLS over `blst` `min_sig`: one committee key, one secret share per validator, and
//! a combined signature that is the same for every `t`-subset of shares.
//!
//! A share is the evaluation of a degree `t - 1` polynomial at a non-zero index. The committee's
//! public polynomial (Feldman commitments in G2) lets anyone derive a share's public key and
//! verify a partial signature against it. Combining `t` partial signatures interpolates the
//! polynomial at zero, so the result is the signature of the constant term, whichever `t`
//! shares were used. That is what makes a certificate unique: every quorum produces the same
//! 48 bytes.
//!
//! Signing and verification use the same `min_sig` keys, signatures and domain separation tag
//! as the rest of the node, so a combined signature verifies with `BlsSignature::verify_secure`
//! against the group key like any single signature does today.

use super::{BlsPublicKey, BlsSignature, Signer, DST_G1};
use blst::{
    blst_bendian_from_scalar, blst_fr, blst_fr_add, blst_fr_from_scalar, blst_fr_from_uint64,
    blst_fr_inverse, blst_fr_mul, blst_fr_sub, blst_scalar, blst_scalar_from_fr,
    min_sig::{AggregatePublicKey, AggregateSignature, PublicKey, SecretKey, Signature},
};
use rand::{CryptoRng, RngCore};
use serde::{Deserialize, Serialize};
use std::{num::NonZeroU32, ops::Deref, ptr};
use thiserror::Error;

/// Scalars passed to `blst`'s multi-scalar multiplication are full field elements.
const SCALAR_BITS: usize = 255;
/// Little-endian size of one such scalar.
const SCALAR_BYTES: usize = 32;
/// Domain of the challenge hash in constant-term proofs.
const PROOF_DST: &[u8] = b"RAYLS_DKG_POK_BLS12381G2_XMD:SHA-256_SSWU_RO_";

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ThresholdError {
    #[error("threshold must be at least 1")]
    ZeroThreshold,
    #[error("share index must not be zero")]
    ZeroIndex,
    #[error("polynomials have different thresholds: {0} and {1}")]
    ThresholdMismatch(usize, usize),
    #[error("shares have different indices: {0} and {1}")]
    IndexMismatch(u32, u32),
    #[error("need {needed} partial signatures from distinct shares, got {got}")]
    NotEnoughShares { needed: usize, got: usize },
    #[error("a secret value was zero")]
    ZeroSecret,
    #[error("a partial signature is not a valid group element")]
    BadSignature,
    #[error("the proof challenge hashed to zero")]
    ZeroChallenge,
}

/// Element of the scalar field in `blst`'s Montgomery form. Wiped on drop because it may hold a
/// secret polynomial value.
struct Fr(blst_fr);

impl Clone for Fr {
    fn clone(&self) -> Self {
        Fr(self.0)
    }
}

impl Drop for Fr {
    fn drop(&mut self) {
        // SAFETY: writing zeros to our own limbs; volatile so the wipe is not optimised away.
        unsafe { ptr::write_volatile(&mut self.0.l, [0u64; 4]) };
    }
}

impl Fr {
    fn from_u64(value: u64) -> Self {
        let limbs = [value, 0, 0, 0];
        let mut out = blst_fr::default();
        // SAFETY: `limbs` has the four words blst reads; `out` is a valid destination.
        unsafe { blst_fr_from_uint64(&mut out, limbs.as_ptr()) };
        Fr(out)
    }

    fn from_scalar(scalar: &blst_scalar) -> Self {
        let mut out = blst_fr::default();
        // SAFETY: both pointers reference live, properly sized values.
        unsafe { blst_fr_from_scalar(&mut out, scalar) };
        Fr(out)
    }

    fn to_scalar(&self) -> blst_scalar {
        let mut out = blst_scalar { b: [0u8; SCALAR_BYTES] };
        // SAFETY: both pointers reference live, properly sized values.
        unsafe { blst_scalar_from_fr(&mut out, &self.0) };
        out
    }

    /// Canonical big-endian encoding, the form `SecretKey::from_bytes` takes.
    fn to_be_bytes(&self) -> [u8; SCALAR_BYTES] {
        let scalar = self.to_scalar();
        let mut out = [0u8; SCALAR_BYTES];
        // SAFETY: `out` is 32 bytes, the size blst writes.
        unsafe { blst_bendian_from_scalar(out.as_mut_ptr(), &scalar) };
        out
    }

    fn add(&self, other: &Fr) -> Fr {
        let mut out = blst_fr::default();
        // SAFETY: all pointers reference live field elements.
        unsafe { blst_fr_add(&mut out, &self.0, &other.0) };
        Fr(out)
    }

    fn sub(&self, other: &Fr) -> Fr {
        let mut out = blst_fr::default();
        // SAFETY: all pointers reference live field elements.
        unsafe { blst_fr_sub(&mut out, &self.0, &other.0) };
        Fr(out)
    }

    fn mul(&self, other: &Fr) -> Fr {
        let mut out = blst_fr::default();
        // SAFETY: all pointers reference live field elements.
        unsafe { blst_fr_mul(&mut out, &self.0, &other.0) };
        Fr(out)
    }

    /// Multiplicative inverse; `None` for zero, which has none.
    fn inverse(&self) -> Option<Fr> {
        if self.is_zero() {
            return None;
        }
        let mut out = blst_fr::default();
        // SAFETY: both pointers reference live field elements.
        unsafe { blst_fr_inverse(&mut out, &self.0) };
        Some(Fr(out))
    }

    fn is_zero(&self) -> bool {
        self.0.l == [0u64; 4]
    }

    /// The field element as a signing key. Fails for zero, which `blst` rejects as a key.
    fn to_secret_key(&self) -> Result<SecretKey, ThresholdError> {
        SecretKey::from_bytes(&self.to_be_bytes()).map_err(|_| ThresholdError::ZeroSecret)
    }

    fn from_secret_key(key: &SecretKey) -> Self {
        let scalar: &blst_scalar = key.into();
        Self::from_scalar(scalar)
    }
}

/// Uniform non-zero field element. `key_gen` reduces HKDF output modulo the field order and
/// never returns zero.
fn random_fr<R: CryptoRng + RngCore>(rng: &mut R) -> Fr {
    let mut ikm = [0u8; 32];
    rng.fill_bytes(&mut ikm);
    let key = SecretKey::key_gen(&ikm, &[]).expect("32 bytes of ikm is enough");
    Fr::from_secret_key(&key)
}

/// Index at which a share is evaluated. Never zero: the polynomial's value at zero is the secret.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShareIndex(NonZeroU32);

impl ShareIndex {
    pub fn new(index: u32) -> Result<Self, ThresholdError> {
        NonZeroU32::new(index).map(Self).ok_or(ThresholdError::ZeroIndex)
    }

    /// Index of the validator at `position` in the committee's bitmap order, which starts at 0.
    pub fn from_position(position: u32) -> Self {
        Self(NonZeroU32::new(position + 1).expect("position + 1 is never zero"))
    }

    pub fn get(&self) -> u32 {
        self.0.get()
    }

    fn as_fr(&self) -> Fr {
        Fr::from_u64(u64::from(self.0.get()))
    }
}

/// A dealer's secret polynomial of degree `threshold - 1`. Its constant term is the dealer's
/// contribution to the committee secret, and its evaluations are the shares it deals.
pub struct SecretPolynomial {
    coefficients: Vec<Fr>,
}

impl std::fmt::Debug for SecretPolynomial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SecretPolynomial(threshold={})", self.threshold())
    }
}

impl SecretPolynomial {
    pub fn random<R: CryptoRng + RngCore>(
        threshold: usize,
        rng: &mut R,
    ) -> Result<Self, ThresholdError> {
        if threshold == 0 {
            return Err(ThresholdError::ZeroThreshold);
        }
        let coefficients = (0..threshold).map(|_| random_fr(rng)).collect();
        Ok(Self { coefficients })
    }

    /// Number of shares needed to reconstruct a signature.
    pub fn threshold(&self) -> usize {
        self.coefficients.len()
    }

    /// The share dealt to `index`.
    pub fn share(&self, index: ShareIndex) -> Result<SecretShare, ThresholdError> {
        SecretShare::from_fr(index, &self.evaluate(&index.as_fr()))
    }

    /// Feldman commitments to the coefficients.
    pub fn commit(&self) -> Result<PublicPolynomial, ThresholdError> {
        let coefficients = self
            .coefficients
            .iter()
            .map(|a| a.to_secret_key().map(|key| key.sk_to_pk().into()))
            .collect::<Result<Vec<BlsPublicKey>, _>>()?;
        Ok(PublicPolynomial { coefficients })
    }

    /// Horner evaluation at `x`.
    fn evaluate(&self, x: &Fr) -> Fr {
        let mut acc = Fr::from_u64(0);
        for coefficient in self.coefficients.iter().rev() {
            acc = acc.mul(x).add(coefficient);
        }
        acc
    }

    /// Schnorr proof of knowledge of the constant term, bound to `context`. A dealer attaches one
    /// to its deal so that it cannot choose its commitment as a function of other dealers'
    /// commitments (the rogue-key attack on key aggregation).
    pub fn prove_constant_term<R: CryptoRng + RngCore>(
        &self,
        context: &[u8],
        rng: &mut R,
    ) -> Result<ConstantTermProof, ThresholdError> {
        let nonce = random_fr(rng);
        let commitment: BlsPublicKey = nonce.to_secret_key()?.sk_to_pk().into();
        let constant_term: BlsPublicKey = self.coefficients[0].to_secret_key()?.sk_to_pk().into();
        let challenge =
            challenge(context, &constant_term, &commitment).ok_or(ThresholdError::ZeroChallenge)?;
        let response = nonce.add(&challenge.mul(&self.coefficients[0])).to_be_bytes();
        Ok(ConstantTermProof { commitment, response })
    }

    /// The constant term as a signing key: the full secret, which no party holds after a DKG.
    #[cfg(test)]
    fn constant_term(&self) -> SecretKey {
        self.coefficients[0].to_secret_key().expect("random coefficient is non-zero")
    }
}

/// Proof that the prover knows the discrete logarithm of a public polynomial's constant term.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConstantTermProof {
    commitment: BlsPublicKey,
    response: [u8; SCALAR_BYTES],
}

/// Fiat-Shamir challenge over the context, the constant term and the proof commitment.
fn challenge(
    context: &[u8],
    constant_term: &BlsPublicKey,
    commitment: &BlsPublicKey,
) -> Option<Fr> {
    let mut message = Vec::with_capacity(context.len() + 2 * 96);
    message.extend_from_slice(context);
    message.extend_from_slice(constant_term.as_ref());
    message.extend_from_slice(commitment.as_ref());
    // `None` only when the hash reduces to zero, which a prover retries and a verifier rejects.
    blst_scalar::hash_to(&message, PROOF_DST).map(|scalar| Fr::from_scalar(&scalar))
}

/// Feldman commitments `A_j = g2^(a_j)` to a polynomial's coefficients. `A_0` is the public key
/// of the constant term and `sum_j A_j^(x^j)` is the public key of the share at index `x`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicPolynomial {
    coefficients: Vec<BlsPublicKey>,
}

impl PublicPolynomial {
    pub fn new(coefficients: Vec<BlsPublicKey>) -> Result<Self, ThresholdError> {
        if coefficients.is_empty() {
            return Err(ThresholdError::ZeroThreshold);
        }
        Ok(Self { coefficients })
    }

    pub fn threshold(&self) -> usize {
        self.coefficients.len()
    }

    pub fn coefficients(&self) -> &[BlsPublicKey] {
        &self.coefficients
    }

    /// Public key of the committee secret, the key a combined signature verifies against.
    pub fn group_key(&self) -> BlsPublicKey {
        self.coefficients[0]
    }

    /// Public key of the share at `index`, the key its partial signatures verify against.
    pub fn share_key(&self, index: ShareIndex) -> BlsPublicKey {
        let keys: Vec<PublicKey> = self.coefficients.iter().map(|key| *key.deref()).collect();
        let x = index.as_fr();
        let mut power = Fr::from_u64(1);
        let mut scalars = Vec::with_capacity(SCALAR_BYTES * keys.len());
        for _ in &keys {
            scalars.extend_from_slice(&power.to_scalar().b);
            power = power.mul(&x);
        }
        AggregatePublicKey::aggregate_with_randomness(&keys, &scalars, SCALAR_BITS, false)
            .expect("at least one coefficient")
            .to_public_key()
            .into()
    }

    /// Check a proof of knowledge of the constant term made with `prove_constant_term` under the
    /// same `context`: `g2^response == commitment * A_0^challenge`.
    pub fn verify_constant_term_proof(&self, context: &[u8], proof: &ConstantTermProof) -> bool {
        let Ok(response) = SecretKey::from_bytes(&proof.response) else {
            return false;
        };
        let lhs: BlsPublicKey = response.sk_to_pk().into();
        let Some(challenge) = challenge(context, &self.group_key(), &proof.commitment) else {
            return false;
        };
        let Ok(mut rhs) = AggregatePublicKey::aggregate_with_randomness(
            &[*self.group_key().deref()],
            &challenge.to_scalar().b,
            SCALAR_BITS,
            false,
        ) else {
            return false;
        };
        if rhs.add_public_key(proof.commitment.deref(), false).is_err() {
            return false;
        }
        lhs == rhs.to_public_key().into()
    }

    /// Coefficient-wise sum: the public polynomial of the sum of two secret polynomials.
    pub fn add(&self, other: &Self) -> Result<Self, ThresholdError> {
        if self.threshold() != other.threshold() {
            return Err(ThresholdError::ThresholdMismatch(self.threshold(), other.threshold()));
        }
        let coefficients = self
            .coefficients
            .iter()
            .zip(&other.coefficients)
            .map(|(a, b)| {
                AggregatePublicKey::aggregate(&[a.deref(), b.deref()], false)
                    .expect("two keys")
                    .to_public_key()
                    .into()
            })
            .collect();
        Ok(Self { coefficients })
    }
}

/// One validator's share of the committee secret. It signs like a normal key; the signatures it
/// produces are partial and must be combined with `threshold - 1` others.
pub struct SecretShare {
    index: ShareIndex,
    key: SecretKey,
}

impl std::fmt::Debug for SecretShare {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SecretShare(index={})", self.index.get())
    }
}

impl SecretShare {
    fn from_fr(index: ShareIndex, value: &Fr) -> Result<Self, ThresholdError> {
        Ok(Self { index, key: value.to_secret_key()? })
    }

    pub fn from_bytes(index: ShareIndex, bytes: &[u8]) -> Result<Self, ThresholdError> {
        let key = SecretKey::from_bytes(bytes).map_err(|_| ThresholdError::ZeroSecret)?;
        Ok(Self { index, key })
    }

    pub fn to_bytes(&self) -> [u8; SCALAR_BYTES] {
        self.key.to_bytes()
    }

    pub fn index(&self) -> ShareIndex {
        self.index
    }

    pub fn public_key(&self) -> BlsPublicKey {
        self.key.sk_to_pk().into()
    }

    /// Sum with another dealer's share for the same index. After a DKG a validator's share is
    /// the sum of the shares every qualified dealer sent it.
    pub fn add(&self, other: &Self) -> Result<Self, ThresholdError> {
        if self.index != other.index {
            return Err(ThresholdError::IndexMismatch(self.index.get(), other.index.get()));
        }
        let sum = Fr::from_secret_key(&self.key).add(&Fr::from_secret_key(&other.key));
        Self::from_fr(self.index, &sum)
    }

    pub fn partial_sign(&self, msg: &[u8]) -> PartialSignature {
        PartialSignature { index: self.index, signature: self.sign(msg) }
    }
}

impl Signer for SecretShare {
    fn sign(&self, msg: &[u8]) -> BlsSignature {
        self.key.sign(msg, DST_G1, &[]).into()
    }
}

/// A share's signature over a message, tagged with the share index the combiner needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartialSignature {
    pub index: ShareIndex,
    pub signature: BlsSignature,
}

impl PartialSignature {
    /// Verify against the share's public key derived from the committee polynomial.
    pub fn verify(&self, polynomial: &PublicPolynomial, msg: &[u8]) -> bool {
        self.signature.verify_raw(msg, &polynomial.share_key(self.index))
    }
}

/// Combine partial signatures into the committee signature. The first `threshold` partials with
/// distinct indices are interpolated and any others are ignored; the result is the same for
/// every choice of `threshold` valid partials. Partials are not verified here: the caller
/// checks each one with `PartialSignature::verify`, as the author checks each vote today.
pub fn combine(
    threshold: usize,
    partials: &[PartialSignature],
) -> Result<BlsSignature, ThresholdError> {
    if threshold == 0 {
        return Err(ThresholdError::ZeroThreshold);
    }
    let mut chosen: Vec<&PartialSignature> = Vec::with_capacity(threshold);
    for partial in partials {
        if chosen.iter().all(|c| c.index != partial.index) {
            chosen.push(partial);
            if chosen.len() == threshold {
                break;
            }
        }
    }
    if chosen.len() < threshold {
        return Err(ThresholdError::NotEnoughShares { needed: threshold, got: chosen.len() });
    }
    let indices: Vec<ShareIndex> = chosen.iter().map(|p| p.index).collect();
    let mut scalars = Vec::with_capacity(SCALAR_BYTES * threshold);
    for coefficient in lagrange_at_zero(&indices) {
        scalars.extend_from_slice(&coefficient.to_scalar().b);
    }
    let signatures: Vec<Signature> = chosen.iter().map(|p| *p.signature.deref()).collect();
    let combined =
        AggregateSignature::aggregate_with_randomness(&signatures, &scalars, SCALAR_BITS, true)
            .map_err(|_| ThresholdError::BadSignature)?;
    Ok(combined.to_signature().into())
}

/// Lagrange coefficients for interpolating at zero over distinct non-zero indices:
/// `l_i = prod_{j != i} x_j / (x_j - x_i)`.
fn lagrange_at_zero(indices: &[ShareIndex]) -> Vec<Fr> {
    let xs: Vec<Fr> = indices.iter().map(ShareIndex::as_fr).collect();
    xs.iter()
        .enumerate()
        .map(|(i, xi)| {
            let mut numerator = Fr::from_u64(1);
            let mut denominator = Fr::from_u64(1);
            for (j, xj) in xs.iter().enumerate() {
                if i != j {
                    numerator = numerator.mul(xj);
                    denominator = denominator.mul(&xj.sub(xi));
                }
            }
            numerator.mul(&denominator.inverse().expect("indices are distinct and non-zero"))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        crypto::{to_intent_message, Digest, ProtocolSignature},
        encode,
    };

    struct Committee {
        polynomial: SecretPolynomial,
        public: PublicPolynomial,
        shares: Vec<SecretShare>,
    }

    fn committee(size: u32, threshold: usize) -> Committee {
        let mut rng = rand::rng();
        let polynomial = SecretPolynomial::random(threshold, &mut rng).unwrap();
        let public = polynomial.commit().unwrap();
        let shares = (0..size)
            .map(|position| polynomial.share(ShareIndex::from_position(position)).unwrap())
            .collect();
        Committee { polynomial, public, shares }
    }

    fn sign_all(shares: &[SecretShare], msg: &[u8]) -> Vec<PartialSignature> {
        shares.iter().map(|share| share.partial_sign(msg)).collect()
    }

    /// Every subset of `k` out of `n` positions, as index lists.
    fn subsets(n: usize, k: usize) -> Vec<Vec<usize>> {
        (0u32..1 << n)
            .filter(|mask| mask.count_ones() as usize == k)
            .map(|mask| (0..n).filter(|i| mask & (1 << i) != 0).collect())
            .collect()
    }

    #[test]
    fn every_threshold_subset_combines_to_the_same_signature() {
        let c = committee(7, 5);
        let msg = b"header digest";
        let partials = sign_all(&c.shares, msg);
        let direct: BlsSignature = c.polynomial.constant_term().sign(msg, DST_G1, &[]).into();

        for subset in subsets(7, 5) {
            let chosen: Vec<PartialSignature> = subset.iter().map(|i| partials[*i]).collect();
            let combined = combine(5, &chosen).unwrap();
            assert_eq!(combined.to_bytes(), direct.to_bytes(), "subset {subset:?}");
            assert!(combined.verify_raw(msg, &c.public.group_key()));
        }
        // More than `threshold` partials give the same result, in any order.
        let mut reversed = partials.clone();
        reversed.reverse();
        assert_eq!(combine(5, &reversed).unwrap().to_bytes(), direct.to_bytes());
    }

    #[test]
    fn fewer_than_threshold_distinct_shares_cannot_combine() {
        let c = committee(4, 3);
        let msg = b"header digest";
        let partials = sign_all(&c.shares, msg);
        assert_eq!(
            combine(3, &partials[..2]),
            Err(ThresholdError::NotEnoughShares { needed: 3, got: 2 })
        );
        // The same share repeated does not count twice.
        let duplicated = [partials[0], partials[0], partials[1]];
        assert_eq!(
            combine(3, &duplicated),
            Err(ThresholdError::NotEnoughShares { needed: 3, got: 2 })
        );
        assert_eq!(combine(0, &partials), Err(ThresholdError::ZeroThreshold));
    }

    #[test]
    fn partials_verify_only_against_their_own_share_key() {
        let c = committee(4, 3);
        let msg = b"header digest";
        for share in &c.shares {
            let partial = share.partial_sign(msg);
            assert!(partial.verify(&c.public, msg));
            assert_eq!(c.public.share_key(share.index()), share.public_key());
            assert!(!partial.verify(&c.public, b"another message"));
            let misattributed =
                PartialSignature { index: ShareIndex::from_position(99), ..partial };
            assert!(!misattributed.verify(&c.public, msg));
        }
    }

    #[test]
    fn no_single_share_signs_for_the_group_and_index_zero_is_rejected() {
        let c = committee(4, 3);
        let msg = b"header digest";
        for share in &c.shares {
            assert!(!share.sign(msg).verify_raw(msg, &c.public.group_key()));
        }
        assert_eq!(ShareIndex::new(0), Err(ThresholdError::ZeroIndex));
        assert_eq!(ShareIndex::from_position(0).get(), 1);
    }

    #[test]
    fn summed_dealings_behave_like_one_polynomial() {
        let mut rng = rand::rng();
        let dealers: Vec<SecretPolynomial> =
            (0..3).map(|_| SecretPolynomial::random(3, &mut rng).unwrap()).collect();
        let public = dealers[1..]
            .iter()
            .try_fold(dealers[0].commit().unwrap(), |acc, d| acc.add(&d.commit().unwrap()))
            .unwrap();
        let shares: Vec<SecretShare> = (0..4)
            .map(|position| {
                let index = ShareIndex::from_position(position);
                dealers[1..]
                    .iter()
                    .try_fold(dealers[0].share(index).unwrap(), |acc, d| {
                        acc.add(&d.share(index).unwrap())
                    })
                    .unwrap()
            })
            .collect();
        let msg = b"header digest";
        let partials = sign_all(&shares, msg);
        for partial in &partials {
            assert!(partial.verify(&public, msg));
        }
        let combined = combine(3, &partials[1..]).unwrap();
        assert!(combined.verify_raw(msg, &public.group_key()));
        assert_eq!(combined.to_bytes(), combine(3, &partials[..3]).unwrap().to_bytes());

        let mismatched = SecretPolynomial::random(2, &mut rng).unwrap();
        assert_eq!(
            public.add(&mismatched.commit().unwrap()),
            Err(ThresholdError::ThresholdMismatch(3, 2))
        );
        assert_eq!(shares[0].add(&shares[1]), Err(ThresholdError::IndexMismatch(1, 2)));
    }

    #[test]
    fn combined_signature_passes_the_existing_intent_verification() {
        let c = committee(4, 3);
        let digest = Digest::<32> { digest: [7u8; 32] };
        let intent = to_intent_message(digest);
        let msg = encode(&intent);
        let partials = sign_all(&c.shares, &msg);
        let combined = combine(3, &partials).unwrap();
        assert!(combined.verify_secure(&intent, &c.public.group_key()));
        assert!(!combined.verify_secure(&intent, &c.shares[0].public_key()));
    }

    #[test]
    fn every_threshold_from_one_to_six_combines_to_the_direct_signature() {
        // Odd and even thresholds: a sign error in the Lagrange coefficients only shows on half
        // of them.
        let msg = b"header digest";
        for t in 1..=6usize {
            let c = committee(7, t);
            let direct: BlsSignature = c.polynomial.constant_term().sign(msg, DST_G1, &[]).into();
            let partials = sign_all(&c.shares, msg);
            for subset in subsets(7, t) {
                let chosen: Vec<PartialSignature> = subset.iter().map(|i| partials[*i]).collect();
                let combined = combine(t, &chosen).unwrap();
                assert_eq!(combined.to_bytes(), direct.to_bytes(), "t={t} subset {subset:?}");
            }
        }
    }

    #[test]
    fn a_proof_whose_challenge_ignores_its_commitment_does_not_verify() {
        // Forged without the secret: pick the response, then solve for the commitment under a
        // challenge computed before the commitment exists. Rejected only because the verifier's
        // challenge hashes the commitment.
        let mut rng = rand::rng();
        let c = committee(4, 3);
        let context = b"epoch 7, dealer 2";
        let response = random_fr(&mut rng);
        let early = challenge(context, &c.public.group_key(), &c.public.group_key()).unwrap();
        let negated = Fr::from_u64(0).sub(&early);
        let mut commitment = AggregatePublicKey::aggregate_with_randomness(
            &[*c.public.group_key().deref()],
            &negated.to_scalar().b,
            SCALAR_BITS,
            false,
        )
        .unwrap();
        commitment.add_public_key(&response.to_secret_key().unwrap().sk_to_pk(), false).unwrap();
        let forged = ConstantTermProof {
            commitment: commitment.to_public_key().into(),
            response: response.to_be_bytes(),
        };
        assert!(!c.public.verify_constant_term_proof(context, &forged));
    }

    #[test]
    fn constant_term_proofs_bind_the_polynomial_and_the_context() {
        let mut rng = rand::rng();
        let c = committee(4, 3);
        let proof = c.polynomial.prove_constant_term(b"epoch 7, dealer 2", &mut rng).unwrap();
        assert!(c.public.verify_constant_term_proof(b"epoch 7, dealer 2", &proof));
        assert!(!c.public.verify_constant_term_proof(b"epoch 7, dealer 3", &proof));
        let other = SecretPolynomial::random(3, &mut rng).unwrap().commit().unwrap();
        assert!(!other.verify_constant_term_proof(b"epoch 7, dealer 2", &proof));
        let mut tampered = proof.clone();
        tampered.response[31] ^= 1;
        assert!(!c.public.verify_constant_term_proof(b"epoch 7, dealer 2", &tampered));
        let zero = ConstantTermProof { commitment: proof.commitment, response: [0u8; 32] };
        assert!(!c.public.verify_constant_term_proof(b"epoch 7, dealer 2", &zero));
    }

    #[test]
    fn shares_round_trip_through_bytes() {
        let c = committee(4, 3);
        let msg = b"header digest";
        for share in &c.shares {
            let restored = SecretShare::from_bytes(share.index(), &share.to_bytes()).unwrap();
            assert_eq!(restored.partial_sign(msg), share.partial_sign(msg));
        }
        assert_eq!(
            SecretShare::from_bytes(ShareIndex::from_position(0), &[0u8; 32]),
            Err(ThresholdError::ZeroSecret)
        );
    }

    impl PartialEq for SecretShare {
        fn eq(&self, other: &Self) -> bool {
            self.index == other.index && self.to_bytes() == other.to_bytes()
        }
    }
}
