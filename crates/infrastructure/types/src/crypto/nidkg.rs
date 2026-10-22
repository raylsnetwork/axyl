//! Publicly verifiable dealing for the per-epoch DKG: Groth's non-interactive DKG (ePrint
//! 2021/339, sections 6.4, 6.5 and 7), without its forward-secure encryption layer.
//!
//! A dealer shares a random secret with a Feldman-committed polynomial and encrypts each
//! recipient's share to that recipient's encryption key, in 16 chunks of 16 bits with ElGamal in
//! G1. Two proofs make the dealing checkable by anyone, with no recipient involved:
//!
//! - correct sharing: the encrypted shares are the evaluations of the committed polynomial;
//! - correct chunking: every encrypted chunk is small enough for its recipient to decrypt.
//!
//! A recipient that was offline, or that joins later, decrypts its share from the dealing
//! whenever it reads it. There are no complaints and no acks.
//!
//! Dropping forward secrecy removes the paper's `R_j, S_j, Z_j` tree-encryption terms and its
//! pairing checks: a ciphertext is `(R_j, C_ij) = (g1^r_j, y_i^r_j * g1^s_ij)` and decryption is
//! `C_ij / R_j^x_i = g1^s_ij` followed by a small discrete-log search.
//!
//! Every Fiat-Shamir transcript starts with the caller's `context` (chain, epoch, dealer), and a
//! dealing carries a Schnorr proof of knowledge of its constant term under the same context, so
//! a dealing cannot be replayed as another dealer's or reused in another epoch.

use super::{
    threshold::{
        g2_generator_key, in_subgroup, random_fr, ConstantTermProof, Fr, PublicPolynomial,
        SecretPolynomial, SecretShare, ShareIndex, ThresholdError, SCALAR_BITS, SCALAR_BYTES,
    },
    BlsPublicKey, BlsSignature, Intent, IntentMessage, IntentScope,
};
use crate::{encode, Address};
use blst::{
    blst_p1, blst_p1_add_or_double, blst_p1_affine, blst_p1_affine_compress, blst_p1_affine_in_g1,
    blst_p1_cneg, blst_p1_compress, blst_p1_from_affine, blst_p1_generator, blst_p1_is_equal,
    blst_p1_is_inf, blst_p1_mult, blst_p1_uncompress, blst_p2, blst_p2_add_or_double,
    blst_p2_affine, blst_p2_from_affine, blst_p2_generator, blst_p2_is_equal, blst_p2_mult,
    blst_p2_to_affine, min_sig::PublicKey, p1_affines, p2_affines, BLST_ERROR,
};
use rand::{CryptoRng, Rng, RngCore};
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::{collections::HashMap, fmt, ops::Deref};
use thiserror::Error;

/// Bits per chunk.
pub const CHUNK_BITS: u32 = 16;
/// Chunk range `B`: an honest chunk is in `[0, B)`.
pub const CHUNK_SIZE: u64 = 1 << CHUNK_BITS;
/// Chunks per share `m`: `B^m >= 2^256` covers every field element.
pub const NUM_CHUNKS: usize = 16;
/// Parallel repetitions `l` of the chunking proof, one per bit of soundness (`lambda = 128`).
pub const NUM_REPETITIONS: usize = 128;
/// Bits per chunking challenge, `ceil(lambda / l)`. With one bit, `E = 2` and the proof's
/// guarantee is that every chunk is in `(-Z, Z)` exactly, with no multiplier (`Delta = 1`).
pub const CHALLENGE_BITS: u32 = 1;
/// Chunking challenge range `E`: challenges are in `[0, E)`.
pub const CHALLENGE_RANGE: u64 = 1 << CHALLENGE_BITS;
/// Prover restarts allowed by the chunking proof's rejection sampling before it gives up.
const MAX_CHUNKING_ATTEMPTS: usize = 256;
/// Size of a compressed G1 point.
const G1_BYTES: usize = 48;
/// Giant steps normalised together in the discrete-log search.
const GIANT_BLOCK: u64 = 512;

const DST_KEY_DERIVE: &[u8] = b"RAYLS_NIDKG_ENC_KEY_DERIVE_V1";
const DST_KEY_POK: &[u8] = b"RAYLS_NIDKG_ENC_KEY_POK_V1";
const DST_SHARING_1: &[u8] = b"RAYLS_NIDKG_SHARING_INSTANCE_V1";
const DST_SHARING_2: &[u8] = b"RAYLS_NIDKG_SHARING_CHALLENGE_V1";
const DST_CHUNKING_E: &str = "RAYLS_NIDKG_CHUNKING_CHALLENGES_V1";
const DST_CHUNKING_X: &[u8] = b"RAYLS_NIDKG_CHUNKING_CHALLENGE_V1";
const DST_CONSTANT_TERM: &[u8] = b"RAYLS_NIDKG_CONSTANT_TERM_V1";
const DST_BATCH: &[u8] = b"RAYLS_NIDKG_BATCH_WEIGHT_V1";

#[derive(Debug, Error, PartialEq, Eq)]
pub enum NidkgError {
    #[error(transparent)]
    Threshold(#[from] ThresholdError),
    #[error("a dealing needs at least one recipient and a threshold of at most the recipients")]
    BadShape,
    #[error("two recipients share one encryption key")]
    DuplicateRecipient,
    #[error("recipient positions are not strictly increasing")]
    UnorderedRecipients,
    #[error("the dealing was made for other recipient positions")]
    WrongRecipients,
    #[error("a G2 point of the dealing is the identity or outside the prime-order subgroup")]
    PointNotInGroup,
    #[error("dealing has {got} ciphertext rows or chunks, expected {expected}")]
    WrongSize { expected: usize, got: usize },
    #[error("the sharing or chunking proof does not verify")]
    BadProof,
    #[error("the constant-term proof does not verify")]
    BadConstantTermProof,
    #[error("a hash reduced to zero")]
    ZeroChallenge,
    #[error("the chunking proof's rejection sampling failed {MAX_CHUNKING_ATTEMPTS} times")]
    ChunkingAborted,
    #[error("recipient position {0} is not in the dealing")]
    UnknownRecipient(u32),
    #[error("chunk {0} is outside the range the chunking proof guarantees")]
    Undecryptable(usize),
    #[error("the decrypted share does not match the committed polynomial")]
    ShareMismatch,
}

// ---------------------------------------------------------------------------------------------
// Groups

/// Point of G1 in projective form.
#[derive(Clone, Copy)]
pub struct G1(blst_p1);

impl G1 {
    fn generator() -> Self {
        // SAFETY: blst returns a pointer to its static generator.
        G1(unsafe { *blst_p1_generator() })
    }

    fn identity() -> Self {
        // All-zero projective coordinates are the point at infinity.
        G1(blst_p1::default())
    }

    fn is_identity(&self) -> bool {
        // SAFETY: `self.0` is a live point.
        unsafe { blst_p1_is_inf(&self.0) }
    }

    fn add(&self, other: &G1) -> G1 {
        let mut out = blst_p1::default();
        // SAFETY: all pointers reference live points.
        unsafe { blst_p1_add_or_double(&mut out, &self.0, &other.0) };
        G1(out)
    }

    fn neg(&self) -> G1 {
        let mut out = self.0;
        // SAFETY: `out` is a live point.
        unsafe { blst_p1_cneg(&mut out, true) };
        G1(out)
    }

    fn mul(&self, scalar: &Fr) -> G1 {
        let scalar = scalar.to_scalar();
        let mut out = blst_p1::default();
        // SAFETY: `scalar.b` holds `SCALAR_BITS` little-endian bits; pointers are live.
        unsafe { blst_p1_mult(&mut out, &self.0, scalar.b.as_ptr(), SCALAR_BITS) };
        G1(out)
    }

    #[cfg(test)]
    fn mul_u64(&self, value: u64) -> G1 {
        let bytes = value.to_le_bytes();
        let mut out = blst_p1::default();
        // SAFETY: `bytes` holds 64 little-endian bits; pointers are live.
        unsafe { blst_p1_mult(&mut out, &self.0, bytes.as_ptr(), 64) };
        G1(out)
    }

    /// `sum_i points[i]^scalars[i]`. Identity points and zero scalars are skipped.
    fn msm(points: &[G1], scalars: &[Fr]) -> G1 {
        debug_assert_eq!(points.len(), scalars.len());
        let mut kept = Vec::with_capacity(points.len());
        let mut bytes = Vec::with_capacity(points.len() * SCALAR_BYTES);
        for (point, scalar) in points.iter().zip(scalars) {
            if point.is_identity() || scalar.is_zero() {
                continue;
            }
            kept.push(point.0);
            bytes.extend_from_slice(&scalar.to_scalar().b);
        }
        if kept.is_empty() {
            return G1::identity();
        }
        G1(p1_affines::from(&kept).mult(&bytes, SCALAR_BITS))
    }

    pub fn to_bytes(&self) -> [u8; G1_BYTES] {
        let mut out = [0u8; G1_BYTES];
        // SAFETY: `out` is 48 bytes, the size blst writes.
        unsafe { blst_p1_compress(out.as_mut_ptr(), &self.0) };
        out
    }

    /// Decode a compressed point, rejecting the identity and points outside the prime-order
    /// subgroup.
    pub fn from_bytes(bytes: &[u8]) -> Option<G1> {
        if bytes.len() != G1_BYTES {
            return None;
        }
        let mut affine = blst_p1_affine::default();
        // SAFETY: `bytes` is 48 bytes; `affine` is a valid destination.
        if unsafe { blst_p1_uncompress(&mut affine, bytes.as_ptr()) } != BLST_ERROR::BLST_SUCCESS {
            return None;
        }
        // SAFETY: `affine` is a live, decoded point.
        if !unsafe { blst_p1_affine_in_g1(&affine) } {
            return None;
        }
        let mut point = blst_p1::default();
        // SAFETY: both pointers reference live points.
        unsafe { blst_p1_from_affine(&mut point, &affine) };
        let point = G1(point);
        (!point.is_identity()).then_some(point)
    }
}

impl PartialEq for G1 {
    fn eq(&self, other: &Self) -> bool {
        // SAFETY: both pointers reference live points.
        unsafe { blst_p1_is_equal(&self.0, &other.0) }
    }
}

impl Eq for G1 {}

impl fmt::Debug for G1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "G1({})", hex::encode(&self.to_bytes()[..8]))
    }
}

impl Serialize for G1 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.to_bytes())
    }
}

impl<'de> Deserialize<'de> for G1 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes = <Vec<u8>>::deserialize(deserializer)?;
        G1::from_bytes(&bytes).ok_or_else(|| de::Error::custom("invalid G1 point"))
    }
}

/// Point of G2 in projective form, the group of the Feldman commitments.
#[derive(Clone, Copy)]
struct G2(blst_p2);

impl G2 {
    fn generator() -> Self {
        // SAFETY: blst returns a pointer to its static generator.
        G2(unsafe { *blst_p2_generator() })
    }

    fn from_key(key: &BlsPublicKey) -> Self {
        let affine: &blst_p2_affine = key.deref().into();
        let mut out = blst_p2::default();
        // SAFETY: both pointers reference live points.
        unsafe { blst_p2_from_affine(&mut out, affine) };
        G2(out)
    }

    fn to_key(self) -> BlsPublicKey {
        let mut affine = blst_p2_affine::default();
        // SAFETY: both pointers reference live points.
        unsafe { blst_p2_to_affine(&mut affine, &self.0) };
        PublicKey::from(affine).into()
    }

    fn add(&self, other: &G2) -> G2 {
        let mut out = blst_p2::default();
        // SAFETY: all pointers reference live points.
        unsafe { blst_p2_add_or_double(&mut out, &self.0, &other.0) };
        G2(out)
    }

    fn mul(&self, scalar: &Fr) -> G2 {
        let scalar = scalar.to_scalar();
        let mut out = blst_p2::default();
        // SAFETY: `scalar.b` holds `SCALAR_BITS` little-endian bits; pointers are live.
        unsafe { blst_p2_mult(&mut out, &self.0, scalar.b.as_ptr(), SCALAR_BITS) };
        G2(out)
    }

    fn msm(points: &[G2], scalars: &[Fr]) -> G2 {
        debug_assert_eq!(points.len(), scalars.len());
        let raw: Vec<blst_p2> = points.iter().map(|p| p.0).collect();
        let mut bytes = Vec::with_capacity(points.len() * SCALAR_BYTES);
        for scalar in scalars {
            bytes.extend_from_slice(&scalar.to_scalar().b);
        }
        G2(p2_affines::from(&raw).mult(&bytes, SCALAR_BITS))
    }
}

impl PartialEq for G2 {
    fn eq(&self, other: &Self) -> bool {
        // SAFETY: both pointers reference live points.
        unsafe { blst_p2_is_equal(&self.0, &other.0) }
    }
}

/// Field element on the wire: canonical big-endian bytes.
#[derive(Clone)]
struct Scalar(Fr);

impl Serialize for Scalar {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0.to_be_bytes())
    }
}

impl<'de> Deserialize<'de> for Scalar {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes = <Vec<u8>>::deserialize(deserializer)?;
        let bytes: [u8; SCALAR_BYTES] =
            bytes.try_into().map_err(|_| de::Error::custom("scalar must be 32 bytes"))?;
        Fr::from_be_bytes(&bytes)
            .map(Scalar)
            .ok_or_else(|| de::Error::custom("scalar not canonical"))
    }
}

impl fmt::Debug for Scalar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Scalar({})", hex::encode(&self.0.to_be_bytes()[..8]))
    }
}

impl PartialEq for Scalar {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_be_bytes() == other.0.to_be_bytes()
    }
}

impl Eq for Scalar {}

/// Signed integer as a field element.
fn fr_from_i128(value: i128) -> Fr {
    let magnitude = Fr::from_u128(value.unsigned_abs());
    if value < 0 {
        magnitude.neg()
    } else {
        magnitude
    }
}

impl Fr {
    fn from_u128(value: u128) -> Fr {
        let high = Fr::from_u64((value >> 64) as u64);
        let low = Fr::from_u64(value as u64);
        high.mul(&Fr::from_u64(1 << 32).mul(&Fr::from_u64(1 << 32))).add(&low)
    }
}

/// `[x^1, ..., x^count]`.
fn powers(x: &Fr, count: usize) -> Vec<Fr> {
    let mut out = Vec::with_capacity(count);
    let mut acc = x.clone();
    for _ in 0..count {
        out.push(acc.clone());
        acc = acc.mul(x);
    }
    out
}

/// Fiat-Shamir transcript: domain-separated, length-prefixed parts.
struct Transcript(Vec<u8>);

impl Transcript {
    fn new(context: &[u8]) -> Self {
        let mut transcript = Transcript(Vec::new());
        transcript.bytes(context);
        transcript
    }

    fn bytes(&mut self, bytes: &[u8]) {
        self.0.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
        self.0.extend_from_slice(bytes);
    }

    fn g1(&mut self, point: &G1) {
        self.0.extend_from_slice(&point.to_bytes());
    }

    /// A list of points, prefixed with its length.
    fn g1s<'a, I>(&mut self, points: I)
    where
        I: IntoIterator<Item = &'a G1>,
        I::IntoIter: ExactSizeIterator,
    {
        let points = points.into_iter();
        self.0.extend_from_slice(&(points.len() as u64).to_le_bytes());
        for point in points {
            self.g1(point);
        }
    }

    fn key(&mut self, key: &BlsPublicKey) {
        self.0.extend_from_slice(key.as_ref());
    }

    fn fr(&mut self, value: &Fr) {
        self.0.extend_from_slice(&value.to_be_bytes());
    }

    fn challenge(&self, dst: &[u8]) -> Result<Fr, NidkgError> {
        Fr::hash_to(&self.0, dst).ok_or(NidkgError::ZeroChallenge)
    }

    fn digest(&self) -> [u8; 32] {
        *blake3::hash(&self.0).as_bytes()
    }
}

// ---------------------------------------------------------------------------------------------
// Contexts and signed messages
//
// The chain builds the same bytes from typed arguments, so neither a dealer nor a caller of the
// precompile chooses them. A validator authorises its dealing and its encryption key with its
// BLS consensus key over intent messages in the `DkgDealing` scope; the encryption key itself is
// derived from a signature in the `DkgShare` scope, which is never published. No consensus
// message uses either scope.

/// Context of a dealing for the key of `epoch`, by the validator whose execution address is
/// `dealer`, on `chain_id`.
pub fn dealing_context(chain_id: u64, epoch: u32, dealer: Address) -> Vec<u8> {
    let mut out = Vec::with_capacity(13 + 8 + 4 + 20);
    out.extend_from_slice(b"RAYLS_DEALING");
    out.extend_from_slice(&chain_id.to_be_bytes());
    out.extend_from_slice(&epoch.to_be_bytes());
    out.extend_from_slice(dealer.as_slice());
    out
}

/// Context of an encryption key's proof of knowledge. It names no chain: the key belongs to the
/// validator's BLS key, so the keytool can derive and prove it before a genesis exists. The
/// registration signature ([`key_registration_message`]) names the chain.
pub fn encryption_key_context() -> Vec<u8> {
    b"RAYLS_ENC_KEY_V2".to_vec()
}

/// What a validator signs, in the never-published `DkgShare` scope, to derive its decryption
/// key (see [`DecryptionKey::from_signature`]). Like [`encryption_key_context`], it names no
/// chain.
pub fn decryption_key_message() -> Vec<u8> {
    encode(&IntentMessage::new(Intent::consensus(IntentScope::DkgShare), "nidkg decryption key"))
}

/// What a validator signs to register `key` as its encryption key on `chain_id`.
pub fn key_registration_message(chain_id: u64, key: &EncryptionKey) -> Vec<u8> {
    encode(&IntentMessage::new(
        Intent::consensus(IntentScope::DkgDealing),
        ("nidkg encryption key", chain_id, key.to_bytes().to_vec()),
    ))
}

/// What a dealer signs to submit the dealing whose encoded bytes are `dealing`, for the key of
/// `epoch`, as the validator whose execution address is `dealer`.
pub fn dealing_message(chain_id: u64, epoch: u32, dealer: Address, dealing: &[u8]) -> Vec<u8> {
    encode(&IntentMessage::new(
        Intent::consensus(IntentScope::DkgDealing),
        ("nidkg dealing", chain_id, epoch, dealer, blake3::hash(dealing).as_bytes().to_vec()),
    ))
}

// ---------------------------------------------------------------------------------------------
// Encryption keys

/// A recipient's public encryption key `y = g1^x`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EncryptionKey(G1);

/// A recipient's decryption key `x`. Derived from the validator's BLS key, so there is no new
/// secret to back up.
pub struct DecryptionKey(Fr);

impl fmt::Debug for DecryptionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DecryptionKey(..)")
    }
}

/// Schnorr proof of knowledge of an encryption key's discrete logarithm, bound to the
/// validator's BLS key and a context. It goes on-chain with the key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EncryptionKeyProof {
    commitment: G1,
    response: Scalar,
}

impl DecryptionKey {
    /// Derive the key from the validator's BLS signature over [`decryption_key_message`]. The
    /// signature is deterministic, so the key is reproducible; it must never be published or sent
    /// anywhere, since it is the key.
    pub fn from_signature(signature: &BlsSignature) -> Result<Self, NidkgError> {
        Fr::hash_to(&signature.deref().compress(), DST_KEY_DERIVE)
            .map(Self)
            .ok_or(NidkgError::ZeroChallenge)
    }

    pub fn encryption_key(&self) -> EncryptionKey {
        EncryptionKey(G1::generator().mul(&self.0))
    }

    /// Proof of knowledge of this key, bound to `owner` and `context`.
    pub fn prove<R: CryptoRng + RngCore>(
        &self,
        owner: &BlsPublicKey,
        context: &[u8],
        rng: &mut R,
    ) -> Result<EncryptionKeyProof, NidkgError> {
        let nonce = random_fr(rng);
        let commitment = G1::generator().mul(&nonce);
        let challenge = key_challenge(owner, context, &self.encryption_key(), &commitment)?;
        let response = nonce.add(&challenge.mul(&self.0));
        Ok(EncryptionKeyProof { commitment, response: Scalar(response) })
    }
}

impl EncryptionKey {
    pub fn to_bytes(&self) -> [u8; G1_BYTES] {
        self.0.to_bytes()
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        G1::from_bytes(bytes).map(Self)
    }

    /// Check a proof from `DecryptionKey::prove` with the same owner and context:
    /// `g1^response == commitment * y^challenge`.
    pub fn verify(&self, owner: &BlsPublicKey, context: &[u8], proof: &EncryptionKeyProof) -> bool {
        let Ok(challenge) = key_challenge(owner, context, self, &proof.commitment) else {
            return false;
        };
        G1::generator().mul(&proof.response.0) == proof.commitment.add(&self.0.mul(&challenge))
    }
}

fn key_challenge(
    owner: &BlsPublicKey,
    context: &[u8],
    key: &EncryptionKey,
    commitment: &G1,
) -> Result<Fr, NidkgError> {
    let mut transcript = Transcript::new(context);
    transcript.key(owner);
    transcript.g1(&key.0);
    transcript.g1(commitment);
    transcript.challenge(DST_KEY_POK)
}

// ---------------------------------------------------------------------------------------------
// Dealing

/// One dealer's contribution to an epoch key: commitments, encrypted shares and the proofs that
/// they agree.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Dealing {
    /// Feldman commitments `A_0..A_{t-1}` in G2.
    public: PublicPolynomial,
    /// Proof of knowledge of `a_0`, so a dealer cannot cancel others' contributions.
    constant_term_proof: ConstantTermProof,
    /// Each recipient's position in the committee's key order, strictly increasing: row `i` of
    /// `ciphertexts` holds share index `positions[i] + 1`.
    positions: Vec<u32>,
    /// `R_j = g1^r_j`, one per chunk.
    randomizers: Vec<G1>,
    /// `C_ij = y_i^r_j * g1^s_ij`, one row of `NUM_CHUNKS` per recipient.
    ciphertexts: Vec<Vec<G1>>,
    sharing_proof: SharingProof,
    chunking_proof: ChunkingProof,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct SharingProof {
    f: G1,
    a: BlsPublicKey,
    y: G1,
    z_r: Scalar,
    z_a: Scalar,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ChunkingProof {
    y0: G1,
    bb: Vec<G1>,
    cc: Vec<G1>,
    dd: Vec<G1>,
    yy: G1,
    z_r: Vec<Scalar>,
    z_s: Vec<u64>,
    z_beta: Scalar,
}

/// Chunking proof parameters for `n` recipients: `S = n m (B-1)(E-1)` and `Z = 2 l S`.
fn chunking_bounds(recipients: usize) -> (i128, i128) {
    let s = recipients as i128
        * NUM_CHUNKS as i128
        * (CHUNK_SIZE as i128 - 1)
        * (CHALLENGE_RANGE as i128 - 1);
    (s, 2 * NUM_REPETITIONS as i128 * s)
}

/// The 16-bit chunks of a field element, least significant first.
fn chunks(value: &Fr) -> [i64; NUM_CHUNKS] {
    let le = value.to_scalar().b;
    let mut out = [0i64; NUM_CHUNKS];
    for (j, chunk) in out.iter_mut().enumerate() {
        *chunk = i64::from(le[2 * j]) | (i64::from(le[2 * j + 1]) << 8);
    }
    out
}

/// `B^j` for `j = 0..m`.
fn chunk_weights() -> Vec<Fr> {
    let base = Fr::from_u64(CHUNK_SIZE);
    let mut weights = Vec::with_capacity(NUM_CHUNKS);
    let mut acc = Fr::from_u64(1);
    for _ in 0..NUM_CHUNKS {
        weights.push(acc.clone());
        acc = acc.mul(&base);
    }
    weights
}

/// Chunking challenges `e_ijk` in `[0, E)`, one byte each, from an XOF over the first message.
fn chunking_challenges(first_message: &Transcript, recipients: usize) -> Vec<u8> {
    let mut hasher = blake3::Hasher::new_derive_key(DST_CHUNKING_E);
    hasher.update(&first_message.0);
    let mut out = vec![0u8; recipients * NUM_CHUNKS * NUM_REPETITIONS];
    hasher.finalize_xof().fill(&mut out);
    for e in &mut out {
        *e &= (1 << CHALLENGE_BITS) - 1;
    }
    out
}

/// Index of `e_ijk` in the challenge vector.
fn e_index(i: usize, j: usize, k: usize) -> usize {
    (i * NUM_CHUNKS + j) * NUM_REPETITIONS + k
}

fn check_recipients(threshold: usize, recipients: &[Recipient]) -> Result<(), NidkgError> {
    if recipients.is_empty() || threshold == 0 || threshold > recipients.len() {
        return Err(NidkgError::BadShape);
    }
    if recipients.windows(2).any(|pair| pair[0].position >= pair[1].position) {
        return Err(NidkgError::UnorderedRecipients);
    }
    let mut seen = std::collections::HashSet::with_capacity(recipients.len());
    if !recipients.iter().all(|recipient| seen.insert(recipient.key.to_bytes())) {
        return Err(NidkgError::DuplicateRecipient);
    }
    Ok(())
}

/// Share index of the recipient at `position`, as everywhere else: position plus one.
fn index_fr(position: u32) -> Fr {
    ShareIndex::from_position(position).as_fr()
}

/// One recipient of a dealing: its position in the committee's key order, whose share index is
/// `position + 1`, and its encryption key. A dealing goes to the members that had registered a
/// key in time, so positions can skip members.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Recipient {
    pub position: u32,
    pub key: EncryptionKey,
}

fn split_recipients(recipients: &[Recipient]) -> (Vec<EncryptionKey>, Vec<u32>) {
    recipients.iter().map(|recipient| (recipient.key, recipient.position)).unzip()
}

impl Dealing {
    /// Deal `polynomial` to `recipients`, in committee order. `context` must identify the chain,
    /// the epoch and the dealer.
    pub fn new<R: CryptoRng + RngCore>(
        context: &[u8],
        polynomial: &SecretPolynomial,
        recipients: &[Recipient],
        rng: &mut R,
    ) -> Result<Self, NidkgError> {
        check_recipients(polynomial.threshold(), recipients)?;
        let shares: Vec<Fr> = recipients
            .iter()
            .map(|recipient| polynomial.evaluate(&index_fr(recipient.position)))
            .collect();
        let share_chunks: Vec<[i64; NUM_CHUNKS]> = shares.iter().map(chunks).collect();
        Self::from_chunks(context, polynomial, recipients, &shares, &share_chunks, rng)
    }

    /// Deal with the given chunk decomposition of each share. Honest dealers use `chunks`; a test
    /// uses other decompositions to play a dishonest dealer.
    fn from_chunks<R: CryptoRng + RngCore>(
        context: &[u8],
        polynomial: &SecretPolynomial,
        recipients: &[Recipient],
        shares: &[Fr],
        share_chunks: &[[i64; NUM_CHUNKS]],
        rng: &mut R,
    ) -> Result<Self, NidkgError> {
        let (recipients, positions) = split_recipients(recipients);
        let recipients = &recipients[..];
        let public = polynomial.commit()?;
        let constant_term_proof =
            polynomial.prove_constant_term(&constant_term_context(context), rng)?;

        let g1 = G1::generator();
        let r: Vec<Fr> = (0..NUM_CHUNKS).map(|_| random_fr(rng)).collect();
        let randomizers: Vec<G1> = r.iter().map(|r_j| g1.mul(r_j)).collect();
        let ciphertexts: Vec<Vec<G1>> = recipients
            .iter()
            .zip(share_chunks)
            .map(|(key, chunks)| {
                r.iter()
                    .zip(chunks)
                    .map(|(r_j, s_ij)| {
                        key.0.mul(r_j).add(&g1.mul(&fr_from_i128(i128::from(*s_ij))))
                    })
                    .collect()
            })
            .collect();

        let weights = chunk_weights();
        let combined_r =
            r.iter().zip(&weights).fold(Fr::from_u64(0), |acc, (r_j, w)| acc.add(&r_j.mul(w)));
        let sharing_proof = prove_sharing(
            context,
            recipients,
            &positions,
            &public,
            &randomizers,
            &ciphertexts,
            &combined_r,
            shares,
            rng,
        )?;
        let chunking_proof =
            prove_chunking(context, recipients, &randomizers, &ciphertexts, &r, share_chunks, rng)?;

        Ok(Self {
            public,
            constant_term_proof,
            positions,
            randomizers,
            ciphertexts,
            sharing_proof,
            chunking_proof,
        })
    }

    /// The dealer's Feldman commitments; their constant term is its contribution to the key.
    pub fn public(&self) -> &PublicPolynomial {
        &self.public
    }

    /// Check the dealing against the recipients and threshold it must have been made for. Needs
    /// no secret and no recipient: any node, and the chain, can run it.
    pub fn verify(
        &self,
        context: &[u8],
        threshold: usize,
        recipients: &[Recipient],
    ) -> Result<(), NidkgError> {
        check_recipients(threshold, recipients)?;
        let (recipients, positions) = split_recipients(recipients);
        let recipients = &recipients[..];
        if self.positions != positions {
            return Err(NidkgError::WrongRecipients);
        }
        if self.public.threshold() != threshold {
            return Err(NidkgError::WrongSize {
                expected: threshold,
                got: self.public.threshold(),
            });
        }
        if self.randomizers.len() != NUM_CHUNKS {
            return Err(NidkgError::WrongSize {
                expected: NUM_CHUNKS,
                got: self.randomizers.len(),
            });
        }
        if self.ciphertexts.len() != recipients.len() {
            return Err(NidkgError::WrongSize {
                expected: recipients.len(),
                got: self.ciphertexts.len(),
            });
        }
        if let Some(row) = self.ciphertexts.iter().find(|row| row.len() != NUM_CHUNKS) {
            return Err(NidkgError::WrongSize { expected: NUM_CHUNKS, got: row.len() });
        }
        // G1 points are subgroup-checked as they decode; G2 points only on the curve
        if !self.public.points_valid()
            || !self.constant_term_proof.points_valid()
            || !in_subgroup(&self.sharing_proof.a)
        {
            return Err(NidkgError::PointNotInGroup);
        }
        if !self
            .public
            .verify_constant_term_proof(&constant_term_context(context), &self.constant_term_proof)
        {
            return Err(NidkgError::BadConstantTermProof);
        }
        verify_proofs(
            context,
            recipients,
            &self.positions,
            &self.public,
            &self.randomizers,
            &self.ciphertexts,
            &self.sharing_proof,
            &self.chunking_proof,
        )
    }

    /// Decrypt the share of the recipient at committee `position`. Call only on a verified
    /// dealing.
    ///
    /// An honest dealer's chunks are in `[0, B)` and each is found by one table lookup. The
    /// chunking proof only guarantees that a chunk is in `(-Z, Z)`, so a dishonest dealer can
    /// force a search of that range: about a second for a chunk near `Z` at `n = 56`. Without
    /// effort the proof keeps the chunks' weighted sum below `Z`, but a dealer that grinds its
    /// rejection sampling (cheap retries) can push every chunk of one recipient near `Z / 5`,
    /// a few seconds of search per such dealing. That is bounded, and the search is complete,
    /// so a verified dealing always decrypts; `Undecryptable` means it was not verified.
    pub fn decrypt(
        &self,
        position: u32,
        key: &DecryptionKey,
        table: &BabySteps,
    ) -> Result<SecretShare, NidkgError> {
        let (_, z_bound) = chunking_bounds(self.ciphertexts.len());
        let search = z_bound as u64;
        let row = self
            .positions
            .binary_search(&position)
            .ok()
            .and_then(|row| self.ciphertexts.get(row))
            .ok_or(NidkgError::UnknownRecipient(position))?;
        let weights = chunk_weights();
        let mut share = Fr::from_u64(0);
        for (j, (c_ij, r_j)) in row.iter().zip(&self.randomizers).enumerate() {
            // g1^s_ij = C_ij / R_j^x
            let point = c_ij.add(&r_j.mul(&key.0).neg());
            let chunk = table.find(&point, search).ok_or(NidkgError::Undecryptable(j))?;
            share = share.add(&fr_from_i128(chunk).mul(&weights[j]));
        }
        let index = ShareIndex::from_position(position);
        let share = SecretShare::from_fr(index, &share)?;
        if share.public_key() != self.public.share_key(index) {
            return Err(NidkgError::ShareMismatch);
        }
        Ok(share)
    }

    /// The committee positions this dealing was made for, strictly increasing.
    pub fn positions(&self) -> &[u32] {
        &self.positions
    }

    /// Bytes this dealing takes on the wire.
    pub fn encoded_len(&self) -> usize {
        bcs::to_bytes(self).map(|bytes| bytes.len()).unwrap_or_default()
    }

    /// Bytes any dealing to `recipients` at `threshold` takes on the wire: every field has a
    /// fixed size for a given shape, so a verifier can refuse a mis-sized encoding before it
    /// spends anything decoding it.
    pub fn encoded_len_for(recipients: usize, threshold: usize) -> usize {
        let g1 = G1::generator();
        let zero = Scalar(Fr::from_u64(0));
        let placeholder = Self {
            public: PublicPolynomial::new(vec![g2_generator_key(); threshold.max(1)])
                .expect("non-empty"),
            constant_term_proof: ConstantTermProof::placeholder(),
            positions: vec![0; recipients],
            randomizers: vec![g1; NUM_CHUNKS],
            ciphertexts: vec![vec![g1; NUM_CHUNKS]; recipients],
            sharing_proof: SharingProof {
                f: g1,
                a: g2_generator_key(),
                y: g1,
                z_r: zero.clone(),
                z_a: zero.clone(),
            },
            chunking_proof: ChunkingProof {
                y0: g1,
                bb: vec![g1; NUM_REPETITIONS],
                cc: vec![g1; NUM_REPETITIONS],
                dd: vec![g1; recipients + 1],
                yy: g1,
                z_r: vec![zero.clone(); recipients],
                z_s: vec![0; NUM_REPETITIONS],
                z_beta: zero,
            },
        };
        placeholder.encoded_len()
    }
}

fn constant_term_context(context: &[u8]) -> Vec<u8> {
    let mut out = DST_CONSTANT_TERM.to_vec();
    out.extend_from_slice(context);
    out
}

// ---------------------------------------------------------------------------------------------
// Proof of correct sharing (section 6.4)
//
// The instance is the paper's `R = prod_j R_j^(B^j)` and `C_i = prod_j C_ij^(B^j)`, which
// encrypt `r = sum_j r_j B^j` and `s_i = sum_j s_ij B^j`. The challenge hashes the chunked
// ciphertexts they are computed from, so the verifier never needs `C_i` on its own.

fn sharing_first_challenge(
    context: &[u8],
    recipients: &[EncryptionKey],
    positions: &[u32],
    public: &PublicPolynomial,
    randomizers: &[G1],
    ciphertexts: &[Vec<G1>],
) -> Result<Fr, NidkgError> {
    let mut transcript = Transcript::new(context);
    transcript.g1s(recipients.iter().map(|key| &key.0));
    for position in positions {
        transcript.0.extend_from_slice(&position.to_le_bytes());
    }
    for coefficient in public.coefficients() {
        transcript.key(coefficient);
    }
    transcript.g1s(randomizers);
    for row in ciphertexts {
        transcript.g1s(row);
    }
    transcript.challenge(DST_SHARING_1)
}

fn sharing_second_challenge(
    x: &Fr,
    proof_f: &G1,
    proof_a: &BlsPublicKey,
    proof_y: &G1,
) -> Result<Fr, NidkgError> {
    let mut transcript = Transcript::new(&[]);
    transcript.fr(x);
    transcript.g1(proof_f);
    transcript.key(proof_a);
    transcript.g1(proof_y);
    transcript.challenge(DST_SHARING_2)
}

#[allow(clippy::too_many_arguments)]
fn prove_sharing<R: CryptoRng + RngCore>(
    context: &[u8],
    recipients: &[EncryptionKey],
    positions: &[u32],
    public: &PublicPolynomial,
    randomizers: &[G1],
    ciphertexts: &[Vec<G1>],
    combined_r: &Fr,
    shares: &[Fr],
    rng: &mut R,
) -> Result<SharingProof, NidkgError> {
    let x =
        sharing_first_challenge(context, recipients, positions, public, randomizers, ciphertexts)?;
    let x_powers = powers(&x, recipients.len());

    let rho = random_fr(rng);
    let alpha = random_fr(rng);
    let g1 = G1::generator();
    let f = g1.mul(&rho);
    let a = G2::generator().mul(&alpha).to_key();
    let keys: Vec<G1> = recipients.iter().map(|key| key.0).collect();
    let y = G1::msm(&keys, &x_powers).mul(&rho).add(&g1.mul(&alpha));
    let x2 = sharing_second_challenge(&x, &f, &a, &y)?;

    let z_r = combined_r.mul(&x2).add(&rho);
    let weighted_shares = shares
        .iter()
        .zip(&x_powers)
        .fold(Fr::from_u64(0), |acc, (s_i, x_i)| acc.add(&s_i.mul(x_i)));
    let z_a = x2.mul(&weighted_shares).add(&alpha);
    Ok(SharingProof { f, a, y, z_r: Scalar(z_r), z_a: Scalar(z_a) })
}

// ---------------------------------------------------------------------------------------------
// Proof of correct chunking (section 6.5)

fn chunking_first_message(
    context: &[u8],
    recipients: &[EncryptionKey],
    randomizers: &[G1],
    ciphertexts: &[Vec<G1>],
    y0: &G1,
    bb: &[G1],
    cc: &[G1],
) -> Transcript {
    let mut transcript = Transcript::new(context);
    transcript.g1s(recipients.iter().map(|key| &key.0));
    transcript.g1s(randomizers);
    for row in ciphertexts {
        transcript.g1s(row);
    }
    transcript.g1(y0);
    transcript.g1s(bb);
    transcript.g1s(cc);
    transcript
}

fn chunking_second_challenge(
    first: &Transcript,
    z_s: &[u64],
    dd: &[G1],
    yy: &G1,
) -> Result<Fr, NidkgError> {
    let mut transcript = Transcript::new(&first.digest());
    for z in z_s {
        transcript.0.extend_from_slice(&z.to_le_bytes());
    }
    transcript.g1s(dd);
    transcript.g1(yy);
    transcript.challenge(DST_CHUNKING_X)
}

/// `w_ij = sum_k e_ijk x^k`, the weight of chunk `(i, j)` in the chunking equations.
fn chunk_challenge_weights(e: &[u8], recipients: usize, x_powers: &[Fr]) -> Vec<Fr> {
    let mut w = Vec::with_capacity(recipients * NUM_CHUNKS);
    for i in 0..recipients {
        for j in 0..NUM_CHUNKS {
            // challenges are bits: sum the powers whose bit is set
            let acc = x_powers
                .iter()
                .enumerate()
                .filter(|(k, _)| e[e_index(i, j, *k)] == 1)
                .fold(Fr::from_u64(0), |acc, (_, x_k)| acc.add(x_k));
            w.push(acc);
        }
    }
    w
}

fn prove_chunking<R: CryptoRng + RngCore>(
    context: &[u8],
    recipients: &[EncryptionKey],
    randomizers: &[G1],
    ciphertexts: &[Vec<G1>],
    r: &[Fr],
    share_chunks: &[[i64; NUM_CHUNKS]],
    rng: &mut R,
) -> Result<ChunkingProof, NidkgError> {
    let n = recipients.len();
    let (s_bound, z_bound) = chunking_bounds(n);
    let g1 = G1::generator();

    for _ in 0..MAX_CHUNKING_ATTEMPTS {
        let y0 = g1.mul(&random_fr(rng));
        let beta: Vec<Fr> = (0..NUM_REPETITIONS).map(|_| random_fr(rng)).collect();
        let sigma: Vec<i128> =
            (0..NUM_REPETITIONS).map(|_| rng.random_range(-s_bound..z_bound)).collect();
        let bb: Vec<G1> = beta.iter().map(|b| g1.mul(b)).collect();
        let cc: Vec<G1> = beta
            .iter()
            .zip(&sigma)
            .map(|(b, s)| y0.mul(b).add(&g1.mul(&fr_from_i128(*s))))
            .collect();
        let first =
            chunking_first_message(context, recipients, randomizers, ciphertexts, &y0, &bb, &cc);
        let e = chunking_challenges(&first, n);

        let mut z_s = Vec::with_capacity(NUM_REPETITIONS);
        for (k, sigma_k) in sigma.iter().enumerate() {
            let mut sum = *sigma_k;
            for (i, row) in share_chunks.iter().enumerate() {
                for (j, s_ij) in row.iter().enumerate() {
                    sum += i128::from(e[e_index(i, j, k)]) * i128::from(*s_ij);
                }
            }
            z_s.push(sum);
        }
        // rejection sampling: a response outside [0, Z) would leak, so restart
        if z_s.iter().any(|z| *z < 0 || *z >= z_bound) {
            continue;
        }
        let z_s: Vec<u64> = z_s.into_iter().map(|z| z as u64).collect();

        let delta: Vec<Fr> = (0..=n).map(|_| random_fr(rng)).collect();
        let dd: Vec<G1> = delta.iter().map(|d| g1.mul(d)).collect();
        let mut y_points = Vec::with_capacity(n + 1);
        y_points.push(y0);
        y_points.extend(recipients.iter().map(|key| key.0));
        let yy = G1::msm(&y_points, &delta);
        let x = chunking_second_challenge(&first, &z_s, &dd, &yy)?;
        let x_powers = powers(&x, NUM_REPETITIONS);
        let w = chunk_challenge_weights(&e, n, &x_powers);

        // z_r_i = sum_j w_ij r_j + delta_i
        let z_r: Vec<Scalar> = (0..n)
            .map(|i| {
                let acc = r
                    .iter()
                    .zip(&w[i * NUM_CHUNKS..(i + 1) * NUM_CHUNKS])
                    .fold(delta[i + 1].clone(), |acc, (r_j, w_ij)| acc.add(&w_ij.mul(r_j)));
                Scalar(acc)
            })
            .collect();
        let z_beta =
            beta.iter().zip(&x_powers).fold(delta[0].clone(), |acc, (b, x_k)| acc.add(&b.mul(x_k)));
        return Ok(ChunkingProof { y0, bb, cc, dd, yy, z_r, z_s, z_beta: Scalar(z_beta) });
    }
    Err(NidkgError::ChunkingAborted)
}

// ---------------------------------------------------------------------------------------------
// Verification of both proofs
//
// The sharing proof has one equation in G2 and two in G1; the chunking proof has `n + 2` in
// G1. The G1 equations are checked together: each is written as a product that must be the
// identity, raised to a weight hashed from the whole proof, responses included, and the
// products are multiplied into one multi-scalar multiplication. A wrong equation survives only
// if the weights hit a relation fixed before they were drawn, probability about `1/p`. All
// points were decoded with a subgroup check, so no small-order component can cancel out.

/// The weights of the batched G1 equations.
struct BatchWeights {
    s1: Fr,
    s3: Fr,
    c0: Fr,
    cb: Fr,
    ci: Vec<Fr>,
}

/// Draws the weights after every element of both proofs is fixed, responses included. A weight
/// known before a response is chosen would let a dealer pick that response to cancel a failing
/// equation inside the weighted sum.
fn batch_weights(
    first: &Transcript,
    x2: &Fr,
    xc: &Fr,
    sharing: &SharingProof,
    chunking: &ChunkingProof,
) -> Result<BatchWeights, NidkgError> {
    let mut transcript = Transcript::new(&first.digest());
    transcript.fr(x2);
    transcript.fr(xc);
    transcript.fr(&sharing.z_r.0);
    transcript.fr(&sharing.z_a.0);
    for z in &chunking.z_r {
        transcript.fr(&z.0);
    }
    transcript.fr(&chunking.z_beta.0);
    let seed = transcript.digest();
    let weight = |label: u64| -> Result<Fr, NidkgError> {
        let mut transcript = Transcript::new(&seed);
        transcript.0.extend_from_slice(&label.to_le_bytes());
        transcript.challenge(DST_BATCH)
    };
    Ok(BatchWeights {
        s1: weight(0)?,
        s3: weight(1)?,
        c0: weight(2)?,
        cb: weight(3)?,
        ci: (0..chunking.z_r.len()).map(|i| weight(4 + i as u64)).collect::<Result<_, _>>()?,
    })
}

fn verify_proofs(
    context: &[u8],
    recipients: &[EncryptionKey],
    positions: &[u32],
    public: &PublicPolynomial,
    randomizers: &[G1],
    ciphertexts: &[Vec<G1>],
    sharing: &SharingProof,
    chunking: &ChunkingProof,
) -> Result<(), NidkgError> {
    let n = recipients.len();
    if chunking.bb.len() != NUM_REPETITIONS
        || chunking.cc.len() != NUM_REPETITIONS
        || chunking.z_s.len() != NUM_REPETITIONS
        || chunking.dd.len() != n + 1
        || chunking.z_r.len() != n
    {
        return Err(NidkgError::BadProof);
    }
    let (_, z_bound) = chunking_bounds(n);
    if chunking.z_s.iter().any(|z| i128::from(*z) >= z_bound) {
        return Err(NidkgError::BadProof);
    }

    // challenges, exactly as the provers drew them
    let x =
        sharing_first_challenge(context, recipients, positions, public, randomizers, ciphertexts)?;
    let x2 = sharing_second_challenge(&x, &sharing.f, &sharing.a, &sharing.y)?;
    let x_powers = powers(&x, n);
    let first = chunking_first_message(
        context,
        recipients,
        randomizers,
        ciphertexts,
        &chunking.y0,
        &chunking.bb,
        &chunking.cc,
    );
    let e = chunking_challenges(&first, n);
    let xc = chunking_second_challenge(&first, &chunking.z_s, &chunking.dd, &chunking.yy)?;
    let xc_powers = powers(&xc, NUM_REPETITIONS);
    let w = chunk_challenge_weights(&e, n, &xc_powers);

    // sharing, G2: (prod_k A_k^(sum_i i^k x^i))^x2 * A == g2^z_a
    let indices: Vec<Fr> = positions.iter().copied().map(index_fr).collect();
    let mut index_powers: Vec<Fr> = vec![Fr::from_u64(1); n];
    let mut commitment_scalars = Vec::with_capacity(public.threshold());
    for _ in 0..public.threshold() {
        let sum = index_powers
            .iter()
            .zip(&x_powers)
            .fold(Fr::from_u64(0), |acc, (i_k, x_i)| acc.add(&i_k.mul(x_i)));
        commitment_scalars.push(sum.mul(&x2));
        for (power, index) in index_powers.iter_mut().zip(&indices) {
            *power = power.mul(index);
        }
    }
    let commitments: Vec<G2> = public.coefficients().iter().map(G2::from_key).collect();
    let lhs = G2::msm(&commitments, &commitment_scalars).add(&G2::from_key(&sharing.a));
    if lhs != G2::generator().mul(&sharing.z_a.0) {
        return Err(NidkgError::BadProof);
    }

    let BatchWeights { s1: g_s1, s3: g_s3, c0: g_c0, cb: g_cb, ci: g_ci } =
        batch_weights(&first, &x2, &xc, sharing, chunking)?;

    let mut points: Vec<G1> = Vec::with_capacity(n * NUM_CHUNKS + 3 * n + 2 * NUM_REPETITIONS + 24);
    let mut scalars: Vec<Fr> = Vec::with_capacity(points.capacity());
    let mut g_exponent = Fr::from_u64(0);
    let chunk_weights = chunk_weights();

    // S1: prod_j R_j^(x2 B^j) * F * g1^-z_r
    // C_i: prod_j R_j^(w_ij) * D_i * g1^-z_r_i
    for (j, r_j) in randomizers.iter().enumerate() {
        let mut scalar = g_s1.mul(&x2).mul(&chunk_weights[j]);
        for (i, g_i) in g_ci.iter().enumerate() {
            scalar = scalar.add(&g_i.mul(&w[i * NUM_CHUNKS + j]));
        }
        points.push(*r_j);
        scalars.push(scalar);
    }
    points.push(sharing.f);
    scalars.push(g_s1.clone());
    g_exponent = g_exponent.add(&g_s1.mul(&sharing.z_r.0));
    for (i, g_i) in g_ci.iter().enumerate() {
        points.push(chunking.dd[i + 1]);
        scalars.push(g_i.clone());
        g_exponent = g_exponent.add(&g_i.mul(&chunking.z_r[i].0));
    }

    // S3: prod_ij C_ij^(x2 x^i B^j) * Y * prod_i y_i^(-z_r x^i) * g1^-z_a
    // CB: prod_ij C_ij^(w_ij) * prod_k CC_k^(xc^k) * YY * prod_i y_i^(-z_r_i) * y0^-z_beta
    //     * g1^-(sum_k z_s_k xc^k)
    for (i, row) in ciphertexts.iter().enumerate() {
        let share_weight = g_s3.mul(&x2).mul(&x_powers[i]);
        for (j, c_ij) in row.iter().enumerate() {
            points.push(*c_ij);
            scalars
                .push(share_weight.mul(&chunk_weights[j]).add(&g_cb.mul(&w[i * NUM_CHUNKS + j])));
        }
    }
    points.push(sharing.y);
    scalars.push(g_s3.clone());
    g_exponent = g_exponent.add(&g_s3.mul(&sharing.z_a.0));
    for (i, key) in recipients.iter().enumerate() {
        let scalar = g_s3.mul(&sharing.z_r.0).mul(&x_powers[i]).add(&g_cb.mul(&chunking.z_r[i].0));
        points.push(key.0);
        scalars.push(scalar.neg());
    }
    for (k, cc_k) in chunking.cc.iter().enumerate() {
        points.push(*cc_k);
        scalars.push(g_cb.mul(&xc_powers[k]));
    }
    points.push(chunking.yy);
    scalars.push(g_cb.clone());
    points.push(chunking.y0);
    scalars.push(g_cb.mul(&chunking.z_beta.0).neg());
    let z_s_combined = chunking
        .z_s
        .iter()
        .zip(&xc_powers)
        .fold(Fr::from_u64(0), |acc, (z, x_k)| acc.add(&Fr::from_u64(*z).mul(x_k)));
    g_exponent = g_exponent.add(&g_cb.mul(&z_s_combined));

    // C0: prod_k B_k^(xc^k) * D_0 * g1^-z_beta
    for (k, b_k) in chunking.bb.iter().enumerate() {
        points.push(*b_k);
        scalars.push(g_c0.mul(&xc_powers[k]));
    }
    points.push(chunking.dd[0]);
    scalars.push(g_c0.clone());
    g_exponent = g_exponent.add(&g_c0.mul(&chunking.z_beta.0));

    points.push(G1::generator());
    scalars.push(g_exponent.neg());
    if !G1::msm(&points, &scalars).is_identity() {
        return Err(NidkgError::BadProof);
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Decryption

/// Baby-step table `g1^v -> v` for `v` in `[0, B)`. Built once per process (a few megabytes).
pub struct BabySteps {
    table: HashMap<[u8; G1_BYTES], u32>,
    /// `g1^B`, the giant step.
    giant: G1,
}

impl fmt::Debug for BabySteps {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BabySteps({} entries)", self.table.len() + 1)
    }
}

impl Default for BabySteps {
    fn default() -> Self {
        Self::new()
    }
}

impl BabySteps {
    pub fn new() -> Self {
        let g1 = G1::generator();
        let mut points = Vec::with_capacity(CHUNK_SIZE as usize);
        let mut acc = G1::identity();
        for _ in 0..CHUNK_SIZE {
            points.push(acc.0);
            acc = acc.add(&g1);
        }
        let mut table = HashMap::with_capacity(CHUNK_SIZE as usize);
        // index 0 is the identity, which blst's batch normalisation does not handle; it is the
        // chunk value zero and is checked separately in `find`.
        let affines = p1_affines::from(&points[1..]);
        for (v, affine) in affines.as_slice().iter().enumerate() {
            let mut bytes = [0u8; G1_BYTES];
            // SAFETY: `bytes` is 48 bytes, the size blst writes; `affine` is live.
            unsafe { blst_p1_affine_compress(bytes.as_mut_ptr(), affine) };
            table.insert(bytes, v as u32 + 1);
        }
        Self { table, giant: acc }
    }

    fn lookup(&self, point: &G1) -> Option<u64> {
        if point.is_identity() {
            return Some(0);
        }
        self.table.get(&point.to_bytes()).map(|v| u64::from(*v))
    }

    /// The discrete log of `point` in `[-search, search]`, honest range first. Giant steps
    /// alternate directions so small magnitudes are found first, and are normalised in blocks so
    /// each step costs one addition and one table probe.
    fn find(&self, point: &G1, search: u64) -> Option<i128> {
        if let Some(v) = self.lookup(point) {
            return Some(i128::from(v));
        }
        // point = g1^(q B + v): walk q both ways from zero.
        let steps = search.div_ceil(CHUNK_SIZE);
        let giant_neg = self.giant.neg();
        let base = i128::from(CHUNK_SIZE);
        let mut up = *point;
        let mut down = *point;
        let mut done = 0u64;
        while done < steps {
            let len = GIANT_BLOCK.min(steps - done);
            let mut batch = Vec::with_capacity(2 * len as usize);
            for offset in 1..=len {
                let q = i128::from(done + offset);
                up = up.add(&giant_neg);
                down = down.add(&self.giant);
                // the identity has no affine form; it means v = 0 at this step
                if up.is_identity() {
                    return Some(q * base);
                }
                if down.is_identity() {
                    return Some(-q * base);
                }
                batch.push(up.0);
                batch.push(down.0);
            }
            let affines = p1_affines::from(&batch);
            for (t, affine) in affines.as_slice().iter().enumerate() {
                let mut bytes = [0u8; G1_BYTES];
                // SAFETY: `bytes` is 48 bytes, the size blst writes; `affine` is live.
                unsafe { blst_p1_affine_compress(bytes.as_mut_ptr(), affine) };
                if let Some(v) = self.table.get(&bytes) {
                    let q = i128::from(done + 1 + (t / 2) as u64);
                    let v = i128::from(*v);
                    return Some(if t % 2 == 0 { q * base + v } else { v - q * base });
                }
            }
            done += len;
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rngs::StdRng, SeedableRng};
    use std::sync::OnceLock;

    fn table() -> &'static BabySteps {
        static TABLE: OnceLock<BabySteps> = OnceLock::new();
        TABLE.get_or_init(BabySteps::new)
    }

    /// `n` recipients at positions `0..n`.
    fn keys(n: usize, rng: &mut StdRng) -> (Vec<DecryptionKey>, Vec<Recipient>) {
        let secrets: Vec<DecryptionKey> = (0..n).map(|_| DecryptionKey(random_fr(rng))).collect();
        let public = secrets
            .iter()
            .enumerate()
            .map(|(position, key)| Recipient {
                position: position as u32,
                key: key.encryption_key(),
            })
            .collect();
        (secrets, public)
    }

    fn threshold(n: usize) -> usize {
        2 * n / 3 + 1
    }

    #[test]
    fn honest_dealing_verifies_and_every_recipient_decrypts_its_share() {
        let mut rng = StdRng::seed_from_u64(7);
        for n in [1usize, 4, 7] {
            let t = threshold(n);
            let (secrets, public) = keys(n, &mut rng);
            let polynomial = SecretPolynomial::random(t, &mut rng).unwrap();
            let dealing = Dealing::new(b"ctx", &polynomial, &public, &mut rng).unwrap();
            dealing.verify(b"ctx", t, &public).unwrap();
            for (position, key) in secrets.iter().enumerate() {
                let share = dealing.decrypt(position as u32, key, table()).unwrap();
                let expected =
                    polynomial.share(ShareIndex::from_position(position as u32)).unwrap();
                assert_eq!(share.to_bytes(), expected.to_bytes(), "n={n} position={position}");
            }
        }
    }

    #[test]
    fn dealing_does_not_verify_in_another_context_or_for_other_recipients() {
        let mut rng = StdRng::seed_from_u64(8);
        let (_, public) = keys(4, &mut rng);
        let polynomial = SecretPolynomial::random(3, &mut rng).unwrap();
        let dealing = Dealing::new(b"epoch 5 dealer 1", &polynomial, &public, &mut rng).unwrap();
        assert!(dealing.verify(b"epoch 5 dealer 2", 3, &public).is_err());
        let (_, others) = keys(4, &mut rng);
        assert!(dealing.verify(b"epoch 5 dealer 1", 3, &others).is_err());
        assert!(matches!(
            dealing.verify(b"epoch 5 dealer 1", 2, &public),
            Err(NidkgError::WrongSize { .. })
        ));
    }

    /// A dealer that encrypts a share off its polynomial fails the sharing proof.
    #[test]
    fn tampered_ciphertext_fails_verification() {
        let mut rng = StdRng::seed_from_u64(9);
        let (_, public) = keys(4, &mut rng);
        let polynomial = SecretPolynomial::random(3, &mut rng).unwrap();
        let dealing = Dealing::new(b"ctx", &polynomial, &public, &mut rng).unwrap();

        let mut bad = dealing.clone();
        bad.ciphertexts[2][3] = bad.ciphertexts[2][3].add(&G1::generator());
        assert!(bad.verify(b"ctx", 3, &public).is_err());

        let mut bad = dealing.clone();
        bad.chunking_proof.z_s[0] += 1;
        assert_eq!(bad.verify(b"ctx", 3, &public), Err(NidkgError::BadProof));

        let mut bad = dealing;
        bad.sharing_proof.z_r = Scalar(bad.sharing_proof.z_r.0.add(&Fr::from_u64(1)));
        assert_eq!(bad.verify(b"ctx", 3, &public), Err(NidkgError::BadProof));
    }

    /// A dealer that knew the batch weights before choosing its responses could break two
    /// equations so that their errors cancel in the weighted sum: here the sharing response
    /// `z_r` is shifted and the chunking response `z_r_0` solved to compensate, using the weights
    /// of the untampered proof. The weights depend on the responses, so the forgery fails.
    #[test]
    fn responses_cannot_cancel_inside_the_batched_check() {
        let mut rng = StdRng::seed_from_u64(13);
        let (secrets, public) = keys(4, &mut rng);
        let polynomial = SecretPolynomial::random(3, &mut rng).unwrap();
        let dealing = Dealing::new(b"ctx", &polynomial, &public, &mut rng).unwrap();
        let (sharing, chunking) = (&dealing.sharing_proof, &dealing.chunking_proof);
        let (keys, positions) = split_recipients(&public);

        let x = sharing_first_challenge(
            b"ctx",
            &keys,
            &positions,
            &dealing.public,
            &dealing.randomizers,
            &dealing.ciphertexts,
        )
        .unwrap();
        let x2 = sharing_second_challenge(&x, &sharing.f, &sharing.a, &sharing.y).unwrap();
        let first = chunking_first_message(
            b"ctx",
            &keys,
            &dealing.randomizers,
            &dealing.ciphertexts,
            &chunking.y0,
            &chunking.bb,
            &chunking.cc,
        );
        let xc =
            chunking_second_challenge(&first, &chunking.z_s, &chunking.dd, &chunking.yy).unwrap();
        let weights = batch_weights(&first, &x2, &xc, sharing, chunking).unwrap();

        // shifting z_r by delta moves the batched exponent by -delta (w_s1 + w_s3 sum_i x^i y_i)
        // and shifting z_r_0 by delta' moves it by -delta' (w_c0' + w_cb y_0), y_i being the
        // recipients' secret keys
        let delta = Fr::from_u64(1);
        let sharing_term = powers(&x, 4)
            .iter()
            .zip(&secrets)
            .fold(weights.s1.clone(), |acc, (x_i, key)| acc.add(&weights.s3.mul(x_i).mul(&key.0)));
        let chunking_term = weights.ci[0].add(&weights.cb.mul(&secrets[0].0));
        let delta_prime = delta.mul(&sharing_term).mul(&chunking_term.inverse().unwrap()).neg();

        let mut forged = dealing.clone();
        forged.sharing_proof.z_r = Scalar(sharing.z_r.0.add(&delta));
        forged.chunking_proof.z_r[0] = Scalar(chunking.z_r[0].0.add(&delta_prime));
        assert_eq!(forged.verify(b"ctx", 3, &public), Err(NidkgError::BadProof));
    }

    /// A dealing to one polynomial with another polynomial's commitments fails.
    #[test]
    fn commitments_from_another_polynomial_fail_verification() {
        let mut rng = StdRng::seed_from_u64(10);
        let (_, public) = keys(4, &mut rng);
        let polynomial = SecretPolynomial::random(3, &mut rng).unwrap();
        let other = SecretPolynomial::random(3, &mut rng).unwrap();
        let mut dealing = Dealing::new(b"ctx", &polynomial, &public, &mut rng).unwrap();
        dealing.public = other.commit().unwrap();
        assert!(dealing.verify(b"ctx", 3, &public).is_err());
    }

    /// A dishonest dealer encodes shares with chunks outside `[0, B)`: a chunk far above `B` for
    /// one recipient and a negative chunk for another, same share values. The proofs only bound
    /// chunks by `Z`, so the dealing verifies, and decryption must still find every chunk. (Much
    /// larger chunks make the dealer's own rejection sampling fail: the proof bounds the
    /// chunks' weighted sum, not just each chunk.)
    #[test]
    fn out_of_range_chunks_still_verify_and_decrypt() {
        let mut rng = StdRng::seed_from_u64(14);
        let n = 4;
        let (secrets, public) = keys(n, &mut rng);
        let polynomial = SecretPolynomial::random(3, &mut rng).unwrap();
        let shares: Vec<Fr> = (0..n as u32).map(|i| polynomial.evaluate(&index_fr(i))).collect();
        let (_, z_bound) = chunking_bounds(n);
        let big = (z_bound / 1024) as i64 / CHUNK_SIZE as i64;
        let b = CHUNK_SIZE as i64;
        let mut share_chunks: Vec<[i64; NUM_CHUNKS]> = shares.iter().map(chunks).collect();
        // c0 + B*k and c1 - k keep the share unchanged
        share_chunks[0][0] += big * b;
        share_chunks[0][1] -= big;
        // c2 - B and c3 + 1: chunk 2 goes negative
        share_chunks[1][2] -= b;
        share_chunks[1][3] += 1;
        let dealing =
            Dealing::from_chunks(b"ctx", &polynomial, &public, &shares, &share_chunks, &mut rng)
                .unwrap();
        dealing.verify(b"ctx", 3, &public).unwrap();
        for (position, key) in secrets.iter().enumerate() {
            let share = dealing.decrypt(position as u32, key, table()).unwrap();
            let expected = polynomial.share(ShareIndex::from_position(position as u32)).unwrap();
            assert_eq!(share.to_bytes(), expected.to_bytes());
        }
    }

    #[test]
    fn dealing_round_trips_through_bcs() {
        let mut rng = StdRng::seed_from_u64(11);
        let (_, public) = keys(4, &mut rng);
        let polynomial = SecretPolynomial::random(3, &mut rng).unwrap();
        let dealing = Dealing::new(b"ctx", &polynomial, &public, &mut rng).unwrap();
        let bytes = bcs::to_bytes(&dealing).unwrap();
        let decoded: Dealing = bcs::from_bytes(&bytes).unwrap();
        assert_eq!(decoded, dealing);
        decoded.verify(b"ctx", 3, &public).unwrap();
    }

    #[test]
    fn encryption_key_proof_binds_owner_and_context() {
        let mut rng = StdRng::seed_from_u64(12);
        let key = DecryptionKey(random_fr(&mut rng));
        let owner: BlsPublicKey = *crate::BlsKeypair::generate(&mut rng).public();
        let other: BlsPublicKey = *crate::BlsKeypair::generate(&mut rng).public();
        let proof = key.prove(&owner, b"chain", &mut rng).unwrap();
        assert!(key.encryption_key().verify(&owner, b"chain", &proof));
        assert!(!key.encryption_key().verify(&other, b"chain", &proof));
        assert!(!key.encryption_key().verify(&owner, b"other chain", &proof));
    }

    /// A dealing to the members at positions 0, 2 and 3 of a committee of four: each decrypts
    /// the share of its own index, the skipped member has none, and the dealing does not verify
    /// for other positions.
    #[test]
    fn dealing_to_a_subset_of_the_committee_keeps_committee_indices() {
        let mut rng = StdRng::seed_from_u64(15);
        let (secrets, all) = keys(4, &mut rng);
        let subset: Vec<Recipient> = [0usize, 2, 3].iter().map(|i| all[*i]).collect();
        let polynomial = SecretPolynomial::random(3, &mut rng).unwrap();
        let dealing = Dealing::new(b"ctx", &polynomial, &subset, &mut rng).unwrap();
        dealing.verify(b"ctx", 3, &subset).unwrap();
        assert_eq!(dealing.positions(), &[0, 2, 3]);
        for position in [0u32, 2, 3] {
            let share = dealing.decrypt(position, &secrets[position as usize], table()).unwrap();
            let expected = polynomial.share(ShareIndex::from_position(position)).unwrap();
            assert_eq!(share.to_bytes(), expected.to_bytes(), "position {position}");
        }
        assert_eq!(
            dealing.decrypt(1, &secrets[1], table()).unwrap_err(),
            NidkgError::UnknownRecipient(1)
        );
        let shifted: Vec<Recipient> =
            subset.iter().map(|r| Recipient { position: r.position + 1, key: r.key }).collect();
        assert_eq!(dealing.verify(b"ctx", 3, &shifted), Err(NidkgError::WrongRecipients));
        let mut unordered = subset.clone();
        unordered.swap(0, 1);
        assert_eq!(dealing.verify(b"ctx", 3, &unordered), Err(NidkgError::UnorderedRecipients));
        // the positions are part of the statement: rewriting them breaks the sharing proof
        let mut moved = dealing.clone();
        moved.positions = vec![0, 1, 3];
        let moved_recipients: Vec<Recipient> = [0usize, 1, 3]
            .iter()
            .zip(&subset)
            .map(|(p, r)| Recipient { position: *p as u32, key: r.key })
            .collect();
        assert_eq!(moved.verify(b"ctx", 3, &moved_recipients), Err(NidkgError::BadProof));
    }

    /// A point on the G2 curve outside the prime-order subgroup: a random compressed encoding
    /// that decompresses, which lands in the subgroup with negligible probability.
    fn g2_point_outside_the_subgroup(rng: &mut StdRng) -> BlsPublicKey {
        use blst::{blst_p2_affine_in_g2, blst_p2_uncompress};
        loop {
            let mut bytes = [0u8; 96];
            rng.fill_bytes(&mut bytes);
            bytes[0] = (bytes[0] & 0x1f) | 0x80; // compressed, not infinity
            let mut affine = blst_p2_affine::default();
            // SAFETY: `bytes` holds 96 bytes and `affine` is a live point.
            let decoded = unsafe { blst_p2_uncompress(&mut affine, bytes.as_ptr()) };
            // SAFETY: `affine` is a live point.
            if decoded == BLST_ERROR::BLST_SUCCESS && !unsafe { blst_p2_affine_in_g2(&affine) } {
                return PublicKey::from(affine).into();
            }
        }
    }

    /// G2 points with a small-order component would let a dealing verify while every share key
    /// misses its share. Commitments, the constant-term proof's commitment and the sharing
    /// proof's `A` are all checked for subgroup membership.
    #[test]
    fn g2_points_outside_the_subgroup_are_refused() {
        let mut rng = StdRng::seed_from_u64(16);
        let (_, public) = keys(4, &mut rng);
        let polynomial = SecretPolynomial::random(3, &mut rng).unwrap();
        let dealing = Dealing::new(b"ctx", &polynomial, &public, &mut rng).unwrap();
        let bad = g2_point_outside_the_subgroup(&mut rng);
        assert!(!in_subgroup(&bad));

        let mut tampered = dealing.clone();
        let mut coefficients = dealing.public.coefficients().to_vec();
        coefficients[1] = bad;
        tampered.public = PublicPolynomial::new(coefficients).unwrap();
        assert_eq!(tampered.verify(b"ctx", 3, &public), Err(NidkgError::PointNotInGroup));

        let mut tampered = dealing.clone();
        tampered.sharing_proof.a = bad;
        assert_eq!(tampered.verify(b"ctx", 3, &public), Err(NidkgError::PointNotInGroup));

        // the identity is refused too
        let identity: BlsPublicKey = PublicKey::from(blst_p2_affine::default()).into();
        let mut tampered = dealing;
        tampered.sharing_proof.a = identity;
        assert_eq!(tampered.verify(b"ctx", 3, &public), Err(NidkgError::PointNotInGroup));
    }

    /// The encoded size depends only on the shape, so the precompile can refuse a mis-sized
    /// dealing before decoding it.
    #[test]
    fn encoded_len_for_matches_real_dealings() {
        let mut rng = StdRng::seed_from_u64(17);
        for (n, t) in [(1usize, 1usize), (4, 3), (7, 5)] {
            let (_, public) = keys(n, &mut rng);
            let polynomial = SecretPolynomial::random(t, &mut rng).unwrap();
            let dealing = Dealing::new(b"ctx", &polynomial, &public, &mut rng).unwrap();
            assert_eq!(dealing.encoded_len(), Dealing::encoded_len_for(n, t), "n={n} t={t}");
        }
    }

    #[test]
    fn baby_steps_find_small_values_both_signs() {
        let g1 = G1::generator();
        for value in [0i128, 1, 65_535, 65_536, 1_000_000, -1, -65_537] {
            let point = g1.mul(&fr_from_i128(value));
            assert_eq!(table().find(&point, 2_000_000), Some(value), "value {value}");
        }
        assert_eq!(table().find(&g1.mul_u64(70_000), 0), None);
    }

    /// Measurements for the spike: deal size and times for the committee sizes in the handoff.
    /// `cargo test --release -p rayls-infrastructure-types nidkg::tests::measure -- --ignored
    /// --nocapture`
    #[test]
    #[ignore]
    fn measure() {
        use std::time::Instant;
        let mut rng = StdRng::seed_from_u64(13);
        let start = Instant::now();
        let table = BabySteps::new();
        println!("baby-step table: {:?}", start.elapsed());
        for n in [4usize, 7, 21, 56] {
            let t = threshold(n);
            let (secrets, public) = keys(n, &mut rng);
            let polynomial = SecretPolynomial::random(t, &mut rng).unwrap();
            let start = Instant::now();
            let dealing = Dealing::new(b"ctx", &polynomial, &public, &mut rng).unwrap();
            let deal_time = start.elapsed();
            let bytes = encode(&dealing);
            let start = Instant::now();
            let decoded: Dealing = crate::try_decode(&bytes).unwrap();
            let decode_time = start.elapsed();
            let start = Instant::now();
            decoded.verify(b"ctx", t, &public).unwrap();
            let verify_time = start.elapsed();
            let start = Instant::now();
            dealing.decrypt(n as u32 - 1, &secrets[n - 1], &table).unwrap();
            let decrypt_time = start.elapsed();
            println!(
                "n={n:>2} t={t:>2} size={:>6} B  deal={deal_time:>10.2?}  decode={decode_time:>10.2?}  verify={verify_time:>10.2?}  decrypt={decrypt_time:>10.2?}",
                dealing.encoded_len()
            );
        }
        // Worst case a dishonest dealer can force for one chunk at n = 56: a chunk at -(Z - 1),
        // the last value the search reaches.
        let (_, z_bound) = chunking_bounds(56);
        let value = -(z_bound - 1);
        let point = G1::generator().mul(&fr_from_i128(value));
        let start = Instant::now();
        assert_eq!(table.find(&point, z_bound as u64), Some(value));
        println!(
            "worst-case chunk search: {:?} (Z = 2^{:.1})",
            start.elapsed(),
            (z_bound as f64).log2()
        );
    }
}
