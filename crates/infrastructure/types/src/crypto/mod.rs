//! Type aliases selecting the signature algorithm for the code base.
//!
//! Here we select the types that are used by default in the code base.
//!
//! Guidelines:
//! - refer to these aliases always (avoid using the individual scheme implementations)
//! - use generic schemes (avoid using the algo's `Struct`` impl functions)
//! - change type aliases to update codebase with new crypto

use crate::bcs_layout::{skip_bytes, BcsCursor, BcsLayout, BcsLayoutError};
use std::{fmt, future::Future};
// This re-export allows using the trait-defined APIs
mod bls_keypair;
mod bls_public_key;
mod bls_signature;
mod intent;
mod network;

pub use bls_keypair::*;
pub use bls_public_key::*;
pub use bls_signature::*;
pub use intent::*;
pub use network::*;
use serde::{Deserialize, Serialize};

/// Represents a digest of `DIGEST_LEN` bytes.
#[derive(Hash, PartialEq, Eq, Clone, Ord, PartialOrd, Copy)]
pub struct Digest<const DIGEST_LEN: usize> {
    pub digest: [u8; DIGEST_LEN],
}

impl<const DIGEST_LEN: usize> Default for Digest<DIGEST_LEN> {
    fn default() -> Self {
        Self { digest: [0_u8; DIGEST_LEN] }
    }
}

// ----- Serde implementations -----

impl<const DIGEST_LEN: usize> Serialize for Digest<DIGEST_LEN> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&bs58::encode(&self.digest).into_string())
        } else {
            serializer.serialize_bytes(&self.digest)
        }
    }
}

impl<'de, const DIGEST_LEN: usize> Deserialize<'de> for Digest<DIGEST_LEN> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::*;

        struct DigestVisitor<const DIGEST_LEN: usize>;

        impl<const DIGEST_LEN: usize> Visitor<'_> for DigestVisitor<DIGEST_LEN> {
            type Value = Digest<DIGEST_LEN>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "valid digest bytes")
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if v.len() == DIGEST_LEN {
                    let mut digest = [0_u8; DIGEST_LEN];
                    digest.copy_from_slice(v);
                    Ok(Digest { digest })
                } else {
                    let exp = format!("{DIGEST_LEN} bytes");
                    let e: &str = &exp;
                    Err(Error::invalid_length(v.len(), &e))
                }
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                // exact-length decode; `onto` rejects an over-long string, a short one leaves
                // `written` below DIGEST_LEN (it must not zero-pad into a valid-looking digest)
                let mut bytes = [0_u8; DIGEST_LEN];
                let written = bs58::decode(v)
                    .onto(&mut bytes)
                    .map_err(|_| Error::invalid_value(Unexpected::Str(v), &self))?;
                if written != DIGEST_LEN {
                    let exp = format!("{DIGEST_LEN} bytes");
                    let e: &str = &exp;
                    return Err(Error::invalid_length(written, &e));
                }
                self.visit_bytes(&bytes)
            }
        }

        if deserializer.is_human_readable() {
            deserializer.deserialize_str(DigestVisitor)
        } else {
            deserializer.deserialize_bytes(DigestVisitor)
        }
    }
}

impl<const DIGEST_LEN: usize> Digest<DIGEST_LEN> {
    /// Create a new digest containing the given bytes
    pub fn new(digest: [u8; DIGEST_LEN]) -> Self {
        Digest { digest }
    }

    /// Copy the digest into a new vector.
    pub fn to_vec(&self) -> Vec<u8> {
        self.digest.to_vec()
    }

    /// The size of this digest in bytes.
    pub fn size(&self) -> usize {
        DIGEST_LEN
    }
}

impl<const DIGEST_LEN: usize> fmt::Debug for Digest<DIGEST_LEN> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", bs58::encode(self.digest).into_string())
    }
}

impl<const DIGEST_LEN: usize> fmt::Display for Digest<DIGEST_LEN> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", bs58::encode(self.digest).into_string())
    }
}

impl<const DIGEST_LEN: usize> AsRef<[u8]> for Digest<DIGEST_LEN> {
    fn as_ref(&self) -> &[u8] {
        self.digest.as_ref()
    }
}

impl<const DIGEST_LEN: usize> From<Digest<DIGEST_LEN>> for [u8; DIGEST_LEN] {
    fn from(digest: Digest<DIGEST_LEN>) -> Self {
        digest.digest
    }
}

/// Trait implemented by hash functions providing a output of fixed length
pub trait HashFunction<const DIGEST_LENGTH: usize>: Default {
    /// The length of this hash functions digests in bytes.
    const OUTPUT_SIZE: usize = DIGEST_LENGTH;

    /// Create a new hash function of the given type
    fn new() -> Self {
        Self::default()
    }

    /// Process the given data, and update the internal of the hash function.
    fn update<Data: AsRef<[u8]>>(&mut self, data: Data);

    /// Retrieve result and consume hash function.
    fn finalize(self) -> Digest<DIGEST_LENGTH>;

    /// Compute the digest of the given data and consume the hash function.
    fn digest<Data: AsRef<[u8]>>(data: Data) -> Digest<DIGEST_LENGTH> {
        let mut h = Self::default();
        h.update(data);
        h.finalize()
    }

    /// Compute a single digest from all slices in the iterator in order and consume the hash
    /// function.
    fn digest_iterator<K: AsRef<[u8]>, I: Iterator<Item = K>>(iter: I) -> Digest<DIGEST_LENGTH> {
        let mut h = Self::default();
        iter.into_iter().for_each(|chunk| h.update(chunk.as_ref()));
        h.finalize()
    }
}

/// This trait is implemented by all messages that can be hashed.
pub trait Hash<const DIGEST_LEN: usize> {
    /// The type of the digest when this is hashed.
    type TypedDigest: Into<Digest<DIGEST_LEN>> + Eq + std::hash::Hash + Copy + fmt::Debug;

    fn digest(&self) -> Self::TypedDigest;
}

/// Trait impl'd by a key/keypair that can create signatures.
pub trait Signer {
    /// Create a new signature over a message.
    fn sign(&self, msg: &[u8]) -> BlsSignature;
}

//
// EXECUTION
//
/// Public key used for signing transactions in the Execution Layer.
pub type ExecutionPublicKey = secp256k1::PublicKey;
/// Keypair used for signing transactions in the Execution Layer.
pub type ExecutionKeypair = secp256k1::Keypair;

/// Type alias selecting the default hash function for the code base.
pub type DefaultHashFunction = blake3::Hasher;
pub const DIGEST_LENGTH: usize = 32;

/// BCS layout: `Digest<N>` serializes via `serialize_bytes` — ULEB128(N) + N
/// raw bytes.
impl<const N: usize> BcsLayout for Digest<N> {
    #[inline]
    fn skip(c: &mut BcsCursor<'_>) -> Result<(), BcsLayoutError> {
        skip_bytes(c)
    }
}
pub const INTENT_MESSAGE_LENGTH: usize = INTENT_PREFIX_LENGTH + DIGEST_LENGTH;

/// Trait to implement Bls key signing.  This allows us to maintain private keys in a
/// secure enclave and provide a signing service.
pub trait BlsSigner: Clone + Send + Sync + Unpin + 'static {
    /// Sync version to sign something with a BLS private key.
    fn request_signature_direct(&self, msg: &[u8]) -> BlsSignature;

    /// Request a signature asynchronously.
    /// Note: used the de-sugared signature here (instead of async fn request_signature...)
    /// due to current async trait limitations and the need for + Send.
    fn request_signature(&self, msg: Vec<u8>) -> impl Future<Output = BlsSignature> + Send {
        let this = self.clone();
        let handle = tokio::task::spawn_blocking(move || this.request_signature_direct(&msg));
        async move { handle.await.expect("Failed to receive signature from Signature Service") }
    }

    /// Return the public key of this signer.
    fn public_key(&self) -> BlsPublicKey;
}

/// Wrap a message in an intent message. Currently in Consensus, the scope is always
/// IntentScope::ConsensusDigest and the app id is AppId::Consensus.
pub fn to_intent_message<T>(value: T) -> IntentMessage<T> {
    IntentMessage::new(Intent::consensus(IntentScope::ConsensusDigest), value)
}

#[cfg(test)]
mod tests {
    use super::{generate_proof_of_possession_bls, verify_proof_of_possession_bls};
    use crate::BlsKeypair;
    use alloy::primitives::Address;
    use rand::{rngs::StdRng, SeedableRng};

    #[test]
    fn test_proof_of_possession_success() {
        let keypair = BlsKeypair::generate(&mut StdRng::from_os_rng());
        let address = Address::from_raw_public_key(&[0; 64]);
        let proof = generate_proof_of_possession_bls(&keypair, &address).unwrap();
        assert!(verify_proof_of_possession_bls(&proof, keypair.public(), &address).is_ok())
    }

    #[test]
    fn test_proof_of_possession_fails_wrong_signature() {
        let keypair = BlsKeypair::generate(&mut StdRng::from_os_rng());
        let malicious_key = BlsKeypair::generate(&mut StdRng::from_os_rng());
        let address = Address::from_raw_public_key(&[0; 64]);
        let proof = generate_proof_of_possession_bls(&malicious_key, &address).unwrap();
        assert!(verify_proof_of_possession_bls(&proof, keypair.public(), &address).is_err())
    }

    #[test]
    fn test_proof_of_possession_fails_wrong_public_key() {
        let keypair = BlsKeypair::generate(&mut StdRng::from_os_rng());
        let malicious_key = BlsKeypair::generate(&mut StdRng::from_os_rng());
        let address = Address::from_raw_public_key(&[0; 64]);
        let proof = generate_proof_of_possession_bls(&keypair, &address).unwrap();
        assert!(verify_proof_of_possession_bls(&proof, malicious_key.public(), &address).is_err())
    }

    #[test]
    fn test_proof_of_possession_fails_wrong_message() {
        let keypair = BlsKeypair::generate(&mut StdRng::from_os_rng());
        let address = Address::from_raw_public_key(&[0; 64]);
        let wrong = Address::from_raw_public_key(&[1; 64]);
        let proof = generate_proof_of_possession_bls(&keypair, &wrong).unwrap();
        assert!(verify_proof_of_possession_bls(&proof, keypair.public(), &address).is_err())
    }
}

#[cfg(test)]
mod digest_serde_tests {
    use super::Digest;

    #[test]
    fn short_base58_digest_is_rejected_not_zero_padded() {
        let full = Digest::<32> { digest: [7u8; 32] };
        let json = serde_json::to_string(&full).unwrap();
        assert_eq!(serde_json::from_str::<Digest<32>>(&json).unwrap().digest, [7u8; 32]);
        // decodes to one byte, not 32
        let err = serde_json::from_str::<Digest<32>>("\"2\"").unwrap_err().to_string();
        assert!(err.contains("expected 32 bytes"), "{err}");
        assert!(!err.contains("  "), "no double space in the message: {err}");
        // one zero byte, not the zero digest
        assert!(serde_json::from_str::<Digest<32>>("\"1\"").is_err());
        assert!(serde_json::from_str::<Digest<32>>(&format!("\"{}\"", "1".repeat(200))).is_err());
        // BCS: 32 raw bytes behind a length prefix
        let bytes = bcs::to_bytes(&full).unwrap();
        assert_eq!(bytes.len(), 33);
        assert_eq!(bcs::from_bytes::<Digest<32>>(&bytes).unwrap().digest, [7u8; 32]);
    }

    /// Leading zero bytes survive the round trip; the all-zero digest is the extreme case.
    #[test]
    fn leading_zero_digests_round_trip() {
        let mut one_leading = [0u8; 32];
        one_leading[1] = 7;
        one_leading[31] = 255;
        let mut many_leading = [0u8; 32];
        many_leading[30] = 1;

        for digest in [[0u8; 32], one_leading, many_leading, [255u8; 32]] {
            let value = Digest::<32> { digest };
            let json = serde_json::to_string(&value).unwrap();
            assert_eq!(
                serde_json::from_str::<Digest<32>>(&json).unwrap().digest,
                digest,
                "JSON round trip lost leading zeros for {digest:?}"
            );
            // binary path unchanged
            assert_eq!(
                bcs::from_bytes::<Digest<32>>(&bcs::to_bytes(&value).unwrap()).unwrap().digest,
                digest
            );
        }

        // 32 '1's is the zero digest; 31 is a 31-byte value
        let zero = serde_json::to_string(&Digest::<32> { digest: [0u8; 32] }).unwrap();
        assert_eq!(zero, format!("\"{}\"", "1".repeat(32)));
        assert!(serde_json::from_str::<Digest<32>>(&format!("\"{}\"", "1".repeat(31))).is_err());
    }
}
