//! Serialize and deserialize roaring bitmap used by certificates.

use std::fmt;

use serde::{
    de::Deserializer,
    ser::{Error as SerError, Serializer},
};
use serde_with::{DeserializeAs, SerializeAs};

/// Deserialize a roaring bitmap from its on-disk bytes and normalize it by
/// rebuilding from its (already-sorted) values.
///
/// `roaring`'s checked deserializer accepts a run-container with zero runs, which
/// yields an *empty container*. That container panics on the next
/// re-serialization — `(container.len() - 1)` underflows under overflow-checks.
/// Rebuilding drops any empty container while preserving every value. Call this
/// at every roaring deserialization boundary that reads untrusted (peer or disk)
/// bytes so a malformed bitmap can't crash the node when it is later re-encoded.
/// See issue #55.
pub fn deserialize_normalized(bytes: &[u8]) -> std::io::Result<roaring::RoaringBitmap> {
    let raw = roaring::RoaringBitmap::deserialize_from(bytes)?;
    roaring::RoaringBitmap::from_sorted_iter(raw)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

/// Serde interface to RoaringBitmap according to the roaring bitmap on-disk standard.
pub(crate) struct RoaringBitmapSerde;

impl SerializeAs<roaring::RoaringBitmap> for RoaringBitmapSerde {
    fn serialize_as<S>(source: &roaring::RoaringBitmap, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut bytes = vec![];

        source
            .serialize_into(&mut bytes)
            .map_err(|e| S::Error::custom(format!("roaring bitmap serialization failed: {e:?}")))?;
        if serializer.is_human_readable() {
            serializer.serialize_str(&bs58::encode(&bytes).into_string())
        } else {
            serializer.serialize_bytes(&bytes)
        }
    }
}

impl<'de> DeserializeAs<'de, roaring::RoaringBitmap> for RoaringBitmapSerde {
    fn deserialize_as<D>(deserializer: D) -> Result<roaring::RoaringBitmap, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::*;

        struct RBVisitor;

        impl Visitor<'_> for RBVisitor {
            type Value = roaring::RoaringBitmap;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "valid roaring bitmap bytes")
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: Error,
            {
                // Normalize on read so a malformed wire bitmap (empty container)
                // can't panic when the certificate is later re-encoded. See #55.
                deserialize_normalized(v).map_err(|e| {
                    Error::custom(format!("roaring bitmap deserialization failed: {e:?}"))
                })
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                let bytes = bs58::decode(v)
                    .into_vec()
                    .map_err(|_| Error::invalid_value(Unexpected::Str(v), &self))?;
                self.visit_bytes(&bytes)
            }
        }

        if deserializer.is_human_readable() {
            deserializer.deserialize_str(RBVisitor)
        } else {
            deserializer.deserialize_bytes(RBVisitor)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{Certificate, EpochCertificate};

    /// Regression for issue #55. This byte sequence — found by the
    /// `bcs_roundtrip` fuzz target — decodes into a `Certificate` whose
    /// `signed_authorities` roaring bitmap contains an empty container (via
    /// roaring's checked deserializer accepting a run-container with zero runs).
    /// Before the normalize-on-deserialize fix in `RoaringBitmapSerde`,
    /// re-encoding this certificate panicked with "attempt to subtract with
    /// overflow" in roaring's serializer.
    const ROARING_EMPTY_CONTAINER_CRASH: &[u8] =
        include_bytes!("testdata/roaring_empty_container_crash.bin");

    #[test]
    fn certificate_with_empty_roaring_container_reencodes_without_panic() {
        // Guard that the input still decodes as a Certificate, so a future
        // layout change can't silently turn this regression test into a no-op.
        let cert: Certificate = bcs::from_bytes(ROARING_EMPTY_CONTAINER_CRASH)
            .expect("crash input should decode as a Certificate");

        // This re-encode is exactly what panicked before the fix.
        let encoded = bcs::to_bytes(&cert).expect("re-encode must not panic");

        // And the normalized certificate must round-trip stably.
        let decoded: Certificate = bcs::from_bytes(&encoded).expect("re-decode must succeed");
        let re_encoded = bcs::to_bytes(&decoded).expect("second re-encode must succeed");
        assert_eq!(encoded, re_encoded, "normalized certificate must round-trip stably");
    }

    /// Bytes of an empty signer bitmap as bcs writes them.
    /// The first byte is the length prefix, then cookie 12346 and a zero block count.
    const EMPTY_BITMAP_BCS: [u8; 9] = [8, 0x3A, 0x30, 0, 0, 0, 0, 0, 0];

    /// One block covers 65,536 signer indexes.
    const BLOCK: u64 = 1 << 16;

    /// Most blocks a signer bitmap may span. A committee indexes signers by position, so one
    /// block is always enough. The fix will define this limit; commit 2 switches these tests to it.
    const MAX_SIGNER_BITMAP_BLOCKS: u32 = 1;

    /// Writes a length as bcs does, seven bits per byte.
    fn uleb128(mut n: usize) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let byte = (n & 0x7F) as u8;
            n >>= 7;
            if n == 0 {
                out.push(byte);
                return out;
            }
            out.push(byte | 0x80);
        }
    }

    /// Builds bitmap bytes with `blocks` blocks that are each fully set.
    /// Each block is stored as one run, so it costs only 14 bytes.
    fn full_run_bitmap(blocks: u32) -> Vec<u8> {
        assert!((1..=64).contains(&blocks), "keep the test cheap");
        let n = blocks as usize;
        let mut b = Vec::new();
        b.extend_from_slice(&(12347_u32 | ((blocks - 1) << 16)).to_le_bytes());
        // One flag bit per block says it is stored as runs.
        b.extend(std::iter::repeat_n(0xFF_u8, n.div_ceil(8)));
        for key in 0..blocks {
            // Block key, then the value count minus one.
            b.extend_from_slice(&(key as u16).to_le_bytes());
            b.extend_from_slice(&u16::MAX.to_le_bytes());
        }
        if n >= 4 {
            // Offset table, needed from four blocks on.
            let start = b.len() + 4 * n;
            for i in 0..n {
                b.extend_from_slice(&((start + 6 * i) as u32).to_le_bytes());
            }
        }
        for _ in 0..blocks {
            // One run, starting at 0, with length minus one.
            b.extend_from_slice(&1_u16.to_le_bytes());
            b.extend_from_slice(&0_u16.to_le_bytes());
            b.extend_from_slice(&u16::MAX.to_le_bytes());
        }
        b
    }

    /// Encodes a default certificate and swaps its empty signer bitmap for `bitmap`.
    fn certificate_bytes_with_bitmap(bitmap: &[u8]) -> Vec<u8> {
        let encoded = bcs::to_bytes(&Certificate::default()).expect("encode certificate");
        let hits: Vec<usize> = encoded
            .windows(EMPTY_BITMAP_BCS.len())
            .enumerate()
            .filter(|(_, w)| *w == EMPTY_BITMAP_BCS)
            .map(|(i, _)| i)
            .collect();
        assert_eq!(hits.len(), 1, "empty signer bitmap must appear once in the certificate");
        let at = hits[0];
        let mut out = encoded[..at].to_vec();
        out.extend(uleb128(bitmap.len()));
        out.extend_from_slice(bitmap);
        out.extend_from_slice(&encoded[at + EMPTY_BITMAP_BCS.len()..]);
        out
    }

    /// Decodes a certificate and fails the test if the decode is accepted.
    fn assert_certificate_rejected(bytes: &[u8], what: &str) {
        if let Ok(cert) = bcs::from_bytes::<Certificate>(bytes) {
            panic!(
                "{what}: certificate decoded with {} signers, it must be refused",
                cert.signed_authorities().len()
            );
        }
    }

    /// The splice helper must give a valid certificate for a normal signer bitmap.
    /// This guards the other tests against a change in the certificate layout.
    #[test]
    fn signer_bitmap_normal_committee_bitmap_still_decodes() {
        let bitmap = roaring::RoaringBitmap::from_iter([0_u32, 1, 2]);
        let mut bytes = Vec::new();
        bitmap.serialize_into(&mut bytes).expect("serialize bitmap");
        let cert: Certificate = bcs::from_bytes(&certificate_bytes_with_bitmap(&bytes))
            .expect("a normal signer bitmap must decode");
        assert_eq!(cert.signed_authorities(), &bitmap);
    }

    /// A bitmap that fills the whole first block stands for 65,536 signers but spans one block.
    /// This is the most a signer bitmap may hold, so it must still decode after the fix.
    #[test]
    fn signer_bitmap_single_full_block_still_decodes() {
        let bytes = certificate_bytes_with_bitmap(&full_run_bitmap(MAX_SIGNER_BITMAP_BLOCKS));
        let cert: Certificate =
            bcs::from_bytes(&bytes).expect("a single full block must decode");
        assert_eq!(cert.signed_authorities().len(), BLOCK);
    }

    /// Two full blocks are 25 bytes on the wire but stand for 131,072 signers.
    /// One block is the limit, so a bitmap one block over it must be refused.
    #[test]
    fn signer_bitmap_two_full_run_blocks_is_rejected() {
        let bytes = certificate_bytes_with_bitmap(&full_run_bitmap(MAX_SIGNER_BITMAP_BLOCKS + 1));
        assert_certificate_rejected(&bytes, "2 full run blocks");
    }

    /// Seventeen full blocks stand for over a million signers.
    /// This also uses the offset table, which is read from four blocks on.
    #[test]
    fn signer_bitmap_seventeen_full_run_blocks_is_rejected() {
        let bytes = certificate_bytes_with_bitmap(&full_run_bitmap(17));
        assert_certificate_rejected(&bytes, "17 full run blocks");
    }

    /// Any signer index past the first block is out of reach for a committee.
    /// A bitmap with two small blocks must be refused too.
    #[test]
    fn signer_bitmap_two_small_blocks_is_rejected() {
        let bitmap = roaring::RoaringBitmap::from_iter([0_u32, BLOCK as u32]);
        let mut bytes = Vec::new();
        bitmap.serialize_into(&mut bytes).expect("serialize bitmap");
        assert_certificate_rejected(&certificate_bytes_with_bitmap(&bytes), "2 small blocks");
    }

    /// Epoch certificates use the same signer bitmap decoder.
    /// A bitmap with two full blocks must be refused there as well.
    #[test]
    fn signer_bitmap_epoch_certificate_two_full_blocks_is_rejected() {
        let mut signed_authorities = roaring::RoaringBitmap::new();
        signed_authorities.insert_range(0..(2 * BLOCK) as u32);
        let cert = EpochCertificate {
            epoch_hash: Default::default(),
            signature: Default::default(),
            signed_authorities,
        };
        let bytes = bcs::to_bytes(&cert).expect("encode epoch certificate");
        if let Ok(decoded) = bcs::from_bytes::<EpochCertificate>(&bytes) {
            panic!(
                "epoch certificate decoded with {} signers, it must be refused",
                decoded.signed_authorities.len()
            );
        }
    }
}
