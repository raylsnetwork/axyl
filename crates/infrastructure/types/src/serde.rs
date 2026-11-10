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
    normalize(roaring::RoaringBitmap::deserialize_from(bytes)?)
}

/// Like [`deserialize_normalized`], but returns `Ok(None)` for a bitmap holding more than
/// `max_len` values.
///
/// The count comes from the raw decode, so it is checked before normalizing walks every value.
/// A few bytes can stand for billions of values, so untrusted bitmaps with a known limit use this.
pub fn deserialize_normalized_bounded(
    bytes: &[u8],
    max_len: u64,
) -> std::io::Result<Option<roaring::RoaringBitmap>> {
    let raw = roaring::RoaringBitmap::deserialize_from(bytes)?;
    if raw.len() > max_len {
        return Ok(None);
    }
    normalize(raw).map(Some)
}

/// Rebuild a decoded bitmap from its values, which drops any empty container. See issue #55.
fn normalize(raw: roaring::RoaringBitmap) -> std::io::Result<roaring::RoaringBitmap> {
    roaring::RoaringBitmap::from_sorted_iter(raw)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

/// Most blocks a certificate signer bitmap may span.
///
/// A certificate's `signed_authorities` bitmap indexes committee members by their position in the
/// committee, so every index is below the committee size and fits the first block (values below
/// 65,536). An honest signer bitmap is therefore a single block. A second block costs only a
/// handful of wire bytes but expands to 8 KiB in memory, and a crafted bitmap can name billions of
/// values in under a MiB (issue #256). Bounding the span to one block caps the decode at 65,536
/// values (~8 KiB), so a small request cannot stand for a huge allocation.
pub const MAX_SIGNER_BITMAP_BLOCKS: u32 = 1;

// Portable roaring header cookies (mirror `roaring`'s `serialization.rs`, whose copies are
// `pub(crate)` and not importable here). 12346 = no run containers (the count follows as a u32);
// 12347 = run containers present (the count lives in the cookie's upper 16 bits, as count - 1).
const ROARING_COOKIE_NO_RUN: u32 = 12346;
const ROARING_COOKIE_RUN: u16 = 12347;

/// Number of containers a serialized roaring bitmap declares, and where its per-container
/// descriptions begin, or `None` when the bytes do not carry a recognizable, complete portable
/// header.
///
/// This mirrors `roaring`'s `deserialize_from_impl` header rules:
///   - no-run (12346): a u32 container count follows the 4-byte cookie, so descriptions start at
///     byte 8;
///   - run (12347): the count lives in the cookie's upper 16 bits (count - 1), so no u32 follows
///     the cookie; instead `count.div_ceil(8)` run-flag bytes (one flag bit per container) sit
///     between the cookie and the descriptions, which start at byte 4 plus those bytes.
///
/// Single home for the portable header format; other bounds (e.g. the skip-round container cap in
/// the primary crate) read it through here rather than parsing the header again.
pub fn roaring_header(bytes: &[u8]) -> Option<(u32, usize)> {
    let cookie = u32::from_le_bytes(bytes.get(..4)?.try_into().ok()?);
    if cookie == ROARING_COOKIE_NO_RUN {
        let count = u32::from_le_bytes(bytes.get(4..8)?.try_into().ok()?);
        Some((count, 8))
    } else if (cookie as u16) == ROARING_COOKIE_RUN {
        let count = (cookie >> 16) + 1;
        Some((count, 4 + count.div_ceil(8) as usize))
    } else {
        // Unknown cookie: not a recognizable header.
        None
    }
}

/// Check a certificate signer bitmap against [`MAX_SIGNER_BITMAP_BLOCKS`] before it is decoded.
///
/// Reads only the portable header (via [`roaring_header`]) and the per-container keys, so it is
/// allocation-free and linear in the input size: a crafted bitmap is refused while it is still
/// tiny on the wire, before [`deserialize_normalized`] expands it. `Ok` means the bitmap is within
/// the bound, or the bytes are not a recognizable portable header (the deserializer rejects
/// those); `Err` carries the reason for refusal - the declared container count is over the bound,
/// or a container sits past the first block (the highest-key rule).
fn check_signer_bitmap_block_bound(bytes: &[u8]) -> Result<(), String> {
    let max_blocks = MAX_SIGNER_BITMAP_BLOCKS;
    let Some((count, desc_start)) = roaring_header(bytes) else {
        // Unknown or truncated header: leave these bytes for the deserializer to reject rather
        // than deciding here.
        return Ok(());
    };

    // A second container is already beyond the allowed span; refuse without reading anything else.
    if count > max_blocks {
        return Err(format!(
            "signer bitmap declares {count} blocks, outside the {max_blocks}-block bound"
        ));
    }

    // At most one container: read its key and apply the highest-key rule, so a single container at
    // a high key (values from 65,536 on) is refused too.
    for i in 0..count as usize {
        let at = desc_start + i * 4;
        let Some(w) = bytes.get(at..at + 2) else {
            // Truncated header: the deserializer rejects it.
            return Ok(());
        };
        let key = u16::from_le_bytes(w.try_into().unwrap());
        if (key as u32) >= max_blocks {
            return Err(format!(
                "signer bitmap spans block {key}, outside the {max_blocks}-block bound"
            ));
        }
    }
    Ok(())
}

/// Serde interface to a certificate signer bitmap, according to the roaring bitmap on-disk
/// standard.
///
/// Decode applies the signer-bitmap rules: at most [`MAX_SIGNER_BITMAP_BLOCKS`] container, at key
/// 0 (issue #256). Its users are the signer bitmaps of `Certificate` and `EpochCertificate`; it is
/// not a general roaring serde, and a bitmap that legitimately spans more blocks (e.g. rounds)
/// must not use it.
pub(crate) struct SignerBitmapSerde;

impl SerializeAs<roaring::RoaringBitmap> for SignerBitmapSerde {
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

impl<'de> DeserializeAs<'de, roaring::RoaringBitmap> for SignerBitmapSerde {
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
                // Bound the block span before decoding so a crafted bitmap can't stand for
                // billions of values (issue #256); the refusal messages stand on their own.
                check_signer_bitmap_block_bound(v).map_err(Error::custom)?;
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
    use super::{check_signer_bitmap_block_bound, roaring_header, MAX_SIGNER_BITMAP_BLOCKS};
    use crate::{Certificate, EpochCertificate};

    /// Regression for issue #55. This byte sequence — found by the
    /// `bcs_roundtrip` fuzz target — decodes into a `Certificate` whose
    /// `signed_authorities` roaring bitmap contains an empty container (via
    /// roaring's checked deserializer accepting a run-container with zero runs).
    /// Before the normalize-on-deserialize fix in `SignerBitmapSerde`,
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

    // --- Issue #256: the signer bitmap must decode to at most one block -------------------------
    //
    // A certificate's `signed_authorities` bitmap indexes committee members by position, so it is
    // a single block. These tests pin the decode-time bound: crafted bitmaps that name more than
    // one block (or a block past the first) must be refused before they expand, while a normal
    // committee bitmap — even one filling the whole first block — must still decode.

    /// The empty signer bitmap as bcs writes it: a ULEB128 length prefix (8), then cookie 12346
    /// and a zero block count.
    const EMPTY_BITMAP_BCS: [u8; 9] = [8, 0x3A, 0x30, 0, 0, 0, 0, 0, 0];

    /// One block covers 65,536 signer indexes.
    const BLOCK: u64 = 1 << 16;

    /// Writes a length the way bcs does (seven bits per byte).
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

    /// Bytes of a portable run-container bitmap (cookie 12347) holding `blocks` full blocks, each
    /// stored as a single run (start 0, length 65,535), so each block costs only a handful of wire
    /// bytes but stands for 65,536 values. Kept to a small block count so the suite stays fast;
    /// the ~512 MiB / 65,536-block case is pinned by the measured harness, not a unit test.
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

    /// Encodes a default certificate and swaps its empty signer bitmap for `bitmap`, so the result
    /// is a full `Certificate` whose `signed_authorities` is the crafted byte sequence.
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

    /// The splice helper must produce a valid certificate for a normal signer bitmap. This guards
    /// the other tests against a change in the certificate layout.
    #[test]
    fn signer_bitmap_normal_committee_bitmap_still_decodes() {
        let bitmap = roaring::RoaringBitmap::from_iter([0_u32, 1, 2]);
        let mut bytes = Vec::new();
        bitmap.serialize_into(&mut bytes).expect("serialize bitmap");
        let cert_bytes = certificate_bytes_with_bitmap(&bytes);
        let cert: Certificate = bcs::from_bytes(&cert_bytes).expect("must decode");
        assert_eq!(cert.signed_authorities(), &bitmap);
    }

    /// A bitmap that fills the whole first block stands for 65,536 signers but spans one block.
    /// That is the most a signer bitmap may hold, so it must still decode after the fix.
    #[test]
    fn signer_bitmap_single_full_block_still_decodes() {
        let bytes = certificate_bytes_with_bitmap(&full_run_bitmap(MAX_SIGNER_BITMAP_BLOCKS));
        let cert: Certificate = bcs::from_bytes(&bytes).expect("a single full block must decode");
        assert_eq!(cert.signed_authorities().len(), BLOCK);
    }

    /// Two full blocks are 25 wire bytes but stand for 131,072 signers. One block is the limit, so
    /// a bitmap one block over it must be refused.
    #[test]
    fn signer_bitmap_two_full_run_blocks_is_rejected() {
        let bytes = certificate_bytes_with_bitmap(&full_run_bitmap(MAX_SIGNER_BITMAP_BLOCKS + 1));
        assert_certificate_rejected(&bytes, "2 full run blocks");
    }

    /// Seventeen full blocks stand for over a million signers. This also uses the offset table,
    /// which is read from four blocks on.
    #[test]
    fn signer_bitmap_seventeen_full_run_blocks_is_rejected() {
        let bytes = certificate_bytes_with_bitmap(&full_run_bitmap(17));
        assert_certificate_rejected(&bytes, "17 full run blocks");
    }

    /// Any signer index past the first block is out of reach for a committee. A bitmap with two
    /// small blocks (indexes 0 and 65,536) must be refused too.
    #[test]
    fn signer_bitmap_two_small_blocks_is_rejected() {
        let bitmap = roaring::RoaringBitmap::from_iter([0_u32, BLOCK as u32]);
        let mut bytes = Vec::new();
        bitmap.serialize_into(&mut bytes).expect("serialize bitmap");
        assert_certificate_rejected(&certificate_bytes_with_bitmap(&bytes), "2 small blocks");
    }

    /// A single container sitting at a high key (value 70,000) names a signer index of 65,536 or
    /// more, which no committee can hold. Pinned by the highest-key rule: even a one-block bitmap
    /// is refused when it reaches past the first block.
    #[test]
    fn signer_bitmap_single_high_block_is_rejected() {
        let bitmap = roaring::RoaringBitmap::from_iter([70_000_u32]);
        let mut bytes = Vec::new();
        bitmap.serialize_into(&mut bytes).expect("serialize bitmap");
        assert_certificate_rejected(&certificate_bytes_with_bitmap(&bytes), "single high block");
    }

    /// An empty signer bitmap (no block at all) must still decode.
    #[test]
    fn signer_bitmap_empty_still_decodes() {
        let bitmap = roaring::RoaringBitmap::new();
        let mut bytes = Vec::new();
        bitmap.serialize_into(&mut bytes).expect("serialize bitmap");
        let cert_bytes = certificate_bytes_with_bitmap(&bytes);
        let cert: Certificate = bcs::from_bytes(&cert_bytes).expect("empty bitmap must decode");
        assert_eq!(cert.signed_authorities().len(), 0);
    }

    /// Epoch certificates use the same signer-bitmap decoder; a two-block bitmap must be refused
    /// there as well.
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

    // --- Unit tests for the header bound itself ----------------------------------------------

    /// The bound rejects a run-header bitmap that declares more than one block (count checked from
    /// the header alone, before any container is read).
    #[test]
    fn bound_rejects_run_header_with_two_blocks() {
        let bytes = full_run_bitmap(2);
        assert!(check_signer_bitmap_block_bound(&bytes).is_err());
    }

    /// A single full block (one container, key 0) is within the bound.
    #[test]
    fn bound_accepts_single_full_block() {
        let bytes = full_run_bitmap(1);
        assert!(check_signer_bitmap_block_bound(&bytes).is_ok());
    }

    /// A normal small no-run bitmap (cookie 12346, one block, key 0) is within the bound.
    #[test]
    fn bound_accepts_normal_small_no_run() {
        let bitmap = roaring::RoaringBitmap::from_iter([0_u32, 1, 2]);
        let mut bytes = Vec::new();
        bitmap.serialize_into(&mut bytes).unwrap();
        assert!(check_signer_bitmap_block_bound(&bytes).is_ok());
    }

    /// An empty no-run bitmap (cookie 12346, zero blocks) is within the bound.
    #[test]
    fn bound_accepts_empty_no_run() {
        let bitmap = roaring::RoaringBitmap::new();
        let mut bytes = Vec::new();
        bitmap.serialize_into(&mut bytes).unwrap();
        assert!(check_signer_bitmap_block_bound(&bytes).is_ok());
    }

    /// A single no-run container at a high key (cookie 12346) is refused by the highest-key rule.
    #[test]
    fn bound_rejects_high_key_no_run() {
        let bitmap = roaring::RoaringBitmap::from_iter([70_000_u32]);
        let mut bytes = Vec::new();
        bitmap.serialize_into(&mut bytes).unwrap();
        assert!(check_signer_bitmap_block_bound(&bytes).is_err());
    }

    /// An unknown cookie is not a recognizable header: the bound does not decide, so it returns
    /// `Ok` and leaves rejection to the deserializer.
    #[test]
    fn bound_ignores_unknown_cookie() {
        assert!(check_signer_bitmap_block_bound(&[99, 9, 9, 9]).is_ok());
    }

    /// A byte slice too short for a header is not a recognizable header: the bound returns `Ok` and
    /// leaves rejection to the deserializer.
    #[test]
    fn bound_ignores_truncated_header() {
        assert!(check_signer_bitmap_block_bound(&[0, 1]).is_ok());
    }

    // --- Unit tests for the shared header parser ---------------------------------------------

    /// A run-header bitmap (cookie 12347) reports its container count from the cookie's upper 16
    /// bits, and its descriptions start after the cookie and the run-flag bytes.
    #[test]
    fn header_reads_run_cookie() {
        for blocks in [1_u32, 2, 8, 64] {
            let bytes = full_run_bitmap(blocks);
            let (count, desc_start) = roaring_header(&bytes).expect("recognizable header");
            assert_eq!(count, blocks);
            assert_eq!(desc_start, 4 + blocks.div_ceil(8) as usize);
        }
    }

    /// A no-run header (cookie 12346) reports the u32 count that follows the cookie, and its
    /// descriptions start right after it at byte 8.
    #[test]
    fn header_reads_no_run_cookie() {
        let bitmap = roaring::RoaringBitmap::from_iter([0_u32, 1, BLOCK as u32]);
        let mut bytes = Vec::new();
        bitmap.serialize_into(&mut bytes).unwrap();
        let (count, desc_start) = roaring_header(&bytes).expect("recognizable header");
        assert_eq!(count, 2);
        assert_eq!(desc_start, 8);
    }

    /// Bytes without a recognizable, complete portable header return `None`: a garbage cookie, a
    /// cookie cut off early, and a no-run cookie whose count word is missing.
    #[test]
    fn header_rejects_unknown_and_truncated() {
        assert!(roaring_header(&[99, 9, 9, 9]).is_none());
        assert!(roaring_header(&[0, 1]).is_none());
        assert!(roaring_header(&[0x3A, 0x30, 0, 0]).is_none());
    }
}
