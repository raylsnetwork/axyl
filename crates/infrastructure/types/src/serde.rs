//! Serialize and deserialize roaring bitmap used by certificates.

use std::fmt;

use serde::{
    de::Deserializer,
    ser::{Error as SerError, Serializer},
};
use serde_with::{DeserializeAs, SerializeAs};

/// Number of containers a serialized roaring bitmap declares, or `None` for an unknown header.
///
/// Read off the fixed header without building anything, so a caller can refuse a bitmap that
/// declares too many containers before `deserialize_from` materializes them (a run container of
/// ~10 bytes becomes an 8 KiB store once decoded).
pub fn roaring_container_count(bytes: &[u8]) -> Option<usize> {
    // Header of a bitmap without run containers: this cookie, then the container count.
    const SERIAL_COOKIE_NO_RUNCONTAINER: u32 = 12346;
    // Header of a bitmap with run containers: this cookie, then the container count minus one.
    const SERIAL_COOKIE: u16 = 12347;
    const WORD: usize = size_of::<u32>();
    let word = |at: usize| -> Option<u32> {
        Some(u32::from_le_bytes(bytes.get(at..at + WORD)?.try_into().ok()?))
    };
    let cookie = word(0)?;
    if cookie as u16 == SERIAL_COOKIE {
        Some((cookie >> u16::BITS) as usize + 1)
    } else if cookie == SERIAL_COOKIE_NO_RUNCONTAINER {
        Some(word(WORD)? as usize)
    } else {
        None
    }
}

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
    use crate::Certificate;

    /// Regression for issue #55. This byte sequence — found by the
    /// `bcs_roundtrip` fuzz target — decodes into a `Certificate` whose
    /// `signed_authorities` roaring bitmap contains an empty container (via
    /// roaring's checked deserializer accepting a run-container with zero runs).
    /// Before the normalize-on-deserialize fix in `RoaringBitmapSerde`,
    /// re-encoding this certificate panicked with "attempt to subtract with
    /// overflow" in roaring's serializer.
    ///
    /// The fuzzer's header had a random epoch with bit 31 set, which the
    /// `HeaderParentSigners` layout reads as "signer claim follows". Byte 39
    /// (the epoch's top byte) was changed from 0xff to 0x7f so the input stays a
    /// legacy-layout certificate; the roaring bytes that trigger the crash are
    /// untouched.
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
}
