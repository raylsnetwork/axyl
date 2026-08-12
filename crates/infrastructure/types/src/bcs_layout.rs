//! Partial BCS decoding: skip the fields you don't need, read the ones you do, without fully
//! deserializing the value.
//!
//! BCS is positional, so a `#[derive(Serialize)]` type's wire layout is its field declaration
//! order. [`BcsCursor`] forward-scans that layout with [`BcsLayout::skip`] (advance past a field)
//! and [`BcsRead::read`] (decode one).
//!
//! Each `BcsLayout` impl is pinned by a skip-consumes-exact test, so adding a field without
//! updating the impl fails fast.

use alloy::primitives::B256;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use thiserror::Error;

/// Errors surfaced by the partial BCS decoder.
#[derive(Debug, Error)]
pub enum BcsLayoutError {
    /// Cursor reached end of input before the layout was complete.
    #[error("truncated BCS input: needed {needed} bytes, had {available}")]
    Truncated {
        /// Bytes the operation needed.
        needed: usize,
        /// Bytes that remained in the cursor.
        available: usize,
    },

    /// ULEB128 sequence exceeded 64 bits.
    #[error("ULEB128 length exceeded u64")]
    OverflowUleb128,

    /// Decoded length cannot fit in `usize` on this platform.
    #[error("BCS length {len} cannot fit in usize")]
    LengthOverflow {
        /// Decoded ULEB128 value.
        len: u64,
    },

    /// Enum variant tag is outside the type's valid range.
    #[error("unknown enum variant tag {tag} for {type_name}")]
    UnknownVariant {
        /// Decoded variant tag.
        tag: u64,
        /// Source type for the error message.
        type_name: &'static str,
    },

    /// Boolean byte was neither 0 nor 1.
    #[error("invalid bool byte {byte}")]
    InvalidBool {
        /// Offending byte.
        byte: u8,
    },
}

/// Advance past the BCS encoding of `Self` without constructing it.
pub trait BcsLayout {
    /// Advance `cursor` past one BCS-encoded instance of `Self`.
    fn skip(cursor: &mut BcsCursor<'_>) -> Result<(), BcsLayoutError>;
}

/// Read and construct one BCS-encoded instance of `Self`.
pub trait BcsRead: BcsLayout + Sized {
    /// Decode one instance of `Self` from `cursor`, advancing past it.
    fn read(cursor: &mut BcsCursor<'_>) -> Result<Self, BcsLayoutError>;
}

/// Types with a constant byte width under BCS. Lets a fixed-width field be
/// skipped in one `take` instead of walking its sub-fields.
pub trait BcsConstSize: BcsLayout {
    /// Byte width of one BCS-encoded instance.
    const BCS_SIZE: usize;
}

/// Cursor over a BCS-encoded byte slice. Methods consume from the front.
#[derive(Debug, Clone)]
pub struct BcsCursor<'a> {
    bytes: &'a [u8],
}

impl<'a> BcsCursor<'a> {
    /// Create a new cursor over `bytes`.
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    /// Bytes not yet consumed.
    pub fn remaining(&self) -> &'a [u8] {
        self.bytes
    }

    /// Number of bytes not yet consumed.
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// `true` once the cursor has consumed all of its input.
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Skip past a BCS-encoded `T`. Chainable.
    pub fn skip<T: BcsLayout>(&mut self) -> Result<&mut Self, BcsLayoutError> {
        T::skip(self)?;
        Ok(self)
    }

    /// Decode one BCS-encoded `T`, advancing the cursor.
    pub fn read<T: BcsRead>(&mut self) -> Result<T, BcsLayoutError> {
        T::read(self)
    }

    /// Skip past a BCS-encoded `T`, returning the bytes it occupied.
    ///
    /// Lets a caller hash or copy a field's exact wire range without
    /// re-stating the field's layout.
    pub fn take_span<T: BcsLayout>(&mut self) -> Result<&'a [u8], BcsLayoutError> {
        let start = self.bytes;
        T::skip(self)?;
        Ok(&start[..start.len() - self.bytes.len()])
    }

    /// Consume `n` bytes and return a borrowed slice.
    pub fn take(&mut self, n: usize) -> Result<&'a [u8], BcsLayoutError> {
        let (head, tail) = self
            .bytes
            .split_at_checked(n)
            .ok_or(BcsLayoutError::Truncated { needed: n, available: self.bytes.len() })?;
        self.bytes = tail;
        Ok(head)
    }

    /// Consume `N` bytes and return them as a fixed-size array.
    pub fn take_array<const N: usize>(&mut self) -> Result<[u8; N], BcsLayoutError> {
        Ok(self.take(N)?.try_into().expect("split_at_checked guarantees the length"))
    }

    /// Consume one ULEB128 value from the front of the cursor.
    pub fn read_uleb128(&mut self) -> Result<u64, BcsLayoutError> {
        let mut value: u64 = 0;
        let mut shift: u32 = 0;
        loop {
            let byte =
                *self.bytes.first().ok_or(BcsLayoutError::Truncated { needed: 1, available: 0 })?;
            self.bytes = &self.bytes[1..];
            value |= u64::from(byte & 0x7F) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
            shift = shift.checked_add(7).ok_or(BcsLayoutError::OverflowUleb128)?;
            if shift >= 64 {
                return Err(BcsLayoutError::OverflowUleb128);
            }
        }
    }

    /// Read a ULEB128 length and convert it to `usize` (or fail on overflow).
    pub fn read_len(&mut self) -> Result<usize, BcsLayoutError> {
        let raw = self.read_uleb128()?;
        usize::try_from(raw).map_err(|_| BcsLayoutError::LengthOverflow { len: raw })
    }
}

// ----- primitive impls -------------------------------------------------------

// Integer fields are skipped by their fixed little-endian width.
macro_rules! impl_int_skip {
    ($($ty:ty: $size:expr),* $(,)?) => {$(
        impl BcsLayout for $ty {
            #[inline]
            fn skip(c: &mut BcsCursor<'_>) -> Result<(), BcsLayoutError> {
                c.take($size).map(drop)
            }
        }
    )*};
}

impl_int_skip! {
    u16: 2,
    u32: 4,
    u64: 8,
}

// Decoded integers: a leader's round/epoch (`u32`) and a consensus block number (`u64`).
impl BcsRead for u32 {
    #[inline]
    fn read(c: &mut BcsCursor<'_>) -> Result<Self, BcsLayoutError> {
        Ok(u32::from_le_bytes(c.take_array::<4>()?))
    }
}
impl BcsRead for u64 {
    #[inline]
    fn read(c: &mut BcsCursor<'_>) -> Result<Self, BcsLayoutError> {
        Ok(u64::from_le_bytes(c.take_array::<8>()?))
    }
}

// `u64`'s const width backs `BlockNumHash::BCS_SIZE` below.
impl BcsConstSize for u64 {
    const BCS_SIZE: usize = 8;
}

impl BcsLayout for bool {
    #[inline]
    fn skip(c: &mut BcsCursor<'_>) -> Result<(), BcsLayoutError> {
        c.take(1).map(drop)
    }
}
impl BcsRead for bool {
    #[inline]
    fn read(c: &mut BcsCursor<'_>) -> Result<Self, BcsLayoutError> {
        match c.take_array::<1>()?[0] {
            0 => Ok(false),
            1 => Ok(true),
            byte => Err(BcsLayoutError::InvalidBool { byte }),
        }
    }
}

impl<const N: usize> BcsLayout for [u8; N] {
    #[inline]
    fn skip(c: &mut BcsCursor<'_>) -> Result<(), BcsLayoutError> {
        c.take(N).map(drop)
    }
}
impl<const N: usize> BcsRead for [u8; N] {
    #[inline]
    fn read(c: &mut BcsCursor<'_>) -> Result<Self, BcsLayoutError> {
        c.take_array::<N>()
    }
}

// B256 encodes via `serialize_bytes`: ULEB128(N) + N raw bytes. N is constant
// and < 128, so BCS_SIZE is 1 + N.
impl BcsLayout for B256 {
    #[inline]
    fn skip(c: &mut BcsCursor<'_>) -> Result<(), BcsLayoutError> {
        let len = c.read_len()?;
        if len != 32 {
            return Err(BcsLayoutError::Truncated { needed: 32, available: len });
        }
        c.take(32).map(drop)
    }
}
impl BcsRead for B256 {
    #[inline]
    fn read(c: &mut BcsCursor<'_>) -> Result<Self, BcsLayoutError> {
        let len = c.read_len()?;
        if len != 32 {
            return Err(BcsLayoutError::Truncated { needed: 32, available: len });
        }
        Ok(B256::from_slice(c.take(32)?))
    }
}
impl BcsConstSize for B256 {
    const BCS_SIZE: usize = 1 + 32;
}

// alloy::eips::BlockNumHash is `{ number: u64, hash: B256 }`, fixed 41 bytes.
impl BcsLayout for alloy::eips::BlockNumHash {
    #[inline]
    fn skip(c: &mut BcsCursor<'_>) -> Result<(), BcsLayoutError> {
        c.take(Self::BCS_SIZE).map(drop)
    }
}
impl BcsConstSize for alloy::eips::BlockNumHash {
    const BCS_SIZE: usize = u64::BCS_SIZE + B256::BCS_SIZE;
}

// ----- container impls -------------------------------------------------------

impl<T: BcsLayout> BcsLayout for Vec<T> {
    fn skip(c: &mut BcsCursor<'_>) -> Result<(), BcsLayoutError> {
        let n = c.read_len()?;
        for _ in 0..n {
            T::skip(c)?;
        }
        Ok(())
    }
}

impl<T: BcsLayout> BcsLayout for Option<T> {
    fn skip(c: &mut BcsCursor<'_>) -> Result<(), BcsLayoutError> {
        match c.take_array::<1>()?[0] {
            0 => Ok(()),
            1 => T::skip(c),
            byte => Err(BcsLayoutError::InvalidBool { byte }),
        }
    }
}

impl<T: BcsLayout> BcsLayout for BTreeSet<T> {
    fn skip(c: &mut BcsCursor<'_>) -> Result<(), BcsLayoutError> {
        let n = c.read_len()?;
        for _ in 0..n {
            T::skip(c)?;
        }
        Ok(())
    }
}

impl<K: BcsLayout, V: BcsLayout> BcsLayout for BTreeMap<K, V> {
    fn skip(c: &mut BcsCursor<'_>) -> Result<(), BcsLayoutError> {
        let n = c.read_len()?;
        for _ in 0..n {
            K::skip(c)?;
            V::skip(c)?;
        }
        Ok(())
    }
}

impl<K: BcsLayout, V: BcsLayout> BcsLayout for HashMap<K, V> {
    fn skip(c: &mut BcsCursor<'_>) -> Result<(), BcsLayoutError> {
        let n = c.read_len()?;
        for _ in 0..n {
            K::skip(c)?;
            V::skip(c)?;
        }
        Ok(())
    }
}

impl<A: BcsLayout, B: BcsLayout> BcsLayout for (A, B) {
    fn skip(c: &mut BcsCursor<'_>) -> Result<(), BcsLayoutError> {
        A::skip(c)?;
        B::skip(c)
    }
}

/// Skip a BCS `bytes` field: ULEB128-prefixed raw byte slice (e.g. `Vec<u8>`,
/// `String`, RoaringBitmap via `RoaringBitmapSerde`). Used by hand-written
/// impls for types serialized as raw bytes.
pub fn skip_bytes(c: &mut BcsCursor<'_>) -> Result<(), BcsLayoutError> {
    let n = c.read_len()?;
    c.take(n).map(drop)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::B256;
    use serde::Serialize;

    fn enc<T: Serialize>(v: &T) -> Vec<u8> {
        bcs::to_bytes(v).unwrap()
    }

    /// Generic round-trip: encode `v` with bcs, then `T::skip` must consume the
    /// whole buffer.
    fn assert_skip_exact<T: BcsLayout + Serialize>(v: &T) {
        let bytes = enc(v);
        let mut c = BcsCursor::new(&bytes);
        T::skip(&mut c).unwrap();
        assert!(
            c.is_empty(),
            "{} bytes left after skipping {} (encoded {})",
            c.len(),
            std::any::type_name::<T>(),
            bytes.len()
        );
    }

    fn assert_read_roundtrip<T: BcsRead + Serialize + PartialEq + std::fmt::Debug>(v: &T) {
        let bytes = enc(v);
        let mut c = BcsCursor::new(&bytes);
        let decoded = T::read(&mut c).unwrap();
        assert_eq!(&decoded, v);
        assert!(c.is_empty());
    }

    #[test]
    fn scalar_round_trip() {
        assert_read_roundtrip(&0u32);
        assert_read_roundtrip(&u32::MAX);
        assert_read_roundtrip(&true);
        assert_read_roundtrip(&false);
    }

    #[test]
    fn b256_skip_consumes_exact() {
        assert_skip_exact(&B256::repeat_byte(0xAB));
    }

    #[test]
    fn b256_read_round_trips() {
        assert_read_roundtrip(&B256::repeat_byte(0xAB));
        assert_read_roundtrip(&B256::ZERO);
    }

    #[test]
    fn vec_skip_consumes_exact() {
        let v: Vec<u32> = (0..1000).collect();
        assert_skip_exact(&v);
    }

    #[test]
    fn vec_of_vec_skip_consumes_exact() {
        let v: Vec<Vec<u32>> = (0..50).map(|i| vec![i as u32; 1 + i as usize * 13]).collect();
        assert_skip_exact(&v);
    }

    #[test]
    fn option_skip_consumes_exact() {
        let some: Option<u64> = Some(42);
        let none: Option<u64> = None;
        assert_skip_exact(&some);
        assert_skip_exact(&none);
    }

    #[test]
    fn btreeset_skip_consumes_exact() {
        let s: BTreeSet<B256> = (0..32).map(|i| B256::repeat_byte(i as u8)).collect();
        assert_skip_exact(&s);
    }

    #[test]
    fn btreemap_skip_consumes_populated() {
        let m: BTreeMap<u32, u64> = (0..16).map(|i| (i, u64::from(i) * 7)).collect();
        assert_skip_exact(&m);
    }

    #[test]
    fn block_num_hash_skip_consumes_exact() {
        let bnh = alloy::eips::BlockNumHash { number: 12345, hash: B256::repeat_byte(0x42) };
        assert_skip_exact(&bnh);
        assert_eq!(alloy::eips::BlockNumHash::BCS_SIZE, 41);
    }

    #[test]
    fn skip_bytes_consumes_byte_blob() {
        let v: Vec<u8> = (0..200).map(|i| i as u8).collect();
        let bytes = enc(&v);
        let mut c = BcsCursor::new(&bytes);
        skip_bytes(&mut c).unwrap();
        assert!(c.is_empty());
    }

    #[test]
    fn read_uleb128_decodes_multibyte() {
        // BCS lengths/tags are ULEB128; integer values are not, so hand-craft
        // the byte sequences the decoder must accept.
        let cases: &[(&[u8], u64)] = &[
            (&[0x00], 0),
            (&[0x7F], 127),
            (&[0x80, 0x01], 128),
            (&[0xAC, 0x02], 300),
            (&[0x80, 0x80, 0x01], 16_384),
            (&[0xFF, 0xFF, 0xFF, 0xFF, 0x0F], u32::MAX as u64),
        ];
        for (bytes, expected) in cases {
            let mut c = BcsCursor::new(bytes);
            assert_eq!(c.read_uleb128().unwrap(), *expected);
            assert!(c.is_empty());
        }
    }

    #[test]
    fn read_uleb128_rejects_overlong() {
        // 11 continuation bytes overruns the 64-bit budget.
        let bytes = [0x80u8; 11];
        let mut c = BcsCursor::new(&bytes);
        assert!(matches!(c.read_uleb128(), Err(BcsLayoutError::OverflowUleb128)));
    }
}
