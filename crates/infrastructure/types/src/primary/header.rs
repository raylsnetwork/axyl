//! Consensus header: a primary's per-round proposal of worker batches and parent certificates.

use crate::{
    bcs_layout::{skip_bytes, BcsCursor, BcsLayout, BcsLayoutError},
    crypto, encode,
    error::{HeaderError, HeaderResult},
    now,
    serde::{deserialize_normalized, RoaringBitmapSerde},
    AuthorityIdentifier, Batch, BlockHash, BlockNumHash, CertificateDigest, Committee, Digest,
    Epoch, Hash, Round, TimestampSec, VoteDigest, WorkerId,
};
use derive_builder::Builder;
use indexmap::IndexMap;
use once_cell::sync::OnceCell;
use roaring::RoaringBitmap;
use serde::{
    de::{self, SeqAccess, Visitor},
    ser::{Error as _, SerializeStruct},
    Deserialize, Deserializer, Serialize, Serializer,
};
use serde_with::{serde_as, DeserializeAs, SerializeAs};
use std::{collections::BTreeSet, fmt};

/// Bit set in the wire `epoch` field of a header that carries `own_parent_signers`.
///
/// The header layout is versioned in-band so every decoder (network, consensus DB, cold archive
/// projections) tells the two layouts apart from the bytes alone, with no activation table: a
/// v1 header encodes exactly as it always did, a v2 header sets this bit and appends the signer
/// bitmap. Real epochs never approach `2^31`, and [`Header::validate`] rejects one that does.
pub const HEADER_SIGNERS_EPOCH_FLAG: Epoch = 1 << 31;

/// `Header` type for consensus layer.
#[derive(Builder, Clone, Default)]
#[builder(pattern = "owned", build_fn(skip))]
pub struct Header {
    /// Primary that created the header. Must be the same primary that broadcasted the header.
    pub author: AuthorityIdentifier,
    /// The round for this header.
    pub round: Round,
    /// The epoch this Header was created in.
    pub epoch: Epoch,
    /// The timestamp for when the header was requested to be created.
    pub created_at: TimestampSec,
    /// Batch digests proposed by this header, each mapped to the worker that sealed it.
    pub payload: IndexMap<BlockHash, WorkerId>,
    /// Parent certificates for this Header.
    pub parents: BTreeSet<CertificateDigest>,
    /// Hash and number of the latest execution block known when the header was built.
    ///
    /// Carrying it in a signed header lets peers validate the author's execution result along with
    /// the proposal.
    pub latest_execution_block: BlockNumHash,
    /// Signers of the author's own parent certificate (its round `round - 1` certificate), as the
    /// bitmap of committee indices in BLS-key order that certificate carries.
    ///
    /// A certificate's signer set is not part of any commitment (the DAG keys certificates by
    /// header digest and the committed sub-dag hashes those digests only), so two honest nodes can
    /// hold the same certificate with different, equally valid signer subsets. Restating the set
    /// here puts it inside a signed, committed header that every node holds byte-identically,
    /// which is what lets the reward tally credit signers deterministically. Voters refuse a
    /// header whose claim differs from the parent certificate they hold.
    ///
    /// `None` before the `HeaderParentSigners` fork; `Some` (possibly empty, when the author's
    /// previous certificate is not among the parents) once it is active.
    pub own_parent_signers: Option<RoaringBitmap>,
    /// Cached digest. Off-wire, so a decoded header recomputes it on first use.
    pub digest: OnceCell<HeaderDigest>,
}

impl Header {
    /// Creates a header and seals its digest.
    pub fn new(
        author: AuthorityIdentifier,
        round: Round,
        epoch: Epoch,
        payload: IndexMap<BlockHash, WorkerId>,
        parents: BTreeSet<CertificateDigest>,
        latest_execution_block: BlockNumHash,
        own_parent_signers: Option<RoaringBitmap>,
    ) -> Self {
        let header = Self {
            author,
            round,
            epoch,
            created_at: now(),
            payload,
            parents,
            digest: OnceCell::default(),
            latest_execution_block,
            own_parent_signers,
        };
        let digest = Hash::digest(&header);
        header.digest.set(digest).expect("digest oncecell empty for new header");
        header
    }

    /// Returns the header digest, computing and caching it on first use.
    pub fn digest(&self) -> HeaderDigest {
        *self.digest.get_or_init(|| Hash::digest(self))
    }

    /// Validates the header against the current committee.
    ///
    /// The digest covers the whole header, so the execution-layer fields are verified with it.
    pub fn validate(&self, committee: &Committee) -> HeaderResult<()> {
        // Ensure the header is from the correct epoch.
        if self.epoch != committee.epoch() {
            return Err(HeaderError::InvalidEpoch { theirs: self.epoch, ours: committee.epoch() });
        }

        // Ensure we don't have too many parents.
        if self.parents.len() > committee.size() {
            return Err(HeaderError::TooManyParents(self.parents.len(), committee.size()));
        }

        // Hash once: a decoded header's cell is empty (the digest is off-wire), so seeding it
        // with the computed value fills the cache, while a cell set before a later mutation still
        // fails the comparison.
        let computed = Hash::digest(self);
        if *self.digest.get_or_init(|| computed) != computed {
            return Err(HeaderError::InvalidHeaderDigest);
        }

        // Ensure authority is in the current committee.
        committee
            .authority(&self.author)
            .ok_or(HeaderError::UnknownAuthority(self.author.to_string()))?;

        // Ensure all worker ids are correct.
        for worker_id in self.payload.values() {
            if *worker_id as usize >= committee.number_of_workers() {
                return Err(HeaderError::UnknownWorkerId);
            }
        }

        // A claimed signer must be a committee index. Whether the claim is allowed at all, and
        // whether it matches the parent certificate, is checked by the voter against its own copy
        // of that certificate and the fork state (`vote_inner`).
        if let Some(max) = self.own_parent_signers.as_ref().and_then(RoaringBitmap::max) {
            if max as usize >= committee.size() {
                return Err(HeaderError::OwnParentSignerOutOfRange {
                    max,
                    committee_size: committee.size(),
                });
            }
        }

        Ok(())
    }

    /// The [AuthorityIdentifier] that produced the header.
    pub fn author(&self) -> &AuthorityIdentifier {
        &self.author
    }
    /// The [Round] for the header.
    pub fn round(&self) -> Round {
        self.round
    }
    /// The [Epoch] for the header.
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }
    /// The [TimestampSec] for the header.
    pub fn created_at(&self) -> &TimestampSec {
        &self.created_at
    }
    /// The payload for the header.
    pub fn payload(&self) -> &IndexMap<BlockHash, WorkerId> {
        &self.payload
    }
    /// The parents for the header.
    pub fn parents(&self) -> &BTreeSet<CertificateDigest> {
        &self.parents
    }
    /// The signers the author claims for its own parent certificate, `None` for a pre-fork header.
    pub fn own_parent_signers(&self) -> Option<&RoaringBitmap> {
        self.own_parent_signers.as_ref()
    }

    /// Replaces the header's payload. Test-only.
    pub fn update_payload_for_test(&mut self, new_payload: IndexMap<BlockHash, WorkerId>) {
        self.payload = new_payload;
    }

    /// Replaces the header's round. Test-only.
    pub fn update_round_for_test(&mut self, new_round: Round) {
        self.round = new_round;
    }

    /// Clears the header's parents. Test-only.
    pub fn clear_parents_for_test(&mut self) {
        self.parents.clear();
    }

    /// The nonce of this header used during execution.
    pub fn nonce(&self) -> u64 {
        crate::nonce::pack_nonce(self.epoch, self.round)
    }

    /// Splits a wire `epoch` field into the epoch and whether the header carries
    /// `own_parent_signers`.
    pub fn split_wire_epoch(wire_epoch: Epoch) -> (Epoch, bool) {
        (wire_epoch & !HEADER_SIGNERS_EPOCH_FLAG, wire_epoch & HEADER_SIGNERS_EPOCH_FLAG != 0)
    }

    /// Projects `author`, `round`, `epoch` and `own_parent_signers` out of one BCS-encoded header
    /// (the span [`crate::Certificate::skip_with_header_span`] returns), without decoding the
    /// payload, parents or execution anchor.
    pub fn project_wire(span: &[u8]) -> Result<HeaderWireMeta, BcsLayoutError> {
        let mut c = BcsCursor::new(span);
        let author = c.read::<AuthorityIdentifier>()?;
        let round = c.read::<Round>()?;
        let (epoch, has_signers) = Self::split_wire_epoch(c.read::<Epoch>()?);
        let own_parent_signers = if has_signers {
            c.skip::<TimestampSec>()?
                .skip::<Vec<(BlockHash, WorkerId)>>()?
                .skip::<BTreeSet<CertificateDigest>>()?
                .skip::<BlockNumHash>()?;
            let len = c.read_len()?;
            let bytes = c.take(len)?;
            Some(deserialize_normalized(bytes).map_err(|e| BcsLayoutError::InvalidValue {
                type_name: "RoaringBitmap",
                reason: e.to_string(),
            })?)
        } else {
            None
        };
        Ok(HeaderWireMeta { author, round, epoch, own_parent_signers })
    }
}

/// The header fields [`Header::project_wire`] reads off a BCS header span.
#[derive(Debug, Clone)]
pub struct HeaderWireMeta {
    /// Primary that created the header.
    pub author: AuthorityIdentifier,
    /// The round for this header.
    pub round: Round,
    /// The epoch this header was created in, flag bit cleared.
    pub epoch: Epoch,
    /// The author's claim for its own parent certificate's signers, `None` for a v1 header.
    pub own_parent_signers: Option<RoaringBitmap>,
}

impl From<Header> for CertificateDigest {
    fn from(value: Header) -> Self {
        Self::new(value.digest().into())
    }
}

impl HeaderBuilder {
    /// Builds the header and seals its digest. Test-only: `latest_execution_block` defaults to
    /// zero, which a real proposal must never carry.
    pub fn build(self) -> Header {
        let h = Header {
            author: self.author.expect("author set for header builder"),
            round: self.round.expect("round set for header builder"),
            epoch: self.epoch.expect("epoch set for header builder"),
            created_at: self.created_at.unwrap_or(0),
            payload: self.payload.unwrap_or_default(),
            parents: self.parents.expect("parents set for header builder"),
            digest: OnceCell::default(),
            latest_execution_block: self.latest_execution_block.unwrap_or_default(),
            own_parent_signers: self.own_parent_signers.flatten(),
        };

        h.digest.set(Hash::digest(&h)).expect("digest oncecell empty for new header");

        h
    }

    /// Adds `batch` to the payload under `worker_id`.
    pub fn with_payload_batch(mut self, batch: Batch, worker_id: WorkerId) -> Self {
        if self.payload.is_none() {
            self.payload = Some(Default::default());
        }
        let payload = self.payload.as_mut().unwrap();

        payload.insert(batch.digest(), worker_id);

        self
    }
}

/// The slice of bytes for the header's digest.
#[derive(
    Clone, Copy, Default, PartialEq, Eq, std::hash::Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct HeaderDigest(Digest<{ crypto::DIGEST_LENGTH }>);

impl HeaderDigest {
    /// Creates a header digest from raw bytes.
    pub fn new(digest: [u8; crypto::DIGEST_LENGTH]) -> Self {
        HeaderDigest(Digest { digest })
    }
}

impl From<HeaderDigest> for Digest<{ crypto::DIGEST_LENGTH }> {
    fn from(hd: HeaderDigest) -> Self {
        Digest::new(hd.0.digest)
    }
}

impl From<HeaderDigest> for [u8; crypto::DIGEST_LENGTH] {
    fn from(hd: HeaderDigest) -> Self {
        hd.0.digest
    }
}

impl AsRef<[u8]> for HeaderDigest {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl From<HeaderDigest> for VoteDigest {
    fn from(value: HeaderDigest) -> Self {
        VoteDigest::new(value.0.digest)
    }
}

impl fmt::Debug for HeaderDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for HeaderDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl Hash<{ crypto::DIGEST_LENGTH }> for Header {
    type TypedDigest = HeaderDigest;

    fn digest(&self) -> HeaderDigest {
        let mut hasher = crypto::DefaultHashFunction::new();
        hasher.update(encode(&self).as_ref());
        HeaderDigest(Digest { digest: hasher.finalize().into() })
    }
}

impl fmt::Debug for Header {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: B{}(v{}, e{}, {}wbs, exec: {:?}, signers: {:?})",
            self.digest(),
            self.round(),
            self.author(),
            self.epoch(),
            self.payload().len(),
            self.latest_execution_block,
            self.own_parent_signers.as_ref().map(|s| s.iter().collect::<Vec<_>>()),
        )
    }
}

impl fmt::Display for Header {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "B{}({})", self.round(), self.author())
    }
}

impl PartialEq for Header {
    fn eq(&self, other: &Self) -> bool {
        self.digest() == other.digest()
    }
}

// ----- Wire format -----
//
// Binary (BCS) layout, v1: `author, round, epoch, created_at, payload, parents,
// latest_execution_block`, byte-identical to the derived encoding the header always had.
// v2 (`own_parent_signers` is `Some`): the same fields with `HEADER_SIGNERS_EPOCH_FLAG` set in
// `epoch`, then the signer bitmap as roaring bytes. `digest` is off-wire. Keep `BcsLayout`,
// `project_wire` and the human-readable form below in lockstep with the struct.

const HEADER_FIELDS: &[&str] = &[
    "author",
    "round",
    "epoch",
    "created_at",
    "payload",
    "parents",
    "latest_execution_block",
    "own_parent_signers",
];

/// `payload` on the wire: a sequence of `(digest, worker)` pairs in insertion order (what
/// `indexmap::map::serde_seq` emits).
struct PayloadSeq<'a>(&'a IndexMap<BlockHash, WorkerId>);

impl Serialize for PayloadSeq<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.iter())
    }
}

/// A roaring bitmap field, serialized through [`RoaringBitmapSerde`].
struct SignerBytes<'a>(&'a RoaringBitmap);

impl Serialize for SignerBytes<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        RoaringBitmapSerde::serialize_as(self.0, serializer)
    }
}

struct SignerBytesOwned(RoaringBitmap);

impl<'de> Deserialize<'de> for SignerBytesOwned {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        RoaringBitmapSerde::deserialize_as(deserializer).map(Self)
    }
}

/// Human-readable (JSON/YAML) form: named fields, bitmap as base58, and the signer field only
/// present when set so pre-fork documents are unchanged.
#[serde_as]
#[derive(Serialize, Deserialize)]
struct HeaderHuman {
    author: AuthorityIdentifier,
    round: Round,
    epoch: Epoch,
    created_at: TimestampSec,
    #[serde(with = "indexmap::map::serde_seq")]
    payload: IndexMap<BlockHash, WorkerId>,
    parents: BTreeSet<CertificateDigest>,
    latest_execution_block: BlockNumHash,
    #[serde_as(as = "Option<RoaringBitmapSerde>")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    own_parent_signers: Option<RoaringBitmap>,
}

impl Serialize for Header {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.epoch & HEADER_SIGNERS_EPOCH_FLAG != 0 {
            return Err(S::Error::custom(format!(
                "header epoch {} collides with the wire layout flag",
                self.epoch
            )));
        }
        if serializer.is_human_readable() {
            let human = HeaderHuman {
                author: self.author.clone(),
                round: self.round,
                epoch: self.epoch,
                created_at: self.created_at,
                payload: self.payload.clone(),
                parents: self.parents.clone(),
                latest_execution_block: self.latest_execution_block,
                own_parent_signers: self.own_parent_signers.clone(),
            };
            return human.serialize(serializer);
        }

        let has_signers = self.own_parent_signers.is_some();
        let wire_epoch =
            if has_signers { self.epoch | HEADER_SIGNERS_EPOCH_FLAG } else { self.epoch };
        let mut s = serializer.serialize_struct("Header", if has_signers { 8 } else { 7 })?;
        s.serialize_field("author", &self.author)?;
        s.serialize_field("round", &self.round)?;
        s.serialize_field("epoch", &wire_epoch)?;
        s.serialize_field("created_at", &self.created_at)?;
        s.serialize_field("payload", &PayloadSeq(&self.payload))?;
        s.serialize_field("parents", &self.parents)?;
        s.serialize_field("latest_execution_block", &self.latest_execution_block)?;
        if let Some(signers) = &self.own_parent_signers {
            s.serialize_field("own_parent_signers", &SignerBytes(signers))?;
        }
        s.end()
    }
}

impl<'de> Deserialize<'de> for Header {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            let human = HeaderHuman::deserialize(deserializer)?;
            if human.epoch & HEADER_SIGNERS_EPOCH_FLAG != 0 {
                return Err(de::Error::custom("header epoch collides with the wire layout flag"));
            }
            return Ok(Header {
                author: human.author,
                round: human.round,
                epoch: human.epoch,
                created_at: human.created_at,
                payload: human.payload,
                parents: human.parents,
                latest_execution_block: human.latest_execution_block,
                own_parent_signers: human.own_parent_signers,
                digest: OnceCell::default(),
            });
        }

        struct HeaderWireVisitor;

        impl<'de> Visitor<'de> for HeaderWireVisitor {
            type Value = Header;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a BCS-encoded consensus header")
            }

            // Reads seven fields, and an eighth when the epoch field carries the layout flag.
            // BCS hands the visitor a sequence of (at most) `HEADER_FIELDS.len()` elements and
            // does not require all of them to be consumed, which is what lets the v1 layout
            // decode unchanged.
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Header, A::Error> {
                macro_rules! field {
                    ($idx:expr, $ty:ty) => {
                        seq.next_element::<$ty>()?
                            .ok_or_else(|| de::Error::invalid_length($idx, &HeaderWireVisitor))?
                    };
                }

                let author = field!(0, AuthorityIdentifier);
                let round = field!(1, Round);
                let (epoch, has_signers) = Header::split_wire_epoch(field!(2, Epoch));
                let created_at = field!(3, TimestampSec);
                let payload: IndexMap<BlockHash, WorkerId> =
                    field!(4, Vec<(BlockHash, WorkerId)>).into_iter().collect();
                let parents = field!(5, BTreeSet<CertificateDigest>);
                let latest_execution_block = field!(6, BlockNumHash);
                let own_parent_signers =
                    if has_signers { Some(field!(7, SignerBytesOwned).0) } else { None };

                Ok(Header {
                    author,
                    round,
                    epoch,
                    created_at,
                    payload,
                    parents,
                    latest_execution_block,
                    own_parent_signers,
                    digest: OnceCell::default(),
                })
            }
        }

        deserializer.deserialize_struct("Header", HEADER_FIELDS, HeaderWireVisitor)
    }
}

/// BCS layout: `author, round, epoch, created_at, payload, parents, latest_execution_block`,
/// then `own_parent_signers` (roaring bytes) iff `epoch` carries
/// [`HEADER_SIGNERS_EPOCH_FLAG`]. `digest` is off-wire. Keep in lockstep with the struct.
impl BcsLayout for Header {
    fn skip(c: &mut BcsCursor<'_>) -> Result<(), BcsLayoutError> {
        c.skip::<AuthorityIdentifier>()?.skip::<Round>()?;
        let (_, has_signers) = Header::split_wire_epoch(c.read::<Epoch>()?);
        c.skip::<TimestampSec>()?
            .skip::<Vec<(BlockHash, WorkerId)>>()?
            .skip::<BTreeSet<CertificateDigest>>()?
            .skip::<BlockNumHash>()?;
        if has_signers {
            skip_bytes(c)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{decode, BlsKeypair, CommitteeBuilder};
    use alloy::primitives::Address;
    use rand::{rngs::StdRng, SeedableRng};

    /// The derived layout the header had before `own_parent_signers` existed.
    #[derive(Serialize)]
    struct LegacyHeader<'a> {
        author: &'a AuthorityIdentifier,
        round: Round,
        epoch: Epoch,
        created_at: TimestampSec,
        #[serde(with = "indexmap::map::serde_seq")]
        payload: &'a IndexMap<BlockHash, WorkerId>,
        parents: &'a BTreeSet<CertificateDigest>,
        latest_execution_block: &'a BlockNumHash,
    }

    fn sample(own_parent_signers: Option<RoaringBitmap>) -> Header {
        let mut payload = IndexMap::new();
        payload.insert(BlockHash::repeat_byte(0xA1), 0u16);
        payload.insert(BlockHash::repeat_byte(0xA2), 1u16);
        let parents = BTreeSet::from([
            CertificateDigest::new([0x11; crypto::DIGEST_LENGTH]),
            CertificateDigest::new([0x22; crypto::DIGEST_LENGTH]),
        ]);
        let mut h = Header::new(
            AuthorityIdentifier::dummy_for_test(7),
            42,
            3,
            payload,
            parents,
            BlockNumHash::new(99, BlockHash::repeat_byte(0x5C)),
            own_parent_signers,
        );
        h.created_at = 1_700_000_000;
        h.digest = OnceCell::default();
        h
    }

    fn assert_fields_eq(a: &Header, b: &Header) {
        assert_eq!(a.author, b.author);
        assert_eq!(a.round, b.round);
        assert_eq!(a.epoch, b.epoch);
        assert_eq!(a.created_at, b.created_at);
        assert_eq!(a.payload, b.payload);
        assert_eq!(a.parents, b.parents);
        assert_eq!(a.latest_execution_block, b.latest_execution_block);
        assert_eq!(a.own_parent_signers, b.own_parent_signers);
        assert_eq!(a.digest(), b.digest());
    }

    #[test]
    fn v1_wire_bytes_and_digest_match_the_legacy_derived_layout() {
        let h = sample(None);
        let legacy = LegacyHeader {
            author: &h.author,
            round: h.round,
            epoch: h.epoch,
            created_at: h.created_at,
            payload: &h.payload,
            parents: &h.parents,
            latest_execution_block: &h.latest_execution_block,
        };
        let bytes = encode(&h);
        assert_eq!(bytes, encode(&legacy), "v1 header must encode exactly as before");

        let mut hasher = crypto::DefaultHashFunction::new();
        hasher.update(&bytes);
        let legacy_digest: [u8; crypto::DIGEST_LENGTH] = hasher.finalize().into();
        assert_eq!(<[u8; crypto::DIGEST_LENGTH]>::from(h.digest()), legacy_digest);

        let decoded: Header = decode(&bytes);
        assert_fields_eq(&h, &decoded);
        assert!(decoded.own_parent_signers.is_none());
    }

    #[test]
    fn v2_wire_roundtrip_sets_the_epoch_flag_and_changes_the_digest() {
        let signers = RoaringBitmap::from_sorted_iter([0u32, 2, 3]).unwrap();
        let v2 = sample(Some(signers.clone()));
        let v1 = sample(None);

        let bytes = encode(&v2);
        // author (32) + round (4), then the epoch field with the flag in its top bit.
        let wire_epoch = Epoch::from_le_bytes(bytes[36..40].try_into().unwrap());
        assert_eq!(Header::split_wire_epoch(wire_epoch), (3, true));
        assert_ne!(v2.digest(), v1.digest());

        let decoded: Header = decode(&bytes);
        assert_fields_eq(&v2, &decoded);
        assert_eq!(decoded.own_parent_signers.as_ref(), Some(&signers));

        // an empty claim is still a v2 header
        let empty = sample(Some(RoaringBitmap::new()));
        let decoded: Header = decode(&encode(&empty));
        assert_eq!(decoded.own_parent_signers, Some(RoaringBitmap::new()));
        assert_ne!(empty.digest(), v1.digest());
    }

    #[test]
    fn skip_and_projection_consume_exactly_for_both_layouts() {
        for signers in [None, Some(RoaringBitmap::from_sorted_iter([1u32, 3]).unwrap())] {
            let h = sample(signers.clone());
            let bytes = encode(&h);
            let mut c = BcsCursor::new(&bytes);
            Header::skip(&mut c).unwrap();
            assert!(c.is_empty(), "{} bytes left after Header::skip", c.len());

            let meta = Header::project_wire(&bytes).unwrap();
            assert_eq!(meta.author, h.author);
            assert_eq!(meta.round, h.round);
            assert_eq!(meta.epoch, h.epoch);
            assert_eq!(meta.own_parent_signers, signers);
        }
    }

    #[test]
    fn human_readable_roundtrip_keeps_v1_documents_unchanged() {
        let v1 = sample(None);
        let json = serde_json::to_value(&v1).unwrap();
        assert!(json.get("own_parent_signers").is_none());
        let back: Header = serde_json::from_value(json).unwrap();
        assert_fields_eq(&v1, &back);

        let v2 = sample(Some(RoaringBitmap::from_sorted_iter([2u32]).unwrap()));
        let json = serde_json::to_value(&v2).unwrap();
        assert!(json.get("own_parent_signers").is_some());
        let back: Header = serde_json::from_value(json).unwrap();
        assert_fields_eq(&v2, &back);
    }

    #[test]
    fn epoch_colliding_with_the_flag_does_not_encode() {
        let mut h = sample(None);
        h.epoch = HEADER_SIGNERS_EPOCH_FLAG | 5;
        assert!(bcs::to_bytes(&h).is_err());
    }

    #[test]
    fn validate_rejects_a_signer_index_outside_the_committee() {
        let mut rng = StdRng::seed_from_u64(7);
        let mut builder = CommitteeBuilder::new(3);
        let mut keys = Vec::new();
        for i in 0..4u8 {
            let keypair = BlsKeypair::generate(&mut rng);
            builder.add_authority(*keypair.public(), 1, Address::with_last_byte(i + 1));
            keys.push(*keypair.public());
        }
        let committee = builder.build();
        let author = AuthorityIdentifier::from(keys[0]);

        let make = |signers: Option<RoaringBitmap>| {
            Header::new(
                author.clone(),
                1,
                3,
                IndexMap::new(),
                BTreeSet::new(),
                BlockNumHash::default(),
                signers,
            )
        };
        assert!(make(None).validate(&committee).is_ok());
        assert!(make(Some(RoaringBitmap::from_sorted_iter([0u32, 3]).unwrap()))
            .validate(&committee)
            .is_ok());
        assert!(matches!(
            make(Some(RoaringBitmap::from_sorted_iter([0u32, 4]).unwrap())).validate(&committee),
            Err(HeaderError::OwnParentSignerOutOfRange { max: 4, committee_size: 4 })
        ));
    }
}
