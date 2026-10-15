//! Consensus header: a primary's per-round proposal of worker batches and parent certificates.

use crate::{
    bcs_layout::{skip_bytes, BcsCursor, BcsLayout, BcsLayoutError},
    crypto::{self, dkg::DkgPayload},
    encode,
    error::{HeaderError, HeaderResult},
    now, try_decode, AuthorityIdentifier, Batch, BlockHash, BlockNumHash, CertificateDigest,
    Committee, Digest, Epoch, Hash, Round, TimestampSec, VoteDigest, WorkerId,
};
use derive_builder::Builder;
use indexmap::IndexMap;
use once_cell::sync::OnceCell;
use serde::{
    de::{self, SeqAccess, Visitor},
    ser::SerializeStruct,
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::{collections::BTreeSet, fmt};

/// `Header` type for consensus layer.
///
/// Serde is hand-written: see the `Serialize` impl for how the DKG payload rides inside the
/// last field without changing the bytes of headers that carry none.
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
    /// The author's deal and relayed complaints for the next epoch's threshold key. Empty before
    /// the `ThresholdCertificates` fork. Covered by the digest.
    pub dkg: DkgPayload,
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
            dkg: DkgPayload::default(),
        };
        let digest = Hash::digest(&header);
        header.digest.set(digest).expect("digest oncecell empty for new header");
        header
    }

    /// Attaches the DKG payload and re-seals the digest. Only for headers that have not been
    /// proposed yet: a proposed header is retransmitted byte for byte.
    pub fn with_dkg(mut self, dkg: DkgPayload) -> Self {
        self.dkg = dkg;
        self.digest = OnceCell::default();
        let digest = Hash::digest(&self);
        self.digest.set(digest).expect("digest oncecell just reset");
        self
    }

    /// Returns the header digest, computing and caching it on first use.
    pub fn digest(&self) -> HeaderDigest {
        *self.digest.get_or_init(|| Hash::digest(self))
    }

    /// The DKG payload for the next epoch's key.
    pub fn dkg(&self) -> &DkgPayload {
        &self.dkg
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
            dkg: self.dkg.unwrap_or_default(),
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
    /// Creates a digest from its raw bytes.
    pub fn new(digest: [u8; crypto::DIGEST_LENGTH]) -> Self {
        HeaderDigest(Digest { digest })
    }
}

impl From<HeaderDigest> for Digest<{ crypto::DIGEST_LENGTH }> {
    fn from(hd: HeaderDigest) -> Self {
        hd.0
    }
}

impl From<HeaderDigest> for [u8; crypto::DIGEST_LENGTH] {
    fn from(hd: HeaderDigest) -> Self {
        hd.0.digest
    }
}

impl AsRef<[u8]> for HeaderDigest {
    fn as_ref(&self) -> &[u8] {
        &self.0.digest
    }
}

impl From<HeaderDigest> for VoteDigest {
    fn from(value: HeaderDigest) -> Self {
        Self::new(value.0.into())
    }
}

impl fmt::Debug for HeaderDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for HeaderDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0.to_string().get(0..16).ok_or(fmt::Error)?)
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
            "{}: B{}(v{}, e{}, {}wbs, exec: {:?})",
            self.digest(),
            self.round(),
            self.author(),
            self.epoch(),
            self.payload().len(),
            self.latest_execution_block,
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

/// BCS layout: `author, round, epoch, created_at, payload, parents, latest_execution_block`,
/// where the last field is `number: u64` followed by a byte string that holds the execution hash
/// and, when present, the DKG payload (see the `Serialize` impl). `digest` is off-wire. Keep in
/// lockstep with the serde impls.
impl BcsLayout for Header {
    fn skip(c: &mut BcsCursor<'_>) -> Result<(), BcsLayoutError> {
        c.skip::<AuthorityIdentifier>()?
            .skip::<Round>()?
            .skip::<Epoch>()?
            .skip::<TimestampSec>()?
            .skip::<Vec<(BlockHash, WorkerId)>>()?
            .skip::<BTreeSet<CertificateDigest>>()?
            .skip::<u64>()?;
        skip_bytes(c)
    }
}

/// Length of the execution hash at the start of the last field's byte string.
const EXECUTION_HASH_LEN: usize = 32;

const HEADER_FIELDS: &[&str] =
    &["author", "round", "epoch", "created_at", "payload", "parents", "latest_execution_block"];

/// A byte string serialized with `serialize_bytes`, the way `B256` is encoded in BCS.
struct Bytes<'a>(&'a [u8]);

impl Serialize for Bytes<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(self.0)
    }
}

/// The last header field on the wire: the execution block number, then a byte string that
/// starts with the execution hash and continues with the BCS of the DKG payload when there is
/// one. A header without DKG data serializes exactly as `BlockNumHash` always has.
struct ExecutionRef<'a> {
    number: u64,
    bytes: &'a [u8],
}

impl Serialize for ExecutionRef<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut s = serializer.serialize_struct("BlockNumHash", 2)?;
        s.serialize_field("number", &self.number)?;
        s.serialize_field("hash", &Bytes(self.bytes))?;
        s.end()
    }
}

/// Owned byte string deserialized with `deserialize_byte_buf`.
struct ByteBuf(Vec<u8>);

impl<'de> Deserialize<'de> for ByteBuf {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ByteBufVisitor;
        impl<'de> Visitor<'de> for ByteBufVisitor {
            type Value = ByteBuf;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a byte string")
            }
            fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<ByteBuf, E> {
                Ok(ByteBuf(v.to_vec()))
            }
            fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<ByteBuf, E> {
                Ok(ByteBuf(v))
            }
        }
        deserializer.deserialize_byte_buf(ByteBufVisitor)
    }
}

struct ExecutionRefOwned {
    number: u64,
    bytes: Vec<u8>,
}

impl<'de> Deserialize<'de> for ExecutionRefOwned {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ExecutionRefVisitor;
        impl<'de> Visitor<'de> for ExecutionRefVisitor {
            type Value = ExecutionRefOwned;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an execution block reference")
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let number =
                    seq.next_element()?.ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let ByteBuf(bytes) =
                    seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;
                Ok(ExecutionRefOwned { number, bytes })
            }
        }
        deserializer.deserialize_struct("BlockNumHash", &["number", "hash"], ExecutionRefVisitor)
    }
}

/// The header as a plain struct, used for human-readable formats (JSON over RPC).
#[derive(Serialize, Deserialize)]
struct HeaderJson {
    author: AuthorityIdentifier,
    round: Round,
    epoch: Epoch,
    created_at: TimestampSec,
    #[serde(with = "indexmap::map::serde_seq")]
    payload: IndexMap<BlockHash, WorkerId>,
    parents: BTreeSet<CertificateDigest>,
    latest_execution_block: BlockNumHash,
    #[serde(default, skip_serializing_if = "DkgPayload::is_empty")]
    dkg: DkgPayload,
}

impl Serialize for Header {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            let json = HeaderJson {
                author: self.author.clone(),
                round: self.round,
                epoch: self.epoch,
                created_at: self.created_at,
                payload: self.payload.clone(),
                parents: self.parents.clone(),
                latest_execution_block: self.latest_execution_block,
                dkg: self.dkg.clone(),
            };
            return json.serialize(serializer);
        }
        let mut bytes = Vec::with_capacity(EXECUTION_HASH_LEN);
        bytes.extend_from_slice(self.latest_execution_block.hash.as_slice());
        if !self.dkg.is_empty() {
            bytes.extend_from_slice(&encode(&self.dkg));
        }
        let mut s = serializer.serialize_struct("Header", HEADER_FIELDS.len())?;
        s.serialize_field("author", &self.author)?;
        s.serialize_field("round", &self.round)?;
        s.serialize_field("epoch", &self.epoch)?;
        s.serialize_field("created_at", &self.created_at)?;
        s.serialize_field("payload", &PayloadSeq(&self.payload))?;
        s.serialize_field("parents", &self.parents)?;
        s.serialize_field(
            "latest_execution_block",
            &ExecutionRef { number: self.latest_execution_block.number, bytes: &bytes },
        )?;
        s.end()
    }
}

/// The payload as a sequence of `(digest, worker)` pairs in insertion order, the encoding
/// `indexmap::map::serde_seq` produces.
struct PayloadSeq<'a>(&'a IndexMap<BlockHash, WorkerId>);

impl Serialize for PayloadSeq<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.iter())
    }
}

impl<'de> Deserialize<'de> for Header {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            let json = HeaderJson::deserialize(deserializer)?;
            return Ok(Header {
                author: json.author,
                round: json.round,
                epoch: json.epoch,
                created_at: json.created_at,
                payload: json.payload,
                parents: json.parents,
                latest_execution_block: json.latest_execution_block,
                dkg: json.dkg,
                digest: OnceCell::default(),
            });
        }
        struct HeaderVisitor;
        impl<'de> Visitor<'de> for HeaderVisitor {
            type Value = Header;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a consensus header")
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Header, A::Error> {
                let missing =
                    |index: usize| de::Error::invalid_length(index, &"a consensus header");
                let author = seq.next_element()?.ok_or_else(|| missing(0))?;
                let round = seq.next_element()?.ok_or_else(|| missing(1))?;
                let epoch = seq.next_element()?.ok_or_else(|| missing(2))?;
                let created_at = seq.next_element()?.ok_or_else(|| missing(3))?;
                let payload: Vec<(BlockHash, WorkerId)> =
                    seq.next_element()?.ok_or_else(|| missing(4))?;
                let parents = seq.next_element()?.ok_or_else(|| missing(5))?;
                let exec: ExecutionRefOwned = seq.next_element()?.ok_or_else(|| missing(6))?;
                if exec.bytes.len() < EXECUTION_HASH_LEN {
                    return Err(de::Error::invalid_length(exec.bytes.len(), &"an execution hash"));
                }
                let (hash, extension) = exec.bytes.split_at(EXECUTION_HASH_LEN);
                let dkg = if extension.is_empty() {
                    DkgPayload::default()
                } else {
                    let dkg: DkgPayload = try_decode(extension).map_err(de::Error::custom)?;
                    if dkg.is_empty() {
                        return Err(de::Error::custom("empty DKG extension is not canonical"));
                    }
                    dkg
                };
                Ok(Header {
                    author,
                    round,
                    epoch,
                    created_at,
                    payload: payload.into_iter().collect(),
                    parents,
                    latest_execution_block: BlockNumHash {
                        number: exec.number,
                        hash: BlockHash::from_slice(hash),
                    },
                    dkg,
                    digest: OnceCell::default(),
                })
            }
        }
        deserializer.deserialize_struct("Header", HEADER_FIELDS, HeaderVisitor)
    }
}

#[cfg(test)]
mod wire_tests {
    use super::*;
    use crate::crypto::{
        dkg::{Complaint, Deal, ShareTag},
        BlsKeypair,
    };

    fn plain_header() -> Header {
        let mut payload = IndexMap::new();
        payload.insert(BlockHash::repeat_byte(1), 0u16);
        payload.insert(BlockHash::repeat_byte(2), 1u16);
        let parents =
            BTreeSet::from([CertificateDigest::new([3u8; 32]), CertificateDigest::new([4u8; 32])]);
        HeaderBuilder::default()
            .author(AuthorityIdentifier::dummy_for_test(0xAA))
            .round(4)
            .epoch(7)
            .created_at(1234)
            .payload(payload)
            .parents(parents)
            .latest_execution_block(BlockNumHash { number: 99, hash: BlockHash::repeat_byte(0x55) })
            .build()
    }

    fn dkg_payload() -> DkgPayload {
        let mut rng = rand::rng();
        let recipients: Vec<BlsKeypair> = (0..3).map(|_| BlsKeypair::generate(&mut rng)).collect();
        let keys: Vec<_> = recipients.iter().map(|k| *k.public()).collect();
        let deal = Deal::new(8, 0, 2, &keys, &mut rng).unwrap();
        let complaint =
            Complaint::new(ShareTag { epoch: 8, dealer: 1, recipient: 2 }, &recipients[2]);
        DkgPayload { deal: Some(deal), complaints: vec![complaint] }
    }

    fn legacy_bytes(h: &Header) -> Vec<u8> {
        let payload: Vec<(BlockHash, WorkerId)> = h.payload.iter().map(|(k, v)| (*k, *v)).collect();
        encode(&(
            h.author.clone(),
            h.round,
            h.epoch,
            h.created_at,
            payload,
            h.parents.clone(),
            h.latest_execution_block,
        ))
    }

    /// Encode the seven wire fields with an arbitrary byte string in the last field.
    fn with_execution_bytes(h: &Header, bytes: &[u8]) -> Vec<u8> {
        let payload: Vec<(BlockHash, WorkerId)> = h.payload.iter().map(|(k, v)| (*k, *v)).collect();
        encode(&(
            h.author.clone(),
            h.round,
            h.epoch,
            h.created_at,
            payload,
            h.parents.clone(),
            ExecutionRef { number: h.latest_execution_block.number, bytes },
        ))
    }

    fn assert_skip_consumes_exactly(bytes: &[u8]) {
        let mut c = BcsCursor::new(bytes);
        Header::skip(&mut c).unwrap();
        assert!(c.remaining().is_empty(), "layout skip must consume the whole header");
    }

    #[test]
    fn header_without_dkg_encodes_exactly_as_the_legacy_struct() {
        let h = plain_header();
        let bytes = encode(&h);
        assert_eq!(bytes, legacy_bytes(&h));
        let decoded: Header = try_decode(&bytes).unwrap();
        assert_eq!(decoded.digest(), h.digest());
        assert!(decoded.dkg().is_empty());
        assert_eq!(decoded.latest_execution_block, h.latest_execution_block);
        assert_eq!(decoded.payload, h.payload);
        assert_skip_consumes_exactly(&bytes);
    }

    #[test]
    fn header_with_dkg_round_trips_and_is_covered_by_the_digest() {
        let plain = plain_header();
        let dkg = dkg_payload();
        let h = plain.clone().with_dkg(dkg.clone());
        assert_ne!(h.digest(), plain.digest(), "the DKG payload is signed with the header");
        let bytes = encode(&h);
        // Everything up to the last field's length prefix is unchanged; the byte string then
        // grows from the 32-byte hash to hash plus extension.
        let legacy = legacy_bytes(&plain);
        assert!(bytes.starts_with(&legacy[..legacy.len() - 33]));
        assert!(bytes.len() > legacy.len());
        let decoded: Header = try_decode(&bytes).unwrap();
        assert_eq!(decoded.digest(), h.digest());
        assert_eq!(decoded.dkg(), &dkg);
        assert_eq!(decoded.latest_execution_block, h.latest_execution_block);
        assert_skip_consumes_exactly(&bytes);
    }

    #[test]
    fn non_canonical_or_short_execution_bytes_are_rejected() {
        let h = plain_header();
        let hash = h.latest_execution_block.hash;
        // An extension that decodes to an empty payload is not canonical.
        let mut empty_extension = hash.to_vec();
        empty_extension.extend_from_slice(&encode(&DkgPayload::default()));
        assert!(try_decode::<Header>(&with_execution_bytes(&h, &empty_extension)).is_err());
        // Fewer than 32 bytes cannot hold the execution hash.
        assert!(try_decode::<Header>(&with_execution_bytes(&h, &hash.as_slice()[..31])).is_err());
        // Trailing bytes after a valid extension are rejected.
        let mut trailing = hash.to_vec();
        trailing.extend_from_slice(&encode(&dkg_payload()));
        trailing.push(0);
        assert!(try_decode::<Header>(&with_execution_bytes(&h, &trailing)).is_err());
        // Garbage after the hash is rejected.
        let mut garbage = hash.to_vec();
        garbage.extend_from_slice(&[0xFF; 5]);
        assert!(try_decode::<Header>(&with_execution_bytes(&h, &garbage)).is_err());
    }

    #[test]
    fn header_json_round_trips_with_and_without_dkg() {
        for h in [plain_header(), plain_header().with_dkg(dkg_payload())] {
            let text = serde_json::to_string(&h).unwrap();
            let json: serde_json::Value = serde_json::from_str(&text).unwrap();
            assert_eq!(json["round"], 4);
            assert_eq!(json.get("dkg").is_some(), !h.dkg().is_empty());
            let decoded: Header = serde_json::from_str(&text).unwrap();
            assert_eq!(decoded.digest(), h.digest());
            assert_eq!(decoded.dkg(), h.dkg());
            assert_eq!(encode(&decoded), encode(&h));
        }
    }
}
