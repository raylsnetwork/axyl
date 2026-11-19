//! Consensus header: a primary's per-round proposal of worker batches and parent certificates.

use super::{
    fork::{epoch_is_post_activation, header_is_post_activation},
    signer_claim::SignerClaim,
};
use crate::{
    bcs_layout::{BcsCursor, BcsLayout, BcsLayoutError},
    crypto, encode,
    error::{HeaderError, HeaderResult},
    now, AuthorityIdentifier, Batch, BlockHash, BlockNumHash, CertificateDigest, Committee, Digest,
    Epoch, Hash, Round, TimestampSec, VoteDigest, WorkerId,
};
use derive_builder::Builder;
use indexmap::IndexMap;
use once_cell::sync::OnceCell;
use serde::{de, ser::SerializeStruct, Deserialize, Deserializer, Serialize, Serializer};
use std::{collections::BTreeSet, fmt};

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
    /// Cached digest. Off-wire, so a decoded header recomputes it on first use.
    pub digest: OnceCell<HeaderDigest>,
    // Invisible unless the epoch is post-activation, when our Serialize writes it.
    pub signer_claims: Vec<SignerClaim>,
}

// The canonical-bytes function is the only definition of a header's bytes.
pub fn header_canonical_bytes(header: &Header) -> Vec<u8> {
    encode(header)
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
            signer_claims: Vec::new(),
        };
        let digest = Hash::digest(&header);
        header.digest.set(digest).expect("digest oncecell empty for new header");
        header
    }

    pub fn with_signer_claims(self, signer_claims: Vec<SignerClaim>) -> Self {
        let header = Self {
            author: self.author,
            round: self.round,
            epoch: self.epoch,
            created_at: self.created_at,
            payload: self.payload,
            parents: self.parents,
            digest: OnceCell::default(),
            latest_execution_block: self.latest_execution_block,
            signer_claims,
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
            signer_claims: self.signer_claims.unwrap_or_default(),
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
        hasher.update(header_canonical_bytes(self).as_ref());
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

// BCS layout this `skip` must follow: the 7 wire fields `author, round, epoch,
// created_at, payload, parents, latest_execution_block`, followed only when this
// header's own epoch is post-activation by one more element, the `signer_claims`
// vector. `digest` is off-wire in every era: it is a cache recomputed from the
// fields that are on-wire, and the hand-written `Serialize` never writes it.
// Keep in lockstep with the struct and `HEADER_FIELDS`.
impl BcsLayout for Header {
    fn skip(c: &mut BcsCursor<'_>) -> Result<(), BcsLayoutError> {
        c.skip::<AuthorityIdentifier>()?.skip::<Round>()?;
        let epoch = c.read::<Epoch>()?;
        c.skip::<TimestampSec>()?
            .skip::<Vec<(BlockHash, WorkerId)>>()?
            .skip::<BTreeSet<CertificateDigest>>()?
            .skip::<BlockNumHash>()?;
        if epoch_is_post_activation(epoch) {
            c.skip::<Vec<SignerClaim>>()?;
        }
        Ok(())
    }
}

// The claims region cannot ride inside the derive stream (bcs budgets struct fields by
// declared count and rejects trailing input), so serde writes the 7 struct fields and
// appends `signer_claims` as an 8th element only when the header's own epoch says so.
// Declaring 8 fields in `deserialize_struct` buys the budget for that conditional read;
// bcs leaves an unconsumed budget untouched, so the cursor lands where a pre-activation
// header ends.

const HEADER_FIELDS: &[&str] = &[
    "author",
    "round",
    "epoch",
    "created_at",
    "payload",
    "parents",
    "latest_execution_block",
    "signer_claims",
];

struct PayloadSeq<'a>(&'a IndexMap<BlockHash, WorkerId>);

impl Serialize for PayloadSeq<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        indexmap::map::serde_seq::serialize(self.0, serializer)
    }
}

struct PayloadSeqOwned(IndexMap<BlockHash, WorkerId>);

impl<'de> Deserialize<'de> for PayloadSeqOwned {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        indexmap::map::serde_seq::deserialize(deserializer).map(Self)
    }
}

impl Serialize for Header {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let post = header_is_post_activation(self);
        let mut s = serializer.serialize_struct("Header", if post { 8 } else { 7 })?;
        s.serialize_field("author", &self.author)?;
        s.serialize_field("round", &self.round)?;
        s.serialize_field("epoch", &self.epoch)?;
        s.serialize_field("created_at", &self.created_at)?;
        s.serialize_field("payload", &PayloadSeq(&self.payload))?;
        s.serialize_field("parents", &self.parents)?;
        s.serialize_field("latest_execution_block", &self.latest_execution_block)?;
        if post {
            s.serialize_field("signer_claims", &self.signer_claims)?;
        }
        s.end()
    }
}

impl<'de> Deserialize<'de> for Header {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_struct("Header", HEADER_FIELDS, HeaderVisitor)
    }
}

struct HeaderVisitor;

impl<'de> de::Visitor<'de> for HeaderVisitor {
    type Value = Header;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a consensus header")
    }

    fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let author = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(0, &"author"))?;
        let round = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &"round"))?;
        let epoch: Epoch =
            seq.next_element()?.ok_or_else(|| de::Error::invalid_length(2, &"epoch"))?;
        let created_at =
            seq.next_element()?.ok_or_else(|| de::Error::invalid_length(3, &"created_at"))?;
        let payload = seq
            .next_element::<PayloadSeqOwned>()?
            .ok_or_else(|| de::Error::invalid_length(4, &"payload"))?
            .0;
        let parents =
            seq.next_element()?.ok_or_else(|| de::Error::invalid_length(5, &"parents"))?;
        let latest_execution_block = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(6, &"latest_execution_block"))?;
        let mut signer_claims = Vec::new();
        if epoch_is_post_activation(epoch) {
            signer_claims = seq
                .next_element()?
                .ok_or_else(|| de::Error::invalid_length(7, &"signer claims region"))?;
        }
        Ok(Header {
            author,
            round,
            epoch,
            created_at,
            payload,
            parents,
            digest: OnceCell::default(),
            latest_execution_block,
            signer_claims,
        })
    }

    fn visit_map<A: de::MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut author: Option<AuthorityIdentifier> = None;
        let mut round: Option<Round> = None;
        let mut epoch: Option<Epoch> = None;
        let mut created_at: Option<TimestampSec> = None;
        let mut payload: Option<IndexMap<BlockHash, WorkerId>> = None;
        let mut parents: Option<BTreeSet<CertificateDigest>> = None;
        let mut latest_execution_block: Option<crate::BlockNumHash> = None;
        let mut signer_claims: Option<Vec<SignerClaim>> = None;
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "author" => author = Some(map.next_value()?),
                "round" => round = Some(map.next_value()?),
                "epoch" => epoch = Some(map.next_value()?),
                "created_at" => created_at = Some(map.next_value()?),
                "payload" => payload = Some(map.next_value::<PayloadSeqOwned>()?.0),
                "parents" => parents = Some(map.next_value()?),
                "latest_execution_block" => latest_execution_block = Some(map.next_value()?),
                "signer_claims" => signer_claims = Some(map.next_value()?),
                field => return Err(de::Error::unknown_field(field, HEADER_FIELDS)),
            }
        }
        let epoch = epoch.ok_or_else(|| de::Error::missing_field("epoch"))?;
        if epoch_is_post_activation(epoch) && signer_claims.is_none() {
            return Err(de::Error::missing_field("signer_claims"));
        }
        Ok(Header {
            author: author.ok_or_else(|| de::Error::missing_field("author"))?,
            round: round.ok_or_else(|| de::Error::missing_field("round"))?,
            epoch,
            created_at: created_at.ok_or_else(|| de::Error::missing_field("created_at"))?,
            payload: payload.ok_or_else(|| de::Error::missing_field("payload"))?,
            parents: parents.ok_or_else(|| de::Error::missing_field("parents"))?,
            digest: OnceCell::default(),
            latest_execution_block: latest_execution_block
                .ok_or_else(|| de::Error::missing_field("latest_execution_block"))?,
            signer_claims: signer_claims.unwrap_or_default(),
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        decode, primary::signer_claim::signer_claims_region_bytes, BlsSignature, CertificateDigest,
    };

    // main's bytes at 7ea68c68, captured in the step-1 spike against the unpatched crate.
    const FIXTURES: &[((u8, u32, u32, u64, usize, usize), &str, &str)] = &[
    ((170, 4, 7, 1700000000, 0, 0), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa040000000700000000f153650000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000000", "456b8cb94821ceccb0655ccfc866c4774531a1712a77bdb9a441bd7d3f2e7d33"),
    ((1, 1, 0, 0, 0, 0), "01010101010101010101010101010101010101010101010101010101010101010100000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000000", "a75812cdd73f2b0bac33e6715788dbc2acb04fe4eb6cd5121bff9358e8d13dd0"),
    ((2, 9, 1, 1600000000, 3, 2), "0202020202020202020202020202020202020202020202020202020202020202090000000100000000105e5f0000000003200101010101010101010101010101010101010101010101010101010101010101000020020202020202020202020202020202020202020202020202020202020202020201002003030303030303030303030303030303030303030303030303030303030303030200022000000000000000000000000000000000000000000000000000000000000000002001010101010101010101010101010101010101010101010101010101010101010000000000000000200000000000000000000000000000000000000000000000000000000000000000", "c4e8671c30ae0c7d78b7403ffd4d8643751cd4323d7201f79a3035ddde5f128d"),
    ((119, 400000, 4294967294, 18446744073709551615, 33, 8), "7777777777777777777777777777777777777777777777777777777777777777801a0600feffffffffffffffffffffff21200101010101010101010101010101010101010101010101010101010101010101000020020202020202020202020202020202020202020202020202020202020202020201002003030303030303030303030303030303030303030303030303030303030303030200200404040404040404040404040404040404040404040404040404040404040404030020050505050505050505050505050505050505050505050505050505050505050504002006060606060606060606060606060606060606060606060606060606060606060500200707070707070707070707070707070707070707070707070707070707070707060020080808080808080808080808080808080808080808080808080808080808080807002009090909090909090909090909090909090909090909090909090909090909090800200a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0900200b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0a00200c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0b00200d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0c00200e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0d00200f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0e002010101010101010101010101010101010101010101010101010101010101010100f00201111111111111111111111111111111111111111111111111111111111111111100020121212121212121212121212121212121212121212121212121212121212121211002013131313131313131313131313131313131313131313131313131313131313131200201414141414141414141414141414141414141414141414141414141414141414130020151515151515151515151515151515151515151515151515151515151515151514002016161616161616161616161616161616161616161616161616161616161616161500201717171717171717171717171717171717171717171717171717171717171717160020181818181818181818181818181818181818181818181818181818181818181817002019191919191919191919191919191919191919191919191919191919191919191800201a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1900201b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1a00201c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1b00201d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1c00201e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1d00201f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1e002020202020202020202020202020202020202020202020202020202020202020201f002021212121212121212121212121212121212121212121212121212121212121212000082000000000000000000000000000000000000000000000000000000000000000002001010101010101010101010101010101010101010101010101010101010101012002020202020202020202020202020202020202020202020202020202020202022003030303030303030303030303030303030303030303030303030303030303032004040404040404040404040404040404040404040404040404040404040404042005050505050505050505050505050505050505050505050505050505050505052006060606060606060606060606060606060606060606060606060606060606062007070707070707070707070707070707070707070707070707070707070707070000000000000000200000000000000000000000000000000000000000000000000000000000000000", "b54bb3e9e63039820362fbfa0211c4690c1d38d0ebc547f70dcd6bfef21ec486"),
    ((255, 0, 12345, 123456789, 1, 1), "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff000000003930000015cd5b0700000000012001010101010101010101010101010101010101010101010101010101010101010000012000000000000000000000000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000000", "3961ece13b08aadeec1f49e4dc3c9e70361284e2beeee5dde32ac575212fd4a6"),
    ];

    const POST_STRUCT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa04000000ffffffff00f153650000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000000";
    const POST_CLAIM_REGION: &str = "01200909090909090909090909090909090909090909090909090909090909090909ffffffff050000000300000000010000000200000030c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000";

    fn build_fixture((a, r, e, t, p, pa): &(u8, u32, u32, u64, usize, usize)) -> Header {
        let mut payload = IndexMap::new();
        for i in 0..*p {
            payload.insert(BlockHash::repeat_byte(i as u8 + 1), i as u16);
        }
        let mut parents = BTreeSet::new();
        for i in 0..*pa {
            parents.insert(CertificateDigest::new([i as u8; 32]));
        }
        HeaderBuilder::default()
            .author(AuthorityIdentifier::dummy_for_test(*a))
            .round(*r)
            .epoch(*e)
            .created_at(*t)
            .payload(payload)
            .parents(parents)
            .build()
    }

    fn post_header() -> Header {
        HeaderBuilder::default()
            .author(AuthorityIdentifier::dummy_for_test(0xAA))
            .round(4)
            .epoch(u32::MAX)
            .created_at(1_700_000_000)
            .payload(IndexMap::new())
            .parents(BTreeSet::new())
            .build()
    }

    fn claim9() -> SignerClaim {
        SignerClaim {
            target_digest: CertificateDigest::new([9; 32]),
            target_epoch: u32::MAX,
            target_round: 5,
            ranks: vec![0, 1, 2],
            signature: BlsSignature::default(),
        }
    }

    #[test]
    fn test_pre_activation_header_bytes_match_main() {
        for (fields, bytes, _digest) in FIXTURES {
            let h = build_fixture(fields);
            assert_eq!(
                hex::encode(header_canonical_bytes(&h)),
                *bytes,
                "fixture {fields:?} diverges from main"
            );
        }
    }

    #[test]
    fn test_pre_activation_digest_unchanged() {
        for (fields, _bytes, digest) in FIXTURES {
            let h = build_fixture(fields);
            assert_eq!(
                hex::encode(Hash::digest(&h).as_ref()),
                *digest,
                "fixture {fields:?} digest diverges from main"
            );
        }
    }

    #[test]
    fn test_post_activation_empty_region_is_present() {
        let h = post_header();
        let mut expected = hex::decode(POST_STRUCT).expect("golden");
        expected.push(0);
        assert_eq!(header_canonical_bytes(&h), expected, "empty region is the length byte 0");
        let back: Header = decode(&expected);
        assert_eq!(back.signer_claims, Vec::new());
        assert_eq!(header_canonical_bytes(&back), expected, "re-encode after roundtrip");
    }

    #[test]
    fn test_header_digest_resealed_after_signer_claims() {
        let h = post_header();
        let claims = vec![claim9()];
        let sealed = h.clone().with_signer_claims(claims.clone());
        assert_ne!(sealed.digest(), h.digest(), "claims must re-seal the digest");
        assert_eq!(
            sealed.digest(),
            Hash::digest(&sealed),
            "sealed digest matches its canonical bytes"
        );
        assert_eq!(
            signer_claims_region_bytes(&claims),
            hex::decode(POST_CLAIM_REGION).expect("golden"),
            "claim layout is pinned"
        );
        let mut expected = hex::decode(POST_STRUCT).expect("golden");
        expected.extend(signer_claims_region_bytes(&claims));
        assert_eq!(
            header_canonical_bytes(&sealed),
            expected,
            "post bytes are struct bytes ++ region"
        );
        let back: Header = decode(&expected);
        assert_eq!(back.signer_claims, claims);
        assert_eq!(back.digest(), sealed.digest(), "decoded header keeps the sealed digest");
    }

    #[test]
    fn test_header_span_equals_canonical_bytes() {
        let mut cases: Vec<Header> = FIXTURES.iter().map(|f| build_fixture(&f.0)).collect();
        cases.push(post_header().with_signer_claims(vec![claim9()]));
        for h in cases {
            let bytes = header_canonical_bytes(&h);
            let mut buf = bytes.clone();
            buf.push(0xFF);
            let mut c = BcsCursor::new(&buf);
            let span = c.take_span::<Header>().expect("span");
            assert_eq!(span, bytes.as_slice(), "span must equal the canonical bytes");
            assert_eq!(c.remaining(), &[0xFF], "the region must not swallow trailing input");
        }
    }
}
