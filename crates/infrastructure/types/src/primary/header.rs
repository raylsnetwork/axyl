//! Consensus header: a primary's per-round proposal of worker batches and parent certificates.

use crate::{
    bcs_layout::{skip_bytes, BcsCursor, BcsLayout, BcsLayoutError},
    crypto::{self, BlsSignature},
    encode,
    error::{HeaderError, HeaderResult},
    now,
    serde::{deserialize_normalized_bounded, roaring_container_count, RoaringBitmapSerde},
    AuthorityIdentifier, Batch, BlockHash, BlockNumHash, Certificate, CertificateDigest, Committee,
    Digest, Epoch, Hash, Round, TimestampSec, VoteDigest, WorkerId,
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
use serde_with::SerializeAs;
use std::{collections::BTreeSet, fmt};

/// Bit set in the wire `epoch` field of a header that carries `own_parent_signers`.
///
/// The header layout is versioned in-band so every decoder (network, consensus DB, cold archive
/// projections) tells the two layouts apart from the bytes alone, with no activation table: a
/// v1 header encodes exactly as it always did, a v2 header sets this bit and appends the claim.
/// Real epochs never approach `2^31`. Decoders strip the bit, so no decoded header carries
/// it; a header *built* with such an epoch fails to serialize, which `encode` (and with it
/// `Header::new`, `HeaderBuilder::build` and `Hash::digest`) turns into a panic, so never build a
/// header from a peer-supplied epoch without checking it against the committee epoch first.
pub const HEADER_SIGNERS_EPOCH_FLAG: Epoch = 1 << 31;

/// Longest serialized `own_parent_signers` bitmap accepted, in bytes.
///
/// A claim names committee indices, so a legitimate one is a single small container (a few dozen
/// bytes). Roaring run containers can stand for 65,536 values in about ten bytes, so the claim is
/// bounded in bytes and in values before it is expanded, on every path that decodes one.
pub const MAX_OWN_PARENT_SIGNERS_BYTES: usize = 4096;

/// Most values an `own_parent_signers` claim may hold. Far above any committee size, far below
/// what an unbounded bitmap could be made to expand to. As an array container this many values
/// take half of [`MAX_OWN_PARENT_SIGNERS_BYTES`], so the two bounds never contradict.
pub const MAX_OWN_PARENT_SIGNERS: u64 = 1024;

/// Longest base58 text a claim within [`MAX_OWN_PARENT_SIGNERS_BYTES`] can have. Base58 writes at
/// most 137 characters per 100 bytes (log 256 / log 58 is 1.3657), rounded up, plus one for a
/// final partial character; anything longer is refused before the quadratic decode runs.
const MAX_OWN_PARENT_SIGNERS_BASE58_LEN: usize = MAX_OWN_PARENT_SIGNERS_BYTES * 138 / 100 + 1;

/// Decodes a serialized `own_parent_signers` bitmap, refusing one over the byte or value bounds
/// before it is expanded. Shared by the serde path and [`Header::project_wire`].
fn decode_own_parent_signers(bytes: &[u8]) -> std::io::Result<RoaringBitmap> {
    use std::io::{Error, ErrorKind};
    if bytes.len() > MAX_OWN_PARENT_SIGNERS_BYTES {
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!(
                "own-parent signer claim of {} bytes exceeds {MAX_OWN_PARENT_SIGNERS_BYTES}",
                bytes.len()
            ),
        ));
    }
    // Committee indices all live in container key 0, so a legitimate claim declares at most one
    // container (none when empty). Checked off the header before `deserialize_from` builds any
    // container: a run container is ~10 bytes on the wire and an 8 KiB store once decoded, so
    // without this a claim within the byte bound could still allocate megabytes before the
    // value-count check rejected it.
    let containers = roaring_container_count(bytes).ok_or_else(|| {
        Error::new(ErrorKind::InvalidData, "own-parent signer claim is not a roaring bitmap")
    })?;
    if containers > 1 {
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!("own-parent signer claim declares {containers} containers, at most 1 allowed"),
        ));
    }
    deserialize_normalized_bounded(bytes, MAX_OWN_PARENT_SIGNERS)?.ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidData,
            format!("own-parent signer claim names more than {MAX_OWN_PARENT_SIGNERS} signers"),
        )
    })
}

/// A proposer's claim about its own previous-round certificate: who signed it, and that
/// certificate's aggregate signature over the previous header's digest.
///
/// The signature is carried so the claim can be verified on its own, against the previous
/// header's digest and the committee keys, independently of which variant of that certificate a
/// voter happens to hold. Voters do not verify it yet: they compare the claim with their stored
/// copy and refuse on a difference without scoring the author (see `check_own_parent_signers`).
/// Switching to verification is a behavioural change with no layout change.
#[derive(Clone, Debug, Default)]
pub struct OwnParentSigners {
    /// Committee indices in BLS-key order, the index space of `Certificate::signed_authorities`.
    pub signers: RoaringBitmap,
    /// The aggregate signature of exactly `signers` over the previous header's digest. `None`
    /// iff `signers` is empty; [`Self::is_well_formed`] holds that invariant.
    pub signature: Option<BlsSignature>,
}

/// Value equality: the same signer indices and the same signature. Written by hand because
/// `RoaringBitmap`'s equality is false across store kinds (array vs bitmap) for the same values,
/// and a claim decoded off the wire need not share the store kind of one built from a
/// certificate.
impl PartialEq for OwnParentSigners {
    fn eq(&self, other: &Self) -> bool {
        self.signature == other.signature && self.signers.iter().eq(other.signers.iter())
    }
}

impl OwnParentSigners {
    /// The claim a proposer makes when its previous-round certificate is not among its parents.
    pub fn empty() -> Self {
        Self::default()
    }

    /// The claim for `certificate`: its signer bitmap and aggregate signature. Genesis
    /// certificates have neither, which yields the empty claim.
    pub fn from_certificate(certificate: &Certificate) -> Self {
        let signers = certificate.signed_authorities().clone();
        let signature = if signers.is_empty() { None } else { certificate.aggregated_signature() };
        Self { signers, signature }
    }

    /// A claim with `signers` and no signature. Only [`Header::validate`] enforces the
    /// signature invariant, so this serves tests of the paths that never validate: projections,
    /// the tally, decoding.
    pub fn unsigned_for_test(signers: RoaringBitmap) -> Self {
        Self { signers, signature: None }
    }

    /// True when the signature is present exactly for a non-empty signer set.
    pub fn is_well_formed(&self) -> bool {
        self.signature.is_some() == !self.signers.is_empty()
    }
}

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
    /// The author's claim about its own parent certificate (its round `round - 1` certificate):
    /// the signer bitmap that certificate carries, plus its aggregate signature.
    ///
    /// A certificate's signer set is not part of any commitment (the DAG keys certificates by
    /// header digest and the committed sub-dag hashes those digests only), and any committee
    /// member can add or remove its own signature to produce another valid variant, so two honest
    /// nodes can hold the same certificate with different signer sets. Restating the set here puts
    /// it inside a signed, committed header that every node holds byte-identically, which is what
    /// lets the reward tally credit signers deterministically. Voters refuse a header whose claim
    /// differs from the parent certificate they hold, without scoring the author.
    ///
    /// `None` before the `HeaderParentSigners` fork; `Some` (possibly empty, when the author's
    /// previous certificate is not among the parents) once it is active.
    pub own_parent_signers: Option<OwnParentSigners>,
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
        own_parent_signers: Option<OwnParentSigners>,
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

        // A claimed signer must be a committee index and the claim must carry a signature exactly
        // when it names signers. Whether the claim is allowed at all, and whether it matches the
        // parent certificate, is checked by the voter against its own copy of that certificate
        // and the fork state (`vote_inner`).
        if let Some(claim) = &self.own_parent_signers {
            if let Some(max) = claim.signers.max() {
                if max as usize >= committee.size() {
                    return Err(HeaderError::OwnParentSignerOutOfRange {
                        max,
                        committee_size: committee.size(),
                    });
                }
            }
            if !claim.is_well_formed() {
                return Err(HeaderError::OwnParentSignersMalformed);
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
    /// The author's claim about its own parent certificate, `None` for a pre-fork header.
    pub fn own_parent_signers(&self) -> Option<&OwnParentSigners> {
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

    /// Projects `author`, `round`, `epoch` and the claimed signer bitmap out of one BCS-encoded
    /// header (the span [`crate::Certificate::skip_with_header_span`] returns), without decoding
    /// the payload, parents, execution anchor or the claim's signature.
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
            let signers = decode_own_parent_signers(bytes).map_err(|e| {
                BcsLayoutError::InvalidValue { type_name: "RoaringBitmap", reason: e.to_string() }
            })?;
            c.skip::<Option<BlsSignature>>()?;
            Some(signers)
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
    /// The signer bitmap of the author's claim about its own parent certificate, `None` for a v1
    /// header. The claim's signature is skipped, not projected.
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
            "{}: B{}(v{}, e{}, {}wbs, exec: {:?}, claims: {:?})",
            self.digest(),
            self.round(),
            self.author(),
            self.epoch(),
            self.payload().len(),
            self.latest_execution_block,
            self.own_parent_signers.as_ref().map(|c| c.signers.iter().collect::<Vec<_>>()),
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
// `epoch`, then the claim's signer bitmap as roaring bytes, then its signature as
// `Option<BlsSignature>`. `digest` is off-wire. Keep `BcsLayout`, `project_wire` and the
// human-readable form below in lockstep with the struct.

const HEADER_FIELDS: &[&str] = &[
    "author",
    "round",
    "epoch",
    "created_at",
    "payload",
    "parents",
    "latest_execution_block",
    "own_parent_signers",
    "own_parent_signature",
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

/// The claimed bitmap as decoded off the wire, through the bounded decoder: the bytes are
/// peer-supplied and arrive on the vote path, so they are bounded before the bitmap is expanded.
struct SignerBytesOwned(RoaringBitmap);

impl<'de> Deserialize<'de> for SignerBytesOwned {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ClaimVisitor;

        impl Visitor<'_> for ClaimVisitor {
            type Value = SignerBytesOwned;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a bounded roaring bitmap of committee indices")
            }

            fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
                decode_own_parent_signers(v).map(SignerBytesOwned).map_err(E::custom)
            }
        }

        deserializer.deserialize_bytes(ClaimVisitor)
    }
}

/// Human-readable (JSON/YAML) form: named fields, bitmap as base58, and the claim only present
/// when set so pre-fork documents are unchanged.
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    own_parent_signers: Option<ClaimHuman>,
}

/// The claim in a human-readable document.
#[derive(Serialize, Deserialize)]
struct ClaimHuman {
    signers: SignersHuman,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    signature: Option<BlsSignature>,
}

impl From<&OwnParentSigners> for ClaimHuman {
    fn from(claim: &OwnParentSigners) -> Self {
        Self { signers: SignersHuman(claim.signers.clone()), signature: claim.signature }
    }
}

impl From<ClaimHuman> for OwnParentSigners {
    fn from(claim: ClaimHuman) -> Self {
        Self { signers: claim.signers.0, signature: claim.signature }
    }
}

/// The claimed bitmap in a human-readable document: base58 roaring bytes, decoded through the
/// same bounded decoder as the binary path so no ingress expands an unbounded bitmap.
struct SignersHuman(RoaringBitmap);

impl Serialize for SignersHuman {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        RoaringBitmapSerde::serialize_as(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for SignersHuman {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        // base58 decoding is quadratic in the text length, so bound the text before decoding
        if text.len() > MAX_OWN_PARENT_SIGNERS_BASE58_LEN {
            return Err(de::Error::custom("own-parent signer claim text too long"));
        }
        let bytes = bs58::decode(&text).into_vec().map_err(de::Error::custom)?;
        decode_own_parent_signers(&bytes).map(SignersHuman).map_err(de::Error::custom)
    }
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
                own_parent_signers: self.own_parent_signers.as_ref().map(ClaimHuman::from),
            };
            return human.serialize(serializer);
        }

        let has_claim = self.own_parent_signers.is_some();
        let wire_epoch =
            if has_claim { self.epoch | HEADER_SIGNERS_EPOCH_FLAG } else { self.epoch };
        let mut s = serializer.serialize_struct("Header", if has_claim { 9 } else { 7 })?;
        s.serialize_field("author", &self.author)?;
        s.serialize_field("round", &self.round)?;
        s.serialize_field("epoch", &wire_epoch)?;
        s.serialize_field("created_at", &self.created_at)?;
        s.serialize_field("payload", &PayloadSeq(&self.payload))?;
        s.serialize_field("parents", &self.parents)?;
        s.serialize_field("latest_execution_block", &self.latest_execution_block)?;
        if let Some(claim) = &self.own_parent_signers {
            s.serialize_field("own_parent_signers", &SignerBytes(&claim.signers))?;
            s.serialize_field("own_parent_signature", &claim.signature)?;
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
                own_parent_signers: human.own_parent_signers.map(OwnParentSigners::from),
                digest: OnceCell::default(),
            });
        }

        struct HeaderWireVisitor;

        impl<'de> Visitor<'de> for HeaderWireVisitor {
            type Value = Header;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a BCS-encoded consensus header")
            }

            // Reads seven fields, and two more when the epoch field carries the layout flag.
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
                let (epoch, has_claim) = Header::split_wire_epoch(field!(2, Epoch));
                let created_at = field!(3, TimestampSec);
                let payload: IndexMap<BlockHash, WorkerId> =
                    field!(4, Vec<(BlockHash, WorkerId)>).into_iter().collect();
                let parents = field!(5, BTreeSet<CertificateDigest>);
                let latest_execution_block = field!(6, BlockNumHash);
                let own_parent_signers = if has_claim {
                    let signers = field!(7, SignerBytesOwned).0;
                    let signature = field!(8, Option<BlsSignature>);
                    Some(OwnParentSigners { signers, signature })
                } else {
                    None
                };

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
/// then the claim's signer bitmap (roaring bytes) and `Option<BlsSignature>` iff `epoch` carries
/// [`HEADER_SIGNERS_EPOCH_FLAG`]. `digest` is off-wire. Keep in lockstep with the struct.
impl BcsLayout for Header {
    fn skip(c: &mut BcsCursor<'_>) -> Result<(), BcsLayoutError> {
        c.skip::<AuthorityIdentifier>()?.skip::<Round>()?;
        let (_, has_claim) = Header::split_wire_epoch(c.read::<Epoch>()?);
        c.skip::<TimestampSec>()?
            .skip::<Vec<(BlockHash, WorkerId)>>()?
            .skip::<BTreeSet<CertificateDigest>>()?
            .skip::<BlockNumHash>()?;
        if has_claim {
            skip_bytes(c)?;
            c.skip::<Option<BlsSignature>>()?;
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

    /// A well-formed claim naming `indices`: a (dummy) signature exactly when non-empty.
    fn claim(indices: &[u32]) -> OwnParentSigners {
        claim_of(RoaringBitmap::from_sorted_iter(indices.iter().copied()).unwrap())
    }

    fn claim_of(signers: RoaringBitmap) -> OwnParentSigners {
        let signature = (!signers.is_empty()).then(BlsSignature::default);
        OwnParentSigners { signers, signature }
    }

    fn sample(own_parent_signers: Option<OwnParentSigners>) -> Header {
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
        let signed = claim(&[0, 2, 3]);
        let v2 = sample(Some(signed.clone()));
        let v1 = sample(None);

        let bytes = encode(&v2);
        // author (32) + round (4), then the epoch field with the flag in its top bit.
        let wire_epoch = Epoch::from_le_bytes(bytes[36..40].try_into().unwrap());
        assert_eq!(Header::split_wire_epoch(wire_epoch), (3, true));
        assert_ne!(v2.digest(), v1.digest());

        let decoded: Header = decode(&bytes);
        assert_fields_eq(&v2, &decoded);
        assert_eq!(decoded.own_parent_signers.as_ref(), Some(&signed));
        assert!(decoded.own_parent_signers.as_ref().unwrap().signature.is_some());

        // an empty claim is still a v2 header, and the digest covers the signature
        let empty = sample(Some(OwnParentSigners::empty()));
        let decoded: Header = decode(&encode(&empty));
        assert_eq!(decoded.own_parent_signers, Some(OwnParentSigners::empty()));
        assert_ne!(empty.digest(), v1.digest());
        let unsigned = sample(Some(OwnParentSigners::unsigned_for_test(signed.signers.clone())));
        assert_ne!(unsigned.digest(), v2.digest());
    }

    #[test]
    fn skip_and_projection_consume_exactly_for_both_layouts() {
        for c in [None, Some(claim(&[1, 3])), Some(OwnParentSigners::empty())] {
            let h = sample(c.clone());
            let bytes = encode(&h);
            let mut cursor = BcsCursor::new(&bytes);
            Header::skip(&mut cursor).unwrap();
            assert!(cursor.is_empty(), "{} bytes left after Header::skip", cursor.len());

            let meta = Header::project_wire(&bytes).unwrap();
            assert_eq!(meta.author, h.author);
            assert_eq!(meta.round, h.round);
            assert_eq!(meta.epoch, h.epoch);
            assert_eq!(meta.own_parent_signers, c.map(|c| c.signers));
        }
    }

    #[test]
    fn human_readable_roundtrip_keeps_v1_documents_unchanged() {
        let v1 = sample(None);
        let json = serde_json::to_value(&v1).unwrap();
        assert!(json.get("own_parent_signers").is_none());
        let back: Header = serde_json::from_value(json).unwrap();
        assert_fields_eq(&v1, &back);

        for c in [claim(&[2]), OwnParentSigners::empty()] {
            let v2 = sample(Some(c));
            let json = serde_json::to_value(&v2).unwrap();
            assert!(json.get("own_parent_signers").is_some());
            let back: Header = serde_json::from_value(json).unwrap();
            assert_fields_eq(&v2, &back);
        }
    }

    /// A claim is peer-supplied on the vote path, so an oversized one is refused before the
    /// bitmap expands, on the serde path and on the projection alike.
    #[test]
    fn oversized_signer_claims_are_refused_before_expansion() {
        // 2^24 values: refused on size before any container is looked at
        let mut huge = RoaringBitmap::new();
        huge.insert_range(0..(1u32 << 24));
        let mut bytes = encode(&sample(Some(claim_of(huge))));

        assert!(bcs::from_bytes::<Header>(&bytes).is_err());
        assert!(matches!(
            Header::project_wire(&bytes),
            Err(BcsLayoutError::InvalidValue { type_name: "RoaringBitmap", .. })
        ));

        // a small bitmap that still names too many signers
        let mut wide = RoaringBitmap::new();
        wide.insert_range(0..(MAX_OWN_PARENT_SIGNERS as u32 + 1));
        bytes = encode(&sample(Some(claim_of(wide))));
        assert!(bcs::from_bytes::<Header>(&bytes).is_err());
        assert!(Header::project_wire(&bytes).is_err());

        // a claim spanning two containers: tiny on the wire and within the value bound, refused
        // on the declared container count before any container is built
        bytes = encode(&sample(Some(claim(&[1, 1 << 16]))));
        assert!(bcs::from_bytes::<Header>(&bytes).is_err());
        assert!(Header::project_wire(&bytes).is_err());

        // the largest legitimate claim decodes
        let full =
            claim_of(RoaringBitmap::from_sorted_iter(0..(MAX_OWN_PARENT_SIGNERS as u32)).unwrap());
        let decoded: Header = decode(&encode(&sample(Some(full.clone()))));
        assert_eq!(decoded.own_parent_signers, Some(full));
    }

    #[test]
    fn epoch_colliding_with_the_flag_does_not_encode() {
        let mut h = sample(None);
        h.epoch = HEADER_SIGNERS_EPOCH_FLAG | 5;
        assert!(bcs::to_bytes(&h).is_err());
    }

    #[test]
    fn validate_checks_the_claim_shape() {
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

        let make = |c: Option<OwnParentSigners>| {
            Header::new(
                author.clone(),
                1,
                3,
                IndexMap::new(),
                BTreeSet::new(),
                BlockNumHash::default(),
                c,
            )
        };
        assert!(make(None).validate(&committee).is_ok());
        assert!(make(Some(OwnParentSigners::empty())).validate(&committee).is_ok());
        assert!(make(Some(claim(&[0, 3]))).validate(&committee).is_ok());
        // a signer index outside the committee
        assert!(matches!(
            make(Some(claim(&[0, 4]))).validate(&committee),
            Err(HeaderError::OwnParentSignerOutOfRange { max: 4, committee_size: 4 })
        ));
        // signers without a signature, and a signature without signers
        let bitmap = RoaringBitmap::from_sorted_iter([0u32, 3]).unwrap();
        assert!(matches!(
            make(Some(OwnParentSigners::unsigned_for_test(bitmap))).validate(&committee),
            Err(HeaderError::OwnParentSignersMalformed)
        ));
        let stray = OwnParentSigners {
            signers: RoaringBitmap::new(),
            signature: Some(BlsSignature::default()),
        };
        assert!(matches!(
            make(Some(stray)).validate(&committee),
            Err(HeaderError::OwnParentSignersMalformed)
        ));
    }
}
