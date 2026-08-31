//! Consensus header: a primary's per-round proposal of worker batches and parent certificates.

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
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, fmt};

/// `Header` type for consensus layer.
#[derive(Builder, Clone, Deserialize, Serialize, Default)]
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
    #[serde(with = "indexmap::map::serde_seq")]
    pub payload: IndexMap<BlockHash, WorkerId>,
    /// Parent certificates for this Header.
    pub parents: BTreeSet<CertificateDigest>,
    /// Hash and number of the latest execution block known when the header was built.
    ///
    /// Carrying it in a signed header lets peers validate the author's execution result along with
    /// the proposal.
    pub latest_execution_block: BlockNumHash,
    /// Cached digest. Off-wire, so a decoded header recomputes it on first use.
    #[serde(skip)]
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

/// BCS layout: `author, round, epoch, created_at, payload, parents,
/// latest_execution_block`. `digest` is `#[serde(skip)]`, off-wire. Keep in
/// lockstep with the struct.
impl BcsLayout for Header {
    fn skip(c: &mut BcsCursor<'_>) -> Result<(), BcsLayoutError> {
        c.skip::<AuthorityIdentifier>()?
            .skip::<Round>()?
            .skip::<Epoch>()?
            .skip::<TimestampSec>()?
            .skip::<Vec<(BlockHash, WorkerId)>>()?
            .skip::<BTreeSet<CertificateDigest>>()?
            .skip::<BlockNumHash>()?;
        Ok(())
    }
}
