//! Batch implementation for consensus.
//!
//! Batches hold transactions and other data. This type is used to represent worker proposals that
//! have reached quorum.

use crate::{
    crypto, encode, Address, BlockHash, Bytes, Epoch, ExecHeader, TimestampSec,
    ETHEREUM_BLOCK_GAS_LIMIT_56BITS, MIN_PROTOCOL_BASE_FEE,
};
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, hash::Hasher as _};
use thiserror::Error;

use super::WorkerId;

/// The batch for workers to communicate for consensus.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct SealedBatch {
    /// The immutable batch fields.
    pub batch: Batch,
    /// The immutable digest of the batch.
    pub digest: BlockHash,
}

impl SealedBatch {
    /// Create a new instance of Self.
    ///
    /// WARNING: this does not verify the provided digest matches the provided batch.
    pub fn new(batch: Batch, digest: BlockHash) -> Self {
        Self { batch, digest }
    }

    /// Consume self to extract the batch so it can be modified.
    pub fn unseal(self) -> Batch {
        self.batch
    }

    /// Return the sealed batch fields.
    pub fn batch(&self) -> &Batch {
        &self.batch
    }

    /// Return the digest of the sealed batch.
    pub fn digest(&self) -> BlockHash {
        self.digest
    }

    /// Split Self into separate parts.
    ///
    /// This is the inverse of [`Batch::seal_slow`].
    pub fn split(self) -> (Batch, BlockHash) {
        (self.batch, self.digest)
    }

    /// Size of the sealed batch.
    pub fn size(&self) -> usize {
        self.batch.size() + size_of::<BlockHash>()
    }
}

/// The batch for workers to communicate for consensus.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct Batch {
    /// The collection of transactions in this batch as bytes.
    pub transactions: Vec<Vec<u8>>,
    /// The epoch that this batch belongs to.
    pub epoch: Epoch,
    /// The 160-bit address to which all fees collected from the successful mining of this batch
    /// be transferred; formally Hc.
    pub beneficiary: Address,
    /// A scalar representing EIP1559 base fee which can move up or down each batch according
    /// to a formula which is a function of gas used in parent batch and gas target
    /// (batch gas limit divided by elasticity multiplier) of parent batch.
    /// The algorithm results in the base fee per gas increasing when batches are
    /// above the gas target, and decreasing when batches are below the gas target. The base fee
    /// per gas is sent to governance address.
    pub base_fee_per_gas: u64,
    /// The worker id for the worker that originated this batch.
    /// Worker ids will be consistent across validators (i.e. worker 0 talks to other worker 0s,
    /// etc). We can use this for tracking to support base fee calculations.
    /// Note: worker id 0 is the default.
    pub worker_id: WorkerId,
    /// Monotonically increasing sequence number per worker, used for ordering.
    /// Persists across epochs and restarts. All validators see the same value.
    ///
    /// `#[serde(default)]` produces `seq=0` when deserializing batches from nodes
    /// that predate this field. The execution layer treats `seq=0` as "unsequenced"
    /// and executes those batches immediately without ordering constraints.
    #[serde(default)]
    pub seq: u64,
    /// Timestamp of when the entity was received by another node. This will help
    /// calculate latencies that are not affected by clock drift or network
    /// delays. This field is not set for own batches.
    #[serde(skip)]
    // This field changes often so don't serialize (i.e. don't use it in the digest)
    pub received_at: Option<TimestampSec>,
}

impl Batch {
    /// Create a new batch for testing only!
    ///
    /// This is NOT a valid batch for consensus.
    pub fn new_for_test(
        transactions: Vec<Vec<u8>>,
        header: ExecHeader,
        worker_id: WorkerId,
        epoch: Epoch,
        seq: u64,
    ) -> Self {
        Self {
            transactions,
            epoch,
            beneficiary: header.beneficiary,
            base_fee_per_gas: header.base_fee_per_gas.unwrap_or(MIN_PROTOCOL_BASE_FEE),
            worker_id,
            seq,
            received_at: None,
        }
    }

    /// Size of the batch in bytes (including transactions).
    pub fn size(&self) -> usize {
        size_of::<Self>() + self.transactions.iter().map(|tx| tx.len()).sum::<usize>()
    }

    /// Digest for this batch (the hash of the sealed header).
    ///
    /// NOTE: `Self::received_at` is skipped during serialization and is excluded from the digest.
    pub fn digest(&self) -> BlockHash {
        let mut hasher = crypto::DefaultHashFunction::new();
        hasher.update(encode(self).as_ref());
        // finalize
        BlockHash::from_slice(hasher.finalize().as_bytes())
    }

    /// Returns a reference to the collection of transaction bytes.
    pub fn transactions(&self) -> &Vec<Vec<u8>> {
        &self.transactions
    }

    /// Returns a mutable reference to a collection of transaction bytes.
    pub fn transactions_mut(&mut self) -> &mut Vec<Vec<u8>> {
        &mut self.transactions
    }

    /// Returns the received at time if available.
    pub fn received_at(&self) -> Option<TimestampSec> {
        self.received_at
    }

    /// Sets the received-at time.
    pub fn set_received_at(&mut self, time: TimestampSec) {
        self.received_at = Some(time)
    }

    /// Seal the header with a known hash.
    ///
    /// WARNING: This method does not verify whether the hash is correct.
    pub fn seal(self, digest: BlockHash) -> SealedBatch {
        SealedBatch::new(self, digest)
    }

    /// Seal the batch.
    ///
    /// Calculate the hash and seal the batch so it can't be changed.
    ///
    /// NOTE: `Batch::received_at` is skipped during serialization and is excluded from the
    /// digest.
    pub fn seal_slow(self) -> SealedBatch {
        let digest = self.digest();
        self.seal(digest)
    }
}

impl Default for Batch {
    fn default() -> Self {
        Self {
            transactions: vec![],
            received_at: None,
            epoch: Epoch::default(),
            beneficiary: Address::ZERO,
            worker_id: 0,
            seq: 0,
            base_fee_per_gas: MIN_PROTOCOL_BASE_FEE,
        }
    }
}

impl From<&SealedBatch> for Vec<u8> {
    fn from(value: &SealedBatch) -> Self {
        crate::encode(value)
    }
}

impl From<&[u8]> for SealedBatch {
    fn from(value: &[u8]) -> Self {
        crate::decode(value)
    }
}

/// Return the max gas per batch in effect for the epoch.
///
/// Currently always 30,000,000; the epoch parameter lets a fork change it.
pub fn max_batch_gas(_epoch: Epoch) -> u64 {
    ETHEREUM_BLOCK_GAS_LIMIT_56BITS
}

/// Return the max batch size in bytes in effect for the epoch.
///
/// Currently always 2,000,000; the epoch parameter lets a fork change it. A larger batch fails
/// the message size check on decode.
pub fn max_batch_size(_epoch: Epoch) -> usize {
    2_000_000
}

/// Pre-`TransactionLoadBalancing` slot digest: read the first 8 bytes as little-endian u64.
///
/// Caller must ensure `input.len() >= 8`.
pub fn legacy_slot_digest(input: &[u8]) -> u64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&input[0..8]);
    u64::from_le_bytes(bytes)
}

/// `FxHasher` over `input`, the committee-slot digest for load balancing.
///
/// Both the forwarder (over a sender address) and the receiving validator (over the same key)
/// call this, so they must agree byte for byte. Do NOT replace with `FxBuildHasher::hash_one`:
/// that path writes a slice-length prefix and changes the digest.
pub fn fxhash_slot_digest(input: &[u8]) -> u64 {
    let mut hasher = rustc_hash::FxHasher::default();
    hasher.write(input);
    hasher.finish()
}

/// Visit every committee slot once, in ring order, starting from `owner`.
///
/// The committee is numbered deterministically (sorted authority order), so every validator walks
/// the same ring, which is what makes live-successor failover agree across nodes.
pub fn ring_walk(owner: u64, size: u64) -> impl Iterator<Item = u64> {
    (0..size).map(move |step| (owner + step) % size)
}

/// This validator's view of committee slot ownership and liveness for sender-affinity dispatch.
///
/// Slots are numbered by committee order (sorted by authority id), identical on every validator, so
/// only `live` is a per-node view. A sender's natural owner is `slot_digest(sender) % size`; if
/// that owner is down, its senders fail over to the next live slot on the ring via
/// [`Self::covers`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitteeSlots {
    /// This validator's slot in committee order.
    pub own_slot: u64,
    /// Per-slot liveness in committee order; the length is the committee size.
    pub live: Vec<bool>,
}

impl CommitteeSlots {
    /// Build a view in which every slot is live, for the degenerate healthy case and tests.
    pub fn all_live(size: usize, own_slot: u64) -> Self {
        Self { own_slot, live: vec![true; size] }
    }

    /// Build a view marking slot `i` live when it is our own slot or `keys[i]` is in `connected`.
    ///
    /// `keys` must be in committee order; a down owner (its key absent from `connected`) then fails
    /// over to the next live slot via [`Self::covers`]. Own slot is always live.
    pub fn from_connectivity<K: PartialEq>(own_slot: u64, keys: &[K], connected: &[K]) -> Self {
        let live = keys
            .iter()
            .enumerate()
            .map(|(slot, key)| slot as u64 == own_slot || connected.contains(key))
            .collect();
        Self { own_slot, live }
    }

    /// The committee size (number of slots).
    pub fn size(&self) -> u64 {
        self.live.len() as u64
    }

    /// Whether this validator covers `owner`'s senders: the first live slot on the ring from
    /// `owner` is our own. A pure predicate of the view, so validators holding the same view
    /// agree on the single covering slot.
    pub fn covers(&self, owner: u64) -> bool {
        let size = self.size();
        if size == 0 {
            return false;
        }
        ring_walk(owner % size, size)
            .find(|slot| self.live[*slot as usize])
            .is_some_and(|covering| covering == self.own_slot)
    }
}

/// Validation of a peer's batch and admission of transactions received from other nodes.
///
/// Invalid transactions receive no further processing.
#[async_trait::async_trait]
pub trait BatchValidation: Send + Sync + Debug {
    /// Determines whether this batch can be voted on.
    async fn validate_batch(&self, b: SealedBatch) -> Result<(), BatchValidationError>;

    /// Admit a gossiped transaction message to the pool if its first transaction maps to a slot
    /// this validator covers.
    fn submit_batch_if_mine(
        &self,
        tx_bytes: &[Bytes],
        slots: &CommitteeSlots,
    ) -> Result<(), SubmitBatchError>;

    /// Admit transactions forwarded directly by an observer, returning the hashes rejected as
    /// stale (already executed) so the sender stops re-forwarding them.
    ///
    /// Takes the decoded bytes by value so the owned buffer moves into the blocking recovery task
    /// without a copy.
    async fn submit_forwarded_txns(&self, tx_bytes: Vec<Bytes>) -> Vec<BlockHash>;
}

/// Errors that can occur during batch submission.
#[derive(Error, Debug)]
pub enum SubmitBatchError {
    /// The transaction is not correctly encoded.
    #[error("Invalid transaction bytes")]
    InvalidTransactionBytes,
}

/// Errors from validating a peer's batch.
#[derive(Error, Debug)]
pub enum BatchValidationError {
    /// The sealed batch hash does not match this worker's calculated digest.
    #[error("Invalid digest for sealed batch.")]
    InvalidDigest,
    /// Canonical chain header cannot be found.
    #[error("Canonical chain header {block_hash} can't be found for peer batch's parent")]
    CanonicalChain {
        /// The executed block hash of the missing canonical chain header.
        block_hash: BlockHash,
    },
    /// Empty batch.
    #[error("Batch contains no transactions")]
    EmptyBatch,
    /// Error when the max gas included in the header exceeds the batch's gas limit.
    #[error("Peer's batch total possible gas ({total_possible_gas}) is greater than batch's gas limit ({gas_limit})")]
    HeaderMaxGasExceedsGasLimit {
        /// The total possible gas used in the batch header measured by included transactions max
        /// gas.
        total_possible_gas: u64,
        /// The gas limit in the batch header.
        gas_limit: u64,
    },
    /// Error while calculating max possible gas from included transactions.
    #[error("Unable to reduce max possible gas limit for peer's batch")]
    CalculateMaxPossibleGas,
    /// Error when peer's transaction list exceeds the maximum bytes allowed.
    #[error("Peer's transactions exceed max byte size: {0}")]
    HeaderTransactionBytesExceedsMax(usize),
    /// Error trying to decode a transaction in a peer's batch.
    /// If any transaction fails to decode, the entire batch validation fails.
    #[error("Failed to decode transaction for batch {0}: {1}")]
    RecoverTransaction(BlockHash, String),
    /// Error, invalid base fee set.
    #[error("Invalid base fee, expected {expected_base_fee} got {base_fee}")]
    InvalidBaseFee { expected_base_fee: u64, base_fee: u64 },
    /// Error, wrong worker id.
    #[error("Invalid worker id, expected {expected_worker_id} got {worker_id}")]
    InvalidWorkerId { expected_worker_id: WorkerId, worker_id: WorkerId },
    /// The batch contains blob transactions EIP-4844.
    #[error("Proposed batch contains blob transaction. Tx hash: {0}")]
    InvalidTx4844(BlockHash),
    /// The total allowable gas in the batch exceeds `u64::MAX`.
    #[error("Overflow calculating max possible gas.")]
    GasOverflow,
    /// Error, wrong epoch.
    #[error("Invalid epoch, expected epoch {expected} got epoch {found}")]
    InvalidEpoch { expected: Epoch, found: Epoch },
}

#[cfg(test)]
mod committee_slots_tests {
    use super::*;

    #[test]
    fn ring_walk_visits_every_slot_once_from_the_owner() {
        assert_eq!(ring_walk(1, 4).collect::<Vec<_>>(), vec![1, 2, 3, 0]);
        assert_eq!(ring_walk(0, 3).collect::<Vec<_>>(), vec![0, 1, 2]);
    }

    #[test]
    fn all_live_owner_covers_only_itself() {
        let slots = CommitteeSlots::all_live(4, 2);
        assert!(slots.covers(2), "the natural owner covers its own senders");
        assert!(!slots.covers(1), "a healthy peer's senders are not ours");
        assert!(!slots.covers(3), "a healthy peer's senders are not ours");
    }

    #[test]
    fn down_owner_fails_over_to_the_next_live_slot() {
        // committee of 4, we are slot 2, slot 1 is down
        let slots = CommitteeSlots { own_slot: 2, live: vec![true, false, true, true] };
        // owner 1 is down: ring walk 1,2,3,0 finds slot 2 (us) first, so we cover it
        assert!(slots.covers(1), "a down owner's senders fail over to the next live slot");
        // owner 0 is live and covered by slot 0, not us
        assert!(!slots.covers(0));
    }

    #[test]
    fn failover_wraps_around_the_ring() {
        // we are slot 0, and every other slot is down
        let slots = CommitteeSlots { own_slot: 0, live: vec![true, false, false, false] };
        // owner 3 is down: ring walk 3,0,1,2 wraps to slot 0 (us)
        assert!(slots.covers(3), "failover wraps past the end of the ring");
        assert!(slots.covers(1), "owner 1 down: walk 1,2,3,0 lands on slot 0 (us)");
    }

    #[test]
    fn no_live_slot_covers_nothing() {
        let slots = CommitteeSlots { own_slot: 0, live: vec![false, false] };
        assert!(!slots.covers(0));
        assert!(!slots.covers(1));
    }

    #[test]
    fn from_connectivity_marks_own_slot_and_connected_peers_live() {
        // committee-ordered peer keys; we are slot 1, and only slot 2's peer is connected
        let keys = [10u8, 20, 30, 40];
        let connected = [30u8];
        let slots = CommitteeSlots::from_connectivity(1, &keys, &connected);

        // own slot is always live; connected peer (slot 2) is live; the rest are down
        assert_eq!(slots.live, vec![false, true, true, false]);
        // senders owned by the down slots 0 and 3 fail over to us (slot 1)
        assert!(slots.covers(0), "down slot 0 fails over to us");
        assert!(slots.covers(3), "down slot 3 fails over to us");
        // slot 2 is live, so it keeps its own senders
        assert!(!slots.covers(2), "a live peer keeps its senders");
    }
}
