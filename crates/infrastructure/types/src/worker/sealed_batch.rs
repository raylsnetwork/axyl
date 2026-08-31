//! Worker batches: the transaction payloads a worker proposes, sealed by digest for consensus.

use crate::{
    crypto, encode, Address, BlockHash, Bytes, Epoch, ExecHeader, TimestampSec,
    ETHEREUM_BLOCK_GAS_LIMIT_56BITS, MIN_PROTOCOL_BASE_FEE,
};
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, hash::Hasher as _};
use thiserror::Error;

use super::WorkerId;

/// A batch paired with its digest.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct SealedBatch {
    /// The immutable batch fields.
    pub batch: Batch,
    /// The immutable digest of the batch.
    pub digest: BlockHash,
}

impl SealedBatch {
    /// Creates a sealed batch from a batch and a digest.
    ///
    /// WARNING: this does not verify the provided digest matches the provided batch.
    pub fn new(batch: Batch, digest: BlockHash) -> Self {
        Self { batch, digest }
    }

    /// Consumes the seal and returns the batch so it can be modified.
    pub fn unseal(self) -> Batch {
        self.batch
    }

    /// Returns the sealed batch.
    pub fn batch(&self) -> &Batch {
        &self.batch
    }

    /// Returns the digest of the sealed batch.
    pub fn digest(&self) -> BlockHash {
        self.digest
    }

    /// Splits the seal into its batch and digest, the inverse of [`Batch::seal_slow`].
    pub fn split(self) -> (Batch, BlockHash) {
        (self.batch, self.digest)
    }

    /// Returns the in-memory size of the sealed batch in bytes.
    pub fn size(&self) -> usize {
        self.batch.size() + size_of::<BlockHash>()
    }
}

/// A worker's proposal: transaction payloads plus the fields that fix their execution context.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct Batch {
    /// The encoded transactions in this batch.
    ///
    /// `Bytes` is refcounted, so cloning a `Batch` through the seal, quorum, and ordering stages
    /// bumps a reference instead of deep-copying every payload. `Vec<Bytes>` and `Vec<Vec<u8>>`
    /// are bcs-identical (each element a length-prefixed byte run), so peers holding either shape
    /// interoperate byte for byte.
    pub transactions: Vec<Bytes>,
    /// The epoch that this batch belongs to.
    pub epoch: Epoch,
    /// The address that receives the fees collected from this batch.
    pub beneficiary: Address,
    /// The EIP-1559 base fee in effect for this batch; a peer rejects a batch whose base fee
    /// differs from the one it expects.
    pub base_fee_per_gas: u64,
    /// The id of the worker that sealed this batch. Worker ids are consistent across validators
    /// (worker 0 talks to every other worker 0); 0 is the default.
    pub worker_id: WorkerId,
    /// Monotonically increasing sequence number per worker, used for ordering.
    /// Persists across epochs and restarts. All validators see the same value.
    ///
    /// `#[serde(default)]` produces `seq=0` when deserializing batches from nodes
    /// that predate this field. The execution layer treats `seq=0` as "unsequenced"
    /// and executes those batches immediately without ordering constraints.
    #[serde(default)]
    pub seq: u64,
    /// When this node received the batch; unset for its own batches. Used for latency
    /// measurements that clock drift cannot skew.
    ///
    /// Node-local, so it is off-wire and excluded from the digest.
    #[serde(skip)]
    pub received_at: Option<TimestampSec>,
}

impl Batch {
    /// Creates a batch for tests. Not a valid batch for consensus.
    pub fn new_for_test(
        transactions: Vec<Bytes>,
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

    /// Returns the in-memory size of the batch in bytes, including its transactions.
    pub fn size(&self) -> usize {
        size_of::<Self>() + self.transactions.iter().map(|tx| tx.len()).sum::<usize>()
    }

    /// Returns the digest of the batch: the hash of its wire encoding, which excludes
    /// `received_at`.
    pub fn digest(&self) -> BlockHash {
        let mut hasher = crypto::DefaultHashFunction::new();
        hasher.update(encode(self).as_ref());
        BlockHash::from_slice(hasher.finalize().as_bytes())
    }

    /// Returns the encoded transactions.
    pub fn transactions(&self) -> &Vec<Bytes> {
        &self.transactions
    }

    /// Returns the encoded transactions mutably.
    pub fn transactions_mut(&mut self) -> &mut Vec<Bytes> {
        &mut self.transactions
    }

    /// Returns when this node received the batch, if it is not its own.
    pub fn received_at(&self) -> Option<TimestampSec> {
        self.received_at
    }

    /// Records when this node received the batch.
    pub fn set_received_at(&mut self, time: TimestampSec) {
        self.received_at = Some(time)
    }

    /// Seals the batch with a known digest.
    ///
    /// WARNING: this does not verify the digest matches the batch.
    pub fn seal(self, digest: BlockHash) -> SealedBatch {
        SealedBatch::new(self, digest)
    }

    /// Computes the digest and seals the batch so it can no longer change.
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

/// Returns the gas limit per batch in effect for `epoch`.
///
/// Epoch-keyed so a future fork can change it at an epoch boundary.
pub fn max_batch_gas(_epoch: Epoch) -> u64 {
    ETHEREUM_BLOCK_GAS_LIMIT_56BITS
}

/// Returns the byte-size limit per batch in effect for `epoch`; a larger batch fails to decode.
///
/// Epoch-keyed so a future fork can change it at an epoch boundary.
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

/// Validation and admission of transactions, whether forwarded singly or received in a peer's
/// batch.
#[async_trait::async_trait]
pub trait BatchValidation: Send + Sync + Debug {
    /// Determines whether this batch can be voted on.
    async fn validate_batch(&self, b: &SealedBatch) -> Result<(), BatchValidationError>;

    /// Submits encoded transactions to this node's pool, but only when the first transaction's
    /// sender maps to a slot this validator covers.
    fn submit_batch_if_mine(
        &self,
        tx_bytes: &[Bytes],
        slots: &CommitteeSlots,
    ) -> Result<(), SubmitBatchError>;

    /// Admits transactions forwarded directly by an observer, returning the hashes rejected as
    /// stale (already executed) so the sender stops re-forwarding them.
    ///
    /// Takes the decoded bytes by value so the owned buffer moves into the blocking recovery task
    /// without a copy.
    async fn submit_forwarded_txns(&self, tx_bytes: Vec<Bytes>) -> Vec<BlockHash>;
}

/// Errors that can occur during batch submission.
#[derive(Error, Debug)]
pub enum SubmitBatchError {
    /// The transaction bytes do not decode.
    #[error("Invalid transaction bytes")]
    InvalidTransactionBytes,
}

/// Batch validation errors.
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
    /// The batch's base fee differs from the one this node expects.
    #[error("Invalid base fee, expected {expected_base_fee} got {base_fee}")]
    InvalidBaseFee {
        /// The base fee this node expects.
        expected_base_fee: u64,
        /// The base fee the batch carries.
        base_fee: u64,
    },
    /// The batch's worker id is not the one this worker pairs with.
    #[error("Invalid worker id, expected {expected_worker_id} got {worker_id}")]
    InvalidWorkerId {
        /// The worker id this node expects.
        expected_worker_id: WorkerId,
        /// The worker id the batch carries.
        worker_id: WorkerId,
    },
    /// The batch contains blob transactions EIP-4844.
    #[error("Proposed batch contains blob transaction. Tx hash: {0}")]
    InvalidTx4844(BlockHash),
    /// The total allowable gas in the batch exceeds `u64::MAX`.
    #[error("Overflow calculating max possible gas.")]
    GasOverflow,
    /// The batch belongs to a different epoch.
    #[error("Invalid epoch, expected epoch {expected} got epoch {found}")]
    InvalidEpoch {
        /// The current epoch.
        expected: Epoch,
        /// The epoch the batch carries.
        found: Epoch,
    },
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

#[cfg(test)]
mod batch_wire_tests {
    use super::*;
    use alloy::primitives::b256;

    /// A transaction-shaped fixture set: empty, tiny, and multi-KB payloads, crossing the 128-byte
    /// ULEB128 length boundary where bcs switches to a two-byte length prefix.
    fn tx_fixtures() -> Vec<Vec<u8>> {
        vec![vec![], vec![0x01], vec![0xab; 127], vec![0xcd; 128], vec![0xef; 3000]]
    }

    /// A `Vec<u8>` payload and a `Bytes` payload encode to byte-identical bcs, in both directions.
    /// bcs is positional and non-self-describing, so this equality is what makes the transaction
    /// container a node-local choice rather than a wire-format change: an un-upgraded peer reads
    /// either shape as the same bytes.
    #[test]
    fn tx_payload_bcs_identical_for_vec_u8_and_bytes() {
        let vecs = tx_fixtures();
        let bytes: Vec<Bytes> = vecs.iter().cloned().map(Bytes::from).collect();

        let vecs_encoded = bcs::to_bytes(&vecs).expect("encode Vec<Vec<u8>>");
        let bytes_encoded = bcs::to_bytes(&bytes).expect("encode Vec<Bytes>");
        assert_eq!(vecs_encoded, bytes_encoded);

        // Cross-decode: each shape decodes what the other encoded.
        let decoded_bytes: Vec<Bytes> = bcs::from_bytes(&vecs_encoded).expect("decode as Bytes");
        let decoded_vecs: Vec<Vec<u8>> = bcs::from_bytes(&bytes_encoded).expect("decode as Vec");
        assert_eq!(decoded_bytes, bytes);
        assert_eq!(decoded_vecs, vecs);
    }

    /// Pins the full `Batch` wire encoding (and therefore its digest) to golden bytes, so any
    /// encoding drift - field reorder, container change, serde attribute change - fails here.
    #[test]
    fn batch_encoding_golden_bytes() {
        let batch = Batch {
            transactions: tx_fixtures().into_iter().map(Into::into).collect(),
            epoch: 7,
            beneficiary: Address::repeat_byte(0x42),
            base_fee_per_gas: 1_000_000_007,
            worker_id: 3,
            seq: 99,
            received_at: Some(123), // #[serde(skip)]: must not affect the encoding
        };
        assert_eq!(encode(&batch).len(), 3307);
        assert_eq!(
            batch.digest(),
            BlockHash::from(b256!(
                "57f612eb1bc9989d90e87cb205e2a3e35332613695a7ef2f1ca70e63bb82784f"
            ))
        );
    }
}
