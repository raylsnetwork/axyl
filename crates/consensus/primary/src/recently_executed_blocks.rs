//! Track the most recent execution blocks for the consensus layer.

use rayls_infrastructure_types::{
    nonce::unpack_nonce, BlockHash, BlockNumHash, SealedHeader, B256,
};
use std::{collections::VecDeque, fmt};

/// The epoch of the committed sub-DAG leader that ordered a [`RecentlyExecutedBlock`].
///
/// A distinct newtype — deliberately NOT the bare `Epoch`/`u32` alias — so the compiler rejects
/// mixing dimensions: you cannot compare a `SubDagLeaderEpoch` against a frontier epoch or against
/// a [`SubDagLeaderRound`] without explicitly unwrapping via [`get`](Self::get). Only this module
/// mints one (from a block nonce), so a value of this type always means "the leader epoch this
/// block was ordered under", never the frontier.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubDagLeaderEpoch(u32);

/// The round of the committed sub-DAG leader that ordered a [`RecentlyExecutedBlock`].
///
/// A distinct newtype — deliberately NOT the bare `Round`/`u32` alias — so the compiler rejects
/// comparing it against a frontier round (the exact mistake that once wedged the proposer throttle:
/// `frontier_round - tip_round` where the tip carried an old leader round) or against a
/// [`SubDagLeaderEpoch`]. Unwrap via [`get`](Self::get) only when you genuinely need the raw value.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubDagLeaderRound(u32);

impl SubDagLeaderEpoch {
    /// The raw epoch value. Prefer keeping the newtype; unwrap only at a genuine boundary.
    pub fn get(self) -> u32 {
        self.0
    }
}

impl SubDagLeaderRound {
    /// The raw round value. Prefer keeping the newtype; unwrap only at a genuine boundary.
    pub fn get(self) -> u32 {
        self.0
    }
}

impl fmt::Display for SubDagLeaderEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for SubDagLeaderRound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The digest of the committed sub-DAG's `ConsensusHeader` that a [`RecentlyExecutedBlock`] was
/// ordered under (carried on the block as its `parent_beacon_block_root`).
///
/// A distinct newtype — deliberately NOT a bare `B256` — because this carries the same provenance
/// hazard as [`SubDagLeaderRound`]: a block drained from an OLD parked output lands as the tip yet
/// commits to that old output's consensus header, so comparing this digest against the *frontier*
/// consensus tip (`some_digest == frontier_digest`) would falsely conclude "the tip is caught up".
///
/// It intentionally does NOT derive `PartialEq`/`Eq`: a frontier digest is a bare `B256`, so that
/// comparison already fails to compile, and there is no legitimate reason to equality-check two of
/// these against each other either. Use [`get`](Self::get) only to feed a genuine lookup (e.g.
/// `get_consensus_by_hash`); if you find yourself unwrapping to compare, that is the bug this type
/// exists to surface.
#[derive(Copy, Clone, Debug)]
pub struct SubDagConsensusDigest(B256);

impl SubDagConsensusDigest {
    /// The raw digest. Unwrap only to look the header up, never to compare against a frontier
    /// digest.
    pub fn get(self) -> B256 {
        self.0
    }
}

impl fmt::Display for SubDagConsensusDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A block returned from [`RecentlyExecutedBlocks`] (e.g. by
/// [`RecentlyExecutedBlocks::latest_block`]).
///
/// This wraps the underlying [`SealedHeader`] for one reason: the epoch and round packed into a
/// block's nonce are the epoch and round of the committed sub-DAG **leader** that ordered it
/// (`ConsensusOutput::nonce()` == `sub_dag.leader.nonce()`), NOT the execution frontier. Draining
/// a parked (out-of-order seq) batch executes a block belonging to an OLDER output yet lands it as
/// the newest height, so the tip's nonce can encode a leader epoch/round far below the true
/// frontier. See the warning on [`RecentlyExecutedBlocks::latest_block`] for the concrete wedge
/// this caused.
///
/// To make that impossible to stumble into, the provenance accessors are named
/// [`subdag_leader_epoch`](Self::subdag_leader_epoch) /
/// [`subdag_leader_round`](Self::subdag_leader_round) /
/// [`subdag_consensus_digest`](Self::subdag_consensus_digest) rather than the tempting `epoch()` /
/// `round()` / raw beacon root, and each returns a distinct newtype so the compiler rejects
/// comparing it to a frontier value. There is deliberately NO `Deref` to [`SealedHeader`]: safe
/// fields are delegated explicitly ([`number`](Self::number), [`hash`](Self::hash),
/// [`num_hash`](Self::num_hash)), and raw header access is only via [`as_header`](Self::as_header)
/// / [`into_header`](Self::into_header) — a deliberate, greppable exit from the guard (e.g. to
/// carry the header over across an epoch reset).
#[derive(Clone, Debug)]
pub struct RecentlyExecutedBlock(SealedHeader);

impl RecentlyExecutedBlock {
    /// The epoch of the committed sub-DAG leader that ordered this block, decoded from the block
    /// nonce (`epoch << 32 | round`). Equals the committing `ConsensusOutput::leader_epoch()`.
    ///
    /// This is NOT the execution frontier epoch: a batch drained from a previous epoch tags this
    /// block with that leader's old epoch even though execution has moved on. Never use this as
    /// "the current epoch".
    pub fn subdag_leader_epoch(&self) -> SubDagLeaderEpoch {
        SubDagLeaderEpoch(unpack_nonce(u64::from(self.0.nonce)).0)
    }

    /// The round of the committed sub-DAG leader that ordered this block, decoded from the block
    /// nonce (`epoch << 32 | round`). Equals the committing `ConsensusOutput::leader_round()`.
    ///
    /// This is NOT the execution frontier round: a drained parked batch can carry a leader round
    /// far below the true frontier (this is exactly what wedged the proposer throttle — see
    /// [`RecentlyExecutedBlocks::latest_block`]). Never derive the frontier round from this.
    pub fn subdag_leader_round(&self) -> SubDagLeaderRound {
        SubDagLeaderRound(unpack_nonce(u64::from(self.0.nonce)).1)
    }

    /// The block number (height). Monotonic — always the true tip, safe to compare.
    pub fn number(&self) -> u64 {
        self.0.number
    }

    /// The block hash (block identity — equality checks are legitimate fork detection).
    pub fn hash(&self) -> BlockHash {
        self.0.hash()
    }

    /// The block number and hash together.
    pub fn num_hash(&self) -> BlockNumHash {
        self.0.num_hash()
    }

    /// The consensus-header digest this block was ordered under (its `parent_beacon_block_root`),
    /// wrapped in [`SubDagConsensusDigest`] so it can't be silently compared to a frontier digest.
    /// `None` for a genesis/unset root.
    pub fn subdag_consensus_digest(&self) -> Option<SubDagConsensusDigest> {
        self.0.parent_beacon_block_root.map(SubDagConsensusDigest)
    }

    /// Consume the wrapper and return the owned underlying [`SealedHeader`].
    ///
    /// This is a deliberate exit from the guard: the raw header exposes `.nonce` and
    /// `.parent_beacon_block_root` as bare values, from which the leader epoch/round/consensus
    /// digest can be read and compared against a frontier value by mistake. Reach for it only when
    /// you genuinely need the whole header (e.g. to carry it over across an epoch reset), not to
    /// read provenance — use [`subdag_leader_epoch`], [`subdag_leader_round`], or
    /// [`subdag_consensus_digest`] for those.
    ///
    /// [`subdag_leader_epoch`]: Self::subdag_leader_epoch
    /// [`subdag_leader_round`]: Self::subdag_leader_round
    /// [`subdag_consensus_digest`]: Self::subdag_consensus_digest
    pub fn into_header(self) -> SealedHeader {
        self.0
    }

    /// Borrow the underlying [`SealedHeader`]. Same caveat as [`into_header`](Self::into_header):
    /// a deliberate exit from the guard, not the way to read epoch/round/consensus digest.
    pub fn as_header(&self) -> &SealedHeader {
        &self.0
    }
}

/// Tracks 'num_blocks' most recently executed block hashes and numbers.
#[derive(Clone, Debug)]
pub struct RecentlyExecutedBlocks {
    num_blocks: usize,
    blocks: VecDeque<SealedHeader>,
}

impl RecentlyExecutedBlocks {
    /// Create a RecentlyExecutedBlocks that will hold the 'num_blocks' most recently executed
    /// blocks.
    pub fn new(num_blocks: usize) -> Self {
        Self { num_blocks, blocks: VecDeque::new() }
    }

    /// Max number of blocks that can be held in RecentlyExecutedBlocks.
    pub fn block_capacity(&self) -> u64 {
        self.num_blocks as u64
    }

    /// Push the latest block onto RecentlyExecutedBlocks, will remove the oldest if needed to make
    /// room.
    pub fn push_latest(&mut self, latest: SealedHeader) {
        if self.blocks.len() >= self.num_blocks {
            self.blocks.pop_front();
        }
        self.blocks.push_back(latest);
    }

    /// Return the hash and number of the last executed block.
    /// This will return a default BlockNumHash if recents blocks are empty.
    /// This should only happen on node startup before any execution has taken
    /// place.  Most callsites will be fine with this, call is_empty() if this
    /// matters to you.
    pub fn latest_block_num_hash(&self) -> BlockNumHash {
        self.blocks.back().cloned().unwrap_or_else(Default::default).num_hash()
    }

    /// Return the number of the oldest block, or 0 if empty.
    pub fn oldest_block_number(&self) -> u64 {
        self.blocks.front().map(|h| h.number).unwrap_or(0)
    }

    /// Return the most recently pushed (highest block-number) executed block.
    ///
    /// WARNING: the tip's block *number* is monotonic, but the nonce it carries
    /// (`epoch << 32 | round`) is NOT - neither half. Draining a parked (out-of-order seq) batch
    /// executes a block that belongs to an OLDER output yet lands here as the newest height, so the
    /// tip's nonce reflects that origin output, not the frontier: its round can sit far below the
    /// true frontier round, and - when the drained batch was carried over from a previous epoch -
    /// its epoch can sit below the current epoch too.
    ///
    /// Example: execution has genuinely reached round 498. A batch for an earlier seq, mapping to
    /// round 200, was parked; the gap then fills and it is drained and executed now. That fresh
    /// block gets the next (highest) block number and becomes this tip, but its nonce encodes round
    /// 200. A caller reading the round here sees 200, not 498. The proposer throttle did exactly
    /// this: with consensus at round 500 it computed lag `500 - 200 = 300 > threshold` and
    /// throttled forever, wedging proposals - when the real lag was `500 - 498 = 2`. The epoch half
    /// regresses the same way: a batch drained from the previous epoch tags this tip with the old
    /// epoch.
    ///
    /// So never derive the execution frontier's epoch or round from this tip. Use the monotonic
    /// `executed_anchor` channel for the frontier, or scan the window for the max-nonce block when
    /// you must work from this window. The return type is [`RecentlyExecutedBlock`], whose only
    /// epoch/round accessors are
    /// [`subdag_leader_epoch`](RecentlyExecutedBlock::subdag_leader_epoch) /
    /// [`subdag_leader_round`](RecentlyExecutedBlock::subdag_leader_round) precisely so this trap
    /// is named at the callsite rather than silently reachable via `.epoch()` / `.round()`.
    ///
    /// On an empty window this returns a `RecentlyExecutedBlock` wrapping a default `SealedHeader`
    /// (number 0, zero hash, nonce 0) rather than `None` — same convention as
    /// [`latest_block_num_hash`](Self::latest_block_num_hash). Call [`is_empty`](Self::is_empty)
    /// first if a synthetic zero block would be mistaken for real execution history.
    pub fn latest_block(&self) -> RecentlyExecutedBlock {
        RecentlyExecutedBlock(self.blocks.back().cloned().unwrap_or_else(Default::default))
    }

    /// Is hash a block we have recently executed?
    pub fn contains_hash(&self, hash: BlockHash) -> bool {
        self.blocks.iter().any(|block| block.hash() == hash)
    }

    /// Get the block at a specific block number, if it exists in the recently-executed window.
    ///
    /// Returns a [`RecentlyExecutedBlock`] for the same reason [`latest_block`](Self::latest_block)
    /// does: a block found by number can still be one drained from a parked batch, so its
    /// epoch/round are creation-time values, not the frontier. The guarded accessors keep that
    /// from being read by accident.
    pub fn block_at_number(&self, number: u64) -> Option<RecentlyExecutedBlock> {
        self.blocks.iter().find(|block| block.number == number).cloned().map(RecentlyExecutedBlock)
    }

    /// Number of blocks actually stored.
    pub fn len(&self) -> usize {
        self.blocks.len()
    }

    /// Do we have any blocks?
    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rayls_infrastructure_types::{nonce::pack_nonce, ExecHeader};

    fn header_with_nonce(number: u64, epoch: u32, round: u32) -> SealedHeader {
        let header =
            ExecHeader { number, nonce: pack_nonce(epoch, round).into(), ..Default::default() };
        SealedHeader::seal_slow(header)
    }

    /// The full chain the prod bug lived on: push a block whose nonce encodes a specific
    /// (epoch, round), then read it back off the tip via the guarded accessors.
    #[test]
    fn subdag_leader_fields_decoded_from_tip_nonce() {
        let mut blocks = RecentlyExecutedBlocks::new(10);
        blocks.push_latest(header_with_nonce(42, 7, 498));

        let tip = blocks.latest_block();
        assert_eq!(tip.subdag_leader_epoch().get(), 7);
        assert_eq!(tip.subdag_leader_round().get(), 498);
        assert_eq!(tip.number(), 42);
    }

    /// The tip's leader round is NOT the frontier: a later-arriving block from an OLDER output
    /// (lower nonce round) becomes the tip by height and regresses the round read off it.
    #[test]
    fn drained_old_batch_regresses_tip_leader_round() {
        let mut blocks = RecentlyExecutedBlocks::new(10);
        blocks.push_latest(header_with_nonce(100, 3, 498)); // frontier round 498
        blocks.push_latest(header_with_nonce(101, 3, 200)); // drained parked batch, round 200

        let tip = blocks.latest_block();
        assert_eq!(tip.number(), 101, "tip is the newest height");
        assert_eq!(tip.subdag_leader_round().get(), 200, "…but its leader round regressed");
    }

    /// `block_at_number` yields the guarded wrapper for the same reason.
    #[test]
    fn block_at_number_exposes_guarded_accessors() {
        let mut blocks = RecentlyExecutedBlocks::new(10);
        blocks.push_latest(header_with_nonce(7, 2, 9));

        let found = blocks.block_at_number(7).expect("block present");
        assert_eq!(found.subdag_leader_epoch().get(), 2);
        assert_eq!(found.subdag_leader_round().get(), 9);
        assert!(blocks.block_at_number(999).is_none());
    }
}
