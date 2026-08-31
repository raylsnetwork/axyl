//! Lightweight batch/transaction lifecycle tracking service.
//!
//! Monitors batch flow across CL and EL, detecting drops, gaps, and
//! out-of-order delivery via structured `[batch_tracker]` logging.

use crate::SenderNonceRanges;
use dashmap::DashMap;
use std::{
    fmt,
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};
use tracing::{debug, trace, warn};

/// Lifecycle stages a batch passes through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum BatchStage {
    /// Worker sealed the batch.
    Sealed = 1 << 0,
    /// Quorum vote succeeded.
    QuorumReached = 1 << 1,
    /// `report_own_batch` sent to primary.
    ReportedToPrimary = 1 << 2,
    /// Proposer received digest.
    QueuedInProposer = 1 << 3,
    /// Proposer overflow dropped this digest.
    DroppedFromProposer = 1 << 4,
    /// Header contains this digest.
    IncludedInHeader = 1 << 5,
    /// Bullshark committed subdag containing this batch.
    CommittedInSubdag = 1 << 6,
    /// Subscriber broadcast ConsensusOutput.
    OutputBroadcast = 1 << 7,
    /// EVM building block from this batch.
    Executing = 1 << 8,
    /// Block finalized for this batch.
    Executed = 1 << 9,
    /// Batch skipped by dedup guard (already executed via a different output).
    Deduped = 1 << 10,
    /// Batch parked awaiting predecessor (may drain later).
    Parked = 1 << 11,
}

impl fmt::Display for BatchStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Sealed => "Sealed",
            Self::QuorumReached => "QuorumReached",
            Self::ReportedToPrimary => "ReportedToPrimary",
            Self::QueuedInProposer => "QueuedInProposer",
            Self::DroppedFromProposer => "DroppedFromProposer",
            Self::IncludedInHeader => "IncludedInHeader",
            Self::CommittedInSubdag => "CommittedInSubdag",
            Self::OutputBroadcast => "OutputBroadcast",
            Self::Executing => "Executing",
            Self::Executed => "Executed",
            Self::Deduped => "Deduped",
            Self::Parked => "Parked",
        })
    }
}

/// Reasons a transaction may be dropped during EVM execution.
#[derive(Debug, Clone)]
pub enum TxDropReason {
    /// Failed to decode the raw transaction bytes.
    DecodeFailure(String),
    /// Transaction rejected by the EVM (e.g. duplicate, invalid nonce).
    ValidationFailure(String),
}

impl fmt::Display for TxDropReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DecodeFailure(e) => write!(f, "decode_failure: {e}"),
            Self::ValidationFailure(e) => write!(f, "validation_failure: {e}"),
        }
    }
}

/// Categorized transaction validation failures for a batch.
#[derive(Debug)]
pub struct TxValidationReport {
    pub digest: crate::BlockHash,
    pub nonce_too_high: u32,
    pub nonce_too_low: u32,
    pub other: u32,
    pub sender_nonce_ranges: SenderNonceRanges,
    /// Per-tx diagnostic info: (tx_hash, sender, tx_nonce, state_nonce).
    pub nonce_too_high_details: Vec<(crate::BlockHash, crate::Address, u64, u64)>,
}

/// Lightweight tracking entry for a single batch.
struct BatchEntry {
    created_at: Instant,
    /// Bitset of reached stages.
    stages: u16,
    /// EL block number if executed.
    block_number: Option<u64>,
    /// Total txs in batch (set when executing).
    tx_count: Option<usize>,
    /// Count of dropped txs during EVM execution.
    dropped_txs: u32,
}

impl Default for BatchEntry {
    fn default() -> Self {
        Self {
            created_at: Instant::now(),
            stages: 0,
            block_number: None,
            tx_count: None,
            dropped_txs: 0,
        }
    }
}

impl BatchEntry {
    fn mark(&mut self, stage: BatchStage) {
        self.stages |= stage as u16;
    }

    fn has(&self, stage: BatchStage) -> bool {
        self.stages & (stage as u16) != 0
    }

    fn stages_str(&self) -> String {
        const ALL: [BatchStage; 12] = [
            BatchStage::Sealed,
            BatchStage::QuorumReached,
            BatchStage::ReportedToPrimary,
            BatchStage::QueuedInProposer,
            BatchStage::DroppedFromProposer,
            BatchStage::IncludedInHeader,
            BatchStage::CommittedInSubdag,
            BatchStage::OutputBroadcast,
            BatchStage::Executing,
            BatchStage::Executed,
            BatchStage::Deduped,
            BatchStage::Parked,
        ];
        ALL.iter().filter(|s| self.has(**s)).map(|s| s.to_string()).collect::<Vec<_>>().join(",")
    }
}

/// Tracks consensus output lifecycle.
struct OutputEntry {
    committed_at: Instant,
}

/// Always-on batch lifecycle tracker shared via `ConsensusBus`.
pub struct BatchTracker {
    batches: DashMap<crate::BlockHash, BatchEntry>,
    outputs: DashMap<u64, OutputEntry>,
    // counters
    total_tracked: AtomicU64,
    total_dropped_proposer: AtomicU64,
    total_txs_dropped: AtomicU64,
    total_nonce_too_high: AtomicU64,
    total_nonce_too_low: AtomicU64,
    total_other_invalid: AtomicU64,
}

impl fmt::Debug for BatchTracker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchTracker")
            .field("batches", &self.batches.len())
            .field("outputs", &self.outputs.len())
            .finish()
    }
}

impl Default for BatchTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl BatchTracker {
    /// Create a new tracker.
    pub fn new() -> Self {
        Self {
            batches: DashMap::new(),
            outputs: DashMap::new(),
            total_tracked: AtomicU64::new(0),
            total_dropped_proposer: AtomicU64::new(0),
            total_txs_dropped: AtomicU64::new(0),
            total_nonce_too_high: AtomicU64::new(0),
            total_nonce_too_low: AtomicU64::new(0),
            total_other_invalid: AtomicU64::new(0),
        }
    }

    // ── CL tracking calls ──

    /// Worker sealed a batch.
    pub fn batch_sealed(
        &self,
        digest: crate::BlockHash,
        tx_count: usize,
        sender_nonce_ranges: &SenderNonceRanges,
    ) {
        self.total_tracked.fetch_add(1, Ordering::Relaxed);
        let mut entry = self.batches.entry(digest).or_default();
        entry.mark(BatchStage::Sealed);
        let sender_count = sender_nonce_ranges.len();
        trace!(
            target: "batch_tracker",
            ?digest,
            tx_count,
            sender_count,
            "batch_sealed"
        );
        for (sender, range) in sender_nonce_ranges {
            trace!(
                target: "batch_tracker",
                ?digest,
                ?sender,
                nonce_min = range.min,
                nonce_max = range.max,
                nonce_span = range.max - range.min + 1,
                "batch_sealed_sender_range"
            );
        }
    }

    /// Batch reached quorum.
    pub fn batch_quorum_reached(&self, digest: crate::BlockHash) {
        let mut entry = self.batches.entry(digest).or_default();
        entry.mark(BatchStage::QuorumReached);
        trace!(target: "batch_tracker", ?digest, "batch_quorum_reached");
    }

    /// Batch reported to primary via `report_own_batch`.
    pub fn batch_reported_to_primary(&self, digest: crate::BlockHash) {
        let mut entry = self.batches.entry(digest).or_default();
        entry.mark(BatchStage::ReportedToPrimary);
        trace!(target: "batch_tracker", ?digest, "batch_reported_to_primary");
    }

    /// Proposer received a digest.
    pub fn digest_queued_in_proposer(&self, digest: crate::BlockHash) {
        let mut entry = self.batches.entry(digest).or_default();
        entry.mark(BatchStage::QueuedInProposer);
        trace!(target: "batch_tracker", ?digest, "digest_queued_in_proposer");
    }

    /// Proposer dropped a digest due to queue overflow.
    pub fn digest_dropped_from_proposer(&self, digest: crate::BlockHash) {
        self.total_dropped_proposer.fetch_add(1, Ordering::Relaxed);
        let mut entry = self.batches.entry(digest).or_default();
        entry.mark(BatchStage::DroppedFromProposer);
        warn!(target: "batch_tracker", ?digest, "digest_dropped_from_proposer");
    }

    /// Digests included in a proposed header.
    pub fn digests_included_in_header(&self, digests: &[(crate::BlockHash, crate::WorkerId)]) {
        for (digest, _) in digests {
            let mut entry = self.batches.entry(*digest).or_default();
            entry.mark(BatchStage::IncludedInHeader);
        }
        trace!(target: "batch_tracker", count = digests.len(), "digests_included_in_header");
    }

    /// Subdag committed by Bullshark, containing these batch digests.
    pub fn subdag_committed(&self, output_number: u64, digests: &[crate::BlockHash]) {
        for digest in digests {
            let mut entry = self.batches.entry(*digest).or_default();
            entry.mark(BatchStage::CommittedInSubdag);
        }
        self.outputs
            .entry(output_number)
            .or_insert_with(|| OutputEntry { committed_at: Instant::now() });
        trace!(target: "batch_tracker", output_number, batch_count = digests.len(), "subdag_committed");
    }

    /// Subscriber broadcast ConsensusOutput.
    pub fn output_broadcast(&self, output_number: u64, digests: &[crate::BlockHash]) {
        for digest in digests {
            let mut entry = self.batches.entry(*digest).or_default();
            entry.mark(BatchStage::OutputBroadcast);
        }
        trace!(target: "batch_tracker", output_number, batch_count = digests.len(), "output_broadcast");
    }

    // ── EL tracking calls ──

    /// Processor received an output (after dedup).
    pub fn output_received(&self, output_number: u64) {
        trace!(target: "batch_tracker", output_number, "output_received");
    }

    /// Processor dropped a duplicate output.
    pub fn output_duplicate_dropped(&self, output_number: u64) {
        warn!(target: "batch_tracker", output_number, "output_duplicate_dropped");
    }

    /// EVM is about to execute a batch.
    pub fn batch_executing(&self, digest: crate::BlockHash, tx_count: usize) {
        let mut entry = self.batches.entry(digest).or_default();
        entry.mark(BatchStage::Executing);
        entry.tx_count = Some(tx_count);
        trace!(target: "batch_tracker", ?digest, tx_count, "batch_executing");
    }

    /// A batch was fully executed as an EVM block.
    pub fn batch_executed(&self, digest: crate::BlockHash, block_number: u64) {
        let mut entry = self.batches.entry(digest).or_default();
        entry.mark(BatchStage::Executed);
        entry.block_number = Some(block_number);
        trace!(target: "batch_tracker", ?digest, block_number, "batch_executed");
    }

    /// Batch skipped by the dedup guard (already executed via a different output).
    pub fn batch_deduped(&self, digest: crate::BlockHash) {
        let mut entry = self.batches.entry(digest).or_default();
        entry.mark(BatchStage::Deduped);
        trace!(target: "batch_tracker", ?digest, "batch_deduped");
    }

    /// Batch parked awaiting its predecessor in the seq ordering.
    pub fn batch_parked(&self, digest: crate::BlockHash) {
        let mut entry = self.batches.entry(digest).or_default();
        entry.mark(BatchStage::Parked);
        trace!(target: "batch_tracker", ?digest, "batch_parked");
    }

    /// Output fully executed (all its batches).
    pub fn output_executed(&self, output_number: u64) {
        trace!(target: "batch_tracker", output_number, "output_executed");
    }

    /// A transaction was dropped during EVM execution.
    pub fn tx_dropped(&self, digest: crate::BlockHash, reason: TxDropReason) {
        self.total_txs_dropped.fetch_add(1, Ordering::Relaxed);
        if let Some(mut entry) = self.batches.get_mut(&digest) {
            entry.dropped_txs += 1;
        }
        warn!(target: "batch_tracker", ?digest, %reason, "tx_dropped");
    }

    /// Report categorized tx validation failures for a batch.
    pub fn tx_validation_counts(&self, report: &TxValidationReport) {
        let total = report.nonce_too_high + report.nonce_too_low + report.other;
        if total == 0 {
            return;
        }
        self.total_txs_dropped.fetch_add(total as u64, Ordering::Relaxed);
        self.total_nonce_too_high.fetch_add(report.nonce_too_high as u64, Ordering::Relaxed);
        self.total_nonce_too_low.fetch_add(report.nonce_too_low as u64, Ordering::Relaxed);
        self.total_other_invalid.fetch_add(report.other as u64, Ordering::Relaxed);
        if let Some(mut entry) = self.batches.get_mut(&report.digest) {
            entry.dropped_txs += total;
        }
        if report.nonce_too_high > 0 {
            warn!(
                target: "batch_tracker",
                digest = ?report.digest,
                report.nonce_too_high,
                report.nonce_too_low,
                report.other,
                sender_count = report.sender_nonce_ranges.len(),
                "tx_dropped_nonce_too_high"
            );
            // log per-sender nonce ranges for gap analysis
            for (sender, range) in &report.sender_nonce_ranges {
                warn!(
                    target: "batch_tracker",
                    digest = ?report.digest,
                    ?sender,
                    nonce_min = range.min,
                    nonce_max = range.max,
                    nonce_span = range.max - range.min + 1,
                    "nonce_range_for_sender"
                );
            }
            // Per-tx detail is noisy (one line per dropped tx — thousands in a single-sender
            // burst), and the per-sender range above already carries the actionable gap
            // info. Keep it at debug for when you're actually chasing a nonce gap
            // (RUST_LOG=batch_tracker=debug).
            for (tx_hash, sender, tx_nonce, state_nonce) in &report.nonce_too_high_details {
                debug!(
                    target: "batch_tracker",
                    digest = ?report.digest,
                    ?tx_hash,
                    ?sender,
                    tx_nonce,
                    state_nonce,
                    nonce_gap = tx_nonce.saturating_sub(*state_nonce),
                    "nonce_too_high_detail"
                );
            }
        } else if report.other > 0 {
            trace!(
                target: "batch_tracker",
                digest = ?report.digest,
                report.nonce_too_low,
                report.other,
                sender_count = report.sender_nonce_ranges.len(),
                "tx_validation_drops"
            );
        }
    }

    // ── Periodic reporting ──

    /// Check for batches stuck at intermediate stages and log gaps.
    ///
    /// Call periodically (e.g. every 30s or on each new block notification).
    /// Combines stuck-batch detection and cleanup in a single `retain` pass
    /// to avoid multiple O(n) iterations over the DashMap.
    pub fn check_gaps(&self) {
        let now = Instant::now();
        let stale_threshold = std::time::Duration::from_secs(60);
        let cleanup_threshold = std::time::Duration::from_secs(300);
        let mut stuck_count = 0u64;

        // Single pass: detect stuck batches and evict entries older than 5 min.
        self.batches.retain(|digest, entry| {
            let age = now.duration_since(entry.created_at);
            if age >= cleanup_threshold {
                return false; // evict
            }
            if age >= stale_threshold {
                let has_sealed = entry.has(BatchStage::Sealed);
                let is_terminal = entry.has(BatchStage::Executed)
                    || entry.has(BatchStage::DroppedFromProposer)
                    || entry.has(BatchStage::Deduped);
                if has_sealed && !is_terminal {
                    stuck_count += 1;
                    if stuck_count <= 10 {
                        warn!(
                            target: "batch_tracker",
                            ?digest,
                            stages = %entry.stages_str(),
                            age_secs = age.as_secs(),
                            "batch stuck at intermediate stage"
                        );
                    }
                }
            }
            true // keep
        });

        // Always log periodic summary at info level so it's visible
        let total_tracked = self.total_tracked.load(Ordering::Relaxed);
        let total_dropped = self.total_dropped_proposer.load(Ordering::Relaxed);
        let total_txs_dropped = self.total_txs_dropped.load(Ordering::Relaxed);
        let total_nonce_too_high = self.total_nonce_too_high.load(Ordering::Relaxed);
        let total_nonce_too_low = self.total_nonce_too_low.load(Ordering::Relaxed);
        let total_other_invalid = self.total_other_invalid.load(Ordering::Relaxed);
        trace!(
            target: "batch_tracker",
            tracked_batches = self.batches.len(),
            tracked_outputs = self.outputs.len(),
            total_tracked,
            total_dropped_proposer = total_dropped,
            total_txs_dropped,
            total_nonce_too_high,
            total_nonce_too_low,
            total_other_invalid,
            stuck_count,
            "periodic gap check"
        );

        if stuck_count > 10 {
            warn!(
                target: "batch_tracker",
                stuck_count,
                "(showing first 10 stuck batches only)"
            );
        }

        // Clean old outputs first, then check gaps on the smaller set.
        self.outputs.retain(|_, entry| now.duration_since(entry.committed_at) < cleanup_threshold);
        self.check_output_gaps();
    }

    /// Detect gaps in output numbers.
    fn check_output_gaps(&self) {
        let mut numbers: Vec<u64> = self.outputs.iter().map(|e| *e.key()).collect();
        if numbers.len() < 2 {
            return;
        }
        numbers.sort_unstable();
        for window in numbers.windows(2) {
            let gap = window[1] - window[0];
            if gap > 1 {
                warn!(
                    target: "batch_tracker",
                    from = window[0],
                    to = window[1],
                    gap,
                    "output number gap detected"
                );
            }
        }
    }

    /// Log a summary of current tracking state.
    pub fn summary(&self) {
        trace!(
            target: "batch_tracker",
            tracked_batches = self.batches.len(),
            tracked_outputs = self.outputs.len(),
            total_tracked = self.total_tracked.load(Ordering::Relaxed),
            total_dropped_proposer = self.total_dropped_proposer.load(Ordering::Relaxed),
            total_txs_dropped = self.total_txs_dropped.load(Ordering::Relaxed),
            total_nonce_too_high = self.total_nonce_too_high.load(Ordering::Relaxed),
            total_nonce_too_low = self.total_nonce_too_low.load(Ordering::Relaxed),
            total_other_invalid = self.total_other_invalid.load(Ordering::Relaxed),
            "batch tracker summary"
        );
    }
}
