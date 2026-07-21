//! The background-archival driver the node runs off the consensus path.
//!
//! [`ColdArchiver`] owns a RAW hot database plus its [`ColdStore`] and exposes the scheduled
//! operations: [`ColdArchiver::reconcile`] at boot, [`ColdArchiver::seal_due`] from the live seal
//! actor, and [`ColdArchiver::archive_due`] for the fused boot/offline drain. The cutoff is the
//! committed epoch floored by the caller's EL execution anchor.

use std::sync::Arc;

use rayls_infrastructure_types::{ConsensusHeaderMeta, Database, DbTx, Epoch};
use tracing::info;

use super::{
    archive_below_epoch,
    producer::{finalize_sealed, seal_next_epoch_jars, JarSeal, PRUNE_YIELD, SEAL_CHUNK_BYTES},
    reconcile, ArchiveStats, ColdStore, SealOutcome,
};
use crate::tables::ConsensusBlocks;

/// Drives cold archival against a raw hot database and its cold store.
///
/// `hot` must be the raw hot database, never a [`ColdDatabase`](super::ColdDatabase) wrapper, so
/// the producer's reads and deletes stay on the hot tier (see [`archive_below_epoch`]).
pub struct ColdArchiver<DB: Database> {
    hot: DB,
    cold: Arc<ColdStore>,
}

impl<DB: Database> std::fmt::Debug for ColdArchiver<DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Hand-rolled so the archiver is `Debug` without requiring `DB: Debug`; the inner database
        // and cold store are not usefully printable here.
        f.debug_struct("ColdArchiver").finish_non_exhaustive()
    }
}

impl<DB: Database> ColdArchiver<DB> {
    /// Builds an archiver over a raw hot database and its shared cold store.
    pub fn new(hot: DB, cold: Arc<ColdStore>) -> Self {
        Self { hot, cold }
    }

    /// Completes every sealed-but-unfinalized epoch (index + high-water + hot prune) and heals a
    /// crash-interrupted archive.
    ///
    /// Runs once at boot before serving (node and `cold-migrate`): a jar-driven rebuild sweeping
    /// whatever a cancelled or crashed [`Self::seal_due`] pass left behind.
    pub fn reconcile(&self) -> eyre::Result<()> {
        reconcile(&self.hot, &self.cold).map_err(|e| eyre::eyre!("cold reconcile failed: {e}"))
    }

    /// Archives every epoch safely below the current committed epoch into cold.
    ///
    /// `el_anchor_epoch` is the leader epoch of the consensus header the highest executed EVM
    /// block commits to; the cutoff is floored by it so a pruned epoch is always one the EL has
    /// executed, even if consensus/EL lockstep is ever violated. `max_epochs` caps one pass
    /// (`None` is unbounded); a zeroed [`ArchiveStats`] means nothing was eligible.
    pub fn archive_due(
        &self,
        el_anchor_epoch: Epoch,
        max_epochs: Option<usize>,
    ) -> eyre::Result<ArchiveStats> {
        let Some(cutoff) = self.cutoff_epoch(el_anchor_epoch) else {
            return Ok(ArchiveStats::default());
        };
        archive_below_epoch(&self.hot, &self.cold, cutoff, max_epochs)
            .map_err(|e| eyre::eyre!("cold archive failed: {e}"))
    }

    /// Archives at most one due epoch fully, off the boundary: seal the jars, then finalize
    /// (index + high-water + a yielding hot prune), so no archival runs on the epoch transition.
    ///
    /// `should_cancel` is polled at each seal chunk seam and prune batch; a cancel leaves the
    /// jars uncommitted (re-sealed whole later) or leftover hot rows for [`Self::reconcile`] to
    /// sweep. Every row is always in at least one tier.
    pub fn seal_due(
        &self,
        el_anchor_epoch: Epoch,
        should_cancel: impl Fn() -> bool,
    ) -> eyre::Result<SealOutcome> {
        let Some(cutoff) = self.cutoff_epoch(el_anchor_epoch) else {
            return Ok(SealOutcome::Drained);
        };
        let seal_started = std::time::Instant::now();
        let seal =
            seal_next_epoch_jars(&self.hot, &self.cold, cutoff, SEAL_CHUNK_BYTES, &should_cancel)
                .map_err(|e| eyre::eyre!("cold jar seal failed: {e}"))?;
        Ok(match seal {
            JarSeal::Sealed(sealed) => {
                let seal_elapsed = seal_started.elapsed();
                let finalize_started = std::time::Instant::now();
                finalize_sealed(&self.hot, &sealed, &should_cancel, PRUNE_YIELD)
                    .map_err(|e| eyre::eyre!("cold finalize failed: {e}"))?;
                // Per-phase split: seal = read+compress+append, finalize = index commit + paced
                // prune. A slow pass is diagnosed from this line alone.
                info!(
                    target: "cold-archive",
                    epoch = sealed.epoch,
                    blocks = sealed.numbers.end() - sealed.numbers.start() + 1,
                    batch_rows = sealed.locations.len(),
                    seal = ?seal_elapsed,
                    finalize = ?finalize_started.elapsed(),
                    "cold pass phases",
                );
                SealOutcome::Sealed(sealed.epoch)
            }
            JarSeal::Drained => SealOutcome::Drained,
            JarSeal::Cancelled => SealOutcome::Cancelled,
        })
    }

    /// Returns the exclusive archival cutoff, or `None` when nothing is eligible.
    ///
    /// Exclusive at the committed epoch (at a boundary: the just-closed one, which thus stays hot
    /// through the transition), floored by the EL anchor so archival never prunes an epoch the EL
    /// has not fully executed: an anchor inside epoch `A` proves everything below `A` executed.
    fn cutoff_epoch(&self, el_anchor_epoch: Epoch) -> Option<Epoch> {
        let current = self.current_epoch()?;
        let cutoff = current.min(el_anchor_epoch);
        (cutoff > 0).then_some(cutoff)
    }

    /// Returns the current committed epoch: the leader epoch of the highest hot consensus block.
    ///
    /// The latest subdag is always hot (never archived), so the raw reverse peek resolves on the
    /// hot tier; the epoch is projected from the stored bytes without decoding the full subdag.
    fn current_epoch(&self) -> Option<Epoch> {
        self.hot
            .with_read_txn(|tx| {
                Ok(tx
                    .reverse_raw_iter::<ConsensusBlocks>()
                    .next()
                    .and_then(|(_, value)| ConsensusHeaderMeta::from_bytes(&value).ok())
                    .map(|meta| meta.leader_epoch))
            })
            .ok()
            .flatten()
    }
}
