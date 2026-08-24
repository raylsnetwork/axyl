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
    producer::{
        finalize_sealed, seal_next_epoch_jars, Finalized, JarSeal, PRUNE_YIELD, SEAL_CHUNK_BYTES,
    },
    reconcile, ArchiveStats, ColdStore, SealOutcome,
};
use crate::tables::ConsensusBlocks;

#[cfg(test)]
mod tests;

/// Drives cold archival against a hot-only database view and its cold store.
///
/// `hot` must never fall through to cold (`LayeredDatabase::without_cold` in production), so the
/// producer's reads and deletes stay on the hot tier (see [`archive_below_epoch`]): a tiered view
/// would answer "is this row still hot?" from the very cold copy archival is creating.
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
    /// Builds an archiver over a hot-only database view and its shared cold store.
    ///
    /// Crate-private so external code cannot hand the archiver a cold-attached handle; production
    /// wiring goes through `cold_archiver_for`, which strips the cold layer first.
    pub(crate) fn new(hot: DB, cold: Arc<ColdStore>) -> Self {
        Self { hot, cold }
    }

    /// Completes every sealed-but-unfinalized epoch (index + high-water mark + hot prune) and heals
    /// a crash-interrupted archive.
    ///
    /// Runs once at boot before serving (node and `cold-migrate`): a jar-driven rebuild sweeping
    /// whatever a cancelled or crashed [`Self::seal_due`] pass left behind.
    pub fn reconcile(&self) -> eyre::Result<()> {
        reconcile(&self.hot, &self.cold).map_err(|e| eyre::eyre!("cold reconcile failed: {e}"))
    }

    /// Archives every epoch safely below the current committed epoch into cold.
    ///
    /// The cutoff is floored by `el_anchor_epoch`, so a pruned epoch is always one the EL has
    /// executed. `max_epochs` caps one pass (`None` is unbounded).
    pub fn archive_due(
        &self,
        el_anchor_epoch: Epoch,
        max_epochs: Option<usize>,
    ) -> eyre::Result<ArchiveStats> {
        let Some(cutoff) = self.cutoff_epoch(el_anchor_epoch)? else {
            return Ok(ArchiveStats::default());
        };
        archive_below_epoch(&self.hot, &self.cold, cutoff, max_epochs)
            .map_err(|e| eyre::eyre!("cold archive failed: {e}"))
    }

    /// Archives at most one due epoch fully: seal the jars, then index, advance the high-water mark
    /// and prune, so no archival runs on the epoch transition.
    ///
    /// `should_cancel` is polled at each seal chunk seam and prune batch, leaving either
    /// uncommitted jars or hot rows for [`Self::reconcile`] to sweep. Every row stays in at least
    /// one tier.
    pub fn seal_due(
        &self,
        el_anchor_epoch: Epoch,
        should_cancel: impl Fn() -> bool,
    ) -> eyre::Result<SealOutcome> {
        let Some(cutoff) = self.cutoff_epoch(el_anchor_epoch)? else {
            return Ok(SealOutcome::Drained);
        };
        let seal_started = std::time::Instant::now();
        let seal =
            seal_next_epoch_jars(&self.hot, &self.cold, cutoff, SEAL_CHUNK_BYTES, &should_cancel)
                .map_err(|e| eyre::eyre!("cold jar seal failed: {e}"))?;
        let sealed = match seal {
            JarSeal::Sealed(sealed) => sealed,
            JarSeal::Drained => return Ok(SealOutcome::Drained),
            JarSeal::Cancelled => return Ok(SealOutcome::Cancelled),
        };

        let seal_elapsed = seal_started.elapsed();
        let finalize_started = std::time::Instant::now();
        // eyre::Report::new preserves the ColdError type in the chain; seal_due_epochs
        // uses downcast_ref::<ColdError>() to distinguish WriteFailed (fatal) from
        // Corruption (retriable). Do not use eyre::eyre!("...: {e}") here — that would
        // stringify the error and lose the concrete type.
        let finalized = finalize_sealed(&self.hot, &sealed, &should_cancel, PRUNE_YIELD)
            .map_err(|e| eyre::Report::new(e).wrap_err("cold finalize failed"))?;
        // Reporting a cancelled finalize as sealed would let the caller close the epoch out and
        // skip the retry that sweeps its leftover hot rows.
        let Finalized::Complete(_) = finalized else {
            return Ok(SealOutcome::Cancelled);
        };

        // Per-phase split: seal = read+compress+append, finalize = index commit plus paced prune.
        // A slow pass is diagnosed from this line alone.
        info!(
            target: "cold-archive",
            epoch = sealed.epoch,
            blocks = sealed.numbers.end() - sealed.numbers.start() + 1,
            batch_rows = sealed.locations.len(),
            seal = ?seal_elapsed,
            finalize = ?finalize_started.elapsed(),
            "cold pass phases",
        );
        Ok(SealOutcome::Sealed(sealed.epoch))
    }

    /// Returns the exclusive archival cutoff, or `None` when nothing is eligible.
    ///
    /// Exclusive at the committed epoch, so the just-closed one stays hot through a transition,
    /// and floored by the EL anchor: an anchor inside epoch `A` proves everything below `A` was
    /// executed.
    ///
    /// # Errors
    ///
    /// Propagates a failed cutoff read rather than folding it into `None`.
    fn cutoff_epoch(&self, el_anchor_epoch: Epoch) -> eyre::Result<Option<Epoch>> {
        let Some(current) = self.current_epoch()? else { return Ok(None) };
        let cutoff = current.min(el_anchor_epoch);
        Ok((cutoff > 0).then_some(cutoff))
    }

    /// Returns the current committed epoch, or `None` only when the hot table holds no block.
    ///
    /// The latest subdag is never archived, so the reverse peek resolves on the hot tier.
    ///
    /// # Errors
    ///
    /// Returns the read or projection failure: "cannot tell" must stay distinct from "no
    /// history", which would silently stop archival for good.
    fn current_epoch(&self) -> eyre::Result<Option<Epoch>> {
        self.hot.with_read_txn(|tx| {
            tx.reverse_raw_iter::<ConsensusBlocks>()
                .next()
                .map(|(_, value)| {
                    ConsensusHeaderMeta::from_bytes(&value)
                        .map(|meta| meta.leader_epoch)
                        .map_err(|e| eyre::eyre!("project highest hot consensus block: {e}"))
                })
                .transpose()
        })
    }
}
