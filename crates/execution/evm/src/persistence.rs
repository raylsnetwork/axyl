//! Thin wrapper around reth's built-in `PersistenceService`.
//!
//! Provides a `spawn_persistence` helper and a `PersistenceState` tracker
//! for coordinating deferred MDBX block writes across consensus rounds.
//!
//! Blocks are accumulated in reth's `CanonicalInMemoryState` (the single
//! source of truth) and extracted at persist time — matching reth's own
//! engine tree approach with zero extra copies.

use std::{sync::Arc, time::Instant};

use crate::traits::RaylsNode;
use alloy::eips::BlockNumHash;
use reth_config::PruneConfig;
use reth_db::DatabaseEnv;
use reth_engine_tree::persistence::{PersistenceHandle, PersistenceResult};
use reth_ethereum_primitives::EthPrimitives;
use reth_exex_types::FinishedExExHeight;
use reth_provider::ProviderFactory;
use reth_prune::{Pruner, PrunerBuilder};
use reth_stages_api::MetricEventsSender;

/// Spawn reth's `PersistenceService` on a dedicated OS thread.
///
/// When `prune_config` is provided, builds a real pruner with segments
/// derived from the config. Otherwise falls back to a no-op pruner.
pub(crate) fn spawn_persistence(
    provider_factory: ProviderFactory<RaylsNode>,
    prune_config: Option<PruneConfig>,
) -> (PersistenceHandle<EthPrimitives>, MetricEventsSender) {
    let pruner = if let Some(config) = prune_config {
        PrunerBuilder::new(config).build_with_provider_factory(provider_factory.clone())
    } else {
        let (_finished_exex_height_tx, finished_exex_height_rx) =
            tokio::sync::watch::channel(FinishedExExHeight::NoExExs);

        Pruner::new_with_factory(
            provider_factory.clone(),
            vec![],     // no prune segments
            usize::MAX, // never trigger pruning
            0,          // delete_limit (no pruning)
            None,       // no timeout
            finished_exex_height_rx,
        )
    };

    let (sync_metrics_tx, _sync_metrics_rx) = tokio::sync::mpsc::unbounded_channel();

    let handle = PersistenceHandle::<EthPrimitives>::spawn_service(
        provider_factory,
        pruner,
        sync_metrics_tx.clone(),
    );

    (handle, sync_metrics_tx)
}

/// Tracks deferred persistence state between consensus rounds.
///
/// Unlike a separate accumulation buffer, blocks live in
/// [`CanonicalInMemoryState`] and are extracted at persist time by walking
/// the canonical chain — matching reth's engine tree pattern.
#[derive(Debug)]
pub(crate) struct PersistenceState {
    /// Last block successfully persisted to disk (number + hash).
    pub(crate) last_persisted_block: BlockNumHash,
    /// In-flight persistence completion to poll.
    pub(crate) pending_rx: Option<crossbeam_channel::Receiver<PersistenceResult>>,
    /// When the in-flight persistence was initiated (for elapsed-time logging).
    pub(crate) pending_started_at: Option<Instant>,
    /// Latest finalized block number (to include in next persist).
    pub(crate) latest_finalized_number: u64,
    /// Latest safe block number (to include in next persist).
    pub(crate) latest_safe_number: u64,
    /// Persist when canonical head exceeds last persisted by this many blocks.
    pub(crate) persistence_threshold: u64,
    #[allow(unused)]
    /// MDBX environment handle for explicit `env_sync()` with SafeNoSync.
    database: Arc<DatabaseEnv>,
    #[allow(unused)]
    /// Number of completed persists since last env sync.
    persist_count: u64,
}

impl PersistenceState {
    /// Create with the given initial finalized block number and threshold.
    pub(crate) fn new(
        last_persisted_block_number: u64,
        persistence_threshold: u64,
        database: Arc<DatabaseEnv>,
    ) -> Self {
        Self {
            last_persisted_block: BlockNumHash::new(
                last_persisted_block_number,
                Default::default(),
            ),
            pending_rx: None,
            pending_started_at: None,
            latest_finalized_number: last_persisted_block_number,
            latest_safe_number: last_persisted_block_number,
            persistence_threshold,
            database,
            persist_count: 0,
        }
    }

    /// Returns true if the canonical head is far enough ahead of the last
    /// persisted block to warrant a persistence flush.
    pub(crate) fn should_persist(&self, canonical_head_number: u64) -> bool {
        canonical_head_number.saturating_sub(self.last_persisted_block.number)
            > self.persistence_threshold
    }

    /// Returns true if a persistence operation is currently in-flight.
    pub(crate) fn in_progress(&self) -> bool {
        self.pending_rx.is_some()
    }
}
