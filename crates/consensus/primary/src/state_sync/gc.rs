//! Garbage collection service for the entire primary.

use crate::{
    certificate_fetcher::CertificateFetcherCommand,
    error::{GarbageCollectorError, GarbageCollectorResult},
    ConsensusBus,
};
use consensus_metrics::monitored_scope;
use rayls_infrastructure_config::ConsensusConfig;
use rayls_infrastructure_types::{Database, RaylsSender as _};
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};
use tokio::{sync::watch, time::interval};
use tracing::{error, warn};

#[cfg(test)]
#[path = "../tests/gc_tests.rs"]
mod gc_tests;

/// Long running task that manages the garbage collection events from consensus.
///
/// When the DAG advances the GC round, this task updates the [AtomicRound] and notifies
/// subscribers.
#[derive(Debug)]
pub(super) struct GarbageCollector<DB> {
    /// The consensus configuration.
    config: ConsensusConfig<DB>,
    /// Consensus message channels.
    consensus_bus: ConsensusBus,
    /// Watch channel for gc updates.
    rx_gc_round_updates: watch::Receiver<u32>,
    /// The atomic gc round.
    ///
    /// This is managed by `Self` and is read by CertificateValidator and CertificateManager.
    gc_round: AtomicRound,
}

impl<DB> GarbageCollector<DB>
where
    DB: Database,
{
    /// Create a new instance of Self.
    pub(super) fn new(
        config: ConsensusConfig<DB>,
        consensus_bus: ConsensusBus,
        gc_round: AtomicRound,
    ) -> Self {
        let rx_gc_round_updates = consensus_bus.gc_round_updates().subscribe();
        Self { config, consensus_bus, rx_gc_round_updates, gc_round }
    }

    /// The round advanced within time. Process the round
    async fn process_next_round(&mut self) -> GarbageCollectorResult<()> {
        let _scope = monitored_scope("primary::gc");

        // update gc round
        let new_round = *self.rx_gc_round_updates.borrow_and_update();
        self.gc_round.store(new_round);

        Ok(())
    }

    /// Request the certificate fetcher to request certificates from peers.
    ///
    /// This method is called after the node hasn't received enough parents from the previous round
    /// to advance. The fallback timer is used to attempt to recover by requesting certificates
    /// from peers.
    async fn process_max_round_timeout(&self) -> GarbageCollectorResult<()> {
        // log warning
        warn!(target: "primary::gc",
            "no consensus commit happened for {:?}, triggering certificate fetching.",
            self.config.network_config().sync_config().max_consensus_round_timeout
        );

        // trigger fetch certs
        if let Err(e) =
            self.consensus_bus.certificate_fetcher().send(CertificateFetcherCommand::Kick).await
        {
            error!(target: "primary::gc", ?e, "failed to send on tx_certificate_fetcher");
            return Err(GarbageCollectorError::RAYLSSend("certificate fetcher".to_string()));
        }

        // log metrics and warning
        self.consensus_bus.primary_metrics().node_metrics.synchronizer_gc_timeout.inc();
        Ok(())
    }

    /// Poll the gc for timeout or consensus commits.
    ///
    /// Upon a successful commit, updates the atomic gc round for all consensus tasks.
    ///
    /// A non-fatal error is returned for timeouts. In a follow-up PR, the manager should handle
    /// certificate fetching.
    pub(super) async fn ready(&mut self) -> GarbageCollectorResult<()> {
        let mut max_round_timeout =
            interval(self.config.network_config().sync_config().max_consensus_round_timeout);
        // reset so interval doesn't tick right away
        max_round_timeout.reset();

        tokio::select! {
            // fallback timer to trigger requesting certificates from peers
            _ = max_round_timeout.tick() => {
                self.process_max_round_timeout().await?;
                return Err(GarbageCollectorError::Timeout);
            }

            // round update watch channel
            update = self.rx_gc_round_updates.changed() => {
                // ensure change notification isn't an error
                update.map_err(GarbageCollectorError::ConsensusRoundWatchChannel).inspect_err(|e| {
                    error!(target: "primary::gc", ?e, "rx_consensus_round_updates watch error. shutting down...");
                })?;

                self.process_next_round().await?;

                // reset timer - the happy path
                max_round_timeout.reset();
            }
        }

        Ok(())
    }
}

/// Holds the atomic round.
#[derive(Clone)]
pub(super) struct AtomicRound {
    /// The inner type.
    inner: Arc<InnerAtomicRound>,
}

/// The inner type for [AtomicRound]
struct InnerAtomicRound {
    /// The atomic gc round.
    atomic: AtomicU32,
}

impl AtomicRound {
    /// Create a new instance of Self.
    pub(super) fn new(num: u32) -> Self {
        Self { inner: Arc::new(InnerAtomicRound { atomic: AtomicU32::new(num) }) }
    }

    /// Load the atomic round.
    pub(super) fn load(&self) -> u32 {
        self.inner.atomic.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Fetch the max.
    pub(super) fn fetch_max(&self, val: u32) -> u32 {
        self.inner.atomic.fetch_max(val, Ordering::AcqRel)
    }

    /// Store the new atomic round.
    ///
    /// NOTE: private so only GC can call this
    fn store(&mut self, new: u32) {
        self.inner.atomic.store(new, std::sync::atomic::Ordering::Release);
    }
}

impl std::fmt::Debug for AtomicRound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.inner.atomic)
    }
}

impl std::default::Default for AtomicRound {
    fn default() -> Self {
        Self { inner: Arc::new(InnerAtomicRound { atomic: AtomicU32::new(0) }) }
    }
}
