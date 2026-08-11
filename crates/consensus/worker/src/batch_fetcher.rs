//! Fetch batches from peers

use crate::{metrics::WorkerMetrics, network::WorkerNetworkHandle};
use async_trait::async_trait;
use rayls_consensus_network::error::NetworkError;
use rayls_infrastructure_storage::tables::Batches;
use rayls_infrastructure_types::{now, Batch, BlockHash, Database, DbTxMut, Table};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tracing::{debug, warn};

#[cfg(test)]
#[path = "tests/batch_fetcher.rs"]
mod batch_fetcher_tests;

#[derive(Debug)]
pub(crate) struct BatchFetcher<DB> {
    network: Arc<dyn RequestBatchesNetwork>,
    batch_store: DB,
    metrics: Arc<WorkerMetrics>,
}

impl<DB: Database> BatchFetcher<DB> {
    pub(crate) fn new(
        network: WorkerNetworkHandle,
        batch_store: DB,
        metrics: Arc<WorkerMetrics>,
    ) -> Self {
        Self { network: Arc::new(network), batch_store, metrics }
    }

    /// Maximum number of remote fetch attempts before returning a partial result.
    const MAX_FETCH_RETRIES: usize = 5;

    /// Bulk fetches payload from local storage and remote workers.
    ///
    /// Retries up to [`Self::MAX_FETCH_RETRIES`] times. If some digests remain
    /// unfetchable (e.g. garbage-collected by peers), the partial result is
    /// returned so callers are not blocked indefinitely.
    pub(crate) async fn fetch(&self, digests: HashSet<BlockHash>) -> HashMap<BlockHash, Batch> {
        debug!(target: "batch_fetcher", "Attempting to fetch {} digests from peers", digests.len(),);

        let mut remaining_digests = digests;
        let mut fetched_batches = HashMap::new();
        let mut retries = 0usize;

        loop {
            if remaining_digests.is_empty() {
                return fetched_batches;
            }

            // Fetch from local storage.
            let _timer = self.metrics.worker_local_fetch_latency.start_timer();
            fetched_batches.extend(self.fetch_local(remaining_digests.clone()).await);
            remaining_digests.retain(|d| !fetched_batches.contains_key(d));
            if remaining_digests.is_empty() {
                return fetched_batches;
            }
            drop(_timer);

            // Fetch from peers.
            let _timer = self.metrics.worker_remote_fetch_latency.start_timer();
            if let Ok(new_batches) = self.safe_request_batches(&remaining_digests).await {
                // Set received_at timestamp for remote batches.
                let mut updated_new_batches = HashMap::new();
                for (digest, batch) in
                    new_batches.iter().filter(|(d, _)| remaining_digests.remove(*d))
                {
                    let mut batch = (*batch).clone();
                    batch.set_received_at(now());
                    updated_new_batches.insert(*digest, batch);
                }
                // Also persist the batches, so they are available after restarts.
                // Serialize against other writers of the Batches table (the worker's own
                // insert path) so a fetch landing on a digest being locally written cannot
                // interleave a read-then-write mid-commit.
                let mut txn = self
                    .batch_store
                    .write_txn()
                    .expect("unable to create DB transaction!");
                txn.lock_table(Batches::NAME)
                    .expect("unable to acquire batch store lock!");
                if let Err(e) = (|| -> eyre::Result<()> {
                    for (digest, batch) in &updated_new_batches {
                        txn.insert::<Batches>(digest, batch).map_err(|e| {
                            tracing::error!(target: "batch_fetcher", "failed to insert batch! We can not continue.. {e}");
                            e
                        })?;
                    }
                    Ok(())
                })() {
                    panic!("unable to insert batch: {e}");
                }
                txn.commit().expect("unable to commit batch transaction!");
                fetched_batches.extend(updated_new_batches.into_iter());

                if remaining_digests.is_empty() {
                    return fetched_batches;
                }
            }

            retries += 1;
            if retries >= Self::MAX_FETCH_RETRIES {
                warn!(
                    target: "batch_fetcher",
                    remaining = remaining_digests.len(),
                    retries,
                    "batch fetch exhausted retries, returning partial result"
                );
                return fetched_batches;
            }
            // back off before retrying to give peers time to propagate
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    async fn fetch_local(&self, digests: HashSet<BlockHash>) -> HashMap<BlockHash, Batch> {
        let mut fetched_batches = HashMap::new();
        if digests.is_empty() {
            return fetched_batches;
        }

        // Continue to bulk request from local worker until no remaining digests
        // are available.
        debug!(target: "batch_fetcher", "Local attempt to fetch {} digests", digests.len());
        if let Ok(local_batches) = self.batch_store.multi_get::<Batches>(digests.iter()) {
            for (digest, batch) in digests.into_iter().zip(local_batches.into_iter()) {
                if let Some(batch) = batch {
                    self.metrics.batch_fetch.with_label_values(&["local", "success"]).inc();
                    fetched_batches.insert(digest, batch);
                } else {
                    self.metrics.batch_fetch.with_label_values(&["local", "missing"]).inc();
                }
            }
        }

        fetched_batches
    }

    /// Issue request_batches RPC and verifies response integrity
    async fn safe_request_batches(
        &self,
        digests_to_fetch: &HashSet<BlockHash>,
    ) -> Result<HashMap<BlockHash, Batch>, NetworkError> {
        let mut fetched_batches = HashMap::new();
        if digests_to_fetch.is_empty() {
            return Ok(fetched_batches);
        }

        let batches = self
            .network
            .request_batches_from_all(digests_to_fetch.clone().into_iter().collect())
            .await?;
        for batch in batches {
            let batch_digest = batch.digest();
            // This batch is part of a certificate, so no need to validate it.
            fetched_batches.insert(batch_digest, batch);
        }

        Ok(fetched_batches)
    }
}

/// Network boundary for fetching batches, indirected so tests can drive it.
///
/// No overall deadline: `request_batches` is already bounded by per-peer timeouts, and an outer
/// cancel would discard already-collected batches, making retries zero-progress under load and
/// surfacing as a "batch not found" subdag violation.
#[async_trait]
trait RequestBatchesNetwork: Send + Sync + std::fmt::Debug {
    /// Fetch the given batch digests from connected peers.
    async fn request_batches_from_all(
        &self,
        batch_digests: Vec<BlockHash>,
    ) -> Result<Vec<Batch>, NetworkError>;
}

#[async_trait]
impl RequestBatchesNetwork for WorkerNetworkHandle {
    async fn request_batches_from_all(
        &self,
        batch_digests: Vec<BlockHash>,
    ) -> Result<Vec<Batch>, NetworkError> {
        self.request_batches(batch_digests).await
    }
}
