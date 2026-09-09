use crate::{engine::ExecutionNode, epoch_manager::types::EpochManager};
use alloy::consensus::Transaction;
use rayls_consensus_worker::{quorum_waiter::QuorumWaiterTrait, Worker};
use rayls_execution_evm::{bytes_to_txn, EthPooledTransaction};
use rayls_infrastructure_config::RaylsDirs;
use rayls_infrastructure_storage::tables::NodeBatchesCache;
use rayls_infrastructure_types::{Batch, BlockHash, Database as ReDatabase};
use rayon::prelude::*;
use std::collections::HashMap;
use tracing::{info, warn};

impl<P, DB> EpochManager<P, DB>
where
    P: RaylsDirs + Clone + 'static,
    DB: ReDatabase,
{
    /// Collect any batches that never got into consensus (at epoch change or node restart) and
    /// Re-introduce them into the mempool for inclusion in future batches.
    pub(super) async fn orphan_batches<QuorumWaiter: QuorumWaiterTrait>(
        &self,
        engine: ExecutionNode,
        worker: Worker<DB, QuorumWaiter>,
    ) -> eyre::Result<()> {
        let now = std::time::Instant::now();
        let orphan_batches_reintroduction_elapsed = now;
        let orphan_batches_collection_elapsed = now;

        let orphan_batches: Vec<(BlockHash, Batch)> =
            self.consensus_db.iter::<NodeBatchesCache>().collect();
        warn!(target: "orphan-batches", elapsed=?orphan_batches_collection_elapsed.elapsed(), "Collected orphan batches");

        if !orphan_batches.is_empty() {
            self.consensus_db.clear_table::<NodeBatchesCache>()?;

            let consensus_bus = self.consensus_bus.clone();
            let is_cvv = consensus_bus.node_mode().borrow().is_cvv();
            warn!(target: "epoch-manager", ?is_cvv, len=orphan_batches.len(), "Re-introducing orphaned batches");
            // Loop through any orphaned batches and resubmit it's transactions.
            // This is most likely because of epoch changes but could be caused by a restart as
            // well.
            if is_cvv {
                // 1. Fetch pools once (I/O bound)
                let fetch_pools_elapsed = std::time::Instant::now();
                let pools = engine.get_all_worker_transaction_pools().await;
                warn!(target: "orphan-batches", elapsed=?fetch_pools_elapsed.elapsed(), "Fetched worker pools");
                // 2. CPU Phase: Group, Parse, and Sort
                // We move this to a blocking thread so the async runtime doesn't freeze.
                // We return a Vector of prepared data to iterate over later.
                let prepare_work_elapsed = std::time::Instant::now();
                let prepared_work = tokio::task::spawn_blocking(move || {
                    // Step A: Group raw bytes by worker_id using fold
                    let grouped_raw: HashMap<usize, Vec<_>> =
                        orphan_batches.into_iter().fold(HashMap::new(), |mut acc, (_, batch)| {
                            acc.entry(batch.worker_id as usize)
                                .or_default()
                                .extend(batch.transactions);
                            acc
                        });

                    // Step B: Parallel Parse & Sort per group
                    let mut results = Vec::with_capacity(grouped_raw.len());

                    for (worker_id, raw_txs) in grouped_raw {
                        // Use Rayon to parse bytes to txns in parallel
                        let mut parsed_txns: Vec<EthPooledTransaction> = raw_txs
                            .par_iter()
                            .filter_map(|tx_bytes| bytes_to_txn(tx_bytes).ok())
                            .collect();

                        // Fix: Correct syntax for sorting by key
                        parsed_txns.sort_unstable_by_key(|tx| tx.nonce());

                        if !parsed_txns.is_empty() {
                            results.push((worker_id, parsed_txns));
                        }
                    }
                    results
                })
                .await
                .unwrap_or_default(); // Handle potential JoinError
                warn!(target: "orphan-batches", elapsed=?prepare_work_elapsed.elapsed(), "Prepared work for workers");

                // 3. I/O Phase: Insert via bypass validator (skips per-tx state validation).
                //
                // Orphan transactions were already validated in the previous epoch.
                // `add_orphan_transactions` reads sender state once per unique sender,
                // populates the bypass map, and the validator returns Valid immediately
                // for every tx — eliminating the ~19s re-validation bottleneck.
                let total_txs: usize = prepared_work.iter().map(|(_, txs)| txs.len()).sum();
                let worker_count = prepared_work.len();
                warn!(
                    target: "orphan-batches",
                    total_txs,
                    worker_count,
                    "Inserting orphan transactions via bypass validator"
                );

                let mut join_handles = Vec::with_capacity(prepared_work.len());
                for (worker_id, txs) in prepared_work {
                    if let Some(pool) = pools.get(worker_id).cloned() {
                        let handle =
                            tokio::spawn(async move { pool.add_orphan_transactions(txs).await });
                        join_handles.push((worker_id, handle));
                    }
                }

                for (worker_id, handle) in join_handles {
                    match handle.await {
                        Ok(results) => {
                            let ok = results.iter().filter(|r| r.is_ok()).count();
                            let err = results.len() - ok;
                            if err > 0 {
                                warn!(target: "orphan-batches", worker_id, ok, err, "Some orphan txs failed insertion");
                            }
                        }
                        Err(e) => {
                            warn!(target: "orphan-batches", worker_id, ?e, "Orphan insertion task panicked");
                        }
                    }
                }
            } else {
                // If we are not a CVV then go ahead and disburse the txns from the batch directly.
                for (digest, batch) in orphan_batches {
                    let _ = worker.disburse_txns(batch.seal(digest)).await;
                }
            }
        } else {
            info!(target: "epoch-manager", "No batches leftover");
        }
        warn!(target: "orphan-batches", elapsed=?orphan_batches_reintroduction_elapsed.elapsed(), "Finished orphan batches re-introduction");
        Ok(())
    }
}
