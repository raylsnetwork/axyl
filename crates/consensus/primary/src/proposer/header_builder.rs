use crate::{
    error::{ProposerError, ProposerResult},
    proposer::{types::ProposerDigest, PendingHeaderTask, Proposer},
    ConsensusBus,
};
use rayls_consensus_primary_metrics::PrimaryMetrics;
use rayls_infrastructure_storage::ProposerStore;
use rayls_infrastructure_types::{
    now, now_in_millis, AuthorityIdentifier, Certificate, Database, Epoch, Hash as _, Header,
    RaylsSender, Round,
};
use std::{collections::VecDeque, sync::Arc};
use tokio::{
    sync::oneshot,
    time::{sleep, Duration},
};
#[cfg(feature = "dev-single-node-setup")]
use tracing::warn;
use tracing::{debug, enabled, error, info, trace};

/// Largest backward wall-clock step the proposer tolerates (dev only) by clamping
/// the proposal timestamp to stay monotonic, instead of failing.
///
/// `now_in_millis()` is wall-clock (`SystemTime`) and can legitimately step
/// backward on NTP / VM time-sync corrections. On a single-validator dev chain
/// running inside WSL2 or a VM the corrections can be large (tens of seconds) and
/// frequent because the node runs as fast as the CPU allows. Production keeps the
/// original strict behavior: any backward step fails the round (epoch restart).
#[cfg(feature = "dev-single-node-setup")]
const MAX_CLOCK_REGRESSION_MS: u64 = 60_000; // WSL2/VM time-sync jitter on fast dev chains

impl<DB: Database> Proposer<DB> {
    /// Make a new header, store it in the proposer store, and forward it to the certifier.
    ///
    /// This task is spawned outside of `Self`.
    ///
    /// - current_header: caller checks to see if there is already a header built for this round. If
    ///   current_header.is_some() the proposer uses this header instead of building a new one.
    #[allow(clippy::too_many_arguments)]
    async fn propose_header(
        current_round: Round,
        current_epoch: Epoch,
        authority_id: AuthorityIdentifier,
        proposer_store: DB,
        consensus_bus: &ConsensusBus,
        parents: Vec<Certificate>,
        digests: VecDeque<ProposerDigest>,
        reason: String,
        metrics: Arc<PrimaryMetrics>,
        leader_and_support: String,
        max_delay: Duration,
    ) -> ProposerResult<Header> {
        // check that the included timestamp is consistent with the parent's timestamp
        //
        // ie) the current time is *after* the timestamp in all included headers
        //
        // if not: log an error and sleep
        let latest_parent = parents.iter().map(|c| *c.header().created_at()).max().unwrap_or(0);
        let current_time = now();
        if current_time < latest_parent {
            let drift_sec = latest_parent - current_time;
            error!(
                ?current_time,
                ?latest_parent,
                "Current time earlier than most recent parent! Sleeping for {}sec until max parent time...",
                drift_sec,
            );
            metrics.header_max_parent_wait_ms.inc_by(drift_sec * 1000);
            sleep(Duration::from_secs(drift_sec)).await;
        }

        let payload_vec: Vec<_> = digests.iter().map(|m| (m.digest, m.worker_id)).collect();
        consensus_bus.batch_tracker().digests_included_in_header(&payload_vec);
        let header = Header::new(
            authority_id,
            current_round,
            current_epoch,
            payload_vec.into_iter().collect(),
            parents.iter().map(|x| x.digest()).collect(),
            consensus_bus.recently_executed_blocks().borrow().latest_block_num_hash(),
        );

        // update metrics before sending/storing header
        metrics.headers_proposed.with_label_values(&[&leader_and_support]).inc();
        metrics.header_parents.observe(parents.len() as f64);

        if enabled!(target: "primary::proposer", tracing::Level::TRACE) {
            let mut msg = format!("Created header {header:?} with parent certificates:\n");
            for parent in parents.iter() {
                msg.push_str(&format!("{parent:?}\n"));
            }
            trace!(target: "primary::proposer", ?header, ?msg, "created new header");
        } else {
            debug!(target: "primary::proposer", ?header, parents=?header.parents(), "created new header");
        }

        // Update metrics related to latency
        let mut total_inclusion_secs = 0.0;
        for digest in &digests {
            let batch_inclusion_secs =
                Duration::from_secs(header.created_at().saturating_sub(now())).as_secs_f64();
            total_inclusion_secs += batch_inclusion_secs;

            // NOTE: this log entry is used to measure performance
            trace!(
                "Batch {:?} from worker {} took {} seconds from creation to be included in a proposed header",
                digest.digest,
                digest.worker_id,
                batch_inclusion_secs
            );
            metrics.proposer_batch_latency.observe(batch_inclusion_secs);
        }

        // NOTE: this log entry is used to measure performance
        let (header_creation_secs, avg_inclusion_secs) = if !digests.is_empty() {
            (
                Duration::from_secs(header.created_at().saturating_sub(now())).as_secs_f64(),
                total_inclusion_secs / digests.len() as f64,
            )
        } else {
            (max_delay.as_secs_f64(), 0.0)
        };

        trace!(
            target: "primary::proposer",
            "Header {:?} was created in {} seconds. Contains {} batches, with average delay {} seconds.",
            header.digest(),
            header_creation_secs,
            digests.len(),
            avg_inclusion_secs,
        );

        // store and send newly built header
        Proposer::store_and_send_header(&header, proposer_store, consensus_bus, &reason).await?;

        Ok(header)
    }

    /// Bypass creating another header and return header.
    ///
    /// This is a convenience method to help the flow of proposing new headers and reproposing
    /// headers. Headers are reproposed under certain conditions:
    /// - during a restart when the last proposed header in Self::proposer_store is from the current
    ///   round.
    /// -
    pub(super) async fn repropose_header(
        header: Header,
        proposer_store: DB,
        consensus_bus: &ConsensusBus,
        reason: String,
    ) -> ProposerResult<Header> {
        Proposer::store_and_send_header(&header, proposer_store, consensus_bus, &reason).await?;

        Ok(header)
    }

    /// Store the header in the `ProposerStore` and send to `Certifier`.
    async fn store_and_send_header(
        header: &Header,
        proposer_store: DB,
        consensus_bus: &ConsensusBus,
        reason: &str,
    ) -> ProposerResult<()> {
        // Store the last header.
        proposer_store
            .write_last_proposed(header)
            .map_err(|e| ProposerError::StoreError(e.to_string()))?;

        // Send the new header to the `Certifier` that will broadcast and certify it.
        let result =
            consensus_bus.headers().send(header.clone()).await.map_err(|e| Box::new(e).into());
        let num_digests = header.payload().len();
        consensus_bus
            .primary_metrics()
            .node_metrics
            .num_of_batch_digests_in_header
            .with_label_values(&[reason])
            .observe(num_digests as f64);

        result
    }

    /// Conditions are met to propose the next header.
    ///
    /// This method ensures proposer is protected against equivocation and sends the next header to
    /// the Certifier.
    ///
    /// If a different header was already produced for the same round, then
    /// this method returns the earlier header. Otherwise the newly created header is returned.
    pub(super) fn propose_next_header(
        &mut self,
        reason: String,
    ) -> ProposerResult<PendingHeaderTask> {
        // round advances here (+1) or via process_parents on future-round parents;
        // primary_round_updates is write-only for GC/metrics, never read back
        self.round += 1;
        self.consensus_bus.primary_round_updates().send_replace(self.round);

        // Update the metrics
        self.consensus_bus.primary_metrics().node_metrics.current_round.set(self.round as i64);
        // Production: strict monotonicity — any backward wall-clock step fails the
        // round (triggers an epoch restart). Unchanged from before dev mode.
        #[cfg(not(feature = "dev-single-node-setup"))]
        let current_timestamp = {
            let current_timestamp = now_in_millis();
            if let Some(t) = &self.last_round_timestamp {
                if current_timestamp < *t {
                    // this error will trigger a epoch restart
                    return Err(ProposerError::OldTimestamp(current_timestamp, *t));
                }

                self.consensus_bus
                    .primary_metrics()
                    .node_metrics
                    .proposal_latency
                    .with_label_values(&[&reason])
                    .observe(Duration::from_millis(current_timestamp - t).as_secs_f64());
            }
            current_timestamp
        };

        // Dev (single-node): tolerate large/frequent NTP/VM time-sync jitter by
        // clamping benign backward steps to stay monotonic instead of dying.
        #[cfg(feature = "dev-single-node-setup")]
        let current_timestamp = {
            let mut current_timestamp = now_in_millis();
            if let Some(t) = self.last_round_timestamp {
                if current_timestamp < t {
                    let backward_ms = t - current_timestamp;
                    if backward_ms > MAX_CLOCK_REGRESSION_MS {
                        // Clock is grossly wrong — refuse rather than emit a wildly
                        // back-dated block. Surfaces as a critical exit.
                        return Err(ProposerError::OldTimestamp(current_timestamp, t));
                    }
                    // Benign backward step (NTP / VM time-sync, common under load):
                    // hold the timestamp monotonic instead of dying. Block time must
                    // never move backward, and a clock blip must not kill the node.
                    warn!(
                        target: "primary::proposer",
                        now_ms = current_timestamp,
                        last_ms = t,
                        backward_ms,
                        "wall clock stepped backward; clamping proposal timestamp to stay monotonic"
                    );
                    current_timestamp = t;
                }

                self.consensus_bus
                    .primary_metrics()
                    .node_metrics
                    .proposal_latency
                    .with_label_values(&[&reason])
                    .observe(Duration::from_millis(current_timestamp - t).as_secs_f64());
            }
            current_timestamp
        };

        self.last_round_timestamp = Some(current_timestamp);
        debug!(target: "primary::proposer", authority=?self.authority_id, round=self.round, "advanced round - proposing next block...");

        // oneshot channel to spawn a task
        let (tx, rx) = oneshot::channel();
        let current_epoch = self.committee.epoch();
        let current_round = self.round;

        // check if proposer store's last header is from this round
        let last_proposed = self
            .proposer_store
            .get_last_proposed()
            .map_err(|e| ProposerError::StoreError(e.to_string()))?;
        let possible_header_to_repropose =
            last_proposed.filter(|h| h.round() == current_round && h.epoch() == current_epoch);
        let proposer_store = self.proposer_store.clone();
        let metrics = self.consensus_bus.primary_metrics().node_metrics.clone();

        match possible_header_to_repropose {
            // resend header
            Some(header) => {
                info!(
                    target: "primary::proposer",
                    authority=?self.authority_id,
                    current_round,
                    current_epoch,
                    header_parents = header.parents().len(),
                    header_parents_digests = ?header.parents(),
                    "reproposing header from proposer store"
                );
                // clear parents if reproposing after restart
                self.last_parents.clear();

                let consensus_bus = self.consensus_bus.clone();
                self.task_spawner.spawn_task("re-propose header", async move {
                    // use this instead of store_and_send to because rx always expects a Header
                    let res =
                        Proposer::repropose_header(header, proposer_store, &consensus_bus, reason)
                            .await;
                    let _ = tx.send(res);
                });
            }
            // create new header
            None => {
                // collect values from &mut self for this header
                let num_of_digests = self.digests.len().min(self.max_header_num_of_batches);
                let digests: VecDeque<_> = self.digests.drain(..num_of_digests).collect();
                let parents = std::mem::take(&mut self.last_parents);
                let authority_id = self.authority_id.clone();
                let min_delay = self.min_header_delay; // copy
                let leader_and_support = if current_round.is_multiple_of(2) {
                    let authority = self.leader_schedule.leader(current_round);
                    if self.authority_id == authority.id() {
                        "even_round_is_leader"
                    } else {
                        "even_round_not_leader"
                    }
                } else {
                    let authority = self.leader_schedule.leader(current_round - 1);
                    if parents.iter().any(|c| c.origin() == &authority.id()) {
                        "odd_round_gives_support"
                    } else {
                        "odd_round_no_support"
                    }
                };

                let consensus_bus = self.consensus_bus.clone();
                // spawn tokio task to create, store, and send new header to certifier
                self.task_spawner.spawn_task("propose header", async move {
                    let proposal = Proposer::propose_header(
                        current_round,
                        current_epoch,
                        authority_id,
                        proposer_store,
                        &consensus_bus,
                        parents,
                        digests,
                        reason,
                        metrics,
                        leader_and_support.to_string(),
                        min_delay,
                    )
                    .await;

                    let _ = tx.send(proposal);
                });
            }
        }

        // return receiver to advance task
        Ok(rx)
    }

    /// Process the result from proposing the header.
    ///
    /// The oneshot channel is ready, indicating a result from the header proposal process. Update
    /// `self` to track latest header, reset the header timeout, min/max delay intervals, insert the
    /// proposed header, and indicate round should not be advanced yet.
    pub(super) fn handle_proposal_result(
        &mut self,
        result: ProposerResult<Header>,
    ) -> ProposerResult<()> {
        // receive result from oneshot channel
        let header = result?;

        // track latest header
        self.opt_latest_header = Some(header.clone());
        // Reset advance flag.
        self.advance_round = false;
        // reschedule intervals
        self.min_delay_interval.reset_after(self.calc_min_delay());
        self.max_delay_interval.reset_after(self.calc_max_delay());
        // track header so proposer can repropose the digests and system messages
        // if this header fails to be committed for some reason
        self.proposed_headers.insert(header.round(), header);

        // Clean up old proposed headers to prevent unbounded growth during partitions
        self.evict_old_proposed_headers();

        Ok(())
    }

    // pub(crate) fn spawn(mut self, task_manager: &TaskManager) {
    //     if self.consensus_bus.node_mode().borrow().is_active_cvv() {
    //         task_manager.spawn_critical_task(
    //             "proposer task",
    //             monitored_future!(
    //                 async move {
    //                     info!(target: "primary::proposer", "Starting proposer");
    //                     self.run().await
    //                 },
    //                 "ProposerTask"
    //             ),
    //         );
    //     }
    //     // If not an active CVV then don't propose anything.
    // }

    /// Wrapper async function to either query the pending header or never resolve.
    pub(super) async fn pending_header(
        pending_header: &mut Option<PendingHeaderTask>,
    ) -> ProposerResult<Header> {
        if let Some(pending_header) = pending_header {
            pending_header.await?
        } else {
            std::future::pending().await
        }
    }
}
