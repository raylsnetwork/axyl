//! The Proposer is responsible for proposing the primary's next header when certain conditions are
//! met.
//!
//! This is the first task in the primary's header cycle. The Proposer processes messages from the
//! `Primary::StateHandler` to track which proposed headers were successfully committed. If a header
//! is not committed before it's round advances, the failed header's block digests are included in a
//! fresh header in FIFO order.
//!
//! Successfully created Headers are sent to the `Primary::Certifier`, where they are reliably
//! broadcast to voting peers. Headers are stored in the `ProposerStore` before they are sent to the
//! Certifier.
//!
//! The Proposer is also responsible for processing batch's that reach quorum.
//! Collections of batches that reach quorum are included in each header. If the Proposer's
//! header fails to be committed, then block digests from the failed round are included in the next
//! header once the Proposer's round advances.

use crate::{
    consensus::LeaderSchedule, error::ProposerResult, proposer::types::ProposerDigest, ConsensusBus,
};
use consensus_metrics::monitored_future;
use rayls_infrastructure_config::ConsensusConfig;
use rayls_infrastructure_storage::ProposerStore;
use rayls_infrastructure_types::{
    AuthorityIdentifier, Certificate, Committee, Database, Header, Noticer, Round, TaskManager,
    TaskSpawner, TimestampMillis,
};
use std::collections::{BTreeMap, VecDeque};
use tokio::{
    sync::oneshot,
    time::{Duration, Interval},
};
use tracing::info;

/// Rayls: Digest queue depth that warns of a possible certification stall.
///
/// Warn-only: dropping a quorum'd digest would gap the per-authority seq stream.
const DIGEST_QUEUE_WARN_THRESHOLD: usize = 4096;

/// Threshold for pending certificates above which we slow down proposals.
const PENDING_BACKPRESSURE_THRESHOLD: usize = 5_000;
/// Delay when backpressure is active.
const BACKPRESSURE_DELAY: Duration = Duration::from_secs(1);

/// Maximum consensus round lag over execution before throttling proposals.
const EXECUTION_LAG_THRESHOLD: u64 = 100;
/// Delay when execution backpressure is active.
const EXECUTION_BACKPRESSURE_DELAY: Duration = Duration::from_millis(500);

/// Type alias for the async task that creates, stores, and sends the proposer's new header.
type PendingHeaderTask = oneshot::Receiver<ProposerResult<Header>>;

mod header_builder;
mod recovery;
mod round;
mod run_loop;
mod types;

#[cfg(test)]
#[path = "../tests/proposer_tests.rs"]
mod proposer_tests;

pub(crate) use types::OurDigestMessage;

/// The proposer creates new headers and send them to the core for broadcasting and further
/// processing.
pub(crate) struct Proposer<DB: ProposerStore> {
    /// The id of this primary.
    authority_id: AuthorityIdentifier,
    /// The committee information.
    committee: Committee,
    /// The threshold number of batches that can trigger
    /// a header creation. When there are available at least
    /// `header_num_of_batches_threshold` batches we are ok
    /// to try and propose a header
    header_num_of_batches_threshold: usize,
    /// The maximum number of batches in header.
    max_header_num_of_batches: usize,
    /// The minimum duration between generating headers.
    min_header_delay: Duration,
    /// The maximum duration to wait for conditions like having leader in parents.
    max_header_delay: Duration,
    /// The minimum interval measured between generating headers.
    min_delay_interval: Interval,
    /// The maximum interval measured for conditions like having leader in parents.
    max_delay_interval: Interval,
    /// The latest header.
    opt_latest_header: Option<Header>,
    /// Receiver for shutdown.
    ///
    /// Also used to signal committee change.
    rx_shutdown: Noticer,
    /// consensus channels
    consensus_bus: ConsensusBus,
    /// The proposer store for persisting the last header.
    proposer_store: DB,
    /// The current round of the dag.
    round: Round,
    /// Last time the round has been updated
    last_round_timestamp: Option<TimestampMillis>,
    /// Holds the certificates' ids waiting to be included in the next header.
    last_parents: Vec<Certificate>,
    /// Holds the certificate of the last leader (if any).
    last_leader: Option<Certificate>,
    /// Holds the batches' digests waiting to be included in the next header.
    /// Digests are roughly oldest to newest, and popped in FIFO order from the front.
    digests: VecDeque<ProposerDigest>,
    /// Holds the map of proposed previous round headers and their digest messages, to ensure that
    /// all batches' digest included will eventually be re-sent.
    proposed_headers: BTreeMap<Round, Header>,
    /// The consensus leader schedule to be used in order to resolve the leader needed for the
    /// protocol advancement.
    leader_schedule: LeaderSchedule,
    /// Flag if enough conditions are met to advance the round.
    advance_round: bool,
    /// Spawner for our tasks- want to confine them to the current epoch.
    task_spawner: TaskSpawner,
    /// Garbage collection depth for cleaning up old proposed headers.
    gc_depth: Round,
}

impl<DB: Database> Proposer<DB> {
    /// Create a new instance of Self.
    ///
    /// The proposer's intervals and genesis certificate are created in this function.
    /// Also set `advance_round` to true.
    pub(crate) fn new(
        config: ConsensusConfig<DB>,
        authority_id: AuthorityIdentifier, // We need to be a validator so must have an id.
        consensus_bus: ConsensusBus,
        leader_schedule: LeaderSchedule,
        task_spawner: TaskSpawner,
    ) -> Self {
        let rx_shutdown = config.shutdown().subscribe();
        // idle until aggregator feeds parents; otherwise round=1 propose is
        // rejected as too-old and trips VoteFailureTracker
        let committed_round = *consensus_bus.committed_round_updates().borrow();
        let (initial_round, initial_parents) = if committed_round > 0 {
            (committed_round, Vec::new())
        } else {
            (0, Certificate::genesis(config.committee()))
        };
        let min_delay_interval = tokio::time::interval(config.parameters().min_header_delay);
        let max_delay_interval = tokio::time::interval(config.parameters().max_header_delay);

        Self {
            authority_id,
            committee: config.committee().clone(),
            header_num_of_batches_threshold: config.parameters().header_num_of_batches_threshold,
            max_header_num_of_batches: config.parameters().max_header_num_of_batches,
            min_header_delay: config.parameters().min_header_delay,
            max_header_delay: config.parameters().max_header_delay,
            min_delay_interval,
            max_delay_interval,
            opt_latest_header: None,
            rx_shutdown,
            consensus_bus,
            proposer_store: config.node_storage().clone(),
            round: initial_round,
            last_round_timestamp: None,
            last_parents: initial_parents,
            last_leader: None,
            digests: VecDeque::with_capacity(2 * config.parameters().max_header_num_of_batches),
            proposed_headers: BTreeMap::new(),
            leader_schedule,
            advance_round: true,
            task_spawner,
            gc_depth: config.parameters().gc_depth,
        }
    }

    pub(crate) fn spawn(mut self, task_manager: &TaskManager) {
        if self.consensus_bus.node_mode().borrow().is_active_cvv() {
            // result-aware spawn so fatal ProposerError surfaces as CriticalExitError
            task_manager.spawn_critical_result_task(
                "proposer task",
                monitored_future!(
                    async move {
                        info!(target: "primary::proposer", "Starting proposer");
                        self.run().await
                    },
                    "ProposerTask"
                ),
            );
        }
        // If not an active CVV then don't propose anything.
    }
}
