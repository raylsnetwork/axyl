//! Metrics for consensus.

use consensus_metrics::histogram::Histogram as MystenHistogram;
use prometheus::{
    default_registry, register_histogram_with_registry, register_int_counter_vec_with_registry,
    register_int_counter_with_registry, register_int_gauge_vec_with_registry,
    register_int_gauge_with_registry, Histogram, IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
    Registry,
};

const LATENCY_SEC_BUCKETS: &[f64] = &[
    0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 3.0, 4.0, 5.0, 8.0, 10.0, 15.0, 20.0, 30.0, 50.0, 100.0, 200.0,
];

#[derive(Clone, Debug)]
pub struct ConsensusMetrics {
    /// The number of rounds for which the Dag holds certificates
    pub consensus_dag_rounds: IntGaugeVec,
    /// The last committed round from consensus
    pub last_committed_round: IntGaugeVec,
    /// The current epoch of the consensus protocol
    pub current_epoch: IntGauge,
    /// The number of times the consensus state was restored from the consensus store
    /// following a node restart
    pub recovered_consensus_state: IntCounter,
    /// The number of certificates from consensus that were restored and sent to the executor
    /// following a node restart
    pub recovered_consensus_output: IntCounter,
    /// The latency between two successful commit rounds
    pub commit_rounds_latency: Histogram,
    /// The number of certificates committed per commit round
    pub committed_certificates: MystenHistogram,
    /// The time it takes for a certificate from the moment it gets created
    /// up to the moment it gets committed.
    pub certificate_commit_latency: Histogram,
    /// On every even round we expect a leader to be elected and committed. However, this is not
    /// always the case and this metric gives more insight. The metric follows the commit path, so
    /// all the nodes are expected to report the same results. For every leader of each round the
    /// output can be one of the following:
    /// * committed: the leader has been found and its subdag will get committed - no matter if the
    ///   leader is committed on its time or not (part of recursion)
    /// * not_found: the leader has not been found on the commit path and doesn't get committed
    /// * no_path: the leader exists but there is no path that leads to it
    pub leader_election: IntCounterVec,
    /// Under normal circumstances every odd round should trigger leader election for its previous
    /// even round. We consider a "hit" in this case when the leader has been elected when the
    /// network has not moved to the next even round (so latency is still in the expected
    /// range). If the network has moved to the next even round and the leader has not been
    /// elected/committed, then we consider this a "miss". The leader might be committed later
    /// on, but we don't consider this a case where the leader has been committed "on time".
    pub leader_commit_accuracy: IntCounterVec,
    /// Count leader certificates committed, and whether the leader has strong support.
    pub leader_commits: IntCounterVec,
    /// number of bad nodes in the committee
    pub num_of_bad_nodes: IntGauge,
    /// This node's header vote requests rejected by a peer, labeled by the rejecting validator
    /// (`authority`) and `reason` (`too_old` | `epoch_mismatch`). Node-local: high totals mean
    /// this node is falling behind and being rejected by the committee.
    pub vote_request_rejections: IntCounterVec,
    /// Size of the committee for the current epoch.
    pub committee_size: IntGauge,
    /// Committed certificates per validator (by `authority`) — a direct per-validator liveness
    /// signal: which validators' certificates actually land in committed sub-dags. Consistent
    /// across nodes (every node commits the same sub-dags), unlike node-local rejection counts.
    pub validator_participation: IntCounterVec,
}

impl ConsensusMetrics {
    fn try_new(registry: &Registry) -> Result<Self, prometheus::Error> {
        Ok(Self {
            consensus_dag_rounds: register_int_gauge_vec_with_registry!(
                "consensus_dag_rounds",
                "The number of rounds for which the consensus Dag holds certificates",
                &[],
                registry
            )?,
            last_committed_round: register_int_gauge_vec_with_registry!(
                "last_committed_round",
                "The most recent round that has been committed from consensus",
                &[],
                registry
            )?,
            current_epoch: register_int_gauge_with_registry!(
                "current_epoch",
                "The current epoch of the consensus protocol",
                registry
            )?,
            recovered_consensus_state: register_int_counter_with_registry!(
                "recovered_consensus_state",
                "The number of times the consensus state was restored from the consensus store following a node restart",
                registry
            )?,
            recovered_consensus_output: register_int_counter_with_registry!(
                "recovered_consensus_output",
                "The number of certificates from consensus that were restored and sent to the executor following a node restart",
                registry
            )?,
            commit_rounds_latency: register_histogram_with_registry!(
                "consensus_commit_rounds_latency",
                "The latency between two successful commit rounds (when we have successful leader election)",
                // buckets in seconds
                LATENCY_SEC_BUCKETS.to_vec(),
                registry
            )?,
            committed_certificates: MystenHistogram::new_in_registry(
                "committed_certificates",
                "The number of certificates committed on a commit round",
                registry
            ),
            certificate_commit_latency: register_histogram_with_registry!(
                "certificate_commit_latency",
                "The time it takes for a certificate from the moment it gets created up to the moment it gets committed.",
                LATENCY_SEC_BUCKETS.to_vec(),
                registry
            )?,
            leader_commit_accuracy: register_int_counter_vec_with_registry!(
                "leader_commit_accuracy",
                "Whether a leader commit has been triggered on time - meaning that network hasn't progress to the next even round before it got committed",
                &["outcome", "authority"],
                registry
            )?,
            leader_election: register_int_counter_vec_with_registry!(
                "leader_election",
                "The outcome of a leader election round",
                &["outcome", "authority"],
                registry
            )?,
            leader_commits: register_int_counter_vec_with_registry!(
                "leader_commits",
                "Count leader commits, broken down by strong vs weak support",
                &["type"],
                registry
            )?,
            num_of_bad_nodes: register_int_gauge_with_registry!(
                "num_of_bad_nodes",
                "The number of bad nodes in the new leader schedule",
                registry
            )?,
            vote_request_rejections: register_int_counter_vec_with_registry!(
                "vote_request_rejections",
                "This node's header vote requests rejected by a peer, by rejecting validator (authority) and reason",
                &["authority", "reason"],
                registry
            )?,
            committee_size: register_int_gauge_with_registry!(
                "committee_size",
                "Size of the committee for the current epoch",
                registry
            )?,
            validator_participation: register_int_counter_vec_with_registry!(
                "validator_participation",
                "Committed certificates per validator (by authority) - a per-validator liveness signal",
                &["authority"],
                registry
            )?,
        })
    }
}

impl Default for ConsensusMetrics {
    fn default() -> Self {
        // try_new() should not fail except under certain conditions with testing (see comment
        // below). This pushes the panic or retry decision lower and supporting try_new
        // allways a user to deal with errors if desired (have a non-panic option).
        // We always want do use default_registry() when not in test.
        match Self::try_new(default_registry()) {
            Ok(metrics) => metrics,
            Err(e) => {
                tracing::warn!(target: "rayls::metrics", ?e, "Executor::try_new ConsensusMetrics error");
                // If we are in a test then don't panic on prometheus errors (usually an already
                // registered error) but try again with a new Registry. This is not
                // great for prod code, however should not happen, but will happen in tests due to
                // how Rust runs them so lets just gloss over it. cfg(test) does not
                // always work as expected.
                Self::try_new(&Registry::new()).expect("Prometheus error, are you using it wrong?")
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct ChannelMetrics {
    /// occupancy of the channel from the `Consensus` to `SubscriberHandler`.
    /// See also:
    /// * tx_committed_certificates in primary, where the committed certificates from `Consensus`
    ///   are sent to `primary::StateHandler`
    /// * tx_new_certificates where the newly accepted certificates are sent from
    ///   `primary::Synchronizer` to `Consensus`
    pub tx_sequence: IntGauge,
}

impl ChannelMetrics {
    fn try_new(registry: &Registry) -> Result<Self, prometheus::Error> {
        Ok(Self {
            tx_sequence: register_int_gauge_with_registry!(
                "tx_sequence",
                "occupancy of the channel from the `Consensus` to `SubscriberHandler`",
                registry
            )?,
        })
    }
}

impl Default for ChannelMetrics {
    fn default() -> Self {
        // try_new() should not fail except under certain conditions with testing (see comment
        // below). This pushes the panic or retry decision lower and supporting try_new
        // allways a user to deal with errors if desired (have a non-panic option).
        // We always want do use default_registry() when not in test.
        match Self::try_new(default_registry()) {
            Ok(metrics) => metrics,
            Err(e) => {
                tracing::warn!(target: "rayls::metrics", ?e, "Executor::try_new ChannelMetrics error");
                // If we are in a test then don't panic on prometheus errors (usually an already
                // registered error) but try again with a new Registry. This is not
                // great for prod code, however should not happen, but will happen in tests due to
                // how Rust runs them so lets just gloss over it. cfg(test) does not
                // always work as expected.
                Self::try_new(&Registry::new()).expect("Prometheus error, are you using it wrong?")
            }
        }
    }
}
