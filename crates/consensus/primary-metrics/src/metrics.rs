//! Metrics for the primary node.

use prometheus::{
    default_registry, linear_buckets, register_histogram_vec_with_registry,
    register_histogram_with_registry, register_int_counter_vec_with_registry,
    register_int_counter_with_registry, register_int_gauge_vec_with_registry,
    register_int_gauge_with_registry, Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Registry,
};
use std::sync::Arc;

const LATENCY_SEC_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.05, 0.1, 0.15, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4,
    1.6, 1.8, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0, 6.5, 7.0, 7.5, 8.0, 8.5, 9.0, 9.5, 10.,
    12.5, 15., 17.5, 20., 25., 30., 60., 90., 120., 180., 300.,
];

#[derive(Clone, Debug)]
pub struct Metrics {
    pub primary_channel_metrics: Arc<PrimaryChannelMetrics>,
    pub node_metrics: Arc<PrimaryMetrics>,
}

impl Metrics {
    fn try_new(registry: &Registry) -> Result<Self, prometheus::Error> {
        // The metrics used for measuring the occupancy of the channels in the primary
        let primary_channel_metrics = Arc::new(PrimaryChannelMetrics::try_new(registry)?);

        // Essential/core metrics across the primary node
        let node_metrics = Arc::new(PrimaryMetrics::try_new(registry)?);

        Ok(Metrics { node_metrics, primary_channel_metrics })
    }
}

impl Default for Metrics {
    fn default() -> Self {
        // try_new() should not fail except under certain conditions with testing (see comment
        // below). This pushes the panic or retry decision lower and supporting try_new
        // allways a user to deal with errors if desired (have a non-panic option).
        // We always want do use default_registry() when not in test.
        match Self::try_new(default_registry()) {
            Ok(metrics) => metrics,
            Err(e) => {
                tracing::warn!(target: "rayls::metrics", ?e, "Executor::try_new metrics error");
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
pub struct PrimaryChannelMetrics {
    /// occupancy of the channel from the `primary::WorkerReceiverHandler` to the
    /// `primary::PayloadReceiver`
    pub tx_others_digests: IntGauge,
    /// occupancy of the channel from the `primary::WorkerReceiverHandler` to the
    /// `primary::Proposer`
    pub tx_our_digests: IntGauge,
    /// occupancy of the channel from the `primary::StateHandler` to the `primary::Proposer`
    pub tx_system_messages: IntGauge,
    /// occupancy of the channel from the `primary::StateSynchronizer` to the `primary::Proposer`
    pub tx_parents: IntGauge,
    /// occupancy of the channel from the `primary::Proposer` to the `primary::Certifier`
    pub tx_headers: IntGauge,
    /// occupancy of the channel from the `primary::StateSynchronizer` to the
    /// `primary::CertificateFetcher`
    pub tx_certificate_fetcher: IntGauge,
    /// occupancy of the channel from the `Consensus` to the `primary::StateHandler`
    pub tx_committed_certificates: IntGauge,
    /// occupancy of the channel from the `primary::StateSynchronizer` to the `Consensus`
    pub tx_new_certificates: IntGauge,
    /// occupancy of the channel signaling own committed headers
    pub tx_committed_own_headers: IntGauge,
    /// An internal synchronizer channel. Occupancy of the channel sending certificates to the
    /// internal task that accepts certificates.
    pub tx_certificate_acceptor: IntGauge,
    /// occupancy of the channel from the primary for epoch certs
    pub tx_new_epoch_certificates: IntGauge,

    // totals
    /// total received on channel from the `primary::WorkerReceiverHandler` to the
    /// `primary::PayloadReceiver`
    pub tx_others_digests_total: IntCounter,
    /// total received on channel from the `primary::WorkerReceiverHandler` to the
    /// `primary::Proposer`
    pub tx_our_digests_total: IntCounter,
    /// total received on channel from the `primary::StateHandler` to the `primary::Proposer`
    pub tx_system_messages_total: IntCounter,
    /// total received on channel from the `primary::StateSynchronizer` to the `primary::Proposer`
    pub tx_parents_total: IntCounter,
    /// total received on channel from the `primary::Proposer` to the `primary::Certifier`
    pub tx_headers_total: IntCounter,
    /// total received on channel from the `primary::StateSynchronizer` to the
    /// `primary::CertificateFetcher`
    pub tx_certificate_fetcher_total: IntCounter,
    /// total received on channel from the `primary::WorkerReceiverHandler` to the
    /// `primary::StateHandler`
    pub tx_state_handler_total: IntCounter,
    /// total received on channel from the `Consensus` to the `primary::StateHandler`
    pub tx_committed_certificates_total: IntCounter,
    /// total received on channel from the `primary::StateSynchronizer` to the `Consensus`
    pub tx_new_certificates_total: IntCounter,
    /// total received on the channel signaling own committed headers
    pub tx_committed_own_headers_total: IntCounter,
    /// Total received by the channel sending certificates to the internal task that accepts
    /// certificates.
    pub tx_certificate_acceptor_total: IntCounter,
    /// Total received by the channel to manage pending certificates with missing parents.
    pub tx_pending_cert_commands_total: IntCounter,
}

impl PrimaryChannelMetrics {
    // The consistent use of this constant in the below, as well as in `node::spawn_primary` is
    // load-bearing, see `replace_registered_committed_certificates_metric`.
    pub const NAME_COMMITTED_CERTS: &'static str = "tx_committed_certificates";
    pub const DESC_COMMITTED_CERTS: &'static str =
        "occupancy of the channel from the `Consensus` to the `primary::StateHandler`";
    // The consistent use of this constant in the below, as well as in `node::spawn_primary` is
    // load-bearing, see `replace_registered_new_certificates_metric`.
    pub const NAME_NEW_CERTS: &'static str = "tx_new_certificates";
    pub const DESC_NEW_CERTS: &'static str =
        "occupancy of the channel from the `primary::StateSynchronizer` to the `Consensus`";

    // The consistent use of this constant in the below, as well as in `node::spawn_primary` is
    // load-bearing, see `replace_registered_committed_certificates_metric`.
    pub const NAME_COMMITTED_CERTS_TOTAL: &'static str = "tx_committed_certificates_total";
    pub const DESC_COMMITTED_CERTS_TOTAL: &'static str =
        "total received on channel from the `Consensus` to the `primary::StateHandler`";
    // The consistent use of this constant in the below, as well as in `node::spawn_primary` is
    // load-bearing, see `replace_registered_new_certificates_metric`.
    pub const NAME_NEW_CERTS_TOTAL: &'static str = "tx_new_certificates_total";
    pub const DESC_NEW_CERTS_TOTAL: &'static str =
        "total received on channel from the `primary::StateSynchronizer` to the `Consensus`";

    // Private so we can make sure to capture registry properly...
    fn try_new(registry: &Registry) -> Result<Self, prometheus::Error> {
        Ok(Self {
            tx_others_digests: register_int_gauge_with_registry!(
                "tx_others_digests",
                "occupancy of the channel from the `primary::WorkerReceiverHandler` to the `primary::PayloadReceiver`",
                registry
            )?,
            tx_our_digests: register_int_gauge_with_registry!(
                "tx_our_digests",
                "occupancy of the channel from the `primary::WorkerReceiverHandler` to the `primary::Proposer`",
                registry
            )?,
            tx_system_messages: register_int_gauge_with_registry!(
                "tx_system_messages",
                "occupancy of the channel from the `primary::StateHandler` to the `primary::Proposer`",
                registry
            )?,
            tx_parents: register_int_gauge_with_registry!(
                "tx_parents",
                "occupancy of the channel from the `primary::StateSynchronizer` to the `primary::Proposer`",
                registry
            )?,
            tx_headers: register_int_gauge_with_registry!(
                "tx_headers",
                "occupancy of the channel from the `primary::Proposer` to the `primary::Certifier`",
                registry
            )?,
            tx_certificate_fetcher: register_int_gauge_with_registry!(
                "tx_certificate_fetcher",
                "occupancy of the channel from the `primary::StateSynchronizer` to the `primary::CertificateFetcher`",
                registry
            )?,
            tx_committed_certificates: register_int_gauge_with_registry!(
                Self::NAME_COMMITTED_CERTS,
                Self::DESC_COMMITTED_CERTS,
                registry
            )?,
            tx_new_certificates: register_int_gauge_with_registry!(
                Self::NAME_NEW_CERTS,
                Self::DESC_NEW_CERTS,
                registry
            )?,
            tx_committed_own_headers: register_int_gauge_with_registry!(
                "tx_committed_own_headers",
                "occupancy of the channel signaling own committed headers.",
                registry
            )?,
            tx_certificate_acceptor: register_int_gauge_with_registry!(
                "tx_certificate_acceptor",
                "occupancy of the internal synchronizer channel that is accepting new certificates.",
                registry
            )?,
            tx_new_epoch_certificates: register_int_gauge_with_registry!(
                "tx_new_epoch_certicates",
                "new epoch certs as recieved",
                registry
            )?,


            // totals
            tx_others_digests_total: register_int_counter_with_registry!(
                "tx_others_digests_total",
                "total received on channel from the `primary::WorkerReceiverHandler` to the `primary::PayloadReceiver`",
                registry
            )?,
            tx_our_digests_total: register_int_counter_with_registry!(
                "tx_our_digests_total",
                "total received on channel from the `primary::WorkerReceiverHandler` to the `primary::Proposer`",
                registry
            )?,
            tx_system_messages_total: register_int_counter_with_registry!(
                "tx_system_messages_total",
                "total received on channel from the `primary::StateHandler` to the `primary::Proposer`",
                registry
            )?,
            tx_parents_total: register_int_counter_with_registry!(
                "tx_parents_total",
                "total received on channel from the `primary::StateSynchronizer` to the `primary::Proposer`",
                registry
            )?,
            tx_headers_total: register_int_counter_with_registry!(
                "tx_headers_total",
                "total received on channel from the `primary::Proposer` to the `primary::Certifier`",
                registry
            )?,
            tx_certificate_fetcher_total: register_int_counter_with_registry!(
                "tx_certificate_fetcher_total",
                "total received on channel from the `primary::StateSynchronizer` to the `primary::CertificateFetcher`",
                registry
            )?,
            tx_state_handler_total: register_int_counter_with_registry!(
                "tx_state_handler_total",
                "total received on channel from the `primary::WorkerReceiverHandler` to the `primary::StateHandler`",
                registry
            )?,
            tx_committed_certificates_total: register_int_counter_with_registry!(
                Self::NAME_COMMITTED_CERTS_TOTAL,
                Self::DESC_COMMITTED_CERTS_TOTAL,
                registry
            )?,
            tx_new_certificates_total: register_int_counter_with_registry!(
                Self::NAME_NEW_CERTS_TOTAL,
                Self::DESC_NEW_CERTS_TOTAL,
                registry
            )?,
            tx_committed_own_headers_total: register_int_counter_with_registry!(
                "tx_committed_own_headers_total",
                "total received on channel signaling own committed headers.",
                registry
            )?,
            tx_certificate_acceptor_total: register_int_counter_with_registry!(
                "tx_certificate_acceptor_total",
                "total received on the internal synchronizer channel that is accepting new certificates.",
                registry
            )?,
            tx_pending_cert_commands_total: register_int_counter_with_registry!(
                "tx_pending_cert_commands_total",
                "total received on the channel managing pending certificates with missing parents",
                registry
            )?,
        })
    }
}

impl Default for PrimaryChannelMetrics {
    fn default() -> Self {
        match Self::try_new(default_registry()) {
            Ok(metrics) => metrics,
            Err(e) => {
                tracing::warn!(target: "rayls::metrics", ?e, "Executor::try_new metrics error");
                // If we are in a test then don't panic on prometheus errors (usually an already
                // registered error) but try again with a new Registry. This is not
                // great for prod code, however should not happen, but will happen in tests do to
                // how Rust runs them so lets just gloss over it. cfg(test) does not
                // always work as expected.
                Self::try_new(&Registry::new()).expect("Prometheus error, are you using it wrong?")
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct PrimaryMetrics {
    /// count number of headers that the node proposed
    pub headers_proposed: IntCounterVec,
    // total number of parents in all proposed headers, for calculating average number of parents
    // per header.
    pub header_parents: Histogram,
    /// the current proposed header round
    pub proposed_header_round: IntGauge,
    /// The number of received votes for the proposed last round
    pub votes_received_last_round: IntGauge,
    // total number of parent certificates included in votes.
    pub certificates_in_votes: IntCounter,
    /// The round of the latest created certificate by our node
    pub certificate_created_round: IntGauge,
    /// count number of certificates that the node created
    pub certificates_created: IntCounter,
    /// count number of certificates that the node processed (others + own)
    pub certificates_processed: IntCounterVec,
    /// count number of certificates that the node suspended their processing
    pub certificates_suspended: IntCounterVec,
    /// number of certificates that are currently suspended.
    pub certificates_currently_suspended: IntGauge,
    /// The current backoff duration in milliseconds for the certificate fetcher.
    pub certificate_fetcher_backoff_ms: IntGauge,
    /// The number of consecutive fetch failures.
    pub certificate_fetcher_consecutive_failures: IntGauge,
    /// count number of duplicate certificates that the node processed (others + own)
    pub duplicate_certificates_processed: IntCounter,
    /// The current Narwhal round in proposer
    pub current_round: IntGauge,
    /// Latency distribution for generating proposals
    pub proposal_latency: HistogramVec,
    /// The highest Narwhal round of certificates that have been accepted.
    pub highest_processed_round: IntGaugeVec,
    /// The highest Narwhal round that has been received.
    pub highest_received_round: IntGaugeVec,
    /// 0 if there is no inflight certificates fetching, 1 otherwise.
    pub certificate_fetcher_inflight_fetch: IntGauge,
    /// Number of fetched certificates successfully processed by core.
    pub certificate_fetcher_num_certificates_processed: IntCounter,
    /// Total time spent in certificate verifications, in microseconds.
    pub certificate_fetcher_total_verification_us: IntCounter,
    /// Number of votes that were requested but not sent due to previously having voted differently
    pub votes_dropped_equivocation_protection: IntCounter,
    /// Number of pending batches in proposer
    pub num_of_pending_batches_in_proposer: IntGauge,
    /// A histogram to track the number of batches included
    /// per header.
    pub num_of_batch_digests_in_header: HistogramVec,
    /// A counter that keeps the number of instances where the proposer
    /// is ready/not ready to advance, by round type (even/odd).
    pub proposer_ready_to_advance: IntCounterVec,
    /// Gauge tracking current ready-to-advance status (1 = ready, 0 = not ready).
    pub proposer_ready_status: IntGauge,
    /// The latency of a batch between the time it has been
    /// created and until it has been included to a header proposal.
    pub proposer_batch_latency: Histogram,
    /// The number of headers being resent because they will not get committed.
    pub proposer_resend_headers: IntCounter,
    /// The number of batches being resent because they will not get committed.
    pub proposer_resend_batches: IntCounter,
    /// The number of batch digests dropped due to queue capacity limit.
    pub proposer_dropped_digests: IntCounter,
    /// Time it takes for a header to be materialised to a certificate
    pub header_to_certificate_latency: Histogram,
    /// Millisecs taken to wait for max parent time, when proposing headers.
    pub header_max_parent_wait_ms: IntCounter,
    /// Counts when the GC loop in synchronizer times out waiting for consensus commit.
    pub synchronizer_gc_timeout: IntCounter,
    // Total number of fetched certificates verified directly.
    pub fetched_certificates_verified_directly: IntCounter,
    // Total number of fetched certificates verified indirectly.
    pub fetched_certificates_verified_indirectly: IntCounter,
}

impl PrimaryMetrics {
    fn try_new(registry: &Registry) -> Result<Self, prometheus::Error> {
        let parents_buckets = [
            linear_buckets(1.0, 1.0, 20)
                .expect("prometheus, invalid width or count on bucket create")
                .as_slice(),
            linear_buckets(21.0, 2.0, 20)
                .expect("prometheus, invalid width or count on bucket create")
                .as_slice(),
            linear_buckets(61.0, 3.0, 20)
                .expect("prometheus, invalid width or count on bucket create")
                .as_slice(),
        ]
        .concat();
        Ok(Self {
            headers_proposed: register_int_counter_vec_with_registry!(
                "headers_proposed",
                "Number of headers that node proposed",
                &["leader_support"],
                registry
            )?,
            header_parents: register_histogram_with_registry!(
                "header_parents",
                "Number of parents included in proposed headers",
                parents_buckets,
                registry
            )?,
            proposed_header_round: register_int_gauge_with_registry!(
                "proposed_header_round",
                "The current proposed header round",
                registry
            )?,
            votes_received_last_round: register_int_gauge_with_registry!(
                "votes_received_last_round",
                "The number of received votes for the proposed last round",
                registry
            )?,
            certificates_in_votes: register_int_counter_with_registry!(
                "certificates_in_votes",
                "Total number of parent certificates included in votes.",
                registry
            )?,
            certificate_created_round: register_int_gauge_with_registry!(
                "certificate_created_round",
                "The round of the latest created certificate by our node",
                registry
            )?,
            certificates_created: register_int_counter_with_registry!(
                "certificates_created",
                "Number of certificates that node created",
                registry
            )?,
            certificates_processed: register_int_counter_vec_with_registry!(
                "certificates_processed",
                "Number of certificates that node processed (others + own)",
                &["source"],
                registry
            )?,
            certificates_suspended: register_int_counter_vec_with_registry!(
                "certificates_suspended",
                "Number of certificates that node suspended processing of",
                &["reason"],
                registry
            )?,
            certificates_currently_suspended: register_int_gauge_with_registry!(
                "certificates_currently_suspended",
                "Number of certificates that are suspended in memory",
                registry
            )?,
            certificate_fetcher_backoff_ms: register_int_gauge_with_registry!(
                "certificate_fetcher_backoff_ms",
                "Current backoff duration in milliseconds for the certificate fetcher",
                registry
            )?,
            certificate_fetcher_consecutive_failures: register_int_gauge_with_registry!(
                "certificate_fetcher_consecutive_failures",
                "Number of consecutive certificate fetch failures",
                registry
            )?,
            duplicate_certificates_processed: register_int_counter_with_registry!(
                "duplicate_certificates_processed",
                "Number of certificates that node processed (others + own)",
                registry
            )?,
            current_round: register_int_gauge_with_registry!(
                "current_round",
                "Current round the node will propose",
                registry
            )?,
            proposal_latency: register_histogram_vec_with_registry!(
                "proposal_latency",
                "Time distribution between node proposals",
                &["reason"],
                LATENCY_SEC_BUCKETS.to_vec(),
                registry
            )?,
            highest_received_round: register_int_gauge_vec_with_registry!(
                "highest_received_round",
                "Highest round received by the primary",
                &["source"],
                registry
            )?,
            highest_processed_round: register_int_gauge_vec_with_registry!(
                "highest_processed_round",
                "Highest round processed (stored) by the primary",
                &["source"],
                registry
            )?,
            certificate_fetcher_inflight_fetch: register_int_gauge_with_registry!(
                "certificate_fetcher_inflight_fetch",
                "0 if there is no inflight certificates fetching, 1 otherwise.",
                registry
            )?,
            certificate_fetcher_num_certificates_processed: register_int_counter_with_registry!(
                "certificate_fetcher_num_certificates_processed",
                "Number of fetched certificates successfully processed by core.",
                registry
            )?,
            certificate_fetcher_total_verification_us: register_int_counter_with_registry!(
                "certificate_fetcher_total_verification_us",
                "Total time spent in certificate verifications, in microseconds.",
                registry
            )?,
            votes_dropped_equivocation_protection: register_int_counter_with_registry!(
                "votes_dropped_equivocation_protection",
                "Number of votes that were requested but not sent due to previously having voted differently",
                registry
            )?,
            num_of_pending_batches_in_proposer: register_int_gauge_with_registry!(
                "num_of_pending_batches_in_proposer",
                "Number of batch digests pending in proposer for next header proposal",
                registry
            )?,
            num_of_batch_digests_in_header: register_histogram_vec_with_registry!(
                "num_of_batch_digests_in_header",
                "The number of batch digests included in a proposed header. A reason label is included.",
                &["reason"],
                // buckets in number of digests
                vec![0.0, 5.0, 10.0, 15.0, 32.0, 50.0, 100.0, 200.0, 500.0, 1000.0],
                registry
            )?,
            proposer_ready_to_advance: register_int_counter_vec_with_registry!(
                "proposer_ready_to_advance",
                "The number of times where the proposer is ready/not ready to advance, by round type.",
                &["round"],
                registry
            )?,
            proposer_ready_status: register_int_gauge_with_registry!(
                "proposer_ready_status",
                "Current ready-to-advance status (1 = ready, 0 = not ready)",
                registry
            )?,
            proposer_batch_latency: register_histogram_with_registry!(
                "proposer_batch_latency",
                "The latency of a batch between the time it has been created and until it has been included to a header proposal.",
                LATENCY_SEC_BUCKETS.to_vec(),
                registry
            )?,
            proposer_resend_headers: register_int_counter_with_registry!(
                "proposer_resend_headers",
                "The number of headers being resent because they will not get committed.",
                registry
            )?,
            proposer_resend_batches: register_int_counter_with_registry!(
                "proposer_resend_batches",
                "The number of batches being resent because they will not get committed.",
                registry
            )?,
            proposer_dropped_digests: register_int_counter_with_registry!(
                "proposer_dropped_digests",
                "The number of batch digests dropped due to queue capacity limit.",
                registry
            )?,
            header_to_certificate_latency: register_histogram_with_registry!(
                "header_to_certificate_latency",
                "Time it takes for a header to be materialised to a certificate",
                LATENCY_SEC_BUCKETS.to_vec(),
                registry
            )?,
            header_max_parent_wait_ms: register_int_counter_with_registry!(
                "header_max_parent_wait_ms",
                "Millisecs taken to wait for max parent time, when proposing headers.",
                registry
            )?,
            synchronizer_gc_timeout: register_int_counter_with_registry!(
                "synchronizer_gc_timeout",
                "Counts when the GC loop in synchronizer times out waiting for consensus commit.",
                registry
            )?,
            fetched_certificates_verified_directly: register_int_counter_with_registry!(
                "fetched_certificates_verified_directly",
                "Total number of fetched certificates verified directly.",
                registry
            )?,
            fetched_certificates_verified_indirectly: register_int_counter_with_registry!(
                "fetched_certificates_verified_indirectly",
                "Total number of fetched certificates verified indirectly.",
                registry
            )?,
        })
    }
}

impl Default for PrimaryMetrics {
    fn default() -> Self {
        match Self::try_new(default_registry()) {
            Ok(metrics) => metrics,
            Err(e) => {
                tracing::warn!(target: "rayls::metrics", ?e, "Executor::try_new metrics error");
                // If we are in a test then don't panic on prometheus errors (usually an already
                // registered error) but try again with a new Registry. This is not
                // great for prod code, however should not happen, but will happen in tests do to
                // how Rust runs them so lets just gloss over it. cfg(test) does not
                // always work as expected.
                Self::try_new(&Registry::new()).expect("Prometheus error, are you using it wrong?")
            }
        }
    }
}
