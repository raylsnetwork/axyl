//! Instrumentation for the transaction forwarder's scan, delivery, and breaker paths.

use prometheus::{
    default_registry, register_histogram_with_registry, register_int_counter_with_registry,
    Histogram, IntCounter, Registry,
};
use std::{sync::LazyLock, time::Duration};

/// Forwarder metrics; a clone of the process-wide [`FORWARD_METRICS`] family.
#[derive(Clone)]
pub(super) struct ForwardMetrics {
    /// Seconds spent scanning the pending pool and grouping due transactions per tick.
    scan_duration: Histogram,
    /// Total pending transactions inspected across ticks (the scan's input size).
    pending_examined: IntCounter,
    /// Total transactions that passed the send gate across ticks (the scan's useful output).
    forwarded: IntCounter,
    /// Subset of `forwarded` that re-sent an already-published hash; a sustained rate is a flood.
    resent: IntCounter,
    /// Hashes a validator acked as already-executed (stale) on a direct submit.
    acked_stale: IntCounter,
    /// Ticks where re-sends were gated because local execution trailed the seen header.
    resend_gated: IntCounter,
    /// Submits a validator answered with the not-batch-producing reply.
    mode_rejected: IntCounter,
    /// Per-validator breakers opened by local failure evidence.
    breaker_tripped: IntCounter,
}

impl ForwardMetrics {
    /// Register the family on `registry`, failing if a name is already registered there.
    fn register(registry: &Registry) -> Result<Self, prometheus::Error> {
        Ok(Self {
            scan_duration: register_histogram_with_registry!(
                "rayls_txn_forwarder_scan_duration_seconds",
                "Seconds spent scanning the pending pool and grouping due transactions per tick",
                vec![
                    0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1
                ],
                registry
            )?,
            pending_examined: register_int_counter_with_registry!(
                "rayls_txn_forwarder_pending_examined_total",
                "Total pending transactions inspected across forward ticks",
                registry
            )?,
            forwarded: register_int_counter_with_registry!(
                "rayls_txn_forwarder_forwarded_total",
                "Total transactions that passed the send gate across forward ticks",
                registry
            )?,
            resent: register_int_counter_with_registry!(
                "rayls_txn_forwarder_resent_total",
                "Sends that re-forwarded an already-published hash (a sustained rate is a flood)",
                registry
            )?,
            acked_stale: register_int_counter_with_registry!(
                "rayls_txn_forwarder_acked_stale_total",
                "Hashes a validator acked as already-executed on a direct submit",
                registry
            )?,
            resend_gated: register_int_counter_with_registry!(
                "rayls_txn_forwarder_resend_gated_total",
                "Ticks where re-sends were withheld because this node trailed the seen header",
                registry
            )?,
            mode_rejected: register_int_counter_with_registry!(
                "rayls_txn_forwarder_mode_rejected_total",
                "Submits a validator answered with the not-batch-producing reply",
                registry
            )?,
            breaker_tripped: register_int_counter_with_registry!(
                "rayls_txn_forwarder_breaker_tripped_total",
                "Per-validator breakers opened by local failure evidence",
                registry
            )?,
        })
    }

    /// Register against a private registry (the unscraped fallback).
    fn register_fresh() -> Self {
        Self::register(&Registry::new()).expect("a fresh registry should always succeed")
    }

    /// Record one scan tick: its duration and the pending, forwarded, and re-sent counts.
    pub(super) fn on_scan(&self, duration: Duration, pending: u64, forwarded: u64, resent: u64) {
        self.scan_duration.observe(duration.as_secs_f64());
        self.pending_examined.inc_by(pending);
        self.forwarded.inc_by(forwarded);
        self.resent.inc_by(resent);
    }

    /// Record a tick whose re-sends were withheld because this node trailed the seen header.
    pub(super) fn on_resend_gated(&self) {
        self.resend_gated.inc();
    }

    /// Record hashes a validator acked as already-executed on a direct submit.
    pub(super) fn on_acked_stale(&self, count: u64) {
        self.acked_stale.inc_by(count);
    }

    /// Record a submit a validator refused because it is not batch-producing.
    pub(super) fn on_mode_rejected(&self) {
        self.mode_rejected.inc();
    }

    /// Record a breaker trip.
    pub(super) fn on_breaker_tripped(&self) {
        self.breaker_tripped.inc();
    }
}

/// Registered once per process, outliving the per-epoch forwarder. Falls back to a private registry
/// if the default already holds the family, degrading to unscraped instead of aborting.
pub(super) static FORWARD_METRICS: LazyLock<ForwardMetrics> = LazyLock::new(|| {
    ForwardMetrics::register(default_registry())
        .unwrap_or_else(|_| ForwardMetrics::register_fresh())
});
