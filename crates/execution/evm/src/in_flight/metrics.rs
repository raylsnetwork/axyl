use prometheus::{default_registry, IntCounter, IntGauge, Registry};
use std::sync::LazyLock;

const IN_FLIGHT_GAUGE: &str = "rayls_txpool_in_flight";
const MARKED_COUNTER: &str = "rayls_txpool_in_flight_marked_total";
const RELEASED_RECONCILE_COUNTER: &str = "rayls_txpool_in_flight_released_reconcile";
const RELEASED_TTL_COUNTER: &str = "rayls_txpool_in_flight_released_ttl";
const RELEASED_CLEAR_COUNTER: &str = "rayls_txpool_in_flight_released_clear";
const RELEASED_DROPPED_COUNTER: &str = "rayls_txpool_in_flight_released_dropped";
const MARKED_FORWARD_COUNTER: &str = "rayls_txpool_in_flight_marked_forward";

/// Prometheus counters and gauge backing one `InFlightTracker`; each registration description is
/// that metric's HELP text.
#[derive(Clone, Debug)]
pub(super) struct InFlightMetrics {
    pub gauge: IntGauge,
    pub marked: IntCounter,
    pub marked_forward: IntCounter,
    pub released_reconcile: IntCounter,
    pub released_dropped: IntCounter,
    pub released_ttl: IntCounter,
    pub released_clear: IntCounter,
}

impl InFlightMetrics {
    /// Registers every counter/gauge against the given registry; fails if any metric name is
    /// already registered there.
    pub(crate) fn register(registry: &Registry) -> Result<Self, prometheus::Error> {
        Ok(Self {
            gauge: prometheus::register_int_gauge_with_registry!(
                IN_FLIGHT_GAUGE,
                "Transactions currently marked in-flight (sealed or forwarded, not yet executed)",
                registry
            )?,
            marked: prometheus::register_int_counter_with_registry!(
                MARKED_COUNTER,
                "Total hashes marked in-flight",
                registry
            )?,
            marked_forward: prometheus::register_int_counter_with_registry!(
                MARKED_FORWARD_COUNTER,
                "Hashes marked in-flight by the forwarder (the rest of `marked` is the sealer's)",
                registry
            )?,
            released_reconcile: prometheus::register_int_counter_with_registry!(
                RELEASED_RECONCILE_COUNTER,
                "Marks released because the transaction left the PENDING sub-pool (executed, superseded, evicted, or downgraded to queued)",
                registry
            )?,
            released_dropped: prometheus::register_int_counter_with_registry!(
                RELEASED_DROPPED_COUNTER,
                "Held marks released because the sender's state nonce advanced (nonce too high); the tx stays pooled and re-sealable",
                registry
            )?,
            released_ttl: prometheus::register_int_counter_with_registry!(
                RELEASED_TTL_COUNTER,
                "Marks released by the TTL sweep (the sealed batch never executed)",
                registry
            )?,
            released_clear: prometheus::register_int_counter_with_registry!(
                RELEASED_CLEAR_COUNTER,
                "Marks released by the epoch-transition clear",
                registry
            )?,
        })
    }
}

/// The process-wide metrics instance shared by every `InFlightTracker::new()`. Falls back to a
/// fresh, unregistered registry if `default_registry()` already has these names registered (e.g.
/// a second tracker in the same process during tests), so construction never panics in that case.
pub(crate) static IN_FLIGHT_METRICS: LazyLock<InFlightMetrics> = LazyLock::new(|| {
    InFlightMetrics::register(default_registry())
        .unwrap_or_else(|_| InFlightMetrics::register(&Registry::new()).expect("fresh registry"))
});

#[cfg(test)]
impl InFlightMetrics {
    /// Registers a fresh, isolated instance for a test so its counters cannot collide with the
    /// process-wide `IN_FLIGHT_METRICS` or another test's.
    pub(crate) fn register_fresh() -> Self {
        Self::register(&Registry::new()).expect("a fresh registry should always succeed")
    }

    /// Returns `(marked, released_reconcile, released_ttl, released_clear)` for assertions.
    pub(crate) fn counts(&self) -> (u64, u64, u64, u64) {
        (
            self.marked.get(),
            self.released_reconcile.get(),
            self.released_ttl.get(),
            self.released_clear.get(),
        )
    }

    /// Derives the outstanding mark count from the counters rather than reading the gauge
    /// directly, so a test can cross-check the gauge against an independent computation.
    pub(crate) fn outstanding(&self) -> i64 {
        let (marked, reconcile, ttl, clear) = self.counts();
        marked as i64 - reconcile as i64 - ttl as i64 - clear as i64
    }

    /// Returns the current gauge reading.
    pub(crate) fn gauge(&self) -> i64 {
        self.gauge.get()
    }
}
