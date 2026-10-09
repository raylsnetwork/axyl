//! Optional tokio runtime and per-task metrics (poll time, scheduling delay).
//!
//! Off unless [`enable`] is called. Once enabled:
//! - the runtime reporter publishes `tokio_*` gauges/counters (worker busy time, queue depths, and
//!   — in `--cfg tokio_unstable` builds — mean poll time and a poll-time histogram);
//! - every task spawned through [`TaskManager`](crate::TaskManager) /
//!   [`TaskSpawner`](crate::TaskSpawner) is wrapped in a [`TaskMonitor`] shared by all tasks of the
//!   same kind, published as `tokio_task_*{task="<kind>"}`.
//!
//! Everything goes through the global `metrics` recorder, so the caller must install it (reth's
//! Prometheus recorder) before calling [`enable`]; handles captured earlier bind to the no-op
//! recorder for good.

use futures::future::Either;
use metrics::{Key, Label};
use parking_lot::Mutex;
use std::{
    collections::HashMap,
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        LazyLock, OnceLock,
    },
    time::Duration,
};
use tokio::task::JoinHandle;
use tokio_metrics::{
    Instrumented, RuntimeMetricsReporterBuilder, TaskMetricsReporterBuilder, TaskMonitor,
};

/// Distinct task kinds that get their own monitor; anything past this shares [`OVERFLOW_KIND`].
const MAX_TASK_KINDS: usize = 256;
/// Label for tasks past [`MAX_TASK_KINDS`].
const OVERFLOW_KIND: &str = "other";
/// Name tokens longer than this are treated as identifiers, not words.
const MAX_WORD_LEN: usize = 32;

static ENABLED: AtomicBool = AtomicBool::new(false);
static INTERVAL: OnceLock<Duration> = OnceLock::new();
static MONITORS: LazyLock<Mutex<HashMap<String, TaskMonitor>>> = LazyLock::new(Default::default);

/// Start reporting runtime metrics and instrument tasks spawned from now on.
///
/// Must run inside the tokio runtime to monitor, after the global `metrics` recorder is
/// installed. `interval` is how often the monitors are sampled. Calling it again is a no-op.
pub fn enable(interval: Duration) {
    if INTERVAL.set(interval).is_err() {
        return;
    }
    tokio::spawn(
        RuntimeMetricsReporterBuilder::default().with_interval(interval).describe_and_run(),
    );
    ENABLED.store(true, Ordering::Release);
}

/// Whether [`enable`] has been called.
pub fn is_enabled() -> bool {
    ENABLED.load(Ordering::Acquire)
}

/// Runtime builder settings the monitors need. The poll-time histogram only exists in
/// `--cfg tokio_unstable` builds; elsewhere this is a no-op.
pub fn configure_runtime(builder: &mut tokio::runtime::Builder) {
    #[cfg(tokio_unstable)]
    builder.enable_metrics_poll_time_histogram().metrics_poll_time_histogram_configuration(
        tokio::runtime::HistogramConfiguration::log(
            // doubling buckets, 16µs .. ~1s
            tokio::runtime::LogHistogram::builder()
                .min_value(Duration::from_micros(16))
                .max_value(Duration::from_secs(1))
                .precision_exact(0)
                .build(),
        ),
    );
    #[cfg(not(tokio_unstable))]
    let _ = builder;
}

/// `tokio::spawn`, instrumented under `name`'s task kind when metrics are enabled.
pub(crate) fn spawn<F>(name: &str, future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(instrument(name, future))
}

/// Wrap `future` in the [`TaskMonitor`] for `name`'s task kind, or return it as is when metrics
/// are disabled.
pub(crate) fn instrument<F: Future>(name: &str, future: F) -> Either<Instrumented<F>, F> {
    if !is_enabled() {
        return Either::Right(future);
    }
    Either::Left(monitor_for(&task_kind(name)).instrument(future))
}

/// The shared monitor for `kind`, creating it (and its reporter) on first use.
fn monitor_for(kind: &str) -> TaskMonitor {
    let mut monitors = MONITORS.lock();
    if let Some(monitor) = monitors.get(kind) {
        return monitor.clone();
    }
    let kind = if monitors.len() < MAX_TASK_KINDS { kind } else { OVERFLOW_KIND };
    monitors
        .entry(kind.to_string())
        .or_insert_with(|| {
            let monitor = TaskMonitor::new();
            let label = kind.to_string();
            let interval = INTERVAL.get().copied().unwrap_or(Duration::from_secs(5));
            tokio::spawn(
                TaskMetricsReporterBuilder::new(move |name| {
                    Key::from_parts(
                        name.replacen("tokio_", "tokio_task_", 1),
                        vec![Label::new("task", label.clone())],
                    )
                })
                .with_interval(interval)
                .describe_and_run(monitor.clone()),
            );
            monitor
        })
        .clone()
}

/// Collapse a task name to its bounded kind so per-request names share one monitor and one label
/// value: identifier-like tokens become `*` (`VoteRequest-<digest>` -> `VoteRequest-*`,
/// `DialPeer <bls key>` -> `DialPeer *`) and anything from the first `{`/`(`/`[` on (`Debug`
/// output) is dropped.
fn task_kind(name: &str) -> String {
    let is_separator = |c: char| matches!(c, ' ' | '-' | ':' | '/');
    let is_word = |token: &str| {
        token.len() <= MAX_WORD_LEN && token.chars().all(|c| c.is_ascii_alphabetic() || c == '_')
    };

    let name = name.split(['{', '(', '[']).next().unwrap_or_default();
    let mut kind = String::with_capacity(name.len());
    let mut has_word = false;
    let mut last_wildcard = false;
    for piece in name.split_inclusive(is_separator) {
        let token = piece.trim_end_matches(is_separator);
        let separator = &piece[token.len()..];
        if token.is_empty() {
            kind.push_str(separator);
        } else if is_word(token) {
            kind.push_str(piece);
            has_word = true;
            last_wildcard = false;
        } else if !last_wildcard {
            kind.push('*');
            kind.push_str(separator);
            last_wildcard = true;
        }
    }

    if !has_word {
        return "unnamed".to_string();
    }
    kind.truncate(kind.trim_end_matches(is_separator).len());
    kind
}

#[cfg(test)]
mod tests {
    use super::task_kind;

    #[test]
    fn task_kind_keeps_static_names() {
        assert_eq!(task_kind("certifier task"), "certifier task");
        assert_eq!(task_kind("Worker Network Peers"), "Worker Network Peers");
        assert_eq!(
            task_kind("state sync: stream consensus headers"),
            "state sync: stream consensus headers"
        );
        assert_eq!(task_kind("reth-blocking-task"), "reth-blocking-task");
    }

    #[test]
    fn task_kind_replaces_identifiers() {
        assert_eq!(task_kind("VoteRequest-0x3f9a1c00ab"), "VoteRequest-*");
        assert_eq!(
            task_kind("DialPeer 7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU"),
            "DialPeer *"
        );
        assert_eq!(
            task_kind(
                "ProcessGossip-tn-primary-12D3KooWEyoppNCUx8Yx66oV9fJnriXwCcXwDDUA2kj6vnc6iDEp"
            ),
            "ProcessGossip-tn-primary-*"
        );
        assert_eq!(task_kind("worker-0 batch-builder"), "worker-* batch-builder");
        assert_eq!(task_kind("a 0x1 0x2 b"), "a * b");
    }

    #[test]
    fn task_kind_drops_debug_output() {
        assert_eq!(task_kind("vote-Header { epoch: 3, round: 7 }-node"), "vote-Header");
        assert_eq!(task_kind("propose-header-Digest(0x3f9a)"), "propose-header-Digest");
    }

    #[test]
    fn task_kind_falls_back_for_unnamed() {
        assert_eq!(task_kind(""), "unnamed");
        assert_eq!(task_kind("0xdeadbeef"), "unnamed");
        assert_eq!(task_kind("--"), "unnamed");
    }
}
