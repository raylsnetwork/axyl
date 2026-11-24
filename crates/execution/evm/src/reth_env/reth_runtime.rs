//! reth's task runtime, tied to our [`TaskManager`].
//!
//! reth 2.0 runs its own background tasks (transaction validation, pool sub-tasks, RPC) on a
//! [`reth_tasks::Runtime`]. Our [`TaskManager`] does not see those tasks, so two things are
//! bridged here:
//!
//! - a panic in a critical reth task stops the node, like a panic in one of our critical tasks
//! - when our [`TaskManager`] shuts down, reth's shutdown signal fires, so those reth tasks stop
//!   with it. This is what happened on reth 1.11, where they ran on our task manager.
//!
//! # What the shutdown signal stops, and what it does not
//!
//! reth has two ways to start work on a `Runtime`:
//!
//! - `spawn_task`, `spawn_critical_task`, `spawn_blocking_task` and the `*_with_signal` forms wrap
//!   the future so it ends when the shutdown signal fires. Transaction validation, the pool
//!   sub-tasks and the RPC tasks are started this way.
//! - `spawn_blocking`, `spawn_blocking_named` and the worker pools do not look at the signal. Block
//!   building (state root, prewarming, trie hashing) and storage use only these.
//!
//! So the signal can fire while the engine still drains its last blocks, which is the case on
//! SIGTERM: the engine's task manager is dropped before the drain and the final flush. The
//! tests below pin that plain blocking work still runs after the signal.

use rayls_infrastructure_types::{TaskKind, TaskManager};
use reth_tasks::{Runtime, RuntimeBuilder, RuntimeConfig, TokioConfig};
use std::time::Duration;
use tracing::{debug, warn};

/// Name of the task that watches reth's critical tasks.
pub(crate) const RETH_RUNTIME_MONITOR_TASK: &str = "reth-runtime-monitor";
/// Name of the task that fires reth's shutdown signal on our shutdown.
pub(crate) const RETH_RUNTIME_SHUTDOWN_TASK: &str = "reth-runtime-shutdown";

/// How long our shutdown waits for reth tasks that asked for a graceful shutdown.
///
/// Must stay below the task manager's phase stall bound (4s), or a slow reth task would make
/// the ordered teardown escalate to a forced abort. None of the reth tasks we run asks for a
/// graceful shutdown today, so the wait normally ends at once.
const RETH_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);

/// Build the reth [`Runtime`] on the current tokio runtime and tie it to `task_manager`.
///
/// MUST be called from within a tokio runtime.
pub(crate) fn build_reth_runtime(task_manager: &TaskManager) -> eyre::Result<Runtime> {
    let runtime = RuntimeBuilder::new(
        RuntimeConfig::default()
            .with_tokio(TokioConfig::existing_handle(tokio::runtime::Handle::current())),
    )
    .build()?;
    bridge_reth_runtime(&runtime, task_manager);
    Ok(runtime)
}

/// Tie `runtime` to `task_manager`. See the module docs.
fn bridge_reth_runtime(runtime: &Runtime, task_manager: &TaskManager) {
    let spawner = task_manager.get_spawner();

    // Panic bridge. reth's own manager task ends with an error when a critical reth task
    // panics. Returning that error from a critical task of ours stops the node.
    match runtime.take_task_manager_handle() {
        Some(reth_manager) => {
            spawner.spawn_critical_result_task(RETH_RUNTIME_MONITOR_TASK, async move {
                match reth_manager.await {
                    // reth's manager ended without a panic: its shutdown signal fired. Only
                    // the shutdown bridge below does that, and by then this task is already
                    // cancelled by the same shutdown. If the signal ever fires on its own,
                    // this critical task ends and the node stops, which is intended: the
                    // reth tasks are gone.
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(panicked)) => Err(format!("critical reth task panicked: {panicked}")),
                    Err(join_err) => Err(format!("reth task manager stopped: {join_err}")),
                }
            });
        }
        None => {
            warn!(
                target: "rayls::reth",
                "reth runtime has no task manager handle; reth task panics are not monitored"
            );
        }
    }

    // Shutdown bridge. `Drainable`, so it is not cancelled by the shutdown signal it waits
    // for. The signal fires in the ordered drain of the task manager and when the manager is
    // dropped; this task covers both.
    let shutdown = task_manager.shutdown_subscriber();
    let runtime = runtime.clone();
    spawner.spawn_classified_task(
        RETH_RUNTIME_SHUTDOWN_TASK,
        async move {
            shutdown.await;
            // reth waits in a busy loop, keep it off the async workers
            let finished = tokio::task::spawn_blocking(move || {
                runtime.graceful_shutdown_with_timeout(RETH_SHUTDOWN_TIMEOUT)
            })
            .await;
            match finished {
                Ok(true) => debug!(target: "rayls::reth", "reth runtime shut down"),
                Ok(false) => warn!(target: "rayls::reth", "reth runtime shutdown timed out"),
                Err(err) => warn!(target: "rayls::reth", %err, "reth runtime shutdown task failed"),
            }
        },
        TaskKind::Drainable,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use rayls_infrastructure_types::{Notifier, TaskJoinError};

    /// A panic in a critical reth task must stop the node: the task manager join returns the
    /// monitor task as the failed critical task.
    #[tokio::test(flavor = "multi_thread")]
    async fn critical_reth_task_panic_stops_the_task_manager() {
        let mut task_manager = TaskManager::new("reth runtime panic test");
        let runtime = build_reth_runtime(&task_manager).expect("runtime");
        task_manager.update_tasks();

        runtime.spawn_critical_task("boom", async {
            panic!("reth task failure");
        });

        let res = tokio::time::timeout(Duration::from_secs(10), task_manager.join(Notifier::new()))
            .await
            .expect("join must return after the reth task panic");
        match res {
            Err(TaskJoinError::CriticalExitError(name, _)) => {
                assert_eq!(name, RETH_RUNTIME_MONITOR_TASK)
            }
            other => panic!("expected the monitor task to fail the join, got {other:?}"),
        }
    }

    /// A panic in a normal (non-critical) reth task must not stop the node.
    #[tokio::test(flavor = "multi_thread")]
    async fn normal_reth_task_panic_is_ignored() {
        let mut task_manager = TaskManager::new("reth runtime normal panic test");
        let runtime = build_reth_runtime(&task_manager).expect("runtime");
        task_manager.update_tasks();

        runtime.spawn_task(async {
            panic!("not critical");
        });

        // 500ms is far more than the panic bridge needs: the critical-panic test above
        // returns in a few milliseconds.
        let res =
            tokio::time::timeout(Duration::from_millis(500), task_manager.join(Notifier::new()))
                .await;
        assert!(res.is_err(), "join must keep waiting: {res:?}");
    }

    /// Start a reth task that reports when it ends through reth's shutdown signal.
    fn signal_aware_task(runtime: &Runtime) -> tokio::sync::oneshot::Receiver<()> {
        let (stopped_tx, stopped_rx) = tokio::sync::oneshot::channel();
        runtime.spawn_critical_with_graceful_shutdown_signal("graceful", |shutdown| async move {
            let _guard = shutdown.await;
            let _ = stopped_tx.send(());
        });
        stopped_rx
    }

    /// Run plain blocking work on the runtime, the way block building does, and return its
    /// result. Fails the test if the work does not finish.
    async fn run_plain_blocking_work(runtime: &Runtime) -> u64 {
        let handle = runtime.spawn_blocking_named("test-plain-work", || 21u64 * 2);
        tokio::time::timeout(
            Duration::from_secs(5),
            tokio::task::spawn_blocking(move || *handle.get()),
        )
        .await
        .expect("plain blocking work must finish")
        .expect("plain blocking work must not panic")
    }

    /// The ordered shutdown of our task manager reaches reth tasks, and the join returns in
    /// time.
    #[tokio::test(flavor = "multi_thread")]
    async fn task_manager_shutdown_stops_reth_tasks() {
        let mut task_manager = TaskManager::new("reth runtime shutdown test");
        let runtime = build_reth_runtime(&task_manager).expect("runtime");
        task_manager.update_tasks();
        let stopped = signal_aware_task(&runtime);

        let shutdown = Notifier::new();
        shutdown.notify();
        tokio::time::timeout(Duration::from_secs(10), task_manager.join(shutdown))
            .await
            .expect("join must return")
            .expect("clean shutdown");

        tokio::time::timeout(Duration::from_secs(1), stopped)
            .await
            .expect("the reth task must see the shutdown signal")
            .expect("the reth task must end through its shutdown branch");
    }

    /// Dropping our task manager also reaches reth tasks. This is the path the node takes on
    /// SIGTERM, where the engine's manager is dropped and never joined in drain mode.
    #[tokio::test(flavor = "multi_thread")]
    async fn dropping_the_task_manager_stops_reth_tasks() {
        let task_manager = TaskManager::new("reth runtime drop test");
        let runtime = build_reth_runtime(&task_manager).expect("runtime");
        let stopped = signal_aware_task(&runtime);

        drop(task_manager);

        tokio::time::timeout(Duration::from_secs(5), stopped)
            .await
            .expect("the reth task must see the shutdown signal")
            .expect("the reth task must end through its shutdown branch");
    }

    /// The fact that makes the early signal safe: after reth's shutdown signal has fired, plain
    /// blocking work on the runtime still runs to its end. Block building and storage use only
    /// this kind of work, and on SIGTERM the engine drains its last blocks after the signal.
    #[tokio::test(flavor = "multi_thread")]
    async fn plain_blocking_work_still_runs_after_the_shutdown_signal() {
        let task_manager = TaskManager::new("reth runtime late work test");
        let runtime = build_reth_runtime(&task_manager).expect("runtime");
        let stopped = signal_aware_task(&runtime);

        drop(task_manager);
        tokio::time::timeout(Duration::from_secs(5), stopped)
            .await
            .expect("signal fired")
            .expect("signal fired");

        // the signal has fired: a signal-aware task spawned now ends without running
        let (ran_tx, ran_rx) = tokio::sync::oneshot::channel::<()>();
        runtime.spawn_task(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = ran_tx.send(());
        });
        assert!(
            tokio::time::timeout(Duration::from_secs(1), ran_rx).await.expect("resolved").is_err(),
            "a signal-aware task spawned after the signal must be cancelled"
        );

        // plain blocking work is not affected
        assert_eq!(run_plain_blocking_work(&runtime).await, 42);
    }

    /// The crash path: after a critical reth task panicked, plain blocking work still runs. The
    /// node then winds down in order and the engine drains its last blocks on this runtime.
    #[tokio::test(flavor = "multi_thread")]
    async fn plain_blocking_work_still_runs_after_a_critical_reth_panic() {
        let mut task_manager = TaskManager::new("reth runtime panic late work test");
        let runtime = build_reth_runtime(&task_manager).expect("runtime");
        task_manager.update_tasks();

        runtime.spawn_critical_task("boom", async {
            panic!("reth task failure");
        });
        let res = tokio::time::timeout(Duration::from_secs(10), task_manager.join(Notifier::new()))
            .await
            .expect("join must return after the reth task panic");
        assert!(res.is_err(), "the panic must fail the join");

        assert_eq!(run_plain_blocking_work(&runtime).await, 42);
    }
}
