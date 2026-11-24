//! reth's task runtime, tied to our [`TaskManager`].
//!
//! reth 2.0 runs its own background tasks (transaction validation, pool sub-tasks, RPC) on a
//! [`reth_tasks::Runtime`]. Our [`TaskManager`] does not see those tasks, so two things are
//! done here:
//!
//! - a panic in a critical reth task stops the node, like a panic in one of our critical tasks
//!   ([`build_reth_runtime`])
//! - the node shutdown shuts the reth runtime down as its last step, so reth tasks stop in order
//!   ([`shutdown_reth_runtime`])

use rayls_infrastructure_types::TaskManager;
use reth_tasks::{Runtime, RuntimeBuilder, RuntimeConfig, TokioConfig};
use std::time::Duration;
use tracing::{debug, warn};

/// Name of the task that watches reth's critical tasks.
pub(crate) const RETH_RUNTIME_MONITOR_TASK: &str = "reth-runtime-monitor";
/// How long the node shutdown waits for reth's graceful tasks. It runs after the final flush,
/// so a task that does not end in time loses nothing.
const RETH_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

/// Build the reth [`Runtime`] on the current tokio runtime and watch its critical tasks from
/// `task_manager`.
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

/// Make a panic in a critical reth task stop the node.
fn bridge_reth_runtime(runtime: &Runtime, task_manager: &TaskManager) {
    let spawner = task_manager.get_spawner();

    // Panic bridge. reth's own manager task ends with an error when a critical reth task
    // panics. Returning that error from a critical task of ours stops the node.
    match runtime.take_task_manager_handle() {
        Some(reth_manager) => {
            spawner.spawn_critical_result_task(RETH_RUNTIME_MONITOR_TASK, async move {
                match reth_manager.await {
                    // reth's manager ended without a panic: its shutdown signal fired. In
                    // normal operation only `shutdown_reth_runtime` does that, at the very
                    // end. If it happens earlier, this critical task ends and the node stops,
                    // which is intended: the reth tasks are gone.
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
}

/// Fire reth's shutdown signal and wait, up to a limit, for reth's graceful tasks to end.
///
/// This is called explicitly, as the last step of the node shutdown, after the engine has
/// drained and the final flush is done. It is NOT tied to the shutdown signal or the drop of
/// a [`TaskManager`]: the engine's manager is dropped early in the node shutdown, while the
/// engine still executes its last blocks, and the state root work of those blocks runs on
/// this runtime. reth ends a task at once when it is spawned after the signal.
pub(crate) async fn shutdown_reth_runtime(runtime: &Runtime) {
    let runtime = runtime.clone();
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

    /// The explicit shutdown reaches reth tasks: a task that listens to reth's shutdown signal
    /// ends, and the call returns in time.
    #[tokio::test(flavor = "multi_thread")]
    async fn explicit_shutdown_stops_reth_tasks() {
        let task_manager = TaskManager::new("reth runtime shutdown test");
        let runtime = build_reth_runtime(&task_manager).expect("runtime");

        let (stopped_tx, stopped_rx) = tokio::sync::oneshot::channel();
        runtime.spawn_critical_with_graceful_shutdown_signal("graceful", |shutdown| async move {
            let _guard = shutdown.await;
            let _ = stopped_tx.send(());
        });

        tokio::time::timeout(Duration::from_secs(10), shutdown_reth_runtime(&runtime))
            .await
            .expect("shutdown must return");

        tokio::time::timeout(Duration::from_secs(1), stopped_rx)
            .await
            .expect("the reth task must see the shutdown signal")
            .expect("the reth task must end through its shutdown branch");
    }

    /// Dropping the task manager must NOT shut the reth runtime down. On the node path the
    /// engine's manager is dropped while the engine still drains its last blocks, and their
    /// state root work runs on this runtime.
    #[tokio::test(flavor = "multi_thread")]
    async fn dropping_the_task_manager_keeps_the_reth_runtime_running() {
        let task_manager = TaskManager::new("reth runtime drop test");
        let runtime = build_reth_runtime(&task_manager).expect("runtime");
        drop(task_manager);
        tokio::time::sleep(Duration::from_millis(200)).await;

        // a task spawned now still runs to its end
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        runtime.spawn_task(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = done_tx.send(());
        });
        tokio::time::timeout(Duration::from_secs(2), done_rx)
            .await
            .expect("task must finish")
            .expect("task must not be cancelled by a shutdown signal");
    }
}
