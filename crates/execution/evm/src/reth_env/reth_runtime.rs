//! reth's task runtime, tied to our [`TaskManager`].
//!
//! reth 2.0 runs its own background tasks (transaction validation, pool sub-tasks, RPC) on a
//! [`reth_tasks::Runtime`]. Our [`TaskManager`] does not see those tasks, so two things are
//! bridged here:
//!
//! - a panic in a critical reth task stops the node, like a panic in one of our critical tasks
//! - our shutdown also shuts the reth runtime down, so reth tasks stop in order

use rayls_infrastructure_types::{TaskKind, TaskManager};
use reth_tasks::{Runtime, RuntimeBuilder, RuntimeConfig, TokioConfig};
use std::time::Duration;
use tracing::{debug, warn};

/// Name of the task that watches reth's critical tasks.
pub(crate) const RETH_RUNTIME_MONITOR_TASK: &str = "reth-runtime-monitor";
/// Name of the task that shuts the reth runtime down on our shutdown.
pub(crate) const RETH_RUNTIME_SHUTDOWN_TASK: &str = "reth-runtime-shutdown";

/// How long our shutdown waits for reth's graceful tasks.
///
/// Must stay below the task manager's phase stall bound (4s), or a slow reth task would make
/// the ordered teardown escalate to a forced abort.
const RETH_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

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
                    // reth asked for a shutdown itself
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

    // Shutdown bridge. `Drainable`, so it is not cancelled by the shutdown signal it waits for.
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
                Ok(false) => {
                    warn!(target: "rayls::reth", "reth runtime shutdown timed out")
                }
                Err(err) => {
                    warn!(target: "rayls::reth", %err, "reth runtime shutdown task failed")
                }
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

        let res =
            tokio::time::timeout(Duration::from_millis(500), task_manager.join(Notifier::new()))
                .await;
        assert!(res.is_err(), "join must keep waiting: {res:?}");
    }

    /// Our shutdown must reach reth tasks: a task that listens to reth's shutdown signal ends,
    /// and the join returns in time.
    #[tokio::test(flavor = "multi_thread")]
    async fn task_manager_shutdown_stops_reth_tasks() {
        let mut task_manager = TaskManager::new("reth runtime shutdown test");
        let runtime = build_reth_runtime(&task_manager).expect("runtime");
        task_manager.update_tasks();

        let (stopped_tx, stopped_rx) = tokio::sync::oneshot::channel();
        runtime.spawn_critical_with_graceful_shutdown_signal("graceful", |shutdown| async move {
            let _guard = shutdown.await;
            let _ = stopped_tx.send(());
        });

        let shutdown = Notifier::new();
        shutdown.notify();
        tokio::time::timeout(Duration::from_secs(10), task_manager.join(shutdown))
            .await
            .expect("join must return")
            .expect("clean shutdown");

        tokio::time::timeout(Duration::from_secs(1), stopped_rx)
            .await
            .expect("the reth task must see the shutdown signal")
            .expect("the reth task must end through its shutdown branch");
    }
}
