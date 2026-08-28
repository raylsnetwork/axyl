//! Task manager interface to spawn tasks to the tokio runtime.

use crate::Notifier;
use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    future::Future,
    pin::pin,
    task::Poll,
    time::Duration,
};
use thiserror::Error;
use tokio::{
    sync::mpsc,
    task::{JoinError, JoinHandle},
};

/// No-progress watchdog for the ordered drain phases (producers, then consumers): if a phase
/// reaps nothing for this long, escalate (proceed to consumers / hard-abort / abandon). It's a
/// stall backstop for a task that ignores `abort()`, NOT the expected reap time — so it's
/// generous on purpose. Escalating weakens the producer→consumer ordering, so we'd rather wait
/// out a slow-but-cancelling task (e.g. a transient network request finishing its drop) than
/// trip on it. Deliberately decoupled from `join_wait_millis` (the post-loop straggler grace).
///
/// Sized against the tightest outer `controlled_shutdown` timeout (15s, mode transition): the
/// three escalation steps (producer reap → consumer drain → force-abort) must fit inside it, so
/// `3 * PHASE_STALL_BOUND <= 15s` (4s → 12s, leaving margin).
const PHASE_STALL_BOUND: Duration = Duration::from_secs(4);

/// Classification for how a task should be handled during epoch transitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskKind {
    /// Task can be safely drained with a timeout (e.g., subscriber, batch builder).
    Drainable,
    /// Task will fail after epoch transition, abort immediately (e.g., certifier, proposer).
    Doomed,
    /// Task has no drainable state, can be cancelled (e.g., network events).
    Cancel,
}

/// Used for the futures that will resolve when tasks do.
/// Allows us to hold a FuturesUnordered directly in the TaskManager struct.
struct TaskHandle {
    /// The  owned permission to join on a task (await its termination).
    handle: JoinHandle<()>,
    /// The name for the task.
    info: TaskInfo,
}

impl TaskHandle {
    /// Create a new instance of `Self`.
    fn new(name: String, handle: JoinHandle<()>, critical: bool) -> Self {
        Self { handle, info: TaskInfo { name, critical, kind: TaskKind::Doomed } }
    }

    /// Create a new instance with explicit task kind.
    fn with_kind(name: String, handle: JoinHandle<()>, critical: bool, kind: TaskKind) -> Self {
        Self { handle, info: TaskInfo { name, critical, kind } }
    }
}

/// The information for task results.
#[derive(Clone, Debug)]
struct TaskInfo {
    /// The name of the task.
    name: String,
    /// Bool indicating if the task is critical. Critical tasks cause the loop to break and force
    /// shutdown.
    critical: bool,
    /// Classification for epoch transition handling.
    kind: TaskKind,
}

impl Future for TaskHandle {
    // Return the `name` and `critical` status for task.
    type Output = Result<TaskInfo, (TaskInfo, JoinError)>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        match this.handle.poll_unpin(cx) {
            Poll::Ready(res) => match res {
                Ok(_) => Poll::Ready(Ok(this.info.clone())),
                Err(err) => Poll::Ready(Err((this.info.clone(), err))),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Spawn a shutdown-cancellable task, return `TaskHandle` + `AbortHandle`.
fn spawn_abortable_handle<F>(
    name: String,
    rx_shutdown: crate::Noticer,
    future: F,
) -> (TaskHandle, tokio::task::AbortHandle)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let handle = tokio::spawn(async move {
        tokio::select! {
            _ = rx_shutdown => {}
            _ = future => {}
        }
    });
    let abort = handle.abort_handle();
    (TaskHandle::with_kind(name, handle, false, TaskKind::Doomed), abort)
}

/// Wrap a fallible task so Err surfaces as a `CriticalExitError` via panic payload.
///
/// The err is logged structurally before the panic, then panicked with a `String`
/// payload so callers can `JoinError::try_into_panic().downcast::<String>()` for
/// structured extraction instead of parsing the Display form.
async fn critical_result_wrapper<F, T, E>(name: String, rx_shutdown: crate::Noticer, future: F)
where
    F: Future<Output = Result<T, E>>,
    E: std::fmt::Debug,
{
    tokio::select! {
        _ = rx_shutdown => {}
        _ = result_task_wrapper(name, future) => {}
    }
}

/// Await a fallible task and surface `Err` as a `CriticalExitError` via panic
/// payload — same as [`critical_result_wrapper`] but WITHOUT the cancelling
/// shutdown `select!`. For [`TaskKind::Drainable`] critical tasks that observe
/// shutdown internally and must run to their own completion (e.g. the execution
/// engine, which drains queued + in-flight outputs), so they are never dropped
/// mid-execution — dropping the engine future orphans its detached execution
/// task, which then finalizes blocks after the shutdown flush.
async fn result_task_wrapper<F, T, E>(name: String, future: F)
where
    F: Future<Output = Result<T, E>>,
    E: std::fmt::Debug,
{
    if let Err(err) = future.await {
        let payload = format!("critical task {name} returned Err: {err:?}");
        tracing::error!(target: "rayls::tasks", task = %name, ?err, "critical task returned Err");
        std::panic::panic_any(payload);
    }
}

/// A basic task manager.
///
/// Allows new tasks to be be started on the tokio runtime and tracks
/// there JoinHandles.
pub struct TaskManager {
    tasks: FuturesUnordered<TaskHandle>,
    submanagers: HashMap<String, TaskManager>,
    name: String,
    new_task_rx: mpsc::Receiver<TaskHandle>,
    new_task_tx: mpsc::Sender<TaskHandle>,
    /// This is used to notify any spawned tasks to exit when task manager is dropped.
    /// Otherwise we will end up with orphaned tasks when epochs change.
    local_shutdown: Notifier,
    /// Grace period for the post-loop cleanup drain (tasks, then sub-managers) — used twice,
    /// so the post-join wait is up to `2 * join_wait_millis`. NOTE: this is NOT the in-drain
    /// per-phase stall bound; that's [`Self::phase_stall_bound`], decoupled from this.
    join_wait_millis: u64,
    /// In-drain per-phase no-progress watchdog (see [`PHASE_STALL_BOUND`]). A field, not the
    /// const directly, so tests can shorten it; production uses the default.
    phase_stall_bound: Duration,
}

impl Drop for TaskManager {
    fn drop(&mut self) {
        self.local_shutdown.notify();
    }
}

/// The type that can spawn tasks for a parent `TaskManager`.
///
/// The TaskSpawner is clone-able and forwards tasks to the task manager
/// to track. This type lives with other types to spawn short-lived tasks.
#[derive(Clone, Debug)]
pub struct TaskSpawner {
    /// The channel to forward task handles to the parent [TaskManager].
    new_task_tx: mpsc::Sender<TaskHandle>,
    local_shutdown: Notifier,
}

impl TaskSpawner {
    /// Spawns a non-critical task on tokio and records it's JoinHandle and name. Other tasks are
    /// unaffected when this task resolves.
    pub fn spawn_task<F, S: ToString>(&self, name: S, future: F)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.create_task(name, future, false, TaskKind::Doomed);
    }

    /// Spawns a critical task on tokio and records it's JoinHandle and name.
    ///
    /// The task is tracked as "critical". When the task resolves, other tasks
    /// will shutdown.
    pub fn spawn_critical_task<F, S: ToString>(&self, name: S, future: F)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.create_task(name, future, true, TaskKind::Doomed);
    }

    /// Spawn a critical task that panics on Err (surfaces as `CriticalExitError`).
    pub fn spawn_critical_result_task<F, T, E, S: ToString>(&self, name: S, future: F)
    where
        F: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: std::fmt::Debug + Send + 'static,
    {
        let name = name.to_string();
        let handle = tokio::spawn(critical_result_wrapper(
            name.clone(),
            self.local_shutdown.subscribe(),
            future,
        ));
        if let Err(err) = self.new_task_tx.try_send(TaskHandle::with_kind(
            name.clone(),
            handle,
            true,
            TaskKind::Doomed,
        )) {
            tracing::error!(target: "rayls::tasks", "Task error sending joiner for {name}: {err}");
        }
    }

    /// Spawn a critical, [`TaskKind::Drainable`] task that panics on Err.
    ///
    /// Unlike [`Self::spawn_critical_result_task`], the future is NOT wrapped in a
    /// cancelling `select!` on `local_shutdown`: a Drainable task observes shutdown
    /// itself and drains in-flight work, so it must run to its own completion.
    /// `abort_doomed_tasks()` does not touch it (it only hard-aborts producers); the drain
    /// signal comes from `join_internal` (after producers are reaped) or `Drop`. Only
    /// `abort_all_tasks()` hard-kills it. Err still surfaces as a `CriticalExitError`.
    pub fn spawn_drainable_result_task<F, T, E, S: ToString>(&self, name: S, future: F)
    where
        F: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: std::fmt::Debug + Send + 'static,
    {
        let name = name.to_string();
        let handle = tokio::spawn(result_task_wrapper(name.clone(), future));
        if let Err(err) = self.new_task_tx.try_send(TaskHandle::with_kind(
            name.clone(),
            handle,
            true,
            TaskKind::Drainable,
        )) {
            tracing::error!(target: "rayls::tasks", "Task error sending joiner for {name}: {err}");
        }
    }

    /// Spawn a critical task with explicit [TaskKind] classification.
    pub fn spawn_classified_task<F, S: ToString>(&self, name: S, future: F, kind: TaskKind)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.create_task(name, future, true, kind);
    }

    /// Spawn a non-critical task, return `AbortHandle` for cancellation.
    pub fn spawn_abortable_task<F, S: ToString>(
        &self,
        name: S,
        future: F,
    ) -> tokio::task::AbortHandle
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let name = name.to_string();
        let (task, abort) =
            spawn_abortable_handle(name.clone(), self.local_shutdown.subscribe(), future);
        if let Err(err) = self.new_task_tx.try_send(task) {
            tracing::error!(target: "rayls::tasks", "Task error sending joiner for {name}: {err}");
        }
        abort
    }

    /// The main function to spawn tasks on the `tokio` runtime. These tasks are tracked by the
    /// manager.
    fn create_task<F, S: ToString>(&self, name: S, future: F, critical: bool, kind: TaskKind)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let name = name.to_string();
        // Drainable tasks handle shutdown internally — do NOT wrap them in a
        // cancelling select! on local_shutdown, otherwise the outer select would
        // kill them the moment local_shutdown is notified (which join_internal does,
        // after producers are reaped) instead of letting them drain.
        let handle = if kind == TaskKind::Drainable {
            tokio::spawn(async move {
                future.await;
            })
        } else {
            let rx_shutdown = self.local_shutdown.subscribe();
            tokio::spawn(async move {
                tokio::select! {
                    _ = rx_shutdown => {}
                    _ = future => {}
                }
            })
        };
        if let Err(err) =
            self.new_task_tx.try_send(TaskHandle::with_kind(name.clone(), handle, critical, kind))
        {
            tracing::error!(target: "rayls::tasks", "Task error sending joiner for {name}: {err}");
        }
    }

    /// Spawns a non-critical, blocking task on tokio and records the JoinHandle and name.
    ///
    /// Other tasks are unaffected when this task resolves.
    /// Note: this spawns a thread on the tokio blocking thread pool.
    /// The closure checks `local_shutdown` before starting — if the task
    /// manager has already been shut down the closure is a no-op.
    pub fn spawn_blocking_task<F, S: ToString>(&self, name: S, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let name = name.to_string();
        let shutdown = self.local_shutdown.clone();
        let handle = tokio::task::spawn_blocking(move || {
            if shutdown.was_notified() {
                return;
            }
            task();
        });
        if let Err(err) = self.new_task_tx.try_send(TaskHandle::new(name.clone(), handle, false)) {
            tracing::error!(target: "rayls::tasks", "Task error sending joiner for {name}: {err}");
        }
    }

    /// Spawn a task for the reth [`TaskSpawner`](reth_tasks::TaskSpawner) trait.
    ///
    /// The reth interface requires a [`JoinHandle`] to be returned while the
    /// [`TaskManager`] needs its own handle for tracking.  We use a single
    /// `tokio::spawn` (or `spawn_blocking`) wrapped in one `select!` with
    /// `local_shutdown`, and bridge a lightweight proxy [`JoinHandle`] back to
    /// the caller via a [`oneshot`](tokio::sync::oneshot) channel.
    fn spawn_reth_task(
        &self,
        name: &str,
        fut: BoxFuture<'static, ()>,
        critical: bool,
        blocking: bool,
    ) -> JoinHandle<()> {
        let rx_shutdown = self.local_shutdown.subscribe();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();

        let f = async move {
            tokio::select! {
                _ = rx_shutdown => {}
                _ = fut => {}
            }
            let _ = done_tx.send(());
        };

        // Single spawn — tracked by TaskManager.
        let tracked_handle = if blocking {
            let handle = tokio::runtime::Handle::current();
            tokio::task::spawn_blocking(move || handle.block_on(f))
        } else {
            tokio::spawn(f)
        };

        if let Err(err) = self.new_task_tx.try_send(TaskHandle::with_kind(
            name.to_string(),
            tracked_handle,
            critical,
            TaskKind::Doomed,
        )) {
            tracing::error!(target: "rayls::tasks", "Task error sending joiner for {name}: {err}");
        }

        // Lightweight proxy handle for the reth caller.  Completes when the
        // tracked task finishes (or is aborted, which drops done_tx).
        tokio::spawn(async move {
            let _ = done_rx.await;
        })
    }
}

impl Default for TaskManager {
    fn default() -> Self {
        Self::new("Default (test) Task Manager")
    }
}

impl TaskManager {
    /// Create a new empty TaskManager.
    pub fn new<S: ToString>(name: S) -> Self {
        let (new_task_tx, new_task_rx) = mpsc::channel(4096);
        Self {
            tasks: FuturesUnordered::new(),
            submanagers: HashMap::new(),
            name: name.to_string(),
            new_task_rx,
            new_task_tx,
            local_shutdown: Notifier::default(),
            join_wait_millis: 2000,
            phase_stall_bound: PHASE_STALL_BOUND,
        }
    }

    /// Sets the post-loop cleanup grace this manager waits on tasks/sub-managers to complete.
    /// Used twice in join (once for tasks, once for sub-managers), so the post-join wait is up
    /// to ~2x this value. Does NOT affect the in-drain per-phase stall bound
    /// ([`PHASE_STALL_BOUND`]).
    pub fn set_join_wait(&mut self, millis: u64) {
        self.join_wait_millis = millis;
    }

    /// Override the per-phase stall bound. Test-only - production uses [`PHASE_STALL_BOUND`].
    #[cfg(any(test, feature = "test-utils"))]
    pub fn set_phase_stall_bound(&mut self, bound: Duration) {
        self.phase_stall_bound = bound;
    }

    /// Spawns a non-critical task on tokio and records it's JoinHandle and name. Other tasks are
    /// unaffected when this task resolves.
    pub fn spawn_task<F, S: ToString>(&self, name: S, future: F)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.create_task(name, future, false, TaskKind::Doomed);
    }

    /// Spawns a critical task on tokio and records it's JoinHandle and name.
    ///
    /// The task is tracked as "critical". When the task resolves, other tasks
    /// will shutdown.
    pub fn spawn_critical_task<F, S: ToString>(&self, name: S, future: F)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.create_task(name, future, true, TaskKind::Doomed);
    }

    /// Spawn a critical task that panics on Err (surfaces as `CriticalExitError`).
    pub fn spawn_critical_result_task<F, T, E, S: ToString>(&self, name: S, future: F)
    where
        F: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: std::fmt::Debug + Send + 'static,
    {
        let name = name.to_string();
        let handle = tokio::spawn(critical_result_wrapper(
            name.clone(),
            self.local_shutdown.subscribe(),
            future,
        ));
        self.tasks.push(TaskHandle::with_kind(name, handle, true, TaskKind::Doomed));
    }

    /// Spawn a critical task with explicit [TaskKind] classification.
    pub fn spawn_classified_task<F, S: ToString>(&self, name: S, future: F, kind: TaskKind)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.create_task(name, future, true, kind);
    }

    /// Spawn a non-critical task, return `AbortHandle` for cancellation.
    pub fn spawn_abortable_task<F, S: ToString>(
        &mut self,
        name: S,
        future: F,
    ) -> tokio::task::AbortHandle
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (task, abort) =
            spawn_abortable_handle(name.to_string(), self.local_shutdown.subscribe(), future);
        self.tasks.push(task);
        abort
    }

    /// The main function to spawn tasks on the `tokio` runtime. These tasks are tracked by the
    /// manager.
    fn create_task<F, S: ToString>(&self, name: S, future: F, critical: bool, kind: TaskKind)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let name = name.to_string();
        // Drainable tasks handle shutdown internally — do NOT wrap them in a
        // cancelling select! on local_shutdown, otherwise the outer select would
        // kill them the moment local_shutdown is notified (which join_internal does,
        // after producers are reaped) instead of letting them drain.
        let handle = if kind == TaskKind::Drainable {
            tokio::spawn(async move {
                future.await;
            })
        } else {
            let rx_shutdown = self.local_shutdown.subscribe();
            tokio::spawn(async move {
                tokio::select! {
                    _ = rx_shutdown => {}
                    _ = future => {}
                }
            })
        };
        self.tasks.push(TaskHandle::with_kind(name, handle, critical, kind));
    }

    /// Subscribe to the local shutdown signal.
    ///
    /// Returns a [`Noticer`] that resolves when `local_shutdown.notify()` fires — which
    /// happens in `join_internal` once every producer has been reaped (the consumer phase of
    /// the ordered teardown), or from [`Drop`]. Note `abort_doomed_tasks` no longer fires it.
    /// Drainable tasks that observe shutdown internally should call this before their main loop.
    pub fn shutdown_subscriber(&self) -> crate::Noticer {
        self.local_shutdown.subscribe()
    }

    /// Return a clonable spawner (also implements Reth's TaskSpawner trait).
    pub fn get_spawner(&self) -> TaskSpawner {
        TaskSpawner {
            new_task_tx: self.new_task_tx.clone(),
            local_shutdown: self.local_shutdown.clone(),
        }
    }

    /// Return a mutable reference to a submanager.
    pub fn get_submanager(&mut self, name: &str) -> Option<&mut TaskManager> {
        self.submanagers.get_mut(name)
    }

    /// Adds a subtask manager by name to this TaskManager.  Allows building a heirarchy of tasks.
    pub fn add_task_manager(&mut self, manager: TaskManager) {
        self.submanagers.insert(manager.name.clone(), manager);
    }

    /// Will resolve once one of the tasks for the manager resolves.
    ///
    /// The manager tracks critical and non-critical tasks. Critical tasks
    /// that stop force the process to shutdown.
    pub async fn join(&mut self, shutdown: Notifier) -> Result<(), TaskJoinError> {
        self.join_internal(shutdown).await
    }

    /// Abort all of our direct tasks (not sub task managers though).
    pub fn abort(&self) {
        for task in self.tasks.iter() {
            tracing::debug!(target: "rayls::tasks", task_name=?task.info.name, "aborting task");
            task.handle.abort();
        }
    }

    /// Abort all tasks including submanagers.
    ///
    /// This is used to close epoch-related tasks.
    pub fn abort_all_tasks(&mut self) {
        self.abort();

        // abort submanager tasks as well
        for manager in self.submanagers.values_mut() {
            manager.abort_all_tasks();
        }
    }

    /// Hard-abort [TaskKind::Doomed] and [TaskKind::Cancel] (producer) tasks immediately.
    ///
    /// Drainable (consumer) tasks are deliberately NOT signalled here — `join_internal` fires
    /// `local_shutdown` for them only AFTER every producer has been reaped, so the ordered
    /// producer→consumer teardown holds. This is an early sever (an optimization); `join_internal`
    /// repeats these aborts idempotently, so callers MUST follow with `join`
    /// for the manager to actually drain. (Drainable tasks aren't wrapped in the abort `select!`;
    /// they observe shutdown via their own `local_shutdown` subscription.)
    pub fn abort_doomed_tasks(&self) {
        for task in self.tasks.iter() {
            match task.info.kind {
                TaskKind::Doomed | TaskKind::Cancel => {
                    tracing::info!(
                        target: "rayls::tasks",
                        task_name=?task.info.name,
                        kind=?task.info.kind,
                        "aborting doomed/cancel task"
                    );
                    task.handle.abort();
                }
                TaskKind::Drainable => {
                    tracing::info!(
                        target: "rayls::tasks",
                        task_name=?task.info.name,
                        "deferring drainable task; will be signalled after producers are reaped"
                    );
                }
            }
        }

        // NOTE: do NOT notify local_shutdown here. Drainable (consumer) tasks are
        // signalled by `join_internal` only AFTER every Doomed/Cancel (producer) task
        // has been reaped, so a producer's held resources (e.g. QueChanReceiver) are
        // released before consumers begin exiting — a deterministic, ordered teardown.
        // The aborts above are just an early sever; `join_internal` repeats them
        // idempotently, so this method is an optimization, not a correctness requirement.

        for manager in self.submanagers.values() {
            manager.abort_doomed_tasks();
        }
    }

    /// Take any tasks on the new task queue and put them in the task list.
    ///
    /// Use this using the Reth spawner interface to update the task list.
    /// For instance to correctly print all the tasks with Display before
    /// calling join.
    pub fn update_tasks(&mut self) {
        while let Ok(task) = self.new_task_rx.try_recv() {
            self.tasks.push(task);
        }
    }

    /// Implements the join logic for the manager.
    ///
    /// Operates in two modes determined by the `shutdown` Notifier state:
    ///
    /// **Normal mode** (`shutdown` not yet notified): waits for events.
    /// Breaks on the first unexpected critical task exit or the `shutdown`
    /// Notifier firing.
    ///
    /// **Drain mode** (`shutdown` already notified when called — the epoch
    /// transition path): processes *every* task result, logging each one
    /// without breaking early, until `self.tasks` and `future_managers` are
    /// both empty.  The `rx_shutdown` branch is disabled so it cannot
    /// short-circuit the drain.
    ///
    /// Termination signals (SIGTERM / ctrl-c) are deliberately NOT caught here:
    /// the node catches them itself via [`Self::exit`] and drives an ordered
    /// shutdown, so the drain loop stays signal-deaf and uninterruptible.
    async fn join_internal(&mut self, shutdown: Notifier) -> Result<(), TaskJoinError> {
        // Snapshot: if shutdown was already notified we are in drain mode.
        // This is stable — once notified, the flag never clears.
        let drain_mode = shutdown.was_notified();

        let shutdown_ref = &shutdown;
        let sub_wait_millis = (self.join_wait_millis / 4) * 3;
        let mut future_managers: FuturesUnordered<_> = self
            .submanagers
            .drain()
            .map(|(name, mut sub)| async move {
                sub.set_join_wait(sub_wait_millis);
                (sub.join(shutdown_ref.clone()).await, name)
            })
            .collect();
        let rx_shutdown = shutdown.subscribe();
        let mut result = Ok(());
        // Ordered teardown: in drain mode we tear tasks down producer-first, consumer-
        // second. Producers are Doomed/Cancel (hard-abortable sources); consumers are
        // Drainable (queue drains). We hard-abort every producer and reap them, and only
        // THEN signal local_shutdown so Drainable consumers begin their graceful drain.
        // This guarantees a producer's held resources (e.g. a QueChanReceiver) are
        // released before consumers start exiting, so the next epoch can re-subscribe
        // against a clean slate. `consumers_signalled` latches once we cross that boundary.
        let mut consumers_signalled = false;
        // Per-phase stall bound: if a phase makes no progress (no task reaped) within
        // `phase_wait`, escalate — stuck producers → proceed to consumers; stuck consumers
        // → hard-abort them; still stuck → abandon to post-join cleanup. It's a no-progress
        // timeout (reset on each reap), not an absolute one, so legit slow drains aren't cut.
        let phase_wait = self.phase_stall_bound;
        let mut consumers_forced = false;
        let mut phase_deadline = tokio::time::Instant::now() + phase_wait;
        loop {
            // Pull any tasks buffered in the spawner channel into the tracked
            // set so the empty-check below has a complete picture.
            while let Ok(task) = self.new_task_rx.try_recv() {
                self.tasks.push(task);
            }

            // Phase boundary. Until consumers are signalled, keep severing producers
            // (idempotent — abort on a finished/aborted task is a no-op) and hold the
            // drainable signal back. Once no producer remains tracked, every producer
            // future has been dropped, so it's safe to release the consumers.
            if drain_mode && !consumers_signalled {
                let mut producer_pending = false;
                for task in self.tasks.iter() {
                    if task.info.kind != TaskKind::Drainable {
                        task.handle.abort();
                        producer_pending = true;
                    }
                }
                if !producer_pending {
                    tracing::info!(
                        target: "rayls::tasks",
                        "{}: producers reaped, signalling drainable consumers",
                        self.name
                    );
                    self.local_shutdown.notify();
                    consumers_signalled = true;
                    phase_deadline = tokio::time::Instant::now() + phase_wait;
                }
            }

            // In drain mode the exit condition is deterministic: every tracked
            // task AND every sub-manager has completed.
            if drain_mode && self.tasks.is_empty() && future_managers.is_empty() {
                tracing::info!(target: "rayls::tasks", "{}: all tasks drained", self.name);
                break;
            }

            tokio::select! {
                // In drain mode rx_shutdown is already resolved (Notifier fix)
                // but we do not want to break — we want to drain tasks first.
                _ = &rx_shutdown, if !drain_mode => {
                    tracing::info!(target: "rayls::tasks", "{}: Node exiting, received shutdown notification", self.name);
                    break;
                },
                // Per-phase stall bound (drain mode only): escalate when the current phase
                // makes no progress within `phase_wait`.
                _ = tokio::time::sleep_until(phase_deadline), if drain_mode => {
                    if !consumers_signalled {
                        // INVARIANT RELAXED HERE: a producer ignored `abort()` for the whole
                        // bound, so we proceed and signal consumers while it's still tracked
                        // (future possibly alive). Producer→consumer ordering is best-effort
                        // on this path. Name the offender(s) — if one holds a resource the next
                        // epoch re-acquires (e.g. a QueChanReceiver), this is where it can race.
                        let pending: Vec<&str> = self
                            .tasks
                            .iter()
                            .filter(|t| t.info.kind != TaskKind::Drainable)
                            .map(|t| t.info.name.as_str())
                            .collect();
                        tracing::warn!(
                            target: "rayls::tasks",
                            ?pending,
                            "{}: producers did not reap within {phase_wait:?}; proceeding to signal consumers",
                            self.name
                        );
                        self.local_shutdown.notify();
                        consumers_signalled = true;
                    } else if !consumers_forced {
                        let pending: Vec<&str> =
                            self.tasks.iter().map(|t| t.info.name.as_str()).collect();
                        tracing::warn!(
                            target: "rayls::tasks",
                            ?pending,
                            "{}: consumers did not drain within {phase_wait:?}; hard-aborting remaining tasks",
                            self.name
                        );
                        for task in self.tasks.iter() {
                            task.handle.abort();
                        }
                        consumers_forced = true;
                    } else {
                        tracing::error!(
                            target: "rayls::tasks",
                            "{}: tasks did not reap after forced abort; abandoning to post-join cleanup",
                            self.name
                        );
                        break;
                    }
                    phase_deadline = tokio::time::Instant::now() + phase_wait;
                },
                Some(task) = self.new_task_rx.recv() => {
                    self.tasks.push(task);
                    continue;
                },
                Some(res) = self.tasks.next() => {
                    // Progress: a task was reaped, so extend the phase stall bound.
                    phase_deadline = tokio::time::Instant::now() + phase_wait;
                    match res {
                        Ok(info) => {
                            if !info.critical {
                                continue;
                            }
                            if info.kind == TaskKind::Drainable {
                                tracing::info!(target: "rayls::tasks", "{}: drainable task {} returned Ok", self.name, info.name);
                                continue;
                            }
                            if drain_mode {
                                tracing::info!(target: "rayls::tasks", "{}: {} returned Ok during shutdown", self.name, info.name);
                                continue;
                            }
                            tracing::info!(target: "rayls::tasks", "{}: {} returned Ok, node exiting", self.name, info.name);
                            result = Err(TaskJoinError::CriticalExitOk(info.name));
                        }
                        Err((info, join_err)) => {
                            if !info.critical {
                                continue;
                            }
                            if join_err.is_cancelled() && info.kind != TaskKind::Drainable {
                                tracing::info!(
                                    target: "rayls::tasks",
                                    "{}: {} ({:?}) was cancelled as expected",
                                    self.name, info.name, info.kind,
                                );
                                continue;
                            }
                            if drain_mode {
                                tracing::warn!(
                                    target: "rayls::tasks",
                                    "{}: {} returned error during shutdown: {join_err}",
                                    self.name, info.name,
                                );
                                continue;
                            }
                            tracing::error!(target: "rayls::tasks", "{}: {} returned error {join_err}, node exiting", self.name, info.name);
                            result = Err(TaskJoinError::CriticalExitError(info.name, join_err));
                        }
                    }
                    // Normal mode: break on first unexpected critical exit.
                    break;
                }
                Some((res, name)) = future_managers.next() => {
                    if drain_mode {
                        tracing::info!(target: "rayls::tasks", "{}: sub-manager {name} completed during shutdown", self.name);
                        continue;
                    }
                    tracing::error!(target: "rayls::tasks", "{}: Sub-Task Manager {name} returned exited, node exiting", self.name);
                    result = res;
                    break;
                }
            }
        }
        // No matter how we exit notify shutdown and allow a chance for other tasks to exit
        // cleanly.
        shutdown.notify();
        let task_name = self.name.clone();
        let join_wait = Duration::from_millis(self.join_wait_millis);
        // wait some time for shutdown...
        // 2 seconds for our tasks to end...
        if tokio::time::timeout(join_wait, async move {
            tracing::debug!(target: "rayls::tasks", "awaiting shutdown for task manager\n{self:?}");
            while let Some(res) = self.tasks.next().await {
                match res {
                    Ok(info) => {
                        tracing::info!(
                            target: "rayls::tasks",
                            "{}: {} shutdown successfully",
                            self.name,
                            info.name,
                        )
                    }
                    Err((info, err)) if err.is_cancelled() => tracing::info!(
                        target: "rayls::tasks",
                        "{}: {} was cancelled",
                        self.name,
                        info.name,
                    ),
                    Err((info, err)) => tracing::error!(
                        target: "rayls::tasks",
                        "{}: {} shutdown with error {err}",
                        self.name,
                        info.name,
                    ),
                }
            }
            tracing::info!(target: "rayls::tasks", "{}: All tasks shutdown", self.name);
        })
        .await
        .is_err()
        {
            tracing::error!(target:"rayls::tasks", "{}: All tasks NOT shutdown", task_name);
        }

        // Another 2 seconds for any of our sub tasks to end...
        let task_name_clone = task_name.clone();
        if tokio::time::timeout(join_wait, async move {
            while let Some((_, name)) = future_managers.next().await {
                tracing::info!(
                    target: "rayls::tasks",
                    "{}: TaskManager {name} shutdown successfully",
                    task_name_clone
                )
            }
            tracing::info!(
                target: "rayls::tasks",
                "{}: All tasks managers shutdown",
                task_name_clone
            );
        })
        .await
        .is_err()
        {
            tracing::error!(target: "rayls::tasks", "{}: All tasks managers NOT shutdown", task_name);
        }
        result
    }

    /// Will resolve when ctrl-c is pressed or a SIGTERM is received.
    ///
    /// Exposed so the node can catch the termination signal itself and drive a graceful,
    /// ordered shutdown rather than letting a task-manager join catch it.
    pub async fn exit() {
        #[cfg(unix)]
        {
            let mut stream =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("could not config sigterm");
            let sigterm = stream.recv();
            let sigterm = pin!(sigterm);
            let ctrl_c = pin!(tokio::signal::ctrl_c());

            tokio::select! {
                _ = ctrl_c => {
                    tracing::info!(target: "rayls::tasks", "Received ctrl-c");
                },
                _ = sigterm => {
                    tracing::info!(target: "rayls::tasks", "Received SIGTERM");
                },
            }
        }

        #[cfg(not(unix))]
        {
            let _ = ctrl_c().await;
        }
    }
}

impl Display for TaskManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{}", self.name)?;
        for task in self.tasks.iter() {
            let critical = if task.info.critical { "critical" } else { "not critical" };
            writeln!(f, "Task: {} ({critical}, {:?})", task.info.name, task.info.kind)?;
        }
        for sub in self.submanagers.values() {
            writeln!(f, "++++++++++++++++++++++++++++++++++++++++++++++++++++")?;
            writeln!(f, "{sub}")?;
            writeln!(f, "++++++++++++++++++++++++++++++++++++++++++++++++++++")?;
        }
        Ok(())
    }
}

impl Debug for TaskManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

impl reth_tasks::TaskSpawner for TaskSpawner {
    fn spawn_task(&self, fut: BoxFuture<'static, ()>) -> JoinHandle<()> {
        self.spawn_reth_task("reth-task", fut, false, false)
    }

    fn spawn_critical_task(
        &self,
        name: &'static str,
        fut: BoxFuture<'static, ()>,
    ) -> JoinHandle<()> {
        self.spawn_reth_task(name, fut, true, false)
    }

    fn spawn_blocking_task(&self, fut: BoxFuture<'static, ()>) -> JoinHandle<()> {
        self.spawn_reth_task("reth-blocking-task", fut, false, true)
    }

    fn spawn_critical_blocking_task(
        &self,
        name: &'static str,
        fut: BoxFuture<'static, ()>,
    ) -> JoinHandle<()> {
        self.spawn_reth_task(name, fut, true, true)
    }
}

/// Indicate a non-normal exit on a a taskmanager join.
#[derive(Debug, Error)]
pub enum TaskJoinError {
    #[error("Critical task {0} has exited unexpectedly: OK")]
    CriticalExitOk(String),
    #[error("Critical task {0} has exited unexpectedly: {1}")]
    CriticalExitError(String, JoinError),
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use tokio::sync::mpsc::{self, Receiver, Sender};

    use crate::{Notifier, TaskKind, TaskManager};

    struct Ping {
        ping_rx: Receiver<u32>,
        pong_tx: Sender<u32>,
    }

    struct Pong {
        ping_tx: Sender<u32>,
        pong_rx: Receiver<u32>,
    }

    fn new_ping_pong() -> (Ping, Pong) {
        let (ping_tx, ping_rx) = mpsc::channel(10);
        let (pong_tx, pong_rx) = mpsc::channel(10);
        (Ping { ping_rx, pong_tx }, Pong { ping_tx, pong_rx })
    }

    impl Ping {
        async fn run(mut self) {
            while let Some(p) = self.ping_rx.recv().await {
                let _ = self.pong_tx.send(p).await;
            }
        }

        fn run_blocking(mut self) {
            while let Some(p) = self.ping_rx.blocking_recv() {
                let _ = self.pong_tx.try_send(p);
            }
        }
    }

    impl Pong {
        async fn ping(&mut self, p: u32) -> eyre::Result<u32> {
            self.ping_tx.send(p).await?;
            self.pong_rx.recv().await.map_or_else(|| Err(eyre::eyre!("No Pong!")), Ok)
        }
    }

    /// Test that the various spawns work and that the spawned tasks are dropped when the spawning
    /// TaskManager is dropped. Except for Spawner.spawn_block_task(), it does not spawn a
    /// future and will not be killed forcefully on drop.
    #[tokio::test]
    async fn test_task_manager() {
        let task_manager = TaskManager::default();
        let (ping_crit, mut pong_crit) = new_ping_pong();
        task_manager.spawn_critical_task("Crit", async move {
            ping_crit.run().await;
        });
        assert_eq!(pong_crit.ping(1).await.unwrap(), 1);
        assert_eq!(pong_crit.ping(2).await.unwrap(), 2);

        let (ping_norm, mut pong_norm) = new_ping_pong();
        task_manager.spawn_task("task", async move {
            ping_norm.run().await;
        });
        assert_eq!(pong_norm.ping(1).await.unwrap(), 1);
        assert_eq!(pong_norm.ping(2).await.unwrap(), 2);

        let spawner = task_manager.get_spawner();
        let (sping_crit, mut spong_crit) = new_ping_pong();
        spawner.spawn_critical_task("Crit", async move {
            sping_crit.run().await;
        });
        assert_eq!(spong_crit.ping(1).await.unwrap(), 1);
        assert_eq!(spong_crit.ping(2).await.unwrap(), 2);

        let (sping_norm, mut spong_norm) = new_ping_pong();
        spawner.spawn_task("task", async move {
            sping_norm.run().await;
        });
        assert_eq!(spong_norm.ping(1).await.unwrap(), 1);
        assert_eq!(spong_norm.ping(2).await.unwrap(), 2);

        let (sping_block, mut spong_block) = new_ping_pong();
        spawner.spawn_blocking_task("SBlock", move || {
            sping_block.run_blocking();
        });
        assert_eq!(spong_block.ping(1).await.unwrap(), 1);
        assert_eq!(spong_block.ping(2).await.unwrap(), 2);

        // Test the reth TaskSpawner trait interface.
        // Use fully qualified syntax because the inherent methods (which take
        // a name + future) shadow the trait methods (which take only a future).
        use reth_tasks::TaskSpawner as RethTaskSpawner;
        let (rsping_crit, mut rspong_crit) = new_ping_pong();
        RethTaskSpawner::spawn_critical_task(
            &spawner,
            "Crit",
            Box::pin(async move {
                rsping_crit.run().await;
            }),
        );
        assert_eq!(rspong_crit.ping(1).await.unwrap(), 1);
        assert_eq!(rspong_crit.ping(2).await.unwrap(), 2);

        let (rsping_norm, mut rspong_norm) = new_ping_pong();
        RethTaskSpawner::spawn_task(
            &spawner,
            Box::pin(async move {
                rsping_norm.run().await;
            }),
        );
        assert_eq!(rspong_norm.ping(1).await.unwrap(), 1);
        assert_eq!(rspong_norm.ping(2).await.unwrap(), 2);

        let (rsping_block, mut rspong_block) = new_ping_pong();
        RethTaskSpawner::spawn_blocking_task(
            &spawner,
            Box::pin(async move {
                rsping_block.run().await;
            }),
        );
        assert_eq!(rspong_block.ping(1).await.unwrap(), 1);
        assert_eq!(rspong_block.ping(2).await.unwrap(), 2);

        let (rsping_crit_block, mut rspong_crit_block) = new_ping_pong();
        RethTaskSpawner::spawn_critical_blocking_task(
            &spawner,
            "Crit block",
            Box::pin(async move {
                rsping_crit_block.run().await;
            }),
        );
        assert_eq!(rspong_crit_block.ping(1).await.unwrap(), 1);
        assert_eq!(rspong_crit_block.ping(2).await.unwrap(), 2);

        drop(task_manager);

        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(pong_crit.ping(2).await.is_err());
        assert!(pong_norm.ping(2).await.is_err());

        assert!(spong_crit.ping(2).await.is_err());
        assert!(spong_norm.ping(2).await.is_err());
        // Note this blocking task is NOT killed when task manager is dropped..
        assert_eq!(spong_block.ping(3).await.unwrap(), 3);

        assert!(rspong_crit.ping(2).await.is_err());
        assert!(rspong_norm.ping(2).await.is_err());
        assert!(rspong_block.ping(2).await.is_err());
        assert!(rspong_crit_block.ping(2).await.is_err());
    }

    /// Drainable tasks survive `abort_doomed_tasks()` but are killed by `abort_all_tasks()`.
    #[tokio::test]
    async fn test_drainable_survives_abort_doomed() {
        let mut task_manager = TaskManager::new("drainable-test");

        // Spawn a Drainable task via TaskManager.
        let (ping_drain, mut pong_drain) = new_ping_pong();
        task_manager.spawn_classified_task(
            "drainable",
            async move {
                ping_drain.run().await;
            },
            TaskKind::Drainable,
        );

        // Spawn a Doomed task via TaskManager.
        let (ping_doomed, mut pong_doomed) = new_ping_pong();
        task_manager.spawn_classified_task(
            "doomed",
            async move {
                ping_doomed.run().await;
            },
            TaskKind::Doomed,
        );

        // Spawn a Drainable task via TaskSpawner.
        let spawner = task_manager.get_spawner();
        let (sping_drain, mut spong_drain) = new_ping_pong();
        spawner.spawn_classified_task(
            "spawner-drainable",
            async move {
                sping_drain.run().await;
            },
            TaskKind::Drainable,
        );

        // Spawn a Doomed task via TaskSpawner.
        let (sping_doomed, mut spong_doomed) = new_ping_pong();
        spawner.spawn_classified_task(
            "spawner-doomed",
            async move {
                sping_doomed.run().await;
            },
            TaskKind::Doomed,
        );

        // Pull spawner tasks into the tracked set.
        task_manager.update_tasks();

        // All four tasks respond before abort.
        assert_eq!(pong_drain.ping(1).await.unwrap(), 1);
        assert_eq!(pong_doomed.ping(1).await.unwrap(), 1);
        assert_eq!(spong_drain.ping(1).await.unwrap(), 1);
        assert_eq!(spong_doomed.ping(1).await.unwrap(), 1);

        // abort_doomed_tasks: hard-aborts Doomed/Cancel. It no longer signals Drainable —
        // join_internal owns that, ordered, after producers are reaped. Drainables here
        // ignore local_shutdown anyway, so they stay alive until abort_all_tasks.
        task_manager.abort_doomed_tasks();
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Doomed tasks are dead.
        assert!(pong_doomed.ping(2).await.is_err());
        assert!(spong_doomed.ping(2).await.is_err());

        // Drainable tasks are still alive (no outer select! to kill them).
        assert_eq!(pong_drain.ping(2).await.unwrap(), 2);
        assert_eq!(spong_drain.ping(2).await.unwrap(), 2);

        // abort_all_tasks: hard-aborts everything including Drainable.
        task_manager.abort_all_tasks();
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(pong_drain.ping(3).await.is_err());
        assert!(spong_drain.ping(3).await.is_err());
    }

    /// Drainable critical tasks return Ok without triggering a node exit in join.
    #[tokio::test]
    async fn test_drainable_ok_does_not_break_join() {
        let shutdown = Notifier::default();
        let mut task_manager = TaskManager::new("drain-join-test");
        task_manager.set_join_wait(500);

        // Drainable task that completes immediately.
        task_manager.spawn_classified_task("drainable-ok", async {}, TaskKind::Drainable);

        // Doomed task that completes immediately — this would normally break join.
        // But we pre-notify shutdown so join runs in drain mode.
        task_manager.spawn_classified_task("doomed-ok", async {}, TaskKind::Doomed);

        shutdown.notify();
        let result = task_manager.join(shutdown).await;
        // In drain mode, both tasks returning Ok is fine — no error.
        assert!(result.is_ok());
    }

    /// Ordered teardown: a Drainable (consumer) is signalled only AFTER every
    /// Doomed/Cancel (producer) has been reaped (its future dropped). This is the
    /// invariant that lets the next epoch re-subscribe to a producer-held channel
    /// without racing the prior subscriber's drop.
    #[tokio::test]
    async fn test_producers_reaped_before_consumers_signalled() {
        use std::sync::{Arc, Mutex};

        let shutdown = Notifier::default();
        let mut tm = TaskManager::new("order-test");
        tm.set_join_wait(1000);

        // Shared log capturing the order of two events: the producer's future being
        // dropped ("producer-dropped") and the consumer observing shutdown ("consumer-signalled").
        let log: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));

        // Producer (Doomed): never completes on its own, so it is reaped ONLY by the
        // ordered abort inside join. It signals once its Drop guard is set up so the test
        // can be sure it's actually running before teardown (otherwise it could be aborted
        // before its first poll and the guard would never be constructed).
        let plog = log.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        tm.spawn_classified_task(
            "producer",
            async move {
                struct Guard(Arc<Mutex<Vec<&'static str>>>);
                impl Drop for Guard {
                    fn drop(&mut self) {
                        self.0.lock().unwrap().push("producer-dropped");
                    }
                }
                let _g = Guard(plog);
                let _ = started_tx.send(());
                std::future::pending::<()>().await;
            },
            TaskKind::Doomed,
        );

        // Consumer (Drainable): records the instant it observes local_shutdown, then exits.
        let clog = log.clone();
        let rx = tm.shutdown_subscriber();
        tm.spawn_classified_task(
            "consumer",
            async move {
                rx.await;
                clog.lock().unwrap().push("consumer-signalled");
            },
            TaskKind::Drainable,
        );

        // Make sure the producer is actually running (guard constructed) before teardown.
        started_rx.await.unwrap();

        // Drain mode: join must abort+reap the producer, THEN signal the consumer.
        shutdown.notify();
        let _ = tm.join(shutdown).await;

        // The producer's future was dropped before the consumer was ever signalled.
        assert_eq!(
            *log.lock().unwrap(),
            vec!["producer-dropped", "consumer-signalled"],
            "consumer must not be signalled until the producer is reaped"
        );
    }

    /// Per-phase stall bound: a Drainable consumer that ignores the shutdown signal is
    /// hard-aborted after `phase_wait`, so join completes (bounded) instead of hanging
    /// until the caller's outer timeout.
    #[tokio::test]
    async fn test_wedged_consumer_is_force_aborted_after_bound() {
        let shutdown = Notifier::default();
        let mut tm = TaskManager::new("force-test");
        tm.set_phase_stall_bound(Duration::from_millis(100)); // short escalation watchdog
        tm.set_join_wait(100); // short post-loop cleanup grace

        // Drainable consumer that never observes shutdown and never exits on its own.
        tm.spawn_classified_task(
            "wedged-consumer",
            async { std::future::pending::<()>().await },
            TaskKind::Drainable,
        );

        shutdown.notify();
        // Without the per-phase force-abort this would hang until an external timeout.
        let res = tokio::time::timeout(Duration::from_secs(5), tm.join(shutdown)).await;
        assert!(res.is_ok(), "join must force-abort the wedged consumer and complete, not hang");
    }
}
