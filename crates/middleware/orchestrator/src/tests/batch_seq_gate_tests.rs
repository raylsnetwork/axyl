//! Unit tests for batch-builder eligibility (`NodeMode::is_batch_producing`) and the
//! execution-replay wait (`await_execution_replay`), whose three outcomes are reached via the
//! pre-check and the select.

use crate::epoch_manager::{await_execution_replay, ReplayWaitOutcome};
use rayls_infrastructure_types::{NodeMode, Notifier};
use std::time::Duration;
use tokio::sync::watch;

/// A catching-up CvvInactive node must not run a batch builder; see `NodeMode::is_batch_producing`.
#[test]
fn cvv_inactive_does_not_produce_batches() {
    assert!(!NodeMode::CvvInactive.is_batch_producing());
}

/// Active CVVs produce batches; observers forward instead of sealing.
#[test]
fn only_active_cvv_produces_batches() {
    assert!(NodeMode::CvvActive.is_batch_producing());
    assert!(!NodeMode::Observer.is_batch_producing());
}

fn channels() -> (watch::Sender<bool>, watch::Sender<Option<NodeMode>>, Notifier) {
    let (replay, _) = watch::channel(false);
    let (transition, _) = watch::channel(None);
    let shutdown = Notifier::new();
    (replay, transition, shutdown)
}

/// Pre-check hits Ready immediately when replay is already complete.
#[tokio::test]
async fn ready_when_replay_already_complete() {
    let (replay, transition, shutdown) = channels();
    replay.send_replace(true);

    let outcome =
        await_execution_replay(replay.subscribe(), transition.subscribe(), shutdown.subscribe())
            .await;

    assert_eq!(outcome, ReplayWaitOutcome::Ready);
}

/// Select hits Defer immediately when a transition is already pending.
#[tokio::test]
async fn defers_when_transition_already_pending() {
    let (replay, transition, shutdown) = channels();
    transition.send_replace(Some(NodeMode::CvvInactive));

    let outcome =
        await_execution_replay(replay.subscribe(), transition.subscribe(), shutdown.subscribe())
            .await;

    assert_eq!(outcome, ReplayWaitOutcome::Defer);
}

/// Select resolves to Ready when replay completes during the wait.
#[tokio::test]
async fn ready_when_replay_completes_during_wait() {
    let (replay, transition, shutdown) = channels();

    let wait = tokio::spawn(await_execution_replay(
        replay.subscribe(),
        transition.subscribe(),
        shutdown.subscribe(),
    ));

    // Let the wait enter its select before flipping the watch.
    tokio::time::sleep(Duration::from_millis(20)).await;
    replay.send_replace(true);

    assert_eq!(wait.await.unwrap(), ReplayWaitOutcome::Ready);
}

/// Select resolves to Defer when a transition is requested during the wait.
#[tokio::test]
async fn defers_when_transition_arrives_during_wait() {
    let (replay, transition, shutdown) = channels();

    let wait = tokio::spawn(await_execution_replay(
        replay.subscribe(),
        transition.subscribe(),
        shutdown.subscribe(),
    ));

    tokio::time::sleep(Duration::from_millis(20)).await;
    transition.send_replace(Some(NodeMode::CvvInactive));

    assert_eq!(wait.await.unwrap(), ReplayWaitOutcome::Defer);
}

/// Select resolves to Shutdown when the shutdown Noticer fires.
#[tokio::test]
async fn shuts_down_when_notifier_fires() {
    let (replay, transition, shutdown) = channels();

    let wait = tokio::spawn(await_execution_replay(
        replay.subscribe(),
        transition.subscribe(),
        shutdown.subscribe(),
    ));

    tokio::time::sleep(Duration::from_millis(20)).await;
    shutdown.notify();

    assert_eq!(wait.await.unwrap(), ReplayWaitOutcome::Shutdown);
}

/// `biased;` ensures shutdown wins if it and another branch fire in the same tick.
#[tokio::test]
async fn shutdown_wins_over_replay_when_both_fire() {
    let (replay, transition, shutdown) = channels();

    let wait = tokio::spawn(await_execution_replay(
        replay.subscribe(),
        transition.subscribe(),
        shutdown.subscribe(),
    ));

    tokio::time::sleep(Duration::from_millis(20)).await;
    // Fire shutdown before flipping replay; biased select should still observe shutdown first.
    shutdown.notify();
    replay.send_replace(true);

    assert_eq!(wait.await.unwrap(), ReplayWaitOutcome::Shutdown);
}
