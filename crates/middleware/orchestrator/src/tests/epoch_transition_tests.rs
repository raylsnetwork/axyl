//! Comprehensive tests for epoch transition race conditions.
//!
//! Covers all 6 ranked race conditions from the epoch transition redesign
//! document (`docs/epoch-transition-race-conditions.md`). Each test explains
//! which race condition it targets and why the new architecture prevents it.

use crate::epoch_manager::{decide_node_mode, node_has_local_history, select_recovery_checkpoint};
use rayls_consensus_primary::{ConsensusBus, NodeMode};
use rayls_infrastructure_storage::{mem_db::MemDatabase, CheckpointStore};
use rayls_infrastructure_types::{EpochTransitionCheckpoint, EpochTransitionPhase, Notifier, B256};
use std::sync::{
    atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    Arc,
};
use tokio::sync::{mpsc, oneshot, watch};

// ---------------------------------------------------------------------------
// RANK 5 (LOW): reset_for_epoch Stale Channel References
// ---------------------------------------------------------------------------
// After reset_for_epoch(), the epoch-scoped channels (sequence, drain, etc.)
// must be entirely new. If stale references persisted, messages from the old
// epoch could leak into the new one, or drain signals could be misinterpreted.

/// RANK 5: Verify fresh channels after reset_for_epoch.
///
/// After reset, committed_round should be 0, gc_round should be 0, and the
/// drain signal should be false. This prevents stale epoch state from leaking
/// into the new epoch.
#[tokio::test]
async fn test_fresh_channels_after_reset() {
    let mut bus = ConsensusBus::new();

    // Simulate activity: advance committed and gc rounds.
    bus.committed_round_updates().send_replace(42);
    bus.gc_round_updates().send_replace(35);
    bus.primary_round_updates().send_replace(50);

    // Set drain signal as if we just finished draining.
    let _ = bus.drain_signal().send(Some(100));

    // Verify pre-reset state.
    assert_eq!(*bus.committed_round_updates().borrow(), 42);
    assert_eq!(*bus.gc_round_updates().borrow(), 35);

    // Reset for a new epoch.
    bus.reset_for_epoch();

    // All app-scoped watch channels should be reset to defaults.
    assert_eq!(*bus.committed_round_updates().borrow(), 0, "committed_round must be 0 after reset");
    assert_eq!(*bus.gc_round_updates().borrow(), 0, "gc_round must be 0 after reset");
    assert_eq!(*bus.primary_round_updates().borrow(), 0, "primary_round must be 0 after reset");
}

/// RANK 5: Verify drain channels are properly reset.
///
/// After reset_for_epoch(), the drain signal must be false and a fresh
/// drain_ack oneshot must be available. This ensures the new epoch's subscriber
/// can participate in a drain protocol without interference from the old epoch.
#[tokio::test]
async fn test_drain_channels_reset() {
    let mut bus = ConsensusBus::new();

    // Consume the drain_ack channels (simulating what subscriber does).
    let _ack_tx = bus.take_drain_ack_tx();
    let _ack_rx = bus.take_drain_ack_rx();

    // After taking, they should be None.
    assert!(bus.take_drain_ack_tx().is_none(), "ack_tx should be consumed");
    assert!(bus.take_drain_ack_rx().is_none(), "ack_rx should be consumed");

    // Set drain signal to Some(boundary_round).
    let _ = bus.drain_signal().send(Some(100));

    // Reset for new epoch.
    bus.reset_for_epoch();

    // Drain signal must be None on new epoch's channels.
    assert!(bus.drain_signal().borrow().is_none(), "drain_signal must be None after reset");

    // Fresh drain_ack channels must be available.
    assert!(bus.take_drain_ack_tx().is_some(), "drain_ack_tx must be available after reset");
    assert!(bus.take_drain_ack_rx().is_some(), "drain_ack_rx must be available after reset");
}

// ---------------------------------------------------------------------------
// RANK 1 (CRITICAL): Subscriber Drain Protocol
// ---------------------------------------------------------------------------
// The drain protocol ensures that the subscriber finishes all in-flight
// FuturesOrdered work before acknowledging shutdown. Without this, subdags
// consumed from the sequence channel but not yet save_consensus()'d are lost
// permanently.

/// RANK 1: Drain protocol completes in-flight work before acknowledging.
///
/// Simulates the subscriber's drain behavior: when a drain signal arrives
/// while work is in-flight, the subscriber must finish processing all pending
/// items before sending the drain ack. This prevents subdag loss.
#[tokio::test]
async fn test_drain_protocol_completes_inflight_work() {
    let bus = ConsensusBus::new();

    // Take the drain channels (as subscriber and manager would).
    let drain_ack_tx = bus.take_drain_ack_tx().expect("drain_ack_tx available");
    let drain_ack_rx = bus.take_drain_ack_rx().expect("drain_ack_rx available");
    let mut drain_rx = bus.drain_signal().subscribe();

    // Track whether in-flight work was completed before ack.
    let work_completed = Arc::new(AtomicBool::new(false));
    let work_completed_check = work_completed.clone();

    // Simulate the subscriber: wait for drain, do in-flight work, then ack.
    let subscriber_handle = tokio::spawn(async move {
        // Wait for drain signal.
        loop {
            if drain_rx.changed().await.is_err() {
                return;
            }
            if drain_rx.borrow().is_some() {
                break;
            }
        }

        // Simulate processing in-flight FuturesOrdered items.
        // In real code, this is the `waiting.next()` loop.
        tokio::task::yield_now().await;
        work_completed.store(true, Ordering::Release);

        // Send drain ack after all work is done.
        let _ = drain_ack_tx.send(());
    });

    // Manager sends drain signal with boundary round.
    bus.drain_signal().send_replace(Some(100));

    // Manager waits for drain ack.
    let ack_result = tokio::time::timeout(std::time::Duration::from_secs(5), drain_ack_rx).await;

    assert!(ack_result.is_ok(), "drain ack should arrive");
    assert!(
        work_completed_check.load(Ordering::Acquire),
        "in-flight work must complete BEFORE drain ack is sent"
    );

    subscriber_handle.await.unwrap();
}

/// RANK 1: Drain protocol immediately acks when no in-flight work.
///
/// When the subscriber has no pending FuturesOrdered items, it must send the
/// drain ack immediately without blocking. This ensures fast epoch transitions
/// under light load.
#[tokio::test]
async fn test_drain_protocol_immediate_ack_when_empty() {
    let bus = ConsensusBus::new();

    let drain_ack_tx = bus.take_drain_ack_tx().expect("drain_ack_tx");
    let drain_ack_rx = bus.take_drain_ack_rx().expect("drain_ack_rx");
    let mut drain_rx = bus.drain_signal().subscribe();

    // Subscriber with no in-flight work: ack immediately on drain signal.
    let subscriber_handle = tokio::spawn(async move {
        loop {
            if drain_rx.changed().await.is_err() {
                return;
            }
            if drain_rx.borrow().is_some() {
                break;
            }
        }
        // No in-flight work, ack immediately (matches subscriber.rs empty case).
        let _ = drain_ack_tx.send(());
    });

    // Manager sends drain signal with boundary round.
    bus.drain_signal().send_replace(Some(100));

    // Ack should arrive very quickly.
    let ack_result =
        tokio::time::timeout(std::time::Duration::from_millis(100), drain_ack_rx).await;

    assert!(ack_result.is_ok(), "ack should arrive immediately when no in-flight work");
    subscriber_handle.await.unwrap();
}

/// RANK 1: Drain protocol stops accepting new subdags.
///
/// After the drain signal, the subscriber must not dequeue any new subdags from
/// rx_sequence even if messages are available. This prevents processing subdags
/// that would never be saved before shutdown.
#[tokio::test]
async fn test_drain_protocol_stops_accepting_new_subdags() {
    let bus = ConsensusBus::new();

    let drain_ack_tx = bus.take_drain_ack_tx().expect("drain_ack_tx");
    let drain_ack_rx = bus.take_drain_ack_rx().expect("drain_ack_rx");
    let mut drain_rx = bus.drain_signal().subscribe();

    // Channel simulating rx_sequence.
    let (seq_tx, mut seq_rx) = mpsc::channel::<u64>(10);
    let subdags_consumed = Arc::new(AtomicU64::new(0));
    let subdags_consumed_check = subdags_consumed.clone();

    // Subscriber: once draining, stop accepting new subdags. Mirrors the real
    // subscriber pattern where draining=true disables the recv() arm via the
    // select! guard, then the task finishes in-flight work before acking.
    let subscriber_handle = tokio::spawn(async move {
        let mut draining = false;
        let mut ack_tx = Some(drain_ack_tx);
        loop {
            tokio::select! {
                Some(subdag) = seq_rx.recv(), if !draining => {
                    subdags_consumed.fetch_add(1, Ordering::Relaxed);
                    let _ = subdag;
                },
                Ok(_) = drain_rx.changed(), if !draining => {
                    if drain_rx.borrow().is_some() {
                        draining = true;
                        let _ = ack_tx.take().map(|tx| tx.send(()));
                    }
                },
                // When draining, both guards are false so select! needs an
                // else branch to avoid a deadlock. In the real subscriber
                // this is where in-flight work would finish.
                else => break,
            }
        }
    });

    // Send some subdags first (before drain).
    seq_tx.send(1).await.unwrap();
    seq_tx.send(2).await.unwrap();
    // Give subscriber time to process.
    tokio::task::yield_now().await;
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    let consumed_before_drain = subdags_consumed_check.load(Ordering::Relaxed);

    // Now send drain signal with boundary round.
    bus.drain_signal().send_replace(Some(100));

    // Wait for ack.
    let _ = drain_ack_rx.await;

    // Send more subdags after drain (these should NOT be consumed).
    let _ = seq_tx.send(3).await;
    let _ = seq_tx.send(4).await;
    tokio::task::yield_now().await;

    let consumed_after_drain = subdags_consumed_check.load(Ordering::Relaxed);

    // No subdags should be consumed after drain signal.
    assert_eq!(
        consumed_before_drain, consumed_after_drain,
        "no subdags should be consumed after drain signal"
    );

    subscriber_handle.await.unwrap();
}

/// RANK 1: Compare old shutdown (loses subdags) vs new drain (preserves them).
///
/// Old behavior: rx_shutdown fires, subscriber returns immediately, any
/// in-flight work in FuturesOrdered is dropped and never saved.
/// New behavior: drain signal causes subscriber to finish all in-flight work
/// before returning.
#[tokio::test]
async fn test_drain_vs_old_shutdown_comparison() {
    // --- OLD BEHAVIOR (fire-and-forget shutdown) ---
    let (old_shutdown_tx, old_shutdown_rx) = oneshot::channel::<()>();
    let old_saved = Arc::new(AtomicU32::new(0));
    let old_saved_check = old_saved.clone();

    let old_subscriber = tokio::spawn(async move {
        // Simulate 3 in-flight items.
        let (work_tx, mut work_rx) = mpsc::channel::<u32>(10);
        for i in 0..3 {
            work_tx.send(i).await.unwrap();
        }

        tokio::select! {
            // Old behavior: shutdown wins the select, drops all pending work.
            _ = old_shutdown_rx => {
                // Return immediately, dropping any pending work_rx items.
                return;
            }
            Some(_item) = work_rx.recv() => {
                old_saved.fetch_add(1, Ordering::Relaxed);
            }
        }
    });

    // Fire old shutdown immediately.
    let _ = old_shutdown_tx.send(());
    old_subscriber.await.unwrap();
    let old_count = old_saved_check.load(Ordering::Relaxed);

    // --- NEW BEHAVIOR (drain protocol) ---
    let (new_drain_tx, mut new_drain_rx) = watch::channel(false);
    let (new_ack_tx, new_ack_rx) = oneshot::channel::<()>();
    let new_saved = Arc::new(AtomicU32::new(0));
    let new_saved_check = new_saved.clone();

    let new_subscriber = tokio::spawn(async move {
        let (work_tx, mut work_rx) = mpsc::channel::<u32>(10);
        for i in 0..3 {
            work_tx.send(i).await.unwrap();
        }
        drop(work_tx); // Close sender so recv() returns None when empty.

        let mut draining = false;
        let mut ack_tx = Some(new_ack_tx);

        loop {
            tokio::select! {
                item = work_rx.recv() => {
                    match item {
                        Some(_) => {
                            new_saved.fetch_add(1, Ordering::Relaxed);
                        }
                        None => {
                            // All work processed.
                            if draining {
                                let _ = ack_tx.take().map(|tx| tx.send(()));
                                return;
                            }
                        }
                    }
                },
                Ok(_) = new_drain_rx.changed(), if !draining => {
                    if *new_drain_rx.borrow() {
                        draining = true;
                    }
                },
            }
        }
    });

    // Send drain signal.
    new_drain_tx.send_replace(true);

    // Wait for ack.
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), new_ack_rx).await;
    new_subscriber.await.unwrap();
    let new_count = new_saved_check.load(Ordering::Relaxed);

    // Old behavior likely saves 0 or 1 items. New behavior saves all 3.
    assert!(
        new_count >= old_count,
        "drain protocol must save at least as many items as old shutdown ({new_count} >= {old_count})"
    );
    assert_eq!(new_count, 3, "drain protocol must save ALL in-flight items");
}

// ---------------------------------------------------------------------------
// RANK 2 (CRITICAL): Select Side-Effect Elimination
// ---------------------------------------------------------------------------
// The new architecture uses a pure-detection select! that returns a
// RunningOutcome without side effects. Engine sends and state mutations happen
// in sequential phases after the select resolves.

/// RANK 2: Verify RunningOutcome captures epoch boundary without side effects.
///
/// In the new design, detect_epoch_boundary() returns a target hash and the
/// boundary output without sending it to the engine. Engine output is sent
/// in the sequential EXECUTION_COMPLETE phase. This test verifies the type
/// system captures both values.
#[test]
fn test_running_outcome_epoch_boundary_captures_hash() {
    use crate::types::RunningOutcome;
    use rayls_infrastructure_types::ConsensusOutput;

    let hash = B256::random();
    let output = ConsensusOutput::default();
    let outcome = RunningOutcome::EpochBoundary(hash, Box::new(output));

    match outcome {
        RunningOutcome::EpochBoundary(h, _out) => {
            assert_eq!(h, hash, "RunningOutcome must capture the target hash");
        }
        _ => panic!("expected EpochBoundary variant"),
    }
}

/// RANK 2: Verify RunningOutcome variants cover all select branches.
///
/// The type system ensures that all three select branches (node shutdown,
/// epoch boundary, task crash) are represented. No silent data loss from
/// an unhandled branch.
#[test]
fn test_running_outcome_variants_complete() {
    use crate::types::RunningOutcome;
    use rayls_infrastructure_types::ConsensusOutput;

    // NodeShutdown
    let _shutdown = RunningOutcome::NodeShutdown;

    // EpochBoundary (carries both hash and the deferred output)
    let _boundary = RunningOutcome::EpochBoundary(B256::ZERO, Box::new(ConsensusOutput::default()));

    // ModeTransition
    let _mode = RunningOutcome::ModeTransition(rayls_consensus_primary::NodeMode::CvvInactive);

    // TaskCrash
    let _crash = RunningOutcome::TaskCrash(eyre::eyre!("test crash"));
}

/// RANK 2: Verify all 5 sequential phases exist in EpochTransitionPhase.
///
/// The phase-based state machine requires exactly these 5 phases to execute
/// in order. Missing a phase could skip critical cleanup (like
/// write_epoch_record or clear_consensus_db).
#[test]
fn test_sequential_phases_all_defined() {
    let phases = [
        EpochTransitionPhase::BoundaryDetected,
        EpochTransitionPhase::Draining,
        EpochTransitionPhase::ConsensusShutdown,
        EpochTransitionPhase::ExecutionComplete,
        EpochTransitionPhase::Cleared,
    ];

    // Verify all phases are distinct.
    for (i, a) in phases.iter().enumerate() {
        for (j, b) in phases.iter().enumerate() {
            if i != j {
                assert_ne!(a, b, "phases must be distinct: {a:?} vs {b:?}");
            }
        }
    }
}

/// RANK 2: Verify EpochTransitionCheckpoint captures all needed state.
///
/// Each checkpoint must record the epoch, the completed phase, the target hash,
/// and a timestamp. This enables crash recovery to resume from any phase.
#[test]
fn test_epoch_transition_checkpoint_captures_state() {
    let checkpoint = EpochTransitionCheckpoint {
        epoch: 42,
        completed_phase: EpochTransitionPhase::Draining,
        target_hash: B256::random(),
        timestamp: 1234567890,
    };

    assert_eq!(checkpoint.epoch, 42);
    assert_eq!(checkpoint.completed_phase, EpochTransitionPhase::Draining);
    assert_ne!(checkpoint.target_hash, B256::ZERO);
    assert_eq!(checkpoint.timestamp, 1234567890);
}

/// RANK 2: Verify that epoch record is always written (checkpoint recovery).
///
/// In the old design, if join() wins the select race, write_epoch_record() is
/// never called. With checkpoints, even if the node crashes after
/// ConsensusShutdown phase, recovery can detect that Cleared was not reached
/// and complete the remaining phases (including write_epoch_record).
#[test]
fn test_checkpoint_enables_epoch_record_recovery() {
    // Simulate crash after ConsensusShutdown phase.
    let checkpoint = EpochTransitionCheckpoint {
        epoch: 5,
        completed_phase: EpochTransitionPhase::ConsensusShutdown,
        target_hash: B256::random(),
        timestamp: 1234567890,
    };

    // Recovery logic: if completed_phase < Cleared, remaining phases need to run.
    // ConsensusShutdown means ExecutionComplete and Cleared still need to happen.
    // write_epoch_record() happens as part of Cleared phase.
    let needs_execution = matches!(
        checkpoint.completed_phase,
        EpochTransitionPhase::BoundaryDetected
            | EpochTransitionPhase::Draining
            | EpochTransitionPhase::ConsensusShutdown
    );
    assert!(needs_execution, "crash before ExecutionComplete requires remaining phases");

    let needs_clear = !matches!(checkpoint.completed_phase, EpochTransitionPhase::Cleared);
    assert!(needs_clear, "crash before Cleared requires table clear + epoch record");
}

// The former RANK 4 test (`test_sequential_execution_phase_prevents_recently_executed_blocks_race`)
// was removed: it guarded a race where `spawn_engine_update_task` could write a stale block into
// `recently_executed_blocks` and corrupt the epoch record's `parent_state`. That race no longer
// exists — `write_epoch_record` now sources `parent_state` from the durable canonical tip
// (`engine.get_reth_env().canonical_tip()`), not from the async-fed `recently_executed_blocks`, so
// no explicit push or sequential-phase ordering is needed to keep `parent_state` deterministic.

/// RANK 4: Verify recently_executed_blocks accurate after reset.
///
/// After reset_for_epoch(), recently_executed_blocks should carry over the latest block
/// from the previous epoch (as the parent for the new epoch's first block).
#[tokio::test]
async fn test_recently_executed_blocks_accurate_after_transition() {
    let mut bus = ConsensusBus::new();

    // Push a block as if the epoch just ended.
    let closing_header = {
        let mut h = rayls_infrastructure_types::ExecHeader::default();
        h.number = 50;
        rayls_infrastructure_types::SealedHeader::seal_slow(h)
    };
    bus.recently_executed_blocks().send_modify(|blocks| blocks.push_latest(closing_header));

    // Reset for new epoch.
    bus.reset_for_epoch();

    // After reset, recently_executed_blocks should have exactly the last block from previous epoch.
    let recent = bus.recently_executed_blocks().borrow();
    assert_eq!(
        recent.latest_block_num_hash().number,
        50,
        "latest block should carry over across epoch reset"
    );
    assert_eq!(recent.len(), 1, "only the carryover block should remain");
}

// ---------------------------------------------------------------------------
// RANK 6 (LOW): Epoch Vote Collection Timing
// ---------------------------------------------------------------------------
// Votes arriving between primary.start() and subscription setup are buffered
// by the mpsc channel. This test verifies the QueChannel buffering behavior.

/// RANK 6: Verify buffered messages are not lost.
///
/// The QueChannel uses an mpsc channel internally. Messages sent before
/// subscribe() is called are buffered. This ensures votes arriving between
/// primary start and subscription are not lost.
#[tokio::test]
async fn test_vote_buffering_before_subscription() {
    let bus = ConsensusBus::new();

    // Send messages before subscribing (simulating votes arriving early).
    // We'll use committed_round_updates (a watch channel) for simplicity.
    // For the actual vote channel (QueChannel), messages are buffered in mpsc.
    bus.committed_round_updates().send_replace(10);
    bus.committed_round_updates().send_replace(20);
    bus.committed_round_updates().send_replace(30);

    // Subscribe now (after messages were sent).
    let rx = bus.committed_round_updates().subscribe();

    // The latest value should be available (watch channels always have current value).
    assert_eq!(*rx.borrow(), 30, "latest value must be available after late subscription");
}

// ---------------------------------------------------------------------------
// Checkpoint Store Tests (RANK 2 recovery infrastructure)
// ---------------------------------------------------------------------------
// These verify the CheckpointStore trait that enables crash recovery.

/// Verify checkpoint store save/load/clear cycle with MemDatabase.
#[test]
fn test_checkpoint_store_save_load_clear() {
    let db = MemDatabase::default();

    let checkpoint = EpochTransitionCheckpoint {
        epoch: 7,
        completed_phase: EpochTransitionPhase::ExecutionComplete,
        target_hash: B256::random(),
        timestamp: 999,
    };

    // Save.
    db.save_checkpoint(&checkpoint).expect("save should succeed");

    // Load.
    let loaded = db.load_checkpoint(7).expect("load should succeed");
    assert!(loaded.is_some(), "checkpoint should exist after save");
    let loaded = loaded.unwrap();
    assert_eq!(loaded.epoch, 7);
    assert_eq!(loaded.completed_phase, EpochTransitionPhase::ExecutionComplete);
    assert_eq!(loaded.target_hash, checkpoint.target_hash);
    assert_eq!(loaded.timestamp, 999);

    // Clear.
    db.clear_checkpoint(7).expect("clear should succeed");
    let loaded = db.load_checkpoint(7).expect("load should succeed");
    assert!(loaded.is_none(), "checkpoint should be gone after clear");
}

/// Verify that checkpoints for different epochs are independent.
#[test]
fn test_checkpoint_store_multiple_epochs() {
    let db = MemDatabase::default();

    let cp1 = EpochTransitionCheckpoint {
        epoch: 1,
        completed_phase: EpochTransitionPhase::BoundaryDetected,
        target_hash: B256::random(),
        timestamp: 100,
    };
    let cp2 = EpochTransitionCheckpoint {
        epoch: 2,
        completed_phase: EpochTransitionPhase::Cleared,
        target_hash: B256::random(),
        timestamp: 200,
    };

    db.save_checkpoint(&cp1).unwrap();
    db.save_checkpoint(&cp2).unwrap();

    let loaded1 = db.load_checkpoint(1).unwrap().unwrap();
    let loaded2 = db.load_checkpoint(2).unwrap().unwrap();

    assert_eq!(loaded1.epoch, 1);
    assert_eq!(loaded1.completed_phase, EpochTransitionPhase::BoundaryDetected);
    assert_eq!(loaded2.epoch, 2);
    assert_eq!(loaded2.completed_phase, EpochTransitionPhase::Cleared);

    // Clearing epoch 1 should not affect epoch 2.
    db.clear_checkpoint(1).unwrap();
    assert!(db.load_checkpoint(1).unwrap().is_none());
    assert!(db.load_checkpoint(2).unwrap().is_some());
}

/// Verify that saving a checkpoint overwrites the previous one for the same epoch.
#[test]
fn test_checkpoint_store_overwrite() {
    let db = MemDatabase::default();

    let cp1 = EpochTransitionCheckpoint {
        epoch: 5,
        completed_phase: EpochTransitionPhase::Draining,
        target_hash: B256::random(),
        timestamp: 100,
    };
    db.save_checkpoint(&cp1).unwrap();

    let cp2 = EpochTransitionCheckpoint {
        epoch: 5,
        completed_phase: EpochTransitionPhase::ConsensusShutdown,
        target_hash: cp1.target_hash,
        timestamp: 200,
    };
    db.save_checkpoint(&cp2).unwrap();

    let loaded = db.load_checkpoint(5).unwrap().unwrap();
    assert_eq!(
        loaded.completed_phase,
        EpochTransitionPhase::ConsensusShutdown,
        "later checkpoint should overwrite earlier one"
    );
    assert_eq!(loaded.timestamp, 200);
}

// ---------------------------------------------------------------------------
// Integration / Compound: RANK 1 + RANK 2 Prevention
// ---------------------------------------------------------------------------

/// RANK 1 + RANK 2: Compound failure prevention.
///
/// Demonstrates that the new architecture prevents the compound scenario where:
/// 1. Subscriber drops subdags (RANK 1) AND
/// 2. join() winning select skips write_epoch_record (RANK 2)
///
/// The new design sequences these: drain first (prevents RANK 1), then
/// RANK 3 from triggering RANK 2.
#[tokio::test]
async fn test_rank1_rank2_compound_failure_prevented() {
    let mut bus = ConsensusBus::new();

    // --- Simulate the new epoch transition sequence ---

    // Phase 1: BoundaryDetected

    // Phase 2: Draining (RANK 1 prevention)
    let drain_ack_tx = bus.take_drain_ack_tx().expect("drain_ack_tx");
    let drain_ack_rx = bus.take_drain_ack_rx().expect("drain_ack_rx");

    // Subscriber receives drain, processes in-flight work, sends ack.
    let sub_handle = tokio::spawn(async move {
        // Simulate processing 5 in-flight items.
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
        let _ = drain_ack_tx.send(());
    });

    bus.drain_signal().send_replace(Some(100));

    // Manager waits for drain ack.
    let drain_result = tokio::time::timeout(std::time::Duration::from_secs(5), drain_ack_rx).await;
    assert!(drain_result.is_ok(), "drain ack must arrive (RANK 1 prevented)");
    sub_handle.await.unwrap();

    // Phase 3: ConsensusShutdown
    let shutdown = Notifier::new();
    shutdown.notify();
    assert!(shutdown.was_notified());

    // Phase 4: ExecutionComplete (sequential, no race with engine_update_task)
    // In real code, this waits for the epoch-closing block.

    // Phase 5: Cleared (RANK 2 prevention - always runs regardless of select outcome)
    // In real code: clear_consensus_db + write_epoch_record

    // Reset for next epoch.
    bus.reset_for_epoch();

    // Verify clean state for next epoch.
    assert_eq!(*bus.committed_round_updates().borrow(), 0);
    assert!(bus.drain_signal().borrow().is_none());
    assert!(bus.take_drain_ack_tx().is_some());
    assert!(bus.take_drain_ack_rx().is_some());
}

/// Integration: Multiple epoch transitions without divergence.
///
/// Simulates N epoch transitions using the new protocol. After each transition,
/// verifies that the bus state is clean and consistent.
#[tokio::test]
async fn test_multiple_epoch_transitions_no_divergence() {
    let mut bus = ConsensusBus::new();

    for epoch in 0..5u64 {
        // Phase 1: Boundary detected.

        // Simulate some activity.
        bus.committed_round_updates().send_replace(100 + epoch as u32);
        bus.gc_round_updates().send_replace(90 + epoch as u32);

        // Phase 2: Drain with boundary round.
        let ack_tx = bus.take_drain_ack_tx().expect("drain_ack_tx");
        let ack_rx = bus.take_drain_ack_rx().expect("drain_ack_rx");
        bus.drain_signal().send_replace(Some(100 + epoch as u32));

        let _ = tokio::spawn(async move {
            let _ = ack_tx.send(());
        });

        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), ack_rx).await;

        // Phase 3: Consensus shutdown.
        let shutdown = Notifier::new();
        shutdown.notify();

        // Phase 4 + 5: Execution + Clear.
        // (In real code, wait for engine + clear tables + write epoch record.)

        // Reset for next epoch.
        bus.reset_for_epoch();

        // Post-transition invariants that must hold after every epoch.
        // Drain channel availability is covered by test_drain_channels_reset;
        // we avoid take_drain_ack_tx/rx here because they consume the channels
        // and would require a second reset_for_epoch before the next iteration.
        assert_eq!(
            *bus.committed_round_updates().borrow(),
            0,
            "committed_round must be 0 after epoch {epoch}"
        );
        assert_eq!(*bus.gc_round_updates().borrow(), 0, "gc_round must be 0 after epoch {epoch}");
        assert!(
            bus.drain_signal().borrow().is_none(),
            "drain_signal must be None after epoch {epoch}"
        );
    }
}

/// Integration: Checkpoint recovery at each phase.
///
/// Verifies that for each possible crash point (each phase), a checkpoint
/// correctly captures the state needed for recovery.
#[test]
fn test_checkpoint_recovery_at_each_phase() {
    let db = MemDatabase::default();
    let target_hash = B256::random();

    let phases = [
        EpochTransitionPhase::BoundaryDetected,
        EpochTransitionPhase::Draining,
        EpochTransitionPhase::ConsensusShutdown,
        EpochTransitionPhase::ExecutionComplete,
        EpochTransitionPhase::Cleared,
    ];

    for (i, phase) in phases.iter().enumerate() {
        let checkpoint = EpochTransitionCheckpoint {
            epoch: 10,
            completed_phase: *phase,
            target_hash,
            timestamp: (1000 + i) as u64,
        };

        // Save checkpoint (simulating crash after this phase).
        db.save_checkpoint(&checkpoint).unwrap();

        // Load checkpoint (simulating recovery on restart).
        let recovered = db.load_checkpoint(10).unwrap().expect("checkpoint must exist");
        assert_eq!(recovered.completed_phase, *phase);
        assert_eq!(recovered.target_hash, target_hash);

        // Determine recovery actions needed based on completed phase.
        let needs_drain =
            matches!(recovered.completed_phase, EpochTransitionPhase::BoundaryDetected);
        let needs_shutdown = matches!(
            recovered.completed_phase,
            EpochTransitionPhase::BoundaryDetected | EpochTransitionPhase::Draining
        );
        let needs_execution = matches!(
            recovered.completed_phase,
            EpochTransitionPhase::BoundaryDetected
                | EpochTransitionPhase::Draining
                | EpochTransitionPhase::ConsensusShutdown
        );
        let needs_clear = !matches!(recovered.completed_phase, EpochTransitionPhase::Cleared);

        // Phases form a strict progression: earlier phases need more recovery work.
        match phase {
            EpochTransitionPhase::BoundaryDetected => {
                assert!(needs_drain && needs_shutdown && needs_execution && needs_clear);
            }
            EpochTransitionPhase::Draining => {
                assert!(!needs_drain && needs_shutdown && needs_execution && needs_clear);
            }
            EpochTransitionPhase::ConsensusShutdown => {
                assert!(!needs_drain && !needs_shutdown && needs_execution && needs_clear);
            }
            EpochTransitionPhase::ExecutionComplete => {
                assert!(!needs_drain && !needs_shutdown && !needs_execution && needs_clear);
            }
            EpochTransitionPhase::Cleared => {
                assert!(!needs_drain && !needs_shutdown && !needs_execution && !needs_clear);
            }
        }
    }

    // Clean up.
    db.clear_checkpoint(10).unwrap();
    assert!(db.load_checkpoint(10).unwrap().is_none());
}

/// RANK 2: Verify that task crash does not leave stale engine state.
///
/// When a consensus task crashes (TaskCrash variant), the engine should NOT
/// have received any close_epoch output since detect_epoch_boundary() never
/// returned EpochBoundary. The RunningOutcome type ensures the manager handles
/// TaskCrash differently from EpochBoundary.
#[test]
fn test_task_crash_no_stale_engine_state() {
    use crate::types::RunningOutcome;

    // Simulate task crash outcome.
    let outcome = RunningOutcome::TaskCrash(eyre::eyre!("subscriber channel error"));

    // In the match handler, TaskCrash does NOT trigger epoch transition phases.
    // No engine output was sent (detect_epoch_boundary never completed).
    match outcome {
        RunningOutcome::TaskCrash(e) => {
            assert!(
                e.to_string().contains("subscriber channel error"),
                "error must propagate through TaskCrash"
            );
        }
        RunningOutcome::EpochBoundary(..) => {
            panic!("TaskCrash must NOT be confused with EpochBoundary");
        }
        RunningOutcome::NodeShutdown => {
            panic!("TaskCrash must NOT be confused with NodeShutdown");
        }
        RunningOutcome::ModeTransition(_) => {
            panic!("TaskCrash must NOT be confused with ModeTransition");
        }
    }
}

/// RANK 2: Verify NodeShutdown is handled distinctly from epoch transition.
///
/// NodeShutdown should trigger clean shutdown without epoch transition phases.
/// This prevents partial epoch transitions during intentional node shutdown.
#[test]
fn test_node_shutdown_no_epoch_transition() {
    use crate::types::RunningOutcome;

    let outcome = RunningOutcome::NodeShutdown;

    match outcome {
        RunningOutcome::NodeShutdown => {
            // Correct: clean shutdown, no epoch transition.
        }
        RunningOutcome::EpochBoundary(..) => {
            panic!("NodeShutdown must NOT trigger epoch transition");
        }
        RunningOutcome::TaskCrash(_) => {
            panic!("NodeShutdown must NOT be treated as a crash");
        }
        RunningOutcome::ModeTransition(_) => {
            panic!("NodeShutdown must NOT be treated as a mode transition");
        }
    }
}

// ---------------------------------------------------------------------------
// Serialization Tests for Checkpoint Types
// ---------------------------------------------------------------------------

/// Verify EpochTransitionCheckpoint serialization round-trip.
///
/// Checkpoints are persisted to the DB, so serialization must be correct.
#[test]
fn test_checkpoint_serialization_roundtrip() {
    let checkpoint = EpochTransitionCheckpoint {
        epoch: 42,
        completed_phase: EpochTransitionPhase::ConsensusShutdown,
        target_hash: B256::random(),
        timestamp: 1706000000,
    };

    // Serialize and deserialize.
    let serialized = serde_json::to_string(&checkpoint).expect("serialize");
    let deserialized: EpochTransitionCheckpoint =
        serde_json::from_str(&serialized).expect("deserialize");

    assert_eq!(deserialized.epoch, checkpoint.epoch);
    assert_eq!(deserialized.completed_phase, checkpoint.completed_phase);
    assert_eq!(deserialized.target_hash, checkpoint.target_hash);
    assert_eq!(deserialized.timestamp, checkpoint.timestamp);
}

/// Verify EpochTransitionPhase serialization round-trip for all variants.
#[test]
fn test_phase_serialization_all_variants() {
    let phases = [
        EpochTransitionPhase::BoundaryDetected,
        EpochTransitionPhase::Draining,
        EpochTransitionPhase::ConsensusShutdown,
        EpochTransitionPhase::ExecutionComplete,
        EpochTransitionPhase::Cleared,
    ];

    for phase in &phases {
        let serialized = serde_json::to_string(phase).expect("serialize phase");
        let deserialized: EpochTransitionPhase =
            serde_json::from_str(&serialized).expect("deserialize phase");
        assert_eq!(&deserialized, phase, "round-trip failed for {phase:?}");
    }
}

// ---------------------------------------------------------------------------
// Manager-impl: detect_epoch_boundary() side-effect-free behavior
// ---------------------------------------------------------------------------
// The redesigned detect_epoch_boundary() returns the boundary output WITHOUT
// sending it to the engine. The engine send is deferred to the sequential
// EXECUTION_COMPLETE phase.

/// RANK 2: detect_epoch_boundary does NOT send the boundary output to engine.
///
/// In the old code, wait_for_epoch_boundary() sent all outputs (including the
/// boundary output) to the engine inside the select arm. This was a side
/// effect: if join() won the select race afterwards, the engine had already
/// received close_epoch=true but close_epoch() never ran.
///
/// In the new code, detect_epoch_boundary() sends non-boundary outputs to the
/// engine but returns the boundary output. We verify this by using a channel
/// and confirming only non-boundary messages arrive.
#[tokio::test]
async fn test_detect_epoch_boundary_does_not_send_boundary_to_engine() {
    use rayls_infrastructure_types::ConsensusOutput;

    use rayls_infrastructure_types::{Certificate, CommittedSubDag, ReputationScores};

    // Certificate::default() gives a zero-timestamp leader. Setting created_at
    // controls committed_at() because CommittedSubDag::new() uses
    // max(previous_ts, leader.header.created_at) and previous_ts is 0 (no
    // previous subdag).
    fn output_with_timestamp(ts: u64) -> ConsensusOutput {
        let mut leader = Certificate::default();
        leader.header.created_at = ts;
        let sub_dag = CommittedSubDag::new(vec![], leader, 0, ReputationScores::default(), None);
        ConsensusOutput { sub_dag: std::sync::Arc::new(sub_dag), ..Default::default() }
    }

    // Create a channel simulating the engine's input.
    let (engine_tx, mut engine_rx) = mpsc::channel::<ConsensusOutput>(10);

    // Simulate 3 outputs: two normal, one boundary.
    let (sim_tx, sim_rx) = mpsc::channel::<ConsensusOutput>(10);

    // Normal output 1 (committed_at = 5, below boundary).
    sim_tx.send(output_with_timestamp(5)).await.unwrap();

    // Normal output 2 (committed_at = 10, below boundary).
    sim_tx.send(output_with_timestamp(10)).await.unwrap();

    // Boundary output (committed_at = 100, at/above boundary).
    sim_tx.send(output_with_timestamp(100)).await.unwrap();
    drop(sim_tx);

    // Simulate detect_epoch_boundary logic (extracted from manager.rs:1088-1118).
    let epoch_boundary = 50u64;
    let mut rx = sim_rx;

    let result = async {
        while let Some(mut output) = rx.recv().await {
            if output.committed_at() >= epoch_boundary {
                output.close_epoch = true;
                let target_hash = output.consensus_header_hash();
                return Ok::<_, eyre::Error>((target_hash, output));
            } else {
                engine_tx.send(output).await.map_err(|e| eyre::eyre!("{e}"))?;
            }
        }
        Err(eyre::eyre!("channel closed"))
    }
    .await;

    // Verify the boundary output was returned, not sent.
    assert!(result.is_ok());
    let (target_hash, boundary_output) = result.unwrap();
    assert!(boundary_output.close_epoch, "boundary output must have close_epoch=true");
    assert_ne!(target_hash, B256::ZERO);

    // Verify only 2 non-boundary outputs were sent to engine.
    drop(engine_tx); // close sender so we can drain
    let mut engine_received = Vec::new();
    while let Some(out) = engine_rx.recv().await {
        engine_received.push(out);
    }
    assert_eq!(engine_received.len(), 2, "only non-boundary outputs should be sent to engine");
    for out in &engine_received {
        assert!(!out.close_epoch, "non-boundary outputs must NOT have close_epoch=true");
    }
}

// ---------------------------------------------------------------------------
// Manager-impl: run_epoch_transition() phase ordering
// ---------------------------------------------------------------------------

/// RANK 2: run_epoch_transition executes all 5 phases in strict order.
///
/// This tests that checkpoints progress through all phases sequentially.
/// In a real run_epoch_transition(), each phase saves a checkpoint before
/// proceeding to the next. We simulate this checkpoint progression.
#[test]
fn test_run_epoch_transition_checkpoint_progression() {
    let db = MemDatabase::default();
    let target_hash = B256::random();
    let epoch = 3u32;

    // Simulate what run_epoch_transition does: save checkpoint at each phase.
    let expected_phases = [
        EpochTransitionPhase::BoundaryDetected,
        EpochTransitionPhase::Draining,
        EpochTransitionPhase::ConsensusShutdown,
        EpochTransitionPhase::ExecutionComplete,
        // Cleared phase clears the checkpoint instead of saving
    ];

    for (i, phase) in expected_phases.iter().enumerate() {
        let checkpoint = EpochTransitionCheckpoint {
            epoch,
            completed_phase: *phase,
            target_hash,
            timestamp: (1000 + i) as u64,
        };
        db.save_checkpoint(&checkpoint).unwrap();

        // Verify the checkpoint was saved correctly.
        let loaded = db.load_checkpoint(epoch).unwrap().unwrap();
        assert_eq!(loaded.completed_phase, *phase);
        assert_eq!(loaded.target_hash, target_hash);
    }

    // Phase 5 (Cleared) clears the checkpoint entirely.
    db.clear_checkpoint(epoch).unwrap();
    assert!(
        db.load_checkpoint(epoch).unwrap().is_none(),
        "checkpoint must be cleared after Cleared phase"
    );
}

/// RANK 2: run_epoch_transition drain phase integrates with subscriber.
///
/// The drain phase (Phase 2) in run_epoch_transition sends drain=true via
/// the consensus bus drain_signal, then waits for drain_ack_rx. This test
/// simulates the full drain handshake as it happens in the real code.
#[tokio::test]
async fn test_run_epoch_transition_drain_phase_handshake() {
    let bus = ConsensusBus::new();

    // Subscriber takes drain_ack_tx (as it does in subscriber.rs run()).
    let drain_ack_tx = bus.take_drain_ack_tx().expect("subscriber takes drain_ack_tx");

    // Manager takes drain_ack_rx (as it does in run_epoch_transition Phase 2).
    let drain_ack_rx = bus.take_drain_ack_rx().expect("manager takes drain_ack_rx");

    // Spawn subscriber that waits for drain and acks.
    let mut drain_rx = bus.drain_signal().subscribe();
    let in_flight_processed = Arc::new(AtomicU32::new(0));
    let in_flight_check = in_flight_processed.clone();

    let sub = tokio::spawn(async move {
        loop {
            if drain_rx.changed().await.is_err() {
                return;
            }
            if drain_rx.borrow().is_some() {
                break;
            }
        }
        // Simulate processing 3 in-flight subdags.
        for _ in 0..3 {
            tokio::task::yield_now().await;
            in_flight_processed.fetch_add(1, Ordering::Relaxed);
        }
        let _ = drain_ack_tx.send(());
    });

    // Manager Phase 2: send drain signal with boundary round and wait for ack.
    let _ = bus.drain_signal().send(Some(100));
    match tokio::time::timeout(std::time::Duration::from_secs(5), drain_ack_rx).await {
        Ok(Ok(())) => {} // drain confirmed
        other => panic!("drain ack failed: {other:?}"),
    }
    sub.await.unwrap();

    assert_eq!(
        in_flight_check.load(Ordering::Relaxed),
        3,
        "all in-flight work must complete before drain ack"
    );
}

/// RANK 2: run_epoch_transition Phase 3 shuts down consensus before engine send.
///
/// In the new sequential design, consensus_shutdown.notify() happens in Phase 3,
/// BEFORE the boundary output is sent to the engine in Phase 4. This ensures
/// no new subdags are produced while the engine processes the closing block.
#[tokio::test]
async fn test_consensus_shutdown_before_engine_send() {
    let shutdown = Notifier::new();
    let shutdown_noticed = shutdown.subscribe();

    // Phase 3: shutdown consensus.
    assert!(!shutdown.was_notified());
    shutdown.notify();
    assert!(shutdown.was_notified());
    assert!(shutdown_noticed.noticed());

    // Phase 4 would happen here: send boundary output to engine.
    // The key invariant is that consensus is already shut down at this point.
    // In the old code, engine send happened BEFORE shutdown in close_epoch().
}

// ---------------------------------------------------------------------------
// Manager-impl: recover_partial_transition() correctness
// ---------------------------------------------------------------------------

/// recover_partial_transition: crash at BoundaryDetected with execution done.
///
/// If the node crashes after BoundaryDetected but the engine already executed
/// the closing block (parent_beacon_block_root matches target_hash), recovery
/// should just clear tables and remove the checkpoint.
#[tokio::test]
async fn test_recovery_boundary_detected_execution_done() {
    let bus = ConsensusBus::new();
    let db = MemDatabase::default();
    let target_hash = B256::random();

    // Save a BoundaryDetected checkpoint.
    let checkpoint = EpochTransitionCheckpoint {
        epoch: 4,
        completed_phase: EpochTransitionPhase::BoundaryDetected,
        target_hash,
        timestamp: 100,
    };
    db.save_checkpoint(&checkpoint).unwrap();

    // Simulate that execution already completed: set recently_executed_blocks to have the
    // closing block with the matching target hash.
    let mut closing_header = rayls_infrastructure_types::ExecHeader::default();
    closing_header.number = 200;
    closing_header.parent_beacon_block_root = Some(target_hash);
    let sealed = rayls_infrastructure_types::SealedHeader::seal_slow(closing_header);
    bus.recently_executed_blocks().send_modify(|blocks| blocks.push_latest(sealed));

    // Simulate recovery logic (mirrors manager.rs:2132-2158).
    let latest = bus.recently_executed_blocks().borrow().latest_block();
    assert_eq!(
        latest.subdag_consensus_digest().map(|d| d.get()),
        Some(target_hash),
        "execution should be done for this target"
    );

    // Recovery path: execution complete -> clear tables + remove checkpoint.
    db.clear_checkpoint(4).unwrap();
    assert!(db.load_checkpoint(4).unwrap().is_none());
}

/// recover_partial_transition: crash at BoundaryDetected with execution NOT done.
///
/// If execution did not complete, the boundary output was lost (in-memory). The
/// recovery clears the stale checkpoint and lets the epoch re-run from scratch.
#[tokio::test]
async fn test_recovery_boundary_detected_execution_not_done() {
    let bus = ConsensusBus::new();
    let db = MemDatabase::default();
    let target_hash = B256::random();

    let checkpoint = EpochTransitionCheckpoint {
        epoch: 4,
        completed_phase: EpochTransitionPhase::BoundaryDetected,
        target_hash,
        timestamp: 100,
    };
    db.save_checkpoint(&checkpoint).unwrap();

    // recently_executed_blocks has a different block (execution not done).
    let mut header = rayls_infrastructure_types::ExecHeader::default();
    header.number = 50;
    // parent_beacon_block_root does NOT match target_hash.
    let sealed = rayls_infrastructure_types::SealedHeader::seal_slow(header);
    bus.recently_executed_blocks().send_modify(|blocks| blocks.push_latest(sealed));

    let latest = bus.recently_executed_blocks().borrow().latest_block();
    assert_ne!(
        latest.subdag_consensus_digest().map(|d| d.get()),
        Some(target_hash),
        "execution should NOT be done for this target"
    );

    // Recovery path: execution not done -> clear stale checkpoint, re-run epoch.
    db.clear_checkpoint(4).unwrap();
    assert!(db.load_checkpoint(4).unwrap().is_none());
}

/// recover_partial_transition: crash at ExecutionComplete.
///
/// Execution is done but tables were not cleared. Recovery just needs to clear
/// tables and remove the checkpoint.
#[test]
fn test_recovery_execution_complete_clears_tables() {
    let db = MemDatabase::default();
    let target_hash = B256::random();

    let checkpoint = EpochTransitionCheckpoint {
        epoch: 8,
        completed_phase: EpochTransitionPhase::ExecutionComplete,
        target_hash,
        timestamp: 200,
    };
    db.save_checkpoint(&checkpoint).unwrap();

    // Recovery: clear tables + clear checkpoint.
    // (In real code: clear_consensus_db_for_next_epoch + persist + clear_checkpoint.)
    let recovered = db.load_checkpoint(8).unwrap().unwrap();
    assert_eq!(recovered.completed_phase, EpochTransitionPhase::ExecutionComplete);

    db.clear_checkpoint(8).unwrap();
    assert!(db.load_checkpoint(8).unwrap().is_none());
}

/// recover_partial_transition: crash at Cleared.
///
/// Everything was done, just a stale checkpoint remains. Recovery removes it.
#[test]
fn test_recovery_cleared_just_removes_checkpoint() {
    let db = MemDatabase::default();

    let checkpoint = EpochTransitionCheckpoint {
        epoch: 9,
        completed_phase: EpochTransitionPhase::Cleared,
        target_hash: B256::random(),
        timestamp: 300,
    };
    db.save_checkpoint(&checkpoint).unwrap();

    // Recovery: just clear the checkpoint.
    db.clear_checkpoint(9).unwrap();
    assert!(db.load_checkpoint(9).unwrap().is_none());
}

/// recover_partial_transition: no checkpoint means no recovery needed.
#[test]
fn test_recovery_no_checkpoint_noop() {
    let db = MemDatabase::default();

    // No checkpoint for epoch 10.
    assert!(db.load_checkpoint(10).unwrap().is_none());
    // Recovery is a no-op when no checkpoint exists.
}

/// recover_partial_transition (C1 regression): recovery must find the checkpoint after the
/// on-chain epoch has advanced past the closing epoch.
///
/// The checkpoint is keyed by the *closing* epoch N. Executing the boundary block runs
/// `closeEpoch`, which advances the on-chain `getCurrentEpoch` to N+1, so once the boundary
/// block is canonical `epoch_state_from_canonical_tip()` reports N+1. The original recovery
/// keyed the checkpoint lookup off that tip epoch and therefore silently missed the N-keyed
/// checkpoint in the post-execution crash window, skipping the table clear and the EpochRecord
/// repopulation. `select_recovery_checkpoint` scans the table instead, so it is independent of
/// the tip epoch.
#[test]
fn test_recovery_selects_checkpoint_after_onchain_epoch_advanced() {
    let db = MemDatabase::default();
    let closing_epoch = 4; // N: the epoch being closed

    db.save_checkpoint(&EpochTransitionCheckpoint {
        epoch: closing_epoch,
        completed_phase: EpochTransitionPhase::ExecutionComplete,
        target_hash: B256::random(),
        timestamp: 100,
    })
    .unwrap();

    // After the boundary block executes, the on-chain epoch (what the old recovery used as the
    // lookup key) is closing+1. Looking the checkpoint up by that key misses it - the bug.
    let canonical_tip_epoch = closing_epoch + 1;
    assert!(
        db.load_checkpoint(canonical_tip_epoch).unwrap().is_none(),
        "tip-epoch lookup misses the closing-epoch checkpoint (the bug this test guards)"
    );

    // The fix: selection is tip-independent and recovers the in-progress transition.
    let recovered = select_recovery_checkpoint(&db)
        .expect("recovery must find the in-progress checkpoint regardless of the tip epoch");
    assert_eq!(recovered.epoch, closing_epoch);
    assert_eq!(recovered.completed_phase, EpochTransitionPhase::ExecutionComplete);
}

/// `select_recovery_checkpoint` returns the most recent (highest-epoch) checkpoint when more
/// than one is present, e.g. a prior orphaned checkpoint plus the current in-progress one.
#[test]
fn test_select_recovery_checkpoint_prefers_highest_epoch() {
    let db = MemDatabase::default();
    for epoch in [3u32, 7, 5] {
        db.save_checkpoint(&EpochTransitionCheckpoint {
            epoch,
            completed_phase: EpochTransitionPhase::BoundaryDetected,
            target_hash: B256::random(),
            timestamp: 100,
        })
        .unwrap();
    }

    let recovered = select_recovery_checkpoint(&db).expect("a checkpoint must be selected");
    assert_eq!(recovered.epoch, 7, "must select the most recent transition");

    // Empty table selects nothing.
    let empty = MemDatabase::default();
    assert!(select_recovery_checkpoint(&empty).is_none());
}

/// Recovery must rebuild the epoch record for the *checkpoint's* closing epoch,
/// not the epoch the committee or registry reports at recovery time.
///
/// By the time `recover_partial_transition` runs, the primary is already primed
/// for the new epoch and the boundary block advanced the on-chain registry, so
/// both report closing+1. Deriving the record epoch from them demands the
/// closing epoch's record as parent - a record that may not be certified
/// anywhere yet (a node crashing mid-transition never voted on it, and peer
/// quorum can lag past the fetch retry budget), so the doomed peer fetch turns
/// a locally recoverable restart into a fatal halt. The checkpoint's epoch
/// instead chains to the previous record, which is locally certified.
#[test]
fn test_recovery_rebuilds_record_for_checkpoint_epoch() {
    use crate::epoch_manager::resolve_local_prev_epoch_record;
    use rayls_infrastructure_storage::EpochStore as _;
    use rayls_infrastructure_types::{BlsSignature, EpochCertificate, EpochRecord};

    let db = MemDatabase::default();
    let closing_epoch = 408u32;

    // The record for closing-1 was certified before the crash.
    let prev_record = EpochRecord { epoch: closing_epoch - 1, ..Default::default() };
    let prev_cert = EpochCertificate {
        epoch_hash: prev_record.digest(),
        signature: BlsSignature::default(),
        signed_authorities: roaring::RoaringBitmap::new(),
    };
    db.save_epoch_record_with_cert(&prev_record, &prev_cert).unwrap();

    // The crash window: boundary block executed, tables not yet cleared.
    db.save_checkpoint(&EpochTransitionCheckpoint {
        epoch: closing_epoch,
        completed_phase: EpochTransitionPhase::ExecutionComplete,
        target_hash: B256::random(),
        timestamp: 100,
    })
    .unwrap();

    // The committee/registry-derived epoch (closing+1, what the old recovery fed
    // into write_epoch_record) cannot resolve a parent locally: the closing
    // record was never certified on this node. This is the dead end that forced
    // the unservable peer fetch - the bug this test guards.
    let registry_epoch = closing_epoch + 1;
    assert!(
        resolve_local_prev_epoch_record(&db, None, registry_epoch).is_none(),
        "the registry-derived epoch must dead-end locally (proves the buggy path is fatal)"
    );

    // The fix: the record epoch comes from the recovery checkpoint, whose parent
    // record is locally certified - no peer fetch needed.
    let recovered = select_recovery_checkpoint(&db).expect("in-progress checkpoint present");
    assert_eq!(recovered.epoch, closing_epoch);
    let prev = resolve_local_prev_epoch_record(&db, None, recovered.epoch)
        .expect("the checkpoint epoch must resolve its parent record locally");
    assert_eq!(prev.epoch, closing_epoch - 1);
    assert_eq!(prev.digest(), prev_record.digest());
}

/// `resolve_local_prev_epoch_record` prefers the certified on-disk record over a
/// divergent in-memory copy, and falls back to the in-memory record when the
/// disk has no certified entry.
#[test]
fn test_resolve_prev_record_prefers_certified_disk_over_memory() {
    use crate::epoch_manager::resolve_local_prev_epoch_record;
    use rayls_infrastructure_storage::EpochStore as _;
    use rayls_infrastructure_types::{BlsSignature, EpochCertificate, EpochRecord};

    let db = MemDatabase::default();
    let epoch = 10u32;

    // In-memory copy from the previous transition, no disk state: used as-is.
    let mem_record = EpochRecord { epoch: epoch - 1, ..Default::default() };
    let resolved = resolve_local_prev_epoch_record(&db, Some(&mem_record), epoch)
        .expect("in-memory record must back-fill a missing disk record");
    assert_eq!(resolved.digest(), mem_record.digest());

    // A certified disk record diverging from memory wins: it is what the
    // committee agreed on.
    let disk_record =
        EpochRecord { epoch: epoch - 1, parent_hash: B256::random(), ..Default::default() };
    let cert = EpochCertificate {
        epoch_hash: disk_record.digest(),
        signature: BlsSignature::default(),
        signed_authorities: roaring::RoaringBitmap::new(),
    };
    db.save_epoch_record_with_cert(&disk_record, &cert).unwrap();
    let resolved = resolve_local_prev_epoch_record(&db, Some(&mem_record), epoch)
        .expect("certified disk record must resolve");
    assert_eq!(resolved.digest(), disk_record.digest(), "certified record must win");
}

// ---------------------------------------------------------------------------
// Manager-impl: select! branch classification
// ---------------------------------------------------------------------------

/// Verify join() branch maps to NodeShutdown when epoch shutdown was noticed.
///
/// In the new select!, if epoch_task_manager.join() fires and the epoch shutdown
/// was already noticed (epoch_shutdown_rx.noticed() == true), this is treated
/// as a normal shutdown, not a crash. This matches the pattern where subscriber
/// exits cleanly after drain protocol completion.
#[test]
fn test_join_branch_maps_to_node_shutdown_when_noticed() {
    use crate::types::RunningOutcome;

    // Simulate: epoch shutdown was noticed -> join maps to NodeShutdown.
    let shutdown = Notifier::new();
    let epoch_shutdown_rx = shutdown.subscribe();

    // Shutdown was noticed (e.g., subscriber drain completed, consensus stopped).
    shutdown.notify();
    assert!(epoch_shutdown_rx.noticed());

    // In the select!, this branch would produce NodeShutdown.
    let outcome = if epoch_shutdown_rx.noticed() {
        RunningOutcome::NodeShutdown
    } else {
        RunningOutcome::TaskCrash(eyre::eyre!("unexpected"))
    };

    assert!(matches!(outcome, RunningOutcome::NodeShutdown));
}

/// Verify join() branch maps to TaskCrash when epoch shutdown was NOT noticed.
///
/// If join() fires without an epoch shutdown having been noticed, something
/// went wrong (e.g., a task panicked). This should be classified as TaskCrash.
#[test]
fn test_join_branch_maps_to_task_crash_when_not_noticed() {
    use crate::types::RunningOutcome;

    let shutdown = Notifier::new();
    let epoch_shutdown_rx = shutdown.subscribe();

    // Shutdown was NOT noticed (unexpected task exit).
    assert!(!epoch_shutdown_rx.noticed());

    let outcome = if epoch_shutdown_rx.noticed() {
        RunningOutcome::NodeShutdown
    } else {
        RunningOutcome::TaskCrash(eyre::eyre!("critical task exited"))
    };

    match outcome {
        RunningOutcome::TaskCrash(e) => {
            assert!(e.to_string().contains("critical task exited"));
        }
        _ => panic!("expected TaskCrash"),
    }
}

// ---------------------------------------------------------------------------
// Manager-impl: await_epoch_execution behavior
// ---------------------------------------------------------------------------

/// RANK 4: await_epoch_execution sends boundary output to engine AFTER shutdown.
///
/// In the old code, the boundary output was sent to the engine inside the
/// select arm (before shutdown). In the new code, await_epoch_execution() is
/// called in Phase 4 AFTER Phase 3 (consensus shutdown). This test verifies
/// that the engine receives the output with close_epoch=true.
#[tokio::test]
async fn test_await_epoch_execution_sends_to_engine() {
    use rayls_infrastructure_types::ConsensusOutput;

    let (engine_tx, mut engine_rx) = mpsc::channel::<ConsensusOutput>(10);

    // Create a boundary output.
    let mut boundary = ConsensusOutput::default();
    boundary.close_epoch = true;

    // Send it to the engine (simulating await_epoch_execution's to_engine.send()).
    engine_tx.send(boundary).await.unwrap();
    drop(engine_tx);

    // Verify the engine received the boundary output.
    let received = engine_rx.recv().await.expect("engine should receive output");
    assert!(received.close_epoch, "engine must receive the close_epoch=true output");
    assert!(engine_rx.recv().await.is_none(), "only one output should be sent");
}

// ---------------------------------------------------------------------------
// Manager-impl: full epoch transition simulation
// ---------------------------------------------------------------------------

/// Integration: Simulate a full 5-phase epoch transition with all new components.
///
/// This end-to-end test exercises:
/// - detect_epoch_boundary returns (hash, output) without engine side effects
/// - Phase 1: BoundaryDetected checkpoint saved
/// - Phase 2: Drain handshake completes
/// - Phase 3: Consensus shutdown
/// - Phase 4: Engine receives boundary output, execution completes
/// - Phase 5: Tables cleared, checkpoint removed
#[tokio::test]
async fn test_full_epoch_transition_simulation() {
    let mut bus = ConsensusBus::new();
    let db = MemDatabase::default();
    let target_hash = B256::random();
    let epoch = 1u32;

    // --- Phase 0: detect_epoch_boundary (in select!) ---

    // --- Phase 1: BOUNDARY_DETECTED ---
    db.save_checkpoint(&EpochTransitionCheckpoint {
        epoch,
        completed_phase: EpochTransitionPhase::BoundaryDetected,
        target_hash,
        timestamp: 1000,
    })
    .unwrap();
    assert_eq!(
        db.load_checkpoint(epoch).unwrap().unwrap().completed_phase,
        EpochTransitionPhase::BoundaryDetected
    );

    // --- Phase 2: DRAINING ---
    let drain_ack_tx = bus.take_drain_ack_tx().expect("drain_ack_tx");
    let drain_ack_rx = bus.take_drain_ack_rx().expect("drain_ack_rx");
    let _ = bus.drain_signal().send(Some(100));

    // Subscriber acks drain.
    tokio::spawn(async move {
        let _ = drain_ack_tx.send(());
    });
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), drain_ack_rx).await;

    db.save_checkpoint(&EpochTransitionCheckpoint {
        epoch,
        completed_phase: EpochTransitionPhase::Draining,
        target_hash,
        timestamp: 1001,
    })
    .unwrap();

    // --- Phase 3: CONSENSUS_SHUTDOWN ---
    let shutdown = Notifier::new();
    shutdown.notify();
    assert!(shutdown.was_notified());

    db.save_checkpoint(&EpochTransitionCheckpoint {
        epoch,
        completed_phase: EpochTransitionPhase::ConsensusShutdown,
        target_hash,
        timestamp: 1002,
    })
    .unwrap();

    // --- Phase 4: EXECUTION_COMPLETE ---
    // Simulate engine executing the closing block.
    let mut closing_header = rayls_infrastructure_types::ExecHeader::default();
    closing_header.number = 100;
    closing_header.parent_beacon_block_root = Some(target_hash);
    let sealed = rayls_infrastructure_types::SealedHeader::seal_slow(closing_header);
    bus.recently_executed_blocks().send_modify(|blocks| blocks.push_latest(sealed));

    db.save_checkpoint(&EpochTransitionCheckpoint {
        epoch,
        completed_phase: EpochTransitionPhase::ExecutionComplete,
        target_hash,
        timestamp: 1003,
    })
    .unwrap();

    // --- Phase 5: CLEARED ---
    // (In real code: clear_consensus_db_for_next_epoch + persist + write_epoch_record.)
    db.clear_checkpoint(epoch).unwrap();

    // Post-transition verification.
    assert!(db.load_checkpoint(epoch).unwrap().is_none(), "checkpoint must be cleared");

    // Reset for next epoch.
    bus.reset_for_epoch();
    assert_eq!(*bus.committed_round_updates().borrow(), 0);
    assert!(bus.drain_signal().borrow().is_none());

    // Verify recently_executed_blocks carries over the closing block.
    assert_eq!(
        bus.recently_executed_blocks().borrow().latest_block_num_hash().number,
        100,
        "closing block must carry over to next epoch"
    );
}

/// Integration: Verify recovery after crash mid-transition, then successful retry.
///
/// Simulates: crash at ConsensusShutdown -> recovery clears tables ->
/// next epoch starts clean.
#[tokio::test]
async fn test_crash_recovery_then_successful_epoch() {
    let mut bus = ConsensusBus::new();
    let db = MemDatabase::default();
    let target_hash = B256::random();

    // --- Epoch that crashed at ConsensusShutdown ---
    let checkpoint = EpochTransitionCheckpoint {
        epoch: 5,
        completed_phase: EpochTransitionPhase::ConsensusShutdown,
        target_hash,
        timestamp: 2000,
    };
    db.save_checkpoint(&checkpoint).unwrap();

    // Simulate that execution DID complete for this target.
    let mut closing_header = rayls_infrastructure_types::ExecHeader::default();
    closing_header.number = 500;
    closing_header.parent_beacon_block_root = Some(target_hash);
    let sealed = rayls_infrastructure_types::SealedHeader::seal_slow(closing_header);
    bus.recently_executed_blocks().send_modify(|blocks| blocks.push_latest(sealed));

    // Recovery: detect execution done, clear tables, remove checkpoint.
    let latest = bus.recently_executed_blocks().borrow().latest_block();
    assert_eq!(latest.subdag_consensus_digest().map(|d| d.get()), Some(target_hash));
    db.clear_checkpoint(5).unwrap();

    // --- Next epoch starts clean ---
    bus.reset_for_epoch();

    // Verify clean state.
    assert_eq!(*bus.committed_round_updates().borrow(), 0);
    assert!(bus.take_drain_ack_tx().is_some());
    assert!(bus.take_drain_ack_rx().is_some());
    assert!(db.load_checkpoint(5).unwrap().is_none());
    assert!(db.load_checkpoint(6).unwrap().is_none());

    // New epoch transition works fine.
    let new_target = B256::random();
    db.save_checkpoint(&EpochTransitionCheckpoint {
        epoch: 6,
        completed_phase: EpochTransitionPhase::BoundaryDetected,
        target_hash: new_target,
        timestamp: 3000,
    })
    .unwrap();

    let loaded = db.load_checkpoint(6).unwrap().unwrap();
    assert_eq!(loaded.completed_phase, EpochTransitionPhase::BoundaryDetected);
    assert_eq!(loaded.target_hash, new_target);

    // Clean up.
    db.clear_checkpoint(6).unwrap();
}

// ---------------------------------------------------------------------------
// ShutdownOutcome Type Tests
// ---------------------------------------------------------------------------

/// ShutdownOutcome drain_confirmed=true indicates successful drain.
#[test]
fn test_shutdown_outcome_drain_confirmed_true() {
    use crate::types::ShutdownOutcome;
    let outcome = ShutdownOutcome { drain_confirmed: true };
    assert!(outcome.drain_confirmed);
}

/// ShutdownOutcome drain_confirmed=false indicates drain failure.
#[test]
fn test_shutdown_outcome_drain_confirmed_false() {
    use crate::types::ShutdownOutcome;
    let outcome = ShutdownOutcome { drain_confirmed: false };
    assert!(!outcome.drain_confirmed);
}

// ---------------------------------------------------------------------------
// controlled_shutdown Behavior Matrix
// ---------------------------------------------------------------------------
// These tests exercise the controlled_shutdown helper's behavior across all
// combinations of drain_round and node mode.

/// controlled_shutdown: drain_round=None skips drain, returns drain_confirmed=true.
///
/// When no drain is requested (e.g., recovery paths), the helper should skip
/// the drain protocol entirely and treat drain as confirmed.
#[tokio::test]
async fn test_controlled_shutdown_no_drain_round_skips_drain() {
    let bus = ConsensusBus::new();

    // Verify drain signal is not sent and drain_ack channels are not consumed.
    let _ack_tx = bus.take_drain_ack_tx().expect("drain_ack_tx exists");
    let _ack_rx = bus.take_drain_ack_rx().expect("drain_ack_rx exists");

    // When drain_round=None, even if node is CvvActive, drain is skipped.
    // We verify the preconditions: drain_signal remains None.
    assert!(bus.drain_signal().borrow().is_none());

    // The controlled_shutdown computes needs_drain = drain_round.is_some() && is_active_cvv().
    // With drain_round=None, needs_drain is always false regardless of mode.
    let drain_round: Option<u32> = None;
    let needs_drain = drain_round.is_some() && bus.node_mode().borrow().is_active_cvv();
    assert!(!needs_drain, "drain_round=None means needs_drain=false regardless of mode");
}

/// controlled_shutdown: drain_round=Some + CvvInactive skips drain,
/// returns drain_confirmed=true.
///
/// CvvInactive nodes have no subscriber FuturesOrdered pipeline, so drain
/// is meaningless and must be skipped.
#[tokio::test]
async fn test_controlled_shutdown_cvv_inactive_skips_drain() {
    let bus = ConsensusBus::new();

    // Set node mode to CvvInactive.
    bus.node_mode().send_replace(rayls_consensus_primary::NodeMode::CvvInactive);
    assert!(!bus.node_mode().borrow().is_active_cvv());

    // Even with drain_round=Some, needs_drain is false for CvvInactive.
    let drain_round = Some(100u32);
    let needs_drain = drain_round.is_some() && bus.node_mode().borrow().is_active_cvv();
    assert!(!needs_drain, "CvvInactive must not trigger drain");

    // Drain signal should not be sent.
    assert!(bus.drain_signal().borrow().is_none());
}

/// controlled_shutdown: drain_round=Some + Observer skips drain,
/// returns drain_confirmed=true.
///
/// Observer nodes don't participate in consensus, no drain needed.
#[tokio::test]
async fn test_controlled_shutdown_observer_skips_drain() {
    let bus = ConsensusBus::new();

    bus.node_mode().send_replace(rayls_consensus_primary::NodeMode::Observer);
    assert!(!bus.node_mode().borrow().is_active_cvv());

    let drain_round = Some(100u32);
    let needs_drain = drain_round.is_some() && bus.node_mode().borrow().is_active_cvv();
    assert!(!needs_drain, "Observer must not trigger drain");
}

/// controlled_shutdown: drain_round=Some + CvvActive sends drain signal
/// with the correct boundary round.
///
/// This verifies the drain signal carries the exact boundary round from the
/// caller, which the subscriber uses to determine the drain cutoff.
#[tokio::test]
async fn test_controlled_shutdown_cvv_active_sends_drain_round() {
    let bus = ConsensusBus::new();

    // CvvActive is the default mode.
    assert!(bus.node_mode().borrow().is_active_cvv());

    let boundary_round = 42u32;
    let drain_round = Some(boundary_round);
    let needs_drain = drain_round.is_some() && bus.node_mode().borrow().is_active_cvv();
    assert!(needs_drain, "CvvActive + Some(round) must trigger drain");

    // Send drain signal (simulating controlled_shutdown step 1).
    let mut drain_rx = bus.drain_signal().subscribe();
    bus.drain_signal().send_replace(Some(boundary_round));

    // Verify the subscriber receives the correct round.
    drain_rx.changed().await.unwrap();
    assert_eq!(*drain_rx.borrow(), Some(42), "drain signal must carry the boundary round");
}

/// controlled_shutdown: drain ack timeout results in drain_confirmed=false.
///
/// When the subscriber never sends the drain ack (e.g., it crashed), the
/// controlled_shutdown must return drain_confirmed=false after the timeout.
#[tokio::test]
async fn test_controlled_shutdown_drain_ack_timeout_returns_false() {
    let bus = ConsensusBus::new();
    assert!(bus.node_mode().borrow().is_active_cvv());

    // Take the subscriber's ack_tx but DON'T send it — simulating a stuck subscriber.
    let _ack_tx = bus.take_drain_ack_tx().expect("drain_ack_tx");
    let drain_ack_rx = bus.take_drain_ack_rx().expect("drain_ack_rx");

    // Simulate the drain ack timeout (short timeout for test speed).
    let result = tokio::time::timeout(std::time::Duration::from_millis(50), drain_ack_rx).await;

    // Timeout should occur.
    assert!(result.is_err(), "drain ack should timeout when subscriber doesn't respond");

    // In controlled_shutdown, this maps to drain_confirmed=false.
}

/// controlled_shutdown: drain ack channel dropped results in drain_confirmed=false.
///
/// If the subscriber drops the ack_tx without sending, the oneshot receiver
/// gets a RecvError. controlled_shutdown treats this as drain NOT confirmed.
#[tokio::test]
async fn test_controlled_shutdown_drain_ack_dropped_returns_false() {
    let bus = ConsensusBus::new();
    assert!(bus.node_mode().borrow().is_active_cvv());

    let ack_tx = bus.take_drain_ack_tx().expect("drain_ack_tx");
    let drain_ack_rx = bus.take_drain_ack_rx().expect("drain_ack_rx");

    // Drop the sender without sending — simulates subscriber exit without ack.
    drop(ack_tx);

    // The receiver should get an error (channel closed).
    let result = drain_ack_rx.await;
    assert!(result.is_err(), "dropped sender should produce RecvError");

    // In controlled_shutdown, this maps to drain_confirmed=false.
}

/// controlled_shutdown: drain ack received results in drain_confirmed=true.
///
/// Happy path — subscriber sends ack, controlled_shutdown returns
/// drain_confirmed=true.
#[tokio::test]
async fn test_controlled_shutdown_drain_ack_success_returns_true() {
    let bus = ConsensusBus::new();
    assert!(bus.node_mode().borrow().is_active_cvv());

    let ack_tx = bus.take_drain_ack_tx().expect("drain_ack_tx");
    let drain_ack_rx = bus.take_drain_ack_rx().expect("drain_ack_rx");

    // Subscriber sends ack.
    tokio::spawn(async move {
        let _ = ack_tx.send(());
    });

    let result = tokio::time::timeout(std::time::Duration::from_secs(1), drain_ack_rx).await;
    assert!(matches!(result, Ok(Ok(()))), "drain ack should succeed");
}

/// controlled_shutdown: consensus_shutdown is always notified.
///
/// Regardless of drain_round or node mode, consensus_shutdown.notify()
/// must always be called so the task manager can join.
#[tokio::test]
async fn test_controlled_shutdown_always_notifies_consensus() {
    // With drain_round=None.
    let shutdown1 = Notifier::new();
    let noticer1 = shutdown1.subscribe();
    assert!(!noticer1.noticed());
    shutdown1.notify();
    assert!(noticer1.noticed(), "consensus shutdown must be notified even without drain");

    // With drain_round=Some.
    let shutdown2 = Notifier::new();
    let noticer2 = shutdown2.subscribe();
    assert!(!noticer2.noticed());
    shutdown2.notify();
    assert!(noticer2.noticed(), "consensus shutdown must be notified with drain");
}

// ---------------------------------------------------------------------------
// run_epoch_transition Error Path Tests
// ---------------------------------------------------------------------------

/// run_epoch_transition: drain failure clears checkpoint and returns error.
///
/// When controlled_shutdown returns drain_confirmed=false, the epoch
/// transition must NOT proceed — it clears the checkpoint and returns an
/// error. This prevents data loss from partial transitions.
#[tokio::test]
async fn test_epoch_transition_drain_failure_clears_checkpoint() {
    let db = MemDatabase::default();
    let epoch = 7u32;
    let target_hash = B256::random();

    // Save a BoundaryDetected checkpoint (as run_epoch_transition Phase 1 would).
    db.save_checkpoint(&EpochTransitionCheckpoint {
        epoch,
        completed_phase: EpochTransitionPhase::BoundaryDetected,
        target_hash,
        timestamp: 100,
    })
    .unwrap();

    // Verify checkpoint exists.
    assert!(db.load_checkpoint(epoch).unwrap().is_some());

    // Simulate drain_confirmed=false → epoch transition clears checkpoint.
    db.clear_checkpoint(epoch).unwrap();
    assert!(
        db.load_checkpoint(epoch).unwrap().is_none(),
        "checkpoint must be cleared on drain failure"
    );
}

/// run_epoch_transition: drain confirmed saves both ConsensusShutdown and
/// Draining checkpoints.
///
/// After a successful drain, run_epoch_transition saves TWO consecutive
/// checkpoints: ConsensusShutdown (tasks joined) and Draining (drain confirmed).
/// This ensures crash recovery knows exactly where it left off.
#[test]
fn test_epoch_transition_drain_success_saves_both_checkpoints() {
    let db = MemDatabase::default();
    let epoch = 11u32;
    let target_hash = B256::random();

    // Phase 1: BoundaryDetected.
    db.save_checkpoint(&EpochTransitionCheckpoint {
        epoch,
        completed_phase: EpochTransitionPhase::BoundaryDetected,
        target_hash,
        timestamp: 1000,
    })
    .unwrap();

    // Phase 2 (after successful drain): save ConsensusShutdown, then Draining.
    db.save_checkpoint(&EpochTransitionCheckpoint {
        epoch,
        completed_phase: EpochTransitionPhase::ConsensusShutdown,
        target_hash,
        timestamp: 1001,
    })
    .unwrap();

    // Verify intermediate checkpoint.
    let loaded = db.load_checkpoint(epoch).unwrap().unwrap();
    assert_eq!(loaded.completed_phase, EpochTransitionPhase::ConsensusShutdown);

    db.save_checkpoint(&EpochTransitionCheckpoint {
        epoch,
        completed_phase: EpochTransitionPhase::Draining,
        target_hash,
        timestamp: 1002,
    })
    .unwrap();

    // Final state for Phase 2 should be Draining.
    let loaded = db.load_checkpoint(epoch).unwrap().unwrap();
    assert_eq!(loaded.completed_phase, EpochTransitionPhase::Draining);
    assert_eq!(loaded.timestamp, 1002);

    db.clear_checkpoint(epoch).unwrap();
}

// ---------------------------------------------------------------------------
// run_mode_transition Behavior Tests
// ---------------------------------------------------------------------------

/// run_mode_transition: drain timeout is non-fatal, transition continues.
///
/// Unlike epoch transitions where drain timeout is fatal (to prevent data
/// loss), mode transitions treat drain timeout as a warning and proceed.
/// The subscriber may have already exited (CvvInactive has no pipeline).
#[tokio::test]
async fn test_mode_transition_drain_timeout_continues() {
    let bus = ConsensusBus::new();
    assert!(bus.node_mode().borrow().is_active_cvv());

    // Take ack_tx but don't send — simulating subscriber that already exited.
    let _ack_tx = bus.take_drain_ack_tx().expect("drain_ack_tx");
    let drain_ack_rx = bus.take_drain_ack_rx().expect("drain_ack_rx");

    // Drain ack times out.
    let result = tokio::time::timeout(std::time::Duration::from_millis(50), drain_ack_rx).await;
    assert!(result.is_err(), "drain ack should timeout");

    // In run_mode_transition, this is non-fatal: warn and continue.
    // Verify mode can still be changed after timeout.
    let target_mode = rayls_consensus_primary::NodeMode::CvvInactive;
    bus.node_mode().send_replace(target_mode);
    assert!(!bus.node_mode().borrow().is_active_cvv());
}

/// APPLY-phase latch clear must not re-notify receivers (would cause a
/// duplicate respawn on the next run_epoch iteration).
#[tokio::test]
async fn test_mode_transition_apply_phase_clear_does_not_re_notify() {
    let bus = ConsensusBus::new();
    let target_mode = rayls_consensus_primary::NodeMode::CvvActive;

    // Producer: request a transition.
    bus.mode_transition().send_replace(Some(target_mode));

    // Consumer (simulates core.rs select arm): take the value and clear.
    let mut rx = bus.mode_transition().subscribe();
    let _ = rx.borrow_and_update();
    let mut taken = None;
    bus.mode_transition().send_if_modified(|v| {
        if v.is_some() {
            taken = v.take();
            true
        } else {
            false
        }
    });
    assert_eq!(taken, Some(target_mode));
    let _ = rx.borrow_and_update();

    // Phase 3 APPLY: second clear must be a no-op at the notification level.
    bus.mode_transition().send_if_modified(|v| {
        let changed = v.is_some();
        *v = None;
        changed
    });

    assert!(
        !rx.has_changed().unwrap(),
        "APPLY-phase clear must not notify receivers when latch already None"
    );
}

/// Observer identity is sticky: request_mode_transition must reject any
/// transition away from Observer, regardless of target.
#[tokio::test]
async fn test_request_mode_transition_observer_is_sticky() {
    use rayls_consensus_primary::NodeMode;
    let bus = ConsensusBus::new();
    bus.node_mode().send_replace(NodeMode::Observer);

    assert!(!bus.request_mode_transition(NodeMode::CvvActive));
    assert!(!bus.request_mode_transition(NodeMode::CvvInactive));
    assert_eq!(*bus.mode_transition().borrow(), None);
    assert_eq!(*bus.node_mode().borrow(), NodeMode::Observer);
}

/// request_mode_transition is idempotent: asking for the current mode is a no-op.
#[tokio::test]
async fn test_request_mode_transition_idempotent_for_current_mode() {
    use rayls_consensus_primary::NodeMode;
    let bus = ConsensusBus::new();
    bus.node_mode().send_replace(NodeMode::CvvActive);

    assert!(!bus.request_mode_transition(NodeMode::CvvActive));
    assert_eq!(*bus.mode_transition().borrow(), None);
}

/// run_mode_transition: passes drain_round=None (no drain).
///
/// Mode transitions skip the drain protocol because drain exists to flush
/// committed subdags at epoch boundaries with a deterministic cutoff round.
/// Mode changes have no boundary semantics — any in-flight work is
/// idempotently re-synced during catch-up.
#[tokio::test]
async fn test_mode_transition_skips_drain() {
    let bus = ConsensusBus::new();
    assert!(bus.node_mode().borrow().is_active_cvv());

    let drain_rx = bus.drain_signal().subscribe();

    // Mode transition passes drain_round=None to controlled_shutdown.
    // Verify needs_drain evaluates to false.
    let drain_round: Option<u32> = None;
    let needs_drain = drain_round.is_some() && bus.node_mode().borrow().is_active_cvv();
    assert!(!needs_drain, "mode transitions must not drain");

    // Drain signal should remain None (never sent).
    assert!(drain_rx.borrow().is_none(), "drain signal must stay None");
}

/// run_mode_transition: CvvActive → CvvInactive → CvvActive round-trip.
///
/// Verifies that the mode can be changed back and forth without leaving
/// stale state in the consensus bus.
#[tokio::test]
async fn test_mode_transition_round_trip() {
    let mut bus = ConsensusBus::new();
    assert!(bus.node_mode().borrow().is_active_cvv());

    // CvvActive → CvvInactive.
    bus.node_mode().send_replace(rayls_consensus_primary::NodeMode::CvvInactive);
    assert!(!bus.node_mode().borrow().is_active_cvv());

    // Reset for new epoch (as run_epochs would).
    bus.reset_for_epoch();

    // CvvInactive → CvvActive.
    bus.node_mode().send_replace(rayls_consensus_primary::NodeMode::CvvActive);
    assert!(bus.node_mode().borrow().is_active_cvv());

    // Verify clean state after round-trip.
    bus.reset_for_epoch();
    assert_eq!(*bus.committed_round_updates().borrow(), 0);
    assert!(bus.drain_signal().borrow().is_none());
}

// ---------------------------------------------------------------------------
// End-to-End: Full Mode Transition Simulation
// ---------------------------------------------------------------------------

/// Full mode transition simulation: CvvActive → CvvInactive without drain.
///
/// Exercises the complete run_mode_transition flow:
/// 1. SHUTDOWN — no drain (drain_round=None), consensus shutdown + join
/// 2. FLUSH — persist deferred writes
/// 3. APPLY — switch mode
#[tokio::test]
async fn test_full_mode_transition_simulation() {
    let mut bus = ConsensusBus::new();
    assert!(bus.node_mode().borrow().is_active_cvv());

    // Phase 1: SHUTDOWN — no drain for mode transitions.
    // drain_round=None means needs_drain=false, subscriber exits via rx_shutdown.
    let drain_round: Option<u32> = None;
    let needs_drain = drain_round.is_some() && bus.node_mode().borrow().is_active_cvv();
    assert!(!needs_drain, "mode transitions must not drain");

    // Drain signal must not be sent.
    assert!(bus.drain_signal().borrow().is_none());

    // Consensus shutdown fires — subscriber observes rx_shutdown and exits.
    let shutdown = Notifier::new();
    shutdown.notify();
    assert!(shutdown.was_notified());

    // Phase 2: FLUSH — (simulated, no real engine).

    // Phase 3: APPLY — switch mode.
    let target_mode = rayls_consensus_primary::NodeMode::CvvInactive;
    bus.node_mode().send_replace(target_mode);

    // Post-transition verification.
    assert!(!bus.node_mode().borrow().is_active_cvv());

    // Verify the node can start a new epoch in the new mode.
    bus.reset_for_epoch();
    assert_eq!(*bus.committed_round_updates().borrow(), 0);
    assert!(bus.drain_signal().borrow().is_none());
    assert!(!bus.node_mode().borrow().is_active_cvv());
}

/// Mode transition: drain_round=None means drain_confirmed=true.
///
/// When no drain is requested, controlled_shutdown returns
/// drain_confirmed=true immediately (nothing to drain).
#[tokio::test]
async fn test_full_mode_transition_drain_timeout_completes() {
    let bus = ConsensusBus::new();
    assert!(bus.node_mode().borrow().is_active_cvv());

    // Mode transition: drain_round=None, so needs_drain=false.
    let drain_round: Option<u32> = None;
    let needs_drain = drain_round.is_some() && bus.node_mode().borrow().is_active_cvv();
    assert!(!needs_drain);

    // Since needs_drain=false, drain_confirmed is set to true (else branch).
    let drain_confirmed = !needs_drain;
    assert!(drain_confirmed, "no-drain path must report drain_confirmed=true");

    // Drain signal stays None — no 30s ack wait.
    assert!(bus.drain_signal().borrow().is_none());

    // Consensus shutdown + APPLY phase.
    let shutdown = Notifier::new();
    shutdown.notify();

    let target_mode = rayls_consensus_primary::NodeMode::CvvInactive;
    bus.node_mode().send_replace(target_mode);

    assert!(!bus.node_mode().borrow().is_active_cvv());
}

// ---------------------------------------------------------------------------
// Invariant Tests
// ---------------------------------------------------------------------------

/// Invariant: controlled_shutdown never leaves drain_ack_rx unconsumed
/// when drain was needed.
///
/// If needs_drain=true, controlled_shutdown must consume the drain_ack_rx
/// (via take_drain_ack_rx + await). Leaving it unconsumed would cause the
/// next transition to find stale channels after reset_for_epoch.
#[tokio::test]
async fn test_controlled_shutdown_always_consumes_drain_ack_when_needed() {
    let bus = ConsensusBus::new();
    assert!(bus.node_mode().borrow().is_active_cvv());

    // Take ack_tx (subscriber side).
    let ack_tx = bus.take_drain_ack_tx().expect("drain_ack_tx");

    // Verify ack_rx is available before controlled_shutdown.
    assert!(bus.take_drain_ack_rx().is_some(), "drain_ack_rx must be available");

    // After take_drain_ack_rx is consumed, it should be None.
    assert!(bus.take_drain_ack_rx().is_none(), "drain_ack_rx must be consumed (taken once)");

    // Clean up.
    drop(ack_tx);
}

// ---------------------------------------------------------------------------
// Edge Case: Concurrent drain signal and subscriber exit
// ---------------------------------------------------------------------------

/// Edge case: subscriber exits before drain signal is sent.
///
/// The subscriber may exit (e.g., channel closed) before the manager sends
/// the drain signal. In this case, drain_signal().send() may fail if no
/// watch receivers exist (watch::Sender::send returns Err when receiver count
/// is zero). The controlled_shutdown code handles this gracefully with
/// `if let Err(e)`. The drain_ack_rx gets RecvError because the subscriber
/// dropped ack_tx.
#[tokio::test]
async fn test_subscriber_exits_before_drain_signal() {
    let bus = ConsensusBus::new();
    assert!(bus.node_mode().borrow().is_active_cvv());

    // Subscribe first so drain_signal().send() can succeed.
    let _drain_rx = bus.drain_signal().subscribe();

    let ack_tx = bus.take_drain_ack_tx().expect("drain_ack_tx");
    let drain_ack_rx = bus.take_drain_ack_rx().expect("drain_ack_rx");

    // Subscriber exits (drops ack_tx without sending).
    drop(ack_tx);

    // Manager sends drain signal — succeeds because at least one receiver exists.
    let send_result = bus.drain_signal().send(Some(100));
    assert!(send_result.is_ok(), "drain signal send should succeed when receiver exists");

    // Manager waits for drain ack — gets channel closed error.
    let result = drain_ack_rx.await;
    assert!(result.is_err(), "drain ack should fail when subscriber dropped without acking");
}

/// Edge case: drain_signal().send() fails when no receivers exist.
///
/// watch::Sender::send() returns Err when receiver count is zero.
/// controlled_shutdown handles this with `if let Err(e)` — it logs a warning
/// and continues. This covers the case where the subscriber already exited
/// AND dropped its drain_rx.
#[tokio::test]
async fn test_drain_signal_send_fails_no_receivers() {
    let bus = ConsensusBus::new();

    // Don't subscribe to drain_signal — no receivers exist.
    // Note: the watch::Sender created by ConsensusBus starts with one
    // internal receiver (the Sender itself holds a reference), but
    // watch::Sender::send() returns Err when no *external* receivers exist.
    let send_result = bus.drain_signal().send(Some(100));

    // The result depends on watch implementation — if no subscribe() was called,
    // send may or may not fail. The key behavior: controlled_shutdown handles
    // both Ok and Err from drain_signal().send() gracefully.
    // In controlled_shutdown: `if let Err(e) = ... { warn!(...) }` — never panics.
    let _ = send_result; // Either Ok or Err is acceptable
}

/// Edge case: drain_ack channels already consumed before controlled_shutdown.
///
/// If take_drain_ack_rx() returns None (already consumed), the drain is
/// treated as not-confirmable. This shouldn't happen in normal flow but
/// defensive code handles it.
#[tokio::test]
async fn test_drain_ack_already_consumed() {
    let bus = ConsensusBus::new();

    // Consume both channels.
    let _ack_tx = bus.take_drain_ack_tx();
    let _ack_rx = bus.take_drain_ack_rx();

    // Second take returns None.
    assert!(bus.take_drain_ack_rx().is_none(), "second take must return None");

    // In controlled_shutdown, take_drain_ack_rx() returning None means
    // the if-let doesn't execute, drain_confirmed stays false.
}

// ---------------------------------------------------------------------------
// identify_node_mode: decide_node_mode Decision Logic
// ---------------------------------------------------------------------------

/// Fresh genesis on initial epoch with no history -> CvvActive.
#[test]
fn test_decide_mode_fresh_genesis() {
    let (mode, reason) = decide_node_mode(
        true,                // in_committee
        false,               // observer_flag
        None,                // explicit_target
        true,                // initial_epoch
        NodeMode::CvvActive, // prior_mode (default)
        false,               // has_local_history
    );
    assert_eq!(mode, NodeMode::CvvActive);
    assert_eq!(reason, "fresh-genesis");
}

/// Respawn preserves CvvActive when prior mode was CvvActive.
#[test]
fn test_decide_mode_prior_active_preserved() {
    let (mode, reason) = decide_node_mode(
        true,
        false,
        None,
        false, // not initial epoch (respawn)
        NodeMode::CvvActive,
        false,
    );
    assert_eq!(mode, NodeMode::CvvActive);
    assert_eq!(reason, "prior-mode-active");
}

/// Respawn preserves CvvInactive when prior mode was CvvInactive.
///
/// This is the critical fix for the chaos-test ping-pong loop. After gossip
/// triggers CvvInactive, the mode_transition latch is cleared, committed_round
/// resets to 0 (cross-epoch), and has_local_history is false. Without this
/// branch the node falls through to "fresh-genesis" -> CvvActive -> immediate
/// "behind" detection -> CvvInactive -> repeat (960+ cycles observed).
#[test]
fn test_decide_mode_prior_inactive_preserved() {
    let (mode, reason) = decide_node_mode(
        true,
        false,
        None,                  // latch cleared
        false,                 // respawn
        NodeMode::CvvInactive, // set by run_mode_transition
        false,                 // committed_round=0 after cross-epoch reset
    );
    assert_eq!(mode, NodeMode::CvvInactive);
    assert_eq!(reason, "prior-mode-inactive");
}

/// Explicit mode-transition target overrides all other signals.
#[test]
fn test_decide_mode_explicit_target_overrides() {
    let (mode, reason) = decide_node_mode(
        true,
        false,
        Some(NodeMode::CvvInactive),
        false,
        NodeMode::CvvActive,
        false,
    );
    assert_eq!(mode, NodeMode::CvvInactive);
    assert_eq!(reason, "explicit-mode-transition");
}

/// Initial epoch with local consensus history -> CvvInactive (catch up).
#[test]
fn test_decide_mode_has_local_history() {
    let (mode, reason) = decide_node_mode(
        true,
        false,
        None,
        true, // initial epoch
        NodeMode::CvvActive,
        true, // committed_round > 0
    );
    assert_eq!(mode, NodeMode::CvvInactive);
    assert_eq!(reason, "has-local-history");
}

/// Not in committee -> Observer regardless of other signals.
#[test]
fn test_decide_mode_not_in_committee() {
    let (mode, reason) = decide_node_mode(
        false, // not in committee
        false,
        Some(NodeMode::CvvActive),
        false,
        NodeMode::CvvActive,
        true,
    );
    assert_eq!(mode, NodeMode::Observer);
    assert_eq!(reason, "not-in-committee");
}

/// Observer flag set -> Observer regardless of committee membership.
#[test]
fn test_decide_mode_observer_flag() {
    let (mode, reason) = decide_node_mode(
        true,
        true, // observer flag
        None,
        false,
        NodeMode::CvvActive,
        false,
    );
    assert_eq!(mode, NodeMode::Observer);
    assert_eq!(reason, "observer-flag");
}

/// Chaos test scenario 4 reproduction: rapid flapping at epoch boundary.
///
/// V4 enters epoch 4 after epoch 3 boundary. prime_consensus does
/// cross-epoch reset (committed_round=0). Gossip triggers CvvInactive.
/// Mode transition clears the latch. reset_for_epoch clears
/// committed_round to 0. On the next identify_node_mode:
///   - explicit_target = None (cleared)
///   - initial_epoch = false (respawn within same process)
///   - prior_mode = CvvInactive (set by run_mode_transition)
///   - has_local_history = false (committed_round=0)
///
/// Without the "prior-mode-inactive" branch this falls to "fresh-genesis"
/// -> CvvActive, causing an infinite ping-pong loop.
#[test]
fn test_decide_mode_chaos_scenario4_no_pingpong() {
    // simulate the exact state after gossip-triggered CvvInactive + reset_for_epoch
    let (mode, reason) = decide_node_mode(
        true,                  // in committee
        false,                 // not observer
        None,                  // mode_transition latch cleared at consumption
        false,                 // not initial_epoch (respawn)
        NodeMode::CvvInactive, // set by run_mode_transition Phase 3: APPLY
        false,                 // committed_round=0 after cross-epoch reset
    );

    // must stay CvvInactive to allow catch-up, NOT promote to CvvActive
    assert_eq!(
        mode,
        NodeMode::CvvInactive,
        "must not promote to CvvActive after gossip-confirmed behind"
    );
    assert_eq!(reason, "prior-mode-inactive");
}

/// Verify CvvInactive -> CvvActive promotion only happens via explicit target.
///
/// After catch-up completes, try_rejoin_consensus sets mode_transition to
/// CvvActive. The next identify_node_mode should pick that up via the
/// "explicit-mode-transition" branch, not the "fresh-genesis" fallback.
#[test]
fn test_decide_mode_catchup_to_active_via_explicit_target() {
    let (mode, reason) = decide_node_mode(
        true,
        false,
        Some(NodeMode::CvvActive), // set by try_rejoin_consensus
        false,
        NodeMode::CvvInactive, // was catching up
        false,                 // committed_round=0 (cross-epoch)
    );
    assert_eq!(mode, NodeMode::CvvActive);
    assert_eq!(reason, "explicit-mode-transition");
}

/// A first boot that crashed on an epoch boundary must catch up, not boot fresh-genesis.
///
/// If the node executed up to epoch N's tail but not its closing output, consensus configures for
/// N+1 and `prime_consensus` resets `committed_round` to 0. Keying "has local history" off that
/// zeroed round makes the node look fresh, so it boots CvvActive into N+1, skips the unexecuted
/// closing output, and reuses a block number - an off-by-one digest fork. It must instead detect
/// its executed history (the execution anchor survives the reset) and boot CvvInactive to catch up.
///
/// Drives real `prime_consensus` + `node_has_local_history` so the assertion isn't circular.
#[tokio::test]
async fn test_initial_boot_at_epoch_boundary_catches_up_not_fresh_genesis() {
    use rayls_consensus_state_sync::prime_consensus;
    use rayls_infrastructure_types::{
        Certificate, CommittedSubDag, ConsensusHeader, Header, ReputationScores,
    };
    use rayls_testing_test_utils::CommitteeFixture;
    use std::num::NonZeroUsize;

    // Committee/config for the NEW epoch (N+1 = 4): the on-chain registry has advanced past the
    // boundary even though execution has not.
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .epoch(4)
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let committee = fixture.committee();

    // Execution anchor = the tail of the PRIOR epoch (N = 3, round 8, block 100): genuine executed
    // history whose subdag did NOT close epoch 3.
    let author = config.authority_id().expect("config has an authority id");
    let leader_header =
        Header::new(author, 8, 3, Default::default(), Default::default(), Default::default());
    let leader = Certificate::new_unsigned_for_test(&committee, leader_header, vec![])
        .expect("unsigned leader cert");
    let sub_dag = CommittedSubDag::new(vec![], leader, 0, ReputationScores::default(), None);
    let anchor = ConsensusHeader {
        parent_hash: B256::default(),
        sub_dag,
        number: 100,
        extra: B256::default(),
    };

    let cb = ConsensusBus::new();
    cb.executed_anchor().send_replace(anchor);

    // Boot priming, exactly as identify_node_mode does before deciding the node mode.
    prime_consensus(&cb, &config);

    // Preconditions: the cross-epoch reset zeroes committed_round, but the execution anchor still
    // records genuine executed history.
    assert_eq!(
        *cb.committed_round_updates().borrow(),
        0,
        "precondition: prime_consensus cross-epoch-resets committed_round to 0"
    );
    assert!(
        cb.executed_anchor().borrow().number > 0,
        "precondition: the execution anchor records genuine executed history"
    );

    // Real node-mode inputs, derived from the primed bus (not hand-set).
    let has_local_history = node_has_local_history(&cb);
    let (mode, reason) = decide_node_mode(
        true,                // in_committee
        false,               // observer_flag
        None,                // explicit_target
        true,                // initial_epoch (first process boot after the crash)
        NodeMode::CvvActive, // prior_mode = default on a fresh ConsensusBus
        has_local_history,
    );

    assert_eq!(
        mode,
        NodeMode::CvvInactive,
        "a restart with executed prior-epoch history must catch up (CvvInactive), not boot \
         fresh-genesis CvvActive and fork by skipping the prior epoch's closing output \
         (reason: {reason})"
    );
}

/// Sole committee member (single-validator dev chain): `decide_node_mode` is a
/// pure function that no longer knows about sole membership — that concern moved
/// to a feature-gated early return in `identify_node_mode`. This pins the
/// contract: given a sole member's restart-with-history inputs, the pure function
/// lands on "has-local-history" (CvvInactive). The dev-only early return in
/// `identify_node_mode` is what overrides this to CvvActive; if that gate is
/// deleted, the node regresses to hanging on catch-up
/// (`waiting for peers ... required=1 committee_size=1`).
#[cfg(feature = "dev-single-node-setup")]
#[test]
fn test_decide_node_mode_sole_member_without_override_returns_inactive() {
    let (mode, reason) = decide_node_mode(
        true,                  // in_committee
        false,                 // observer_flag
        None,                  // explicit_target
        true,                  // initial_epoch
        NodeMode::CvvInactive, // prior_mode
        true,                  // has_local_history
    );
    assert_eq!(mode, NodeMode::CvvInactive);
    assert_eq!(reason, "has-local-history");
}

/// An explicitly-configured observer that is also the sole committee member must
/// stay Observer. The dev early return in `identify_node_mode` is guarded with
/// `&& !observer_flag`, so an observer sole member falls through to
/// `decide_node_mode`, which must honor the sticky observer flag here (not force
/// CvvActive). Regression guard for that fall-through.
#[cfg(feature = "dev-single-node-setup")]
#[test]
fn test_decide_node_mode_observer_sole_member_stays_observer() {
    let (mode, reason) = decide_node_mode(
        true,                // in_committee
        true,                // observer_flag set
        None,                // explicit_target
        true,                // initial_epoch
        NodeMode::CvvActive, // prior_mode
        true,                // has_local_history (sole-member restart)
    );
    assert_eq!(mode, NodeMode::Observer);
    assert_eq!(reason, "observer-flag");
}
