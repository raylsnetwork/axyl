//! Typestate and seal-ahead-budget tests for the batch-build pipeline, run directly against the
//! transition machine with no pool, worker, or async runtime.

use rayls_batch_builder::{
    pipeline::{calculate_in_flight_depth, BatchPipeline, Clean, EpochPhase, GateRejectionReason},
    watermark::OwnWatermarkReceiver,
    BOUNDARY_QUIESCE_WINDOW_SECS, MAX_SEAL_AHEAD,
};

#[test]
fn epoch_phase_tracks_distance_to_the_boundary() {
    let boundary = 1_000;
    assert_eq!(
        EpochPhase::evaluate(boundary, boundary - BOUNDARY_QUIESCE_WINDOW_SECS - 1),
        EpochPhase::Active
    );
    assert_eq!(
        EpochPhase::evaluate(boundary, boundary - BOUNDARY_QUIESCE_WINDOW_SECS),
        EpochPhase::Quiescing
    );
    assert_eq!(EpochPhase::evaluate(boundary, boundary), EpochPhase::Closed);
    assert_eq!(EpochPhase::evaluate(boundary, boundary + 1), EpochPhase::Closed);
}

#[test]
fn max_allowed_ahead_collapses_toward_the_boundary() {
    assert_eq!(EpochPhase::Active.max_allowed_ahead(), MAX_SEAL_AHEAD);
    assert_eq!(EpochPhase::Quiescing.max_allowed_ahead(), 1);
    assert_eq!(EpochPhase::Closed.max_allowed_ahead(), 0);
}

#[test]
fn in_flight_depth_is_sealed_minus_executed() {
    // nothing sealed yet leaves no depth, whatever the watermark reads
    assert_eq!(calculate_in_flight_depth(None, 5, None), 0);
    // ten sealed, six executed leaves four outstanding
    assert_eq!(calculate_in_flight_depth(Some(10), 5, Some(6)), 4);
    // with no watermark the executed floor falls back to start_seq - 1 (= 4)
    assert_eq!(calculate_in_flight_depth(Some(10), 5, None), 6);
    // execution running ahead of the seal cannot drive the depth negative
    assert_eq!(calculate_in_flight_depth(Some(3), 1, Some(9)), 0);
}

#[test]
fn can_start_build_enforces_the_seal_ahead_budget() {
    let far_boundary = u64::MAX;
    // highest sealed 10, resume seq 5, canonical tip far from the boundary (Active budget = 4)
    let accumulating = BatchPipeline::<Clean>::new(Some(10), 5, 0).on_event();

    // executed 6 leaves depth 4, at the budget, so a new build is refused
    assert_eq!(
        accumulating.can_start_build(Some(6), far_boundary),
        Err(GateRejectionReason::BudgetExhausted)
    );
    // executed 9 leaves depth 1, under the budget, so a build may start
    assert_eq!(accumulating.can_start_build(Some(9), far_boundary), Ok(()));
}

#[test]
fn can_start_build_refuses_past_the_boundary() {
    let boundary = 100;
    // canonical tip at the boundary closes the epoch regardless of budget
    let accumulating = BatchPipeline::<Clean>::new(None, 1, boundary).on_event();
    assert_eq!(
        accumulating.can_start_build(None, boundary),
        Err(GateRejectionReason::EpochBoundaryReached)
    );
}

#[test]
fn can_start_build_throttles_to_one_batch_until_the_epochs_first_batch_executes() {
    // The lagged-boundary reorder needs a seal-ahead tail to exist while the head is still
    // unexecuted. Resume at seq 10 with execution reported through 9 (the previous epoch's last):
    // the head (10) may seal, but nothing may seal ahead of it until execution reaches it.
    let far_boundary = u64::MAX;

    // nothing sealed yet: the head itself may start
    let fresh = BatchPipeline::<Clean>::new(None, 10, 0).on_event();
    assert_eq!(fresh.can_start_build(Some(9), far_boundary), Ok(()));

    // head sealed (depth 1), execution still at 9: throttled, even though the Active budget is 4
    let head_out = BatchPipeline::<Clean>::new(Some(10), 10, 0).on_event();
    assert_eq!(
        head_out.can_start_build(Some(9), far_boundary),
        Err(GateRejectionReason::EpochStartThrottled)
    );
    // no watermark at all (genesis) is the same: the floor is start_seq - 1, still unexecuted
    assert_eq!(
        head_out.can_start_build(None, far_boundary),
        Err(GateRejectionReason::EpochStartThrottled)
    );

    // the head executed: the throttle lifts and the ordinary budget applies again
    assert_eq!(head_out.can_start_build(Some(10), far_boundary), Ok(()));
    // three ahead of the executed head is under the Active budget of four
    let three_ahead = BatchPipeline::<Clean>::new(Some(13), 10, 0).on_event();
    assert_eq!(three_ahead.can_start_build(Some(10), far_boundary), Ok(()));
    // four ahead fills it, and the reason is now the budget, not the throttle
    let four_ahead = BatchPipeline::<Clean>::new(Some(14), 10, 0).on_event();
    assert_eq!(
        four_ahead.can_start_build(Some(10), far_boundary),
        Err(GateRejectionReason::BudgetExhausted)
    );
}

#[test]
fn epoch_start_throttle_yields_to_the_boundary_and_quiesce_rules() {
    // Closed still wins over everything; Quiescing's budget of one coincides with the throttle, so
    // the reason reported is the throttle's while the head is unexecuted and the budget's after.
    let boundary = 100;
    let quiescing_tip = boundary - BOUNDARY_QUIESCE_WINDOW_SECS;

    let at_boundary = BatchPipeline::<Clean>::new(Some(10), 10, boundary).on_event();
    assert_eq!(
        at_boundary.can_start_build(Some(9), boundary),
        Err(GateRejectionReason::EpochBoundaryReached)
    );

    let quiescing = BatchPipeline::<Clean>::new(Some(10), 10, quiescing_tip).on_event();
    assert_eq!(
        quiescing.can_start_build(Some(9), boundary),
        Err(GateRejectionReason::EpochStartThrottled)
    );
    let quiescing_ahead = BatchPipeline::<Clean>::new(Some(11), 10, quiescing_tip).on_event();
    assert_eq!(
        quiescing_ahead.can_start_build(Some(10), boundary),
        Err(GateRejectionReason::BudgetExhausted)
    );
}

#[test]
fn clean_closes_when_the_tip_reaches_the_boundary() {
    let boundary = 100;
    let below = BatchPipeline::<Clean>::new(None, 1, boundary - 1);
    assert!(below.check_boundary(boundary).is_ok(), "a tip below the boundary keeps building");

    let at = BatchPipeline::<Clean>::new(None, 1, boundary);
    assert!(at.check_boundary(boundary).is_err(), "a tip at the boundary closes the pipeline");
}

#[test]
fn resume_seq_prefers_the_executed_watermark() {
    let (_tx, rx) = tokio::sync::watch::channel(Some(5u64));
    let reader = OwnWatermarkReceiver::new(rx);
    assert_eq!(reader.resume_seq(9), 6, "an executed watermark resumes one past it");

    let (_fresh_tx, fresh_rx) = tokio::sync::watch::channel(None);
    let fresh = OwnWatermarkReceiver::new(fresh_rx);
    assert_eq!(fresh.resume_seq(9), 9, "a fresh epoch trusts the persisted counter");
}
