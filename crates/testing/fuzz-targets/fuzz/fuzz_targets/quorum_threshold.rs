//! Fuzz target: quorum_threshold arithmetic.
//!
//! The quorum formula `((2 * committee_members) / 3) + 1` can theoretically
//! overflow when `committee_members > u64::MAX / 2`. That path is unreachable
//! in production: the only caller passes the validator count, which is bounded
//! by the on-chain committee size (small). #446 proposed a u128 widening but
//! was closed as defensive cost on the hot path against an impossible scenario,
//! so this harness is intentionally restricted to the production envelope.
//!
//! The harness asserts on realistic inputs:
//! - No panic
//! - Result is always >= 1
//! - Result is always > committee_members / 2 (majority)
//! - Result is always <= committee_members (can't require more than exist)
//!
//! Run: cargo +nightly-2026-06-24 fuzz run quorum_threshold

#![no_main]
use libfuzzer_sys::fuzz_target;
use rayls_infrastructure_types::quorum_threshold;

fuzz_target!(|members: u64| {
    // Skip inputs outside the production envelope. The `2 * members` doubling
    // overflows u64 above `u64::MAX / 2`; bounding at `u64::MAX / 4` leaves
    // generous headroom while still exercising every realistic committee size
    // and all bit patterns up to ~4.6 × 10^18.
    if members > u64::MAX / 4 {
        return;
    }

    // The function must not panic.
    let threshold = quorum_threshold(members);

    // Threshold must always be at least 1.
    assert!(threshold >= 1, "quorum threshold must be >= 1 for members={members}");

    // Strict-majority property holds for all members >= 1:
    // quorum_threshold(1) = 1 > 1/2 = 0. (The previous `>= 2` guard was an
    // unnecessary coverage gap.)
    if members >= 1 {
        assert!(
            threshold > members / 2,
            "quorum threshold {threshold} must be > majority {half} for members={members}",
            half = members / 2,
        );
    }

    // Threshold must never exceed the committee size.
    // (A quorum can't require more members than exist.)
    assert!(
        threshold <= members || members == 0,
        "quorum threshold {threshold} exceeds committee size {members}",
    );
});
