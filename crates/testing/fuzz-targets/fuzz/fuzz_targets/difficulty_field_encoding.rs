//! Fuzz target: EVM difficulty field encoding round-trip.
//!
//! Axyl repurposes the EVM `difficulty` field to pack:
//!   difficulty = (batch_index << 16) | worker_id
//! and the EL's `first_batch()` treats `difficulty < 65536` as "batch_index == 0"
//! (see `crates/execution/evm/src/evm/config.rs` and `evm/block.rs::first_batch`).
//!
//! This target verifies the pack/unpack invariant that production relies on:
//! - packing never overflows for any (batch_index, worker_id)
//! - decoding recovers batch_index and worker_id
//! - the `difficulty < 65536  <=>  batch_index == 0` rule `first_batch()` uses
//!
//! NOTE: the canonical pack (`config.rs`) and unpack (`first_batch`) live in the
//! `rayls-execution-evm` crate. Calling them directly would pull the entire reth /
//! rocksdb stack into this isolated fuzz workspace, so this target re-checks the
//! invariant rather than importing the (private) `first_batch`. If a callable
//! `deconstruct_difficulty` is later exposed on `RethEnv` (mirroring
//! `deconstruct_nonce`), switch this target to call it. The previous `mix_hash`
//! XOR section was removed: XORing two values derived from `difficulty` and then
//! recovering them is a pure tautology that cannot fail.
//!
//! Run: cargo +nightly-2026-06-24 fuzz run difficulty_field_encoding

#![no_main]
use libfuzzer_sys::fuzz_target;

#[derive(arbitrary::Arbitrary, Debug)]
struct FuzzInput {
    batch_index: u32,
    worker_id: u16,
}

fuzz_target!(|input: FuzzInput| {
    let batch_index = input.batch_index;
    let worker_id = input.worker_id;

    // Encode exactly as production does (evm/config.rs): batch_index << 16 | worker_id.
    let difficulty: u64 = ((batch_index as u64) << 16) | (worker_id as u64);

    // Decode and assert recovery.
    let decoded_worker_id = (difficulty & 0xFFFF) as u16;
    let decoded_batch_index = (difficulty >> 16) as u32;
    assert_eq!(decoded_worker_id, worker_id, "worker_id round-trip failed: packed={difficulty:#x}");
    assert_eq!(decoded_batch_index, batch_index, "batch_index round-trip failed: packed={difficulty:#x}");

    // The rule `first_batch()` depends on: difficulty < 65536  <=>  batch_index == 0.
    assert_eq!(
        difficulty < 65536,
        batch_index == 0,
        "first_batch boundary inconsistent: difficulty={difficulty}, batch_index={batch_index}, worker_id={worker_id}"
    );
});
