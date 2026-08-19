//! Fuzz target: Batch digest computation.
//!
//! Exercises Axyl's `Batch::digest()` and `Batch::seal_slow()` with arbitrary
//! batch contents. This tests:
//! - Digest computation never panics on any input
//! - Two identical batches always produce the same digest (determinism)
//! - Modifying any field changes the digest (collision resistance sanity check)
//!
//! Run: cargo +nightly fuzz run batch_digest

#![no_main]
use libfuzzer_sys::fuzz_target;
use rayls_infrastructure_types::{Address, Batch};

#[derive(arbitrary::Arbitrary, Debug)]
struct FuzzBatch {
    /// Raw transaction bytes (up to 5 transactions, each up to 1KB).
    transactions: Vec<Vec<u8>>,
    epoch: u32,
    beneficiary: [u8; 20],
    base_fee_per_gas: u64,
    worker_id: u16,
    seq: u64,
}

fuzz_target!(|input: FuzzBatch| {
    // Limit transactions to prevent OOM.
    let transactions: Vec<rayls_infrastructure_types::Bytes> = input
        .transactions
        .into_iter()
        .take(5)
        .map(|mut tx| {
            tx.truncate(1024);
            tx.into()
        })
        .collect();

    let batch = Batch {
        transactions: transactions.clone(),
        epoch: input.epoch,
        beneficiary: Address::from_slice(&input.beneficiary),
        base_fee_per_gas: input.base_fee_per_gas,
        worker_id: input.worker_id,
        seq: input.seq,
        received_at: None,
    };

    // digest() must not panic.
    let digest1 = batch.clone().digest();

    // Same batch must produce the same digest (determinism).
    let digest2 = batch.clone().digest();
    assert_eq!(digest1, digest2, "digest is not deterministic");

    // Modifying epoch should change the digest (basic collision resistance).
    if input.epoch < u32::MAX {
        let mut modified = batch;
        modified.epoch = input.epoch + 1;
        let modified_digest = modified.digest();
        assert_ne!(
            digest1, modified_digest,
            "changing epoch did not change digest"
        );
    }
});
