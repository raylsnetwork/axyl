use alloy::primitives::Address;
pub use alloy::primitives::FixedBytes;
use criterion::{criterion_group, criterion_main, Criterion};
use rand::{rngs::StdRng, SeedableRng as _};
use rayls_execution_evm::{
    reth_env::types::reth_recover_raw_transactions, test_utils::TransactionFactory,
};
use rayls_infrastructure_types::{Bytes, TransactionSigned, U256};
use reth::rpc::server_types::eth::utils::recover_raw_transaction as reth_recover_raw_transaction;
use std::sync::Arc;
use tracing::error;

/// Generates `count` deterministic signed transactions, encoded as [`Bytes`].
fn generate_dummy_transactions(count: usize) -> Vec<Bytes> {
    let mut rng = StdRng::seed_from_u64(42);
    let mut txs = Vec::with_capacity(count);
    for _ in 0..count {
        let mut tf = TransactionFactory::new_random_from_seed(&mut rng);
        let tx = tf.create_eip1559_encoded(
            Arc::new(rayls_infrastructure_types::test_genesis().into()),
            None,
            1,
            Some(Address::from_slice(&[0x11; 20])),
            U256::from(1_000_000u64),
            vec![].into(),
        );
        txs.push(tx);
    }
    txs
}

fn test_reth_recover_raw_transactions_parallel_benchmark(c: &mut Criterion) {
    let txs = generate_dummy_transactions(200);
    let batch_digest = Some(FixedBytes::<32>::ZERO);

    c.bench_function("reth_recover_raw_transactions_parallel", |b| {
        b.iter(|| {
            let _ = reth_recover_raw_transactions(batch_digest, &txs);
        })
    });
}

fn test_reth_recover_raw_transactions_sequentialbench(c: &mut Criterion) {
    let txs = generate_dummy_transactions(200);
    let batch_digest = Some(FixedBytes::<32>::ZERO);

    let rec_fn = |tx_bytes: &Bytes| {
        reth_recover_raw_transaction::<TransactionSigned>(tx_bytes).inspect_err(|e| {
            error!(
                target: "engine",
                batch=?batch_digest,
                ?tx_bytes,
                "failed to recover signer: {e}"
            )
        })
    };

    c.bench_function("reth_recover_raw_transactions_sequential", |b| {
        b.iter(|| {
            let _: Vec<_> = txs.iter().map(rec_fn).collect();
        })
    });
}

criterion_group!(
    benches,
    test_reth_recover_raw_transactions_parallel_benchmark,
    test_reth_recover_raw_transactions_sequentialbench
);
criterion_main!(benches);
