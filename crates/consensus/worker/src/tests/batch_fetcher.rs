//! Batch fetcher tests.
use super::*;
use crate::test_utils::TestRequestBatchesNetwork;
use rayls_execution_evm::test_utils::transaction;
use rayls_infrastructure_storage::open_db;
use rayls_infrastructure_types::test_chain_spec_arc;
use std::collections::{HashMap, HashSet};
use tempfile::TempDir;

#[tokio::test]
async fn test_fetchertt() {
    let mut network = TestRequestBatchesNetwork::new();
    let temp_dir = TempDir::new().unwrap();
    let batch_store = open_db(temp_dir.path());
    let chain = test_chain_spec_arc();
    let batch1 = Batch { transactions: vec![transaction(chain.clone())], ..Default::default() };
    let batch2 = Batch { transactions: vec![transaction(chain)], ..Default::default() };
    let digests = HashSet::from_iter(vec![batch1.digest(), batch2.digest()]);
    network.put(&[1, 2], batch1.clone()).await;
    network.put(&[2, 3], batch2.clone()).await;
    let fetcher = BatchFetcher {
        network: Arc::new(network.handle()),
        batch_store: batch_store.clone(),
        metrics: Arc::new(WorkerMetrics::default()),
    };
    let mut expected_batches = HashMap::from_iter(vec![
        (batch1.digest(), batch1.clone()),
        (batch2.digest(), batch2.clone()),
    ]);
    let mut fetched_batches = fetcher.fetch(digests).await;
    // Reset metadata from the fetched and expected batches
    for batch in fetched_batches.values_mut() {
        // assert received_at was set to some value before resetting.
        assert!(batch.received_at().is_some());
        batch.set_received_at(0);
    }
    for batch in expected_batches.values_mut() {
        batch.set_received_at(0);
    }
    assert_eq!(fetched_batches, expected_batches);
    assert_eq!(
        batch_store.get::<Batches>(&batch1.digest()).unwrap().unwrap().digest(),
        batch1.digest()
    );
    assert_eq!(
        batch_store.get::<Batches>(&batch2.digest()).unwrap().unwrap().digest(),
        batch2.digest()
    );
}

#[tokio::test]
async fn test_fetcher_locally_with_remaining() {
    // Limit is set to two batches in test request_batches(). Request 3 batches
    // and ensure another request is sent to get the remaining batches.
    let mut network = TestRequestBatchesNetwork::new();
    let temp_dir = TempDir::new().unwrap();
    let batch_store = open_db(temp_dir.path());
    let chain = test_chain_spec_arc();
    let batch1 = Batch { transactions: vec![transaction(chain.clone())], ..Default::default() };
    let batch2 = Batch { transactions: vec![transaction(chain.clone())], ..Default::default() };
    let batch3 = Batch { transactions: vec![transaction(chain)], ..Default::default() };
    let digests = HashSet::from_iter(vec![batch1.digest(), batch2.digest(), batch3.digest()]);
    for batch in &[&batch1, &batch2, &batch3] {
        batch_store.insert::<Batches>(&batch.digest(), batch).unwrap();
    }
    network.put(&[1, 2], batch1.clone()).await;
    network.put(&[2, 3], batch2.clone()).await;
    network.put(&[3, 4], batch3.clone()).await;
    let fetcher = BatchFetcher {
        network: Arc::new(network.handle()),
        batch_store,
        metrics: Arc::new(WorkerMetrics::default()),
    };
    let expected_batches = HashMap::from_iter(vec![
        (batch1.digest(), batch1.clone()),
        (batch2.digest(), batch2.clone()),
        (batch3.digest(), batch3.clone()),
    ]);
    let fetched_batches = fetcher.fetch(digests).await;
    assert_eq!(fetched_batches, expected_batches);
}

#[tokio::test]
async fn test_fetcher_remote_with_remaining() {
    // Limit is set to two batches in test request_batches(). Request 3 batches
    // and ensure another request is sent to get the remaining batches.
    let mut network = TestRequestBatchesNetwork::new();
    let temp_dir = TempDir::new().unwrap();
    let batch_store = open_db(temp_dir.path());
    let chain = test_chain_spec_arc();
    let batch1 = Batch { transactions: vec![transaction(chain.clone())], ..Default::default() };
    let batch2 = Batch { transactions: vec![transaction(chain.clone())], ..Default::default() };
    let batch3 = Batch { transactions: vec![transaction(chain)], ..Default::default() };
    let digests = HashSet::from_iter(vec![batch1.digest(), batch2.digest(), batch3.digest()]);
    network.put(&[3, 4], batch1.clone()).await;
    network.put(&[2, 3], batch2.clone()).await;
    network.put(&[2, 3, 4], batch3.clone()).await;
    let fetcher = BatchFetcher {
        network: Arc::new(network.handle()),
        batch_store,
        metrics: Arc::new(WorkerMetrics::default()),
    };
    let mut expected_batches = HashMap::from_iter(vec![
        (batch1.digest(), batch1.clone()),
        (batch2.digest(), batch2.clone()),
        (batch3.digest(), batch3.clone()),
    ]);
    let mut fetched_batches = fetcher.fetch(digests).await;

    // Reset metadata from the fetched and expected batches
    for batch in fetched_batches.values_mut() {
        // assert received_at was set to some value before resetting.
        assert!(batch.received_at().is_some());
        batch.set_received_at(0);
    }
    for batch in expected_batches.values_mut() {
        batch.set_received_at(0);
    }

    assert_eq!(fetched_batches, expected_batches);
}

#[tokio::test]
async fn test_fetcher_local_and_remote() {
    let mut network = TestRequestBatchesNetwork::new();
    let temp_dir = TempDir::new().unwrap();
    let batch_store = open_db(temp_dir.path());
    let chain = test_chain_spec_arc();
    let batch1 = Batch { transactions: vec![transaction(chain.clone())], ..Default::default() };
    let batch2 = Batch { transactions: vec![transaction(chain.clone())], ..Default::default() };
    let batch3 = Batch { transactions: vec![transaction(chain)], ..Default::default() };
    let digests = HashSet::from_iter(vec![batch1.digest(), batch2.digest(), batch3.digest()]);
    batch_store.insert::<Batches>(&batch1.digest(), &batch1).unwrap();
    network.put(&[1, 2, 3], batch1.clone()).await;
    network.put(&[2, 3, 4], batch2.clone()).await;
    network.put(&[1, 4], batch3.clone()).await;
    let fetcher = BatchFetcher {
        network: Arc::new(network.handle()),
        batch_store,
        metrics: Arc::new(WorkerMetrics::default()),
    };
    let mut expected_batches = HashMap::from_iter(vec![
        (batch1.digest(), batch1.clone()),
        (batch2.digest(), batch2.clone()),
        (batch3.digest(), batch3.clone()),
    ]);
    let mut fetched_batches = fetcher.fetch(digests).await;

    // Reset metadata from the fetched and expected remote batches
    for batch in fetched_batches.values_mut() {
        if batch.digest() != batch1.digest() {
            // assert received_at was set to some value for remote batches before resetting.
            assert!(batch.received_at().is_some());
            batch.set_received_at(0);
        }
    }
    for batch in expected_batches.values_mut() {
        if batch.digest() != batch1.digest() {
            batch.set_received_at(0);
        }
    }

    assert_eq!(fetched_batches, expected_batches);
}

#[tokio::test]
async fn test_fetcher_response_size_limit() {
    let mut network = TestRequestBatchesNetwork::new();
    let temp_dir = TempDir::new().unwrap();
    let batch_store = open_db(temp_dir.path());
    let num_digests = 12;
    let mut expected_batches = Vec::new();
    let mut local_digests = Vec::new();
    // 6 batches available locally with response size limit of 2
    let chain = test_chain_spec_arc();
    for _i in 0..num_digests / 2 {
        let batch = Batch { transactions: vec![transaction(chain.clone())], ..Default::default() };
        local_digests.push(batch.digest());
        batch_store.insert::<Batches>(&batch.digest(), &batch).unwrap();
        network.put(&[1, 2, 3], batch.clone()).await;
        expected_batches.push(batch);
    }
    // 6 batches available remotely with response size limit of 2
    for _i in (num_digests / 2)..num_digests {
        let batch = Batch { transactions: vec![transaction(chain.clone())], ..Default::default() };
        network.put(&[1, 2, 3], batch.clone()).await;
        expected_batches.push(batch);
    }

    let mut expected_batches =
        HashMap::from_iter(expected_batches.iter().map(|batch| (batch.digest(), batch.clone())));
    let digests = HashSet::from_iter(expected_batches.clone().into_keys());
    let fetcher = BatchFetcher {
        network: Arc::new(network.handle()),
        batch_store,
        metrics: Arc::new(WorkerMetrics::default()),
    };
    let mut fetched_batches = fetcher.fetch(digests).await;

    // Reset metadata from the fetched and expected remote batches
    for batch in fetched_batches.values_mut() {
        if !local_digests.contains(&batch.digest()) {
            // assert received_at was set to some value for remote batches before resetting.
            assert!(batch.received_at().is_some());
            batch.set_received_at(0);
        }
    }
    for batch in expected_batches.values_mut() {
        if !local_digests.contains(&batch.digest()) {
            batch.set_received_at(0);
        }
    }

    assert_eq!(fetched_batches, expected_batches);
}

/// A slow/unresponsive peer must not abort the whole fetch: batches from healthy peers must
/// still come back. Aborting would re-fetch the whole subdag on every retry and surface as a
/// "Batch not found" violation under load.
#[tokio::test(flavor = "multi_thread")]
async fn test_request_batches_returns_progress_despite_slow_peer() {
    let mut network = TestRequestBatchesNetwork::new();
    let chain = test_chain_spec_arc();

    // Two digests stuck on the never-replying peer keep the remaining set above the peer count,
    // so every fetch iteration still queries the slow peer while `fetchable` is already collected.
    let fetchable = Batch { transactions: vec![transaction(chain.clone())], ..Default::default() };
    let stuck_a = Batch { transactions: vec![transaction(chain.clone())], ..Default::default() };
    let stuck_b = Batch { transactions: vec![transaction(chain)], ..Default::default() };
    network.put(&[1], fetchable.clone()).await;
    network.put(&[2], stuck_a.clone()).await;
    network.put(&[2], stuck_b.clone()).await;
    network.set_delay(2, std::time::Duration::from_secs(3600)).await;

    let digests = vec![fetchable.digest(), stuck_a.digest(), stuck_b.digest()];
    let batches = network
        .handle()
        .request_batches_from_all(digests)
        .await
        .expect("a slow peer must not abort the whole fetch");

    assert!(
        batches.iter().any(|b| b.digest() == fetchable.digest()),
        "batch reachable via the healthy peer must be returned despite a slow/unresponsive peer"
    );
}

/// Verify that unfetchable digests (not on any peer or local store) don't
/// cause an infinite loop. The fetcher must return a partial result after
/// exhausting MAX_FETCH_RETRIES.
#[tokio::test]
async fn test_fetcher_returns_partial_on_unfetchable_digests() {
    let mut network = TestRequestBatchesNetwork::new();
    let temp_dir = TempDir::new().unwrap();
    let batch_store = open_db(temp_dir.path());
    let chain = test_chain_spec_arc();

    // batch1 is available, batch2 is not (simulates GC'd batch).
    let batch1 = Batch { transactions: vec![transaction(chain.clone())], ..Default::default() };
    let batch2 = Batch { transactions: vec![transaction(chain)], ..Default::default() };
    let unfetchable_digest = batch2.digest();

    // Only put batch1 on the network; batch2 is nowhere.
    network.put(&[1, 2], batch1.clone()).await;

    let digests = HashSet::from_iter(vec![batch1.digest(), unfetchable_digest]);
    let fetcher = BatchFetcher {
        network: Arc::new(network.handle()),
        batch_store,
        metrics: Arc::new(WorkerMetrics::default()),
    };

    // Must complete (not hang forever) and return only batch1.
    let fetched = tokio::time::timeout(std::time::Duration::from_secs(15), fetcher.fetch(digests))
        .await
        .expect("fetch must not hang - MAX_FETCH_RETRIES should bound it");

    assert!(fetched.contains_key(&batch1.digest()), "fetchable batch must be returned");
    assert!(
        !fetched.contains_key(&unfetchable_digest),
        "unfetchable digest must not appear in result"
    );
    assert_eq!(fetched.len(), 1, "only the fetchable batch should be returned");
}
