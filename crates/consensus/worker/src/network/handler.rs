use super::{
    error::{WorkerNetworkError, WorkerNetworkResult},
    message::WorkerGossip,
    WorkerNetworkHandle,
};
use crate::WorkerResponse;
use rayls_consensus_network::GossipMessage;
use rayls_infrastructure_config::{ConsensusConfig, LibP2pConfig};
use rayls_infrastructure_network_types::{WorkerOthersBatchMessage, WorkerToPrimaryClient};
use rayls_infrastructure_storage::tables::Batches;
use rayls_infrastructure_types::{
    encode, ensure, now, try_decode, Batch, BatchValidation, BlockHash, BlsPublicKey, Database,
    DbTx, SealedBatch, WorkerId,
};
use std::sync::{Arc, LazyLock};
use tracing::{debug, error};

/// The minimal length of a single, encoded, default [Batch] used to set a local min for
/// message validation.
static LOCAL_MIN_REQUEST_SIZE: LazyLock<usize> = LazyLock::new(|| encode(&Batch::default()).len());
/// The minimal response wrapper using a default, empty message.
static MESSAGE_OVERHEAD: LazyLock<usize> =
    LazyLock::new(|| encode(&WorkerResponse::RequestBatches(vec![])).len());

/// The maximum number of requested digests one response is served from.
///
/// Sits above an honest requester's ceiling of `gc_depth` * committee * `max_header_num_of_batches`
/// (35k at defaults), and deliberately is not divided by the peers a request is sharded across:
/// that count is runtime state, and a cap below the ceiling strands the digests past it, since
/// shard membership is index-stable. Bounds probes on a peer-controlled list; the byte budget
/// bounds served bytes.
const MAX_SERVED_DIGESTS: usize = 40_000;

/// The type that handles requests from peers.
#[derive(Clone, Debug)]
pub struct RequestHandler<DB> {
    /// This worker's id.
    id: WorkerId,
    /// The type that validates batches received from peers.
    validator: Arc<dyn BatchValidation>,
    /// Consensus config with access to database.
    consensus_config: ConsensusConfig<DB>,
    /// Network handle- so we can respond to gossip.
    network_handle: WorkerNetworkHandle,
}

impl<DB> RequestHandler<DB>
where
    DB: Database,
{
    /// Create a new instance of Self.
    pub fn new(
        id: WorkerId,
        validator: Arc<dyn BatchValidation>,
        consensus_config: ConsensusConfig<DB>,
        network_handle: WorkerNetworkHandle,
    ) -> Self {
        Self { id, validator, consensus_config, network_handle }
    }

    /// Process gossip from the committee.
    ///
    /// Workers gossip the Batch Digests once accepted so that non-committee peers can request the
    /// Batch.
    pub(super) async fn process_gossip(&self, msg: &GossipMessage) -> WorkerNetworkResult<()> {
        // deconstruct message
        let GossipMessage { data, source: _, sequence_number: _, topic } = msg;

        // gossip is uncompressed
        let gossip = try_decode(data)?;

        match gossip {
            WorkerGossip::Batch(batch_hash) => {
                ensure!(
                    topic.to_string().eq(&LibP2pConfig::worker_batch_topic()),
                    WorkerNetworkError::InvalidTopic
                );
                // Retrieve the block...
                let store = self.consensus_config.node_storage();
                if !matches!(store.get::<Batches>(&batch_hash), Ok(Some(_))) {
                    // If we don't have this batch already then try to get it.
                    // If we are a CVV then we should already have it.
                    // This allows non-CVVs to pre fetch batches they will soon need.
                    match self.network_handle.request_batches(vec![batch_hash]).await {
                        Ok(batches) => {
                            if let Some(batch) = batches.first() {
                                store.insert::<Batches>(&batch.digest(), batch).map_err(|e| {
                                    WorkerNetworkError::Internal(format!(
                                        "failed to write to batch store: {e}"
                                    ))
                                })?;
                            }
                        }
                        Err(e) => {
                            tracing::error!(target: "worker:network", "failed to get gossipped batch {batch_hash}: {e}");
                        }
                    }
                }
            }
            WorkerGossip::Txn(tx_bytes) => {
                ensure!(
                    topic.to_string().eq(&LibP2pConfig::worker_txn_topic()),
                    WorkerNetworkError::InvalidTopic
                );
                if let Some(authority) = self.consensus_config.authority() {
                    let committee = self.consensus_config.committee();
                    let authorities = committee.authorities();
                    let size = authorities.len();
                    for (slot, auth) in authorities.into_iter().enumerate() {
                        if &auth == authority {
                            if let Err(e) = self.validator.submit_batch_if_mine(
                                &tx_bytes,
                                size as u64,
                                slot as u64,
                            ) {
                                error!(target: "worker:network", "failed to submit batch: {e}");
                            }
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Process a new reported batch.
    pub(super) async fn process_report_batch(
        &self,
        peer: &BlsPublicKey,
        sealed_batch: SealedBatch,
    ) -> WorkerNetworkResult<()> {
        // return error if reporter isn't in current committee
        if !self.consensus_config.committee_pub_keys().contains(peer) {
            return Err(WorkerNetworkError::NonCommitteeBatch);
        }

        let client = self.consensus_config.local_network().clone();
        let store = self.consensus_config.node_storage().clone();
        // validate batch - log error if invalid
        self.validator.validate_batch(sealed_batch.clone()).await?;

        let (mut batch, digest) = sealed_batch.split();

        // Set received_at timestamp for remote batch.
        batch.set_received_at(now());
        store.insert::<Batches>(&digest, &batch).map_err(|e| {
            WorkerNetworkError::Internal(format!("failed to write to batch store: {e}"))
        })?;

        // notify primary for payload store
        client
            .report_others_batch(WorkerOthersBatchMessage { digest, worker_id: self.id })
            .await
            .map_err(|e| WorkerNetworkError::Internal(e.to_string()))?;

        Ok(())
    }

    /// Attempt to return requested batches.
    ///
    /// MDBX reads are offloaded to `spawn_blocking` so assembling a large response does not stall
    /// the async runtime under load.
    pub(super) async fn process_request_batches(
        &self,
        batch_digests: Vec<BlockHash>,
        max_response_size: usize,
    ) -> WorkerNetworkResult<Vec<Batch>> {
        let consensus_config = self.consensus_config.clone();
        tokio::task::spawn_blocking(move || {
            collect_requested_batches_blocking(batch_digests, max_response_size, consensus_config)
        })
        .await
        .map_err(|e| WorkerNetworkError::Internal(format!("batch-collector join error: {e}")))?
    }
}

/// Reads the requested batches from storage, truncated to the response size cap.
fn collect_requested_batches_blocking<DB: Database>(
    batch_digests: Vec<BlockHash>,
    max_response_size: usize,
    consensus_config: ConsensusConfig<DB>,
) -> WorkerNetworkResult<Vec<Batch>> {
    // assume reasonable min is 1 encoded batch (no transactions)
    // NOTE: caller needs to account for batches + msg overhead, and batches must have
    // transactions
    if max_response_size < *LOCAL_MIN_REQUEST_SIZE {
        debug!(target: "cert-collector", "batch request max size too small: {}", max_response_size);
        return Err(WorkerNetworkError::InvalidRequest("Request size too small".into()));
    }

    // return error for empty batches
    if batch_digests.is_empty() {
        debug!(target: "cert-collector", "batch request empty");
        return Err(WorkerNetworkError::InvalidRequest("Empty batch digests".into()));
    }

    // use the min value between this node's max rpc message size and the requestor's reported
    // max message size
    //
    // NOTE: assume safe overhead is accounted for because the codec will also compress messages
    let local_max =
        consensus_config.network_config().libp2p_config().max_rpc_message_size - *MESSAGE_OVERHEAD;
    let max_message_size = max_response_size.min(local_max);

    let store = consensus_config.node_storage();

    // A capped response is otherwise indistinguishable from a peer that simply held nothing, and
    // that ambiguity is what would hide an under-sized cap starving the digests past it. Once per
    // request, since the digest set is peer-controlled.
    if batch_digests.len() > MAX_SERVED_DIGESTS {
        debug!(
            target: "worker::network",
            requested = batch_digests.len(),
            examined = MAX_SERVED_DIGESTS,
            "batch request truncated to the digest cap"
        );
    }

    // Serve from one cold-aware read txn: `tx.get` resolves each digest hot-first and falls through
    // to cold within the same txn (see `ColdTx`), so a lagging peer's archived batches are served
    // without opening a fresh read txn per digest.
    //
    // Stop at the response budget: requesters rely on responders truncating below the requested
    // set, so reading further would only decode (often large) batches to discard them.
    store
        .with_read_txn(|tx| {
            let mut batches = Vec::new();
            let mut total_size = 0usize;
            let mut reported_unreadable = false;
            for digest in batch_digests.iter().take(MAX_SERVED_DIGESTS) {
                let batch = match tx.get::<Batches>(digest) {
                    Ok(Some(batch)) => batch,
                    Ok(None) => continue,
                    Err(e) => {
                        // A row this node cannot read is a local, on-disk fault: failing the whole
                        // response would drop the batches it can produce and leave the requester
                        // re-asking the same set indefinitely, since the fault does not heal.
                        if !reported_unreadable {
                            // Once per response, because the digest set is peer-controlled and a
                            // per-digest log would be a remote log-flood vector.
                            reported_unreadable = true;
                            error!(target: "worker::network", %digest, "skipping unreadable batch while serving: {e:?}");
                        }
                        continue;
                    }
                };
                let batch_size = batch.size();
                if total_size + batch_size > max_message_size {
                    break;
                }
                total_size += batch_size;
                batches.push(batch);
            }
            Ok(batches)
        })
        .map_err(|e| {
            WorkerNetworkError::Internal(format!("failed to read from batch store: {e:?}"))
        })
}

// support IT tests
#[cfg(any(test, feature = "test-utils"))]
impl<DB> RequestHandler<DB>
where
    DB: Database,
{
    // /// Publicly available for tests.
    // /// See [Self::process_gossip].
    // pub async fn pub_process_gossip(&self, msg: &GossipMessage) -> WorkerNetworkResult<()> {
    //     self.process_gossip(msg).await
    // }

    // /// Publicly available for tests.
    // /// See [Self::process_report_batch].
    // pub async fn pub_process_report_batch(
    //     &self,
    //     peer: &BlsPublicKey,
    //     sealed_batch: SealedBatch,
    // ) -> WorkerNetworkResult<()> {
    //     self.process_report_batch(peer, sealed_batch).await
    // }

    // /// Publicly available for tests.
    // /// See [Self::process_request_batches].
    // pub async fn pub_process_request_batches(
    //     &self,
    //     batch_digests: Vec<BlockHash>,
    //     max_response_size: usize,
    // ) -> WorkerNetworkResult<Vec<Batch>> {
    //     self.process_request_batches(batch_digests, max_response_size).await
    // }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rayls_infrastructure_storage::{
        cold::{ColdConfig, ColdDatabase, ColdLocation},
        layered_db::LayeredDatabase,
        mdbx::MdbxDatabase,
        mem_db::MemDatabase,
        tables::ColdBatchLocations,
    };
    use rayls_infrastructure_types::B256;
    use rayls_testing_test_utils::CommitteeFixture;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;

    /// The node storage stack under test for the cold cases: mem overlay -> mdbx -> cold jars.
    type ColdTieredDb = ColdDatabase<LayeredDatabase<MdbxDatabase>>;

    /// Returns the first authority's config, backed by the cold-tiered stack rooted under `tmp`.
    ///
    /// Composed explicitly rather than through `open_db` so an `--all-features` build, where the
    /// redb backend takes over the `DatabaseType` alias, still exercises the cold composition.
    fn cold_tiered_config(tmp: &TempDir) -> ConsensusConfig<ColdTieredDb> {
        let next = AtomicUsize::new(0);
        let fixture = CommitteeFixture::builder(|| {
            let dir = tmp.path().join(format!("authority-{}", next.fetch_add(1, Ordering::SeqCst)));
            std::fs::create_dir_all(dir.join("hot")).expect("create db dir");
            let layered =
                LayeredDatabase::open(MdbxDatabase::open(dir.join("hot")).expect("open mdbx"));
            let db = ColdDatabase::open(layered, &ColdConfig { dir: dir.join("cold") })
                .expect("open cold store");
            db.open_table::<Batches>().expect("open Batches");
            db.open_table::<ColdBatchLocations>().expect("open ColdBatchLocations");
            db
        })
        .build();
        fixture.first_authority().consensus_config()
    }

    /// The responder returns the prefix of requested batches that fits the size budget, in order,
    /// and skips digests with no stored batch.
    #[test]
    fn responder_truncates_to_size_budget() {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let config = fixture.first_authority().consensus_config();
        let store = config.node_storage().clone();

        // Equal-sized batches so the budget maps cleanly to a batch count.
        let sample = Batch { transactions: vec![vec![1u8; 256]], ..Default::default() };
        let batch_size = sample.size();

        const TOTAL: usize = 20;
        const FIT: usize = 5;
        let mut digests = Vec::with_capacity(TOTAL + 1);
        for _ in 0..TOTAL {
            let digest = B256::random();
            store.insert::<Batches>(&digest, &sample).expect("write batch to db");
            digests.push(digest);
        }
        // A digest with no stored batch must be skipped, not returned.
        digests.push(B256::random());

        // Budget sits between FIT and FIT+1 batches, so exactly FIT come back.
        let budget = batch_size * FIT + batch_size / 2;
        let batches =
            collect_requested_batches_blocking(digests, budget, config).expect("collect batches");

        assert_eq!(batches.len(), FIT, "must return exactly the batches that fit the budget");
        assert!(
            batches.iter().map(Batch::size).sum::<usize>() <= budget,
            "returned batches must fit within the budget",
        );
    }

    /// A batch present only in the cold tier (archived and pruned from hot) is still served: the
    /// responder's read txn falls through to cold, so a lagging peer can fetch history past the
    /// hot window.
    #[test]
    fn responder_serves_cold_archived_batches() {
        let tmp = TempDir::new().expect("tempdir");
        let config = cold_tiered_config(&tmp);
        let store = config.node_storage().clone();

        let hot_digest = B256::repeat_byte(0x11);
        let hot_batch = Batch { transactions: vec![vec![1u8; 64]], ..Default::default() };
        store.insert::<Batches>(&hot_digest, &hot_batch).expect("insert hot batch");

        // The cold batch exists only as a jar row plus its auxiliary-index entry, never hot.
        let cold_digest = B256::repeat_byte(0x22);
        let cold_batch = Batch { transactions: vec![vec![2u8; 64]], ..Default::default() };
        let cold = store.cold();
        cold.batches().begin_epoch(1, 0).expect("begin epoch");
        cold.batches()
            .append_row(&[cold_digest.as_slice(), &encode(&cold_batch)])
            .expect("append row");
        cold.batches().commit().expect("commit");
        store
            .insert::<ColdBatchLocations>(&cold_digest, &ColdLocation { epoch: 1, row: 0 })
            .expect("insert cold location");

        // Request hot + cold + absent: both stored batches serve, the absent digest is skipped.
        let digests = vec![hot_digest, cold_digest, B256::repeat_byte(0x33)];
        let batches =
            collect_requested_batches_blocking(digests, usize::MAX, config).expect("collect");
        assert_eq!(batches.len(), 2, "hot and cold batches must both serve");
        assert!(
            batches.iter().any(|b| encode(b) == encode(&cold_batch)),
            "the cold-archived batch must serve through the fall-through"
        );
    }

    /// One unreadable cold row costs only its own digest: the batches this node can produce still
    /// serve, so a damaged jar cannot wedge a requester into re-asking the same set forever.
    #[test]
    fn responder_skips_unreadable_cold_row() {
        let tmp = TempDir::new().expect("tempdir");
        let config = cold_tiered_config(&tmp);
        let store = config.node_storage().clone();

        let hot = [B256::repeat_byte(0x11), B256::repeat_byte(0x22)];
        let hot_batch = Batch { transactions: vec![vec![1u8; 64]], ..Default::default() };
        for digest in &hot {
            store.insert::<Batches>(digest, &hot_batch).expect("insert hot batch");
        }

        // Point the auxiliary index at a row holding a different batch's digest. Any on-disk fault
        // that makes `read_batch_checked` fail reaches the responder the same way; the digest
        // cross-check is simply the cheapest one to stage.
        let archived = B256::repeat_byte(0x33);
        let cold = store.cold();
        cold.batches().begin_epoch(1, 0).expect("begin epoch");
        cold.batches().append_row(&[archived.as_slice(), &encode(&hot_batch)]).expect("append row");
        cold.batches().commit().expect("commit");
        let unreadable = B256::repeat_byte(0x44);
        store
            .insert::<ColdBatchLocations>(&unreadable, &ColdLocation { epoch: 1, row: 0 })
            .expect("insert cold location");

        // The bad digest sits between two good ones, so a passing run proves the loop continued
        // rather than merely swallowing the error at the end.
        let digests = vec![hot[0], unreadable, hot[1]];
        let batches = collect_requested_batches_blocking(digests, usize::MAX, config)
            .expect("an unreadable cold row must not fail the whole response");
        assert_eq!(batches.len(), 2, "every readable batch must still serve");
    }

    /// The responder examines at most [`MAX_SERVED_DIGESTS`] of a request, so a peer cannot drive
    /// unbounded storage work with a digest list that is mostly misses. A batch stored past the cap
    /// is never read, which is the property that decides whether a capped request can starve its
    /// own tail.
    #[test]
    fn responder_caps_digests_examined() {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let config = fixture.first_authority().consensus_config();
        let store = config.node_storage().clone();

        const PRESENT: usize = 4;
        let within_cap = Batch { transactions: vec![vec![1u8; 64]], ..Default::default() };
        let past_cap = Batch { transactions: vec![vec![2u8; 64]], ..Default::default() };

        // Stored batches sit on both sides of the cap, separated by a run of digests with nothing
        // stored. Misses cost no response bytes, so only the cap can decide where serving stops,
        // and the request is long enough to exercise the truncation log.
        let mut digests = Vec::with_capacity(MAX_SERVED_DIGESTS + PRESENT);
        for _ in 0..PRESENT {
            let digest = B256::random();
            store.insert::<Batches>(&digest, &within_cap).expect("write batch to db");
            digests.push(digest);
        }
        digests.resize_with(MAX_SERVED_DIGESTS, B256::random);
        for _ in 0..PRESENT {
            let digest = B256::random();
            store.insert::<Batches>(&digest, &past_cap).expect("write batch to db");
            digests.push(digest);
        }

        let batches =
            collect_requested_batches_blocking(digests, usize::MAX, config).expect("collect");
        assert_eq!(batches.len(), PRESENT, "serving must stop at the digest cap");
        assert!(
            batches.iter().all(|b| encode(b) == encode(&within_cap)),
            "a batch stored past the cap must never be read",
        );
    }
}
