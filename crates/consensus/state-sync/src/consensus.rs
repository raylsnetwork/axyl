//! Tasks and helpers for collecting consensus headers trustlessly.
use futures::{stream::FuturesUnordered, StreamExt};
use rayls_consensus_primary::{network::PrimaryNetworkHandle, ConsensusBus};
use rayls_infrastructure_config::ConsensusConfig;
use rayls_infrastructure_storage::{
    tables::{ConsensusBlockNumbersByDigest, ConsensusBlocks, ConsensusBlocksCache, EpochRecords},
    ConsensusStore, EpochStore as _,
};
use rayls_infrastructure_types::{
    BlsPublicKey, ConsensusHeader, ConsensusHeaderChainMeta, Database as ReDatabase, DbTxMut as _,
    Epoch, Noticer, B256,
};
use std::{
    collections::HashMap,
    ops::{ControlFlow, Range},
    sync::LazyLock,
    time::Duration,
};
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

const COMMITTEE_KEYS_TIMEOUT: Duration = Duration::from_secs(30);
const COMMITTEE_KEYS_POLL_INTERVAL: Duration = Duration::from_millis(500);
/// How long to wait for a gossip tip before actively probing peers for the network head.
const PROBE_INTERVAL: Duration = Duration::from_secs(10);
/// How long a probe waits for peers' certified tips before ranking the ones that arrived. One
/// valid tip is enough, so a silent peer must not hold every probe for the request timeout.
const CERTIFIED_TIP_PROBE_DEADLINE: Duration = Duration::from_secs(3);

/// Headers the walk prefetches per round, and so its most requests in flight at once.
const WALK_PREFETCH: u64 = 32;

/// Cache a consensus header in persistent storage.
pub fn store_consensus_header_in_cache<DB: ReDatabase>(db: &DB, header: &ConsensusHeader) {
    store_consensus_headers_in_cache(db, std::slice::from_ref(header));
}

/// Cache consensus headers in persistent storage in one write transaction.
fn store_consensus_headers_in_cache<DB: ReDatabase>(db: &DB, headers: &[ConsensusHeader]) {
    if headers.is_empty() {
        return;
    }
    let result = db.with_write_txn(|txn| {
        for header in headers {
            txn.insert::<ConsensusBlocksCache>(&header.number, header)?;
            txn.insert::<ConsensusBlockNumbersByDigest>(&header.digest(), &header.number)?;
        }
        Ok(())
    });
    if let Err(e) = result {
        error!(target: "state-sync", ?e, "error storing consensus header in cache");
    }
}

/// Trigger the epoch record collector and poll until committee keys arrive.
///
/// Return `None` on shutdown or timeout.
pub(crate) async fn wait_for_committee_keys<DB: ReDatabase>(
    epoch: Epoch,
    config: &ConsensusConfig<DB>,
    consensus_bus: &ConsensusBus,
    shutdown: &Noticer,
) -> Option<Vec<BlsPublicKey>> {
    if let Some(keys) = config.get_committee_keys_for_epoch(epoch) {
        return Some(keys);
    }

    // the collector iterates upward from its last_epoch, so requesting
    // the highest unknown epoch bridges all intermediate gaps
    consensus_bus.requested_missing_epoch().send_if_modified(|current| {
        if epoch > *current {
            *current = epoch;
            true
        } else {
            false
        }
    });

    info!(
        target: "state-sync",
        epoch,
        "backwards walk: waiting for committee keys (triggered epoch record collector)"
    );

    let deadline = tokio::time::Instant::now() + COMMITTEE_KEYS_TIMEOUT;
    loop {
        if shutdown.noticed() {
            return None;
        }
        if let Some(keys) = config.get_committee_keys_for_epoch(epoch) {
            info!(target: "state-sync", epoch, "backwards walk: committee keys arrived");
            return Some(keys);
        }
        if tokio::time::Instant::now() >= deadline {
            warn!(target: "state-sync", epoch, "backwards walk: timed out waiting for committee keys");
            return None;
        }
        tokio::time::sleep(COMMITTEE_KEYS_POLL_INTERVAL).await;
    }
}

/// Republish `header` as the latest consensus tip when it advances the current watermark.
///
/// The compare-and-set is atomic under the watch lock, so it never overwrites a higher tip with a
/// lower one.
fn republish_if_higher(consensus_bus: &ConsensusBus, header: &ConsensusHeader) {
    let advanced = consensus_bus.last_consensus_header().send_if_modified(|current| {
        let advance = header.number > current.number;
        if advance {
            *current = header.clone();
        }
        advance
    });
    if advanced {
        info!(target: "state-sync", header_number = header.number, "notifying watchers");
    }
}

/// Descend one step of the backwards walk: stop at the canonical anchor, traverse a cached header,
/// or fetch and BLS-verify from a peer.
///
/// Returns `None` at the canonical anchor (the walk is grounded) or on failure; otherwise returns
/// the parent hash to continue descending and the number of the header just descended.
async fn get_consensus_header<DB: ReDatabase>(
    hash: B256,
    config: &ConsensusConfig<DB>,
    consensus_bus: &ConsensusBus,
    network: &PrimaryNetworkHandle,
    shutdown: &Noticer,
) -> Option<(B256, u64)> {
    let db = config.node_storage();
    // Stop only at the canonical anchor (never an unverified cache row); its per-row digest==hash
    // check means a stale, never-pruned ByDigest index entry cannot false-stop the walk.
    if let Some(block) = db.get_canonical_consensus_by_hash(hash) {
        republish_if_higher(consensus_bus, &block);
        return None;
    }
    // Traverse a cached header from the DB instead of refetching it, and keep descending.
    if let Some(cached) = db.get_consensus_by_hash(hash) {
        republish_if_higher(consensus_bus, &cached);
        return Some((cached.parent_hash, cached.number));
    }
    for attempt in 0u32..3 {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_millis(500 * attempt as u64)).await;
        }
        if shutdown.noticed() {
            return None;
        }
        match network.request_consensus(None, Some(hash)).await {
            Ok(header) => {
                let leader_epoch = header.sub_dag.leader_epoch();
                let Some(keys) =
                    wait_for_committee_keys(leader_epoch, config, consensus_bus, shutdown).await
                else {
                    warn!(
                        target: "state-sync",
                        ?hash,
                        epoch = leader_epoch,
                        "committee keys unavailable, cannot verify the header"
                    );
                    return None;
                };
                // The digest does not cover the signatures, so a peer can send the right header
                // with bad ones. Ask another peer rather than end the walk.
                let Ok(verified) = header.verify_header_with_keys(&keys).inspect_err(|e| {
                    warn!(target: "state-sync", ?hash, attempt, "BLS verification failed, discarding header: {e}");
                }) else {
                    continue;
                };
                let parent = verified.parent_hash;
                store_consensus_header_in_cache(db, &verified);
                republish_if_higher(consensus_bus, &verified);
                return Some((parent, verified.number));
            }
            Err(e) => {
                warn!(target: "state-sync", ?hash, ?e, attempt, "failed to fetch consensus header from peers");
            }
        }
    }
    None
}

/// Digest of the genesis consensus header (number 0, the default header).
///
/// A genuine number-1 header carries this as its parent hash, so a walk that descends to it has
/// reached the chain's base and is grounded.
static GENESIS_DIGEST: LazyLock<B256> = LazyLock::new(|| ConsensusHeader::default().digest());

/// Terminal state of a backwards walk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalkOutcome {
    /// Linked the start anchor to local coverage (canonical, genesis, or the session anchor).
    Covered,
    /// Stopped before reaching coverage (a failed fetch).
    Incomplete,
    /// Stopped to walk to an epoch end first, because its record landed (see
    /// [`WalkSession::diverts`]).
    Diverted,
    /// Shutdown was noticed mid-walk.
    Shutdown,
}

/// Recovers the covered anchor: the highest cached header that hash-links, number by number, down
/// to the canonical tip (or genesis).
///
/// Re-verifies each parent link rather than trusting a row's presence, since an interrupted descent
/// can leave another fork's rows behind. [`ConsensusHeaderChainMeta`] projects each link from the
/// raw bytes without deserializing.
fn derive_covered_anchor<DB: ReDatabase>(db: &DB) -> Option<B256> {
    let (mut expected_parent, mut expected_number) = match db
        .reverse_raw_iter::<ConsensusBlocks>()
        .next()
        .map(|(_, value)| ConsensusHeaderChainMeta::from_bytes(&value))
    {
        Some(Ok(meta)) => (meta.digest, meta.number + 1),
        // an unreadable tip row proves nothing; let the walk re-fetch
        Some(Err(_)) => return None,
        None => (*GENESIS_DIGEST, 1),
    };
    let mut frontier = None;
    for (_, value) in db.raw_iter::<ConsensusBlocksCache>() {
        // an unreadable row ends the chain at the row below it
        let Ok(meta) = ConsensusHeaderChainMeta::from_bytes(&value) else { break };
        if meta.number < expected_number {
            // a stale row below the canonical tip
            continue;
        }
        // a gap or a broken/fork-overwritten link ends the proven run here
        if meta.number != expected_number || meta.parent_hash != expected_parent {
            break;
        }
        expected_parent = meta.digest;
        frontier = Some((meta.digest, meta.number));
        expected_number += 1;
    }
    frontier.map(|(digest, number)| {
        info!(target: "state-sync", anchor = number, "derived walk coverage from cached headers");
        digest
    })
}

/// One tracker session of backwards walks, owning the coverage anchor that outlives a single walk.
///
/// In-memory on purpose: teardown drops it, and it is re-derived from the persisted cache when
/// unset, so coverage survives a restart.
pub(crate) struct WalkSession<DB> {
    config: ConsensusConfig<DB>,
    consensus_bus: ConsensusBus,
    network: PrimaryNetworkHandle,
    shutdown: Noticer,
    /// Digest where the last successful walk reached coverage; a later walk stops here instead of
    /// re-descending. Cleared on an incomplete walk (stale rows may lie below it), then
    /// re-derived.
    covered: Option<B256>,
    /// Lowest epoch whose end may still need a checkpoint walk; every epoch below it ends at or
    /// below a header a checkpoint walk already covered.
    next_checkpoint: Epoch,
    /// Highest target epoch whose records this session has asked the collector for.
    records_requested: Epoch,
    /// Epochs whose ends the next walk to a target diverts to once their records land, set when
    /// their records are asked for.
    divert: Option<Range<Epoch>>,
}

impl<DB: ReDatabase> WalkSession<DB> {
    /// Create a session for one tracker task lifetime.
    pub(crate) fn new(
        config: ConsensusConfig<DB>,
        consensus_bus: ConsensusBus,
        network: PrimaryNetworkHandle,
    ) -> Self {
        let shutdown = config.shutdown().subscribe();
        Self {
            config,
            consensus_bus,
            network,
            shutdown,
            covered: None,
            next_checkpoint: 0,
            records_requested: 0,
            divert: None,
        }
    }

    /// Walk backwards from `start_hash` to local coverage, recording the result for the next call.
    /// Walks to the end of each closed epoch below it first (see
    /// [`Self::walk_epoch_checkpoints`]). If a record lands during the walk to `start_hash`, it
    /// walks to the epoch ends again and then to `start_hash`, at most once per call. Returns
    /// `Break` when shutdown was noticed and the tracker must exit.
    pub(crate) async fn sync_to(&mut self, start_hash: B256) -> ControlFlow<()> {
        loop {
            self.refresh_coverage().await;
            if self.walk_epoch_checkpoints(start_hash).await.is_break() {
                return ControlFlow::Break(());
            }
            // The target could not be fetched. Walking to it would only ask for it again, so leave
            // the rest of the gap to the next tip.
            if !self.is_local(start_hash) {
                return ControlFlow::Continue(());
            }
            // Only this walk may divert: the records were asked for once, so the walk to the
            // target after the epoch ends runs to the end.
            let divert = self.divert.take();
            match self.walk(start_hash, divert.as_ref()).await {
                // `start_hash` now heads a verified chain to canonical; stop here next time.
                WalkOutcome::Covered => {
                    self.covered = Some(start_hash);
                    return ControlFlow::Continue(());
                }
                // Drop the anchor so the next walk re-derives and re-descends to self-heal the gap.
                WalkOutcome::Incomplete => {
                    self.covered = None;
                    return ControlFlow::Continue(());
                }
                // A record landed: walk to the epoch ends first, then to the target again. The rows
                // this walk stored lie above the anchor, so the anchor holds, and the next walk
                // reads them from storage.
                WalkOutcome::Diverted => {}
                WalkOutcome::Shutdown => return ControlFlow::Break(()),
            }
        }
    }

    /// Recover coverage from the persisted cache when the session has none (fresh task, or a prior
    /// walk cleared it), so this walk stops at the top of the cached run rather than re-descending
    /// it. Derivation sweeps and hashes every cached row, so it runs on a blocking thread to keep
    /// the async worker responsive through a long catch-up stall; a join failure just leaves the
    /// anchor unset and re-derives next call.
    async fn refresh_coverage(&mut self) {
        if self.covered.is_none() {
            let db = self.config.node_storage().clone();
            self.covered = tokio::task::spawn_blocking(move || derive_covered_anchor(&db))
                .await
                .unwrap_or(None);
        }
    }

    /// Walk to the last header of each closed epoch first, oldest first, so the forward streamer
    /// can execute one epoch while the walk fetches the next.
    ///
    /// The streamer executes stored headers from its anchor up and stops at the first missing one.
    /// A walk stores its gap from the top down, so with only the walk to `start_hash`, nothing
    /// executes until the whole gap is walked. A certified epoch record names the digest of its
    /// epoch's last consensus header, which makes it a trusted start for a walk, as `start_hash`
    /// is. Walking to each in turn stores the gap one epoch at a time from the bottom.
    ///
    /// An epoch with no certified record is covered by the walk to `start_hash`, which comes back
    /// here if the record lands meanwhile (see [`Self::diverts`]). A walk to an epoch end that
    /// does not complete ends this, and the walk to `start_hash` covers the rest without coming
    /// back, as without records.
    async fn walk_epoch_checkpoints(&mut self, start_hash: B256) -> ControlFlow<()> {
        let db = self.config.node_storage().clone();
        // Fetching the target stores it for the walk to the target, and makes the record collector
        // fetch the records below it when the target's committee keys are missing. The walks to
        // the epoch ends need only the records already stored, so a failed fetch does not stop
        // them.
        if !self.is_local(start_hash)
            && get_consensus_header(
                start_hash,
                &self.config,
                &self.consensus_bus,
                &self.network,
                &self.shutdown,
            )
            .await
            .is_none()
            && self.shutdown.noticed()
        {
            return ControlFlow::Break(());
        }
        let tip_epoch =
            db.last_record::<ConsensusBlocks>().map_or(0, |(_, tip)| tip.sub_dag.leader_epoch());
        if let Some(target) = db.get_consensus_by_hash(start_hash) {
            self.request_epoch_records(target.sub_dag.leader_epoch(), tip_epoch);
        }
        let mut epoch = tip_epoch.max(self.next_checkpoint);
        // Records the collector stores meanwhile are walked too.
        while db.last_record::<EpochRecords>().is_some_and(|(last, _)| epoch <= last) {
            let current = epoch;
            epoch += 1;
            // Only a certified record is trusted. The collector stores one only once it anchors
            // to the record before it, or a super-quorum of that epoch's committee attests it.
            let Some((record, Some(_))) = db.get_epoch_by_number(current) else { continue };
            let checkpoint = record.parent_consensus;
            if !self.is_local(checkpoint) {
                info!(target: "state-sync", epoch = current, "backwards walk: walking to the end of an epoch");
                match self.walk(checkpoint, None).await {
                    WalkOutcome::Covered => self.covered = Some(checkpoint),
                    // Rows this walk stored lie above the covered anchor, so the anchor holds. The
                    // walk to the target covers the rest, so it must not divert back to this end,
                    // which it would while the end is missing. An epoch-end walk is given no
                    // epochs to divert to, so it never returns `Diverted`.
                    WalkOutcome::Incomplete | WalkOutcome::Diverted => {
                        self.divert = None;
                        break;
                    }
                    WalkOutcome::Shutdown => return ControlFlow::Break(()),
                }
            }
            self.next_checkpoint = current + 1;
        }
        ControlFlow::Continue(())
    }

    /// Make the record collector fetch the records from the node's epoch up to the target's, once
    /// per target epoch, and have the next walk to the target divert to their epoch ends once they
    /// land.
    ///
    /// Fetching the target asks the collector for records only when the target's committee keys
    /// are missing. A node one or two epochs behind already has those keys from the committee
    /// lookahead, so without this it would never fetch the records for its gap, and would get no
    /// walk to an epoch end in the most common case.
    ///
    /// The walk does not wait for them: at an epoch change the last record may not be certified
    /// anywhere yet. It walks to the target at once and diverts when a record lands (see
    /// [`Self::diverts`]), so a node that follows the tip never stops for a record.
    ///
    /// A node that closed that epoch itself, which it has when it runs a later one, has already
    /// executed the epoch's last header, where the record would send the walk, so it does not
    /// ask.
    fn request_epoch_records(&mut self, target_epoch: Epoch, tip_epoch: Epoch) {
        // The last epoch that closed below the target, whose record ends the node's gap.
        let Some(last_closed) = target_epoch.checked_sub(1) else { return };
        // The node holds the end of every epoch below its tip's and below the one it runs.
        let node_epoch = tip_epoch.max(self.config.epoch());
        if last_closed < node_epoch || self.records_requested >= target_epoch {
            return;
        }
        self.records_requested = target_epoch;
        // A stored record is walked to before the target, so there is nothing to divert to.
        let db = self.config.node_storage();
        if matches!(db.get_epoch_by_number(last_closed), Some((_, Some(_)))) {
            return;
        }
        // The collector fetches every record from the last one it holds upward, so asking for
        // this one also fetches those below it. It acts only on an epoch above the last record it
        // holds, which counts as 0 when it holds none, so epoch 0's record is asked for as
        // epoch 1. The collector then also tries epoch 1's record, which may not exist yet, a
        // bounded number of times, as it does when a key wait asks for the target's epoch.
        let request = last_closed.max(1);
        self.consensus_bus.requested_missing_epoch().send_if_modified(|current| {
            let raise = request > *current;
            if raise {
                *current = request;
            }
            raise
        });
        info!(target: "state-sync", epoch = last_closed, "backwards walk: asked for epoch records");
        self.divert = Some(node_epoch..target_epoch);
    }

    /// True when a walk given `epochs` should stop and walk to an epoch end first: the oldest of
    /// those epochs whose end is not stored has a certified record.
    ///
    /// Records land oldest first, so the first epoch without a certified record ends the search.
    /// The walk to the target stores the gap from the top down, so once it reaches an epoch's end
    /// every later end is stored too, and it no longer diverts. It reads the record table each
    /// time, so a record from any writer counts, not only from the collector.
    fn diverts(&self, epochs: Option<&Range<Epoch>>) -> bool {
        let Some(epochs) = epochs else { return false };
        let db = self.config.node_storage();
        for epoch in epochs.clone() {
            let Some((record, Some(_))) = db.get_epoch_by_number(epoch) else { return false };
            if !self.is_local(record.parent_consensus) {
                return true;
            }
        }
        false
    }

    /// Descend parent hashes from `start_hash` until reaching local coverage, or until
    /// [`Self::diverts`] says to walk to an epoch end in `divert` first.
    async fn walk(&self, start_hash: B256, divert: Option<&Range<Epoch>>) -> WalkOutcome {
        let db = self.config.node_storage();
        let covered_hash = self.covered;
        let mut current = start_hash;
        let mut depth = 0u64;
        // Prefetched replies not yet linked, kept across walk steps so a bad reply costs only a
        // fetch of that one header by hash, not a refetch of the replies below it.
        let mut held = HashMap::new();
        let outcome = loop {
            if self.shutdown.noticed() {
                return WalkOutcome::Shutdown;
            }
            // Stop at proven coverage without re-descending it: the session anchor or genesis.
            if covered_hash == Some(current) || current == *GENESIS_DIGEST {
                break WalkOutcome::Covered;
            }
            if self.diverts(divert) {
                info!(target: "state-sync", "backwards walk: epoch records landed, walking to the epoch ends first");
                break WalkOutcome::Diverted;
            }
            match get_consensus_header(
                current,
                &self.config,
                &self.consensus_bus,
                &self.network,
                &self.shutdown,
            )
            .await
            {
                Some((parent, number)) => {
                    let (next, prefetched) =
                        self.prefetch_below(parent, number, &mut held, divert).await;
                    current = next;
                    let before = depth;
                    depth += 1 + prefetched;
                    if depth / 100 > before / 100 {
                        info!(target: "state-sync", depth, "backwards walk: progress");
                    }
                }
                // `get_consensus_header` stopped at the canonical anchor (Covered) or could not
                // fetch (Incomplete: coverage below `current` is unknown).
                None => {
                    break if db.get_canonical_consensus_by_hash(current).is_some() {
                        WalkOutcome::Covered
                    } else {
                        WalkOutcome::Incomplete
                    };
                }
            }
            tokio::task::yield_now().await;
        };
        if depth > 0 {
            info!(target: "state-sync", depth, ?outcome, "backwards walk: completed");
        }
        outcome
    }

    /// True when the walk needs no peer for `hash`: it is the session anchor, genesis, or a header
    /// already stored.
    fn is_local(&self, hash: B256) -> bool {
        let db = self.config.node_storage();
        self.covered == Some(hash)
            || hash == *GENESIS_DIGEST
            || db.get_canonical_consensus_by_hash(hash).is_some()
            || db.get_consensus_by_hash(hash).is_some()
    }

    /// Prefetch the headers below the trusted header at `above`, whose parent hash is `expected`,
    /// instead of fetching them one hash at a time.
    ///
    /// Each round asks for up to [`WALK_PREFETCH`] numbers by number at once, then checks them from
    /// the top down: each must hash to the parent hash of the header above it and pass BLS, which
    /// runs in parallel over the linked run. The checked run is stored in one write. So every
    /// stored header is still hash-linked to a trusted one above it, as with a fetch by hash. A
    /// full round goes on to the next; the first missing, mismatched or unverifiable reply ends
    /// prefetching, and the walk fetches that header by hash from another peer. Nothing at or
    /// below the canonical tip or the session's covered anchor is requested.
    ///
    /// Replies below the bad one stay in `held` and are linked again once the walk has fetched
    /// the bad one by hash, so they are not requested twice. Each is held with whether it already
    /// passed BLS, so one that did is not verified twice. `held` only holds numbers in the current
    /// round, so it never has more than [`WALK_PREFETCH`] headers.
    ///
    /// One call usually descends the whole gap, so it also stops before a round when
    /// [`Self::diverts`] says to walk to an epoch end in `divert` first.
    ///
    /// Returns the parent hash to continue descending from and how many headers were stored.
    async fn prefetch_below(
        &self,
        mut expected: B256,
        mut above: u64,
        held: &mut HashMap<u64, (ConsensusHeader, bool)>,
        divert: Option<&Range<Epoch>>,
    ) -> (B256, u64) {
        let db = self.config.node_storage();
        // Anything at or above `above` is settled: stored, or fetched by hash.
        held.retain(|number, _| *number < above);
        let mut floor = None;
        let mut stored = 0u64;
        loop {
            if self.shutdown.noticed() || self.is_local(expected) || self.diverts(divert) {
                break;
            }
            // Below the canonical tip and the covered anchor the walk needs nothing from a peer.
            let floor = *floor.get_or_insert_with(|| {
                let canonical = db.last_record::<ConsensusBlocks>().map_or(0, |(number, _)| number);
                let covered = self
                    .covered
                    .and_then(|hash| db.get_consensus_by_hash(hash))
                    .map_or(0, |header| header.number);
                canonical.max(covered) + 1
            });
            let low = above.saturating_sub(WALK_PREFETCH).max(floor);
            if low >= above {
                break;
            }
            let network = &self.network;
            let mut fetches: FuturesUnordered<_> = (low..above)
                .filter(|number| !held.contains_key(number))
                .map(|number| async move {
                    (number, network.request_consensus(Some(number), None).await)
                })
                .collect();
            while let Some((number, reply)) = fetches.next().await {
                match reply {
                    Ok(header) => {
                        held.insert(number, (header, false));
                    }
                    Err(e) => debug!(target: "state-sync", number, ?e, "prefetch: no reply"),
                }
            }

            // Link check first, from the top down: it is cheap and fixes which replies can be
            // stored at all. A reply that does not link is dropped.
            let mut linked = Vec::new();
            let mut next = expected;
            for number in (low..above).rev() {
                if self.is_local(next) {
                    break;
                }
                let Some((header, verified)) = held.remove(&number) else { break };
                if header.digest() != next {
                    warn!(
                        target: "state-sync",
                        number,
                        "prefetch: reply does not link to the header above, fetching it by hash"
                    );
                    break;
                }
                next = header.parent_hash;
                linked.push((header, verified));
            }
            // BLS dominates the walk's cost (a pairing check per certificate in each sub-dag), so
            // verify the linked run in parallel and keep the prefix that verifies.
            let verifications = linked.into_iter().map(|(header, verified)| {
                let (number, epoch) = (header.number, header.sub_dag.leader_epoch());
                let keys = self.config.get_committee_keys_for_epoch(epoch);
                async move {
                    let result = match keys {
                        _ if verified => Ok(header),
                        None => Err(format!("no committee keys for epoch {epoch}")),
                        Some(keys) => tokio::task::spawn_blocking(move || {
                            header.verify_header_with_keys(&keys).map_err(|e| e.to_string())
                        })
                        .await
                        .unwrap_or_else(|e| {
                            error!(target: "state-sync", number, "prefetch: verification task failed: {e}");
                            Err(format!("verification task failed: {e}"))
                        }),
                    };
                    (number, epoch, result)
                }
            });
            let mut results = futures::future::join_all(verifications).await.into_iter().map(
                |(number, epoch, result)| {
                    result
                        .inspect_err(|e| {
                            warn!(
                                target: "state-sync",
                                number,
                                epoch,
                                "prefetch: could not verify reply, fetching it by hash: {e}"
                            )
                        })
                        .ok()
                },
            );
            let checked: Vec<ConsensusHeader> = results.by_ref().map_while(|v| v).collect();
            // Headers below one that failed BLS still link to the trusted chain, since the failed
            // one has the right digest. Hold them, marked verified, for the next pass instead of
            // fetching and verifying them again.
            for header in results.flatten() {
                held.insert(header.number, (header, true));
            }
            if let Some(lowest) = checked.last() {
                expected = lowest.parent_hash;
            }
            let full_round = checked.len() as u64 == above - low;
            store_consensus_headers_in_cache(db, &checked);
            stored += checked.len() as u64;
            if !full_round {
                break;
            }
            above = low;
            tokio::task::yield_now().await;
        }
        (expected, stored)
    }
}

/// Whether this node finds the network head from peers' certified tips: once the `CertifiedTipV1`
/// fork is active at the next block to execute, the point every other fork check uses.
/// `activation` is the fork's block, `None` while it is `Never`. Not before: until this node has
/// executed the activation block it cannot tell whether its peers serve certified tips yet, and
/// the old probe is the one every peer answers, so a lagging node can still catch up past
/// activation. The cost is that such a node, including one bootstrapping from below the
/// activation block after the network passed it, can still be forked or stranded by one member
/// on that probe (#312, #370).
fn uses_certified_tip(activation: Option<u64>, executed_tip: u64) -> bool {
    activation.is_some_and(|block| executed_tip.saturating_add(1) >= block)
}

/// Find a network head above `current` to walk to, as its number and hash. Before the
/// `CertifiedTipV1` fork, the single highest latest header any committee member reports; after,
/// a certified tip, which one member alone cannot forge (#370, #312).
async fn probe_for_head<DB: ReDatabase>(
    current: u64,
    config: &ConsensusConfig<DB>,
    consensus_bus: &ConsensusBus,
    network: &PrimaryNetworkHandle,
) -> Option<(u64, B256)> {
    let executed_tip =
        consensus_bus.recently_executed_blocks().borrow().latest_block_num_hash().number;
    if uses_certified_tip(config.certified_tip_v1_block(), executed_tip) {
        probe_peers_for_certified_tip(current, config, consensus_bus, network).await
    } else {
        probe_peers_for_latest(current, config, network).await.map(|h| (h.number, h.digest()))
    }
}

/// Ask every connected peer, observers included, for its certified tip and return the highest
/// one above `current` that verifies against its epoch's committee. One valid answer is enough;
/// asking every peer only guards against peers that are down, on an older release, or behind.
/// Answers that have not arrived within [`CERTIFIED_TIP_PROBE_DEADLINE`] are not waited for.
/// A tip from before this node's previous epoch is dropped: f+1 keys of a retired committee
/// could sign any number, and the previous epoch counts so a chain halted on its last header can
/// still be found.
/// A tip from an epoch whose committee is not settled yet, from config or a certified record (the
/// lookahead does not count), is skipped, and the epoch record collector is asked for the record
/// after the newest one held: it walks upward as far as peers serve, so a later probe can verify
/// the tip. Neither waiting here nor requesting the tip's own
/// epoch, which is unverified: a peer naming a far epoch would hold up the probe or send the
/// collector chasing it.
async fn probe_peers_for_certified_tip<DB: ReDatabase>(
    current: u64,
    config: &ConsensusConfig<DB>,
    consensus_bus: &ConsensusBus,
    network: &PrimaryNetworkHandle,
) -> Option<(u64, B256)> {
    let peers = network.inner_handle().connected_peers().await.ok()?;
    let mut inflight: FuturesUnordered<_> =
        peers.into_iter().map(|peer| network.request_certified_tip_from_peer(peer)).collect();
    let floor = config.epoch().saturating_sub(1);
    let mut candidates = Vec::new();
    let _ = tokio::time::timeout(CERTIFIED_TIP_PROBE_DEADLINE, async {
        while let Some(result) = inflight.next().await {
            if let Ok(Some(tip)) = result {
                if tip.number > current && tip.epoch >= floor {
                    candidates.push(tip);
                }
            }
        }
    })
    .await;
    // epoch first: a high number signed by f+1 retired keys of an earlier epoch must not rank
    // above an honest tip from a later one
    candidates.sort_by(|a, b| (b.epoch, b.number).cmp(&(a.epoch, a.number)));
    // whole-struct: a copy with the honest number and hash but a bad signature must not stand
    // in for the honest ones
    candidates.dedup();
    let mut collector_asked = false;
    for tip in candidates {
        // the committee the tip's epoch ran with, never the lookahead: an honest tip fails
        // against a lookahead that still lists a key ejected since it was read
        let Some(committee) = config.get_settled_committee_keys_for_epoch(tip.epoch) else {
            // the same record is next for every such tip, so ask once
            if !collector_asked {
                collector_asked = true;
                let next = config
                    .node_storage()
                    .last_record::<EpochRecords>()
                    .map_or(0, |(epoch, _)| epoch)
                    .saturating_add(1);
                consensus_bus.requested_missing_epoch().send_if_modified(|requested| {
                    let higher = next > *requested;
                    if higher {
                        *requested = next;
                    }
                    higher
                });
            }
            continue;
        };
        if tip.verify(&committee) {
            return Some((tip.number, tip.hash));
        }
        warn!(
            target: "state-sync",
            number = tip.number,
            epoch = tip.epoch,
            "probe: certified tip did not verify against its committee, skipping"
        );
    }
    None
}

/// Probe all committee peers concurrently, return the highest header above `current`.
/// BLS is re-verified downstream in `get_consensus_header`.
///
/// Never call this once `CertifiedTipV1` is active (see [`uses_certified_tip`]): the answer of one
/// member is taken on its own, and a header carries no signature of its own, so one member can
/// serve a forged copy of a real header and fork or strand the node (#370, #312). It stays only
/// for nodes before the fork, which still talk to peers that cannot serve a certified tip.
async fn probe_peers_for_latest<DB: ReDatabase>(
    current: u64,
    config: &ConsensusConfig<DB>,
    network: &PrimaryNetworkHandle,
) -> Option<ConsensusHeader> {
    let mut inflight: FuturesUnordered<_> = config
        .committee()
        .others_primaries_by_id(config.authority_id().as_ref())
        .into_iter()
        .map(|(_, peer)| network.request_consensus_from_peer(peer, None, None))
        .collect();
    let mut best: Option<ConsensusHeader> = None;
    while let Some(result) = inflight.next().await {
        if let Ok(h) = result {
            if h.number > current && best.as_ref().map(|b| h.number > b.number).unwrap_or(true) {
                best = Some(h);
            }
        }
    }
    best
}

/// Blocks until a backwards walk is warranted, returning the tip hash to walk toward.
///
/// Resolves at once if gossip holds an unobserved tip, else waits for gossip, falling back to a
/// periodic peer probe. Returns `None` on shutdown or a closed gossip channel.
async fn next_walk_target<DB: ReDatabase>(
    rx_gossip: &mut watch::Receiver<(u64, B256)>,
    consensus_bus: &ConsensusBus,
    config: &ConsensusConfig<DB>,
    network: &PrimaryNetworkHandle,
    shutdown: &Noticer,
) -> Option<B256> {
    // Counts consecutive idle probe ticks for the log; reset implicitly by returning a target.
    let mut idle_intervals = 0u64;
    loop {
        tokio::select! {
            // biased: shutdown must win; gossip and the probe timer otherwise commute.
            biased;
            _ = shutdown => return None,
            result = rx_gossip.changed() => {
                result.ok()?;
                let (_, hash) = *rx_gossip.borrow_and_update();
                return Some(hash);
            }
            _ = tokio::time::sleep(PROBE_INTERVAL) => {
                idle_intervals += 1;
                let current = consensus_bus.last_consensus_header().borrow().number;
                info!(
                    target: "state-sync",
                    current,
                    idle_secs = idle_intervals * PROBE_INTERVAL.as_secs(),
                    "track_recent_consensus: no gossip received, probing peers"
                );
                if let Some((number, hash)) =
                    probe_for_head(current, config, consensus_bus, network).await
                {
                    info!(
                        target: "state-sync",
                        current,
                        peer_latest = number,
                        "probe discovered newer consensus header from peer"
                    );
                    return Some(hash);
                }
                info!(target: "state-sync", "probe: no peer returned a newer header");
            }
        }
    }
}

/// Walks parent hashes backwards from gossip tips until reaching local state.
///
/// Runs one [`WalkSession`] walk at a time; mid-walk tips are coalesced by the gossip watch and
/// re-trigger the next iteration, which resumes from the session's covered anchor.
pub(crate) async fn spawn_track_recent_consensus<DB: ReDatabase>(
    config: ConsensusConfig<DB>,
    consensus_bus: ConsensusBus,
    network: PrimaryNetworkHandle,
) -> eyre::Result<()> {
    let rx_shutdown = config.shutdown().subscribe();
    let mut rx_gossip_update = consensus_bus.last_published_consensus_num_hash().subscribe();
    let mut session = WalkSession::new(config.clone(), consensus_bus.clone(), network.clone());

    // Probe at startup so the first walk begins without waiting for a gossip tip.
    let current = consensus_bus.last_consensus_header().borrow().number;
    if let Some((number, hash)) = probe_for_head(current, &config, &consensus_bus, &network).await {
        info!(
            target: "state-sync",
            current,
            peer_latest = number,
            "startup probe discovered newer consensus header from peer"
        );
        if session.sync_to(hash).await.is_break() {
            return Ok(());
        }
    }

    loop {
        let Some(hash) = next_walk_target(
            &mut rx_gossip_update,
            &consensus_bus,
            &config,
            &network,
            &rx_shutdown,
        )
        .await
        else {
            return Ok(());
        };

        let current_latest = consensus_bus.last_consensus_header().borrow().number;
        info!(target: "state-sync", current_latest, "starting backwards walk");

        if session.sync_to(hash).await.is_break() {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests;
