//! Tasks and helpers for collecting consensus headers trustlessly.
use futures::{stream::FuturesUnordered, StreamExt};
use rayls_consensus_primary::{
    network::{is_no_consensus_output, NetworkError, PrimaryNetworkHandle},
    ConsensusBus,
};
use rayls_infrastructure_config::ConsensusConfig;
use rayls_infrastructure_storage::{
    tables::{ConsensusBlockNumbersByDigest, ConsensusBlocks, ConsensusBlocksCache},
    ConsensusStore,
};
use rayls_infrastructure_types::{
    BlsPublicKey, ConsensusHeader, ConsensusHeaderChainMeta, Database as ReDatabase, DbTxMut as _,
    Epoch, Noticer, B256,
};
use std::{collections::BTreeSet, ops::ControlFlow, sync::LazyLock, time::Duration};
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

const COMMITTEE_KEYS_TIMEOUT: Duration = Duration::from_secs(30);
const COMMITTEE_KEYS_POLL_INTERVAL: Duration = Duration::from_millis(500);
/// How long to wait for a gossip tip before actively probing peers for the network head.
const PROBE_INTERVAL: Duration = Duration::from_secs(10);
/// Overall wall-clock budget for the boot-time peer-head probe.
pub const BOOT_HEAD_PROBE_TIMEOUT: Duration = Duration::from_secs(10);
/// How long to wait between boot-time peer-head probe rounds.
const BOOT_HEAD_PROBE_RETRY_INTERVAL: Duration = Duration::from_millis(500);
/// Upper bound on one boot-time peer request, so an unresponsive peer cannot consume the whole
/// probe budget and starve the retries that reach a peer still connecting.
const BOOT_HEAD_PROBE_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

/// Cache a consensus header in persistent storage.
pub fn store_consensus_header_in_cache<DB: ReDatabase>(db: &DB, header: &ConsensusHeader) {
    let result = db.with_write_txn(|txn| {
        txn.insert::<ConsensusBlocksCache>(&header.number, header)?;
        txn.insert::<ConsensusBlockNumbersByDigest>(&header.digest(), &header.number)?;
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
/// the parent hash to continue descending.
async fn get_consensus_header<DB: ReDatabase>(
    hash: B256,
    config: &ConsensusConfig<DB>,
    consensus_bus: &ConsensusBus,
    network: &PrimaryNetworkHandle,
    shutdown: &Noticer,
) -> Option<B256> {
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
        return Some(cached.parent_hash);
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
                        "committee keys unavailable, stopping walk"
                    );
                    return None;
                };
                let Ok(verified) = header.verify_header_with_keys(&keys).inspect_err(|e| {
                    warn!(target: "state-sync", ?hash, "BLS verification failed, discarding header: {e}");
                }) else {
                    return None;
                };
                let parent = verified.parent_hash;
                store_consensus_header_in_cache(db, &verified);
                republish_if_higher(consensus_bus, &verified);
                return Some(parent);
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
}

impl<DB: ReDatabase> WalkSession<DB> {
    /// Create a session for one tracker task lifetime.
    pub(crate) fn new(
        config: ConsensusConfig<DB>,
        consensus_bus: ConsensusBus,
        network: PrimaryNetworkHandle,
    ) -> Self {
        let shutdown = config.shutdown().subscribe();
        Self { config, consensus_bus, network, shutdown, covered: None }
    }

    /// Walk backwards from `start_hash` to local coverage, recording the result for the next call.
    /// Returns `Break` when shutdown was noticed and the tracker must exit.
    pub(crate) async fn sync_to(&mut self, start_hash: B256) -> ControlFlow<()> {
        // Recover coverage from the persisted cache when the session has none yet (fresh task, or a
        // prior walk cleared it), so this walk stops at the top of the cached run rather than
        // re-descending it. Derivation sweeps and hashes every cached row, so it runs on a blocking
        // thread to keep the async worker responsive through a long catch-up stall; a join failure
        // just leaves the anchor unset and re-derives next call.
        if self.covered.is_none() {
            let db = self.config.node_storage().clone();
            self.covered = tokio::task::spawn_blocking(move || derive_covered_anchor(&db))
                .await
                .unwrap_or(None);
        }
        match self.walk(start_hash).await {
            // `start_hash` now heads a verified chain to canonical; stop here next time.
            WalkOutcome::Covered => {
                self.covered = Some(start_hash);
                ControlFlow::Continue(())
            }
            // Drop the anchor so the next walk re-derives and re-descends to self-heal the gap.
            WalkOutcome::Incomplete => {
                self.covered = None;
                ControlFlow::Continue(())
            }
            WalkOutcome::Shutdown => ControlFlow::Break(()),
        }
    }

    /// Descend parent hashes from `start_hash` until reaching local coverage.
    async fn walk(&self, start_hash: B256) -> WalkOutcome {
        let db = self.config.node_storage();
        let covered_hash = self.covered;
        let mut current = start_hash;
        let mut depth = 0u64;
        let outcome = loop {
            if self.shutdown.noticed() {
                return WalkOutcome::Shutdown;
            }
            // Stop at proven coverage without re-descending it: the session anchor or genesis.
            if covered_hash == Some(current) || current == *GENESIS_DIGEST {
                break WalkOutcome::Covered;
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
                Some(parent) => current = parent,
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
            depth += 1;
            if depth.is_multiple_of(100) {
                info!(target: "state-sync", depth, "backwards walk: progress");
            }
            tokio::task::yield_now().await;
        };
        if depth > 0 {
            info!(target: "state-sync", depth, ?outcome, "backwards walk: completed");
        }
        outcome
    }
}

/// Probe all committee peers concurrently, returning the highest header above `current`.
///
/// Responses are not BLS-verified; callers that rely on a header must verify it
/// themselves (the state-sync path re-verifies it in `get_consensus_header`).
/// Returns `None` when no peer answers, or when every answering peer reports a
/// header at or below `current`.
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

/// What a boot-time peer-head probe found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootHeadProbe {
    /// No other committee primary exists: there is nobody to ask.
    NoPeers,
    /// At least one peer answered. `head` is a non-zero consensus-output number reported by one
    /// of them, or `None` when every answering committee peer has produced no output yet (a
    /// chain still at genesis).
    Responded { head: Option<u64> },
    /// No committee peer reported its chain state before the probe budget expired (all
    /// unreachable, their response channels closed, only connected non-committee peers answered,
    /// or every answer was an RPC error that does not report the chain's state).
    NoResponse,
}

/// Probe the genesis committee and the peers this node is currently connected to for the
/// chain's consensus head, retrying until [`BOOT_HEAD_PROBE_TIMEOUT`] expires.
///
/// The target set is refreshed every round, so a peer that is still connecting gets another
/// chance before the budget runs out. Only
/// [`NO_CONSENSUS_OUTPUT`](rayls_consensus_primary::network::NO_CONSENSUS_OUTPUT) counts as an
/// answer from genesis; other RPC errors are retried because they may just mean the peer has not
/// mapped this node yet.
///
/// Returns the first non-zero head found, `None` when the genesis committee reports genesis, and
/// [`BootHeadProbe::NoResponse`] when no peer reports its chain state.
pub async fn probe_peers_for_boot_head<DB: ReDatabase>(
    config: &ConsensusConfig<DB>,
    network: &PrimaryNetworkHandle,
) -> BootHeadProbe {
    let committee: BTreeSet<BlsPublicKey> = config
        .committee()
        .others_primaries_by_id(config.authority_id().as_ref())
        .into_iter()
        .map(|(_, peer)| peer)
        .collect();
    if committee.is_empty() {
        return BootHeadProbe::NoPeers;
    }

    let mut saw_genesis = false;
    let probe = async {
        loop {
            // Refresh each round: a peer that was still dialing at boot becomes reachable, and a
            // joining node may discover a different set of peers while the probe runs.
            let connected = network.inner_handle().connected_peers().await.unwrap_or_default();
            let targets: BTreeSet<BlsPublicKey> =
                committee.iter().copied().chain(connected).collect();

            let mut inflight: FuturesUnordered<_> = targets
                .iter()
                .map(|&peer| async move {
                    let answer = tokio::time::timeout(
                        BOOT_HEAD_PROBE_REQUEST_TIMEOUT,
                        network.request_consensus_from_peer(peer, None, None),
                    )
                    .await;
                    let head = match answer {
                        Ok(Ok(header)) => Some(header.number),
                        // A chain still at genesis cannot report a header at number 0, so a
                        // peer with no output answers with the explicit no-output RPC error.
                        Ok(Err(NetworkError::RPCError(msg))) if is_no_consensus_output(&msg) => {
                            Some(0)
                        }
                        // Any other failure - transport, timeout, or an RPC error that does not
                        // report the chain's state - is no usable answer.
                        _ => None,
                    };
                    (peer, head)
                })
                .collect();

            let mut genesis_answers = 0usize;
            while let Some((peer, head)) = inflight.next().await {
                match head {
                    Some(number) if number > 0 => {
                        info!(
                            target: "state-sync",
                            peer = %peer,
                            head = number,
                            "boot head probe: peer reported consensus output"
                        );
                        return BootHeadProbe::Responded { head: Some(number) };
                    }
                    // Only the genesis committee can establish a new chain; a connected peer
                    // outside it merely proves the chain exists when it reports a head.
                    Some(_) if committee.contains(&peer) => {
                        saw_genesis = true;
                        genesis_answers += 1;
                    }
                    _ => {}
                }
            }

            if genesis_answers == committee.len() {
                info!(
                    target: "state-sync",
                    peers = genesis_answers,
                    "boot head probe: every committee peer is at genesis"
                );
                return BootHeadProbe::Responded { head: None };
            }

            debug!(
                target: "state-sync",
                "boot head probe: no conclusive answer, retrying"
            );
            tokio::time::sleep(BOOT_HEAD_PROBE_RETRY_INTERVAL).await;
        }
    };

    match tokio::time::timeout(BOOT_HEAD_PROBE_TIMEOUT, probe).await {
        Ok(outcome) => outcome,
        // A partial set of genesis answers is still a genesis report: the remaining peers were
        // unreachable for the whole budget and cannot contradict it. Only with no genesis answer
        // at all is the chain's state unknown.
        Err(_) if saw_genesis => BootHeadProbe::Responded { head: None },
        Err(_) => BootHeadProbe::NoResponse,
    }
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
                if let Some(header) = probe_peers_for_latest(current, config, network).await {
                    info!(
                        target: "state-sync",
                        current,
                        peer_latest = header.number,
                        "probe discovered newer consensus header from peer"
                    );
                    return Some(header.digest());
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
    if let Some(header) = probe_peers_for_latest(current, &config, &network).await {
        info!(
            target: "state-sync",
            current,
            peer_latest = header.number,
            "startup probe discovered newer consensus header from peer"
        );
        if session.sync_to(header.digest()).await.is_break() {
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
mod tests {
    use super::*;
    use rayls_consensus_primary::network::{
        NetworkCommand, PrimaryRPCError, PrimaryRequest, PrimaryResponse, NO_CONSENSUS_OUTPUT,
    };
    use rayls_infrastructure_storage::{mem_db::MemDatabase, tables::ConsensusBlocks};
    use rayls_testing_test_utils_committee::CommitteeFixture;
    use std::{collections::HashMap, num::NonZeroUsize, sync::Arc};

    /// A network handle whose receiver is dropped: every fetch fails fast, so a walk can make
    /// progress only through the local DB/cache, never a peer.
    fn no_peer_network() -> PrimaryNetworkHandle {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        drop(rx);
        PrimaryNetworkHandle::new_for_test(tx)
    }

    /// A network that reports `connected` peers and answers each `SendRequest` with the response
    /// `respond` scripts for that peer and 1-based request attempt. `None` fails the request the
    /// way a not-yet-connected peer does.
    fn scripted_boot_probe_network(
        connected: Vec<BlsPublicKey>,
        mut respond: impl FnMut(&BlsPublicKey, usize) -> Option<PrimaryResponse> + Send + 'static,
    ) -> PrimaryNetworkHandle {
        let (tx, mut rx) =
            tokio::sync::mpsc::channel::<NetworkCommand<PrimaryRequest, PrimaryResponse>>(64);
        tokio::spawn(async move {
            let mut attempts: HashMap<BlsPublicKey, usize> = HashMap::new();
            while let Some(command) = rx.recv().await {
                match command {
                    NetworkCommand::ConnectedPeers { reply } => {
                        let _ = reply.send(connected.clone());
                    }
                    NetworkCommand::SendRequest { peer, reply, .. } => {
                        let attempt = attempts.entry(peer).or_default();
                        *attempt += 1;
                        let result = match respond(&peer, *attempt) {
                            Some(response) => Ok(response),
                            None => Err(NetworkError::PeerNotConnected),
                        };
                        let _ = reply.send(result);
                    }
                    _ => {}
                }
            }
        });
        PrimaryNetworkHandle::new_for_test(tx)
    }

    fn committee_peers(config: &ConsensusConfig<MemDatabase>) -> Vec<BlsPublicKey> {
        config
            .committee()
            .others_primaries_by_id(config.authority_id().as_ref())
            .into_iter()
            .map(|(_, peer)| peer)
            .collect()
    }

    fn header_response(number: u64) -> PrimaryResponse {
        PrimaryResponse::ConsensusHeader(Arc::new(ConsensusHeader { number, ..Default::default() }))
    }

    fn genesis_response() -> PrimaryResponse {
        PrimaryResponse::Error(PrimaryRPCError(format!("Internal error: {NO_CONSENSUS_OUTPUT}")))
    }

    /// A committee peer that is still connecting when the probe starts must be asked again: the
    /// first attempt fails, a later one reports a head.
    #[tokio::test(start_paused = true)]
    async fn boot_probe_retries_peers_that_are_still_connecting() {
        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .randomize_ports(true)
            .committee_size(NonZeroUsize::new(4).unwrap())
            .build();
        let primary = fixture.authorities().next().unwrap();
        let config = primary.consensus_config();
        let late_peer = committee_peers(&config)[0];

        let network = scripted_boot_probe_network(Vec::new(), move |peer, attempt| {
            (*peer == late_peer && attempt >= 2).then(|| header_response(42))
        });

        let probe = probe_peers_for_boot_head(&config, &network).await;
        assert_eq!(probe, BootHeadProbe::Responded { head: Some(42) });
    }

    /// A node joining an existing chain is typically connected to the chain's *current*
    /// committee, which on a fresh datadir is not the local genesis committee. Those connected
    /// peers must be asked too.
    #[tokio::test]
    async fn boot_probe_asks_connected_peers_outside_the_committee() {
        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .randomize_ports(true)
            .committee_size(NonZeroUsize::new(4).unwrap())
            .build();
        let primary = fixture.authorities().next().unwrap();
        let config = primary.consensus_config();

        let outsider = BlsPublicKey::default();
        assert!(
            !config.committee().bls_keys().contains(&outsider),
            "test key must not be in the genesis committee"
        );

        let network = scripted_boot_probe_network(vec![outsider], move |peer, _| {
            (*peer == outsider).then(|| header_response(7))
        });

        let probe = probe_peers_for_boot_head(&config, &network).await;
        assert_eq!(probe, BootHeadProbe::Responded { head: Some(7) });
    }

    /// Genesis from one peer is not conclusive: another committee peer may still be connecting
    /// and report that the chain already exists. The probe must keep asking rather than lock in
    /// the new-chain rules on the first genesis answer.
    #[tokio::test(start_paused = true)]
    async fn boot_probe_does_not_treat_partial_genesis_as_final() {
        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .randomize_ports(true)
            .committee_size(NonZeroUsize::new(4).unwrap())
            .build();
        let primary = fixture.authorities().next().unwrap();
        let config = primary.consensus_config();
        let peers = committee_peers(&config);
        let (genesis_peer, late_peer) = (peers[0], peers[1]);

        let network = scripted_boot_probe_network(Vec::new(), move |peer, attempt| {
            if *peer == genesis_peer {
                Some(PrimaryResponse::Error(PrimaryRPCError(format!(
                    "Internal error: {NO_CONSENSUS_OUTPUT}"
                ))))
            } else if *peer == late_peer && attempt >= 2 {
                Some(header_response(42))
            } else {
                None
            }
        });

        let probe = probe_peers_for_boot_head(&config, &network).await;
        assert_eq!(
            probe,
            BootHeadProbe::Responded { head: Some(42) },
            "a partial genesis answer must not short-circuit the probe"
        );
    }

    /// An unreachable committee peer does not turn a genesis answer into a refusal: after the
    /// budget expires with no non-zero head, the answering peer still establishes a new chain.
    #[tokio::test(start_paused = true)]
    async fn boot_probe_falls_back_to_genesis_when_a_peer_stays_unreachable() {
        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .randomize_ports(true)
            .committee_size(NonZeroUsize::new(4).unwrap())
            .build();
        let primary = fixture.authorities().next().unwrap();
        let config = primary.consensus_config();
        let genesis_peer = committee_peers(&config)[0];

        let network = scripted_boot_probe_network(Vec::new(), move |peer, _| {
            (*peer == genesis_peer).then(genesis_response)
        });

        let probe = probe_peers_for_boot_head(&config, &network).await;
        assert_eq!(probe, BootHeadProbe::Responded { head: None });
    }

    /// A stale `ConsensusBlockNumbersByDigest` entry (foreign digest mapped to a canonical number)
    /// must not false-stop the backwards walk or republish the wrong header.
    #[tokio::test(start_paused = true)]
    async fn stale_index_entry_does_not_false_stop_the_walk() {
        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .randomize_ports(true)
            .committee_size(NonZeroUsize::new(4).unwrap())
            .build();
        let primary = fixture.authorities().next().unwrap();
        let config = primary.consensus_config();
        let db = config.node_storage();

        // Canonical block at 100, plus a STALE index entry pointing a foreign digest at 100.
        let canonical = ConsensusHeader { number: 100, ..Default::default() };
        let canonical_digest = canonical.digest();
        let stale_digest = B256::repeat_byte(0xEE);
        assert_ne!(stale_digest, canonical_digest, "precondition: the stale hash is not canonical");
        db.with_write_txn(|txn| {
            txn.insert::<ConsensusBlocks>(&canonical.number, &canonical)?;
            txn.insert::<ConsensusBlockNumbersByDigest>(&canonical_digest, &canonical.number)?;
            txn.insert::<ConsensusBlockNumbersByDigest>(&stale_digest, &canonical.number)?;
            Ok(())
        })
        .unwrap();

        let consensus_bus = ConsensusBus::new();
        let before = consensus_bus.last_consensus_header().borrow().number;

        // Network with a dropped receiver: every fetch fails fast, so the ONLY path that can change
        // `last_consensus_header` is the buggy canonical-hit branch.
        let network = no_peer_network();
        let shutdown = config.shutdown().subscribe();

        let parent =
            get_consensus_header(stale_digest, &config, &consensus_bus, &network, &shutdown).await;

        let after = consensus_bus.last_consensus_header().borrow().number;
        assert_eq!(
            after, before,
            "stale index entry false-stopped the walk and republished the wrong block"
        );
        assert!(parent.is_none(), "no reachable peer, so the fetch fails and the walk yields None");
    }

    /// A header already cached by an earlier leg (fetched but not yet executed/canonical) must be
    /// traversed from the DB, not refetched from a peer: the walk reads it, republishes the tip,
    /// and returns its parent to keep descending toward the canonical anchor. While execution
    /// lags catch-up every not-yet-executed header lives only in the cache, so refetching each
    /// one on every gossip update re-walks the whole gap over the network.
    #[tokio::test(start_paused = true)]
    async fn cached_header_is_traversed_not_refetched() {
        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .randomize_ports(true)
            .committee_size(NonZeroUsize::new(4).unwrap())
            .build();
        let primary = fixture.authorities().next().unwrap();
        let config = primary.consensus_config();
        let db = config.node_storage();

        // Cache-only header at 100 (no canonical `ConsensusBlocks` row): the steady state while
        // execution lags the backwards walk.
        let parent_hash = B256::repeat_byte(0x77);
        let cached = ConsensusHeader { number: 100, parent_hash, ..Default::default() };
        let cached_digest = cached.digest();
        store_consensus_header_in_cache(db, &cached);
        assert!(
            db.get_canonical_consensus_by_hash(cached_digest).is_none(),
            "precondition: a cache-only header is invisible to the canonical anchor check"
        );

        let consensus_bus = ConsensusBus::new();

        // Dropped-receiver network: every fetch fails fast. So returning the parent (and advancing
        // `last_consensus_header` to 100) is only reachable by traversing the cache; a refetch path
        // would fail and yield `None` with the watermark left at 0.
        let network = no_peer_network();
        let shutdown = config.shutdown().subscribe();

        let next =
            get_consensus_header(cached_digest, &config, &consensus_bus, &network, &shutdown).await;

        assert_eq!(
            next,
            Some(parent_hash),
            "a cached header is traversed from the DB, returning its parent to keep descending"
        );
        assert_eq!(
            consensus_bus.last_consensus_header().borrow().number,
            100,
            "traversing the cached header republishes it without a peer refetch"
        );
    }

    /// A walk stops at the session's covered anchor instead of descending below it. The anchor
    /// heads a chain a prior walk already grounded, so re-descending it is wasted work; with no
    /// reachable peer, only stopping at the anchor lets the walk reach `Covered` rather than
    /// `Incomplete`.
    #[tokio::test(start_paused = true)]
    async fn walk_stops_at_the_covered_anchor() {
        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .randomize_ports(true)
            .committee_size(NonZeroUsize::new(4).unwrap())
            .build();
        let primary = fixture.authorities().next().unwrap();
        let config = primary.consensus_config();
        let db = config.node_storage();

        // The covered anchor (number 105) is NOT itself in the DB; a cached child at 106 links onto
        // it, so the walk reaches the anchor by traversing the child and must stop there.
        let covered_hash = B256::repeat_byte(0x55);
        let child =
            ConsensusHeader { number: 106, parent_hash: covered_hash, ..Default::default() };
        store_consensus_header_in_cache(db, &child);

        // Dropped-receiver network: any fetch below the anchor fails, so a walk that does not stop
        // at the anchor falls through to a failing fetch and yields `Incomplete`.
        let network = no_peer_network();

        let mut session = WalkSession::new(config.clone(), ConsensusBus::new(), network);
        session.covered = Some(covered_hash);

        assert_eq!(
            session.walk(child.digest()).await,
            WalkOutcome::Covered,
            "the walk must stop at the covered anchor, not fetch below it"
        );
    }

    /// `derive_covered_anchor` is a pure projection of the persisted cache: on an unchanged cache
    /// it returns the same anchor every call. The sweep is offloaded to a blocking thread, so its
    /// result must stay deterministic and independent of where it runs.
    #[tokio::test]
    async fn derive_covered_anchor_is_stable_across_calls() {
        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .randomize_ports(true)
            .committee_size(NonZeroUsize::new(4).unwrap())
            .build();
        let primary = fixture.authorities().next().unwrap();
        let config = primary.consensus_config();
        let db = config.node_storage();

        // Canonical tip at 100, with a contiguous cached run 101 -> 102 hash-linked above it.
        let canonical = ConsensusHeader { number: 100, ..Default::default() };
        db.with_write_txn(|txn| {
            txn.insert::<ConsensusBlocks>(&canonical.number, &canonical)?;
            Ok(())
        })
        .unwrap();
        let child_101 =
            ConsensusHeader { number: 101, parent_hash: canonical.digest(), ..Default::default() };
        let child_102 =
            ConsensusHeader { number: 102, parent_hash: child_101.digest(), ..Default::default() };
        store_consensus_header_in_cache(db, &child_101);
        store_consensus_header_in_cache(db, &child_102);

        let first = derive_covered_anchor(db);
        let second = derive_covered_anchor(db);
        assert_eq!(first, Some(child_102.digest()), "anchor must be the top of the contiguous run");
        assert_eq!(first, second, "derive must be stable across calls on an unchanged cache");
    }
}
