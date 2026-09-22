//! Tasks and helpers for collecting epoch records trustlessly.

use eyre::OptionExt;
use rayls_consensus_primary::{network::PrimaryNetworkHandle, ConsensusBus};
use rayls_infrastructure_storage::{tables::EpochRecords, EpochStore as _};
use rayls_infrastructure_types::{
    BlsPublicKey, Database as ReDatabase, Epoch, EpochCertificate, EpochRecord, Noticer,
    TaskSpawner, B256,
};
use std::{
    collections::{BTreeSet, HashMap},
    time::Duration,
};
use tracing::info;

/// Maximum number of consecutive orphaned epoch records to skip past during catch-up.
/// When an epoch record is unavailable network-wide, we probe up to this many successors
/// to decide whether it is a hole below the tip (skip it) or simply the chain tip (stop).
/// Bounds how far a rare run of consecutive holes can be crossed, and how many probes the
/// tip check costs.
const MAX_GAP_SKIP: Epoch = 8;

/// How long the collector waits before trying again while it is still behind the epoch it was
/// asked for. Without it the only wake-up is the `requested_missing_epoch` watch, which changes
/// at the next epoch boundary, so a record missed at a close waited a whole epoch.
const COLLECTOR_RETRY_INTERVAL: Duration = Duration::from_secs(5);

/// Total time spent retrying one epoch the upward walk could not reach, before falling back to
/// the slower cadence. Peers usually certify the record a fraction of a second after this node
/// asks for it, so a short retry removes the wait for the next boundary.
const COLLECTOR_RETRY_BUDGET: Duration = Duration::from_secs(60);

/// How many times the same epoch may be pursued with [`COLLECTOR_RETRY_BUDGET`] before it is left
/// to the periodic passes, so an epoch nobody can serve cannot hold the collector forever.
const MAX_RETRY_ROUNDS: u32 = 3;

/// Committee-size floor for accepting a record on its certificate alone (no parent to anchor to).
const MIN_CERT_ONLY_COMMITTEE: usize = 4;

/// How often the collector rescans the record table for holes below the tip while caught up.
const COLLECTOR_BACKFILL_INTERVAL: Duration = Duration::from_secs(30);

/// Highest number of holes considered in one backfill scan.
const MAX_BACKFILL_SCAN: usize = 64;

/// Highest number of holes fetched in one backfill pass, so a long-missing range is filled a
/// few records at a time instead of flooding peers.
const MAX_BACKFILL_PER_PASS: usize = 4;

/// After this many failed passes a hole is treated as orphaned network-wide and only retried
/// every [`BACKFILL_COOLDOWN_PASSES`] passes.
const MAX_BACKFILL_ATTEMPTS: u32 = 10;

/// Retry period, in backfill passes, for a hole that exhausted [`MAX_BACKFILL_ATTEMPTS`].
const BACKFILL_COOLDOWN_PASSES: u64 = 20;

/// Return true if committee is compatable with epoch_rec_committee.
/// These will usually be equal but it is possible for a validator to be
/// booted and still in committee but not in epoch_rec.committee.
/// This is very unlikely, but check for it just in case.
pub fn epoch_committee_valid(epoch_rec: &EpochRecord, committee: &[BlsPublicKey]) -> bool {
    let epoch_committee_len = epoch_rec.committee.len();
    let committee_len = committee.len();
    match committee_len.cmp(&epoch_committee_len) {
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Equal => committee == epoch_rec.committee,
        std::cmp::Ordering::Greater => {
            let required = (committee_len * 2).div_ceil(3);
            if epoch_committee_len < 4 || epoch_committee_len < required {
                // Make sure we have a reasonable committe size, i.e. don't let
                // a bogus record with one signer through, etc.
                false
            } else {
                for k in &epoch_rec.committee {
                    if !committee.contains(k) {
                        return false;
                    }
                }
                true
            }
        }
    }
}

/// get committee from the database for the given epoch. Will return an error if it can't be found.
fn get_committee(
    db: &impl ReDatabase,
    epoch: u32,
) -> Result<(B256, Vec<BlsPublicKey>), eyre::Error> {
    // Try to recover by downloading the epoch record and cert from a peer.
    if epoch == 0 {
        // If we can't find the genesis committee something is very wrong.
        let committee =
            db.get_committee_keys(0).ok_or_eyre("always can retreive epoch 0 committee")?;
        return Ok((B256::default(), committee));
    }

    db.get::<EpochRecords>(&(epoch - 1))
        .ok()
        .flatten()
        .map(|prev| (prev.digest(), prev.next_committee.clone()))
        .ok_or_else(|| eyre::eyre!("Failed to retrieve committee for epoch {epoch}"))
}

/// Validate an epoch record and its certificate against what this node already knows.
///
/// Normally the record is anchored to its parent (parent digest + the committee the parent names
/// for this epoch) and the certificate is verified against that committee; epoch 0 is anchored to
/// the genesis committee. If the parent record is missing - a permanent hole, orphaned
/// network-wide, that can never be fetched or rebuilt here - fall back to trusting the
/// certificate alone: it proves 2f+1 of the committee named in the record signed it, and the next
/// epoch re-anchors the chain against this one (its `parent_hash` must match). That fallback is
/// guarded by a committee-size floor so a record naming a tiny committee cannot slip through.
pub fn epoch_record_valid<DB>(
    db: &DB,
    epoch: Epoch,
    epoch_rec: &EpochRecord,
    cert: &EpochCertificate,
) -> bool
where
    DB: ReDatabase,
{
    if epoch_rec.epoch != epoch {
        return false;
    }
    match get_committee(db, epoch) {
        Ok((parent_hash, committee)) => {
            parent_hash == epoch_rec.parent_hash
                && epoch_committee_valid(epoch_rec, &committee)
                && epoch_rec.verify_with_cert(cert)
        }
        Err(_) => {
            let ok = epoch_rec.committee.len() >= MIN_CERT_ONLY_COMMITTEE
                && epoch_rec.verify_with_cert(cert);
            if ok {
                tracing::warn!(
                    target: "epoch-manager",
                    "epoch {epoch} parent record is orphaned; accepting cert-verified record to skip the gap",
                );
            }
            ok
        }
    }
}

/// Outcome of asking peers for one epoch record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FetchOutcome {
    /// Fetched, validated and saved.
    Saved,
    /// A peer answered but the record did not validate, or it could not be saved.
    Rejected,
    /// No peer could serve the record.
    Unavailable,
}

/// Fetch one epoch record with its certificate from a peer and save it if it validates.
async fn fetch_and_save_epoch<DB>(
    epoch: Epoch,
    db: &DB,
    primary_handle: &PrimaryNetworkHandle,
) -> FetchOutcome
where
    DB: ReDatabase,
{
    match primary_handle.request_epoch_cert(Some(epoch), None).await {
        Ok((epoch_rec, cert)) => {
            if !epoch_record_valid(db, epoch, &epoch_rec, &cert) {
                return FetchOutcome::Rejected;
            }
            let epoch_hash = epoch_rec.digest();
            if let Err(e) = db.save_epoch_record_with_cert(&epoch_rec, &cert) {
                tracing::error!(
                    target: "epoch-manager",
                    "failed to save epoch record with cert for epoch {epoch}: {e}",
                );
                return FetchOutcome::Rejected;
            }
            info!(
                target: "epoch-manager",
                "retrieved cert for epoch {epoch}: {epoch_hash} from a peer",
            );
            FetchOutcome::Saved
        }
        Err(err) => {
            // We delibrately go past the latest epoch so this is expected to happen.
            info!(
                target: "epoch-manager",
                "failed to retrieve epoch from a peer {epoch}: {err}",
            );
            FetchOutcome::Unavailable
        }
    }
}

/// Asks peers for records from last_epoch to requested_epoch.
/// Returns the Epoch that was last retrieved.
async fn collect_epoch_records<DB>(
    last_epoch: Epoch,
    db: &DB,
    primary_handle: &PrimaryNetworkHandle,
) -> Epoch
where
    DB: ReDatabase,
{
    let mut result_epoch = last_epoch;
    for epoch in last_epoch.. {
        // If we already have epoch record AND it's certificate then continue.
        if let Some((_, Some(_))) = db.get_epoch_by_number(epoch) {
            continue;
        }
        match fetch_and_save_epoch(epoch, db, primary_handle).await {
            FetchOutcome::Saved => result_epoch = epoch,
            FetchOutcome::Rejected => {}
            FetchOutcome::Unavailable => {
                // `epoch` may simply be past the tip (nothing to fetch), or it may be a
                // record that is orphaned network-wide while later epochs still exist -
                // possibly across a short run of consecutive holes. Probe up to
                // MAX_GAP_SKIP successors: if any is fetchable, `epoch` is a hole below the
                // tip and we skip it so catch-up can continue; if none are, we are at the
                // tip and stop.
                let mut later_epoch_exists = false;
                for ahead in 1..=MAX_GAP_SKIP {
                    if primary_handle.request_epoch_cert(Some(epoch + ahead), None).await.is_ok() {
                        later_epoch_exists = true;
                        break;
                    }
                }
                if later_epoch_exists {
                    tracing::warn!(
                        target: "epoch-manager",
                        "epoch {epoch} is unavailable network-wide but a later epoch exists; skipping the gap",
                    );
                    // Mark progress so the loop advances past the hole instead of breaking.
                    result_epoch = epoch;
                    continue;
                }
            }
        }
        if result_epoch != epoch {
            break;
        }
    }
    result_epoch
}

/// Retry one epoch directly until it lands or `budget` runs out.
///
/// Used for the epoch an upward walk could not reach: peers certify the record a moment after
/// this node asks for it, so a few direct attempts (three peers each, no gap probing) spaced
/// `interval` apart turn a whole-epoch wait into a few seconds. Returns true once the record is
/// stored - by us or by whoever else landed it meanwhile.
async fn retry_missing_epoch<DB>(
    target: Epoch,
    db: &DB,
    primary_handle: &PrimaryNetworkHandle,
    interval: Duration,
    budget: Duration,
) -> bool
where
    DB: ReDatabase,
{
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        if db.get_epoch_by_number(target).is_some_and(|(_, cert)| cert.is_some()) {
            return true;
        }
        if fetch_and_save_epoch(target, db, primary_handle).await == FetchOutcome::Saved {
            info!(
                target: "epoch-manager",
                epoch = target,
                "retrieved the epoch record a retry after the first attempt failed",
            );
            return true;
        }
        if tokio::time::Instant::now() + interval >= deadline {
            return false;
        }
        tokio::time::sleep(interval).await;
    }
}

/// Epochs missing from the local record chain, oldest first, at most `max`.
///
/// Only holes *between* the oldest and the newest record held are returned: anything below the
/// oldest may have been pruned or archived on purpose, and anything above the newest belongs to
/// the upward walk in [`collect_epoch_records`].
fn missing_epochs(present: &BTreeSet<Epoch>, max: usize) -> Vec<Epoch> {
    let (Some(&first), Some(&last)) = (present.first(), present.last()) else {
        return Vec::new();
    };
    (first..last).filter(|epoch| !present.contains(epoch)).take(max).collect()
}

/// Whether a hole should be retried on this backfill pass.
///
/// A hole that keeps failing is most likely orphaned network-wide, so after
/// [`MAX_BACKFILL_ATTEMPTS`] it is only retried once every [`BACKFILL_COOLDOWN_PASSES`] passes
/// rather than given up on for the life of the process (a node that was simply isolated must
/// still recover).
fn backfill_due(attempts: u32, pass: u64) -> bool {
    attempts < MAX_BACKFILL_ATTEMPTS || pass.is_multiple_of(BACKFILL_COOLDOWN_PASSES)
}

/// Try to fill holes in the local epoch record chain.
///
/// A record missed at a close (the node was not active and its vote collection was aborted) is
/// never revisited by [`collect_epoch_records`], which only walks forward from its last success
/// and only when the `requested_missing_epoch` watch changes; without this pass the hole stays
/// for the life of the database. Returns the number of records saved.
async fn backfill_missing_records<DB>(
    db: &DB,
    primary_handle: &PrimaryNetworkHandle,
    attempts: &mut HashMap<Epoch, u32>,
    pass: u64,
) -> usize
where
    DB: ReDatabase,
{
    let present: BTreeSet<Epoch> = db.iter::<EpochRecords>().map(|(epoch, _)| epoch).collect();
    let holes: Vec<Epoch> = missing_epochs(&present, MAX_BACKFILL_SCAN)
        .into_iter()
        .filter(|epoch| backfill_due(attempts.get(epoch).copied().unwrap_or(0), pass))
        .take(MAX_BACKFILL_PER_PASS)
        .collect();
    // Forget epochs that are no longer holes so the map cannot grow without bound.
    attempts.retain(|epoch, _| !present.contains(epoch));

    let mut saved = 0;
    for epoch in holes {
        if fetch_and_save_epoch(epoch, db, primary_handle).await == FetchOutcome::Saved {
            tracing::info!(
                target: "epoch-manager",
                epoch,
                "backfilled a missing epoch record below the chain tip",
            );
            attempts.remove(&epoch);
            saved += 1;
        } else {
            let failures = attempts.entry(epoch).or_default();
            *failures = failures.saturating_add(1);
        }
    }
    saved
}

/// Spawn a long running task to collect missing epoch records.
///
/// Most likely because a node is syncing.
pub async fn spawn_epoch_record_collector<DB>(
    db: DB,
    primary_handle: PrimaryNetworkHandle,
    consensus_bus: ConsensusBus,
    node_task_spawner: TaskSpawner,
    node_shutdown: Noticer,
) -> eyre::Result<()>
where
    DB: ReDatabase,
{
    let mut epoch_rx = consensus_bus.requested_missing_epoch().subscribe();
    node_task_spawner.spawn_critical_task("Epoch Record Collector", async move {
        let mut last_epoch = if let Some((last_epoch, _)) = db.last_record::<EpochRecords>() {
            last_epoch
        } else {
            0
        };
        if last_epoch == 0 {
            while get_committee(&db, last_epoch).is_err() {
                tokio::select! {
                    _ = &node_shutdown => {
                        return;
                    },
                    _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => { }
                }
            }
        }
        let mut backfill_attempts: HashMap<Epoch, u32> = HashMap::new();
        let mut backfill_pass = 0u64;
        let mut next_backfill = tokio::time::Instant::now();
        // The epoch an upward walk could not reach, and how many retry rounds it has had. Kept
        // here rather than read back from the watch: the sanity reset below lowers the watch to
        // what we actually hold, which would otherwise erase the fact that work is pending.
        let mut retry_target: Option<(Epoch, u32)> = None;
        loop {
            let requested_epoch =
                (*epoch_rx.borrow_and_update()).max(retry_target.map_or(0, |(epoch, _)| epoch));
            if requested_epoch > last_epoch {
                last_epoch = collect_epoch_records(last_epoch, &db, &primary_handle).await;
                if last_epoch < requested_epoch {
                    retry_target = match retry_target {
                        Some((epoch, rounds)) if epoch == requested_epoch => {
                            Some((epoch, rounds + 1))
                        }
                        _ => Some((requested_epoch, 0)),
                    };
                    // Small sanity check in case someone sends a malicious large epoch restore to
                    // sanity.
                    if *epoch_rx.borrow() > last_epoch {
                        consensus_bus.requested_missing_epoch().send_replace(last_epoch);
                        // Our own reset is not new work: do not let it wake the waits below.
                        let _ = epoch_rx.borrow_and_update();
                    }
                } else {
                    retry_target = None;
                }
            }

            // Retry the epoch the walk could not reach, directly and on a bounded budget. Without
            // this the next wake-up is the watch changing at the following epoch boundary.
            if let Some((target, rounds)) = retry_target {
                if rounds >= MAX_RETRY_ROUNDS {
                    retry_target = None;
                } else {
                    let landed = tokio::select! {
                        biased;
                        _ = &node_shutdown => break,
                        _ = epoch_rx.changed() => false,
                        landed = retry_missing_epoch(
                            target,
                            &db,
                            &primary_handle,
                            COLLECTOR_RETRY_INTERVAL,
                            COLLECTOR_RETRY_BUDGET,
                        ) => landed,
                    };
                    // Landed: the target is done (keeping it would re-run the walk, tip probes
                    // included, once per remaining round). Otherwise count the round and go on.
                    retry_target = if landed { None } else { Some((target, rounds + 1)) };
                    // Re-enter: the walk resumes if the watch is asking for a newer epoch.
                    continue;
                }
            }
            // Fill holes left below the tip by closes this node could not collect a record for.
            if tokio::time::Instant::now() >= next_backfill {
                backfill_pass += 1;
                backfill_missing_records(
                    &db,
                    &primary_handle,
                    &mut backfill_attempts,
                    backfill_pass,
                )
                .await;
                next_backfill = tokio::time::Instant::now() + COLLECTOR_BACKFILL_INTERVAL;
            }
            // Retry on a timer while behind instead of waiting for the watch to change (which
            // happens at the next epoch boundary), and rescan for holes periodically otherwise.
            let wait = if *epoch_rx.borrow() > last_epoch {
                COLLECTOR_RETRY_INTERVAL
            } else {
                COLLECTOR_BACKFILL_INTERVAL
            };
            tokio::select!(
                _ = &node_shutdown => {
                    break;  // Break the outer loop.
                },
                _ = epoch_rx.changed() => { },
                _ = tokio::time::sleep(wait) => { }
            );
        }
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rayls_consensus_primary::network::{
        NetworkCommand, NetworkError, PrimaryNetworkHandle, PrimaryRequest, PrimaryResponse,
    };
    use rayls_infrastructure_storage::mem_db::MemDatabase;
    use rayls_infrastructure_types::{
        BlsAggregateSignature, BlsSigner, EpochCertificate, Notifier, TaskManager,
    };
    use rayls_testing_test_utils_committee::CommitteeFixture;
    use std::{
        collections::BTreeMap,
        num::NonZeroUsize,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };

    /// A network handle whose receiver is dropped: every request fails immediately.
    fn no_peer_network() -> PrimaryNetworkHandle {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        drop(rx);
        PrimaryNetworkHandle::new_for_test(tx)
    }

    /// A network handle that answers `EpochRecord` requests from an in-memory chain.
    fn serving_network(
        records: BTreeMap<Epoch, (EpochRecord, EpochCertificate)>,
    ) -> PrimaryNetworkHandle {
        let (tx, mut rx) =
            tokio::sync::mpsc::channel::<NetworkCommand<PrimaryRequest, PrimaryResponse>>(64);
        tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                let NetworkCommand::SendRequestAny { request, reply } = cmd else { continue };
                let PrimaryRequest::EpochRecord { epoch, hash } = request else { continue };
                let found = match (epoch, hash) {
                    (Some(epoch), _) => records.get(&epoch).cloned(),
                    (None, Some(hash)) => {
                        records.values().find(|(rec, _)| rec.digest() == hash).cloned()
                    }
                    (None, None) => None,
                };
                let response = match found {
                    Some((record, certificate)) => {
                        Ok(PrimaryResponse::EpochRecord { record, certificate })
                    }
                    None => Err(NetworkError::RPCError("no such epoch record".to_string())),
                };
                let _ = reply.send(response);
            }
        });
        PrimaryNetworkHandle::new_for_test(tx)
    }

    /// Like [`serving_network`], but the first `fail_first` requests are refused. Models the
    /// real race: the peers have not certified the record yet when this node first asks.
    fn flaky_network(
        records: BTreeMap<Epoch, (EpochRecord, EpochCertificate)>,
        fail_first: usize,
    ) -> PrimaryNetworkHandle {
        counting_flaky_network(records, fail_first).0
    }

    /// [`flaky_network`] that also counts every epoch record request it receives.
    fn counting_flaky_network(
        records: BTreeMap<Epoch, (EpochRecord, EpochCertificate)>,
        fail_first: usize,
    ) -> (PrimaryNetworkHandle, Arc<AtomicUsize>) {
        let requests = Arc::new(AtomicUsize::new(0));
        let counter = requests.clone();
        let (tx, mut rx) =
            tokio::sync::mpsc::channel::<NetworkCommand<PrimaryRequest, PrimaryResponse>>(64);
        tokio::spawn(async move {
            let mut seen = 0usize;
            while let Some(cmd) = rx.recv().await {
                let NetworkCommand::SendRequestAny { request, reply } = cmd else { continue };
                let PrimaryRequest::EpochRecord { epoch, .. } = request else { continue };
                seen += 1;
                counter.fetch_add(1, Ordering::SeqCst);
                let found =
                    if seen <= fail_first { None } else { epoch.and_then(|e| records.get(&e)) };
                let response = match found {
                    Some((record, certificate)) => Ok(PrimaryResponse::EpochRecord {
                        record: record.clone(),
                        certificate: certificate.clone(),
                    }),
                    None => Err(NetworkError::RPCError("not certified yet".to_string())),
                };
                let _ = reply.send(response);
            }
        });
        (PrimaryNetworkHandle::new_for_test(tx), requests)
    }

    /// A record signed by a super quorum of `signers`, for the cert-only path (committee of four
    /// or more, no parent record to anchor against).
    fn multi_signed_record<S: BlsSigner>(
        signers: &[&S],
        epoch: Epoch,
        parent_hash: B256,
    ) -> (EpochRecord, EpochCertificate) {
        let mut keys: Vec<BlsPublicKey> = signers.iter().map(|s| s.public_key()).collect();
        keys.sort_unstable();
        let record = EpochRecord {
            epoch,
            committee: keys.clone(),
            next_committee: keys.clone(),
            parent_hash,
            ..Default::default()
        };
        let mut signed_authorities = roaring::RoaringBitmap::new();
        let mut sigs = Vec::new();
        // Sign in committee order so the bitmap positions match the aggregate.
        for (index, key) in keys.iter().enumerate().take(record.super_quorum()) {
            let signer = signers.iter().find(|s| s.public_key() == *key).expect("signer");
            sigs.push(record.sign_vote(*signer).signature);
            signed_authorities.push(index as u32);
        }
        let aggregate = BlsAggregateSignature::aggregate(&sigs[..], true).expect("aggregate");
        let cert = EpochCertificate {
            epoch_hash: record.digest(),
            signature: aggregate.to_signature(),
            signed_authorities,
        };
        (record, cert)
    }

    /// A chain of single-signer epoch records, each anchored to the previous one, so the
    /// production validation in `fetch_and_save_epoch` accepts them.
    fn record_chain<S: BlsSigner>(key: &S, len: Epoch) -> Vec<(EpochRecord, EpochCertificate)> {
        let public_key = key.public_key();
        let mut parent_hash = B256::default();
        let mut chain = Vec::new();
        for epoch in 0..len {
            let record = EpochRecord {
                epoch,
                committee: vec![public_key],
                next_committee: vec![public_key],
                parent_hash,
                ..Default::default()
            };
            let vote = record.sign_vote(key);
            let mut signed_authorities = roaring::RoaringBitmap::new();
            signed_authorities.push(0);
            let cert = EpochCertificate {
                epoch_hash: record.digest(),
                signature: vote.signature,
                signed_authorities,
            };
            parent_hash = record.digest();
            chain.push((record, cert));
        }
        chain
    }

    fn test_db_and_chain(len: Epoch) -> (MemDatabase, Vec<(EpochRecord, EpochCertificate)>) {
        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .randomize_ports(true)
            .committee_size(NonZeroUsize::new(4).unwrap())
            .build();
        let primary = fixture.authorities().next().unwrap();
        let chain = record_chain(primary.consensus_config().key_config(), len);
        (MemDatabase::default(), chain)
    }

    #[test]
    fn missing_epochs_finds_holes_between_the_records_held() {
        let present: BTreeSet<Epoch> = [0, 1, 2, 5, 6].into_iter().collect();
        assert_eq!(missing_epochs(&present, 8), vec![3, 4]);
    }

    #[test]
    fn missing_epochs_ignores_everything_outside_the_records_held() {
        // Records below the oldest one held may have been pruned or archived on purpose, and
        // records above the newest are the upward walk's job, so neither is a hole.
        let present: BTreeSet<Epoch> = [10, 11, 12].into_iter().collect();
        assert!(missing_epochs(&present, 8).is_empty());
        assert!(missing_epochs(&BTreeSet::new(), 8).is_empty());
        assert!(missing_epochs(&[7].into_iter().collect(), 8).is_empty());
    }

    #[test]
    fn missing_epochs_is_bounded_and_oldest_first() {
        let present: BTreeSet<Epoch> = [0, 100].into_iter().collect();
        assert_eq!(missing_epochs(&present, 3), vec![1, 2, 3]);
    }

    #[test]
    fn backfill_due_backs_off_after_repeated_failures() {
        assert!(backfill_due(0, 1));
        assert!(backfill_due(MAX_BACKFILL_ATTEMPTS - 1, 1));
        // Exhausted: only retried on a cooldown pass, and never given up on for good.
        assert!(!backfill_due(MAX_BACKFILL_ATTEMPTS, 1));
        assert!(backfill_due(MAX_BACKFILL_ATTEMPTS, BACKFILL_COOLDOWN_PASSES));
    }

    /// The hole this issue leaves behind (validator-2 lost records 36 and 37 for good): the
    /// collector only walks upward from its last success, so a backfill pass has to notice a
    /// gap below the tip and fetch it.
    #[tokio::test]
    async fn backfill_fills_holes_below_the_tip() {
        let (db, chain) = test_db_and_chain(6);
        for epoch in [0, 1, 2, 5] {
            let (record, cert) = &chain[epoch as usize];
            db.save_epoch_record_with_cert(record, cert).unwrap();
        }
        let network = serving_network(chain.iter().cloned().map(|rc| (rc.0.epoch, rc)).collect());

        let mut attempts = HashMap::new();
        let saved = backfill_missing_records(&db, &network, &mut attempts, 1).await;

        assert_eq!(saved, 2, "both holes below the tip must be filled");
        for epoch in [3, 4] {
            assert!(
                matches!(db.get_epoch_by_number(epoch), Some((_, Some(_)))),
                "epoch {epoch} record and cert must be stored",
            );
        }
        assert!(attempts.is_empty(), "filled holes must not be remembered as failures");
    }

    /// A hole nobody can serve must not be retried forever: it is attempted a bounded number of
    /// times and then only on cooldown passes.
    #[tokio::test]
    async fn backfill_backs_off_on_an_unavailable_hole() {
        let (db, chain) = test_db_and_chain(6);
        for epoch in [0, 1, 2, 5] {
            let (record, cert) = &chain[epoch as usize];
            db.save_epoch_record_with_cert(record, cert).unwrap();
        }
        let network = no_peer_network();

        let mut attempts = HashMap::new();
        for pass in 1..=u64::from(MAX_BACKFILL_ATTEMPTS) {
            assert_eq!(backfill_missing_records(&db, &network, &mut attempts, pass).await, 0);
        }
        assert_eq!(attempts.get(&3), Some(&MAX_BACKFILL_ATTEMPTS));

        // Next pass is not a cooldown pass: the hole is skipped, so the counter does not move.
        backfill_missing_records(&db, &network, &mut attempts, MAX_BACKFILL_ATTEMPTS as u64 + 1)
            .await;
        assert_eq!(attempts.get(&3), Some(&MAX_BACKFILL_ATTEMPTS));
    }

    /// Backfill uses the same validation as catch-up: a record that does not anchor to the
    /// parent we hold is rejected, not written.
    #[tokio::test]
    async fn backfill_rejects_a_record_that_does_not_anchor() {
        let (db, chain) = test_db_and_chain(4);
        for epoch in [0, 1, 3] {
            let (record, cert) = &chain[epoch as usize];
            db.save_epoch_record_with_cert(record, cert).unwrap();
        }
        // Serve a record for epoch 2 that belongs to another chain (wrong parent hash).
        let (mut bogus, cert) = chain[2].clone();
        bogus.parent_hash = B256::repeat_byte(9);
        let network = serving_network([(2, (bogus, cert))].into_iter().collect());

        let mut attempts = HashMap::new();
        assert_eq!(backfill_missing_records(&db, &network, &mut attempts, 1).await, 0);
        assert!(db.get_epoch_by_number(2).is_none(), "an unanchored record must not be stored");
        assert_eq!(attempts.get(&2), Some(&1));
    }

    /// The record of an epoch the upward walk could not reach must land on a retry, without
    /// anything changing the `requested_missing_epoch` watch (the boundary-driven wake-up is
    /// exactly what used to make this wait a whole epoch).
    #[tokio::test(start_paused = true)]
    async fn retry_lands_a_record_the_walk_could_not_reach() {
        let (db, chain) = test_db_and_chain(4);
        for epoch in [0, 1] {
            let (record, cert) = &chain[epoch as usize];
            db.save_epoch_record_with_cert(record, cert).unwrap();
        }
        // `request_epoch_cert` asks up to three peers per call, so six refusals fail two calls.
        let network = flaky_network(chain.iter().cloned().map(|rc| (rc.0.epoch, rc)).collect(), 6);

        let start = tokio::time::Instant::now();
        let landed =
            retry_missing_epoch(2, &db, &network, Duration::from_secs(5), COLLECTOR_RETRY_BUDGET)
                .await;

        assert!(landed, "the record must land once a peer serves it");
        assert!(
            matches!(db.get_epoch_by_number(2), Some((_, Some(_)))),
            "record and cert must be stored",
        );
        let waited = start.elapsed();
        assert!(
            waited >= Duration::from_secs(10) && waited < COLLECTOR_RETRY_BUDGET,
            "must land after the first two retries, not a whole epoch later: {waited:?}",
        );
    }

    /// An epoch nobody can serve must give the budget back instead of retrying forever.
    #[tokio::test(start_paused = true)]
    async fn retry_gives_up_when_the_budget_is_spent() {
        let (db, chain) = test_db_and_chain(2);
        let (record, cert) = &chain[0];
        db.save_epoch_record_with_cert(record, cert).unwrap();

        let start = tokio::time::Instant::now();
        let landed = retry_missing_epoch(
            1,
            &db,
            &no_peer_network(),
            Duration::from_secs(5),
            Duration::from_secs(30),
        )
        .await;

        assert!(!landed);
        assert!(start.elapsed() <= Duration::from_secs(30), "must not overrun its budget");
    }

    /// Once the retry lands the record the collector must stop pursuing that epoch. Keeping the
    /// retry target after a success made the loop re-run the upward walk, tip probes included,
    /// once per remaining round: dozens of pointless peer requests for every recovered epoch.
    #[tokio::test(start_paused = true)]
    async fn collector_stops_pursuing_an_epoch_once_it_lands() {
        let (db, chain) = test_db_and_chain(3);
        for epoch in [0, 1] {
            let (record, cert) = &chain[epoch as usize];
            db.save_epoch_record_with_cert(record, cert).unwrap();
        }
        // The walk's request for epoch 2 (three tries) fails; the retry then gets it.
        let (network, requests) =
            counting_flaky_network(chain.iter().cloned().map(|rc| (rc.0.epoch, rc)).collect(), 3);
        let bus = ConsensusBus::new();
        let task_manager = TaskManager::new("collector-test");
        let shutdown = Notifier::new();
        spawn_epoch_record_collector(
            db.clone(),
            network,
            bus.clone(),
            task_manager.get_spawner(),
            shutdown.subscribe(),
        )
        .await
        .unwrap();
        bus.requested_missing_epoch().send_replace(2);

        // Paused clock: this runs the walk, the retry and several idle passes.
        tokio::time::sleep(Duration::from_secs(600)).await;
        shutdown.notify();

        assert!(matches!(db.get_epoch_by_number(2), Some((_, Some(_)))), "epoch 2 must land");
        // One walk costs three tries for epoch 2 plus three tries per tip probe; the retry adds
        // one request. A second walk would add another `walk`.
        let walk = 3 + 3 * MAX_GAP_SKIP as usize;
        let seen = requests.load(Ordering::SeqCst);
        assert!(seen < 2 * walk, "collector kept walking after the record landed: {seen} requests");
    }

    /// Validation used by every path that writes a fetched record: anchored to the parent when we
    /// hold it, certificate-only (with a real committee) when we do not.
    #[test]
    fn record_validation_anchors_to_the_parent_when_it_is_held() {
        let (db, chain) = test_db_and_chain(3);
        for epoch in [0, 1] {
            let (record, cert) = &chain[epoch as usize];
            db.save_epoch_record_with_cert(record, cert).unwrap();
        }
        let (record, cert) = chain[2].clone();
        assert!(epoch_record_valid(&db, 2, &record, &cert));

        let mut wrong_parent = record.clone();
        wrong_parent.parent_hash = B256::repeat_byte(9);
        assert!(!epoch_record_valid(&db, 2, &wrong_parent, &cert), "parent must anchor the record");
        assert!(!epoch_record_valid(&db, 3, &record, &cert), "epoch number must match");
    }

    #[test]
    fn record_validation_falls_back_to_the_cert_only_for_an_orphaned_parent() {
        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .randomize_ports(true)
            .committee_size(NonZeroUsize::new(4).unwrap())
            .build();
        let configs: Vec<_> = fixture.authorities().map(|a| a.consensus_config()).collect();
        let signers: Vec<_> = configs.iter().map(|c| c.key_config()).collect();
        let db = MemDatabase::default();

        // No record for epoch 4 or its parent: only the certificate can carry it.
        let (record, cert) = multi_signed_record(&signers, 5, B256::repeat_byte(3));
        assert!(epoch_record_valid(&db, 5, &record, &cert), "a four-key committee is acceptable");

        // The same record with a committee too small to mean anything is not.
        let (small, small_cert) = multi_signed_record(&signers[..1], 5, B256::repeat_byte(3));
        assert!(
            !epoch_record_valid(&db, 5, &small, &small_cert),
            "a one-key committee must not pass the cert-only path",
        );
    }
}
