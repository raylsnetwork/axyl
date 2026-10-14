//! Test-only load generator for the missing-certificate job-slot reproduction.
//!
//! Compiled only with the `cert-job-flood-hook` cargo feature, never in a production build.
//! When armed by environment, this node sends ordinary, valid missing-certificates requests to
//! its committee peers, on many concurrent streams, over and over. Each request is well within
//! the size limits: it names no authorities and asks for every certificate above the GC round,
//! which is the most work a single valid request can ask a peer to do. The requester gives up on
//! each request after a short timeout, as a real catch-up node would, but the peer's storage job
//! keeps running. On a node with no limit on these jobs, the jobs pile up.
//!
//! Honest nodes never run this code: default builds do not compile it.

use super::{MissingCertificatesRequest, PrimaryNetworkHandle};
use rayls_infrastructure_types::{AuthorityIdentifier, Committee, TaskSpawner};
use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, LazyLock,
    },
    time::{Duration, Instant},
};
use tracing::{info, warn};

/// First network start, so the start delay survives the per-epoch restart of this task.
static PROCESS_START: LazyLock<Instant> = LazyLock::new(Instant::now);

/// Read a number from the environment, with a default.
fn env_num(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// Start the load if the environment asks for it.
///
/// `RL_CJFLOOD_TARGETS=<n>`  send to the first n committee peers (arms the load)
/// `RL_CJFLOOD_TARGET_BLS=<a,b>` send only to peers with these BLS keys, overriding the count
/// `RL_CJFLOOD_STREAMS=<n>`  concurrent requests per peer (default 32)
/// `RL_CJFLOOD_GIVEUP_MS=<n>` how long before the requester drops each request (default 1000)
/// `RL_CJFLOOD_DELAY_SECS=<n>` delay before the first request, from process start (default 0)
pub(super) fn spawn_flood(
    network: PrimaryNetworkHandle,
    committee: Committee,
    myself: Option<AuthorityIdentifier>,
    max_response_size: usize,
    task_spawner: &TaskSpawner,
) {
    let start = *PROCESS_START;
    warn!(target: "primary::load", "cert-job-flood test-only hook is compiled into this binary");
    let Some(targets_wanted) =
        std::env::var("RL_CJFLOOD_TARGETS").ok().and_then(|v| v.parse::<usize>().ok())
    else {
        return;
    };
    let streams = env_num("RL_CJFLOOD_STREAMS", 32);
    let giveup = Duration::from_millis(env_num("RL_CJFLOOD_GIVEUP_MS", 1000));
    let delay = Duration::from_secs(env_num("RL_CJFLOOD_DELAY_SECS", 0));

    // A valid request: name every committee member with no rounds to skip, so the peer returns
    // every certificate it has for them above the GC round, up to the largest response. This is
    // the most work a single valid request can ask a peer to do, the same shape an honest
    // catch-up request has.
    let skip_rounds: BTreeMap<AuthorityIdentifier, BTreeSet<u32>> =
        committee.authorities().iter().map(|a| (a.id(), BTreeSet::new())).collect();
    let request = MissingCertificatesRequest::default()
        .set_bounds(0, skip_rounds)
        .expect("bounds")
        .set_max_response_size(max_response_size);

    // Either send to named peers (by BLS key), or to the first `targets_wanted` committee peers.
    let only: Option<HashSet<String>> = std::env::var("RL_CJFLOOD_TARGET_BLS")
        .ok()
        .map(|v| v.split(',').map(|s| s.trim().to_string()).collect());
    let mut targets: Vec<_> = committee
        .others_primaries_by_id(myself.as_ref())
        .into_iter()
        .filter(|(_, key)| only.as_ref().is_none_or(|set| set.contains(&key.to_string())))
        .collect();
    if only.is_none() {
        targets.truncate(targets_wanted);
    }
    // Refuse to start rather than run with no target, so a test cannot pass without load.
    if targets.is_empty() {
        warn!(target: "primary::load", "cert-job-flood not started: no target peer");
        return;
    }
    warn!(
        target: "primary::load",
        streams, giveup_ms = giveup.as_millis(), delay_secs = delay.as_secs(),
        targets = ?targets.iter().map(|(id, _)| id.to_string()).collect::<Vec<_>>(),
        "cert-job-flood enabled",
    );

    let sent = Arc::new(AtomicU64::new(0));
    for (peer_id, peer_key) in targets {
        for stream in 0..streams {
            let network = network.clone();
            let request = request.clone();
            let sent = sent.clone();
            let peer_id = peer_id.clone();
            task_spawner.spawn_task(format!("cert-job-flood-{peer_id}-{stream}"), async move {
                tokio::time::sleep_until((start + delay).into()).await;
                loop {
                    // Give up on each request quickly, as a real catch-up node would. The peer's
                    // storage job keeps running whether or not we wait for its answer.
                    let _ = tokio::time::timeout(
                        giveup,
                        network.fetch_certificates(peer_key, request.clone()),
                    )
                    .await;
                    let n = sent.fetch_add(1, Ordering::Relaxed) + 1;
                    if n.is_multiple_of(200) {
                        info!(target: "primary::load", sent = n, %peer_id, "cert-job-flood progress");
                    }
                }
            });
        }
    }
}
