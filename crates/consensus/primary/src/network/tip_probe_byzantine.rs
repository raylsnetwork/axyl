//! Test-only Byzantine override for the latest-consensus-header probe.
//!
//! Compiled only with the `byzantine-test-hooks` feature, never in a production build. It lets a
//! committee member answer a follower's tip probe with a fake header, which is the reproduction
//! lever for the tip-probe-number issue. It is armed per process by environment, so a single node
//! in a test network lies while the rest stay honest:
//!
//!   RL_BYZ_FAKE_TIP=1   answer a request with no number and no hash with the fake header, and a
//!                       request by hash for the fake header's digest with the same header

use rayls_infrastructure_types::{
    Certificate, CommittedSubDag, ConsensusHeader, Epoch, Hash as _, ReputationScores, B256,
};
use std::sync::LazyLock;
use tracing::warn;

/// The fake tip: the highest block number there is, and an unsigned leader claiming an epoch no
/// committee exists for. Built once so its digest is stable across requests.
static FAKE_TIP: LazyLock<ConsensusHeader> = LazyLock::new(|| {
    let mut leader = Certificate::default();
    leader.header.epoch = Epoch::MAX;
    let sub_dag = CommittedSubDag::new(vec![], leader, 0, ReputationScores::default(), None);
    ConsensusHeader { parent_hash: B256::repeat_byte(0xAB), sub_dag, number: u64::MAX, extra: B256::ZERO }
});

/// Returns the fake header to serve for a latest-header request, or for a by-hash request for the
/// fake header itself, or None to serve the honest reply.
pub(super) fn override_tip_probe(
    number: Option<u64>,
    hash: Option<B256>,
) -> Option<ConsensusHeader> {
    if std::env::var("RL_BYZ_FAKE_TIP").ok().as_deref() != Some("1") {
        return None;
    }
    let fake = &*FAKE_TIP;
    let kind = match (number, hash) {
        (None, None) => "latest",
        (_, Some(h)) if h == fake.digest() => "by-hash",
        _ => return None,
    };
    warn!(
        target: "primary::byzantine",
        kind,
        fake_number = fake.number,
        fake_digest = ?fake.digest(),
        "BYZANTINE TIP-PROBE: serving fake consensus header"
    );
    Some(fake.clone())
}
