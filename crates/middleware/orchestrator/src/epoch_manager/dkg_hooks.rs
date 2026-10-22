//! Fault injection for the DKG end-to-end tests. Compiled in only with the `dkg-test-hooks`
//! feature; without it every hook is a no-op. A test sets these on one node process:
//!
//! - `RL_TEST_DKG_HOLD_DEALINGS=<path>`: while a file exists at `path`, the node submits no dealing
//!   (it still registers its key). A held epoch reaches its boundary without the next key, which
//!   forces the extended epoch.
//! - `RL_TEST_DKG_CRASH_AT=<point>`, with `RL_TEST_DKG_CRASH_EPOCH=<epoch>` (default 1): the
//!   process aborts, with no shutdown, the first time it reaches `point` in an epoch at or past
//!   that one. The points are [`CrashPoint`]'s names.
//! - `RL_TEST_DKG_KEY_WAIT=<seconds>`: how long past its boundary an epoch waits for the next key
//!   before it closes without one. It decides where the epoch closes, so a test sets it on every
//!   node.

use rayls_infrastructure_types::Epoch;

/// A step of the DKG a test can crash a node at.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CrashPoint {
    /// The encryption-key registration is in the node's pool, not executed yet.
    RegistrationSubmitted,
    /// The dealing is built and signed, not submitted yet.
    DealingBuilt,
    /// The dealing is in the node's pool, not executed yet.
    DealingSubmitted,
    /// The node saw its dealing accepted on-chain.
    DealingAccepted,
    /// The close gate kept the epoch open past its boundary: the next key is not ready.
    EpochHeldOpen,
    /// The epoch's DKG outcome is saved, before the node derives its share.
    OutcomeSaved,
    /// The node derived its share of the epoch key and is about to start the epoch.
    ShareDerived,
}

impl CrashPoint {
    #[cfg_attr(not(feature = "dkg-test-hooks"), allow(dead_code))]
    fn name(self) -> &'static str {
        match self {
            Self::RegistrationSubmitted => "registration-submitted",
            Self::DealingBuilt => "dealing-built",
            Self::DealingSubmitted => "dealing-submitted",
            Self::DealingAccepted => "dealing-accepted",
            Self::EpochHeldOpen => "epoch-held-open",
            Self::OutcomeSaved => "outcome-saved",
            Self::ShareDerived => "share-derived",
        }
    }
}

/// Abort the process if the test asked for a crash at `point` in `epoch`.
#[cfg(feature = "dkg-test-hooks")]
pub(super) fn crash_point(point: CrashPoint, epoch: Epoch) {
    if std::env::var("RL_TEST_DKG_CRASH_AT").ok().as_deref() != Some(point.name()) {
        return;
    }
    let from = std::env::var("RL_TEST_DKG_CRASH_EPOCH")
        .ok()
        .and_then(|value| value.parse::<Epoch>().ok())
        .unwrap_or(1);
    if epoch < from {
        return;
    }
    eprintln!("dkg test hook: crashing at {} in epoch {epoch}", point.name());
    tracing::error!(target: "epoch-manager", point = point.name(), epoch, "dkg test hook: crashing");
    std::process::abort();
}

#[cfg(not(feature = "dkg-test-hooks"))]
#[inline(always)]
pub(super) fn crash_point(_point: CrashPoint, _epoch: Epoch) {}

/// Whether the test holds this node's dealings back.
#[cfg(feature = "dkg-test-hooks")]
pub(super) fn dealings_held() -> bool {
    std::env::var_os("RL_TEST_DKG_HOLD_DEALINGS")
        .is_some_and(|path| std::path::Path::new(&path).exists())
}

#[cfg(not(feature = "dkg-test-hooks"))]
#[inline(always)]
pub(super) fn dealings_held() -> bool {
    false
}

/// The test's override of how long an epoch waits past its boundary for the next key.
#[cfg(feature = "dkg-test-hooks")]
pub(super) fn key_wait() -> Option<u64> {
    std::env::var("RL_TEST_DKG_KEY_WAIT").ok().and_then(|value| value.parse().ok())
}

#[cfg(not(feature = "dkg-test-hooks"))]
#[inline(always)]
pub(super) fn key_wait() -> Option<u64> {
    None
}
