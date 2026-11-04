//! Test-only fault injection, compiled only with the `cert-gossip-drop-hook` feature. NOT FOR
//! PRODUCTION.
//!
//! Drops one round of certificate gossip, both sent and received, when the e2e test asks for
//! it: the file named by `RAYLS_TEST_CERT_GOSSIP_DROP_FILE` appearing arms the hook for the
//! second round after the certificate being handled (a round whose gossip has not started yet),
//! and the hook removes the file. It re-arms only after the chain has moved past that round.

use parking_lot::Mutex;
use rayls_infrastructure_types::{Epoch, Round};
use std::path::PathBuf;
use tracing::warn;

/// Rounds between the certificate that finds the trigger file and the round dropped.
const ARM_AHEAD: Round = 2;

/// The round whose certificate gossip is dropped, once armed.
static ARMED: Mutex<Option<(Epoch, Round)>> = Mutex::new(None);

/// Whether the certificate gossip (sent or received) of `epoch`/`round` must be dropped.
pub(crate) fn drop_certificate_gossip(epoch: Epoch, round: Round, sending: bool) -> bool {
    let Some(trigger) = std::env::var_os("RAYLS_TEST_CERT_GOSSIP_DROP_FILE").map(PathBuf::from)
    else {
        return false;
    };
    let mut armed = ARMED.lock();
    match *armed {
        Some((e, r)) if e == epoch && r == round => {
            warn!(target: "primary::test_hook", epoch, round, sending, "test hook: dropping certificate gossip");
            return true;
        }
        Some((e, r)) if e != epoch || round > r + ARM_AHEAD => *armed = None,
        Some(_) => return false,
        None => {}
    }
    if std::fs::remove_file(&trigger).is_ok() {
        let target = round + ARM_AHEAD;
        warn!(target: "primary::test_hook", epoch, round = target, "test hook: armed certificate gossip drop");
        *armed = Some((epoch, target));
    }
    false
}
