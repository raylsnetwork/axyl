//! Test-only fault: a proposer that anchors its headers on an old execution block.
//!
//! Compiled only with the `fault-injection-test-hooks` feature, never in a production build. A
//! header's `latest_execution_block` is the block a syncing node must execute before its header
//! streamer releases a commit led by that header. Voters check only that the anchor does not
//! diverge from a block in their recently executed window, not that it is recent, so a faulty
//! proposer can anchor a new epoch's header on a block of the previous epoch. A syncing node then
//! receives the new epoch's first commit while its subscriber still holds the previous epoch's
//! committee. It is armed per process by environment:
//!
//!   RL_FAULT_STALE_ANCHOR_ROUNDS=<R>  anchor headers of rounds 1..=R of every epoch on an older
//!                                     block (arms the hook)
//!   RL_FAULT_STALE_ANCHOR_BACK=<K>    the anchor is at least K blocks behind the honest one, or
//!                                     the oldest one available if none is that old (default 10)
//!
//! The older block is a real one: an anchor this process proposed with earlier, or the anchor of
//! one of the header's parents. Right after an epoch transition the recently executed window starts
//! at the previous epoch's closing block, so it holds nothing older; a validator in its first epoch
//! has no earlier anchor of its own and takes its parents' until it has. With no older anchor at
//! all, the header stays honest.

use rayls_infrastructure_types::{BlockNumHash, Epoch, Round};
use std::{collections::VecDeque, sync::Mutex};
use tracing::warn;

/// Honest anchors this process proposed with, oldest first.
static HISTORY: Mutex<VecDeque<BlockNumHash>> = Mutex::new(VecDeque::new());

/// Anchors kept in [`HISTORY`].
const HISTORY_LEN: usize = 1024;

/// Returns the anchor to put in a header for `(epoch, round)`: an older anchor when the hook is
/// armed for that round, otherwise `honest`. `parents` are the anchors of the header's parents.
pub(super) fn override_anchor(
    epoch: Epoch,
    round: Round,
    honest: BlockNumHash,
    parents: impl Iterator<Item = BlockNumHash>,
) -> BlockNumHash {
    let Some(rounds) = std::env::var("RL_FAULT_STALE_ANCHOR_ROUNDS")
        .ok()
        .and_then(|s| s.trim().parse::<Round>().ok())
    else {
        return honest;
    };
    let back: u64 =
        std::env::var("RL_FAULT_STALE_ANCHOR_BACK").ok().and_then(|s| s.parse().ok()).unwrap_or(10);

    let mut history = HISTORY.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if history.back() != Some(&honest) {
        history.push_back(honest);
        if history.len() > HISTORY_LEN {
            history.pop_front();
        }
    }
    if round > rounds || honest.number == 0 {
        return honest;
    }
    // Genesis parents carry no anchor (block 0, zero hash), so they are skipped.
    let mut candidates: Vec<BlockNumHash> = history
        .iter()
        .copied()
        .chain(parents.filter(|anchor| anchor.number > 0))
        .filter(|anchor| anchor.number < honest.number)
        .collect();
    candidates.sort_by_key(|anchor| anchor.number);
    let Some(stale) = candidates
        .iter()
        .rev()
        .find(|anchor| anchor.number.saturating_add(back) <= honest.number)
        .or_else(|| candidates.first())
        .copied()
    else {
        return honest;
    };
    warn!(
        target: "primary::fault",
        epoch,
        round,
        honest_anchor = honest.number,
        stale_anchor = stale.number,
        "FAULT STALE ANCHOR: proposing with an older execution anchor"
    );
    stale
}
