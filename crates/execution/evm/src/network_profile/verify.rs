use std::path::Path;

use crate::chainspec::RaylsHardFork;

use super::{
    activation::ForkActivation, fork_name::ForkName, profile::NetworkProfile,
    record::ScheduleRecord,
};

/// A fork whose boundary differs between the recorded and selected schedules
/// but is still in the future (not yet executed at verification time).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FutureForkMove {
    /// The fork whose boundary moved.
    pub fork: RaylsHardFork,
    /// The recorded boundary; `None` when the record has the fork as `never`.
    pub recorded: Option<u64>,
    /// The boundary selected for this boot; `None` when never.
    pub selected: Option<u64>,
}

/// Verify the hardfork schedule selected for this boot against the datadir's
/// [`ScheduleRecord`].
///
/// `head` is the chain's current highest block. For every fork whose boundary
/// differs between record and selection, the lower of the two boundaries is the
/// first block the two schedules disagree about: `<= head` means the executed
/// history would be re-interpreted, so this collects an error for it; `> head`
/// is a future move, allowed and reported in the returned list for the caller
/// to warn about. All executed-history disagreements are reported together in
/// a single error, so one refusal lists every fork that must be fixed.
///
/// `record_path` is named in the error so the refusal carries its remedy (fix
/// the fork's entry in the record, or delete it to re-record).
pub fn verify_schedule(
    record: &ScheduleRecord,
    profile: &NetworkProfile,
    head: u64,
    record_path: &Path,
) -> eyre::Result<Vec<FutureForkMove>> {
    let chain_id = profile.chain_id;
    if record.chain_id != chain_id {
        eyre::bail!(
            "schedule record belongs to chain-id {} but the selected schedule targets \
             chain-id {chain_id}; the datadir's schedule record does not match the \
             selected schedule",
            record.chain_id
        );
    }
    for name in record.hardforks.keys() {
        if RaylsHardFork::from_name(name.as_str()).is_none() {
            eyre::bail!("schedule record contains unknown hardfork '{name}'");
        }
    }
    let mut moves = Vec::new();
    let mut refused = Vec::new();
    for fork in RaylsHardFork::VARIANTS {
        let recorded = record.activation(fork.name());
        let selected_block =
            profile.hardforks.get(&ForkName::from(fork.name())).and_then(ForkActivation::block);
        if recorded == selected_block {
            continue;
        }
        // `recorded != selected_block`, so at least one of the two is `Some`.
        let first_disagreement = [recorded, selected_block]
            .into_iter()
            .flatten()
            .min()
            .expect("at least one boundary is Some when the schedules differ");
        if first_disagreement <= head {
            let detail = match (record.entry(fork.name()), selected_block) {
                // The record has no entry for the fork: it was written before
                // the fork existed, and the selected schedule back-dates the
                // fork into the executed history.
                (None, Some(block)) => format!(
                    "hardfork '{}' is absent from the schedule record (the datadir predates \
                     this fork), but the selected schedule activates it at block {block} while \
                     the chain has already executed block {head}",
                    fork.name()
                ),
                // The record pins the fork as never, and the selected schedule
                // back-dates its activation into the executed history.
                (Some(ForkActivation::Never), Some(block)) => format!(
                    "hardfork '{}' is recorded as never, but the selected schedule activates \
                     it at block {block} while the chain has already executed block {head}",
                    fork.name()
                ),
                // The record activated the fork within the executed history at
                // a different boundary.
                (Some(ForkActivation::Block(from_block)), Some(to_block)) => format!(
                    "hardfork '{}' boundary changed from {from_block} to {to_block} while the \
                     chain has already executed block {head}",
                    fork.name()
                ),
                // The record activated the fork within the executed history;
                // the selected schedule never activates it.
                (Some(ForkActivation::Block(from_block)), None) => format!(
                    "hardfork '{}' was recorded at block {from_block}, but the selected \
                     schedule never activates it while the chain has already executed block \
                     {head}",
                    fork.name()
                ),
                // `activation()` reads both as `None`, so these cannot differ. Reached only
                // when `recorded != selected_block`, so `None == None` is impossible here.
                (None, None) | (Some(ForkActivation::Never), None) => {
                    unreachable!("a never-activating selection cannot disagree with the record")
                }
            };
            refused.push(detail);
            continue;
        }
        moves.push(FutureForkMove { fork: *fork, recorded, selected: selected_block });
    }
    if !refused.is_empty() {
        eyre::bail!(
            "schedule inconsistencies:\n{}\n\
             An executed fork's activation block cannot change and a new fork cannot be \
             back-dated into the executed history. Refusing to start with a schedule \
             inconsistent with the chain's history. Remedy: add or fix the fork's entry in \
             {record_path:?}, or delete {record_path:?} to re-record the selected schedule \
             (the executed-history check then starts from the current head)",
            refused.join("\n\n")
        );
    }
    Ok(moves)
}
