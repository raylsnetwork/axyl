//! Boot-time schedule shape verification.
//!
//! The *selected* schedule (built-in `--network` or `--config-file`) must
//! match the canonical shape of its network family before the node touches
//! the datadir's schedule record. Mainnet and testnet are excluded: their
//! schedules are frozen in CI (freeze tests in `chainspec.rs`) and guarded
//! per datadir by the schedule-record gate. Every other chain-id (local,
//! devnet, any new production chain) is checked against the rule set of its
//! state:
//!
//! - A **new chain** — the datadir has no executed blocks (`head == 0`) —
//!   must match the canonical new-chain shape, because a greenfield genesis
//!   only works with it.
//! - An **existing chain** (`head > 0`) may legitimately run a historical
//!   schedule that deviates from the canonical one, so only the constraints
//!   an executed chain still needs are enforced: Set A forks at `Block(0)` or
//!   `Never`, `Erc20PrecompileBytecode` exactly `Block(1)`,
//!   `UsdrSupplyCorrection` at any value (warned when not `Never`), and
//!   `HybridRewards` at `Never` or `Block(>= 1)`. The schedule-record gate
//!   guards the executed history per datadir.
//!
//! See `docs/hardforks/README.md`.

use rayls_infrastructure_types::RaylsNetwork;
use tracing::warn;

use super::{ForkActivation, ForkName, NetworkProfile};
use crate::chainspec::RaylsHardFork;

/// Verify the shape of the selected schedule for a chain at the given
/// executed `head`. See the module docs for the rules. Refuses with every
/// violation listed in one message.
pub fn verify_schedule_shape(profile: &NetworkProfile, head: u64) -> eyre::Result<()> {
    let mainnet_id = RaylsNetwork::Mainnet.chain_id();
    let testnet_id = RaylsNetwork::Testnet.chain_id();
    if profile.chain_id == mainnet_id || profile.chain_id == testnet_id {
        // Historical networks: frozen in CI, guarded per datadir by the
        // schedule-record gate; no shape constraint here.
        return Ok(());
    }

    let activation = |fork: RaylsHardFork| -> ForkActivation {
        profile
            .hardforks
            .get(&ForkName::from(fork.name()))
            .copied()
            .unwrap_or(ForkActivation::Never)
    };
    let mut violations: Vec<String> = Vec::new();

    if head == 0 {
        // New chain: the canonical new-chain shape — a greenfield genesis
        // only works with it.
        // Set A: Block(0).
        for fork in [
            RaylsHardFork::AdminTransfer,
            RaylsHardFork::RlsStorage,
            RaylsHardFork::Tokenomics,
            RaylsHardFork::Uups,
        ] {
            let fork_activation = activation(fork);
            if fork_activation != ForkActivation::Block(0) {
                violations
                    .push(format!("{} must be Block(0), got {fork_activation:?}", fork.name()))
            }
        }

        // Set B: USDr precompile.
        let fork_activation = activation(RaylsHardFork::Erc20PrecompileBytecode);
        if fork_activation != ForkActivation::Block(1) {
            violations.push(format!(
                "Erc20PrecompileBytecode must be exactly Block(1), got {fork_activation:?}"
            ));
        }
        let fork_activation = activation(RaylsHardFork::UsdrSupplyCorrection);
        if fork_activation != ForkActivation::Never {
            violations.push(format!("UsdrSupplyCorrection must be Never, got {fork_activation:?}"));
        }

        // Set C: hybrid rewards (unconstrained for Devnet).
        let devnet_id = RaylsNetwork::Devnet.chain_id();
        if profile.chain_id != devnet_id {
            let fork_activation = activation(RaylsHardFork::HybridRewards);
            if fork_activation != ForkActivation::Block(1) {
                violations.push(format!(
                    "HybridRewards must be exactly Block(1), got {fork_activation:?}"
                ));
            }
        }
    } else {
        // Existing chain: the relaxed rules. The chain has executed history,
        // so the schedule it ran under is pinned per datadir by the
        // schedule-record gate; only the constraints an executed chain still
        // needs are enforced here.
        // Set A: Block(0) or Never.
        for fork in [
            RaylsHardFork::AdminTransfer,
            RaylsHardFork::RlsStorage,
            RaylsHardFork::Tokenomics,
            RaylsHardFork::Uups,
        ] {
            let fork_activation = activation(fork);
            if !matches!(fork_activation, ForkActivation::Block(0) | ForkActivation::Never) {
                violations.push(format!(
                    "{} must be Block(0) or Never, got {fork_activation:?}",
                    fork.name()
                ))
            }
        }

        // Set B: USDr precompile. The precompile must be in place from block
        // 1; the supply correction is history-dependent, so any value is
        // allowed (warned below).
        let fork_activation = activation(RaylsHardFork::Erc20PrecompileBytecode);
        if fork_activation != ForkActivation::Block(1) {
            violations.push(format!(
                "Erc20PrecompileBytecode must be exactly Block(1), got {fork_activation:?}"
            ));
        }
        let fork_activation = activation(RaylsHardFork::UsdrSupplyCorrection);
        if fork_activation != ForkActivation::Never {
            warn!(
                target: "rayls::reth",
                chain_id = profile.chain_id,
                head,
                "UsdrSupplyCorrection is set to {fork_activation:?} (expected Never); this \
                 is allowed on an existing chain but unusual — verify the schedule matches \
                 what the chain has executed"
            );
        }

        // Set C: hybrid rewards — Never or Block(>= 1); devnet's canonical
        // Never is covered, so there is no devnet carve-out for existing
        // chains.
        let fork_activation = activation(RaylsHardFork::HybridRewards);
        if !matches!(fork_activation, ForkActivation::Never | ForkActivation::Block(1..)) {
            violations.push(format!(
                "HybridRewards must be Never or Block(>= 1), got {fork_activation:?}"
            ));
        }
    }

    if violations.is_empty() {
        return Ok(());
    }
    let rule = if head == 0 { "canonical new-chain shape" } else { "existing-chain shape" };
    eyre::bail!(
        "hardfork schedule for chain-id {} does not match the {rule}: {}",
        profile.chain_id,
        violations.join("; ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mainnet is excluded from the shape check (frozen in CI).
    #[test]
    fn mainnet_profile_passes_excluded() {
        let profile = NetworkProfile::from_builtin(RaylsNetwork::Mainnet);
        assert!(verify_schedule_shape(&profile, 0).is_ok());
    }

    /// Testnet is excluded from the shape check (frozen in CI).
    #[test]
    fn testnet_profile_passes_excluded() {
        let profile = NetworkProfile::from_builtin(RaylsNetwork::Testnet);
        assert!(verify_schedule_shape(&profile, 0).is_ok());
    }

    /// Mainnet with a different behavioral value still passes (total exclusion).
    #[test]
    fn mainnet_with_behavioral_change_passes() {
        let mut profile = NetworkProfile::from_builtin(RaylsNetwork::Mainnet);
        profile.hardforks.insert(ForkName::from("EpochCloseSeedV2"), ForkActivation::Block(1));
        assert!(verify_schedule_shape(&profile, 0).is_ok());
    }

    /// Local built-in passes the new-chain shape.
    #[test]
    fn local_builtin_passes() {
        let profile = NetworkProfile::from_builtin(RaylsNetwork::Local);
        assert!(verify_schedule_shape(&profile, 0).is_ok());
    }

    /// Devnet built-in passes the new-chain shape.
    #[test]
    fn devnet_builtin_passes() {
        let profile = NetworkProfile::from_builtin(RaylsNetwork::Devnet);
        assert!(verify_schedule_shape(&profile, 0).is_ok());
    }

    /// A synthetic new-chain profile with canonical values passes.
    #[test]
    fn canonical_new_chain_passes() {
        let profile = new_chain_profile(99_999, (RaylsHardFork::Eip1559, ForkActivation::Block(0)));
        assert!(verify_schedule_shape(&profile, 0).is_ok());
    }

    /// Erc20PrecompileBytecode = Never is rejected.
    #[test]
    fn erc20_never_is_refused() {
        let profile = new_chain_profile(
            99_999,
            (RaylsHardFork::Erc20PrecompileBytecode, ForkActivation::Never),
        );
        let err = verify_schedule_shape(&profile, 0).unwrap_err();
        assert!(err.to_string().contains("Erc20PrecompileBytecode"));
    }

    /// Erc20PrecompileBytecode = Block(5) is rejected.
    #[test]
    fn erc20_block_5_is_refused() {
        let profile = new_chain_profile(
            99_999,
            (RaylsHardFork::Erc20PrecompileBytecode, ForkActivation::Block(5)),
        );
        let err = verify_schedule_shape(&profile, 0).unwrap_err();
        assert!(err.to_string().contains("Erc20PrecompileBytecode"));
    }

    /// UsdrSupplyCorrection = Block(1) is rejected.
    #[test]
    fn usdr_supply_correction_block_1_is_refused() {
        let profile = new_chain_profile(
            99_999,
            (RaylsHardFork::UsdrSupplyCorrection, ForkActivation::Block(1)),
        );
        let err = verify_schedule_shape(&profile, 0).unwrap_err();
        assert!(err.to_string().contains("UsdrSupplyCorrection"));
    }

    /// HybridRewards = Never is rejected.
    #[test]
    fn hybrid_never_is_refused() {
        let profile =
            new_chain_profile(99_999, (RaylsHardFork::HybridRewards, ForkActivation::Never));
        let err = verify_schedule_shape(&profile, 0).unwrap_err();
        assert!(err.to_string().contains("HybridRewards"));
    }

    /// HybridRewards = Block(0) is rejected.
    #[test]
    fn hybrid_block_0_is_refused() {
        let profile =
            new_chain_profile(99_999, (RaylsHardFork::HybridRewards, ForkActivation::Block(0)));
        let err = verify_schedule_shape(&profile, 0).unwrap_err();
        assert!(err.to_string().contains("HybridRewards"));
    }

    /// HybridRewards = Block(5) is rejected.
    #[test]
    fn hybrid_block_5_is_refused() {
        let profile =
            new_chain_profile(99_999, (RaylsHardFork::HybridRewards, ForkActivation::Block(5)));
        let err = verify_schedule_shape(&profile, 0).unwrap_err();
        assert!(err.to_string().contains("HybridRewards"));
    }

    /// AdminTransfer = Block(500) is rejected.
    #[test]
    fn admin_transfer_block_500_is_refused() {
        let profile =
            new_chain_profile(99_999, (RaylsHardFork::AdminTransfer, ForkActivation::Block(500)));
        let err = verify_schedule_shape(&profile, 0).unwrap_err();
        assert!(err.to_string().contains("AdminTransfer"));
    }

    /// RlsStorage = Block(500) is rejected.
    #[test]
    fn rls_storage_block_500_is_refused() {
        let profile =
            new_chain_profile(99_999, (RaylsHardFork::RlsStorage, ForkActivation::Block(500)));
        let err = verify_schedule_shape(&profile, 0).unwrap_err();
        assert!(err.to_string().contains("RlsStorage"));
    }

    /// Tokenomics = Block(500) is rejected.
    #[test]
    fn tokenomics_block_500_is_refused() {
        let profile =
            new_chain_profile(99_999, (RaylsHardFork::Tokenomics, ForkActivation::Block(500)));
        let err = verify_schedule_shape(&profile, 0).unwrap_err();
        assert!(err.to_string().contains("Tokenomics"));
    }

    /// Uups = Block(500) is rejected.
    #[test]
    fn uups_block_500_is_refused() {
        let profile = new_chain_profile(99_999, (RaylsHardFork::Uups, ForkActivation::Block(500)));
        let err = verify_schedule_shape(&profile, 0).unwrap_err();
        assert!(err.to_string().contains("Uups"));
    }

    /// Set A forks at Never are disllowed.
    #[test]
    fn set_a_at_never_is_disallowed() {
        let profile =
            new_chain_profile(99_999, (RaylsHardFork::AdminTransfer, ForkActivation::Never));
        assert!(verify_schedule_shape(&profile, 0).is_err());
        let profile = new_chain_profile(99_999, (RaylsHardFork::RlsStorage, ForkActivation::Never));
        assert!(verify_schedule_shape(&profile, 0).is_err());
        let profile = new_chain_profile(99_999, (RaylsHardFork::Tokenomics, ForkActivation::Never));
        assert!(verify_schedule_shape(&profile, 0).is_err());
        let profile = new_chain_profile(99_999, (RaylsHardFork::Uups, ForkActivation::Never));
        assert!(verify_schedule_shape(&profile, 0).is_err());
    }

    /// Two independent violations are joined into a single error message.
    #[test]
    fn multiple_violations_are_joined_in_one_message() {
        let mut profile = new_chain_profile(
            99_999,
            (RaylsHardFork::Erc20PrecompileBytecode, ForkActivation::Never),
        );
        profile
            .hardforks
            .insert(ForkName::from(RaylsHardFork::HybridRewards.name()), ForkActivation::Block(5));
        let msg = verify_schedule_shape(&profile, 0).unwrap_err().to_string();
        assert!(msg.contains("Erc20PrecompileBytecode"));
        assert!(msg.contains("HybridRewards"));
    }

    /// Behavioral forks are unconstrained.
    #[test]
    fn behavioral_forks_are_unconstrained() {
        let profile = new_chain_profile(
            99_999,
            (RaylsHardFork::EpochCloseSeedV2, ForkActivation::Block(999_999)),
        );
        assert!(verify_schedule_shape(&profile, 0).is_ok());
    }

    // --- Existing chains (head > 0): the relaxed rules ----------------------

    /// Mainnet stays excluded from the shape check at any head.
    #[test]
    fn mainnet_profile_passes_excluded_at_any_head() {
        let profile = NetworkProfile::from_builtin(RaylsNetwork::Mainnet);
        assert!(verify_schedule_shape(&profile, 1_000_000).is_ok());
    }

    /// An existing chain with the canonical shape passes.
    #[test]
    fn canonical_shape_passes_on_existing_chain() {
        let profile = NetworkProfile::from_builtin(RaylsNetwork::Local);
        assert!(verify_schedule_shape(&profile, 1000).is_ok());
    }

    /// Set A at Block(0) or Never is allowed on an existing chain.
    #[test]
    fn set_a_at_block_0_or_never_passes_on_existing_chain() {
        for fork in [
            RaylsHardFork::AdminTransfer,
            RaylsHardFork::RlsStorage,
            RaylsHardFork::Tokenomics,
            RaylsHardFork::Uups,
        ] {
            let profile = new_chain_profile(99_999, (fork, ForkActivation::Never));
            assert!(verify_schedule_shape(&profile, 1000).is_ok());
        }
        let profile =
            new_chain_profile(99_999, (RaylsHardFork::AdminTransfer, ForkActivation::Block(0)));
        assert!(verify_schedule_shape(&profile, 1000).is_ok());
    }

    /// Set A at Block(> 0) is still refused on an existing chain.
    #[test]
    fn set_a_at_block_500_is_refused_on_existing_chain() {
        let profile =
            new_chain_profile(99_999, (RaylsHardFork::AdminTransfer, ForkActivation::Block(500)));
        let err = verify_schedule_shape(&profile, 1000).unwrap_err();
        assert!(err.to_string().contains("AdminTransfer"));
    }

    /// Erc20PrecompileBytecode must still be exactly Block(1) on an existing
    /// chain.
    #[test]
    fn erc20_deviation_is_refused_on_existing_chain() {
        let profile = new_chain_profile(
            99_999,
            (RaylsHardFork::Erc20PrecompileBytecode, ForkActivation::Never),
        );
        let err = verify_schedule_shape(&profile, 1000).unwrap_err();
        assert!(err.to_string().contains("Erc20PrecompileBytecode"));
        let profile = new_chain_profile(
            99_999,
            (RaylsHardFork::Erc20PrecompileBytecode, ForkActivation::Block(5)),
        );
        let err = verify_schedule_shape(&profile, 1000).unwrap_err();
        assert!(err.to_string().contains("Erc20PrecompileBytecode"));
    }

    /// UsdrSupplyCorrection is warning-only on an existing chain: any value
    /// passes.
    #[test]
    fn usdr_supply_correction_passes_on_existing_chain() {
        let profile = new_chain_profile(
            99_999,
            (RaylsHardFork::UsdrSupplyCorrection, ForkActivation::Block(1)),
        );
        assert!(verify_schedule_shape(&profile, 1000).is_ok());
        let profile = new_chain_profile(
            99_999,
            (RaylsHardFork::UsdrSupplyCorrection, ForkActivation::Block(1_000_000)),
        );
        assert!(verify_schedule_shape(&profile, 1000).is_ok());
    }

    /// HybridRewards at Never or Block(>= 1) is allowed on an existing chain.
    #[test]
    fn hybrid_never_or_future_passes_on_existing_chain() {
        let profile =
            new_chain_profile(99_999, (RaylsHardFork::HybridRewards, ForkActivation::Never));
        assert!(verify_schedule_shape(&profile, 1000).is_ok());
        let profile =
            new_chain_profile(99_999, (RaylsHardFork::HybridRewards, ForkActivation::Block(5)));
        assert!(verify_schedule_shape(&profile, 1000).is_ok());
    }

    /// HybridRewards at Block(0) is refused on an existing chain — the devnet
    /// carve-out does not apply once the chain has executed blocks.
    #[test]
    fn hybrid_block_0_is_refused_on_existing_chain() {
        let profile =
            new_chain_profile(99_999, (RaylsHardFork::HybridRewards, ForkActivation::Block(0)));
        let err = verify_schedule_shape(&profile, 1000).unwrap_err();
        assert!(err.to_string().contains("HybridRewards"));
    }

    /// The devnet carve-out is gone for executed devnet chains.
    #[test]
    fn devnet_hybrid_block_0_is_refused_on_existing_chain() {
        let mut profile = NetworkProfile::from_builtin(RaylsNetwork::Devnet);
        profile
            .hardforks
            .insert(ForkName::from(RaylsHardFork::HybridRewards.name()), ForkActivation::Block(0));
        let err = verify_schedule_shape(&profile, 1000).unwrap_err();
        assert!(err.to_string().contains("HybridRewards"));
    }

    /// Build a new-chain profile with one fork overridden.
    fn new_chain_profile(
        chain_id: u64,
        override_: (RaylsHardFork, ForkActivation),
    ) -> NetworkProfile {
        let mut profile = NetworkProfile::from_builtin(RaylsNetwork::Local);
        profile.chain_id = chain_id;
        profile.hardforks.insert(ForkName::from(override_.0.name()), override_.1);
        profile
    }
}
