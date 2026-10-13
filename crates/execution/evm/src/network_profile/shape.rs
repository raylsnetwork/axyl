//! Boot-time schedule shape verification.
//!
//! The *selected* schedule (built-in `--network` or `--config-file`) must
//! match the canonical shape of its network family before the node touches
//! the datadir's schedule record. Mainnet and testnet are excluded: their
//! schedules are frozen in CI (freeze tests in `chainspec.rs`) and guarded
//! per datadir by the schedule-record gate. Every other chain-id (local,
//! devnet, any new production chain) must match the canonical new-chain
//! shape, because a greenfield genesis only works with it (see
//! `docs/hardforks/README.md`):

use rayls_infrastructure_types::RaylsNetwork;

use super::{ForkActivation, ForkName, NetworkProfile};
use crate::chainspec::RaylsHardFork;

/// Verify the shape of the selected schedule. See the module docs for the
/// rules. Refuses with every violation listed in one message.
pub fn verify_schedule_shape(profile: &NetworkProfile) -> eyre::Result<()> {
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

    // Set A: Block(0).
    for fork in [
        RaylsHardFork::AdminTransfer,
        RaylsHardFork::RlsStorage,
        RaylsHardFork::Tokenomics,
        RaylsHardFork::Uups,
    ] {
        let fork_activation = activation(fork);
        if fork_activation != ForkActivation::Block(0) {
            violations.push(format!("{} must be Block(0), got {fork_activation:?}", fork.name()))
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
            violations
                .push(format!("HybridRewards must be exactly Block(1), got {fork_activation:?}",));
        }
    }

    if violations.is_empty() {
        return Ok(());
    }
    eyre::bail!(
        "hardfork schedule for chain-id {} does not match the canonical new-chain shape: {}",
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
        assert!(verify_schedule_shape(&profile).is_ok());
    }

    /// Testnet is excluded from the shape check (frozen in CI).
    #[test]
    fn testnet_profile_passes_excluded() {
        let profile = NetworkProfile::from_builtin(RaylsNetwork::Testnet);
        assert!(verify_schedule_shape(&profile).is_ok());
    }

    /// Mainnet with a different behavioral value still passes (total exclusion).
    #[test]
    fn mainnet_with_behavioral_change_passes() {
        let mut profile = NetworkProfile::from_builtin(RaylsNetwork::Mainnet);
        profile.hardforks.insert(ForkName::from("EpochCloseSeedV2"), ForkActivation::Block(1));
        assert!(verify_schedule_shape(&profile).is_ok());
    }

    /// Local built-in passes the new-chain shape.
    #[test]
    fn local_builtin_passes() {
        let profile = NetworkProfile::from_builtin(RaylsNetwork::Local);
        assert!(verify_schedule_shape(&profile).is_ok());
    }

    /// Devnet built-in passes the new-chain shape.
    #[test]
    fn devnet_builtin_passes() {
        let profile = NetworkProfile::from_builtin(RaylsNetwork::Devnet);
        assert!(verify_schedule_shape(&profile).is_ok());
    }

    /// A synthetic new-chain profile with canonical values passes.
    #[test]
    fn canonical_new_chain_passes() {
        let profile = new_chain_profile(99_999, (RaylsHardFork::Eip1559, ForkActivation::Block(0)));
        assert!(verify_schedule_shape(&profile).is_ok());
    }

    /// Erc20PrecompileBytecode = Never is rejected.
    #[test]
    fn erc20_never_is_refused() {
        let profile = new_chain_profile(
            99_999,
            (RaylsHardFork::Erc20PrecompileBytecode, ForkActivation::Never),
        );
        let err = verify_schedule_shape(&profile).unwrap_err();
        assert!(err.to_string().contains("Erc20PrecompileBytecode"));
    }

    /// Erc20PrecompileBytecode = Block(5) is rejected.
    #[test]
    fn erc20_block_5_is_refused() {
        let profile = new_chain_profile(
            99_999,
            (RaylsHardFork::Erc20PrecompileBytecode, ForkActivation::Block(5)),
        );
        let err = verify_schedule_shape(&profile).unwrap_err();
        assert!(err.to_string().contains("Erc20PrecompileBytecode"));
    }

    /// UsdrSupplyCorrection = Block(1) is rejected.
    #[test]
    fn usdr_supply_correction_block_1_is_refused() {
        let profile = new_chain_profile(
            99_999,
            (RaylsHardFork::UsdrSupplyCorrection, ForkActivation::Block(1)),
        );
        let err = verify_schedule_shape(&profile).unwrap_err();
        assert!(err.to_string().contains("UsdrSupplyCorrection"));
    }

    /// HybridRewards = Never is rejected.
    #[test]
    fn hybrid_never_is_refused() {
        let profile =
            new_chain_profile(99_999, (RaylsHardFork::HybridRewards, ForkActivation::Never));
        let err = verify_schedule_shape(&profile).unwrap_err();
        assert!(err.to_string().contains("HybridRewards"));
    }

    /// HybridRewards = Block(0) is rejected.
    #[test]
    fn hybrid_block_0_is_refused() {
        let profile =
            new_chain_profile(99_999, (RaylsHardFork::HybridRewards, ForkActivation::Block(0)));
        let err = verify_schedule_shape(&profile).unwrap_err();
        assert!(err.to_string().contains("HybridRewards"));
    }

    /// HybridRewards = Block(5) is rejected.
    #[test]
    fn hybrid_block_5_is_refused() {
        let profile =
            new_chain_profile(99_999, (RaylsHardFork::HybridRewards, ForkActivation::Block(5)));
        let err = verify_schedule_shape(&profile).unwrap_err();
        assert!(err.to_string().contains("HybridRewards"));
    }

    /// AdminTransfer = Block(500) is rejected.
    #[test]
    fn admin_transfer_block_500_is_refused() {
        let profile =
            new_chain_profile(99_999, (RaylsHardFork::AdminTransfer, ForkActivation::Block(500)));
        let err = verify_schedule_shape(&profile).unwrap_err();
        assert!(err.to_string().contains("AdminTransfer"));
    }

    /// RlsStorage = Block(500) is rejected.
    #[test]
    fn rls_storage_block_500_is_refused() {
        let profile =
            new_chain_profile(99_999, (RaylsHardFork::RlsStorage, ForkActivation::Block(500)));
        let err = verify_schedule_shape(&profile).unwrap_err();
        assert!(err.to_string().contains("RlsStorage"));
    }

    /// Tokenomics = Block(500) is rejected.
    #[test]
    fn tokenomics_block_500_is_refused() {
        let profile =
            new_chain_profile(99_999, (RaylsHardFork::Tokenomics, ForkActivation::Block(500)));
        let err = verify_schedule_shape(&profile).unwrap_err();
        assert!(err.to_string().contains("Tokenomics"));
    }

    /// Uups = Block(500) is rejected.
    #[test]
    fn uups_block_500_is_refused() {
        let profile = new_chain_profile(99_999, (RaylsHardFork::Uups, ForkActivation::Block(500)));
        let err = verify_schedule_shape(&profile).unwrap_err();
        assert!(err.to_string().contains("Uups"));
    }

    /// Set A forks at Never are disllowed.
    #[test]
    fn set_a_at_never_is_disallowed() {
        let profile =
            new_chain_profile(99_999, (RaylsHardFork::AdminTransfer, ForkActivation::Never));
        assert!(verify_schedule_shape(&profile).is_err());
        let profile = new_chain_profile(99_999, (RaylsHardFork::RlsStorage, ForkActivation::Never));
        assert!(verify_schedule_shape(&profile).is_err());
        let profile = new_chain_profile(99_999, (RaylsHardFork::Tokenomics, ForkActivation::Never));
        assert!(verify_schedule_shape(&profile).is_err());
        let profile = new_chain_profile(99_999, (RaylsHardFork::Uups, ForkActivation::Never));
        assert!(verify_schedule_shape(&profile).is_err());
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
        let msg = verify_schedule_shape(&profile).unwrap_err().to_string();
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
        assert!(verify_schedule_shape(&profile).is_ok());
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
