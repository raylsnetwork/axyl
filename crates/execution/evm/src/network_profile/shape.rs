//! Boot-time schedule shape verification.
//!
//! The *selected* schedule (built-in `--network` or `--config-file`) must
//! match the relaxed shape before the node touches the datadir's schedule
//! record. Mainnet and testnet are excluded: their schedules are frozen in CI
//! (freeze tests in `chainspec.rs`) and guarded per datadir by the
//! schedule-record gate. Every other chain-id (local, devnet, any new
//! production chain) is checked against the same relaxed rules: Set A forks at
//! `Block(0)` or `Never`, `Erc20PrecompileBytecode` exactly `Block(1)`,
//! `UsdrSupplyCorrection` at any value (warned when not `Never`), and
//! `HybridRewards` at `Never` or `Block(>= 1)`. The canonical new-chain values
//! are recommended but not enforced; the schedule-record gate guards the
//! executed history per datadir.
//!
//! See `docs/hardforks/README.md`.

use rayls_infrastructure_types::RaylsNetwork;
use tracing::warn;

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

    // The relaxed rules. A chain may legitimately run a historical schedule
    // that deviates from the canonical new-chain values, so only the
    // constraints every chain still needs are enforced here; the
    // schedule-record gate pins the executed history per datadir.
    // Set A: Block(0) or Never.
    for fork in [
        RaylsHardFork::AdminTransfer,
        RaylsHardFork::RlsStorage,
        RaylsHardFork::Tokenomics,
        RaylsHardFork::Uups,
    ] {
        let fork_activation = activation(fork);
        if !matches!(fork_activation, ForkActivation::Block(0) | ForkActivation::Never) {
            violations
                .push(format!("{} must be Block(0) or Never, got {fork_activation:?}", fork.name()))
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
            "UsdrSupplyCorrection is set to {fork_activation:?} (expected Never); this \
             is allowed for backward compatibility but on a chain that has never run the \
             pre-#404 history, this corrupts supply accounting — verify the schedule \
             matches what the chain has executed"
        );
    }

    // Set C: hybrid rewards — Never or Block(>= 1); both the canonical
    // new-chain Block(1) and devnet's canonical Never pass.
    let fork_activation = activation(RaylsHardFork::HybridRewards);
    if !matches!(fork_activation, ForkActivation::Never | ForkActivation::Block(1..)) {
        violations
            .push(format!("HybridRewards must be Never or Block(>= 1), got {fork_activation:?}"));
    }

    // Set D: HeaderParentSigners never before EpochCloseSeedV2. Before SeedV2 the
    // epoch-closing block's seed still depends on the leader certificate's local
    // aggregate signature, the per-node value HeaderParentSigners exists to stop
    // relying on. Worse, HeaderParentSigners rewrites a stored certificate's
    // signature when a voter adopts a verified claim, so with the pre-SeedV2 seed
    // still live a restarted node could feed a different leader signature into the
    // close block than its peers did. A profile that schedules HeaderParentSigners
    // must therefore schedule SeedV2 at the same block or earlier. Baked-in
    // schedules are pinned by a chainspec test; this covers `--config-file`
    // profiles.
    if let ForkActivation::Block(signers) = activation(RaylsHardFork::HeaderParentSigners) {
        match activation(RaylsHardFork::EpochCloseSeedV2) {
            ForkActivation::Block(seed) if seed <= signers => {}
            other => violations.push(format!(
                "HeaderParentSigners at Block({signers}) requires EpochCloseSeedV2 at the same \
                 block or earlier, got {other:?}"
            )),
        }
    }

    if violations.is_empty() {
        return Ok(());
    }
    eyre::bail!(
        "hardfork schedule for chain-id {} does not match the relaxed shape: {}",
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

    /// Local built-in passes the relaxed shape.
    #[test]
    fn local_builtin_passes() {
        let profile = NetworkProfile::from_builtin(RaylsNetwork::Local);
        assert!(verify_schedule_shape(&profile).is_ok());
    }

    /// Devnet built-in passes the relaxed shape.
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

    /// HybridRewards = Block(0) is rejected.
    #[test]
    fn hybrid_block_0_is_refused() {
        let profile =
            new_chain_profile(99_999, (RaylsHardFork::HybridRewards, ForkActivation::Block(0)));
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

    /// Behavioral forks are unconstrained, apart from the `HeaderParentSigners` /
    /// `EpochCloseSeedV2` ordering tested below.
    #[test]
    fn behavioral_forks_are_unconstrained() {
        let profile = new_chain_profile(
            99_999,
            (RaylsHardFork::OutputSeqNormalization, ForkActivation::Block(999_999)),
        );
        assert!(verify_schedule_shape(&profile).is_ok());
    }

    /// The one cross-fork constraint: a profile scheduling `HeaderParentSigners` must schedule
    /// `EpochCloseSeedV2` at the same block or earlier.
    #[test]
    fn header_parent_signers_requires_epoch_close_seed_v2_first() {
        // SeedV2 after the signers fork
        let mut profile = new_chain_profile(
            99_999,
            (RaylsHardFork::HeaderParentSigners, ForkActivation::Block(100)),
        );
        profile.hardforks.insert(ForkName::from("EpochCloseSeedV2"), ForkActivation::Block(101));
        let msg = verify_schedule_shape(&profile).unwrap_err().to_string();
        assert!(msg.contains("HeaderParentSigners"), "{msg}");

        // SeedV2 never
        profile.hardforks.insert(ForkName::from("EpochCloseSeedV2"), ForkActivation::Never);
        assert!(verify_schedule_shape(&profile).is_err());

        // SeedV2 at the same block, and earlier: fine
        profile.hardforks.insert(ForkName::from("EpochCloseSeedV2"), ForkActivation::Block(100));
        assert!(verify_schedule_shape(&profile).is_ok());
        profile.hardforks.insert(ForkName::from("EpochCloseSeedV2"), ForkActivation::Block(7));
        assert!(verify_schedule_shape(&profile).is_ok());

        // the signers fork unscheduled: no constraint on SeedV2
        profile.hardforks.insert(ForkName::from("HeaderParentSigners"), ForkActivation::Never);
        profile.hardforks.insert(ForkName::from("EpochCloseSeedV2"), ForkActivation::Never);
        assert!(verify_schedule_shape(&profile).is_ok());
    }

    // --- Relaxed rules -------------------------------------------------------

    /// Set A at Block(0) or Never is allowed.
    #[test]
    fn set_a_at_block_0_or_never_passes() {
        for fork in [
            RaylsHardFork::AdminTransfer,
            RaylsHardFork::RlsStorage,
            RaylsHardFork::Tokenomics,
            RaylsHardFork::Uups,
        ] {
            let profile = new_chain_profile(99_999, (fork, ForkActivation::Never));
            assert!(verify_schedule_shape(&profile).is_ok());
        }
        let profile =
            new_chain_profile(99_999, (RaylsHardFork::AdminTransfer, ForkActivation::Block(0)));
        assert!(verify_schedule_shape(&profile).is_ok());
    }

    /// UsdrSupplyCorrection is warning-only: any value passes.
    #[test]
    fn usdr_supply_correction_passes() {
        let profile = new_chain_profile(
            99_999,
            (RaylsHardFork::UsdrSupplyCorrection, ForkActivation::Block(1)),
        );
        assert!(verify_schedule_shape(&profile).is_ok());
        let profile = new_chain_profile(
            99_999,
            (RaylsHardFork::UsdrSupplyCorrection, ForkActivation::Block(1_000_000)),
        );
        assert!(verify_schedule_shape(&profile).is_ok());
    }

    /// HybridRewards at Never or Block(>= 1) is allowed.
    #[test]
    fn hybrid_never_or_future_passes() {
        let profile =
            new_chain_profile(99_999, (RaylsHardFork::HybridRewards, ForkActivation::Never));
        assert!(verify_schedule_shape(&profile).is_ok());
        let profile =
            new_chain_profile(99_999, (RaylsHardFork::HybridRewards, ForkActivation::Block(5)));
        assert!(verify_schedule_shape(&profile).is_ok());
    }

    /// The devnet built-in's HybridRewards at Block(0) is still refused.
    #[test]
    fn devnet_hybrid_block_0_is_refused() {
        let mut profile = NetworkProfile::from_builtin(RaylsNetwork::Devnet);
        profile
            .hardforks
            .insert(ForkName::from(RaylsHardFork::HybridRewards.name()), ForkActivation::Block(0));
        let err = verify_schedule_shape(&profile).unwrap_err();
        assert!(err.to_string().contains("HybridRewards"));
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
            .insert(ForkName::from(RaylsHardFork::HybridRewards.name()), ForkActivation::Block(0));
        let msg = verify_schedule_shape(&profile).unwrap_err().to_string();
        assert!(msg.contains("Erc20PrecompileBytecode"));
        assert!(msg.contains("HybridRewards"));
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
