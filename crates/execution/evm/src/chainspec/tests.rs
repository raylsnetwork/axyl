//! Tests for the Rayls chain spec module.

use std::sync::Arc;

use rayls_infrastructure_types::RaylsNetwork;
use reth_chainspec::{ChainSpec, ForkCondition};

use crate::chainspec::{
    RaylsChainHardforks, RaylsChainSpec, RaylsHardFork, RaylsHardforks, ScheduledFork,
};

/// The activation block of `fork` in `spec`; panics for non-block-scheduled forks.
fn activation_block(spec: &RaylsChainHardforks, fork: RaylsHardFork) -> u64 {
    spec.rayls_fork_activation(fork).block_number().expect("fork is block-scheduled")
}

mod schedule {
    use super::*;

    #[test]
    fn fork_inactive_before_activation_block() {
        let hardforks = RaylsChainHardforks::local();
        let block = activation_block(&hardforks, RaylsHardFork::HybridRewards);
        assert!(!hardforks.is_rayls_fork_active_at_block(RaylsHardFork::HybridRewards, block - 1,));
    }

    #[test]
    fn fork_active_at_activation_block() {
        let hardforks = RaylsChainHardforks::local();
        let block = activation_block(&hardforks, RaylsHardFork::HybridRewards);
        assert!(hardforks.is_rayls_fork_active_at_block(RaylsHardFork::HybridRewards, block,));
    }

    #[test]
    fn fork_active_after_activation_block() {
        let hardforks = RaylsChainHardforks::local();
        let block = activation_block(&hardforks, RaylsHardFork::HybridRewards);
        assert!(hardforks.is_rayls_fork_active_at_block(RaylsHardFork::HybridRewards, block + 1,));
    }

    /// On local, Erc20PrecompileBytecode and HybridRewards share activation block 1;
    /// exactly those two must be reported as newly activated there.
    #[test]
    fn newly_activated_forks_reports_only_forks_crossed_at_block() {
        let hardforks = RaylsChainHardforks::local();
        let block = activation_block(&hardforks, RaylsHardFork::HybridRewards);
        let activated = hardforks.newly_activated_forks(block - 1, block);
        assert_eq!(
            activated,
            vec![RaylsHardFork::Erc20PrecompileBytecode, RaylsHardFork::HybridRewards],
        );
    }

    #[test]
    fn newly_activated_forks_is_empty_when_no_fork_crosses() {
        let hardforks = RaylsChainHardforks::local();
        let block = activation_block(&hardforks, RaylsHardFork::HybridRewards);
        let activated = hardforks.newly_activated_forks(block, block + 1);
        assert!(activated.is_empty());
    }

    /// BatchDigestV2 and AdminTransfer share one activation block on testnet; both must
    /// be reported as newly activated there.
    #[test]
    fn newly_activated_forks_reports_all_forks_at_shared_block() {
        let hardforks = RaylsChainHardforks::for_network(RaylsNetwork::Testnet);
        let block = activation_block(&hardforks, RaylsHardFork::AdminTransfer);
        let activated = hardforks.newly_activated_forks(block - 1, block);
        assert!(activated.contains(&RaylsHardFork::BatchDigestV2));
        assert!(activated.contains(&RaylsHardFork::AdminTransfer));
    }

    /// A fork scheduled `Never` on a network must stay inactive at any block.
    #[test]
    fn never_scheduled_forks_stay_inactive() {
        let never_forks = [
            (RaylsNetwork::Devnet, RaylsHardFork::UsdrSupplyCorrection),
            (RaylsNetwork::Devnet, RaylsHardFork::HybridRewards),
            (RaylsNetwork::Devnet, RaylsHardFork::EpochCloseSeedV2),
            (RaylsNetwork::Testnet, RaylsHardFork::Erc20PrecompileBytecode),
            (RaylsNetwork::Testnet, RaylsHardFork::UsdrSupplyCorrection),
            (RaylsNetwork::Testnet, RaylsHardFork::HybridRewards),
            (RaylsNetwork::Testnet, RaylsHardFork::EpochCloseSeedV2),
            (RaylsNetwork::Mainnet, RaylsHardFork::AdminTransfer),
            (RaylsNetwork::Mainnet, RaylsHardFork::RlsStorage),
            (RaylsNetwork::Mainnet, RaylsHardFork::Tokenomics),
            (RaylsNetwork::Mainnet, RaylsHardFork::Uups),
            (RaylsNetwork::Mainnet, RaylsHardFork::HybridRewards),
            (RaylsNetwork::Mainnet, RaylsHardFork::EpochCloseSeedV2),
            (RaylsNetwork::Local, RaylsHardFork::UsdrSupplyCorrection),
        ];
        for (network, fork) in never_forks {
            let hardforks = RaylsChainHardforks::for_network(network);
            assert!(
                !hardforks.is_rayls_fork_active_at_block(fork, u64::MAX),
                "{network}: {fork:?} is scheduled Never but active at u64::MAX",
            );
        }
    }

    /// Every local fork is genesis-active except three: Erc20PrecompileBytecode and
    /// HybridRewards activate at the first post-genesis block, and UsdrSupplyCorrection
    /// never activates (mainnet-only rebase).
    #[test]
    fn local_schedule_genesis_active_except_block_1_and_never_forks() {
        let hardforks = RaylsChainHardforks::local();
        for fork in RaylsHardFork::VARIANTS {
            let genesis_exception = matches!(
                *fork,
                RaylsHardFork::Erc20PrecompileBytecode
                    | RaylsHardFork::HybridRewards
                    | RaylsHardFork::UsdrSupplyCorrection
            );
            assert_eq!(
                hardforks.is_rayls_fork_active_at_block(*fork, 0),
                !genesis_exception,
                "fork {:?} has unexpected active state at block 0",
                fork
            );
        }
    }

    /// The version byte reports the highest version byte among the forks *active* at the
    /// block, not the most recently crossed block. EpochCloseSeedV2 (0x10) is genesis-active
    /// on local, so it owns the byte across every later activation (HybridRewards at 0x0d
    /// included). On real networks it is Never, so there the highest active fork still
    /// advances the byte normally.
    #[test]
    fn version_byte_is_the_max_active_fork_code() {
        let hardforks = RaylsChainHardforks::local();
        let block = activation_block(&hardforks, RaylsHardFork::HybridRewards);
        assert_eq!(hardforks.version_byte_at_block(0), Some(0x10));
        assert_eq!(hardforks.version_byte_at_block(block - 1), Some(0x10));
        assert_eq!(hardforks.version_byte_at_block(block), Some(0x10));
        assert_eq!(hardforks.version_byte_at_block(1_000_000), Some(0x10));
    }

    /// Every network schedule lists all 16 hardforks in variant order.
    #[test]
    fn schedule_contains_all_hardforks_in_order_for_all_networks() {
        for network in [
            RaylsNetwork::Devnet,
            RaylsNetwork::Testnet,
            RaylsNetwork::Mainnet,
            RaylsNetwork::Local,
        ] {
            let schedule = RaylsHardFork::for_network(network);
            assert_eq!(
                schedule.len(),
                RaylsHardFork::VARIANTS.len(),
                "expected one entry per hardfork for {network}"
            );
            for (i, variant) in RaylsHardFork::VARIANTS.iter().enumerate() {
                assert_eq!(
                    schedule[i].fork, *variant,
                    "network {network}: schedule[{i}] fork mismatch"
                );
            }
        }
    }

    #[test]
    fn builder_overrides_batch_digest_v2_activation() {
        let genesis = rayls_infrastructure_types::test_genesis();
        let chain_spec: ChainSpec = genesis.into();
        let spec = RaylsChainSpec::builder(Arc::new(chain_spec))
            .add_rayls_hardforks_by_type(RaylsNetwork::Devnet)
            .batch_digest_v2(999)
            .build();
        assert!(!spec.is_batch_digest_v2_active_at_block(998));
        assert!(spec.is_batch_digest_v2_active_at_block(999));
    }

    /// The mainnet schedule is frozen: any change requires updating this table and the
    /// schedule in the same PR.
    #[test]
    fn mainnet_schedule_is_frozen() {
        expect_schedule(
            RaylsNetwork::Mainnet,
            &[
                (RaylsHardFork::Eip1559, ForkCondition::Block(0)),
                (RaylsHardFork::BatchDigestV2, ForkCondition::Block(0)),
                (RaylsHardFork::AdminTransfer, ForkCondition::Never),
                (RaylsHardFork::PrecompileGasFix, ForkCondition::Block(0)),
                (RaylsHardFork::RlsStorage, ForkCondition::Never),
                (RaylsHardFork::Tokenomics, ForkCondition::Never),
                (RaylsHardFork::Uups, ForkCondition::Never),
                (RaylsHardFork::Erc20PrecompileBytecode, ForkCondition::Block(893_558)),
                (RaylsHardFork::TransactionLoadBalancing, ForkCondition::Block(893_558)),
                (RaylsHardFork::UsdrSupplyCorrection, ForkCondition::Block(3_569_194)),
                (RaylsHardFork::EmptyOutputBlock, ForkCondition::Block(3_569_194)),
                (RaylsHardFork::DynamicCommitteeSizing, ForkCondition::Block(8_291_010)),
                (RaylsHardFork::HybridRewards, ForkCondition::Never),
                (RaylsHardFork::OutputSeqNormalization, ForkCondition::Block(12_231_705)),
                (RaylsHardFork::SenderAffinityLoadBalancing, ForkCondition::Block(12_231_705)),
                (RaylsHardFork::EpochCloseSeedV2, ForkCondition::Never),
            ],
        );
    }

    /// The testnet schedule is frozen: any change requires updating this table and the
    /// schedule in the same PR.
    #[test]
    fn testnet_schedule_is_frozen() {
        expect_schedule(
            RaylsNetwork::Testnet,
            &[
                (RaylsHardFork::Eip1559, ForkCondition::Block(281_800)),
                (RaylsHardFork::BatchDigestV2, ForkCondition::Block(560_539)),
                (RaylsHardFork::AdminTransfer, ForkCondition::Block(560_539)),
                (RaylsHardFork::PrecompileGasFix, ForkCondition::Block(900_000)),
                (RaylsHardFork::RlsStorage, ForkCondition::Block(900_000)),
                (RaylsHardFork::Tokenomics, ForkCondition::Block(1_879_000)),
                (RaylsHardFork::Uups, ForkCondition::Block(2_872_000)),
                (RaylsHardFork::Erc20PrecompileBytecode, ForkCondition::Never),
                (RaylsHardFork::TransactionLoadBalancing, ForkCondition::Block(4_386_290)),
                (RaylsHardFork::UsdrSupplyCorrection, ForkCondition::Never),
                (RaylsHardFork::EmptyOutputBlock, ForkCondition::Block(6_663_630)),
                (RaylsHardFork::DynamicCommitteeSizing, ForkCondition::Block(10_934_554)),
                (RaylsHardFork::HybridRewards, ForkCondition::Never),
                (RaylsHardFork::OutputSeqNormalization, ForkCondition::Block(17_300_000)),
                (RaylsHardFork::SenderAffinityLoadBalancing, ForkCondition::Block(17_300_000)),
                (RaylsHardFork::EpochCloseSeedV2, ForkCondition::Never),
            ],
        );
    }

    fn expect_schedule(network: RaylsNetwork, expected: &[(RaylsHardFork, ForkCondition)]) {
        let actual = RaylsHardFork::for_network(network);
        assert_eq!(actual.len(), expected.len(), "{network}: fork count");
        for (ScheduledFork { fork, condition }, (want_fork, want_condition)) in
            actual.iter().zip(expected.iter())
        {
            assert_eq!(fork, want_fork, "{network}: fork order");
            assert_eq!(
                condition, want_condition,
                "{network}: {fork:?} must stay {want_condition:?} (frozen schedule — update the freeze test and the schedule in the same PR)"
            );
        }
    }
}

mod hardforks {
    use super::*;

    #[test]
    fn local_network_version_byte_at_block_0() {
        let hardforks = RaylsChainHardforks::local();
        let version = hardforks.version_byte_at_block(0);
        // CertificateFetchEpochV2 (0x11) activates at block 0 on local and is the highest
        // such fork, so it owns the version byte from block 0.
        assert_eq!(version, Some(0x11));
    }

    #[test]
    fn local_network_version_byte_is_the_max_active_code() {
        // CertificateFetchEpochV2 (0x11) is genesis-active on local, so it owns the version
        // byte across every later activation (HybridRewards at 0x0d included): the byte reports
        // the max active code, not the most recently crossed block. On real networks it is Never,
        // so there the highest active fork still advances the byte normally.
        let hardforks = RaylsChainHardforks::local();
        let hybrid_rewards_block = activation_block(&hardforks, RaylsHardFork::HybridRewards);
        assert_eq!(hardforks.version_byte_at_block(hybrid_rewards_block - 1), Some(0x11));
        assert_eq!(hardforks.version_byte_at_block(hybrid_rewards_block), Some(0x11));
        assert_eq!(hardforks.version_byte_at_block(1_000_000), Some(0x11));
    }
}

mod spec {
    use super::*;

    #[test]
    fn builder_overrides_batch_digest_v2_activation() {
        let genesis = rayls_infrastructure_types::test_genesis();
        let chain_spec: ChainSpec = genesis.into();
        let spec = RaylsChainSpec::builder(Arc::new(chain_spec))
            .add_rayls_hardforks_by_type(RaylsNetwork::Devnet)
            .batch_digest_v2(999)
            .build();
        assert!(!spec.is_batch_digest_v2_active_at_block(998));
        assert!(spec.is_batch_digest_v2_active_at_block(999));
    }
}

#[cfg(feature = "archive-replay")]
mod tokenomics_outage {
    use super::*;

    #[test]
    fn testnet_outage_window_matches_only_the_documented_range() {
        let testnet = RaylsChainHardforks::testnet();
        assert!(!testnet.is_tokenomics_outage_block(2_879_899));
        assert!(testnet.is_tokenomics_outage_block(2_879_900));
        assert!(testnet.is_tokenomics_outage_block(2_949_654));
        assert!(!testnet.is_tokenomics_outage_block(2_949_655));
        assert!(!testnet.is_tokenomics_outage_block(3_000_000));
    }

    #[test]
    fn outage_window_never_matches_non_testnet_schedules() {
        for network in [RaylsNetwork::Devnet, RaylsNetwork::Mainnet, RaylsNetwork::Local] {
            let spec = RaylsChainHardforks::for_network(network);
            assert!(
                !spec.is_tokenomics_outage_block(2_879_900),
                "{network} must not match the testnet outage window"
            );
        }
    }
}
