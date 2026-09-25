//! Tests for the Rayls chain spec module.

use std::sync::Arc;

use rayls_infrastructure_types::RaylsNetwork;
use reth_chainspec::ChainSpec;

use crate::chainspec::{RaylsChainHardforks, RaylsChainSpec, RaylsHardFork, RaylsHardforks};

/// The activation block of `fork` in `spec`; panics for non-block-scheduled forks.
fn activation_block(spec: &RaylsChainHardforks, fork: RaylsHardFork) -> u64 {
    spec.rayls_fork_activation(fork).block_number().expect("fork is block-scheduled")
}

mod schedule {
    use super::*;

    #[test]
    fn batch_digest_v2_inactive_before_activation_block() {
        let hardforks = RaylsChainHardforks::for_network(RaylsNetwork::Devnet);
        let block = activation_block(&hardforks, RaylsHardFork::BatchDigestV2);
        assert!(!hardforks.is_batch_digest_v2_active_at_block(block - 1));
    }

    #[test]
    fn batch_digest_v2_active_at_activation_block() {
        let hardforks = RaylsChainHardforks::for_network(RaylsNetwork::Devnet);
        let block = activation_block(&hardforks, RaylsHardFork::BatchDigestV2);
        assert!(hardforks.is_batch_digest_v2_active_at_block(block));
    }

    #[test]
    fn batch_digest_v2_active_after_activation_block() {
        let hardforks = RaylsChainHardforks::for_network(RaylsNetwork::Devnet);
        let block = activation_block(&hardforks, RaylsHardFork::BatchDigestV2);
        assert!(hardforks.is_batch_digest_v2_active_at_block(block + 1));
    }

    #[test]
    fn newly_activated_forks_includes_batch_digest_v2() {
        let hardforks = RaylsChainHardforks::for_network(RaylsNetwork::Devnet);
        let block = activation_block(&hardforks, RaylsHardFork::BatchDigestV2);
        let activated = hardforks.newly_activated_forks(block - 1, block);
        assert!(activated.contains(&RaylsHardFork::BatchDigestV2));
    }

    #[test]
    fn newly_activated_forks_excludes_batch_digest_v2_when_already_active() {
        let hardforks = RaylsChainHardforks::for_network(RaylsNetwork::Devnet);
        let block = activation_block(&hardforks, RaylsHardFork::BatchDigestV2);
        let activated = hardforks.newly_activated_forks(block, block + 1);
        assert!(!activated.contains(&RaylsHardFork::BatchDigestV2));
    }

    /// Test Never block
    #[test]
    fn admin_transfer_never_activates_on_devnet() {
        let hardforks = RaylsChainHardforks::for_network(RaylsNetwork::Devnet);
        for block in [0u64, 1, 1_000, 1_000_000] {
            assert!(!hardforks.is_rayls_fork_active_at_block(RaylsHardFork::AdminTransfer, block,));
        }
    }

    #[test]
    fn admin_transfer_active_after_testnet_activation_block() {
        let hardforks = RaylsChainHardforks::for_network(RaylsNetwork::Testnet);
        let block = activation_block(&hardforks, RaylsHardFork::AdminTransfer);
        assert!(!hardforks.is_rayls_fork_active_at_block(RaylsHardFork::AdminTransfer, block - 1,));
        assert!(hardforks.is_rayls_fork_active_at_block(RaylsHardFork::AdminTransfer, block + 1,));
    }

    #[test]
    fn newly_activated_forks_on_testnet_includes_admin_transfer() {
        let hardforks = RaylsChainHardforks::for_network(RaylsNetwork::Testnet);
        let block = activation_block(&hardforks, RaylsHardFork::AdminTransfer);
        let activated = hardforks.newly_activated_forks(block - 1, block);
        assert!(activated.contains(&RaylsHardFork::AdminTransfer));
    }

    #[test]
    fn local_network_first_four_hardforks_active_at_block_0() {
        let hardforks = RaylsChainHardforks::local();
        // First four forks in declaration order are genesis-active on local (9 of 15 are;
        // this pins the leading four).
        let active_forks = [
            RaylsHardFork::Eip1559,
            RaylsHardFork::BatchDigestV2,
            RaylsHardFork::AdminTransfer,
            RaylsHardFork::PrecompileGasFix,
        ];
        for fork in active_forks {
            assert!(
                hardforks.is_rayls_fork_active_at_block(fork, 0),
                "fork {:?} should be active at block 0",
                fork
            );
        }
    }

    #[test]
    fn local_network_migration_forks_never_activate() {
        let hardforks = RaylsChainHardforks::local();
        // Three migration-only forks that are Never on local (local genesis already carries
        // the migrated state); declaration indices 4-6, not the last three.
        let never_forks =
            [RaylsHardFork::RlsStorage, RaylsHardFork::Tokenomics, RaylsHardFork::Uups];
        for fork in never_forks {
            assert!(
                !hardforks.is_rayls_fork_active_at_block(fork, 0),
                "fork {:?} should never be active",
                fork
            );
            assert!(
                !hardforks.is_rayls_fork_active_at_block(fork, 1_000_000),
                "fork {:?} should never be active",
                fork
            );
        }
    }

    #[test]
    fn schedule_matches_declaration_order_for_all_networks() {
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
            assert_eq!(schedule[0].fork, RaylsHardFork::Eip1559);
            assert_eq!(schedule[1].fork, RaylsHardFork::BatchDigestV2);
            assert_eq!(schedule[2].fork, RaylsHardFork::AdminTransfer);
            assert_eq!(schedule[3].fork, RaylsHardFork::PrecompileGasFix);
            assert_eq!(schedule[4].fork, RaylsHardFork::RlsStorage);
            assert_eq!(schedule[5].fork, RaylsHardFork::Tokenomics);
            assert_eq!(schedule[6].fork, RaylsHardFork::Uups);
            assert_eq!(schedule[7].fork, RaylsHardFork::Erc20PrecompileBytecode);
            assert_eq!(schedule[8].fork, RaylsHardFork::TransactionLoadBalancing);
            assert_eq!(schedule[9].fork, RaylsHardFork::UsdrSupplyCorrection);
            assert_eq!(schedule[10].fork, RaylsHardFork::EmptyOutputBlock);
            assert_eq!(schedule[11].fork, RaylsHardFork::DynamicCommitteeSizing);
            assert_eq!(schedule[12].fork, RaylsHardFork::HybridRewards);
            assert_eq!(schedule[13].fork, RaylsHardFork::OutputSeqNormalization);
            assert_eq!(schedule[14].fork, RaylsHardFork::SenderAffinityLoadBalancing);
        }
    }

    #[test]
    fn erc20_precompile_bytecode_is_never_on_testnet() {
        let hardforks = RaylsChainHardforks::for_network(RaylsNetwork::Testnet);
        assert!(
            !hardforks.is_erc20_precompile_bytecode_active_at_block(u64::MAX),
            "testnet bytecode is already present; migration must stay Never",
        );
    }
}

mod hardforks {
    use super::*;

    #[test]
    fn local_network_version_byte_at_block_0() {
        let hardforks = RaylsChainHardforks::local();
        let version = hardforks.version_byte_at_block(0);
        // SenderAffinityLoadBalancing (0x0f) activates at block 0 on local and is the highest
        // such fork, so it owns the version byte from block 0.
        assert_eq!(version, Some(0x0f));
    }

    #[test]
    fn local_network_version_byte_is_the_max_active_code() {
        // SenderAffinityLoadBalancing (0x0f) is genesis-active on local, so it owns the version
        // byte across every later activation (HybridRewards at 0x0d included): the byte reports
        // the max active code, not the most recently crossed block. On real networks it is Never,
        // so there the highest active fork still advances the byte normally.
        let hardforks = RaylsChainHardforks::local();
        let hybrid_rewards_block = activation_block(&hardforks, RaylsHardFork::HybridRewards);
        assert_eq!(hardforks.version_byte_at_block(hybrid_rewards_block - 1), Some(0x0f));
        assert_eq!(hardforks.version_byte_at_block(hybrid_rewards_block), Some(0x0f));
        assert_eq!(hardforks.version_byte_at_block(1_000_000), Some(0x0f));
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
