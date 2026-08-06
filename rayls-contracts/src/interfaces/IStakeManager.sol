// SPDX-License-Identifier: BUSL-1.1
pragma solidity 0.8.26;

import {BlsG1} from "../consensus/BlsG1.sol";

/**
 * @title IStakeManager
 * @notice Rayls Core Ltd., Telcoin Association
 *
 * @notice This interface declares the ConsensusRegistry's staking API and data structures
 * @dev Implemented within StakeManager.sol, which is inherited by the ConsensusRegistry
 */

/// @notice Protocol info for system calls to record block production performance
/// @notice Used to weight fee-based reward distribution by a hybrid of participation,
/// anchor (leader) commit share, and a stake tier - see `ConsensusRegistry.applyIncentives`
struct RewardInfo {
    address validatorAddress;
    /// @notice Distinct committed rounds this validator's certificate was included in
    uint256 participationRounds;
    /// @notice Committed rounds this validator led (formerly `consensusHeaderCount`)
    uint256 anchorRounds;
}

/// @notice Slash information for system calls to decrement outstanding validator balances
/// @notice Not enabled during MNO pilot
struct Slash {
    address validatorAddress;
    uint256 amount;
}

interface IStakeManager {
    /// @notice New StakeConfig versions take effect in the next epoch
    /// ie they are set for each epoch at its start
    struct StakeConfig {
        uint256 stakeAmount;
        uint256 minWithdrawAmount;
        uint32 epochDuration;
    }

    struct Delegation {
        bytes32 blsPubkeyHash;
        address validatorAddress;
        address delegator;
        uint8 validatorVersion;
        uint64 nonce;
    }

    error InvalidProofOfPossession(
        BlsG1.ProofOfPossession proof,
        bytes message
    );
    error InvalidTokenId(uint256 tokenId);
    error InvalidStakeAmount(uint256 stakeAmount, uint256 requiredAmount);
    error InsufficientRewards(uint256 withdrawAmount);
    error NotRecipient(address recipient);
    error NotTransferable();
    error InvalidValidatorSupply();
    error ValidatorNotFound(address validatorAddress);

    /// @dev Accepts the ERC-20 RLS stake amount from the calling validator, enabling later self-activation
    /// @notice Ensuring `uncompressedPubkey` corresponds to `ValidatorInfo::blsPubkey` is better
    /// performed externally in Rust by the protocol due to EIP2537 precompile & EVM limitations
    /// so this contract does not perform any (un)compression checks
    function stake(
        bytes calldata blsPubkey,
        BlsG1.ProofOfPossession calldata proofOfPossession
    ) external;

    /// @dev Accepts delegated stake from a non-validator caller authorized by a validator's EIP712 signature
    /// @notice `validatorAddress` must be a validator already`
    /// @notice Ensuring `uncompressedPubkey` corresponds to `ValidatorInfo::blsPubkey` is better
    /// performed externally in Rust by the protocol due to EIP2537 precompile & EVM limitations
    /// so this contract does not perform any (un)compression checks
    function delegateStake(
        bytes calldata blsPubkey,
        BlsG1.ProofOfPossession calldata proofOfPossession,
        address validatorAddress,
        bytes calldata validatorSig
    ) external;

    /// @dev Used by rewardees to claim staking rewards
    function claimStakeRewards(address ecdaPubkey) external;

    /// @dev Returns previously staked funds in addition to accrued rewards, if any, to the staker
    /// @notice May be used to reverse validator onboarding pre-activation or permanently retire after full exit
    /// @notice Once unstaked and retired, validator addresses cannot be reused
    function unstake(address validatorAddress) external;

    /// @notice Returns the delegation digest that a validator should sign to accept a delegation
    /// @return _ EIP-712 typed struct hash used to enable delegated proof of stake
    function delegationDigest(
        bytes memory blsPubkey,
        address validatorAddress,
        address delegator
    ) external view returns (bytes32);

    /// @dev Fetches the claimable rewards accrued for a given validator address
    /// @return _ The validator's claimable rewards, not including the validator's stake
    function getRewards(
        address validatorAddress
    ) external view returns (uint256);

    /// @dev Returns the ERC-20 RLS token address used for staking
    function rlsToken() external view returns (address);

    /// @dev Returns staking information for the given address
    function getBalanceBreakdown(address validatorAddress) external view returns (uint256, uint256, uint256);

    /// @dev Returns the current version
    function getCurrentStakeVersion() external view returns (uint8);

    /// @dev Returns the queried stake configuration
    function stakeConfig(
        uint8 version
    ) external view returns (StakeConfig memory);

    /// @dev Returns the current stake configuration
    function getCurrentStakeConfig() external view returns (StakeConfig memory);

    /// @dev Permissioned function to upgrade stake, withdrawal, and consensus block reward configurations
    /// @notice The new version takes effect in the next epoch
    function upgradeStakeVersion(
        StakeConfig calldata newVersion
    ) external returns (uint8);


    /// @dev Returns the DelegationPool contract address
    function delegationPool() external view returns (address);

    /// @dev Returns accumulated slashed funds held by the ConsensusRegistry
    function slashedFunds() external view returns (uint256);

    /// @dev Governance function to withdraw accumulated slashed funds
    event SlashedFundsWithdrawn(address indexed to, uint256 amount);
    function withdrawSlashedFunds(address to, uint256 amount) external;
}
