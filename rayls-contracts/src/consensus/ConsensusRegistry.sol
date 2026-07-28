// SPDX-License-Identifier: BUSL-1.1
pragma solidity 0.8.26;

import {Pausable} from "@openzeppelin/contracts/utils/Pausable.sol";
import {Ownable} from "@openzeppelin/contracts/access/Ownable.sol";
import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {SafeERC20} from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import {SignatureCheckerLib} from "solady/utils/SignatureCheckerLib.sol";
import {ReentrancyGuard} from "solady/utils/ReentrancyGuard.sol";
import {RewardInfo, Slash, IStakeManager} from "../interfaces/IStakeManager.sol";
import {StakeManager} from "./StakeManager.sol";
import {IConsensusRegistry} from "../interfaces/IConsensusRegistry.sol";
import {SystemCallable} from "./SystemCallable.sol";
import {BlsG1} from "./BlsG1.sol";
import {IDelegationPool} from "../interfaces/IDelegationPool.sol";

/**
 * @title ConsensusRegistry
 * @notice Rayls Core Ltd., Telcoin Association
 *
 * @notice This contract manages consensus validator external keys, staking, and committees
 * @dev This contract should be deployed to a predefined system address for use with system calls
 */
contract ConsensusRegistry is
    StakeManager,
    Pausable,
    Ownable,
    ReentrancyGuard,
    SystemCallable,
    IConsensusRegistry
{
    using BlsG1 for bytes;
    using SafeERC20 for IERC20;

    /// @dev Basis-points denominator (1 bps = 0.01%), mirrors the `MAX_BPS` idiom
    /// used elsewhere in this contract family (e.g. `FeeAggregator.MAX_BPS`)
    uint256 private constant MAX_BPS = 10_000;

    /// @dev Hybrid weight split between participation, anchor (leader), and stake -
    /// must sum to `MAX_BPS`. Fixed for this deployment; retuning requires a new
    /// hardfork-gated bytecode swap, same as any other change to this contract.
    uint256 private constant PARTICIPATION_BPS = 8_000;
    uint256 private constant ANCHOR_BPS = 1_000;
    uint256 private constant STAKE_BPS = 1_000;

    /// @dev Stake-tier bonus: own required stake (`getBalanceBreakdown` position 1,
    /// not position 0 which is contaminated by accrued rewards) plus delegated
    /// stake, tiered every `STAKE_TIER_STEP` above `STAKE_TIER_MIN`, capped at
    /// `STAKE_TIER_MAX_TIERS` tiers. With the current 5M-RLS minimum this is a 5M
    /// baseline (everyone) plus up to 25M delegated (5 tiers), capping combined
    /// stake consideration at 30M.
    uint256 private constant STAKE_TIER_MIN = 5_000_000e18;
    uint256 private constant STAKE_TIER_STEP = 5_000_000e18;
    uint256 private constant STAKE_TIER_MAX_TIERS = 5;
    uint256 private constant STAKE_TIER_BONUS_BPS = 200;

    uint32 internal currentEpoch;
    uint8 internal epochPointer;
    EpochInfo[4] public epochInfo;
    EpochInfo[4] public futureEpochInfo;
    mapping(address => ValidatorInfo) public validators;
    mapping(bytes32 => bool) private usedBLSPubkeys;

    /// @notice Accumulated slashed funds received from DelegationPool, withdrawable by governance
    uint256 public slashedFunds;

    /// @notice Mapping of addresses allowed to become validators
    mapping(address => bool) public validatorAllowlist;

    /// @dev Performance weights recorded by applyIncentives for fee-based reward distribution
    PerformanceWeights internal _performanceWeights;

    /// @notice Minimum participation share (in bps of `totalRounds`) a validator must
    /// meet to be eligible for any reward this epoch; `0` disables the floor.
    /// @dev Governance-settable without a new hardfork, unlike the reward-split
    /// constants above - starts disabled pending real post-activation participation
    /// data to tune it. MUST stay at the end of storage (appended after the prior
    /// last var `_performanceWeights`): the `HybridRewards` hardfork installs this
    /// contract's bytecode over an EXISTING `ConsensusRegistry`'s storage via a
    /// bytecode swap, so every pre-existing slot must keep its offset and new state
    /// may only be appended, never inserted.
    uint256 public participationFloorBps;

    /// @dev Signals a validator's pending status until activation/exit to correctly apply incentives
    uint32 internal constant PENDING_EPOCH = type(uint32).max;

    /// @notice Fixed prefixes inserted by rust protocol; see `proofOfPossessionMessage()`
    /// @dev The proof of possession message prefix, used by the protocol to differentiate BLS proof intents
    bytes5 constant POP_INTENT_PREFIX = 0x000000d501;
    /// @dev The proof of possession's validator address length prefix, signifying 20 byte length encoding
    bytes1 constant ADDRESS_LEN_PREFIX = 0x14;

    /**
     *
     *   consensus
     *
     */

    /// @inheritdoc IConsensusRegistry
    function concludeEpoch(
        address[] calldata futureCommittee
    ) external override onlySystemCall {
        // ensure future committee is sorted
        _enforceSorting(futureCommittee);

        // update epoch ring buffer info, validator queue
        (
            uint32 newEpoch,
            uint32 duration,
            address[] memory newCommittee
        ) = _updateEpochInfo(futureCommittee);
        _updateValidatorQueue(futureCommittee, newEpoch);

        // assert future epoch committee is valid against total now eligible
        ValidatorInfo[] memory newActive = _getValidators(
            ValidatorStatus.Active
        );
        _checkCommitteeSize(newActive.length, futureCommittee.length);

        emit NewEpoch(
            EpochInfo(
                newCommittee,
                uint64(block.number + 1),
                duration,
                stakeVersion
            )
        );
    }

    /// @inheritdoc IConsensusRegistry
    /// @dev Blends three normalized per-validator shares into one weight:
    /// `PARTICIPATION_BPS` of the pool by participation-round share (of the
    /// *submitted* total, not `totalRounds` - see note below), `ANCHOR_BPS` by
    /// anchor/leader-round share of `totalRounds` (a true committee-wide share,
    /// since exactly one leader commits per round), and `STAKE_BPS` by stake-tier
    /// share. Mirrors `RewardDistributor.sol`'s own "sum totals, then proportional
    /// split" idiom one level deeper.
    function applyIncentives(
        RewardInfo[] calldata rewardInfos,
        uint256 totalRounds
    ) public override onlySystemCall {
        // clear previous epoch's performance weights
        delete _performanceWeights;

        uint256 n = rewardInfos.length;
        // `stakeWeight[i] == 0` doubles as "ineligible" - `_stakeTierWeightOf`
        // never returns 0 for an eligible validator, since it floors at `MAX_BPS`.
        uint256[] memory stakeWeight = new uint256[](n);
        uint256 totalParticipationRounds;
        uint256 totalStakeWeight;
        uint256 eligibleCount;

        // Pass 1: eligibility (absent / retired / below the participation floor) + totals.
        for (uint256 i; i < n; ++i) {
            RewardInfo calldata reward = rewardInfos[i];
            if (isRetired(reward.validatorAddress)) continue;
            // Absent -> 0: a validator that contributed no certificate to any
            // committed sub-dag this epoch earns nothing from any component,
            // independent of the floor. `order_dag` always includes a leader's own
            // certificate in the sub-dag it commits, so `participationRounds >=
            // anchorRounds`; `participationRounds == 0` therefore also implies
            // `anchorRounds == 0` - the validator neither participated nor led.
            if (reward.participationRounds == 0) continue;
            if (
                participationFloorBps > 0 &&
                totalRounds > 0 &&
                (reward.participationRounds * MAX_BPS) / totalRounds < participationFloorBps
            ) continue;

            stakeWeight[i] = _stakeTierWeightOf(reward.validatorAddress);
            totalParticipationRounds += reward.participationRounds;
            totalStakeWeight += stakeWeight[i];
            eligibleCount++;
        }

        if (eligibleCount == 0) return;

        // Pass 2: blend each eligible validator's normalized shares into one weight.
        // Note: participation normalizes against `totalParticipationRounds` (the
        // *submitted* sum), not `totalRounds` - unlike anchor rounds, participation
        // rounds don't sum to `totalRounds` across the committee (every live
        // validator can participate in every round simultaneously), so submitted-sum
        // normalization is what turns it into a true "slice of the 80% pool".
        address[] memory tmpValidators = new address[](eligibleCount);
        uint256[] memory tmpWeights = new uint256[](eligibleCount);
        uint256 totalWeight;
        uint256 validatorCount;

        for (uint256 i; i < n; ++i) {
            if (stakeWeight[i] == 0) continue;
            RewardInfo calldata reward = rewardInfos[i];

            uint256 weight;
            if (totalParticipationRounds > 0) {
                weight += (PARTICIPATION_BPS * reward.participationRounds) / totalParticipationRounds;
            }
            if (totalRounds > 0) {
                weight += (ANCHOR_BPS * reward.anchorRounds) / totalRounds;
            }
            weight += (STAKE_BPS * stakeWeight[i]) / totalStakeWeight;
            if (weight == 0) continue;

            tmpValidators[validatorCount] = reward.validatorAddress;
            tmpWeights[validatorCount] = weight;
            totalWeight += weight;
            validatorCount++;
        }

        if (totalWeight == 0) return;

        // store compacted performance weights for RewardDistributor to consume
        address[] memory finalValidators = new address[](validatorCount);
        uint256[] memory finalWeights = new uint256[](validatorCount);
        for (uint256 i; i < validatorCount; ++i) {
            finalValidators[i] = tmpValidators[i];
            finalWeights[i] = tmpWeights[i];
        }

        _performanceWeights = PerformanceWeights({
            validators: finalValidators,
            weights: finalWeights,
            totalWeight: totalWeight
        });
    }

    /// @dev Stake-tier weight multiplier (in bps, `MAX_BPS` baseline) for
    /// `validatorAddress`: own required stake (`getBalanceBreakdown` position 1 -
    /// see `RewardDistributor.sol`'s identical destructuring - not position 0,
    /// which is contaminated by accrued rewards) plus delegated stake, tiered
    /// above `STAKE_TIER_MIN` and capped at `STAKE_TIER_MAX_TIERS` tiers.
    function _stakeTierWeightOf(address validatorAddress) internal view returns (uint256) {
        (, uint256 requiredStake, ) = getBalanceBreakdown(validatorAddress);
        address pool = delegationPool;
        uint256 delegated = pool != address(0)
            ? IDelegationPool(pool).getTotalDelegatedStake(validatorAddress)
            : 0;
        uint256 totalStake = requiredStake + delegated;

        if (totalStake <= STAKE_TIER_MIN) {
            return MAX_BPS;
        }
        uint256 maxStake = STAKE_TIER_MIN + STAKE_TIER_STEP * STAKE_TIER_MAX_TIERS;
        if (totalStake > maxStake) {
            totalStake = maxStake;
        }
        uint256 tiers = (totalStake - STAKE_TIER_MIN) / STAKE_TIER_STEP;
        return MAX_BPS + tiers * STAKE_TIER_BONUS_BPS;
    }

    /// @notice Governance sets the minimum participation share (bps) required for
    /// eligibility; `0` disables the floor.
    /// @param newFloorBps New floor, in bps of `totalRounds`. Must be `<= MAX_BPS`.
    function setParticipationFloorBps(uint256 newFloorBps) external onlyOwner {
        if (newFloorBps > MAX_BPS) revert InvalidParticipationFloor(newFloorBps);
        uint256 oldFloorBps = participationFloorBps;
        participationFloorBps = newFloorBps;
        emit ParticipationFloorUpdated(oldFloorBps, newFloorBps);
    }

    /// @inheritdoc IConsensusRegistry
    function getEpochPerformanceWeights() external view override returns (PerformanceWeights memory) {
        return _performanceWeights;
    }

    /// @inheritdoc IConsensusRegistry
    function applySlashes(
        Slash[] calldata slashes
    ) external override onlySystemCall {
        address pool = delegationPool;

        for (uint256 i; i < slashes.length; ++i) {
            Slash calldata slash = slashes[i];
            // signed consensus header means validator is staked, & active
            // unless validator was forcibly retired & ejected via burn: skip
            if (isRetired(slash.validatorAddress)) continue;

            uint256 validatorSlash = slash.amount;
            uint256 poolSlash = 0;

            // split slash proportionally between validator and pool
            if (pool != address(0)) {
                uint256 poolTotal = IDelegationPool(pool)
                    .getTotalDelegatedStake(slash.validatorAddress);
                if (poolTotal > 0) {
                    uint256 validatorBalance = balances[slash.validatorAddress];
                    uint256 totalStake = validatorBalance + poolTotal;

                    // Ceiling division: validator pays at least their fair share
                    validatorSlash = (slash.amount * validatorBalance + totalStake - 1) /
                        totalStake;
                    if (validatorSlash > slash.amount) validatorSlash = slash.amount;
                    poolSlash = slash.amount - validatorSlash;
                }
            }

            // apply validator's portion
            if (balances[slash.validatorAddress] > validatorSlash) {
                balances[slash.validatorAddress] -= validatorSlash;
            } else {
                // apply pool slash before burning
                if (poolSlash > 0) {
                    uint256 slashed = IDelegationPool(pool).applyPoolSlash(
                        slash.validatorAddress,
                        poolSlash
                    );
                    slashedFunds += slashed;
                }
                _consensusBurn(slash.validatorAddress);
                emit ValidatorSlashed(slash);
                continue;
            }

            // apply pool's portion
            if (poolSlash > 0) {
                uint256 slashed = IDelegationPool(pool).applyPoolSlash(
                    slash.validatorAddress,
                    poolSlash
                );
                slashedFunds += slashed;
            }

            emit ValidatorSlashed(slash);
        }
    }

    /// @inheritdoc IStakeManager
    function getCurrentStakeVersion() public view override returns (uint8) {
        return getCurrentEpochInfo().stakeVersion;
    }

    /// @inheritdoc IConsensusRegistry
    function getCurrentEpoch() public view returns (uint32) {
        return currentEpoch;
    }

    /// @inheritdoc IConsensusRegistry
    function getCurrentEpochInfo() public view returns (EpochInfo memory) {
        return _getRecentEpochInfo(currentEpoch, currentEpoch, epochPointer);
    }

    /// @inheritdoc IConsensusRegistry
    function getEpochInfo(uint32 epoch) public view returns (EpochInfo memory) {
        uint32 current = currentEpoch;
        if (epoch > current + 2 || (current >= 3 && epoch < current - 3)) {
            revert InvalidEpoch(epoch);
        }

        uint8 currentPointer = epochPointer;
        if (epoch > current) {
            return _getFutureEpochInfo(epoch, current, currentPointer);
        } else {
            return _getRecentEpochInfo(epoch, current, currentPointer);
        }
    }

    /// @inheritdoc IConsensusRegistry
    function getValidators(
        ValidatorStatus status
    ) public view returns (ValidatorInfo[] memory) {
        if (status == ValidatorStatus.Undefined) revert InvalidStatus(status);

        return _getValidators(status);
    }

    /// @inheritdoc IConsensusRegistry
    function getCommitteeValidators(
        uint32 epoch
    ) public view returns (ValidatorInfo[] memory) {
        address[] memory committee = getEpochInfo(epoch).committee;
        ValidatorInfo[] memory committeeValidators = new ValidatorInfo[](
            committee.length
        );
        for (uint256 i; i < committeeValidators.length; ++i) {
            committeeValidators[i] = getValidator(committee[i]);
        }

        return committeeValidators;
    }

    /// @inheritdoc IConsensusRegistry
    function getValidator(
        address validatorAddress
    ) public view returns (ValidatorInfo memory) {
        return validators[validatorAddress];
    }

    /// @inheritdoc IConsensusRegistry
    function isRetired(address validatorAddress) public view returns (bool) {
        if (
            validators[validatorAddress].currentStatus ==
            ValidatorStatus.Undefined
        ) {
            // validator doesn't exist but never existed in the first place
            return false;
        }

        return validators[validatorAddress].isRetired;
    }

    /// @inheritdoc StakeManager
    function getRewards(
        address validatorAddress
    ) public view override returns (uint256) {
        uint8 stakeVersion = validators[validatorAddress].stakeVersion;
        uint256 initialStake = versions[stakeVersion].stakeAmount;

        return _getRewards(validatorAddress, initialStake);
    }

    /// @inheritdoc StakeManager
    function getBalanceBreakdown(address validatorAddress) public view override returns (uint256, uint256, uint256) {
        uint8 validatorVersion = validators[validatorAddress].stakeVersion;
        uint256 initialStakeAmount = versions[validatorVersion].stakeAmount;
        uint256 rewards = _getRewards(validatorAddress, initialStakeAmount);
        uint256 outstandingBalance = balances[validatorAddress];

        return (outstandingBalance, initialStakeAmount, rewards);
    }

    /// @inheritdoc IStakeManager
    function delegationDigest(
        bytes memory blsPubkey,
        address validatorAddress,
        address delegator
    ) external view override returns (bytes32) {
        uint8 stakeVersion = getCurrentEpochInfo().stakeVersion;
        uint64 nonce = delegations[validatorAddress].nonce;
        bytes32 blsPubkeyHash = keccak256(blsPubkey);
        bytes32 structHash = keccak256(
            abi.encode(
                DELEGATION_TYPEHASH,
                blsPubkeyHash,
                validatorAddress,
                delegator,
                stakeVersion,
                nonce
            )
        );

        return _hashTypedData(structHash);
    }

    /// @inheritdoc IConsensusRegistry
    function proofOfPossessionMessage(
        bytes memory blsPubkeyUncompressed,
        address validatorAddress
    ) public view returns (bytes memory) {
        bytes memory blsPubkeyEIP2537 = BlsG1.encodeG2PointForEIP2537(
            blsPubkeyUncompressed
        );
        if (!BlsG1.validatePointG2(blsPubkeyEIP2537))
            revert BlsG1.InvalidBLSPubkey();

        return
            bytes.concat(
                POP_INTENT_PREFIX,
                blsPubkeyUncompressed,
                ADDRESS_LEN_PREFIX,
                bytes20(validatorAddress)
            );
    }

    /**
     *
     *   validators
     *
     */

    /// @inheritdoc StakeManager
    function stake(
        bytes calldata blsPubkey,
        BlsG1.ProofOfPossession memory proofOfPossession
    ) external override whenNotPaused {
        if (blsPubkey.length != 96) revert BlsG1.InvalidBLSPubkey();

        // require validator is allowlisted (cheap SLOAD check before expensive PoP)
        if (!validatorAllowlist[msg.sender]) revert NotAllowlisted(msg.sender);

        // verify the BLS signature proves ownership of the BLS secret key
        bytes memory message = proofOfPossessionMessage(
            proofOfPossession.uncompressedPubkey,
            msg.sender
        );
        bytes memory blsPubkeyEIP2537 = BlsG1.encodeG2PointForEIP2537(
            proofOfPossession.uncompressedPubkey
        );
        bytes memory popEIP2537 = BlsG1.encodeG1PointForEIP2537(
            proofOfPossession.uncompressedSignature
        );
        if (
            !BlsG1.verifyProofOfPossessionG1(
                blsPubkeyEIP2537,
                popEIP2537,
                message
            )
        ) {
            revert InvalidProofOfPossession(
                BlsG1.ProofOfPossession(
                    proofOfPossession.uncompressedPubkey,
                    proofOfPossession.uncompressedSignature
                ),
                message
            );
        }

        uint8 validatorVersion = getCurrentEpochInfo().stakeVersion;
        uint256 stakeAmt = _checkStakeValue(versions[validatorVersion].stakeAmount, validatorVersion);
        // require validator has not yet staked
        _checkValidatorStatus(msg.sender, ValidatorStatus.Undefined);

        // transfer ERC-20 RLS from caller
        _rls.safeTransferFrom(msg.sender, address(this), stakeAmt);

        // enter validator in activation queue
        _recordStaked(blsPubkey, msg.sender, false, validatorVersion, stakeAmt);
    }

    /// @inheritdoc StakeManager
    function delegateStake(
        bytes calldata blsPubkey,
        BlsG1.ProofOfPossession memory proofOfPossession,
        address validatorAddress,
        bytes calldata validatorEIP712Signature
    ) external override whenNotPaused {
        if (blsPubkey.length != 96) revert BlsG1.InvalidBLSPubkey();
        // verify the delegate has obtained validator's BLS signature proving ownership of the BLS secret key
        bytes memory message = proofOfPossessionMessage(
            proofOfPossession.uncompressedPubkey,
            validatorAddress
        );
        bytes memory blsPubkeyEIP2537 = BlsG1.encodeG2PointForEIP2537(
            proofOfPossession.uncompressedPubkey
        );
        bytes memory popEIP2537 = BlsG1.encodeG1PointForEIP2537(
            proofOfPossession.uncompressedSignature
        );
        if (
            !BlsG1.verifyProofOfPossessionG1(
                blsPubkeyEIP2537,
                popEIP2537,
                message
            )
        ) {
            revert InvalidProofOfPossession(
                BlsG1.ProofOfPossession(
                    proofOfPossession.uncompressedPubkey,
                    proofOfPossession.uncompressedSignature
                ),
                message
            );
        }

        uint8 validatorVersion = getCurrentEpochInfo().stakeVersion;
        uint256 stakeAmt = _checkStakeValue(versions[validatorVersion].stakeAmount, validatorVersion);

        // require validator status is `Undefined`
        _checkValidatorStatus(validatorAddress, ValidatorStatus.Undefined);
        uint64 nonce = delegations[validatorAddress].nonce;
        bytes32 blsPubkeyHash = keccak256(blsPubkey);

        // always require validator is allowlisted (governance controls onboarding)
        if (!validatorAllowlist[validatorAddress]) revert NotAllowlisted(validatorAddress);

        // owner (governance) may skip EIP-712 signature verification
        if (msg.sender != owner()) {
            bytes32 structHash = keccak256(
                abi.encode(
                    DELEGATION_TYPEHASH,
                    blsPubkeyHash,
                    validatorAddress,
                    msg.sender,
                    validatorVersion,
                    nonce
                )
            );
            bytes32 digest = _hashTypedData(structHash);
            if (
                !SignatureCheckerLib.isValidSignatureNowCalldata(
                    validatorAddress,
                    digest,
                    validatorEIP712Signature
                )
            ) {
                revert NotValidator(validatorAddress);
            }
        }

        // transfer ERC-20 RLS from caller
        _rls.safeTransferFrom(msg.sender, address(this), stakeAmt);

        delegations[validatorAddress] = Delegation(
            blsPubkeyHash,
            validatorAddress,
            msg.sender,
            validatorVersion,
            nonce + 1
        );
        _recordStaked(
            blsPubkey,
            validatorAddress,
            true,
            validatorVersion,
            stakeAmt
        );
    }

    /// @inheritdoc IConsensusRegistry
    function activate() external override whenNotPaused {
        // require caller status is `Staked`
        _checkValidatorStatus(msg.sender, ValidatorStatus.Staked);

        ValidatorInfo storage validator = validators[msg.sender];
        // begin validator activation, completing automatically next epoch
        _beginActivation(validator, currentEpoch);
    }

    /// @inheritdoc StakeManager
    function claimStakeRewards(
        address validatorAddress
    ) external override whenNotPaused nonReentrant {
        if (validatorIndex[validatorAddress] == 0) revert ValidatorNotFound(validatorAddress);

        uint8 validatorVersion = validators[validatorAddress].stakeVersion;

        // require caller is either the validator or its delegator
        address recipient = _getRecipient(validatorAddress);
        if (msg.sender != validatorAddress && msg.sender != recipient)
            revert NotRecipient(recipient);
        uint256 rewards = _claimStakeRewards(
            validatorAddress,
            recipient,
            validatorVersion
        );

        emit RewardsClaimed(recipient, rewards);
    }

    /// @inheritdoc IConsensusRegistry
    function beginExit() external override whenNotPaused {

        if (validatorIndex[msg.sender] == 0) revert ValidatorNotFound(msg.sender);

        // disallow filling up the exit queue — at least one strictly-Active validator must
        // remain after this exit. _getValidators(Active) includes PendingExit, so we subtract
        // PendingExit count to get the true number of validators not yet in the exit queue.
        uint256 numEligible = _getValidators(ValidatorStatus.Active).length;
        uint256 numPendingExit = _getValidators(ValidatorStatus.PendingExit).length;
        uint256 numStrictlyActive = numEligible - numPendingExit;
        uint256 committeeSize = epochInfo[epochPointer].committee.length;
        // after this caller exits, numStrictlyActive - 1 must still be > 0
        if (numStrictlyActive <= 1) revert InvalidCommitteeSize(numStrictlyActive, committeeSize);
        _checkCommitteeSize(numEligible, committeeSize);

        // require caller status is `Active` and `currentEpoch >= activationEpoch`
        _checkValidatorStatus(msg.sender, ValidatorStatus.Active);
        ValidatorInfo storage validator = validators[msg.sender];
        uint32 current = currentEpoch;
        if (current < validators[msg.sender].activationEpoch) {
            revert InvalidEpoch(current);
        }

        // enter validator in pending exit queue
        _beginExit(validator);
    }

    /// @inheritdoc StakeManager
    function unstake(
        address validatorAddress
    ) external override whenNotPaused nonReentrant {
        // require caller is either the validator or its delegator
        address recipient = _getRecipient(validatorAddress);
        if (msg.sender != validatorAddress && msg.sender != recipient)
            revert NotRecipient(recipient);

        ValidatorInfo storage validator = validators[validatorAddress];        // stake originator can only reclaim stake pre-activation or after exiting
        if (!_eligibleForUnstake(validator)) revert IneligibleUnstake(validator);

        // retire the validator
        _retire(validator);

        // return stake and send any outstanding rewards
        uint256 stakeAndRewards = _unstake(validatorAddress, recipient);

        emit RewardsClaimed(recipient, stakeAndRewards);
    }
    /// @notice Governance sets the DelegationPool contract address
    /// @param pool The DelegationPool contract address
    function setDelegationPool(address pool) external onlyOwner {
        address oldPool = delegationPool;
        _setDelegationPool(pool);
        emit DelegationPoolUpdated(oldPool, pool);
    }

    /// @notice Governance function to withdraw accumulated slashed funds (ERC-20 RLS)
    /// @param to The address to send the slashed funds to
    /// @param amount The amount of slashed funds to withdraw
    function withdrawSlashedFunds(address to, uint256 amount) external onlyOwner {
        require(amount > 0, "Zero amount");
        require(amount <= slashedFunds, "Insufficient slashed funds");
        slashedFunds -= amount;
        _rls.safeTransfer(to, amount);
        emit SlashedFundsWithdrawn(to, amount);
    }

    /// @inheritdoc IConsensusRegistry
    function allowlistValidator(address validatorAddress) external override onlyOwner {
        if (validatorAddress == address(0)) revert InvalidValidatorAddress();
        if (validatorAllowlist[validatorAddress]) return;

        validatorAllowlist[validatorAddress] = true;
        emit ValidatorAllowlisted(validatorAddress);
    }

    /// @inheritdoc IConsensusRegistry
    function delistValidator(address validatorAddress) external override onlyOwner {
        if (!validatorAllowlist[validatorAddress]) return;

        validatorAllowlist[validatorAddress] = false;
        emit ValidatorDelisted(validatorAddress);
    }

    /// @inheritdoc IConsensusRegistry
    function updateAllowlistBatch(
        address[] calldata validatorAddresses,
        bool[] calldata allowed
    ) external override onlyOwner {
        if (validatorAddresses.length != allowed.length)
            revert AllowlistBatchLengthMismatch();

        for (uint256 i; i < validatorAddresses.length; ++i) {
            if (validatorAddresses[i] == address(0))
                revert InvalidValidatorAddress();
            validatorAllowlist[validatorAddresses[i]] = allowed[i];
        }
    }

    /// @inheritdoc IConsensusRegistry
    function isAllowlisted(address validatorAddress) external view override returns (bool) {
        return validatorAllowlist[validatorAddress];
    }

    /**
     *
     *   internals
     *
     */

    /// @notice Enters a validator into the activation queue upon receiving stake
    /// @dev Stores the new validator in the `validators` vector
    function _recordStaked(
        bytes calldata blsPubkey,
        address validatorAddress,
        bool isDelegated,
        uint8 stakeVersion,
        uint256 stakeAmt
    ) internal {
        bytes32 blsPubkeyHash = keccak256(blsPubkey);
        if (usedBLSPubkeys[blsPubkeyHash]) revert DuplicateBLSPubkey();
        usedBLSPubkeys[blsPubkeyHash] = true;

        ValidatorInfo memory newValidator = ValidatorInfo(
            blsPubkey,
            validatorAddress,
            PENDING_EPOCH,
            uint32(0),
            ValidatorStatus.Staked,
            false,
            isDelegated,
            stakeVersion
        );
        validators[validatorAddress] = newValidator;

        _stake(validatorAddress, stakeAmt);

        emit ValidatorStaked(newValidator);
    }

    /// @dev Sets the next epoch as activation timestamp for epoch completeness wrt incentives
    function _beginActivation(
        ValidatorInfo storage validator,
        uint32 epoch
    ) internal {
        validator.activationEpoch = epoch + 1;
        validator.currentStatus = ValidatorStatus.PendingActivation;

        emit ValidatorPendingActivation(validator);
    }

    /// @dev Activates a validator
    /// @dev Performed by protocol system call at commencement of validator's first full epoch
    function _activate(ValidatorInfo storage validator) internal {
        validator.currentStatus = ValidatorStatus.Active;

        emit ValidatorActivated(validator);
    }

    /// @notice Enters a validator into the exit queue
    /// @dev Finalized by the protocol when the validator is no longer required for committees
    function _beginExit(ValidatorInfo storage validator) internal {
        validator.currentStatus = ValidatorStatus.PendingExit;
        validator.exitEpoch = PENDING_EPOCH;

        emit ValidatorPendingExit(validator);
    }

    /// @notice Exits a validator from the network,
    /// @dev Only invoked via protocol client system call to `concludeEpoch()` or governance ejection
    /// @dev Once exited, the validator may unstake to reclaim their stake and rewards
    function _exit(ValidatorInfo storage validator, uint32 epoch) internal {
        validator.currentStatus = ValidatorStatus.Exited;
        validator.exitEpoch = epoch;

        emit ValidatorExited(validator);
    }

    /// @notice Permanently retires validator from the network
    /// @dev Ensures an validator cannot rejoin after exiting + unstaking or after governance ejection
    /// @dev Rejoining must be done by restarting validator onboarding process with new keys
    function _retire(ValidatorInfo storage validator) internal {
        validator.currentStatus = ValidatorStatus.Any;
        validator.isRetired = true;

        emit ValidatorRetired(validator);
    }

    /// @notice Performs activation and/or exit for validators pending in queue where applicable
    /// @dev Validators initiate activation, gaining `PendingActivation` status which resolves to
    /// `Active` at the end of the current epoch. Since they could time activation initiation
    /// with the epoch boundary, they are ineligible for rewards until completing a full epoch
    /// @dev Protocol determines exit eligibility via voter committee assignments across 3 epochs
    function _updateValidatorQueue(
        address[] calldata futureCommittee,
        uint32 current
    ) internal {
        ValidatorInfo[] memory pendingActivation = _getValidators(
            ValidatorStatus.PendingActivation
        );
        for (uint256 i; i < pendingActivation.length; ++i) {
            ValidatorInfo storage activateValidator = validators[
                pendingActivation[i].validatorAddress
            ];

            _activate(activateValidator);
        }

        ValidatorInfo[] memory pendingExit = _getValidators(
            ValidatorStatus.PendingExit
        );
        uint8 currentEpochPointer = epochPointer;
        uint8 nextEpochPointer = (currentEpochPointer + 1) % 4;
        address[] memory currentCommittee = epochInfo[currentEpochPointer]
            .committee;
        address[] memory nextCommittee = futureEpochInfo[nextEpochPointer]
            .committee;
        for (uint256 i; i < pendingExit.length; ++i) {
            // skip if validator is in current or either future committee
            address validatorAddress = pendingExit[i].validatorAddress;
            if (
                _isCommitteeMember(validatorAddress, currentCommittee) ||
                _isCommitteeMember(validatorAddress, nextCommittee) ||
                _isCommitteeMember(validatorAddress, futureCommittee)
            ) continue;

            ValidatorInfo storage exitValidator = validators[validatorAddress];
            _exit(exitValidator, current);
        }
    }

    /// @notice Forcibly eject a validator from the current, next, and subsequent committees
    /// @dev Intended for sparing use; only reverts if burning results in empty committee
    function _ejectFromCommittees(
        address validatorAddress,
        uint256 numEligible
    ) internal {
        uint32 current = currentEpoch;
        uint8 currentEpochPointer = epochPointer;
        address[] storage currentCommittee = _getRecentEpochInfo(
            current,
            current,
            currentEpochPointer
        ).committee;
        _eject(currentCommittee, validatorAddress);
        _checkCommitteeSize(numEligible, currentCommittee.length);

        uint32 nextEpoch = current + 1;
        address[] storage nextCommittee = _getFutureEpochInfo(
            nextEpoch,
            current,
            currentEpochPointer
        ).committee;
        _eject(nextCommittee, validatorAddress);
        _checkCommitteeSize(numEligible, nextCommittee.length);

        uint32 subsequentEpoch = current + 2;
        address[] storage subsequentCommittee = _getFutureEpochInfo(
            subsequentEpoch,
            current,
            currentEpochPointer
        ).committee;
        _eject(subsequentCommittee, validatorAddress);
        _checkCommitteeSize(numEligible, subsequentCommittee.length);
    }

    /// @dev Removes a validator from a committee using shift-left deletion
    /// to preserve ascending sort order (required by _enforceSorting in concludeEpoch).
    function _eject(
        address[] storage committee,
        address validatorAddress
    ) internal returns (bool) {
        uint256 len = committee.length;
        for (uint256 i; i < len; ++i) {
            if (committee[i] == validatorAddress) {
                // shift remaining elements left to maintain sorted order
                for (uint256 j = i; j < len - 1; ++j) {
                    committee[j] = committee[j + 1];
                }
                committee.pop();
                return true;
            }
        }
        return false;
    }

    /// @dev Invoked either as part of a governance-initiated burn or a validator's final slash to 0
    /// @notice Burns or final slashes confiscate the validator's remaining stake and rewards,
    /// repurposing them as future reward pool balance
    function _consensusBurn(address validatorAddress) internal {
        ValidatorInfo storage validator = validators[validatorAddress];
        ValidatorStatus status = validator.currentStatus;
        // reverts if decremented committee size after ejection reaches 0, preventing network halt
        uint256 numEligible = _getValidators(ValidatorStatus.Active).length;
        // if validator being ejected is committee-eligible, ejection will decrement `numEligible`
        if (_eligibleForCommitteeNextEpoch(status)) {
            numEligible = numEligible - 1;
        }
        _ejectFromCommittees(validatorAddress, numEligible);

        // settle ledgers — confiscate all remaining balance
        balances[validatorAddress] = 0;

        // exit, retire, and unstake + burn validator immediately
        _exit(validator, currentEpoch);
        _retire(validator);
        address recipient = _getRecipient(validatorAddress);
        _unstake(validatorAddress, recipient);
    }

    /// @dev Stores the number of blocks finalized in previous epoch and the voter committee for the new epoch
    function _updateEpochInfo(
        address[] memory futureCommittee
    ) internal returns (uint32, uint32, address[] memory) {
        // cache epoch ring buffer's pointers in memory
        uint8 prevEpochPointer = epochPointer;
        uint8 newEpochPointer = (prevEpochPointer + 1) % 4;

        EpochInfo storage newInfo = epochInfo[newEpochPointer];

        newInfo.committee = futureEpochInfo[newEpochPointer].committee;

        StakeConfig memory newStakeConfig = getCurrentStakeConfig();
        newInfo.blockHeight = uint64(block.number) + 1;
        newInfo.epochDuration = newStakeConfig.epochDuration;
        newInfo.stakeVersion = stakeVersion;

        epochPointer = newEpochPointer;
        uint32 newEpoch = ++currentEpoch;

        // update future epoch info
        uint8 twoEpochsInFuturePointer = (newEpochPointer + 2) % 4;
        futureEpochInfo[twoEpochsInFuturePointer].committee = futureCommittee;

        return (
            newEpoch,
            newStakeConfig.epochDuration,
            newInfo.committee
        );
    }

    /// @dev Fetch info for a future epoch; two epochs into future are stored
    /// @notice Block height is not known for future epochs, so it will be 0
    function _getFutureEpochInfo(
        uint32 future,
        uint32 current,
        uint8 currentPointer
    ) internal view returns (EpochInfo storage) {
        uint8 futurePointer = (uint8(future - current) + currentPointer) % 4;
        return futureEpochInfo[futurePointer];
    }

    /// @dev Fetch info for a current or past epoch; four latest are stored (current and three in past)
    function _getRecentEpochInfo(
        uint32 recent,
        uint32 current,
        uint8 currentPointer
    ) internal view returns (EpochInfo storage) {
        // identify diff from pointer, preventing underflow by adding 4 (will be modulo'd away)
        uint8 dist = uint8(current - recent);
        uint8 pointer = (currentPointer + 4 - (dist % 4)) % 4;
        return epochInfo[pointer];
    }

    function _enforceSorting(address[] calldata futureCommittee) internal pure {
        for (uint256 i = 1; i < futureCommittee.length; ++i) {
            if (futureCommittee[i - 1] >= futureCommittee[i])
                revert CommitteeRequirement(futureCommittee[i - 1]);
        }
    }

    /// @dev Checks current committee size against total eligible for committee service in next epoch
    /// @notice Prevents the network from reaching invalid committee state
    function _checkCommitteeSize(
        uint256 activeOrPending,
        uint256 committeeSize
    ) internal pure {
        if (
            activeOrPending == 0 ||
            committeeSize == 0 ||
            committeeSize > activeOrPending
        ) {
            revert InvalidCommitteeSize(activeOrPending, committeeSize);
        }
    }

    /// @dev Reverts if the provided validator's status doesn't match the provided `requiredStatus`
    function _checkValidatorStatus(
        address validatorAddress,
        ValidatorStatus requiredStatus
    ) private view {
        ValidatorStatus status = validators[validatorAddress].currentStatus;
        if (status != requiredStatus) revert InvalidStatus(status);
    }

    /// @dev Returns whether given `validatorAddress` is a member of the given committee
    function _isCommitteeMember(
        address validatorAddress,
        address[] memory committee
    ) internal pure returns (bool) {
        // cache len to memory
        uint256 committeeLen = committee.length;
        for (uint256 i; i < committeeLen; ++i) {
            // terminate if `validatorAddress` is a member of committee
            if (committee[i] == validatorAddress) return true;
        }

        return false;
    }

    /// @dev Active and pending activation/exit validators are eligible for committee service in next epoch
    function _eligibleForCommitteeNextEpoch(
        ValidatorStatus status
    ) internal pure returns (bool) {
        return (status == ValidatorStatus.Active ||
            status == ValidatorStatus.PendingExit ||
            status == ValidatorStatus.PendingActivation);
    }

    /// @dev Returns true for `Staked` or `Exited` validators that have elapsed one full epoch since exit
    function _eligibleForUnstake(ValidatorInfo storage validator) internal view returns (bool) {
        ValidatorStatus status = validator.currentStatus;
        if (status == ValidatorStatus.Staked) return true;

        if (status == ValidatorStatus.Exited) {
            uint32 eligibleEpoch = validator.exitEpoch + 1;
            return currentEpoch >= eligibleEpoch;
        }

        return false;
    }


    /// @dev There are ~1000 total MNOs in the world so `SLOAD` loops should not run out of gas
    /// @dev Room for storage optimization (SSTORE2 etc) to hold more validators
    function _getValidators(
        ValidatorStatus status
    ) internal view returns (ValidatorInfo[] memory) {
        ValidatorInfo[] memory validatorsMatched = new ValidatorInfo[](validatorsAddresses.length);
        uint256 numMatches;

        for (uint256 i; i < validatorsAddresses.length; ++i) {
            address validatorAddress = validatorsAddresses[i];
            ValidatorInfo storage current = validators[validatorAddress];
            if (current.isRetired) continue;

            // queries for `Any` status include all unretired validators
            bool matchFound = status == ValidatorStatus.Any;
            if (!matchFound) {
                // mem cache to save SLOADs
                ValidatorStatus currentStatus = current.currentStatus;

                // include pending activation/exit due to committee service eligibility in next epoch
                if (status == ValidatorStatus.Active) {
                    matchFound = _eligibleForCommitteeNextEpoch(currentStatus);
                } else {
                    // all other queries return only exact matches
                    matchFound = currentStatus == status;
                }
            }

            if (matchFound) {
                validatorsMatched[numMatches++] = current;
            }
        }

        // trim and return array
        assembly {
            mstore(validatorsMatched, numMatches)
        }

        return validatorsMatched;
    }

    /**
     *
     *   pausability
     *
     */

    /// @dev Emergency function to pause validator and stake management
    /// @notice Does not pause system callable fns. Only accessible by `owner`
    function pause() external onlyOwner {
        _pause();
    }

    /// @dev Emergency function to unpause validator and stake management
    /// @notice Does not affect system callable fns. Only accessible by `owner`
    function unpause() external onlyOwner {
        _unpause();
    }

    /**
     *
     *   configuration
     *
     */

    /// @param initialValidators_ The initial validator set running Rayls Network; these validators will
    /// comprise the voter committee for the first three epochs, ie `epochInfo[0:2]`
    /// @dev Stake for `initialValidators_` is allocated as ERC-20 RLS balance entries;
    /// the corresponding RLS tokens must be transferred to this contract at genesis
    /// @dev Only governance delegation is enabled at genesis
    constructor(
        address rls_,
        StakeConfig memory genesisConfig_,
        ValidatorInfo[] memory initialValidators_,
        BlsG1.ProofOfPossession[] memory proofsOfPossession,
        address owner_
    ) StakeManager(rls_) Ownable(owner_) {
        if (
            initialValidators_.length == 0 ||
            initialValidators_.length != proofsOfPossession.length
        ) {
            revert GenesisArityMismatch();
        }

        // set stake storage configs
        versions[0] = genesisConfig_;

        for (uint256 j; j <= 2; ++j) {
            EpochInfo storage epoch = epochInfo[j];
            epoch.epochDuration = genesisConfig_.epochDuration;
        }

        for (uint256 i; i < initialValidators_.length; ++i) {
            ValidatorInfo memory currentValidator = initialValidators_[i];

            // assert `validatorIndex` struct members match expected value
            bytes memory currentMsg = proofOfPossessionMessage(
                proofsOfPossession[i].uncompressedPubkey,
                currentValidator.validatorAddress
            );
            bytes memory eip2537Pubkey = BlsG1.encodeG2PointForEIP2537(
                proofsOfPossession[i].uncompressedPubkey
            );
            bytes memory eip2537Signature = BlsG1.encodeG1PointForEIP2537(
                proofsOfPossession[i].uncompressedSignature
            );
            if (
                !BlsG1.verifyProofOfPossessionG1(
                    eip2537Pubkey,
                    eip2537Signature,
                    currentMsg
                )
            ) {
                revert BlsG1.InvalidBLSPubkey();
            }
            if (currentValidator.validatorAddress == address(0x0)) {
                revert InvalidValidatorAddress();
            }
            if (currentValidator.activationEpoch != uint32(0)) {
                revert InvalidEpoch(currentValidator.activationEpoch);
            }
            if (currentValidator.exitEpoch != uint32(0)) {
                revert InvalidEpoch(currentValidator.exitEpoch);
            }
            if (currentValidator.currentStatus != ValidatorStatus.Active) {
                revert InvalidStatus(currentValidator.currentStatus);
            }
            if (currentValidator.isRetired != false) {
                revert InvalidStatus(ValidatorStatus.Exited);
            }
            if (currentValidator.isDelegated == true) {
                // at genesis, only governance delegations are enabled
                delegations[currentValidator.validatorAddress] = Delegation(
                    keccak256(currentValidator.blsPubkey),
                    currentValidator.validatorAddress,
                    owner_,
                    uint8(0),
                    uint64(1)
                );
            }
            if (currentValidator.stakeVersion != 0) {
                revert InvalidStakeAmount(currentValidator.stakeVersion, 0);
            }

            // first three epochs use initial validators as committee
            for (uint256 j; j <= 2; ++j) {
                epochInfo[j].committee.push(currentValidator.validatorAddress);
                futureEpochInfo[j].committee.push(
                    currentValidator.validatorAddress
                );
            }

            validators[currentValidator.validatorAddress] = currentValidator;

            _stake(currentValidator.validatorAddress, genesisConfig_.stakeAmount);

            // automatically allowlist genesis validators
            validatorAllowlist[currentValidator.validatorAddress] = true;

            emit ValidatorActivated(currentValidator);
        }
    }

    /// @inheritdoc IStakeManager
    function upgradeStakeVersion(
        StakeConfig calldata newConfig
    ) external override onlyOwner whenNotPaused returns (uint8) {
        if (newConfig.epochDuration == 0)
            revert InvalidDuration(newConfig.epochDuration);

        uint8 newVersion = ++stakeVersion;
        versions[newVersion] = newConfig;

        return newVersion;
    }
}
