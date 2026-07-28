// SPDX-License-Identifier: BUSL-1.1
pragma solidity 0.8.26;

import "forge-std/Test.sol";
import {ConsensusRegistry} from "src/consensus/ConsensusRegistry.sol";
import {RewardInfo} from "src/interfaces/IStakeManager.sol";
import {BlsG1} from "src/consensus/BlsG1.sol";
import {BlsG1Harness} from "../EIP2537/BlsG1Harness.sol";
import {GenesisPrecompiler} from "../../deployments/genesis/GenesisPrecompiler.sol";
import {ERC20} from "@openzeppelin/contracts/token/ERC20/ERC20.sol";

/// @notice Mock RLS token (ERC-20 staking token) for consensus registry tests
contract MockRLS is ERC20 {
    constructor() ERC20("Mock RLS", "RLS") {}
    function mint(address to, uint256 amount) external {
        _mint(to, amount);
    }
}

contract ConsensusRegistryTestUtils is ConsensusRegistry, BlsG1Harness, GenesisPrecompiler {
    using BlsG1 for bytes;

    ConsensusRegistry public consensusRegistry;
    MockRLS public mockRLS;

    address public crOwner = address(0xc0ffee);
    address public validator1 = _addressFromPrivateKey(1);
    address public validator2 = _addressFromPrivateKey(2);
    address public validator3 = _addressFromPrivateKey(3);
    address public validator4 = _addressFromPrivateKey(4);

    ValidatorInfo validatorInfo1;
    ValidatorInfo validatorInfo2;
    ValidatorInfo validatorInfo3;
    ValidatorInfo validatorInfo4;

    ValidatorInfo[] initialValidators; // contains validatorInfo1-4
    BlsG1.ProofOfPossession[] initialBLSPops;

    address public sysAddress;

    // non-genesis validator for testing
    uint256 public validator5Secret = 5;
    address public validator5 = _addressFromPrivateKey(validator5Secret);
    bytes public validator5BlsPubkey =
        eip2537PointG2ToUncompressed(
            _blsEIP2537PubkeyFromSecret(validator5Secret)
        );

    uint256 public rlsMaxSupply = 100_000_000_000e18;
    uint256 public stakeAmount_ = 1_000_000e18;
    uint256 public minWithdrawAmount_ = 1000e18;
    uint32 public epochDuration_ = 24 hours;
    // `OZ::ERC721Upgradeable::mint()` supports up to ~14_300 fuzzed mint iterations
    uint256 public MAX_MINTABLE = 14_000;

    constructor()
        ConsensusRegistry(
            address(_deployMockRLS()),
            StakeConfig(
                stakeAmount_,
                minWithdrawAmount_,
                epochDuration_
            ),
            _populateInitialValidators(),
            _populateinitialBLSPops(),
            crOwner
        )
    {}

    function _deployMockRLS() internal returns (MockRLS) {
        mockRLS = new MockRLS();
        return mockRLS;
    }

    // convenience fn for constructor
    function _populateInitialValidators()
        internal
        returns (ValidatorInfo[] memory)
    {
        // provide initial validator set as the network will launch with at least four validators
        validatorInfo1 = ValidatorInfo(
            _blsDummyPubkeyFromSecret(1),
            validator1,
            uint32(0),
            uint32(0),
            ValidatorStatus.Active,
            false,
            false,
            uint8(0)
        );
        validatorInfo2 = ValidatorInfo(
            _blsDummyPubkeyFromSecret(2),
            validator2,
            uint32(0),
            uint32(0),
            ValidatorStatus.Active,
            false,
            false,
            uint8(0)
        );
        validatorInfo3 = ValidatorInfo(
            _blsDummyPubkeyFromSecret(3),
            validator3,
            uint32(0),
            uint32(0),
            ValidatorStatus.Active,
            false,
            false,
            uint8(0)
        );
        validatorInfo4 = ValidatorInfo(
            _blsDummyPubkeyFromSecret(4),
            validator4,
            uint32(0),
            uint32(0),
            ValidatorStatus.Active,
            false,
            false,
            uint8(0)
        );
        initialValidators.push(validatorInfo1);
        initialValidators.push(validatorInfo2);
        initialValidators.push(validatorInfo3);
        initialValidators.push(validatorInfo4);

        return initialValidators;
    }

    // convenience fn for constructor
    function _populateinitialBLSPops()
        internal
        returns (BlsG1.ProofOfPossession[] memory)
    {
        for (uint256 i; i < initialValidators.length; ++i) {
            uint256 secretI = i + 1;
            address validatorI = initialValidators[i].validatorAddress;
            bytes memory pubkey = eip2537PointG2ToUncompressed(
                _blsEIP2537PubkeyFromSecret(secretI)
            );
            bytes memory messageI = proofOfPossessionMessage(
                pubkey,
                validatorI
            );
            bytes memory signature = eip2537PointG1ToUncompressed(
                _blsEIP2537SignatureFromSecret(secretI, messageI)
            );

            initialBLSPops.push(BlsG1.ProofOfPossession(pubkey, signature));
        }

        return initialBLSPops;
    }

    function _sortAddresses(address[] memory arr) internal pure {
        uint256 length = arr.length;
        for (uint256 i; i < length; i++) {
            for (uint256 j; j < length - 1; j++) {
                if (arr[j] > arr[j + 1]) {
                    address temp = arr[j];
                    arr[j] = arr[j + 1];
                    arr[j + 1] = temp;
                }
            }
        }
    }

    /// @notice Never do this onchain in production!! Only for testing
    function _addressFromPrivateKey(
        uint256 pk
    ) internal pure returns (address) {
        return vm.addr(pk);
    }

    function _fuzz_stake(uint24 numValidators, uint256 amount) internal {
        for (uint256 i; i < numValidators; ++i) {
            // recreate `newValidator`, accounting for initial validators
            uint256 secret = i + 5;
            address newValidator = _addressFromPrivateKey(secret);

            // generate new validator keys & signatures
            bytes memory newBLSPubkey = eip2537PointG2ToUncompressed(
                _blsEIP2537PubkeyFromSecret(secret)
            );
            bytes memory message = proofOfPossessionMessage(
                newBLSPubkey,
                newValidator
            );
            bytes memory blsSignature = eip2537PointG1ToUncompressed(
                _blsEIP2537SignatureFromSecret(secret, message)
            );

            // allowlist, stake, and activate
            vm.prank(crOwner);
            consensusRegistry.allowlistValidator(newValidator);
            mockRLS.mint(newValidator, amount);
            vm.startPrank(newValidator);
            mockRLS.approve(address(consensusRegistry), amount);
            consensusRegistry.stake(
                _blsDummyPubkeyFromSecret(secret),
                BlsG1.ProofOfPossession(newBLSPubkey, blsSignature)
            );
            vm.stopPrank();
        }
    }

    function _fuzz_activate(uint24 numValidators) internal {
        for (uint256 i; i < numValidators; ++i) {
            // recreate `newValidator`, accounting for initial validators
            uint256 tokenId = i + 5;
            address newValidator = _addressFromPrivateKey(tokenId);

            vm.prank(newValidator);
            consensusRegistry.activate();
        }
    }

    function _fuzz_computeCommitteeSize(
        uint256 numActive,
        uint256 numFuzzedValidators
    ) internal pure returns (uint256) {
        // identify expected committee size
        uint256 committeeSize;
        if (numFuzzedValidators <= 6) {
            // 4 initial and 6 new validators would be under the 10 committee size
            committeeSize = numActive;
        } else {
            committeeSize = (numActive * 1e32) / 3 / 1e32 + 1;
        }

        return committeeSize;
    }

    function _fuzz_createFutureCommittee(
        uint256 numActive,
        uint256 committeeSize
    ) internal pure returns (address[] memory) {
        // reloop to construct `futureCommittee` array
        address[] memory futureCommittee = new address[](committeeSize);
        uint256 committeeCounter;
        // `tokenId` is 1-indexed
        uint256 index = 1 +
            (uint256(keccak256(abi.encode(committeeSize))) % committeeSize);
        // handle index overflow by wrapping around to first index
        uint256 nonOverflowIndex = 1 + numActive - committeeSize;
        index = index > nonOverflowIndex ? nonOverflowIndex : index;
        while (committeeCounter < futureCommittee.length) {
            // recreate `validator` address in `setUp()` loop
            address validator = _addressFromPrivateKey(index);
            futureCommittee[committeeCounter] = validator;
            committeeCounter++;
            index++;
        }

        _sortAddresses(futureCommittee);

        return futureCommittee;
    }

    function _createTokenIdCommittee(
        uint256 committeeSize
    ) internal pure returns (address[] memory) {
        address[] memory committee = new address[](committeeSize);
        for (uint256 i; i < committee.length; ++i) {
            // create dummy `validator` address equivalent to their `tokenId`
            uint256 tokenId = i + 1;
            address validator = address(uint160(tokenId));
            committee[i] = validator;
        }

        return committee;
    }

    /// @dev `totalRounds` bounds both `participationRounds` and `anchorRounds` per
    /// rewardee (`[0, totalRounds]`, as a validator can participate/lead at most
    /// once per round) but the fixture does not constrain their cross-validator
    /// sums (e.g. anchor rounds summing to exactly `totalRounds`) - the blended
    /// weight formula doesn't require that invariant to hold to compute a result,
    /// and per-validator round counts are exactly what `applyIncentives` trusts.
    function _fuzz_createRewardInfos(
        uint24 numRewardees
    ) internal view returns (RewardInfo[] memory, uint256 totalRounds) {
        totalRounds = 10_000;
        RewardInfo[] memory rewardInfos = new RewardInfo[](numRewardees);
        for (uint256 i; i < numRewardees; ++i) {
            address rewardee = _addressFromPrivateKey(i + 1);
            uint256 uniqueSeed = i + numRewardees;
            uint256 participationRounds = uint256(
                keccak256(abi.encode(uniqueSeed, "participation"))
            ) % (totalRounds + 1);
            uint256 anchorRounds = uint256(
                keccak256(abi.encode(uniqueSeed, "anchor"))
            ) % (totalRounds + 1);

            rewardInfos[i] = RewardInfo(rewardee, participationRounds, anchorRounds);
        }

        return (rewardInfos, totalRounds);
    }
}
