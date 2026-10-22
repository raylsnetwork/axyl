// SPDX-License-Identifier: BUSL-1.1
pragma solidity 0.8.26;

import "forge-std/Test.sol";
import { DkgManager } from "src/consensus/DkgManager.sol";
import { SystemCallable } from "src/consensus/SystemCallable.sol";
import { IConsensusRegistry } from "src/interfaces/IConsensusRegistry.sol";

/// @notice Registry stand-in with only the getters `DkgManager` reads.
contract MockRegistry {
    uint32 public currentEpoch;
    mapping(uint32 => address[]) internal committees;
    mapping(uint32 => uint64) internal startBlocks;
    mapping(address => bytes) internal blsKeys;

    function setStartBlock(uint32 epoch, uint64 blockHeight) external {
        startBlocks[epoch] = blockHeight;
    }

    function setCurrentEpoch(uint32 epoch) external {
        currentEpoch = epoch;
    }

    function setCommittee(uint32 epoch, address[] calldata committee) external {
        committees[epoch] = committee;
    }

    function setBlsKey(address validator, bytes calldata key) external {
        blsKeys[validator] = key;
    }

    function getCurrentEpoch() external view returns (uint32) {
        return currentEpoch;
    }

    function getEpochInfo(uint32 epoch) external view returns (IConsensusRegistry.EpochInfo memory info) {
        info.committee = committees[epoch];
        info.blockHeight = startBlocks[epoch];
    }

    function getValidator(address validator) external view returns (IConsensusRegistry.ValidatorInfo memory info) {
        info.validatorAddress = validator;
        info.blsPubkey = blsKeys[validator];
        info.currentStatus = blsKeys[validator].length == 0
            ? IConsensusRegistry.ValidatorStatus.Undefined
            : IConsensusRegistry.ValidatorStatus.Active;
    }
}

/// @notice Precompile stand-in: answers `verdict` to every call and remembers the last input.
contract MockPrecompile {
    bool public verdict;

    function setVerdict(bool value) external {
        verdict = value;
    }

    fallback(bytes calldata) external returns (bytes memory) {
        return abi.encode(verdict);
    }
}

/// @notice A sender that refuses native transfers.
contract Refuser {
    function register(DkgManager dkg, address validator) external {
        dkg.registerEncryptionKey(validator, hex"ee", hex"01", hex"02");
    }
}

contract DkgManagerTest is Test {
    DkgManager dkg;
    MockRegistry registry;
    MockPrecompile precompile;

    address v1 = address(0x1001);
    address v2 = address(0x1002);
    address v3 = address(0x1003);
    address v4 = address(0x1004);
    address outsider = address(0x2000);
    address constant SYSTEM = address(0xffffFFFfFFffffffffffffffFfFFFfffFFFfFFfE);

    function setUp() public {
        dkg = new DkgManager();
        vm.etch(address(dkg.REGISTRY()), address(new MockRegistry()).code);
        vm.etch(dkg.NIDKG_PRECOMPILE(), address(new MockPrecompile()).code);
        registry = MockRegistry(address(dkg.REGISTRY()));
        precompile = MockPrecompile(dkg.NIDKG_PRECOMPILE());

        address[] memory committee = _committee4();
        registry.setCurrentEpoch(5);
        // epoch 5, which deals the key of 6, starts at block 100
        registry.setStartBlock(5, 100);
        registry.setStartBlock(6, 200);
        registry.setCommittee(4, committee);
        registry.setCommittee(5, committee);
        registry.setCommittee(6, committee);
        for (uint256 i; i < committee.length; ++i) {
            registry.setBlsKey(committee[i], abi.encodePacked(bytes32(uint256(i + 1))));
        }
        precompile.setVerdict(true);
    }

    function _committee4() internal view returns (address[] memory committee) {
        committee = new address[](4);
        committee[0] = v1;
        committee[1] = v2;
        committee[2] = v3;
        committee[3] = v4;
    }

    function _key(uint256 i) internal pure returns (bytes memory) {
        return abi.encodePacked(bytes32(uint256(100 + i)));
    }

    /// Register every member's key in block 50, before epoch 5 starts, then move into epoch 5.
    function _registerAll() internal {
        vm.roll(50);
        address[4] memory all = [v1, v2, v3, v4];
        for (uint256 i; i < all.length; ++i) {
            dkg.registerEncryptionKey(all[i], _key(i), hex"01", hex"02");
        }
        vm.roll(150);
    }

    function test_registration_needs_a_valid_proof_and_happens_once() public {
        precompile.setVerdict(false);
        vm.expectRevert(abi.encodeWithSelector(DkgManager.InvalidRegistration.selector, v1));
        dkg.registerEncryptionKey(v1, hex"aa", hex"01", hex"02");

        precompile.setVerdict(true);
        dkg.registerEncryptionKey(v1, hex"aa", hex"01", hex"02");
        assertEq(dkg.encryptionKeys(v1), hex"aa");
        assertEq(dkg.keyOwners(keccak256(hex"aa")), v1);
        vm.expectRevert(abi.encodeWithSelector(DkgManager.KeyAlreadyRegistered.selector, v1));
        dkg.registerEncryptionKey(v1, hex"bb", hex"01", hex"02");

        vm.expectRevert(abi.encodeWithSelector(DkgManager.UnknownValidator.selector, outsider));
        dkg.registerEncryptionKey(outsider, hex"cc", hex"01", hex"02");
    }

    /// Two validators cannot register one key: a dealing to both would never verify.
    function test_a_key_serves_one_validator() public {
        dkg.registerEncryptionKey(v1, hex"aa", hex"01", hex"02");
        vm.expectRevert(abi.encodeWithSelector(DkgManager.KeyInUse.selector, v1));
        dkg.registerEncryptionKey(v2, hex"aa", hex"01", hex"02");
        // nor one set at genesis
        _setGenesisKey(v3, hex"dd");
        vm.expectRevert(abi.encodeWithSelector(DkgManager.KeyInUse.selector, v3));
        dkg.registerEncryptionKey(v4, hex"dd", hex"01", hex"02");
    }

    /// Only keys set at genesis or registered before the dealing epoch's first block count: one
    /// registered in that block, or later, does not, and one member without a key in time leaves
    /// the epoch without a key.
    function test_recipients_are_fixed_before_the_dealing_epoch() public {
        vm.roll(99);
        dkg.registerEncryptionKey(v1, hex"aa", hex"01", hex"02");
        vm.roll(100);
        dkg.registerEncryptionKey(v2, hex"bb", hex"01", hex"02");
        vm.roll(150);
        dkg.registerEncryptionKey(v3, hex"cc", hex"01", hex"02");
        _setGenesisKey(v4, hex"dd");

        (address[] memory members, bytes[] memory keys) = dkg.recipients(6);
        assertEq(members.length, 4);
        assertEq(keys[0], hex"aa");
        assertEq(keys[1], hex"");
        assertEq(keys[2], hex"");
        assertEq(keys[3], hex"dd");
        assertEq(dkg.registrationBlocks(v1), 99);
        assertFalse(dkg.hasKey(6));
        assertTrue(dkg.isReady(6), "an epoch without a key is settled at once");
        vm.expectRevert(abi.encodeWithSelector(DkgManager.NoKeyForEpoch.selector, 6));
        dkg.submitDealing(6, v1, hex"03", hex"04");
    }

    function test_dealing_rules() public {
        _registerAll();
        assertTrue(dkg.hasKey(6));
        vm.expectRevert(abi.encodeWithSelector(DkgManager.WrongDealingEpoch.selector, 7, 6));
        dkg.submitDealing(7, v1, hex"03", hex"04");
        vm.expectRevert(abi.encodeWithSelector(DkgManager.NotADealer.selector, 6, outsider));
        dkg.submitDealing(6, outsider, hex"03", hex"04");

        precompile.setVerdict(false);
        vm.expectRevert(abi.encodeWithSelector(DkgManager.InvalidDealing.selector, 6, v1));
        dkg.submitDealing(6, v1, hex"03", hex"04");

        precompile.setVerdict(true);
        vm.roll(142);
        bytes32 digest = dkg.recipientsDigest(6);
        vm.expectEmit(address(dkg));
        emit DkgManager.DealingAccepted(6, v1, digest, keccak256(hex"04"), hex"04");
        dkg.submitDealing(6, v1, hex"03", hex"04");
        assertEq(dkg.dealingHash(6, v1), keccak256(hex"04"));
        assertEq(dkg.dealingBlock(6, v1), 142);
        vm.expectRevert(abi.encodeWithSelector(DkgManager.AlreadyDealt.selector, 6, v1));
        dkg.submitDealing(6, v1, hex"03", hex"05");
    }

    /// The key needs ceil(n/3) dealings: 2 of 4.
    function test_ready_at_a_third_of_the_dealers() public {
        _registerAll();
        assertFalse(dkg.isReady(6));
        dkg.submitDealing(6, v3, hex"03", hex"04");
        assertFalse(dkg.isReady(6));
        // anyone may submit a dealing; the dealer's signature authorises it
        vm.prank(outsider);
        dkg.submitDealing(6, v1, hex"03", hex"05");
        assertTrue(dkg.isReady(6));
        address[] memory accepted = dkg.dealers(6);
        assertEq(accepted.length, 2);
        assertEq(accepted[0], v3);
        assertEq(accepted[1], v1);
    }

    /// An ejection from the next committee while dealing is open changes the recipient set:
    /// earlier dealings stop counting and the dealers deal again. Once finalized, the set and
    /// its dealings stay, whatever the registry does next.
    function test_an_ejection_restarts_dealing_and_finalize_freezes_the_key() public {
        _registerAll();
        dkg.submitDealing(6, v1, hex"03", hex"04");
        dkg.submitDealing(6, v2, hex"03", hex"05");
        assertTrue(dkg.isReady(6));
        bytes32 before = dkg.recipientsDigest(6);

        address[] memory three = new address[](3);
        (three[0], three[1], three[2]) = (v1, v2, v3);
        registry.setCommittee(6, three);
        assertTrue(dkg.recipientsDigest(6) != before);
        assertFalse(dkg.isReady(6), "the dealings were for the old set");
        assertEq(dkg.dealers(6).length, 0);
        dkg.submitDealing(6, v1, hex"03", hex"06");
        dkg.submitDealing(6, v3, hex"03", hex"07");
        assertTrue(dkg.isReady(6));

        vm.expectRevert(abi.encodeWithSelector(SystemCallable.OnlySystemCall.selector, address(this)));
        dkg.finalize(6);
        vm.prank(SYSTEM);
        dkg.finalize(6);
        (bool finalized, bool keyed, bytes32 digest) = dkg.finals(6);
        assertTrue(finalized && keyed);
        assertEq(digest, dkg.recipientsDigest(6));
        assertEq(dkg.finalMembers(6).length, 3);

        // a later ejection changes nothing about epoch 6's key
        address[] memory two = new address[](2);
        (two[0], two[1]) = (v1, v3);
        registry.setCommittee(6, two);
        assertTrue(dkg.hasKey(6) && dkg.isReady(6));
        assertEq(dkg.activeDigest(6), digest);
        assertEq(dkg.dealers(6).length, 2);
        assertEq(dkg.dealingHash(6, v3), keccak256(hex"07"));
        assertEq(dkg.finalMembers(6).length, 3);
        // and finalize runs once
        vm.prank(SYSTEM);
        dkg.finalize(6);
        (,, bytes32 again) = dkg.finals(6);
        assertEq(again, digest);
    }

    /// Finalized with too few dealings, an epoch has no key and uses aggregate certificates.
    function test_finalize_without_enough_dealings_leaves_no_key() public {
        _registerAll();
        dkg.submitDealing(6, v1, hex"03", hex"04");
        vm.prank(SYSTEM);
        dkg.finalize(6);
        (bool finalized, bool keyed,) = dkg.finals(6);
        assertTrue(finalized);
        assertFalse(keyed);
        assertFalse(dkg.hasKey(6));
        assertTrue(dkg.isReady(6));
    }

    /// A committee of `n` new validators, numbered from `first`, with BLS keys and encryption keys
    /// registered in block 50, set as the committee of epoch 6.
    function _registeredCommittee(uint256 n, uint256 first) internal returns (address[] memory committee) {
        committee = new address[](n);
        vm.roll(50);
        for (uint256 i; i < n; ++i) {
            committee[i] = address(uint160(0x5000 + first + i));
            registry.setBlsKey(committee[i], abi.encodePacked(bytes32(uint256(1000 + first + i))));
            dkg.registerEncryptionKey(committee[i], _key(1000 + first + i), hex"01", hex"02");
        }
        registry.setCommittee(6, committee);
        vm.roll(150);
    }

    /// A committee of `MAX_KEYED_COMMITTEE` gets a key; one more member and the epoch uses
    /// aggregate certificates: ready at once, and no dealing is accepted.
    function test_committee_size_cap() public {
        uint256 cap = dkg.MAX_KEYED_COMMITTEE();
        _registeredCommittee(cap, 0);
        assertTrue(dkg.hasKey(6));
        assertFalse(dkg.isReady(6));

        address[] memory larger = _registeredCommittee(cap + 1, cap);
        assertFalse(dkg.hasKey(6));
        assertTrue(dkg.isReady(6));
        vm.expectRevert(abi.encodeWithSelector(DkgManager.NoKeyForEpoch.selector, uint32(6)));
        dkg.submitDealing(6, v1, hex"03", hex"04");
        vm.prank(SYSTEM);
        dkg.finalize(6);
        (bool finalized, bool keyed,) = dkg.finals(6);
        assertTrue(finalized);
        assertFalse(keyed);
        assertEq(dkg.finalMembers(6).length, larger.length);
    }

    /// A key at genesis, as the genesis builder writes it: the bytes in `encryptionKeys` (slot 0),
    /// the owner in `keyOwners` (slot 1) and no registration block.
    function _setGenesisKey(address validator, bytes memory key) internal {
        require(key.length < 32, "short keys only in this helper");
        bytes32 slot = keccak256(abi.encode(validator, uint256(0)));
        vm.store(address(dkg), slot, bytes32(abi.encodePacked(key)) | bytes32(key.length * 2));
        vm.store(address(dkg), keccak256(abi.encode(keccak256(key), uint256(1))), bytes32(uint256(uint160(validator))));
        assertEq(dkg.encryptionKeys(validator), key);
        assertEq(dkg.keyOwners(keccak256(key)), validator);
    }

    /// Accepted calls are refunded at no more than the base fee while the contract has funds; a
    /// rejected call, one made when the contract is empty, and bytes appended to a call are not.
    function test_gas_refunds() public {
        address sender = address(0x3000);
        vm.fee(10 gwei);
        vm.txGasPrice(30 gwei);
        vm.prank(sender);
        dkg.registerEncryptionKey(v1, hex"aa", hex"01", hex"02");
        assertEq(sender.balance, 0, "no funds, no refund");

        vm.deal(address(dkg), 1 ether);
        vm.prank(sender);
        dkg.registerEncryptionKey(v2, hex"bb", hex"01", hex"02");
        uint256 refund = sender.balance;
        assertGt(refund, 0);
        // at the base fee, not the sender's price: well under 1M gas
        assertLt(refund, 1_000_000 * 10 gwei);
        assertEq(address(dkg).balance, 1 ether - refund);

        precompile.setVerdict(false);
        vm.prank(sender);
        vm.expectRevert(abi.encodeWithSelector(DkgManager.InvalidRegistration.selector, v3));
        dkg.registerEncryptionKey(v3, hex"cc", hex"01", hex"02");
        assertEq(sender.balance, refund);

        // calldata padded with 100 kB of zeros: refunded as the canonical call, give or take the
        // gas of copying the padding
        precompile.setVerdict(true);
        bytes memory padded = abi.encodePacked(
            abi.encodeCall(DkgManager.registerEncryptionKey, (v3, hex"cc", hex"01", hex"02")), new bytes(100_000)
        );
        vm.prank(sender);
        (bool ok,) = address(dkg).call(padded);
        assertTrue(ok);
        uint256 paddedRefund = sender.balance - refund;
        assertLt(paddedRefund, refund + 200_000 * 10 gwei, "padding is not refunded");

        // a sender that cannot receive still registers
        Refuser refuser = new Refuser();
        refuser.register(dkg, v4);
        assertEq(dkg.encryptionKeys(v4), hex"ee");
    }

    function test_thresholds() public view {
        assertEq(dkg.threshold(4), 3);
        assertEq(dkg.threshold(7), 5);
        assertEq(dkg.threshold(56), 38);
        assertEq(dkg.dealersNeeded(1), 1);
        assertEq(dkg.dealersNeeded(4), 2);
        assertEq(dkg.dealersNeeded(7), 3);
        assertEq(dkg.dealersNeeded(56), 19);
    }
}
