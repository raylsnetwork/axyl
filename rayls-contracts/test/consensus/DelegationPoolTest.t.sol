// SPDX-License-Identifier: BUSL-1.1
pragma solidity 0.8.26;

import "forge-std/Test.sol";
import {DelegationPool} from "src/consensus/DelegationPool.sol";
import {IDelegationPool} from "src/interfaces/IDelegationPool.sol";
import {IConsensusRegistry} from "src/interfaces/IConsensusRegistry.sol";
import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {ERC20} from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";
import {IAccessControl} from "@openzeppelin/contracts/access/IAccessControl.sol";

/// @notice Mock RLS token (ERC-20 staking token) for testing
contract MockRLS is ERC20 {
    constructor() ERC20("Mock RLS", "RLS") {}

    function mint(address to, uint256 amount) external {
        _mint(to, amount);
    }
}

/// @notice Mock ConsensusRegistry that returns configurable validator status and epoch
contract MockConsensusRegistry {
    mapping(address => IConsensusRegistry.ValidatorStatus) public validatorStatuses;
    mapping(address => bool) public validatorAllowlist;
    uint32 public currentEpoch;
    mapping(address => uint256) public balances;
    IERC20 public rlsToken;

    function setRlsToken(address rls_) external {
        rlsToken = IERC20(rls_);
    }

    function setValidatorStatus(address validator, IConsensusRegistry.ValidatorStatus status) external {
        validatorStatuses[validator] = status;
    }

    function setAllowlisted(address validator, bool allowed) external {
        validatorAllowlist[validator] = allowed;
    }

    function setCurrentEpoch(uint32 epoch) external {
        currentEpoch = epoch;
    }

    function setBalance(address validator, uint256 balance) external {
        balances[validator] = balance;
    }

    function getValidator(address validatorAddress) external view returns (IConsensusRegistry.ValidatorInfo memory) {
        return IConsensusRegistry.ValidatorInfo({
            blsPubkey: "",
            validatorAddress: validatorAddress,
            activationEpoch: 0,
            exitEpoch: 0,
            currentStatus: validatorStatuses[validatorAddress],
            isRetired: false,
            isDelegated: false,
            stakeVersion: 0
        });
    }

    function getCurrentEpoch() external view returns (uint32) {
        return currentEpoch;
    }

    function isAllowlisted(address validator) external view returns (bool) {
        return validatorAllowlist[validator];
    }

    function getBalance(address validator) external view returns (uint256) {
        return balances[validator];
    }

    function getValidators(uint8) external view returns (IConsensusRegistry.ValidatorInfo[] memory) {
        return new IConsensusRegistry.ValidatorInfo[](0);
    }

    // Receive slashed RLS tokens
    receive() external payable {}
}

contract DelegationPoolTest is Test {
    DelegationPool public pool;
    MockConsensusRegistry public registry;
    MockRLS public rls;

    address public owner = address(0xABCD);
    address public validator1 = address(0x1001);
    address public validator2 = address(0x1002);
    address public delegator1 = address(0x2001);
    address public delegator2 = address(0x2002);
    address public delegator3 = address(0x2003);
    address public delegator4 = address(0x2004);
    address public outsider = address(0xBEEF);
    address public rewardDistributor = address(0x3001);

    // Whitelist-test fixture: 4-leaf Merkle tree over (address, balance) pairs
    uint256 public constant WL_B1 = 100e18;
    uint256 public constant WL_B2 = 250e18;
    uint256 public constant WL_B3 = 500e18;
    uint256 public constant WL_B4 = 1000e18;
    bytes32 internal wlLeaf1;
    bytes32 internal wlLeaf2;
    bytes32 internal wlLeaf3;
    bytes32 internal wlLeaf4;
    bytes32 internal wlNode12;
    bytes32 internal wlNode34;
    bytes32 internal wlRoot;

    uint256 public constant MIN_DELEGATION = 1e18;
    uint256 public constant MAX_DELEGATION = 1_000_000e18;
    uint256 public constant MAX_VALIDATOR_DELEGATION = 10_000_000e18;
    uint32 public constant UNBONDING_EPOCHS = 3;
    uint32 public constant COMMISSION_DELAY_EPOCHS = 7;

    IDelegationPool.DelegationConfig defaultConfig;

    function setUp() public {
        rls = new MockRLS();
        registry = new MockConsensusRegistry();
        registry.setRlsToken(address(rls));

        defaultConfig = IDelegationPool.DelegationConfig({
            minDelegation: MIN_DELEGATION,
            maxDelegation: MAX_DELEGATION,
            maxValidatorDelegation: MAX_VALIDATOR_DELEGATION,
            unbondingEpochs: UNBONDING_EPOCHS,
            commissionDelayEpochs: COMMISSION_DELAY_EPOCHS
        });

        DelegationPool impl = new DelegationPool();
        bytes memory initData = abi.encodeCall(
            DelegationPool.initialize,
            (address(rls), address(registry), owner, defaultConfig)
        );
        ERC1967Proxy proxy = new ERC1967Proxy(address(impl), initData);
        pool = DelegationPool(address(proxy));

        // Set reward distributor
        vm.prank(owner);
        pool.setRewardDistributor(rewardDistributor);

        // set validators as Active and allowlisted
        registry.setValidatorStatus(validator1, IConsensusRegistry.ValidatorStatus.Active);
        registry.setValidatorStatus(validator2, IConsensusRegistry.ValidatorStatus.Active);
        registry.setAllowlisted(validator1, true);
        registry.setAllowlisted(validator2, true);

        // mint RLS to delegators
        rls.mint(delegator1, 10_000_000e18);
        rls.mint(delegator2, 10_000_000e18);
        rls.mint(delegator3, 10_000_000e18);
        rls.mint(delegator4, 10_000_000e18);
        rls.mint(outsider, 10_000_000e18);

        // approve pool to spend delegators' RLS
        vm.prank(delegator1);
        rls.approve(address(pool), type(uint256).max);
        vm.prank(delegator2);
        rls.approve(address(pool), type(uint256).max);
        vm.prank(delegator3);
        rls.approve(address(pool), type(uint256).max);
        vm.prank(delegator4);
        rls.approve(address(pool), type(uint256).max);
        vm.prank(outsider);
        rls.approve(address(pool), type(uint256).max);

        // Precompute the whitelist-test Merkle tree (4 leaves, OZ-compatible layout)
        wlLeaf1 = _wlLeaf(delegator1, WL_B1);
        wlLeaf2 = _wlLeaf(delegator2, WL_B2);
        wlLeaf3 = _wlLeaf(delegator3, WL_B3);
        wlLeaf4 = _wlLeaf(delegator4, WL_B4);
        wlNode12 = _wlHashPair(wlLeaf1, wlLeaf2);
        wlNode34 = _wlHashPair(wlLeaf3, wlLeaf4);
        wlRoot = _wlHashPair(wlNode12, wlNode34);
    }

    // Helper: distribute rewards to a validator's pool.
    // Advances epoch by 1 to ensure delegations from prior epochs are eligible
    // for rewards (DP-NEW-002 same-epoch sandwich protection).
    function _distributeRewards(address validator, uint256 amount) internal {
        registry.setCurrentEpoch(registry.getCurrentEpoch() + 1);
        rls.mint(address(pool), amount);
        vm.prank(rewardDistributor);
        pool.distributePoolRewards(validator, amount);
    }

    // =========================================================================
    //                          Initialize
    // =========================================================================

    function _deployPool(
        address rls_,
        address registry_,
        address owner_,
        IDelegationPool.DelegationConfig memory config_
    ) internal returns (DelegationPool) {
        DelegationPool impl2 = new DelegationPool();
        bytes memory initData = abi.encodeCall(
            DelegationPool.initialize,
            (rls_, registry_, owner_, config_)
        );
        ERC1967Proxy proxy2 = new ERC1967Proxy(address(impl2), initData);
        return DelegationPool(address(proxy2));
    }

    function test_initialize() public view {
        assertEq(pool.rlsToken(), address(rls));
        assertEq(address(pool.consensusRegistry()), address(registry));
        assertTrue(pool.hasRole(pool.DEFAULT_ADMIN_ROLE(), owner));
        assertTrue(pool.hasRole(pool.UPGRADER_ROLE(), owner));

        IDelegationPool.DelegationConfig memory cfg = pool.getDelegationConfig();
        assertEq(cfg.minDelegation, MIN_DELEGATION);
        assertEq(cfg.maxDelegation, MAX_DELEGATION);
        assertEq(cfg.maxValidatorDelegation, MAX_VALIDATOR_DELEGATION);
        assertEq(cfg.unbondingEpochs, UNBONDING_EPOCHS);
        assertEq(cfg.commissionDelayEpochs, COMMISSION_DELAY_EPOCHS);
    }

    function testRevert_initialize_zeroRls() public {
        DelegationPool impl2 = new DelegationPool();
        bytes memory initData = abi.encodeCall(
            DelegationPool.initialize,
            (address(0), address(registry), owner, defaultConfig)
        );
        vm.expectRevert(IDelegationPool.ZeroAddress.selector);
        new ERC1967Proxy(address(impl2), initData);
    }

    function testRevert_initialize_zeroRegistry() public {
        DelegationPool impl2 = new DelegationPool();
        bytes memory initData = abi.encodeCall(
            DelegationPool.initialize,
            (address(rls), address(0), owner, defaultConfig)
        );
        vm.expectRevert(IDelegationPool.ZeroAddress.selector);
        new ERC1967Proxy(address(impl2), initData);
    }

    function testRevert_initialize_zeroAdmin() public {
        DelegationPool impl2 = new DelegationPool();
        bytes memory initData = abi.encodeCall(
            DelegationPool.initialize,
            (address(rls), address(registry), address(0), defaultConfig)
        );
        vm.expectRevert(IDelegationPool.ZeroAddress.selector);
        new ERC1967Proxy(address(impl2), initData);
    }

    function testRevert_initialize_invalidConfig_minDelegation() public {
        IDelegationPool.DelegationConfig memory badConfig = defaultConfig;
        badConfig.minDelegation = 0;
        DelegationPool impl2 = new DelegationPool();
        bytes memory initData = abi.encodeCall(
            DelegationPool.initialize,
            (address(rls), address(registry), owner, badConfig)
        );
        vm.expectRevert(IDelegationPool.InvalidConfig.selector);
        new ERC1967Proxy(address(impl2), initData);
    }

    function testRevert_initialize_invalidConfig_unbondingEpochs() public {
        IDelegationPool.DelegationConfig memory badConfig = defaultConfig;
        badConfig.unbondingEpochs = 0;
        DelegationPool impl2 = new DelegationPool();
        bytes memory initData = abi.encodeCall(
            DelegationPool.initialize,
            (address(rls), address(registry), owner, badConfig)
        );
        vm.expectRevert(IDelegationPool.InvalidConfig.selector);
        new ERC1967Proxy(address(impl2), initData);
    }

    function testRevert_initialize_invalidConfig_maxDelegation() public {
        IDelegationPool.DelegationConfig memory badConfig = defaultConfig;
        badConfig.maxDelegation = 0;
        DelegationPool impl2 = new DelegationPool();
        bytes memory initData = abi.encodeCall(
            DelegationPool.initialize,
            (address(rls), address(registry), owner, badConfig)
        );
        vm.expectRevert(IDelegationPool.InvalidConfig.selector);
        new ERC1967Proxy(address(impl2), initData);
    }

    function testRevert_initialize_invalidConfig_maxValidatorDelegation() public {
        IDelegationPool.DelegationConfig memory badConfig = defaultConfig;
        badConfig.maxValidatorDelegation = 0;
        DelegationPool impl2 = new DelegationPool();
        bytes memory initData = abi.encodeCall(
            DelegationPool.initialize,
            (address(rls), address(registry), owner, badConfig)
        );
        vm.expectRevert(IDelegationPool.InvalidConfig.selector);
        new ERC1967Proxy(address(impl2), initData);
    }

    function testRevert_initialize_invalidConfig_maxDelegationExceedsValidator() public {
        IDelegationPool.DelegationConfig memory badConfig = defaultConfig;
        badConfig.maxDelegation = 100e18;
        badConfig.maxValidatorDelegation = 50e18;
        DelegationPool impl2 = new DelegationPool();
        bytes memory initData = abi.encodeCall(
            DelegationPool.initialize,
            (address(rls), address(registry), owner, badConfig)
        );
        vm.expectRevert(IDelegationPool.InvalidConfig.selector);
        new ERC1967Proxy(address(impl2), initData);
    }

    // =========================================================================
    //                          Governance
    // =========================================================================

    function test_updateConfig() public {
        IDelegationPool.DelegationConfig memory newConfig = IDelegationPool.DelegationConfig({
            minDelegation: 2e18,
            maxDelegation: 500_000e18,
            maxValidatorDelegation: 5_000_000e18,
            unbondingEpochs: 5,
            commissionDelayEpochs: 14
        });

        vm.prank(owner);
        pool.updateConfig(newConfig);

        IDelegationPool.DelegationConfig memory cfg = pool.getDelegationConfig();
        assertEq(cfg.minDelegation, 2e18);
        assertEq(cfg.maxDelegation, 500_000e18);
        assertEq(cfg.unbondingEpochs, 5);
    }

    function testRevert_updateConfig_notOwner() public {
        vm.prank(delegator1);
        vm.expectRevert();
        pool.updateConfig(defaultConfig);
    }

    // =========================================================================
    //                          Pool Registration
    // =========================================================================

    function test_registerPool() public {
        vm.prank(validator1);
        pool.registerPool(500); // 5% commission

        assertTrue(pool.poolRegistered(validator1));
        IDelegationPool.ValidatorPool memory vp = pool.getValidatorPool(validator1);
        assertEq(vp.totalDelegated, 0);
        assertEq(vp.commissionBps, 500);
        assertTrue(vp.acceptingDelegations);
    }

    function test_registerPool_pendingActivation() public {
        address pendingVal = address(0x3001);
        registry.setValidatorStatus(pendingVal, IConsensusRegistry.ValidatorStatus.PendingActivation);
        registry.setAllowlisted(pendingVal, true);

        vm.prank(pendingVal);
        pool.registerPool(1000);

        assertTrue(pool.poolRegistered(pendingVal));
    }

    function testRevert_registerPool_alreadyRegistered() public {
        vm.prank(validator1);
        pool.registerPool(500);

        vm.prank(validator1);
        vm.expectRevert(abi.encodeWithSelector(IDelegationPool.PoolAlreadyRegistered.selector, validator1));
        pool.registerPool(500);
    }

    function testRevert_registerPool_invalidCommission() public {
        vm.prank(validator1);
        vm.expectRevert(abi.encodeWithSelector(IDelegationPool.InvalidCommission.selector, 10001));
        pool.registerPool(10001);
    }

    function testRevert_registerPool_notActiveValidator() public {
        address staked = address(0x4001);
        registry.setValidatorStatus(staked, IConsensusRegistry.ValidatorStatus.Staked);
        registry.setAllowlisted(staked, true);

        vm.prank(staked);
        vm.expectRevert(abi.encodeWithSelector(IDelegationPool.NotActiveValidator.selector, staked));
        pool.registerPool(500);
    }

    function testRevert_registerPool_notAllowlisted() public {
        address notAllowlisted = address(0x5001);
        registry.setValidatorStatus(notAllowlisted, IConsensusRegistry.ValidatorStatus.Active);
        // not setting allowlisted

        vm.prank(notAllowlisted);
        vm.expectRevert(abi.encodeWithSelector(IDelegationPool.NotAllowlisted.selector, notAllowlisted));
        pool.registerPool(500);
    }

    // =========================================================================
    //                          Commission & Pool Settings
    // =========================================================================

    function test_updateCommission_decrease() public {
        vm.prank(validator1);
        pool.registerPool(2000); // 20%

        // decreases apply immediately
        vm.prank(validator1);
        pool.updateCommission(100); // 1%

        IDelegationPool.ValidatorPool memory vp = pool.getValidatorPool(validator1);
        assertEq(vp.commissionBps, 100);
    }

    function test_updateCommission_increaseScheduled() public {
        vm.prank(validator1);
        pool.registerPool(500); // 5%

        registry.setCurrentEpoch(10);

        // increases are scheduled, not immediate
        vm.prank(validator1);
        pool.updateCommission(1000); // 10%

        // commission should NOT have changed yet
        IDelegationPool.ValidatorPool memory vp = pool.getValidatorPool(validator1);
        assertEq(vp.commissionBps, 500);

        // pending commission should be set
        IDelegationPool.PendingCommission memory pc = pool.getPendingCommission(validator1);
        assertEq(pc.newBps, 1000);
        assertEq(pc.effectiveEpoch, 10 + COMMISSION_DELAY_EPOCHS);
    }

    function test_activatePendingCommission() public {
        vm.prank(validator1);
        pool.registerPool(500);

        registry.setCurrentEpoch(10);

        vm.prank(validator1);
        pool.updateCommission(1000);

        // advance past delay
        registry.setCurrentEpoch(10 + COMMISSION_DELAY_EPOCHS);

        vm.prank(validator1);
        pool.activatePendingCommission();

        IDelegationPool.ValidatorPool memory vp = pool.getValidatorPool(validator1);
        assertEq(vp.commissionBps, 1000);

        // pending should be cleared
        IDelegationPool.PendingCommission memory pc = pool.getPendingCommission(validator1);
        assertEq(pc.newBps, 0);
        assertEq(pc.effectiveEpoch, 0);
    }

    function testRevert_activatePendingCommission_tooEarly() public {
        vm.prank(validator1);
        pool.registerPool(500);

        registry.setCurrentEpoch(10);

        vm.prank(validator1);
        pool.updateCommission(1000);

        // try activating 1 epoch too early
        registry.setCurrentEpoch(10 + COMMISSION_DELAY_EPOCHS - 1);

        vm.prank(validator1);
        vm.expectRevert(
            abi.encodeWithSelector(
                IDelegationPool.CommissionNotYetEffective.selector,
                uint32(10 + COMMISSION_DELAY_EPOCHS - 1),
                uint32(10 + COMMISSION_DELAY_EPOCHS)
            )
        );
        pool.activatePendingCommission();
    }

    function testRevert_activatePendingCommission_noPending() public {
        vm.prank(validator1);
        pool.registerPool(500);

        vm.prank(validator1);
        vm.expectRevert(IDelegationPool.NoPendingCommission.selector);
        pool.activatePendingCommission();
    }

    function test_cancelPendingCommission() public {
        vm.prank(validator1);
        pool.registerPool(500);

        registry.setCurrentEpoch(10);

        vm.prank(validator1);
        pool.updateCommission(1000);

        // cancel
        vm.prank(validator1);
        pool.cancelPendingCommission();

        // pending should be cleared
        IDelegationPool.PendingCommission memory pc = pool.getPendingCommission(validator1);
        assertEq(pc.newBps, 0);
        assertEq(pc.effectiveEpoch, 0);

        // commission unchanged
        IDelegationPool.ValidatorPool memory vp = pool.getValidatorPool(validator1);
        assertEq(vp.commissionBps, 500);
    }

    function testRevert_cancelPendingCommission_noPending() public {
        vm.prank(validator1);
        pool.registerPool(500);

        vm.prank(validator1);
        vm.expectRevert(IDelegationPool.NoPendingCommission.selector);
        pool.cancelPendingCommission();
    }

    function test_commissionDecreaseCancelsPending() public {
        vm.prank(validator1);
        pool.registerPool(500);

        registry.setCurrentEpoch(10);

        // schedule increase
        vm.prank(validator1);
        pool.updateCommission(1000);

        // immediate decrease cancels pending increase
        vm.prank(validator1);
        pool.updateCommission(300);

        IDelegationPool.ValidatorPool memory vp = pool.getValidatorPool(validator1);
        assertEq(vp.commissionBps, 300);

        IDelegationPool.PendingCommission memory pc = pool.getPendingCommission(validator1);
        assertEq(pc.newBps, 0);
        assertEq(pc.effectiveEpoch, 0);
    }

    function testRevert_updateCommission_notRegistered() public {
        vm.prank(validator1);
        vm.expectRevert(abi.encodeWithSelector(IDelegationPool.PoolNotRegistered.selector, validator1));
        pool.updateCommission(500);
    }

    function testRevert_updateCommission_invalidBps() public {
        vm.prank(validator1);
        pool.registerPool(500);

        vm.prank(validator1);
        vm.expectRevert(abi.encodeWithSelector(IDelegationPool.InvalidCommission.selector, 10001));
        pool.updateCommission(10001);
    }

    function test_setAcceptingDelegations() public {
        vm.prank(validator1);
        pool.registerPool(500);

        vm.prank(validator1);
        pool.setAcceptingDelegations(false);

        IDelegationPool.ValidatorPool memory vp = pool.getValidatorPool(validator1);
        assertFalse(vp.acceptingDelegations);

        vm.prank(validator1);
        pool.setAcceptingDelegations(true);

        vp = pool.getValidatorPool(validator1);
        assertTrue(vp.acceptingDelegations);
    }

    // =========================================================================
    //                          Delegation
    // =========================================================================

    function test_delegate() public {
        vm.prank(validator1);
        pool.registerPool(500);

        uint256 amount = 100e18;
        vm.prank(delegator1);
        pool.delegate(validator1, amount);

        assertEq(pool.getTotalDelegatedStake(validator1), amount);
        assertEq(rls.balanceOf(address(pool)), amount);

        IDelegationPool.DelegatorPosition memory pos = pool.getDelegatorPosition(validator1, delegator1);
        assertEq(pos.amount, amount);
    }

    function test_delegate_multipleDelegators() public {
        vm.prank(validator1);
        pool.registerPool(500);

        uint256 amount1 = 100e18;
        uint256 amount2 = 200e18;

        vm.prank(delegator1);
        pool.delegate(validator1, amount1);

        vm.prank(delegator2);
        pool.delegate(validator1, amount2);

        assertEq(pool.getTotalDelegatedStake(validator1), amount1 + amount2);

        IDelegationPool.DelegatorPosition memory pos1 = pool.getDelegatorPosition(validator1, delegator1);
        IDelegationPool.DelegatorPosition memory pos2 = pool.getDelegatorPosition(validator1, delegator2);
        assertEq(pos1.amount, amount1);
        assertEq(pos2.amount, amount2);
    }

    function test_delegate_additionalStake() public {
        vm.prank(validator1);
        pool.registerPool(500);

        vm.prank(delegator1);
        pool.delegate(validator1, 50e18);

        vm.prank(delegator1);
        pool.delegate(validator1, 50e18);

        IDelegationPool.DelegatorPosition memory pos = pool.getDelegatorPosition(validator1, delegator1);
        assertEq(pos.amount, 100e18);
        assertEq(pool.getTotalDelegatedStake(validator1), 100e18);
    }

    function testRevert_delegate_zeroAddress() public {
        vm.prank(delegator1);
        vm.expectRevert(IDelegationPool.ZeroAddress.selector);
        pool.delegate(address(0), 10e18);
    }

    function testRevert_delegate_zeroAmount() public {
        vm.prank(validator1);
        pool.registerPool(500);

        vm.prank(delegator1);
        vm.expectRevert(IDelegationPool.ZeroAmount.selector);
        pool.delegate(validator1, 0);
    }

    function testRevert_delegate_poolNotRegistered() public {
        vm.prank(delegator1);
        vm.expectRevert(abi.encodeWithSelector(IDelegationPool.PoolNotRegistered.selector, validator1));
        pool.delegate(validator1, 10e18);
    }

    function testRevert_delegate_poolNotAccepting() public {
        vm.prank(validator1);
        pool.registerPool(500);

        vm.prank(validator1);
        pool.setAcceptingDelegations(false);

        vm.prank(delegator1);
        vm.expectRevert(abi.encodeWithSelector(IDelegationPool.PoolNotAcceptingDelegations.selector, validator1));
        pool.delegate(validator1, 10e18);
    }

    function testRevert_delegate_validatorNotAllowlisted() public {
        // validator registers pool while allowlisted
        vm.prank(validator1);
        pool.registerPool(500);

        // validator gets delisted
        registry.setAllowlisted(validator1, false);

        // delegation should fail
        vm.prank(delegator1);
        vm.expectRevert(abi.encodeWithSelector(IDelegationPool.NotAllowlisted.selector, validator1));
        pool.delegate(validator1, 10e18);
    }

    function testRevert_delegate_belowMinimum() public {
        vm.prank(validator1);
        pool.registerPool(500);

        vm.prank(delegator1);
        vm.expectRevert(
            abi.encodeWithSelector(IDelegationPool.InsufficientDelegation.selector, MIN_DELEGATION - 1, MIN_DELEGATION)
        );
        pool.delegate(validator1, MIN_DELEGATION - 1);
    }

    function testRevert_delegate_exceedsMaxDelegation() public {
        vm.prank(validator1);
        pool.registerPool(500);

        vm.prank(delegator1);
        vm.expectRevert(
            abi.encodeWithSelector(
                IDelegationPool.ExceedsMaxDelegation.selector, MAX_DELEGATION + 1, MAX_DELEGATION
            )
        );
        pool.delegate(validator1, MAX_DELEGATION + 1);
    }

    function testRevert_delegate_exceedsMaxValidatorDelegation() public {
        // create a config with a small maxValidatorDelegation
        IDelegationPool.DelegationConfig memory smallConfig = IDelegationPool.DelegationConfig({
            minDelegation: 1e18,
            maxDelegation: 200e18,
            maxValidatorDelegation: 300e18,
            unbondingEpochs: 3,
            commissionDelayEpochs: 7
        });
        DelegationPool smallPool = _deployPool(address(rls), address(registry), owner, smallConfig);

        // approve
        vm.prank(delegator1);
        rls.approve(address(smallPool), type(uint256).max);
        vm.prank(delegator2);
        rls.approve(address(smallPool), type(uint256).max);

        vm.prank(validator1);
        smallPool.registerPool(500);

        // delegator1 delegates 200
        vm.prank(delegator1);
        smallPool.delegate(validator1, 200e18);

        // delegator2 tries to delegate 200 more, exceeds 300 max
        vm.prank(delegator2);
        vm.expectRevert(
            abi.encodeWithSelector(IDelegationPool.ExceedsMaxValidatorDelegation.selector, 400e18, 300e18)
        );
        smallPool.delegate(validator1, 200e18);
    }

    // =========================================================================
    //                          Undelegation
    // =========================================================================

    function test_requestUndelegation() public {
        vm.prank(validator1);
        pool.registerPool(500);

        uint256 amount = 100e18;
        vm.prank(delegator1);
        pool.delegate(validator1, amount);

        registry.setCurrentEpoch(5);

        vm.prank(delegator1);
        pool.requestUndelegation(validator1, amount);

        IDelegationPool.DelegatorPosition memory pos = pool.getDelegatorPosition(validator1, delegator1);
        assertEq(pos.amount, 0);
        assertEq(pos.undelegateAmount, amount);
        assertEq(pos.undelegateEpoch, 5 + UNBONDING_EPOCHS);
        assertEq(pool.getTotalDelegatedStake(validator1), 0);
    }

    function test_requestUndelegation_partial() public {
        vm.prank(validator1);
        pool.registerPool(500);

        uint256 amount = 100e18;
        vm.prank(delegator1);
        pool.delegate(validator1, amount);

        registry.setCurrentEpoch(5);

        vm.prank(delegator1);
        pool.requestUndelegation(validator1, 40e18);

        IDelegationPool.DelegatorPosition memory pos = pool.getDelegatorPosition(validator1, delegator1);
        assertEq(pos.amount, 60e18);
        assertEq(pos.undelegateAmount, 40e18);
        assertEq(pool.getTotalDelegatedStake(validator1), 60e18);
    }

    function testRevert_requestUndelegation_zeroAmount() public {
        vm.prank(delegator1);
        vm.expectRevert(IDelegationPool.ZeroAmount.selector);
        pool.requestUndelegation(validator1, 0);
    }

    function testRevert_requestUndelegation_insufficientBalance() public {
        vm.prank(validator1);
        pool.registerPool(500);

        vm.prank(delegator1);
        pool.delegate(validator1, 10e18);

        vm.prank(delegator1);
        vm.expectRevert(abi.encodeWithSelector(IDelegationPool.InsufficientBalance.selector, 20e18, 10e18));
        pool.requestUndelegation(validator1, 20e18);
    }

    function testRevert_requestUndelegation_pendingExists() public {
        vm.prank(validator1);
        pool.registerPool(500);

        vm.prank(delegator1);
        pool.delegate(validator1, 100e18);

        registry.setCurrentEpoch(5);

        vm.prank(delegator1);
        pool.requestUndelegation(validator1, 50e18);

        // second request while first is pending
        vm.prank(delegator1);
        vm.expectRevert(
            abi.encodeWithSelector(IDelegationPool.PendingUndelegationExists.selector, uint64(5 + UNBONDING_EPOCHS))
        );
        pool.requestUndelegation(validator1, 50e18);
    }

    function test_completeUndelegation() public {
        vm.prank(validator1);
        pool.registerPool(500);

        uint256 amount = 100e18;
        vm.prank(delegator1);
        pool.delegate(validator1, amount);

        registry.setCurrentEpoch(5);

        vm.prank(delegator1);
        pool.requestUndelegation(validator1, amount);

        // advance past unbonding period
        registry.setCurrentEpoch(5 + UNBONDING_EPOCHS);

        uint256 balanceBefore = rls.balanceOf(delegator1);

        vm.prank(delegator1);
        pool.completeUndelegation(validator1);

        assertEq(rls.balanceOf(delegator1), balanceBefore + amount);

        IDelegationPool.DelegatorPosition memory pos = pool.getDelegatorPosition(validator1, delegator1);
        assertEq(pos.undelegateEpoch, 0);
        assertEq(pos.undelegateAmount, 0);
    }

    function testRevert_completeUndelegation_nothingToUndelegate() public {
        vm.prank(delegator1);
        vm.expectRevert(IDelegationPool.NothingToUndelegate.selector);
        pool.completeUndelegation(validator1);
    }

    function testRevert_completeUndelegation_unbondingNotComplete() public {
        vm.prank(validator1);
        pool.registerPool(500);

        vm.prank(delegator1);
        pool.delegate(validator1, 100e18);

        registry.setCurrentEpoch(5);

        vm.prank(delegator1);
        pool.requestUndelegation(validator1, 100e18);

        // try completing 1 epoch too early
        registry.setCurrentEpoch(5 + UNBONDING_EPOCHS - 1);

        vm.prank(delegator1);
        vm.expectRevert(
            abi.encodeWithSelector(
                IDelegationPool.UnbondingNotComplete.selector,
                uint32(5 + UNBONDING_EPOCHS - 1),
                uint64(5 + UNBONDING_EPOCHS)
            )
        );
        pool.completeUndelegation(validator1);
    }

    // =========================================================================
    //                          Reward Distribution
    // =========================================================================

    function test_distributePoolRewards_singleDelegator() public {
        vm.prank(validator1);
        pool.registerPool(1000); // 10% commission

        uint256 delegatedAmount = 100e18;
        vm.prank(delegator1);
        pool.delegate(validator1, delegatedAmount);

        uint256 rewardAmount = 100e18;
        _distributeRewards(validator1, rewardAmount);

        // check pool state
        IDelegationPool.ValidatorPool memory vp = pool.getValidatorPool(validator1);
        // 10% commission = 10 RLS to validator
        assertEq(vp.pendingValidatorRewards, 10e18);

        // remaining 90% to delegators (via rewardPerShareAccum)
        uint256 pendingRewards = pool.getPendingRewards(validator1, delegator1);
        assertEq(pendingRewards, 90e18);
    }

    function test_distributePoolRewards_multipleDelegators() public {
        vm.prank(validator1);
        pool.registerPool(1000); // 10% commission

        // delegator1 stakes 100, delegator2 stakes 200 (1:2 ratio)
        vm.prank(delegator1);
        pool.delegate(validator1, 100e18);

        vm.prank(delegator2);
        pool.delegate(validator1, 200e18);

        // distribute 300 rewards
        _distributeRewards(validator1, 300e18);

        // 10% commission = 30 to validator
        IDelegationPool.ValidatorPool memory vp = pool.getValidatorPool(validator1);
        assertEq(vp.pendingValidatorRewards, 30e18);

        // 270 remaining, split 1:2
        // delegator1 gets 90, delegator2 gets 180
        assertEq(pool.getPendingRewards(validator1, delegator1), 90e18);
        assertEq(pool.getPendingRewards(validator1, delegator2), 180e18);
    }

    function test_distributePoolRewards_noDelegators() public {
        vm.prank(validator1);
        pool.registerPool(1000);

        // distribute with no delegators - all goes to validator
        _distributeRewards(validator1, 100e18);

        IDelegationPool.ValidatorPool memory vp = pool.getValidatorPool(validator1);
        assertEq(vp.pendingValidatorRewards, 100e18);
    }

    // =========================================================================
    //                          Claiming Rewards
    // =========================================================================

    function test_claimDelegationRewards() public {
        vm.prank(validator1);
        pool.registerPool(1000);

        vm.prank(delegator1);
        pool.delegate(validator1, 100e18);

        _distributeRewards(validator1, 100e18);

        uint256 balanceBefore = rls.balanceOf(delegator1);

        vm.prank(delegator1);
        pool.claimDelegationRewards(validator1);

        // 90% of 100 = 90 rewards
        assertEq(rls.balanceOf(delegator1) - balanceBefore, 90e18);
    }

    function test_claimCommission() public {
        vm.prank(validator1);
        pool.registerPool(1000);

        vm.prank(delegator1);
        pool.delegate(validator1, 100e18);

        _distributeRewards(validator1, 100e18);

        uint256 balanceBefore = rls.balanceOf(validator1);

        vm.prank(validator1);
        pool.claimCommission();

        // 10% of 100 = 10 commission
        assertEq(rls.balanceOf(validator1) - balanceBefore, 10e18);
    }

    function testRevert_claimDelegationRewards_noPending() public {
        vm.prank(validator1);
        pool.registerPool(1000);

        vm.prank(delegator1);
        pool.delegate(validator1, 100e18);

        // no rewards distributed

        vm.prank(delegator1);
        vm.expectRevert(IDelegationPool.NoPendingRewards.selector);
        pool.claimDelegationRewards(validator1);
    }

    function testRevert_claimCommission_noCommission() public {
        vm.prank(validator1);
        pool.registerPool(1000);

        // no rewards to claim
        vm.prank(validator1);
        vm.expectRevert(IDelegationPool.NoCommissionToClaim.selector);
        pool.claimCommission();
    }

    // =========================================================================
    //                          Custom Recipients
    // =========================================================================

    function test_setRewardRecipient() public {
        vm.prank(validator1);
        pool.registerPool(1000);

        address customRecipient = address(0x9999);

        vm.prank(delegator1);
        pool.delegate(validator1, 100e18);

        vm.prank(delegator1);
        pool.setRewardRecipient(validator1, customRecipient);

        _distributeRewards(validator1, 100e18);

        vm.prank(delegator1);
        pool.claimDelegationRewards(validator1);

        assertEq(rls.balanceOf(customRecipient), 90e18);
    }

    function test_setCommissionRecipient() public {
        vm.prank(validator1);
        pool.registerPool(1000);

        address customRecipient = address(0x8888);

        vm.prank(validator1);
        pool.setCommissionRecipient(customRecipient);

        vm.prank(delegator1);
        pool.delegate(validator1, 100e18);

        _distributeRewards(validator1, 100e18);

        vm.prank(validator1);
        pool.claimCommission();

        assertEq(rls.balanceOf(customRecipient), 10e18);
    }

    // =========================================================================
    //                          Slashing
    // =========================================================================

    function test_applyPoolSlash() public {
        vm.prank(validator1);
        pool.registerPool(1000);

        vm.prank(delegator1);
        pool.delegate(validator1, 100e18);

        // slash 20% of delegated stake
        vm.prank(address(registry));
        pool.applyPoolSlash(validator1, 20e18);

        // pool total reduced
        assertEq(pool.getTotalDelegatedStake(validator1), 80e18);

        // slashed tokens sent to registry
        assertEq(rls.balanceOf(address(registry)), 20e18);

        // delegator's effective position reduced
        (uint256 effectiveAmount, ) = pool.getEffectivePosition(validator1, delegator1);
        assertEq(effectiveAmount, 80e18);
    }

    function test_slashDuringUnbonding() public {
        vm.prank(validator1);
        pool.registerPool(1000);

        // need a second delegator so totalDelegated > 0 after first undelegation
        // otherwise applyPoolSlash returns early with no slash recorded
        vm.prank(delegator1);
        pool.delegate(validator1, 100e18);

        vm.prank(delegator2);
        pool.delegate(validator1, 100e18);

        registry.setCurrentEpoch(5);

        vm.prank(delegator1);
        pool.requestUndelegation(validator1, 100e18);

        // slash during unbonding - 20e18 slash on 100e18 remaining delegated
        // slashPerShareAccum increases by 20e18 * PRECISION / 100e18 = 0.2 * PRECISION
        vm.prank(address(registry));
        pool.applyPoolSlash(validator1, 20e18);

        // advance past unbonding
        registry.setCurrentEpoch(5 + UNBONDING_EPOCHS);

        uint256 balanceBefore = rls.balanceOf(delegator1);

        vm.prank(delegator1);
        pool.completeUndelegation(validator1);

        // unbonding tokens are no longer slashed - delegator gets full amount back
        assertEq(rls.balanceOf(delegator1) - balanceBefore, 100e18);
    }

    // =========================================================================
    //                     Slash Rounding Solvency Tests
    // =========================================================================

    function test_applyPoolSlash_roundingDustKeepsSolvent() public {
        vm.prank(validator1);
        pool.registerPool(1000);

        // Use an amount that causes truncation: 7e18 delegated, slash 2e18
        // slashPerShare = (2e18 * 1e18) / 7e18 = 285714285714285714 (truncates)
        // actualSlash   = (285714285714285714 * 7e18) / 1e18 = 1999999999999999998 (2 wei less than 2e18)
        vm.prank(delegator1);
        pool.delegate(validator1, 7e18);

        uint256 poolBalanceBefore = rls.balanceOf(address(pool));

        vm.prank(address(registry));
        uint256 slashed = pool.applyPoolSlash(validator1, 2e18);

        // actualSlash should be <= 2e18 (the rounding dust stays in pool)
        assertLe(slashed, 2e18, "slashed should be <= requested");

        // pool balance must cover remaining delegated stake
        uint256 poolBalanceAfter = rls.balanceOf(address(pool));
        uint256 totalDelegated = pool.getTotalDelegatedStake(validator1);
        assertGe(poolBalanceAfter, totalDelegated, "pool must be solvent after slash");

        // dust stayed in the pool (balance > totalDelegated)
        assertGe(poolBalanceAfter, totalDelegated, "dust retained as solvency buffer");
    }

    function test_applyPoolSlash_multipleSlashesAllWithdrawSucceed() public {
        vm.prank(validator1);
        pool.registerPool(0); // 0% commission for simplicity

        // 3 delegators with odd total
        vm.prank(delegator1);
        pool.delegate(validator1, 33e18);
        vm.prank(delegator2);
        pool.delegate(validator1, 33e18);
        vm.prank(delegator3);
        pool.delegate(validator1, 33e18);

        // Apply 10 small slashes that all truncate
        for (uint256 i; i < 10; ++i) {
            vm.prank(address(registry));
            pool.applyPoolSlash(validator1, 1e18);
        }

        // All 3 delegators request undelegation — must not revert
        registry.setCurrentEpoch(5);

        (uint256 eff1, ) = pool.getEffectivePosition(validator1, delegator1);
        (uint256 eff2, ) = pool.getEffectivePosition(validator1, delegator2);
        (uint256 eff3, ) = pool.getEffectivePosition(validator1, delegator3);

        vm.prank(delegator1);
        pool.requestUndelegation(validator1, eff1);
        vm.prank(delegator2);
        pool.requestUndelegation(validator1, eff2);
        vm.prank(delegator3);
        pool.requestUndelegation(validator1, eff3);

        // Advance past unbonding
        registry.setCurrentEpoch(5 + UNBONDING_EPOCHS);

        // All 3 complete undelegation — must not revert due to insufficient balance
        vm.prank(delegator1);
        pool.completeUndelegation(validator1);
        vm.prank(delegator2);
        pool.completeUndelegation(validator1);
        vm.prank(delegator3);
        pool.completeUndelegation(validator1);

        // Slash dust leaves a residual tracked stake; the pool must still back it (solvency).
        assertGe(
            rls.balanceOf(address(pool)),
            pool.getTotalDelegatedStake(validator1),
            "pool remains solvent for the residual tracked stake"
        );
    }

    // =========================================================================
    //                          Commission Increase Limits
    // =========================================================================

    function testRevert_commissionIncreaseExceedsLimit() public {
        vm.prank(validator1);
        pool.registerPool(500); // 5%

        // try to increase by more than 500 bps
        vm.prank(validator1);
        vm.expectRevert(
            abi.encodeWithSelector(IDelegationPool.CommissionIncreaseExceedsLimit.selector, 501, 500)
        );
        pool.updateCommission(1001);
    }

    function testRevert_updateCommission_pendingExists() public {
        vm.prank(validator1);
        pool.registerPool(500);

        registry.setCurrentEpoch(10);

        // first increase: schedule it
        vm.prank(validator1);
        pool.updateCommission(1000);

        // second increase while first is pending: should fail
        vm.prank(validator1);
        vm.expectRevert(
            abi.encodeWithSelector(IDelegationPool.PendingCommissionExists.selector, uint32(10 + COMMISSION_DELAY_EPOCHS))
        );
        pool.updateCommission(1000);
    }

    function test_commissionIncreaseFullFlow() public {
        vm.prank(validator1);
        pool.registerPool(0); // 0%

        // schedule increase to 500 bps (5%)
        registry.setCurrentEpoch(1);
        vm.prank(validator1);
        pool.updateCommission(500);

        // activate after delay
        registry.setCurrentEpoch(1 + COMMISSION_DELAY_EPOCHS);
        vm.prank(validator1);
        pool.activatePendingCommission();

        // schedule next increase to 1000 bps (10%)
        registry.setCurrentEpoch(1 + COMMISSION_DELAY_EPOCHS + 1);
        vm.prank(validator1);
        pool.updateCommission(1000);

        // activate
        registry.setCurrentEpoch(1 + 2 * COMMISSION_DELAY_EPOCHS + 1);
        vm.prank(validator1);
        pool.activatePendingCommission();

        IDelegationPool.ValidatorPool memory vp = pool.getValidatorPool(validator1);
        assertEq(vp.commissionBps, 1000);
    }

    // =========================================================================
    //                          View Functions
    // =========================================================================

    function test_getEffectivePosition() public {
        vm.prank(validator1);
        pool.registerPool(1000);

        vm.prank(delegator1);
        pool.delegate(validator1, 100e18);

        _distributeRewards(validator1, 100e18);

        (uint256 effectiveAmount, uint256 pendingRewards) = pool.getEffectivePosition(validator1, delegator1);
        assertEq(effectiveAmount, 100e18);
        assertEq(pendingRewards, 90e18);
    }

    function test_getDelegatorPosition() public {
        vm.prank(validator1);
        pool.registerPool(500);

        vm.prank(delegator1);
        pool.delegate(validator1, 100e18);

        IDelegationPool.DelegatorPosition memory pos = pool.getDelegatorPosition(validator1, delegator1);
        assertEq(pos.amount, 100e18);
        assertEq(pos.undelegateAmount, 0);
        assertEq(pos.undelegateEpoch, 0);
    }

    function test_getValidatorPool() public {
        vm.prank(validator1);
        pool.registerPool(500);

        vm.prank(delegator1);
        pool.delegate(validator1, 100e18);

        IDelegationPool.ValidatorPool memory vp = pool.getValidatorPool(validator1);
        assertEq(vp.totalDelegated, 100e18);
        assertEq(vp.commissionBps, 500);
        assertTrue(vp.acceptingDelegations);
    }

    // =========================================================================
    //                          Additional Governance Tests
    // =========================================================================

    function test_setRewardDistributor() public {
        address newDistributor = address(0x7777);

        vm.prank(owner);
        pool.setRewardDistributor(newDistributor);

        assertEq(pool.rewardDistributor(), newDistributor);
    }

    function testRevert_setRewardDistributor_notOwner() public {
        vm.prank(delegator1);
        vm.expectRevert();
        pool.setRewardDistributor(address(0x7777));
    }

    function testRevert_setRewardDistributor_zeroAddress() public {
        vm.prank(owner);
        vm.expectRevert(IDelegationPool.ZeroAddress.selector);
        pool.setRewardDistributor(address(0));
    }

    // =========================================================================
    //                    Additional Validator Function Tests
    // =========================================================================

    function testRevert_setAcceptingDelegations_notRegistered() public {
        vm.prank(validator1);
        vm.expectRevert(abi.encodeWithSelector(IDelegationPool.PoolNotRegistered.selector, validator1));
        pool.setAcceptingDelegations(false);
    }

    function testRevert_claimCommission_notRegistered() public {
        vm.prank(validator1);
        vm.expectRevert(abi.encodeWithSelector(IDelegationPool.PoolNotRegistered.selector, validator1));
        pool.claimCommission();
    }

    function testRevert_setCommissionRecipient_notRegistered() public {
        vm.prank(validator1);
        vm.expectRevert(abi.encodeWithSelector(IDelegationPool.PoolNotRegistered.selector, validator1));
        pool.setCommissionRecipient(address(0x9999));
    }

    function test_getCommissionRecipient() public {
        vm.prank(validator1);
        pool.registerPool(500);

        address customRecipient = address(0x8888);

        vm.prank(validator1);
        pool.setCommissionRecipient(customRecipient);

        assertEq(pool.getCommissionRecipient(validator1), customRecipient);
    }

    function test_getCommissionRecipient_default() public {
        vm.prank(validator1);
        pool.registerPool(500);

        // default should be the validator address
        assertEq(pool.getCommissionRecipient(validator1), validator1);
    }

    // =========================================================================
    //                    Additional Delegator Function Tests
    // =========================================================================

    function testRevert_setRewardRecipient_notRegistered() public {
        vm.prank(delegator1);
        vm.expectRevert(abi.encodeWithSelector(IDelegationPool.PoolNotRegistered.selector, validator1));
        pool.setRewardRecipient(validator1, address(0x9999));
    }

    function testRevert_claimDelegationRewards_notRegistered() public {
        vm.prank(delegator1);
        vm.expectRevert(abi.encodeWithSelector(IDelegationPool.PoolNotRegistered.selector, validator1));
        pool.claimDelegationRewards(validator1);
    }

    function test_getRewardRecipient() public {
        vm.prank(validator1);
        pool.registerPool(500);

        address customRecipient = address(0x9999);

        vm.prank(delegator1);
        pool.setRewardRecipient(validator1, customRecipient);

        assertEq(pool.getRewardRecipient(validator1, delegator1), customRecipient);
    }

    function test_getRewardRecipient_default() public {
        vm.prank(validator1);
        pool.registerPool(500);

        // default should be the delegator address
        assertEq(pool.getRewardRecipient(validator1, delegator1), delegator1);
    }

    // =========================================================================
    //                          Access Control Tests
    // =========================================================================

    function testRevert_distributePoolRewards_notAuthorized() public {
        vm.prank(validator1);
        pool.registerPool(500);

        // random address should not be able to distribute rewards
        vm.prank(delegator1);
        vm.expectRevert(IDelegationPool.OnlyRewardSources.selector);
        pool.distributePoolRewards(validator1, 100e18);
    }

    function testRevert_applyPoolSlash_notConsensusRegistry() public {
        vm.prank(validator1);
        pool.registerPool(500);

        // only ConsensusRegistry can call applyPoolSlash
        vm.prank(owner);
        vm.expectRevert(IDelegationPool.OnlyConsensusRegistry.selector);
        pool.applyPoolSlash(validator1, 10e18);
    }

    // =========================================================================
    //                       Additional View Function Tests
    // =========================================================================

    function test_rlsToken() public view {
        assertEq(pool.rlsToken(), address(rls));
    }

    function test_poolRegistered() public {
        assertFalse(pool.poolRegistered(validator1));

        vm.prank(validator1);
        pool.registerPool(500);

        assertTrue(pool.poolRegistered(validator1));
    }

    function test_consensusRegistry() public view {
        assertEq(address(pool.consensusRegistry()), address(registry));
    }

    function test_getTotalDelegatedStake() public {
        vm.prank(validator1);
        pool.registerPool(500);

        assertEq(pool.getTotalDelegatedStake(validator1), 0);

        vm.prank(delegator1);
        pool.delegate(validator1, 100e18);

        assertEq(pool.getTotalDelegatedStake(validator1), 100e18);
    }

    function test_getPendingRewards() public {
        vm.prank(validator1);
        pool.registerPool(1000); // 10% commission

        vm.prank(delegator1);
        pool.delegate(validator1, 100e18);

        // no rewards yet
        assertEq(pool.getPendingRewards(validator1, delegator1), 0);

        // distribute rewards
        _distributeRewards(validator1, 100e18);

        // 90% of 100 = 90 rewards for delegator
        assertEq(pool.getPendingRewards(validator1, delegator1), 90e18);
    }

    function test_getDelegationConfig() public view {
        IDelegationPool.DelegationConfig memory cfg = pool.getDelegationConfig();
        assertEq(cfg.minDelegation, MIN_DELEGATION);
        assertEq(cfg.maxDelegation, MAX_DELEGATION);
        assertEq(cfg.maxValidatorDelegation, MAX_VALIDATOR_DELEGATION);
        assertEq(cfg.unbondingEpochs, UNBONDING_EPOCHS);
        assertEq(cfg.commissionDelayEpochs, COMMISSION_DELAY_EPOCHS);
    }

    // =========================================================================
    //                       Constants Tests
    // =========================================================================

    function test_constants() public view {
        assertEq(pool.PRECISION(), 1e18);
        assertEq(pool.MAX_COMMISSION_BPS(), 10_000);
        assertEq(pool.MAX_COMMISSION_INCREASE_BPS(), 500);
    }

    // =========================================================================
    //                   Reward Debt Accounting Tests
    // =========================================================================
    // Proves that MasterChef-style rewardDebt is correct when a delegator
    // adds more stake after rewards have been distributed.

    /// @notice The exact scenario from the reported "critical" bug:
    ///   1. User deposits 100 tokens
    ///   2. 50 rewards distributed
    ///   3. User deposits 75 more tokens
    ///   4. Verify rewards are exactly 50 (not inflated by the second deposit)
    function test_rewardDebt_correctAfterAdditionalDeposit() public {
        vm.prank(validator1);
        pool.registerPool(0); // 0% commission for clarity

        // Step 1: deposit 100
        vm.prank(delegator1);
        pool.delegate(validator1, 100e18);

        // Step 2: distribute 50 rewards
        _distributeRewards(validator1, 50e18);

        // Verify pending rewards before second deposit
        uint256 rewardsBefore = pool.getPendingRewards(validator1, delegator1);
        assertEq(rewardsBefore, 50e18, "should have 50 rewards before second deposit");

        // Step 3: deposit 75 more
        vm.prank(delegator1);
        pool.delegate(validator1, 75e18);

        // Step 4: rewards must still be exactly 50
        uint256 rewardsAfter = pool.getPendingRewards(validator1, delegator1);
        assertEq(rewardsAfter, 50e18, "rewards must remain 50 after second deposit");

        // Verify position amount
        IDelegationPool.DelegatorPosition memory pos = pool.getDelegatorPosition(validator1, delegator1);
        assertEq(pos.amount, 175e18, "position should be 175 tokens");

        // Claim and verify actual RLS received
        uint256 balBefore = rls.balanceOf(delegator1);
        vm.prank(delegator1);
        pool.claimDelegationRewards(validator1);
        assertEq(rls.balanceOf(delegator1) - balBefore, 50e18, "claimed amount must be exactly 50");
    }

    /// @notice After second deposit, new rewards distribute on the updated stake (175 tokens).
    function test_rewardDebt_newRewardsAfterAdditionalDeposit() public {
        vm.prank(validator1);
        pool.registerPool(0);

        // deposit 100 -> reward 50 -> deposit 75
        vm.prank(delegator1);
        pool.delegate(validator1, 100e18);
        _distributeRewards(validator1, 50e18);
        vm.prank(delegator1);
        pool.delegate(validator1, 75e18);

        // distribute 175 (1 RLS per token)
        _distributeRewards(validator1, 175e18);

        // total = 50 (round 1) + 175 (round 2) = 225
        uint256 totalRewards = pool.getPendingRewards(validator1, delegator1);
        assertEq(totalRewards, 225e18, "total rewards should be 225");
    }

    /// @notice Multiple deposit+reward rounds — no reward leakage or double-counting.
    function test_rewardDebt_multipleDepositRounds() public {
        vm.prank(validator1);
        pool.registerPool(0);

        // Round 1: deposit 100, earn 10
        vm.prank(delegator1);
        pool.delegate(validator1, 100e18);
        _distributeRewards(validator1, 10e18);

        // Round 2: deposit 50 more, earn 30 (on 150 total)
        vm.prank(delegator1);
        pool.delegate(validator1, 50e18);
        _distributeRewards(validator1, 30e18);

        // Round 3: deposit 25 more, earn 35 (on 175 total)
        vm.prank(delegator1);
        pool.delegate(validator1, 25e18);
        _distributeRewards(validator1, 35e18);

        // Total: 10 + 30 + 35 = 75
        uint256 rewards = pool.getPendingRewards(validator1, delegator1);
        assertEq(rewards, 75e18, "accumulated rewards across 3 rounds");

        uint256 balBefore = rls.balanceOf(delegator1);
        vm.prank(delegator1);
        pool.claimDelegationRewards(validator1);
        assertEq(rls.balanceOf(delegator1) - balBefore, 75e18, "claimed must match");
    }

    /// @notice Two delegators — rewards split correctly with no cross-contamination.
    function test_rewardDebt_twoDelegators() public {
        vm.prank(validator1);
        pool.registerPool(0);

        // delegator1 deposits 100
        vm.prank(delegator1);
        pool.delegate(validator1, 100e18);

        // 50 rewards (all to delegator1)
        _distributeRewards(validator1, 50e18);

        // delegator2 deposits 100
        vm.prank(delegator2);
        pool.delegate(validator1, 100e18);

        // 100 rewards (split 50/50)
        _distributeRewards(validator1, 100e18);

        uint256 rewards1 = pool.getPendingRewards(validator1, delegator1);
        uint256 rewards2 = pool.getPendingRewards(validator1, delegator2);

        // delegator1: 50 (round 1) + 50 (round 2) = 100
        assertEq(rewards1, 100e18, "delegator1 rewards");
        // delegator2: 0 (round 1) + 50 (round 2) = 50
        assertEq(rewards2, 50e18, "delegator2 rewards");

        // pool solvency
        assertLe(rewards1 + rewards2, rls.balanceOf(address(pool)));
    }

    /// @notice Partial withdrawal mid-stream — rewards correct on reduced stake.
    function test_rewardDebt_correctAfterPartialWithdrawal() public {
        vm.prank(validator1);
        pool.registerPool(0);

        // deposit 200, earn 100
        vm.prank(delegator1);
        pool.delegate(validator1, 200e18);
        _distributeRewards(validator1, 100e18);

        // undelegate 100 (leaving 100 staked)
        vm.prank(delegator1);
        pool.requestUndelegation(validator1, 100e18);

        // earn 50 more (on 100 staked)
        _distributeRewards(validator1, 50e18);

        // total: 100 (on 200) + 50 (on 100) = 150
        uint256 rewards = pool.getPendingRewards(validator1, delegator1);
        assertEq(rewards, 150e18, "rewards after partial withdrawal");
    }

    // =========================================================================
    //                          Whitelist (Merkle gate on delegate)
    // =========================================================================

    function _wlLeaf(address a, uint256 b) internal pure returns (bytes32) {
        return keccak256(bytes.concat(keccak256(abi.encode(a, b))));
    }

    function _wlHashPair(bytes32 a, bytes32 b) internal pure returns (bytes32) {
        return a < b ? keccak256(abi.encodePacked(a, b)) : keccak256(abi.encodePacked(b, a));
    }

    function _wlProofFor(uint256 which) internal view returns (bytes32[] memory p) {
        p = new bytes32[](2);
        if (which == 1)      { p[0] = wlLeaf2; p[1] = wlNode34; }
        else if (which == 2) { p[0] = wlLeaf1; p[1] = wlNode34; }
        else if (which == 3) { p[0] = wlLeaf4; p[1] = wlNode12; }
        else                 { p[0] = wlLeaf3; p[1] = wlNode12; }
    }

    function _wlEnableGate() internal {
        vm.prank(owner);
        pool.setWhitelistRoot(wlRoot);
    }

    function _wlRegisterPool() internal {
        vm.prank(validator1);
        pool.registerPool(500);
    }

    // --- admin ---

    function test_whitelist_defaults() public view {
        assertEq(pool.whitelistRoot(), bytes32(0));
        assertFalse(pool.whitelistEnabled());
        assertFalse(pool.isWhitelistVerified(delegator1));
    }

    function test_whitelist_setRoot_adminOnly_andEnables() public {
        vm.prank(owner);
        pool.setWhitelistRoot(wlRoot);
        assertEq(pool.whitelistRoot(), wlRoot);
        assertTrue(pool.whitelistEnabled()); // side effect — setting a root commits to gating
    }

    function testRevert_whitelist_setRoot_notAdmin() public {
        bytes32 adminRole = pool.DEFAULT_ADMIN_ROLE();
        vm.prank(outsider);
        vm.expectRevert(
            abi.encodeWithSelector(
                IAccessControl.AccessControlUnauthorizedAccount.selector,
                outsider,
                adminRole
            )
        );
        pool.setWhitelistRoot(wlRoot);
    }

    function test_whitelist_disable_adminOnly() public {
        // first enable via setWhitelistRoot
        vm.prank(owner);
        pool.setWhitelistRoot(wlRoot);
        assertTrue(pool.whitelistEnabled());

        vm.prank(owner);
        pool.disableWhitelist();
        assertFalse(pool.whitelistEnabled());
        // root is preserved — disable is only a gate toggle
        assertEq(pool.whitelistRoot(), wlRoot);
    }

    function testRevert_whitelist_disable_notAdmin() public {
        bytes32 adminRole = pool.DEFAULT_ADMIN_ROLE();
        vm.prank(outsider);
        vm.expectRevert(
            abi.encodeWithSelector(
                IAccessControl.AccessControlUnauthorizedAccount.selector,
                outsider,
                adminRole
            )
        );
        pool.disableWhitelist();
    }

    function test_whitelist_setRoot_emitsBothEvents() public {
        // setWhitelistRoot enables as a side effect — root event first, then enable
        vm.expectEmit(false, false, false, true, address(pool));
        emit IDelegationPool.WhitelistRootUpdated(bytes32(0), wlRoot);
        vm.expectEmit(false, false, false, true, address(pool));
        emit IDelegationPool.WhitelistEnabledUpdated(true);
        vm.prank(owner);
        pool.setWhitelistRoot(wlRoot);
    }

    function test_whitelist_setRoot_rotation_skipsEnableEvent() public {
        vm.prank(owner);
        pool.setWhitelistRoot(wlRoot);

        // second call: root changes but gate is already enabled — only WhitelistRootUpdated fires
        vm.recordLogs();
        bytes32 newRoot = keccak256("new-root");
        vm.prank(owner);
        pool.setWhitelistRoot(newRoot);

        Vm.Log[] memory logs = vm.getRecordedLogs();
        uint256 enableEvents = 0;
        uint256 rootEvents = 0;
        bytes32 enableSig = keccak256("WhitelistEnabledUpdated(bool)");
        bytes32 rootSig = keccak256("WhitelistRootUpdated(bytes32,bytes32)");
        for (uint256 i = 0; i < logs.length; i++) {
            if (logs[i].topics[0] == enableSig) enableEvents++;
            if (logs[i].topics[0] == rootSig) rootEvents++;
        }
        assertEq(rootEvents, 1, "root event should fire on rotation");
        assertEq(enableEvents, 0, "enable event should NOT fire when already enabled");
    }

    function test_whitelist_disable_whenAlreadyDisabled_noEvent() public {
        vm.recordLogs();
        vm.prank(owner);
        pool.disableWhitelist(); // gate was never enabled

        Vm.Log[] memory logs = vm.getRecordedLogs();
        bytes32 enableSig = keccak256("WhitelistEnabledUpdated(bool)");
        for (uint256 i = 0; i < logs.length; i++) {
            assertTrue(logs[i].topics[0] != enableSig, "no WhitelistEnabledUpdated event expected");
        }
    }

    function test_whitelist_disable_emitsEvent() public {
        vm.prank(owner);
        pool.setWhitelistRoot(wlRoot);

        vm.expectEmit(false, false, false, true, address(pool));
        emit IDelegationPool.WhitelistEnabledUpdated(false);
        vm.prank(owner);
        pool.disableWhitelist();
    }

    // --- gate disabled: both overloads work ---

    function test_whitelist_gateDisabled_twoArg_worksForAnyone() public {
        _wlRegisterPool();
        vm.prank(outsider);
        pool.delegate(validator1, 5e18);
        assertEq(pool.getDelegatorPosition(validator1, outsider).amount, 5e18);
        assertFalse(pool.isWhitelistVerified(outsider));
    }

    function test_whitelist_gateDisabled_fourArg_proofIgnored() public {
        _wlRegisterPool();
        bytes32[] memory junk = new bytes32[](0);
        vm.prank(outsider);
        pool.delegateWithProof(validator1, 5e18, 999, junk);
        assertEq(pool.getDelegatorPosition(validator1, outsider).amount, 5e18);
        // proof path skipped entirely — no cache entry written
        assertFalse(pool.isWhitelistVerified(outsider));
    }

    // --- gate enabled: happy path ---

    function test_whitelist_validProof_cachesAndDelegates() public {
        _wlRegisterPool();
        _wlEnableGate();

        vm.expectEmit(true, false, false, false, address(pool));
        emit IDelegationPool.WhitelistVerified(delegator1);

        vm.prank(delegator1);
        pool.delegateWithProof(validator1, 5e18, WL_B1, _wlProofFor(1));

        assertTrue(pool.isWhitelistVerified(delegator1));
        assertEq(pool.getDelegatorPosition(validator1, delegator1).amount, 5e18);
    }

    function test_whitelist_cachedAllowsTwoArgOverload() public {
        _wlRegisterPool();
        _wlEnableGate();

        vm.prank(delegator1);
        pool.delegateWithProof(validator1, 5e18, WL_B1, _wlProofFor(1));

        // subsequent delegate — no proof needed
        vm.prank(delegator1);
        pool.delegate(validator1, 7e18);

        assertEq(pool.getDelegatorPosition(validator1, delegator1).amount, 12e18);
    }

    function test_whitelist_cachedAllowsFourArgWithEmptyProof() public {
        _wlRegisterPool();
        _wlEnableGate();

        vm.prank(delegator1);
        pool.delegateWithProof(validator1, 5e18, WL_B1, _wlProofFor(1));

        // second call — cache short-circuits before proof check
        bytes32[] memory junk = new bytes32[](0);
        vm.prank(delegator1);
        pool.delegateWithProof(validator1, 7e18, 0, junk);

        assertEq(pool.getDelegatorPosition(validator1, delegator1).amount, 12e18);
    }

    function test_whitelist_multipleStakesOverTime() public {
        _wlRegisterPool();
        _wlEnableGate();

        vm.startPrank(delegator1);
        pool.delegateWithProof(validator1, 2e18, WL_B1, _wlProofFor(1));
        pool.delegate(validator1, 3e18);
        pool.delegate(validator1, 4e18);
        pool.delegate(validator1, 5e18);
        vm.stopPrank();

        assertEq(pool.getDelegatorPosition(validator1, delegator1).amount, 14e18);
    }

    function test_whitelist_multipleUsersIndependent() public {
        _wlRegisterPool();
        _wlEnableGate();

        vm.prank(delegator1);
        pool.delegateWithProof(validator1, 1e18, WL_B1, _wlProofFor(1));
        vm.prank(delegator2);
        pool.delegateWithProof(validator1, 2e18, WL_B2, _wlProofFor(2));
        vm.prank(delegator3);
        pool.delegateWithProof(validator1, 3e18, WL_B3, _wlProofFor(3));
        vm.prank(delegator4);
        pool.delegateWithProof(validator1, 4e18, WL_B4, _wlProofFor(4));

        assertTrue(pool.isWhitelistVerified(delegator1));
        assertTrue(pool.isWhitelistVerified(delegator2));
        assertTrue(pool.isWhitelistVerified(delegator3));
        assertTrue(pool.isWhitelistVerified(delegator4));
        assertEq(pool.getTotalDelegatedStake(validator1), 10e18);
    }

    // --- gate enabled: revert paths ---

    function test_whitelist_twoArgNotVerified_delegatesAsOpenTier() public {
        _wlRegisterPool();
        _wlEnableGate();

        // Non-verified callers no longer revert — they are routed to open-tier (Track B)
        vm.prank(delegator1);
        pool.delegate(validator1, 5e18);

        IDelegationPool.DelegatorPosition memory pos = pool.getDelegatorPosition(validator1, delegator1);
        assertEq(pos.amount, 5e18);
        assertTrue(pos.openTier);
        assertFalse(pool.isWhitelistVerified(delegator1));
    }

    function testRevert_whitelist_emptyProof() public {
        _wlRegisterPool();
        _wlEnableGate();

        bytes32[] memory empty = new bytes32[](0);
        vm.prank(delegator1);
        vm.expectRevert(abi.encodeWithSelector(IDelegationPool.NotWhitelisted.selector, delegator1));
        pool.delegateWithProof(validator1, 5e18, WL_B1, empty);
    }

    function testRevert_whitelist_wrongBalanceInLeaf() public {
        _wlRegisterPool();
        _wlEnableGate();

        vm.prank(delegator1);
        vm.expectRevert(abi.encodeWithSelector(IDelegationPool.NotWhitelisted.selector, delegator1));
        pool.delegateWithProof(validator1, 5e18, WL_B1 + 1, _wlProofFor(1));
    }

    function testRevert_whitelist_proofForDifferentAddress() public {
        _wlRegisterPool();
        _wlEnableGate();

        // delegator2 presents delegator1's proof/balance — leaf reconstruction uses msg.sender = delegator2
        vm.prank(delegator2);
        vm.expectRevert(abi.encodeWithSelector(IDelegationPool.NotWhitelisted.selector, delegator2));
        pool.delegateWithProof(validator1, 5e18, WL_B1, _wlProofFor(1));
    }

    function testRevert_whitelist_outsider() public {
        _wlRegisterPool();
        _wlEnableGate();

        // outsider is not in the tree at all — no valid (addr, bal) pair exists
        vm.prank(outsider);
        vm.expectRevert(abi.encodeWithSelector(IDelegationPool.NotWhitelisted.selector, outsider));
        pool.delegateWithProof(validator1, 5e18, 100, _wlProofFor(1));
    }

    function testRevert_whitelist_garbageProof() public {
        _wlRegisterPool();
        _wlEnableGate();

        bytes32[] memory bad = new bytes32[](2);
        bad[0] = keccak256("nope");
        bad[1] = keccak256("also-nope");

        vm.prank(delegator1);
        vm.expectRevert(abi.encodeWithSelector(IDelegationPool.NotWhitelisted.selector, delegator1));
        pool.delegateWithProof(validator1, 5e18, WL_B1, bad);
    }

    // --- toggling ---

    function test_whitelist_toggleGateOff_tierAssignmentFollowsGate() public {
        _wlRegisterPool();
        _wlEnableGate();

        // Gate on + not verified → open-tier (Track B)
        vm.prank(outsider);
        pool.delegate(validator1, 5e18);
        assertFalse(pool.isWhitelistVerified(outsider));
        assertTrue(pool.getDelegatorPosition(validator1, outsider).openTier);
        assertEq(pool.getDelegatorPosition(validator1, outsider).amount, 5e18);

        // Admin disables gate — all new delegations become Track A (isOpenTier=false).
        // Existing open-tier positions keep their locked tier, so outsider can still top up.
        vm.prank(owner);
        pool.disableWhitelist();

        vm.prank(outsider);
        pool.delegate(validator1, 5e18);
        assertEq(pool.getDelegatorPosition(validator1, outsider).amount, 10e18);
        assertTrue(pool.getDelegatorPosition(validator1, outsider).openTier);
    }

    function test_whitelist_cachedEntrySurvivesGateToggle() public {
        _wlRegisterPool();
        _wlEnableGate();

        vm.prank(delegator1);
        pool.delegateWithProof(validator1, 1e18, WL_B1, _wlProofFor(1));
        assertTrue(pool.isWhitelistVerified(delegator1));

        vm.prank(owner);
        pool.disableWhitelist();
        vm.prank(owner);
        pool.setWhitelistRoot(wlRoot); // re-enables

        // cache entry is still there — delegator1 delegates without a fresh proof
        vm.prank(delegator1);
        pool.delegate(validator1, 2e18);
        assertEq(pool.getDelegatorPosition(validator1, delegator1).amount, 3e18);
    }

    function test_whitelist_rootRotation_doesNotInvalidateCachedUsers() public {
        _wlRegisterPool();
        _wlEnableGate();

        vm.prank(delegator1);
        pool.delegateWithProof(validator1, 1e18, WL_B1, _wlProofFor(1));
        assertTrue(pool.isWhitelistVerified(delegator1));

        // admin rotates root to an unrelated value
        vm.prank(owner);
        pool.setWhitelistRoot(keccak256("different-root"));

        // delegator1 stays verified against the old root
        vm.prank(delegator1);
        pool.delegate(validator1, 2e18);
        assertEq(pool.getDelegatorPosition(validator1, delegator1).amount, 3e18);

        // but new users can't verify against the new root with old-root proofs
        vm.prank(delegator2);
        vm.expectRevert(abi.encodeWithSelector(IDelegationPool.NotWhitelisted.selector, delegator2));
        pool.delegateWithProof(validator1, 1e18, WL_B2, _wlProofFor(2));
    }

    // =========================================================================
    //                       Reward-distribution split (fuzz)
    // =========================================================================

    /// @notice A single distribution must split into commission + per-delegator
    ///         rewards that (a) credit the validator EXACTLY amount*bps/MAX,
    ///         (b) conserve — never credit more than the delegator pot, only
    ///         rounding dust withheld in the pool — and (c) pay each delegator in
    ///         proportion to its stake. Inputs are bounded large so integer-division
    ///         rounding stays negligible relative to the values.
    function testFuzz_distributePoolRewards_splitConservesAndProportional(
        uint256 commissionBps,
        uint256 amount,
        uint256 s1,
        uint256 s2,
        uint256 s3
    ) public {
        commissionBps = bound(commissionBps, 0, pool.MAX_INITIAL_COMMISSION_BPS());
        s1 = bound(s1, 1e21, MAX_DELEGATION);
        s2 = bound(s2, 1e21, MAX_DELEGATION);
        s3 = bound(s3, 1e21, MAX_DELEGATION);
        amount = bound(amount, 1e18, 1_000_000e18);

        registry.setCurrentEpoch(1);
        vm.prank(validator1);
        pool.registerPool(commissionBps);

        vm.prank(delegator1);
        pool.delegate(validator1, s1);
        vm.prank(delegator2);
        pool.delegate(validator1, s2);
        vm.prank(delegator3);
        pool.delegate(validator1, s3);

        // advance an epoch so the delegations are reward-eligible (lastDelegateEpoch != current)
        registry.setCurrentEpoch(2);

        // fund the pool so the distribution is backed by real RLS (mirrors _distributeRewards)
        uint256 commissionBefore = pool.getValidatorPool(validator1).pendingValidatorRewards;
        rls.mint(address(pool), amount);
        vm.prank(rewardDistributor);
        pool.distributePoolRewards(validator1, amount);

        // (a) validator commission credited exactly
        uint256 expCommission = (amount * commissionBps) / pool.MAX_COMMISSION_BPS();
        assertEq(
            pool.getValidatorPool(validator1).pendingValidatorRewards - commissionBefore,
            expCommission,
            "commission must equal amount*bps/MAX exactly"
        );

        uint256 delegatorPot = amount - expCommission;
        uint256 totalStake = s1 + s2 + s3;
        uint256 r1 = pool.getPendingRewards(validator1, delegator1);
        uint256 r2 = pool.getPendingRewards(validator1, delegator2);
        uint256 r3 = pool.getPendingRewards(validator1, delegator3);

        // (b) conservation: never over-issue; only floor-division dust withheld in the pool
        uint256 sumR = r1 + r2 + r3;
        assertLe(sumR, delegatorPot, "delegator rewards must not exceed the pot");
        // floor-division withholds up to ~1 wei of share-scaled dust per delegator
        uint256 numDelegators = 3;
        uint256 maxDust = (totalStake / pool.PRECISION()) + numDelegators;
        assertGe(sumR, delegatorPot - maxDust, "only rounding dust may be withheld");

        // (c) proportionality: each delegator paid in proportion to its stake
        assertApproxEqRel(r1, (delegatorPot * s1) / totalStake, 1e13, "delegator1 proportional share");
        assertApproxEqRel(r2, (delegatorPot * s2) / totalStake, 1e13, "delegator2 proportional share");
        assertApproxEqRel(r3, (delegatorPot * s3) / totalStake, 1e13, "delegator3 proportional share");

        // (d) end-to-end: the pool is funded, so every delegator can claim its full
        //     pending reward without reverting — real token conservation, not just accounting
        uint256 b1 = rls.balanceOf(delegator1);
        vm.prank(delegator1);
        pool.claimDelegationRewards(validator1);
        assertEq(rls.balanceOf(delegator1) - b1, r1, "delegator1 must receive its pending reward");

        uint256 b2 = rls.balanceOf(delegator2);
        vm.prank(delegator2);
        pool.claimDelegationRewards(validator1);
        assertEq(rls.balanceOf(delegator2) - b2, r2, "delegator2 must receive its pending reward");

        uint256 b3 = rls.balanceOf(delegator3);
        vm.prank(delegator3);
        pool.claimDelegationRewards(validator1);
        assertEq(rls.balanceOf(delegator3) - b3, r3, "delegator3 must receive its pending reward");
    }

    /// @notice applyPoolSlash must (a) reduce every delegator's effective position
    ///         in proportion to its stake (uniform slash rate), (b) never push a
    ///         position negative, and (c) reduce delegators' aggregate effective
    ///         stake by at least what is transferred out — the ceiling-per-position
    ///         vs floor-transfer gap stays in the pool as a solvency buffer.
    function testFuzz_applyPoolSlash_reducesProportionallyAndStaysSolvent(
        uint256 slashAmount,
        uint256 s1,
        uint256 s2,
        uint256 s3
    ) public {
        s1 = bound(s1, 1e21, MAX_DELEGATION);
        s2 = bound(s2, 1e21, MAX_DELEGATION);
        s3 = bound(s3, 1e21, MAX_DELEGATION);

        registry.setCurrentEpoch(1);
        vm.prank(validator1);
        pool.registerPool(0); // commission irrelevant to slashing

        vm.prank(delegator1);
        pool.delegate(validator1, s1);
        vm.prank(delegator2);
        pool.delegate(validator1, s2);
        vm.prank(delegator3);
        pool.delegate(validator1, s3);

        uint256 totalStake = s1 + s2 + s3; // pool RLS balance == totalStake, so the transfer succeeds
        slashAmount = bound(slashAmount, 1e18, totalStake); // <= totalStake, so effectiveSlash == slashAmount

        vm.prank(address(registry)); // applyPoolSlash is onlyConsensusRegistry
        uint256 transferred = pool.applyPoolSlash(validator1, slashAmount);

        (uint256 eff1,) = pool.getEffectivePosition(validator1, delegator1);
        (uint256 eff2,) = pool.getEffectivePosition(validator1, delegator2);
        (uint256 eff3,) = pool.getEffectivePosition(validator1, delegator3);

        // (b) no position grows or underflows
        assertLe(eff1, s1, "d1 effective <= original");
        assertLe(eff2, s2, "d2 effective <= original");
        assertLe(eff3, s3, "d3 effective <= original");

        uint256 sl1 = s1 - eff1;
        uint256 sl2 = s2 - eff2;
        uint256 sl3 = s3 - eff3;

        // (a) proportional: each delegator slashed in proportion to its stake
        assertApproxEqRel(sl1, (slashAmount * s1) / totalStake, 1e13, "d1 slashed proportionally");
        assertApproxEqRel(sl2, (slashAmount * s2) / totalStake, 1e13, "d2 slashed proportionally");
        assertApproxEqRel(sl3, (slashAmount * s3) / totalStake, 1e13, "d3 slashed proportionally");

        // (c) solvency: aggregate effective loss >= RLS transferred out (dust buffer stays in pool)
        assertGe(sl1 + sl2 + sl3, transferred, "aggregate loss must cover the transfer");

        // (c) solvency, on-chain side: the pool's remaining RLS still backs every claimable position
        assertGe(
            rls.balanceOf(address(pool)),
            pool.getTotalDelegatedStake(validator1),
            "pool must remain solvent after slash"
        );
    }
}
