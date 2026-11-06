// SPDX-License-Identifier: MIT or Apache-2.0
pragma solidity 0.8.26;

import "forge-std/Test.sol";
import {DelegationPool} from "src/consensus/DelegationPool.sol";
import {IDelegationPool} from "src/interfaces/IDelegationPool.sol";
import {IConsensusRegistry} from "src/interfaces/IConsensusRegistry.sol";
import {ERC20} from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";

contract MockRLS is ERC20 {
    constructor() ERC20("Mock RLS", "RLS") {}

    function mint(address to, uint256 amount) external {
        _mint(to, amount);
    }
}

contract MockConsensusRegistry {
    mapping(address => IConsensusRegistry.ValidatorStatus) public validatorStatuses;
    mapping(address => bool) public validatorAllowlist;
    uint32 public currentEpoch;

    function setValidatorStatus(address v, IConsensusRegistry.ValidatorStatus s) external {
        validatorStatuses[v] = s;
    }

    function setAllowlisted(address v, bool a) external {
        validatorAllowlist[v] = a;
    }

    function setCurrentEpoch(uint32 e) external {
        currentEpoch = e;
    }

    function getValidator(address v) external view returns (IConsensusRegistry.ValidatorInfo memory) {
        return IConsensusRegistry.ValidatorInfo({
            blsPubkey: "",
            validatorAddress: v,
            activationEpoch: 0,
            exitEpoch: 0,
            currentStatus: validatorStatuses[v],
            isRetired: false,
            isDelegated: false,
            stakeVersion: 0
        });
    }

    function getCurrentEpoch() external view returns (uint32) {
        return currentEpoch;
    }

    function isAllowlisted(address v) external view returns (bool) {
        return validatorAllowlist[v];
    }
}

/// @dev Drives random sequences of delegate / undelegate / claim / distribute over a
///      fixed actor set so the invariant can enumerate every obligation.
contract DelegationPoolHandler is Test {
    DelegationPool public pool;
    MockRLS public rls;
    MockConsensusRegistry public registry;
    address[] public validators;
    address public rewardDistributor;
    address[] public delegators;

    uint256 internal constant MIN_DELEGATION = 1e18;
    uint256 internal constant MAX_DELEGATION = 1_000_000e18;
    uint256 internal constant MAX_VALIDATOR_DELEGATION = 10_000_000e18;

    constructor(
        DelegationPool _pool,
        MockRLS _rls,
        MockConsensusRegistry _registry,
        address[] memory _validators,
        address _rewardDistributor,
        address[] memory _delegators
    ) {
        pool = _pool;
        rls = _rls;
        registry = _registry;
        validators = _validators;
        rewardDistributor = _rewardDistributor;
        delegators = _delegators;
    }

    function _actor(uint256 seed) internal view returns (address) {
        return delegators[seed % delegators.length];
    }

    function _validator(uint256 seed) internal view returns (address) {
        return validators[seed % validators.length];
    }

    function delegate(uint256 seed, uint256 vSeed, uint256 amount) external {
        address d = _actor(seed);
        address validator = _validator(vSeed);
        IDelegationPool.ValidatorPool memory vp = pool.getValidatorPool(validator);
        IDelegationPool.DelegatorPosition memory pos = pool.getDelegatorPosition(validator, d);

        uint256 poolRoom = MAX_VALIDATOR_DELEGATION - vp.totalDelegated;
        uint256 actorRoom = MAX_DELEGATION - pos.amount;
        uint256 cap = poolRoom < actorRoom ? poolRoom : actorRoom;
        uint256 bal = rls.balanceOf(d);
        if (bal < cap) cap = bal;
        if (cap < MIN_DELEGATION) return;

        amount = bound(amount, MIN_DELEGATION, cap);
        vm.prank(d);
        pool.delegate(validator, amount);
    }

    function requestUndelegation(uint256 seed, uint256 vSeed, uint256 amount) external {
        address d = _actor(seed);
        address validator = _validator(vSeed);
        IDelegationPool.DelegatorPosition memory pos = pool.getDelegatorPosition(validator, d);
        if (pos.amount == 0 || pos.undelegateEpoch != 0) return;

        amount = bound(amount, 1, pos.amount);
        vm.prank(d);
        pool.requestUndelegation(validator, amount);
    }

    function transferStake(uint256 seed, uint256 fromSeed, uint256 toSeed, uint256 amount) external {
        address d = _actor(seed);
        address from = _validator(fromSeed);
        address to = _validator(toSeed);
        if (from == to) return;

        IDelegationPool.DelegatorPosition memory fromPos = pool.getDelegatorPosition(from, d);
        if (fromPos.amount == 0) return;
        // lastTransferEpoch is stored 1-based; already used this epoch if it equals current+1
        if (fromPos.lastTransferEpoch == registry.getCurrentEpoch() + 1) return;

        IDelegationPool.ValidatorPool memory toPool = pool.getValidatorPool(to);
        IDelegationPool.DelegatorPosition memory toPos = pool.getDelegatorPosition(to, d);

        uint256 poolRoom = MAX_VALIDATOR_DELEGATION - (toPool.totalDelegated + toPool.openTierDelegated);
        uint256 actorRoom = MAX_DELEGATION - toPos.amount;
        uint256 cap = poolRoom < actorRoom ? poolRoom : actorRoom;
        if (cap > fromPos.amount) cap = fromPos.amount;
        if (cap == 0) return;

        amount = bound(amount, 1, cap);
        vm.prank(d);
        pool.transferStake(from, to, amount);
    }

    function completeUndelegation(uint256 seed, uint256 vSeed) external {
        address d = _actor(seed);
        address validator = _validator(vSeed);
        IDelegationPool.DelegatorPosition memory pos = pool.getDelegatorPosition(validator, d);
        if (pos.undelegateEpoch == 0 || registry.getCurrentEpoch() < pos.undelegateEpoch) return;

        vm.prank(d);
        pool.completeUndelegation(validator);
    }

    function claimRewards(uint256 seed, uint256 vSeed) external {
        address d = _actor(seed);
        address validator = _validator(vSeed);
        (, uint256 rewards) = pool.getEffectivePosition(validator, d);
        if (rewards == 0) return;

        vm.prank(d);
        pool.claimDelegationRewards(validator);
    }

    function claimCommission(uint256 vSeed) external {
        address validator = _validator(vSeed);
        if (pool.getValidatorPool(validator).pendingValidatorRewards == 0) return;
        vm.prank(validator);
        pool.claimCommission();
    }

    function distributeRewards(uint256 vSeed, uint256 amount) external {
        address validator = _validator(vSeed);
        amount = bound(amount, 1, MAX_DELEGATION);
        // mirror the production reward path: advance an epoch, fund the pool, distribute
        registry.setCurrentEpoch(registry.getCurrentEpoch() + 1);
        rls.mint(address(pool), amount);
        vm.prank(rewardDistributor);
        pool.distributePoolRewards(validator, amount);
    }

    function advanceEpoch(uint256 n) external {
        registry.setCurrentEpoch(registry.getCurrentEpoch() + uint32(bound(n, 1, 5)));
    }

    function slash(uint256 vSeed, uint256 amount) external {
        address validator = _validator(vSeed);
        uint256 total = pool.getValidatorPool(validator).totalDelegated;
        if (total == 0) return;
        amount = bound(amount, 1, total);
        vm.prank(address(registry)); // applyPoolSlash is onlyConsensusRegistry
        pool.applyPoolSlash(validator, amount);
    }
}

/// @notice Invariant: the DelegationPool always holds enough RLS to pay everything it
///         owes — active principal, claimable rewards, pending undelegations, and
///         accrued commission — across arbitrary operation sequences. A drain or
///         accounting-corruption bug would push the balance below total obligations.
contract DelegationPoolSolvencyInvariant is Test {
    DelegationPool internal pool;
    MockRLS internal rls;
    MockConsensusRegistry internal registry;
    DelegationPoolHandler internal handler;

    address internal owner = address(0xABCD);
    address internal validator = address(0x1001);
    address internal validator2 = address(0x1002);
    address internal rewardDistributor = address(0x3001);
    address[] internal delegators;

    function setUp() public {
        rls = new MockRLS();
        registry = new MockConsensusRegistry();

        IDelegationPool.DelegationConfig memory cfg = IDelegationPool.DelegationConfig({
            minDelegation: 1e18,
            maxDelegation: 1_000_000e18,
            maxValidatorDelegation: 10_000_000e18,
            unbondingEpochs: 3,
            commissionDelayEpochs: 7
        });

        DelegationPool impl = new DelegationPool();
        ERC1967Proxy proxy = new ERC1967Proxy(
            address(impl),
            abi.encodeCall(DelegationPool.initialize, (address(rls), address(registry), owner, cfg))
        );
        pool = DelegationPool(address(proxy));

        vm.prank(owner);
        pool.setRewardDistributor(rewardDistributor);

        registry.setValidatorStatus(validator, IConsensusRegistry.ValidatorStatus.Active);
        registry.setAllowlisted(validator, true);
        registry.setValidatorStatus(validator2, IConsensusRegistry.ValidatorStatus.Active);
        registry.setAllowlisted(validator2, true);
        registry.setCurrentEpoch(1);

        // two validators open pools (different commissions) — exercises cross-pool isolation
        vm.prank(validator);
        pool.registerPool(1000); // 10%
        vm.prank(validator2);
        pool.registerPool(2000); // 20%

        for (uint256 i = 0; i < 4; i++) {
            address d = address(uint160(0x2000 + i));
            delegators.push(d);
            rls.mint(d, 10_000_000e18);
            vm.prank(d);
            rls.approve(address(pool), type(uint256).max);
        }

        address[] memory vals = new address[](2);
        vals[0] = validator;
        vals[1] = validator2;
        handler = new DelegationPoolHandler(pool, rls, registry, vals, rewardDistributor, delegators);

        bytes4[] memory selectors = new bytes4[](9);
        selectors[0] = handler.delegate.selector;
        selectors[1] = handler.requestUndelegation.selector;
        selectors[2] = handler.completeUndelegation.selector;
        selectors[3] = handler.claimRewards.selector;
        selectors[4] = handler.claimCommission.selector;
        selectors[5] = handler.distributeRewards.selector;
        selectors[6] = handler.advanceEpoch.selector;
        selectors[7] = handler.slash.selector;
        selectors[8] = handler.transferStake.selector;
        targetSelector(FuzzSelector({addr: address(handler), selectors: selectors}));
        targetContract(address(handler));
        // only the handler is fuzzed; exclude the rest (defensive vs Forge target-selection changes)
        excludeContract(address(pool));
        excludeContract(address(rls));
        excludeContract(address(registry));
    }

    function invariant_poolIsSolvent() public view {
        address[2] memory vals = [validator, validator2];
        uint256 obligations;
        for (uint256 v = 0; v < vals.length; v++) {
            for (uint256 i = 0; i < delegators.length; i++) {
                (uint256 effectiveAmount, uint256 pendingRewards) = pool.getEffectivePosition(vals[v], delegators[i]);
                uint256 pendingUndelegation = pool.getDelegatorPosition(vals[v], delegators[i]).undelegateAmount;
                obligations += effectiveAmount + pendingRewards + pendingUndelegation;
            }
            obligations += pool.getValidatorPool(vals[v]).pendingValidatorRewards;
        }

        assertGe(rls.balanceOf(address(pool)), obligations, "DelegationPool insolvent: balance < obligations across pools");
    }
}
