// SPDX-License-Identifier: BUSL-1.1
pragma solidity 0.8.26;

import { IConsensusRegistry } from "../interfaces/IConsensusRegistry.sol";
import { SystemCallable } from "./SystemCallable.sol";

/**
 * @title DkgManager
 * @notice Rayls Core Ltd.
 *
 * @notice Collects the publicly verifiable dealings that make each epoch's threshold BLS key.
 * @dev The key for epoch `E` is dealt during epoch `E - 1` by `E - 1`'s committee, to `E`'s
 * committee. Every dealing is checked in full by the NI-DKG precompile before it is accepted,
 * so an accepted dealing is good for every recipient and needs no complaint or acknowledgement.
 * The key is the sum of every accepted dealing; it exists once at least a third of the dealing
 * committee (rounded up) has dealt, which `isReady` reports to the protocol's epoch boundary.
 *
 * Recipients are `E`'s committee, ordered by BLS public key bytes, the protocol's committee
 * order; the member at position `p` holds share `p + 1`. `E` gets a key only when every member's
 * encryption key was set at genesis or registered in a block before the first block of `E - 1`:
 * the set is then fixed before any dealing of the epoch executes, and no member is left unable to
 * sign, which would cost the epoch fault tolerance. Otherwise `E` uses aggregate certificates.
 * So does a committee larger than `MAX_KEYED_COMMITTEE`, whose dealings would not fit one
 * transaction.
 *
 * Dealings are kept per recipient set (`recipientsDigest`): if the registry ejects a member of
 * `E`'s committee while dealing is open, earlier dealings stop counting and dealers deal again.
 * In the block that closes `E - 1` the protocol calls `finalize(E)`, which records the set and
 * whether its dealings make `E`'s key; later ejections cannot change it.
 *
 * Dealings and registrations are authorised by the validator's BLS consensus key, checked by the
 * precompile, so any account can submit them. The contract refunds the gas of every accepted one
 * from its own balance, so a validator's submitting account needs only enough for one
 * transaction. The contract stores only a hash of each dealing and the block that accepted it;
 * nodes read the bytes from that block's `DealingAccepted` event.
 */
contract DkgManager is SystemCallable {
    /// @dev The consensus registry, at its genesis address.
    IConsensusRegistry public constant REGISTRY = IConsensusRegistry(0x07E17e17E17e17E17e17E17E17E17e17e17E17e1);
    /// @dev The NI-DKG verifier precompile.
    address public constant NIDKG_PRECOMPILE = address(0x0401);

    /// @dev The key of one epoch as `finalize` recorded it.
    struct Final {
        /// @dev `finalize` ran for the epoch.
        bool finalized;
        /// @dev The epoch has a threshold key, made of the dealings for `digest`.
        bool keyed;
        /// @dev The recipient set the key was dealt to.
        bytes32 digest;
    }

    // Storage layout: the genesis builder writes `encryptionKeys` (slot 0), `keyOwners` (slot 1)
    // and `genesisDealings` (slot 8) directly. Keep them where they are.

    /// @dev Each validator's compressed G1 encryption key, set once.
    mapping(address validator => bytes key) public encryptionKeys;
    /// @dev The validator that holds each encryption key, by the key's hash: a key serves one
    /// validator, since two recipients with one key would let a dealing to both never verify.
    mapping(bytes32 keyHash => address validator) public keyOwners;
    /// @dev Block in which each validator registered its encryption key; 0 for a key set at genesis.
    mapping(address validator => uint64 blockNumber) public registrationBlocks;
    /// @dev Hash of each dealer's accepted dealing for the key of each epoch, per recipient set.
    mapping(uint32 epoch => mapping(bytes32 digest => mapping(address dealer => bytes32 dealingHash))) internal
        _dealingHashes;
    /// @dev Block in which each dealing was accepted; its `DealingAccepted` event has the bytes.
    mapping(uint32 epoch => mapping(bytes32 digest => mapping(address dealer => uint64 blockNumber))) internal
        _dealingBlocks;
    /// @dev Dealers whose dealing was accepted, per recipient set, in acceptance order.
    mapping(uint32 epoch => mapping(bytes32 digest => address[] dealers)) internal _dealers;
    /// @dev Each epoch's key as finalized in the block that closed the epoch before it.
    mapping(uint32 epoch => Final) public finals;
    /// @dev The committee the finalized key of each epoch was dealt to.
    mapping(uint32 epoch => address[] members) internal _finalMembers;
    /// @dev Hash of the genesis validators' dealings of epoch 0's key (`genesis/dkg-dealings.yaml`,
    /// checked by the genesis builder), or zero when epoch 0 uses aggregate certificates. Genesis
    /// has no blocks to carry them, so nodes read the dealings from their genesis files and check
    /// them against this hash.
    bytes32 public genesisDealings;

    /// @dev Gas a refund cannot measure: the transaction's base cost and the refund transfer.
    /// Calldata is counted at the non-zero byte price, an upper bound, for the canonical encoding
    /// of the call only: bytes appended to it are not refunded.
    uint256 internal constant REFUND_OVERHEAD = 21_000 + 12_000;
    /// @dev Most gas one call is refunded.
    uint256 internal constant MAX_REFUNDED_GAS = 30_000_000;
    /// @dev Largest committee that gets a threshold key. A dealing grows by about 0.94 kB per
    /// member; at this size its transaction is about 110 kB, under the 128 KiB a default node
    /// pool accepts, and its gas limit under `MAX_REFUNDED_GAS`.
    uint256 public constant MAX_KEYED_COMMITTEE = 100;

    event EncryptionKeyRegistered(address indexed validator, bytes key);
    event DealingAccepted(
        uint32 indexed epoch, address indexed dealer, bytes32 digest, bytes32 dealingHash, bytes dealing
    );
    event KeyFinalized(uint32 indexed epoch, bool keyed, bytes32 digest);
    event GasRefunded(address indexed sender, uint256 amount);

    error UnknownValidator(address validator);
    error KeyAlreadyRegistered(address validator);
    error KeyInUse(address owner);
    error InvalidRegistration(address validator);
    error WrongDealingEpoch(uint32 epoch, uint32 expected);
    error NotADealer(uint32 epoch, address dealer);
    error AlreadyDealt(uint32 epoch, address dealer);
    error InvalidDealing(uint32 epoch, address dealer);
    error NoKeyForEpoch(uint32 epoch);

    /// @dev Refund the gas of a successful call at no more than the base fee, from this contract's
    /// balance, while it lasts. `abiLength` is the canonical length of the call's calldata. A
    /// sender that cannot receive goes unrefunded.
    modifier refundsGas(uint256 abiLength) {
        uint256 gasAtStart = gasleft();
        _;
        uint256 used = gasAtStart - gasleft() + REFUND_OVERHEAD + 16 * abiLength;
        if (used > MAX_REFUNDED_GAS) used = MAX_REFUNDED_GAS;
        uint256 price = tx.gasprice < block.basefee ? tx.gasprice : block.basefee;
        uint256 amount = used * price;
        if (amount != 0 && amount <= address(this).balance) {
            (bool sent,) = payable(msg.sender).call{ value: amount }("");
            if (sent) emit GasRefunded(msg.sender, amount);
        }
    }

    /// @notice Funds for gas refunds.
    receive() external payable { }

    /**
     * @notice Register `validator`'s encryption key, once. A key is accepted for one validator.
     * @param key Compressed G1 encryption key.
     * @param proof BCS-encoded proof of knowledge of the key, bound to the validator's BLS key.
     * @param signature The validator's BLS signature over the registration.
     */
    function registerEncryptionKey(
        address validator,
        bytes calldata key,
        bytes calldata proof,
        bytes calldata signature
    )
        external
        refundsGas(4 + 4 * 32 + _abiBytesLength(key) + _abiBytesLength(proof) + _abiBytesLength(signature))
    {
        if (encryptionKeys[validator].length != 0) revert KeyAlreadyRegistered(validator);
        address owner = keyOwners[keccak256(key)];
        if (owner != address(0)) revert KeyInUse(owner);
        bytes memory blsPubkey = _blsKey(validator);
        bool valid = _verify(
            abi.encodeWithSignature(
                "verifyEncryptionKey(uint64,bytes,bytes,bytes,bytes)",
                uint64(block.chainid),
                blsPubkey,
                key,
                proof,
                signature
            )
        );
        if (!valid) revert InvalidRegistration(validator);
        encryptionKeys[validator] = key;
        keyOwners[keccak256(key)] = validator;
        registrationBlocks[validator] = uint64(block.number);
        emit EncryptionKeyRegistered(validator, key);
    }

    /**
     * @notice Submit `dealer`'s dealing for the key of `epoch`, which must be the next epoch, to
     * its current recipient set.
     * @param signature The dealer's BLS signature over the dealing.
     * @param dealing BCS-encoded dealing.
     */
    function submitDealing(
        uint32 epoch,
        address dealer,
        bytes calldata signature,
        bytes calldata dealing
    )
        external
        refundsGas(4 + 4 * 32 + _abiBytesLength(signature) + _abiBytesLength(dealing))
    {
        uint32 current = REGISTRY.getCurrentEpoch();
        if (epoch != current + 1) revert WrongDealingEpoch(epoch, current + 1);
        if (!_contains(REGISTRY.getEpochInfo(current).committee, dealer)) revert NotADealer(epoch, dealer);
        (address[] memory members, bytes[] memory keys) = recipients(epoch);
        if (!_allKeyed(members, keys)) revert NoKeyForEpoch(epoch);
        bytes32 digest = _digest(members, keys);
        if (_dealingHashes[epoch][digest][dealer] != bytes32(0)) revert AlreadyDealt(epoch, dealer);

        if (!_verify(_dealingCall(epoch, dealer, members, keys, signature, dealing))) {
            revert InvalidDealing(epoch, dealer);
        }

        bytes32 hash = keccak256(dealing);
        _dealingHashes[epoch][digest][dealer] = hash;
        _dealingBlocks[epoch][digest][dealer] = uint64(block.number);
        _dealers[epoch][digest].push(dealer);
        emit DealingAccepted(epoch, dealer, digest, hash, dealing);
    }

    /**
     * @notice Record the key of `epoch` for good: the current recipient set and whether its
     * dealings make a key. Called by the protocol in the block that closes `epoch - 1`, before
     * the registry moves to `epoch`. Runs once per epoch.
     */
    function finalize(uint32 epoch) external onlySystemCall {
        if (finals[epoch].finalized) return;
        (address[] memory members, bytes[] memory keys) = recipients(epoch);
        bytes32 digest = _digest(members, keys);
        bool keyed = _allKeyed(members, keys) && _enoughDealers(epoch, digest);
        finals[epoch] = Final({ finalized: true, keyed: keyed, digest: digest });
        _finalMembers[epoch] = members;
        emit KeyFinalized(epoch, keyed, digest);
    }

    /**
     * @notice The committee of `epoch` and each member's encryption key as a recipient of
     * `epoch`'s key: empty for a member whose key was neither set at genesis nor registered before
     * the first block of `epoch - 1`. Read live, for the next epoch.
     */
    function recipients(uint32 epoch) public view returns (address[] memory members, bytes[] memory keys) {
        members = REGISTRY.getEpochInfo(epoch).committee;
        uint64 dealingStart = REGISTRY.getEpochInfo(epoch - 1).blockHeight;
        keys = new bytes[](members.length);
        for (uint256 i; i < members.length; ++i) {
            bytes memory key = encryptionKeys[members[i]];
            uint64 registered = registrationBlocks[members[i]];
            if (key.length != 0 && (registered == 0 || registered < dealingStart)) keys[i] = key;
        }
    }

    /// @notice Digest of `epoch`'s current recipient set, which its dealings are kept under.
    function recipientsDigest(uint32 epoch) public view returns (bytes32) {
        (address[] memory members, bytes[] memory keys) = recipients(epoch);
        return _digest(members, keys);
    }

    /// @notice The recipient set the key of `epoch` is dealt to: the finalized one once there is
    /// one, else the current one.
    function activeDigest(uint32 epoch) public view returns (bytes32) {
        Final memory fin = finals[epoch];
        return fin.finalized ? fin.digest : recipientsDigest(epoch);
    }

    /// @notice Whether `epoch` gets a threshold key: as finalized, or while dealing is open,
    /// whether every member of its committee is a recipient. Otherwise the epoch uses aggregate
    /// certificates.
    function hasKey(uint32 epoch) public view returns (bool) {
        Final memory fin = finals[epoch];
        if (fin.finalized) return fin.keyed;
        (address[] memory members, bytes[] memory keys) = recipients(epoch);
        return _allKeyed(members, keys);
    }

    /// @notice Whether the key of `epoch` is settled: finalized, the epoch gets no key, or at
    /// least a third (rounded up) of the dealing committee, that of `epoch - 1`, has dealt to the
    /// current recipient set.
    function isReady(uint32 epoch) external view returns (bool) {
        if (finals[epoch].finalized) return true;
        (address[] memory members, bytes[] memory keys) = recipients(epoch);
        if (!_allKeyed(members, keys)) return true;
        return _enoughDealers(epoch, _digest(members, keys));
    }

    /// @notice Dealers accepted for the key of `epoch` to its active recipient set, in
    /// acceptance order.
    function dealers(uint32 epoch) external view returns (address[] memory) {
        return _dealers[epoch][activeDigest(epoch)];
    }

    /// @notice Block that accepted `dealer`'s dealing for the key of `epoch` to its active
    /// recipient set; 0 if none.
    function dealingBlock(uint32 epoch, address dealer) external view returns (uint64) {
        return _dealingBlocks[epoch][activeDigest(epoch)][dealer];
    }

    /// @notice Hash of `dealer`'s accepted dealing for the key of `epoch` to its active recipient
    /// set; zero if none.
    function dealingHash(uint32 epoch, address dealer) external view returns (bytes32) {
        return _dealingHashes[epoch][activeDigest(epoch)][dealer];
    }

    /// @notice The committee the finalized key of `epoch` was dealt to, in the registry's order.
    function finalMembers(uint32 epoch) external view returns (address[] memory) {
        return _finalMembers[epoch];
    }

    /// @notice Signing threshold of a committee of `n`: `floor(2n/3) + 1`, the quorum.
    function threshold(uint32 n) public pure returns (uint32) {
        return (2 * n) / 3 + 1;
    }

    /// @notice Dealings needed for a key from a dealing committee of `n`: `ceil(n/3)`, at least 1.
    function dealersNeeded(uint256 n) public pure returns (uint256) {
        uint256 needed = (n + 2) / 3;
        return needed == 0 ? 1 : needed;
    }

    function _enoughDealers(uint32 epoch, bytes32 digest) internal view returns (bool) {
        uint256 dealerCount = REGISTRY.getEpochInfo(epoch - 1).committee.length;
        return _dealers[epoch][digest].length >= dealersNeeded(dealerCount);
    }

    function _allKeyed(address[] memory members, bytes[] memory keys) internal pure returns (bool) {
        if (members.length == 0 || members.length > MAX_KEYED_COMMITTEE) return false;
        for (uint256 i; i < keys.length; ++i) {
            if (keys[i].length == 0) return false;
        }
        return true;
    }

    function _digest(address[] memory members, bytes[] memory keys) internal pure returns (bytes32) {
        return keccak256(abi.encode(members, keys));
    }

    /// @dev The precompile call checking `dealer`'s dealing against `members` and their `keys`.
    function _dealingCall(
        uint32 epoch,
        address dealer,
        address[] memory members,
        bytes[] memory keys,
        bytes calldata signature,
        bytes calldata dealing
    )
        internal
        view
        returns (bytes memory)
    {
        bytes[] memory blsKeys = new bytes[](members.length);
        for (uint256 i; i < members.length; ++i) {
            blsKeys[i] = _blsKey(members[i]);
        }
        return abi.encodeWithSignature(
            "verifyDealing(uint64,uint32,address,bytes,bytes,uint32,bytes[],bytes[],bytes)",
            uint64(block.chainid),
            epoch,
            dealer,
            _blsKey(dealer),
            signature,
            threshold(uint32(members.length)),
            blsKeys,
            keys,
            dealing
        );
    }

    /// @dev Length of a `bytes` argument in canonical ABI encoding: its length word and its data
    /// padded to 32 bytes.
    function _abiBytesLength(bytes calldata value) internal pure returns (uint256) {
        return 32 + ((value.length + 31) / 32) * 32;
    }

    function _blsKey(address validator) internal view returns (bytes memory) {
        IConsensusRegistry.ValidatorInfo memory info = REGISTRY.getValidator(validator);
        if (info.currentStatus == IConsensusRegistry.ValidatorStatus.Undefined) revert UnknownValidator(validator);
        return info.blsPubkey;
    }

    function _verify(bytes memory call) internal view returns (bool) {
        (bool ok, bytes memory result) = NIDKG_PRECOMPILE.staticcall(call);
        return ok && result.length == 32 && abi.decode(result, (bool));
    }

    function _contains(address[] memory list, address item) internal pure returns (bool) {
        for (uint256 i; i < list.length; ++i) {
            if (list[i] == item) return true;
        }
        return false;
    }
}
