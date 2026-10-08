// SPDX-License-Identifier: BUSL-1.1
pragma solidity ^0.8.20;

import {Math} from "@openzeppelin/contracts/utils/math/Math.sol";

/// @title BlsG1 Proof of Possession Library
/// @notice Utility to perform singular proof of possessions for BLS12-381 using EIP2537
/// @author Rayls Core Ltd., Telcoin Association

library BlsG1 {
    /// @dev Represents a field element in BLS12-381 base field Fp
    /// @param data The 64-byte field element data padded with 16 zero bytes for EIP2537 compliance
    struct Fp {
        bytes data;
    }

    /// @dev Represents a proof of possession for a validator's BLS public key
    /// @param uncompressedPubkey A 192-byte uncompressed G2 point
    /// @param uncompressedSignature A 96-byte uncompressed G1 point PoP over the `proofOfPossessionMessage()`
    /// @notice Ensuring `uncompressedPubkey` corresponds to `ValidatorInfo::blsPubkey` is better
    /// performed externally in Rust by the protocol due to EIP2537 precompile & EVM limitations
    /// so this contract does not perform any (un)compression checks
    struct ProofOfPossession {
        bytes uncompressedPubkey;
        bytes uncompressedSignature;
    }

    error InvalidPoint(uint256 actualLen, uint256 expectedLen);
    error InvalidBLSPubkey();
    error InvalidUncompressedBLSPubkey();
    error InvalidFpOffset();
    error InvalidBLSProof();
    error EllTooLarge(uint256 ell);
    error LengthTooLarge(uint256 len);
    error CountTooLarge(uint256 count);
    error DSTTooLong(uint256 dstLen);
    error I2OSPIntegerTooLarge(uint256 i2ospInteger);
    error LowLevelCallFailure(bytes err);

    /// @dev Relevant BLS12-381 precompiles
    address public constant G1_ADD = address(0x0B);
    address public constant G2_ADD = address(0x0D);
    address public constant PAIRING_CHECK = address(0x0F);
    address public constant MAP_FP_TO_G1 = address(0x10);

    /// @dev Negation of the BLS12-381 `G2_GENERATOR` point, used for signature pairing checks in our PoP
    bytes public constant G2_GENERATOR_NEG =
        hex"00000000000000000000000000000000024aa2b2f08f0a91260805272dc51051c6e47ad4fa403b02b4510b647ae3d1770bac0326a805bbefd48056c8c121bdb80000000000000000000000000000000013e02b6052719f607dacd3a088274f65596bd0d09920b61ab5da61bbdc7f5049334cf11213945d57e5ac7d055d042b7e000000000000000000000000000000000d1b3cc2c7027888be51d9ef691d77bcb679afda66c73f17f9ee3837a55024f78c71363275a75d75d86bab79f74782aa0000000000000000000000000000000013fa4d4a0ad8b1ce186ed5061789213d993923066dddaf1040bc3ff59f825c78df74f2d75467e25e0f55f8a00fa030ed";
    /// @dev 381-bit base field prime modulus
    bytes public constant P =
        hex"1a0111ea397fe69a4b1ba7b6434bacd764774b84f38512bf6730d2a0f6b0f6241eabfffeb153ffffb9feffffffffaaab";
    /// @dev The BLS12-381 identity elements (infinity/zero points) encoded to comply with EIP2537
    bytes public constant G1_IDENTITY = new bytes(EIP2537_G1_POINT_SIZE);
    bytes public constant G2_IDENTITY = new bytes(EIP2537_G2_POINT_SIZE);

    /// @dev Raw (unpadded) identity points for pre-encoding checks (96 and 192 bytes respectively)
    bytes public constant G1_IDENTITY_RAW = new bytes(96);
    bytes public constant G2_IDENTITY_RAW = new bytes(192);

    /// @dev Rayls-specific Domain Separator Tag for BLS12-381 G1
    /// https://datatracker.ietf.org/doc/html/rfc9380#appendix-J.9.1
    /// @notice Used only for single proof of possession checks via blst::min_sig
    bytes public constant HASH_TO_G1_DST =
        "BLS_SIG_BLS12381G1_XMD:SHA-256_SSWU_RO_NUL_";

    /// @dev Constants for `expand_message_xmd` variant defined in RFC9380
    uint256 public constant B_IN_BYTES = 32; // output size in bytes by compliant hash fn (sha256)
    uint256 public constant S_IN_BYTES = 64; // block size in bytes by compliant hash fn (sha256)

    /// @dev L parameter from RFC 9380 for hash-to-field operations
    /// guarantees proper statistical distribution in hash-to-field operations
    /// L = ceil((ceil(log2(p)) + k) / 8) where:
    /// - p is the BLS12-381 field modulus (381 bits)
    /// - k is the security parameter (128 bits)
    /// - L = ceil((381 + 128) / 8) = ceil(509 / 8) = 64 bytes
    uint256 public constant L = 64;

    /// @dev Actual size of BLS12-381 field elements in bytes (381 bits = 48 bytes)
    uint256 public constant FIELD_ELEMENT_SIZE = 48;
    /// @dev EIP-2537 encoding sizes
    uint256 public constant EIP2537_FIELD_ELEMENT_SIZE = 64;
    uint256 public constant EIP2537_G1_POINT_SIZE = 128;
    uint256 public constant EIP2537_G2_POINT_SIZE = 256;

    /// @dev Encode a BLS12-381 G1 point (signature for min_sig) to EIP2537 format (128 bytes)
    /// @param uncompressedSignature 96-byte uncompressed G1 point
    /// @return _ EIP2537-compliant, 128-byte encoded G1 point comprising two 64-byte padded field elements (x, y
    /// coordinates)
    function encodeG1PointForEIP2537(
        bytes memory uncompressedSignature
    ) external pure returns (bytes memory) {
        if (uncompressedSignature.length != 96) {
            revert InvalidBLSProof();
        }

        // if the provided signature is the identity, return early with 128 zero bytes
        bytes memory encoded = new bytes(EIP2537_G1_POINT_SIZE);
        if (isRawInfinityPointG1(uncompressedSignature)) return encoded;

        // otherwise, pad x and y coordinates with 16 leading zeroes
        fieldElementToEIP2537Bytes(uncompressedSignature, 0, encoded, 0);
        fieldElementToEIP2537Bytes(uncompressedSignature, 48, encoded, 64);

        return encoded;
    }

    /// @dev Encode a BLS12-381 G2 point (public key for min_sig) to EIP2537 format (256 bytes)
    /// @param uncompressedPubkey 192-byte uncompressed G2 point
    /// @return _ EIP2537-compliant, 256-byte encoded G1 point comprising four 64-byte padded field elements (x, y
    /// coordinates)
    /// @notice Coordinate reordering is performed because EIP2537 precompiles expect: `x.c0 || x.c1 || y.c0 || y.c1`
    /// This differs from some bls libraries (such as blst used by RL) which serialize to `x.c1 || x.c0 || y.c1 || y.c0`
    function encodeG2PointForEIP2537(
        bytes memory uncompressedPubkey
    ) external pure returns (bytes memory) {
        if (uncompressedPubkey.length != 192) {
            revert InvalidUncompressedBLSPubkey();
        }

        // if the provided public key is the identity, return early with 256 zero bytes
        bytes memory encoded = new bytes(EIP2537_G2_POINT_SIZE);
        if (isRawInfinityPointG2(uncompressedPubkey)) return encoded;

        // reorder blst serialization of coordinates for EIP2537 compliance: `x.c0 || x.c1 || y.c0 || y.c1`
        // x.c0 (second 48 bytes of x coordinate, bytes 48-96)
        fieldElementToEIP2537Bytes(uncompressedPubkey, 48, encoded, 0);
        // x.c1 (first 48 bytes of x, bytes 0-48)
        fieldElementToEIP2537Bytes(uncompressedPubkey, 0, encoded, 64);
        // y.c0 (second 48 bytes of y, bytes 144-192)
        fieldElementToEIP2537Bytes(uncompressedPubkey, 144, encoded, 128);
        // y.c1 (first 48 bytes of y, bytes 96-144)
        fieldElementToEIP2537Bytes(uncompressedPubkey, 96, encoded, 192);

        return encoded;
    }

    /// @dev Validates an EIP2537-compliant G1 point by adding it to the identity using G1_ADD
    /// @return _ False if not 128 bytes in length, is not on the curve, or is the identity point
    function validatePointG1(bytes memory point) external view returns (bool) {
        if (point.length != EIP2537_G1_POINT_SIZE || isInfinityPointG1(point)) {
            revert InvalidPoint(point.length, EIP2537_G1_POINT_SIZE);
        }
        return keccak256(addG1(point, G1_IDENTITY)) == keccak256(point);
    }

    /// @dev Validates an EIP2537-compliant G2 point by adding it to the identity using G2_ADD
    /// @return _ False if not 256 bytes in length, is not on the curve, or is the identity point
    function validatePointG2(bytes memory point) external view returns (bool) {
        if (point.length != EIP2537_G2_POINT_SIZE || isInfinityPointG2(point)) {
            revert InvalidPoint(point.length, EIP2537_G2_POINT_SIZE);
        }
        return keccak256(addG2(point, G2_IDENTITY)) == keccak256(point);
    }

    /// @dev Checks if an EIP2537-compliant G1 point (128 bytes) is the identity
    /// @param uncompressedPointG1 Is not validated to be a G1 point on the curve; this must be enforced separately
    function isInfinityPointG1(
        bytes memory uncompressedPointG1
    ) public pure returns (bool) {
        return keccak256(uncompressedPointG1) == keccak256(G1_IDENTITY);
    }

    /// @dev Checks if an EIP2537-compliant G2 point (256 bytes) is the identity
    /// @param uncompressedPointG2 Is not validated to be a G2 point on the curve; this must be enforced separately
    function isInfinityPointG2(
        bytes memory uncompressedPointG2
    ) public pure returns (bool) {
        return keccak256(uncompressedPointG2) == keccak256(G2_IDENTITY);
    }

    /// @dev Checks if a raw (unpadded, 96-byte) G1 point is the identity
    function isRawInfinityPointG1(
        bytes memory rawPointG1
    ) public pure returns (bool) {
        return keccak256(rawPointG1) == keccak256(G1_IDENTITY_RAW);
    }

    /// @dev Checks if a raw (unpadded, 192-byte) G2 point is the identity
    function isRawInfinityPointG2(
        bytes memory rawPointG2
    ) public pure returns (bool) {
        return keccak256(rawPointG2) == keccak256(G2_IDENTITY_RAW);
    }

    /**
     * @notice Provided parameters must be uncompressed and EIP2537-compliant due to BLS12-381 precompile limitations
     * Uses G2 group for public keys (256bytes for EIP2537) and G1 group for signature (128bytes for EIP2537)
     * Validators must sign over `prefixA || BLS pubkey || prefixB || validatorAddress` to ensure possession
     * Where `prefixA = POP_INTENT_PREFIX && prefixB = ADDRESS_LEN_PREFIX` (constants inserted by rust protocol
     * encoding)
     * Flow:
     * 1. hash `msg = concat(validatorAddress, blsPubkey)` to G1 curve using the RFC9380 hash-to-curve spec
     *   a. hash msg with domain separator to obtain two field elements in the BLS12-381 base field:
     *      `fieldElements = hashToField(msg, domainSeparator, 2)`
     *   b. map each fp field element to a point on the G1 curve using the MAP_FP_TO_G1 precompile:
     *      `point = mapFieldElementToG1(fp)`
     *   c. add the two resulting G1 curve points using the G1_ADD precompile:
     *      `msgPointHash = addG1(point0, point1)`
     * 2. check BLS signature using PAIRING_CHECK precompile:
     *      `concat(msgPointHash, blsPubkey, signature, G2_GENERATOR_NEG)`
     */
    function verifyProofOfPossessionG1(
        bytes memory blsPubkey,
        bytes memory signature,
        bytes memory message
    ) external view returns (bool) {
        // EIP2537 precompiles handle byte length and curve validity checks so only check identity
        if (isInfinityPointG2(blsPubkey)) revert InvalidBLSPubkey();
        if (isInfinityPointG1(signature)) revert InvalidBLSProof();

        // EIP2537 pairing precompile enforces valid points
        bytes memory messagePointHash = hashToG1(message);
        bytes memory input = bytes.concat(
            messagePointHash,
            blsPubkey,
            signature,
            G2_GENERATOR_NEG
        );
        (bool r, bytes memory res) = PAIRING_CHECK.staticcall(input);
        if (!r) revert LowLevelCallFailure(res);

        /// @dev Pairing check precompile returns single EVM word
        return uint256(bytes32(res)) == 1;
    }

    /// @dev Hash to a point on the G1 curve using hash-to-curve method outlined by RFC9380::8.8.1
    /// https://datatracker.ietf.org/doc/html/rfc9380#section-3-4.2.1
    /// @return _ The deterministic point on G1 curve derived from `input`
    function hashToG1(bytes memory input) public view returns (bytes memory) {
        Fp[] memory fieldElements = hashToField(input, HASH_TO_G1_DST, 2);
        bytes memory point0 = mapFieldElementToG1(fieldElements[0]);
        bytes memory point1 = mapFieldElementToG1(fieldElements[1]);

        return addG1(point0, point1);
    }

    /// @dev Maps a field element to a point on the G1 curve using the map-to-curve precompile
    /// @param element The field element to map
    /// @return _ The mapped G1 point
    function mapFieldElementToG1(
        Fp memory element
    ) public view returns (bytes memory) {
        (bool r, bytes memory res) = MAP_FP_TO_G1.staticcall(element.data);
        if (!r) revert LowLevelCallFailure(res);

        return res;
    }

    /// @dev Adds two G1 curve points using the G1 addition precompile
    /// @return _ The G1 point addition result
    function addG1(
        bytes memory point0,
        bytes memory point1
    ) public view returns (bytes memory) {
        (bool r, bytes memory res) = G1_ADD.staticcall(
            bytes.concat(point0, point1)
        );
        if (!r) revert LowLevelCallFailure(res);

        return res;
    }

    /// @dev Adds two G2 curve points using the G2 addition precompile
    /// @return _ The G2 point addition result
    function addG2(
        bytes memory point0,
        bytes memory point1
    ) public view returns (bytes memory) {
        (bool r, bytes memory res) = BlsG1.G2_ADD.staticcall(
            bytes.concat(point0, point1)
        );
        if (!r) revert BlsG1.LowLevelCallFailure(res);

        return res;
    }

    /// @dev Hash input bytes to field element(s) in BLS12-381 base field Fp as outlined by RFC9380::5.2
    /// https://datatracker.ietf.org/doc/html/rfc9380#name-hash_to_field-implementation
    /// @notice Extension degree of field `m = 1` thus simpler `sgn0_m_eq_1(x)` applies
    /// @param input The value to hash
    /// @param dst Domain separator tag to prevent collisions between different hash usages
    /// @param count The number of field elements to generate
    /// @return _ Resulting field elements array
    function hashToField(
        bytes memory input,
        bytes memory dst,
        uint256 count
    ) public view returns (Fp[] memory) {
        Fp[] memory result = new Fp[](count);
        // supports up to 1023 field elements although we only use 2 for PoP
        if (count > type(uint16).max / L) revert CountTooLarge(count);
        // `len_in_bytes = count * m * L`
        uint16 bytesLen = uint16(count * L);
        // `uniform_bytes = expand_message(msg, DST, len_in_bytes)`
        bytes memory uniformBytes = expandMessageXmd(input, dst, bytesLen);
        for (uint256 i; i < count; i++) {
            result[i] = processFieldElement(uniformBytes, i);
        }

        return result;
    }

    /// @notice Expands a message using the expand_message_xmd method as per RFC9380::5.3.1
    /// https://datatracker.ietf.org/doc/html/rfc9380#name-expand_message_xmd
    /// @param message The input message as bytes
    /// @param dst The domain separation tag as string of at most 255 bytes
    /// @param outputLen The desired output length of the expanded message bytes
    /// @return result The expanded message as bytes
    function expandMessageXmd(
        bytes memory message,
        bytes memory dst,
        uint16 outputLen
    ) public pure returns (bytes memory result) {
        // ell = ceil(len_in_bytes / b_in_bytes)
        uint256 ell = (outputLen + B_IN_BYTES - 1) / B_IN_BYTES;

        // ABORT if ell > 255 or len_in_bytes > 65535 or len(DST) > 255
        if (ell > type(uint8).max) revert EllTooLarge(ell);
        if (outputLen > type(uint16).max) revert LengthTooLarge(outputLen);
        if (bytes(dst).length > type(uint8).max)
            revert DSTTooLong(bytes(dst).length);

        bytes memory dstPrime;
        bytes32 b0;
        {
            // DST_prime = DST || I2OSP(len(DST), 1)
            bytes memory dstBytes = bytes(dst);
            dstPrime = bytes.concat(dstBytes, I2OSP(dstBytes.length, 1));

            // Z_pad = I2OSP(0, s_in_bytes)
            // l_i_b_str = I2OSP(len_in_bytes, 2)
            bytes memory Z_pad = I2OSP(0, S_IN_BYTES);
            bytes memory l_i_b_str = I2OSP(uint256(outputLen), 2);

            // msg_prime = Z_pad || msg || l_i_b_str || I2OSP(0, 1) || dstPrime
            bytes memory msgPrime = bytes.concat(
                Z_pad,
                message,
                l_i_b_str,
                I2OSP(0, 1),
                dstPrime
            );

            // b_0 = H(msg_prime)
            b0 = sha256(msgPrime);
        }

        {
            // b_1 = H(b_0 || I2OSP(1, 1) || DST_prime)
            // RFC 9380 concatenation: b0 is 32 bytes and I2OSP(1, 1) is always 1 byte, so dstPrime is the
            // only variable-length part and the packing is unambiguous.
            // slither-disable-next-line encode-packed-collision
            bytes32 b1 = sha256(abi.encodePacked(b0, I2OSP(1, 1), dstPrime));

            // produce uniformly random byte string ie `uniform_bytes = b_1 || ..b_i.. || b_ell`
            bytes memory uniformBytes = abi.encodePacked(b1);
            bytes32 prevBlock = b1;
            // compute b_i for i = 2 to ell
            for (uint256 i = 2; i <= ell; i++) {
                // strxor(b0, b_{i-1})
                bytes32 xorInput = b0 ^ prevBlock;
                bytes memory iBytes = I2OSP(i, 1);
                // b_i = H(strxor(b_0, b_(i - 1)) || I2OSP(i, 1) || DST_prime)
                bytes32 bi = sha256(bytes.concat(xorInput, iBytes, dstPrime));

                // append b_i to uniform_bytes and update prev_block
                uniformBytes = abi.encodePacked(uniformBytes, bi);
                prevBlock = bi;
            }

            result = uniformBytes;
        }

        // return substr(uniform_bytes, 0, len_in_bytes)
        assembly {
            // note truncate to outputLen, leave dirty memory
            mstore(result, outputLen)
        }
    }

    /// @dev Convert nonnegative integer to its big-endian byte representation as per RFC8017::4.1
    /// https://datatracker.ietf.org/doc/html/rfc8017#section-4.1
    /// @param x Nonnegative integer to convert
    /// @param xLen Length for the output octet byte array
    /// @return _ The resulting octet byte array
    function I2OSP(uint256 x, uint256 xLen) public pure returns (bytes memory) {
        // enforce `x < 256 ** xLen`; if `xLen >= 32` then any `uint256 x` is valid
        if (xLen < 32 && x >= 256 ** xLen) {
            revert I2OSPIntegerTooLarge(x);
        }

        bytes memory octet = new bytes(xLen);
        for (uint256 i; i < xLen; i++) {
            // write in big-endian order
            octet[xLen - 1 - i] = bytes1(uint8(x >> (8 * i)));
        }

        return octet;
    }

    /// @dev Extract and process a single field element from uniform bytes
    /// @param uniformBytes The uniform random bytes from an `expand_message` schema (we use xmd)
    /// @param index The index of the field element to extract
    /// @return _ The processed field element
    function processFieldElement(
        bytes memory uniformBytes,
        uint256 index
    ) public view returns (Fp memory) {
        // extract 64 bytes starting at `elm_offset = L * (j + i * m)` where `m = 1` and `j = 0`
        uint256 elmOffset = L * index;
        // `tv = substr(uniform_bytes, elm_offset, L)`
        bytes memory tv = extractBytes(uniformBytes, elmOffset, L);
        // reduce modulo p to ensure the result is a valid field element ie `e_j = OS2IP(tv) mod p`
        bytes memory ej = Math.modExp(tv, hex"01", P);
        // pad to 64 bytes with leading zeroes
        bytes memory data = new bytes(L);
        fieldElementToEIP2537Bytes(ej, 0, data, 0);

        return Fp(data);
    }

    /// @dev Extract a slice of bytes from a larger byte array
    /// @param source The source byte array
    /// @param offset Starting position in the source array
    /// @param length Number of bytes to extract
    /// @return _ The extracted bytes
    function extractBytes(
        bytes memory source,
        uint256 offset,
        uint256 length
    ) public pure returns (bytes memory) {
        if (offset + length > source.length) revert LengthTooLarge(length);

        bytes memory result = new bytes(length);
        for (uint256 i; i < length; ++i) {
            result[i] = source[offset + i];
        }

        return result;
    }

    /// @dev Pads a 48-byte BLS12-381 field element to 64 bytes with 16 leading zeros for EIP2537
    /// @param source Source bytes containing the field element
    /// @param sourceOffset Offset in source where the 48-byte field element starts
    /// @param dest Destination bytes to write the padded element.
    /// @param destOffset Offset in dest where the 64-byte padded element should start,
    /// meaning the bytes in range `dest[destOffset:destOffset + 16]` must be zero!
    function fieldElementToEIP2537Bytes(
        bytes memory source,
        uint256 sourceOffset,
        bytes memory dest,
        uint256 destOffset
    ) public pure {
        if (sourceOffset + FIELD_ELEMENT_SIZE > source.length)
            revert InvalidFpOffset();
        if (destOffset + EIP2537_FIELD_ELEMENT_SIZE > dest.length)
            revert InvalidFpOffset();

        for (uint256 i; i < FIELD_ELEMENT_SIZE; ++i) {
            dest[destOffset + 16 + i] = source[sourceOffset + i];
        }
    }

    /// @dev Extracts a 48-byte BLS12-381 field element from EIP2537 64-byte padded format
    /// @param paddedElement 64-byte EIP2537 padded field element (16 zeros + 48 bytes)
    /// @return fieldElement 48-byte field element
    function eip2537BytesToFieldElement(
        bytes memory paddedElement
    ) external pure returns (bytes memory fieldElement) {
        if (paddedElement.length != EIP2537_FIELD_ELEMENT_SIZE) {
            revert InvalidPoint(
                paddedElement.length,
                EIP2537_FIELD_ELEMENT_SIZE
            );
        }

        // Verify the first 16 bytes are zeros (required by EIP2537)
        for (uint256 i = 0; i < 16; i++) {
            require(
                paddedElement[i] == 0,
                "Invalid padding: non-zero in first 16 bytes"
            );
        }

        // Extract bytes 16-63 (the actual field element)
        return extractBytes(paddedElement, 16, 48);
    }
}
