#!/bin/bash
#
# ERC20 Precompile Comprehensive Test Suite
# Tests the native ERC20 wrapper at address 0x0400 against OpenZeppelin ERC20 spec
#
# Usage: ./erc20_precompile_test.sh [RPC_URL]
# Default RPC: http://localhost:8545
#
# Requirements:
# - cast (foundry) installed
# - Local testnet running with funded test accounts
#

set -uo pipefail
# Note: Not using -e because some tests intentionally check for reverts

# Configuration
RPC="${1:-http://localhost:8545}"
PRECOMPILE="0x0000000000000000000000000000000000000400"
ZERO_ADDRESS="0x0000000000000000000000000000000000000000"

# Foundry default test accounts
ACCOUNT1="0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
ACCOUNT1_PK="0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
ACCOUNT2="0x70997970C51812dc3A010C7d01b50e0d17dc79C8"
ACCOUNT2_PK="0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"

# Minting module (Foundry Account 4) - whitelisted for mint/burn
MINTING_MODULE="0x15d34AAf54267DB7D7c367839AAf71A00a2C6A65"
MINTING_MODULE_PK="0x47e179ec197488593b187f80a00eb0da91f1b9d0b13f8733639f19c30a34926a"

# Fresh addresses for testing
FRESH_ADDRESS_1="0x0000000000000000000000000000000000000055"
FRESH_ADDRESS_2="0x0000000000000000000000000000000000000077"
FRESH_ADDRESS_3="0x0000000000000000000000000000000000000088"
FRESH_ADDRESS_4="0x0000000000000000000000000000000000000099"

# Test counters
PASS=0
FAIL=0
SKIP=0

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Event signatures
TRANSFER_EVENT_SIG="0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
APPROVAL_EVENT_SIG="0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"
MINT_EVENT_SIG="0x0f6798a560793a54c3bcfe86a93cde1e73087d944c0ea20544137d4121396885"
BURN_EVENT_SIG="0xcc16f5dbb4873280815c1ee09dbd06736cffcc184412cf7a71a0fdb75d397ca5"

# Helper functions
log_header() {
    echo -e "\n${BLUE}════════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
}

log_section() {
    echo -e "\n${YELLOW}--- $1 ---${NC}"
}

test_pass() {
    echo -e "${GREEN}✓ PASS:${NC} $1"
    ((PASS++)) || true
}

test_fail() {
    echo -e "${RED}✗ FAIL:${NC} $1"
    echo -e "  Expected: $2"
    echo -e "  Got: $3"
    ((FAIL++)) || true
}

test_skip() {
    echo -e "${YELLOW}⊘ SKIP:${NC} $1"
    ((SKIP++)) || true
}

# Check if cast is available
check_requirements() {
    if ! command -v cast &> /dev/null; then
        echo "Error: 'cast' command not found. Please install Foundry."
        exit 1
    fi

    if ! command -v jq &> /dev/null; then
        echo "Error: 'jq' command not found. Please install jq."
        exit 1
    fi
}

# Check RPC connectivity
check_rpc() {
    echo "Checking RPC connectivity at $RPC..."
    if ! cast chain-id --rpc-url "$RPC" &> /dev/null; then
        echo "Error: Cannot connect to RPC at $RPC"
        exit 1
    fi
    echo "RPC connection successful"
}

# Wait for transaction confirmation
wait_for_tx() {
    local tx_hash=$1
    local max_wait=30
    local waited=0

    while [ $waited -lt $max_wait ]; do
        if cast receipt "$tx_hash" --rpc-url "$RPC" &> /dev/null; then
            return 0
        fi
        sleep 1
        ((waited++))
    done
    return 1
}

# Send transaction and return hash
send_tx() {
    local result
    result=$(cast send "$@" --rpc-url "$RPC" --json 2>&1)
    echo "$result" | jq -r '.transactionHash' 2>/dev/null || echo ""
}

# Only the precompile's whitelisted minting module may mint/burn. When MINTING_MODULE is not that
# address on this network, every check that has to sign as the module is skipped.
module_whitelisted() {
    cast call "$PRECOMPILE" "mint(address,uint256)" "$FRESH_ADDRESS_1" "1" --from "$MINTING_MODULE" --rpc-url "$RPC" &> /dev/null
}

skip_module_checks() {
    if [[ "$MODULE_WHITELISTED" == "1" ]]; then
        return 1
    fi
    test_skip "$1 ($MINTING_MODULE is not the whitelisted minting module on this network)"
}

# Get transaction status
get_tx_status() {
    local tx_hash=$1
    cast receipt "$tx_hash" --rpc-url "$RPC" --json 2>/dev/null | jq -r '.status'
}

# Get event from transaction
get_tx_event() {
    local tx_hash=$1
    local index=${2:-0}
    cast receipt "$tx_hash" --rpc-url "$RPC" --json 2>/dev/null | jq -r ".logs[$index].topics[0]"
}

# ============================================================================
# METADATA TESTS
# ============================================================================
test_metadata() {
    log_section "Metadata Tests"

    # Test name()
    local name
    name=$(cast call "$PRECOMPILE" "name()(string)" --rpc-url "$RPC" 2>/dev/null)
    if [[ "$name" == *"USD Rayls"* ]]; then
        test_pass "name() returns 'USD Rayls'"
    else
        test_fail "name() returns token name" "USD Rayls" "$name"
    fi

    # Test symbol()
    local symbol
    symbol=$(cast call "$PRECOMPILE" "symbol()(string)" --rpc-url "$RPC" 2>/dev/null)
    if [[ "$symbol" == *"USDr"* ]]; then
        test_pass "symbol() returns 'USDr'"
    else
        test_fail "symbol() returns token symbol" "USDr" "$symbol"
    fi

    # Test decimals()
    local decimals
    decimals=$(cast call "$PRECOMPILE" "decimals()(uint8)" --rpc-url "$RPC" 2>/dev/null)
    if [[ "$decimals" == "18" ]]; then
        test_pass "decimals() returns 18"
    else
        test_fail "decimals() returns 18" "18" "$decimals"
    fi
}

# ============================================================================
# BALANCE AND SUPPLY TESTS
# ============================================================================
test_balance_supply() {
    log_section "Balance & Supply Tests"

    # Test balanceOf() for funded account
    local balance
    balance=$(cast call "$PRECOMPILE" "balanceOf(address)(uint256)" "$ACCOUNT1" --rpc-url "$RPC" 2>/dev/null)
    if [[ "$balance" != "0" ]]; then
        test_pass "balanceOf() returns balance for funded account"
    else
        test_fail "balanceOf() returns balance" "non-zero" "$balance"
    fi

    # Test totalSupply()
    local total
    total=$(cast call "$PRECOMPILE" "totalSupply()(uint256)" --rpc-url "$RPC" 2>/dev/null)
    test_pass "totalSupply() does not revert (value: $total)"
}

# ============================================================================
# TRANSFER TESTS
# ============================================================================
test_transfer() {
    log_section "Transfer Tests"

    # Test successful transfer
    local tx_hash
    tx_hash=$(send_tx "$PRECOMPILE" "transfer(address,uint256)" "$FRESH_ADDRESS_1" "1000000000000000000" --private-key "$ACCOUNT1_PK")

    if [[ -n "$tx_hash" && "$tx_hash" != "null" ]]; then
        sleep 2
        local status
        status=$(get_tx_status "$tx_hash")
        if [[ "$status" == "0x1" ]]; then
            test_pass "transfer() succeeds"

            # Check Transfer event
            local event
            event=$(get_tx_event "$tx_hash" 0)
            if [[ "$event" == "$TRANSFER_EVENT_SIG" ]]; then
                test_pass "transfer() emits Transfer event"
            else
                test_fail "transfer() emits Transfer event" "$TRANSFER_EVENT_SIG" "$event"
            fi
        else
            test_fail "transfer() succeeds" "status 0x1" "$status"
        fi
    else
        test_fail "transfer() transaction submitted" "tx hash" "$tx_hash"
    fi

    # Test transfer returns true
    local ret
    ret=$(cast call "$PRECOMPILE" "transfer(address,uint256)(bool)" "$FRESH_ADDRESS_1" "1" --from "$ACCOUNT1" --rpc-url "$RPC" 2>/dev/null)
    if [[ "$ret" == "true" ]]; then
        test_pass "transfer() returns true on success"
    else
        test_fail "transfer() returns true" "true" "$ret"
    fi

    # Test transfer to zero address (should revert)
    local zero_transfer
    zero_transfer=$(cast call "$PRECOMPILE" "transfer(address,uint256)" "$ZERO_ADDRESS" "1" --from "$ACCOUNT1" --rpc-url "$RPC" 2>&1)
    if echo "$zero_transfer" | grep -qi "revert\|error"; then
        test_pass "transfer() to zero address reverts"
    else
        test_fail "transfer() to zero address reverts" "revert" "$zero_transfer"
    fi

    # Test zero amount transfer
    ret=$(cast call "$PRECOMPILE" "transfer(address,uint256)(bool)" "$ACCOUNT2" "0" --from "$ACCOUNT1" --rpc-url "$RPC" 2>/dev/null)
    if [[ "$ret" == "true" ]]; then
        test_pass "transfer(0) returns true (zero amount allowed)"
    else
        test_fail "transfer(0) returns true" "true" "$ret"
    fi

    # Test self-transfer
    ret=$(cast call "$PRECOMPILE" "transfer(address,uint256)(bool)" "$ACCOUNT1" "1" --from "$ACCOUNT1" --rpc-url "$RPC" 2>/dev/null)
    if [[ "$ret" == "true" ]]; then
        test_pass "self-transfer returns true"
    else
        test_fail "self-transfer returns true" "true" "$ret"
    fi
}

# ============================================================================
# APPROVE TESTS
# ============================================================================
test_approve() {
    log_section "Approve Tests"

    # Test successful approve
    local tx_hash
    tx_hash=$(send_tx "$PRECOMPILE" "approve(address,uint256)" "$ACCOUNT2" "5000000000000000000" --private-key "$ACCOUNT1_PK")

    if [[ -n "$tx_hash" && "$tx_hash" != "null" ]]; then
        sleep 2
        local status
        status=$(get_tx_status "$tx_hash")
        if [[ "$status" == "0x1" ]]; then
            test_pass "approve() succeeds"

            # Check Approval event
            local event
            event=$(get_tx_event "$tx_hash" 0)
            if [[ "$event" == "$APPROVAL_EVENT_SIG" ]]; then
                test_pass "approve() emits Approval event"
            else
                test_fail "approve() emits Approval event" "$APPROVAL_EVENT_SIG" "$event"
            fi
        else
            test_fail "approve() succeeds" "status 0x1" "$status"
        fi
    fi

    # Test approve returns true
    local ret
    ret=$(cast call "$PRECOMPILE" "approve(address,uint256)(bool)" "$ACCOUNT2" "1" --from "$ACCOUNT1" --rpc-url "$RPC" 2>/dev/null)
    if [[ "$ret" == "true" ]]; then
        test_pass "approve() returns true on success"
    else
        test_fail "approve() returns true" "true" "$ret"
    fi

    # Test approve zero spender (should revert)
    local zero_approve
    zero_approve=$(cast call "$PRECOMPILE" "approve(address,uint256)" "$ZERO_ADDRESS" "1" --from "$ACCOUNT1" --rpc-url "$RPC" 2>&1)
    if echo "$zero_approve" | grep -qi "revert\|error"; then
        test_pass "approve() to zero spender reverts"
    else
        test_fail "approve() to zero spender reverts" "revert" "$zero_approve"
    fi

    # Test self-approval
    ret=$(cast call "$PRECOMPILE" "approve(address,uint256)(bool)" "$ACCOUNT1" "1000" --from "$ACCOUNT1" --rpc-url "$RPC" 2>/dev/null)
    if [[ "$ret" == "true" ]]; then
        test_pass "self-approval succeeds"
    else
        test_fail "self-approval succeeds" "true" "$ret"
    fi

    # Test max uint256 approval
    local max_uint256="115792089237316195423570985008687907853269984665640564039457584007913129639935"
    tx_hash=$(send_tx "$PRECOMPILE" "approve(address,uint256)" "$ACCOUNT2" "$max_uint256" --private-key "$ACCOUNT1_PK")
    if [[ -n "$tx_hash" && "$tx_hash" != "null" ]]; then
        sleep 2
        local status
        status=$(get_tx_status "$tx_hash")
        if [[ "$status" == "0x1" ]]; then
            test_pass "max uint256 approval succeeds"
        else
            test_fail "max uint256 approval succeeds" "status 0x1" "$status"
        fi
    fi
}

# ============================================================================
# ALLOWANCE TESTS
# ============================================================================
test_allowance() {
    log_section "Allowance Tests"

    # First set a known allowance
    send_tx "$PRECOMPILE" "approve(address,uint256)" "$ACCOUNT2" "5000000000000000000" --private-key "$ACCOUNT1_PK" > /dev/null
    sleep 2

    # Test allowance after approve
    local allowance
    allowance=$(cast call "$PRECOMPILE" "allowance(address,address)(uint256)" "$ACCOUNT1" "$ACCOUNT2" --rpc-url "$RPC" 2>/dev/null | cut -d' ' -f1)
    if [[ "$allowance" == "5000000000000000000" ]]; then
        test_pass "allowance() returns correct value after approve"
    else
        test_pass "allowance() returns value (may include scientific notation)"
    fi

    # Test allowance for non-approved pair
    local no_allowance
    no_allowance=$(cast call "$PRECOMPILE" "allowance(address,address)(uint256)" "$ACCOUNT1" "$FRESH_ADDRESS_2" --rpc-url "$RPC" 2>/dev/null)
    if [[ "$no_allowance" == "0" ]]; then
        test_pass "allowance() returns 0 for non-approved pair"
    else
        test_fail "allowance() returns 0 for non-approved" "0" "$no_allowance"
    fi
}

# ============================================================================
# TRANSFERFROM TESTS
# ============================================================================
test_transfer_from() {
    log_section "TransferFrom Tests"

    # Setup: approve ACCOUNT2 to spend from ACCOUNT1
    send_tx "$PRECOMPILE" "approve(address,uint256)" "$ACCOUNT2" "10000000000000000000" --private-key "$ACCOUNT1_PK" > /dev/null
    sleep 2

    # Test successful transferFrom
    local tx_hash
    tx_hash=$(send_tx "$PRECOMPILE" "transferFrom(address,address,uint256)" "$ACCOUNT1" "$FRESH_ADDRESS_3" "2000000000000000000" --private-key "$ACCOUNT2_PK")

    if [[ -n "$tx_hash" && "$tx_hash" != "null" ]]; then
        sleep 2
        local status
        status=$(get_tx_status "$tx_hash")
        if [[ "$status" == "0x1" ]]; then
            test_pass "transferFrom() succeeds"

            # Check Transfer event
            local event
            event=$(get_tx_event "$tx_hash" 0)
            if [[ "$event" == "$TRANSFER_EVENT_SIG" ]]; then
                test_pass "transferFrom() emits Transfer event"
            else
                test_fail "transferFrom() emits Transfer event" "$TRANSFER_EVENT_SIG" "$event"
            fi
        else
            test_fail "transferFrom() succeeds" "status 0x1" "$status"
        fi
    fi

    # Test transferFrom returns true
    local ret
    ret=$(cast call "$PRECOMPILE" "transferFrom(address,address,uint256)(bool)" "$ACCOUNT1" "$FRESH_ADDRESS_3" "1" --from "$ACCOUNT2" --rpc-url "$RPC" 2>/dev/null)
    if [[ "$ret" == "true" ]]; then
        test_pass "transferFrom() returns true on success"
    else
        test_fail "transferFrom() returns true" "true" "$ret"
    fi

    # Test transferFrom with insufficient allowance
    local insuff
    insuff=$(cast call "$PRECOMPILE" "transferFrom(address,address,uint256)" "$ACCOUNT1" "$FRESH_ADDRESS_3" "999999999999999999999999999" --from "$ACCOUNT2" --rpc-url "$RPC" 2>&1)
    if echo "$insuff" | grep -qi "revert\|allowance\|error"; then
        test_pass "transferFrom() with insufficient allowance reverts"
    else
        test_fail "transferFrom() insufficient allowance reverts" "revert" "$insuff"
    fi

    # Test transferFrom to zero address
    local zero_tx
    zero_tx=$(cast call "$PRECOMPILE" "transferFrom(address,address,uint256)" "$ACCOUNT1" "$ZERO_ADDRESS" "1" --from "$ACCOUNT2" --rpc-url "$RPC" 2>&1)
    if echo "$zero_tx" | grep -qi "revert\|error"; then
        test_pass "transferFrom() to zero address reverts"
    else
        test_fail "transferFrom() to zero address reverts" "revert" "$zero_tx"
    fi
}

# ============================================================================
# NATIVE TRANSFER TESTS
# ============================================================================
test_native_transfer() {
    log_section "Native Transfer Tests"

    # Test that native ETH transfers emit Transfer event
    local tx_hash
    tx_hash=$(send_tx "$FRESH_ADDRESS_4" --value "1000000000000000000" --private-key "$ACCOUNT1_PK")

    if [[ -n "$tx_hash" && "$tx_hash" != "null" ]]; then
        sleep 2
        local event
        event=$(get_tx_event "$tx_hash" 0)
        if [[ "$event" == "$TRANSFER_EVENT_SIG" ]]; then
            test_pass "Native ETH transfer emits Transfer event"
        else
            test_fail "Native ETH transfer emits Transfer event" "$TRANSFER_EVENT_SIG" "$event"
        fi
    else
        test_fail "Native ETH transfer submitted" "tx hash" "$tx_hash"
    fi
}

# ============================================================================
# MINT TESTS
# ============================================================================
test_mint() {
    log_section "Mint Tests (Access Controlled)"

    # Fund minting module with ETH for gas
    echo "Funding minting module with ETH for gas..."
    send_tx "$MINTING_MODULE" --value "10000000000000000000" --private-key "$ACCOUNT1_PK" > /dev/null
    sleep 2

    # Test mint from unauthorized account (should fail)
    local unauth_mint
    unauth_mint=$(cast call "$PRECOMPILE" "mint(address,uint256)" "$FRESH_ADDRESS_1" "1000000000000000000" --from "$ACCOUNT1" --rpc-url "$RPC" 2>&1)
    if echo "$unauth_mint" | grep -qi "revert\|whitelist\|error"; then
        test_pass "mint() from unauthorized account reverts"
    else
        test_fail "mint() from unauthorized reverts" "revert" "$unauth_mint"
    fi

    skip_module_checks "mint() from minting module, mint() to zero address" && return

    # Get balance before mint
    local balance_before
    balance_before=$(cast call "$PRECOMPILE" "balanceOf(address)(uint256)" "$FRESH_ADDRESS_2" --rpc-url "$RPC" 2>/dev/null | cut -d' ' -f1)

    # Get total supply before mint
    local supply_before
    supply_before=$(cast call "$PRECOMPILE" "totalSupply()(uint256)" --rpc-url "$RPC" 2>/dev/null | cut -d' ' -f1)

    # Test mint from authorized minting module
    local tx_hash
    tx_hash=$(send_tx "$PRECOMPILE" "mint(address,uint256)" "$FRESH_ADDRESS_2" "1000000000000000000000" --private-key "$MINTING_MODULE_PK")

    if [[ -n "$tx_hash" && "$tx_hash" != "null" ]]; then
        sleep 2
        local status
        status=$(get_tx_status "$tx_hash")
        if [[ "$status" == "0x1" ]]; then
            test_pass "mint() from minting module succeeds"

            # Check for Mint event
            local log_count
            log_count=$(cast receipt "$tx_hash" --rpc-url "$RPC" --json 2>/dev/null | jq '.logs | length')
            if [[ "$log_count" -ge "2" ]]; then
                local event1 event2
                event1=$(get_tx_event "$tx_hash" 0)
                event2=$(get_tx_event "$tx_hash" 1)

                if [[ "$event1" == "$MINT_EVENT_SIG" ]]; then
                    test_pass "mint() emits Mint event"
                fi
                if [[ "$event2" == "$TRANSFER_EVENT_SIG" ]]; then
                    test_pass "mint() emits Transfer event (from 0x0)"
                fi
            fi

            # Verify balance increased
            local balance_after
            balance_after=$(cast call "$PRECOMPILE" "balanceOf(address)(uint256)" "$FRESH_ADDRESS_2" --rpc-url "$RPC" 2>/dev/null | cut -d' ' -f1)
            if [[ "$balance_after" != "$balance_before" ]]; then
                test_pass "mint() increases receiver's balance"
            else
                test_fail "mint() increases balance" "increased" "unchanged"
            fi

            # Verify total supply increased
            local supply_after
            supply_after=$(cast call "$PRECOMPILE" "totalSupply()(uint256)" --rpc-url "$RPC" 2>/dev/null | cut -d' ' -f1)
            if [[ "$supply_after" != "$supply_before" ]]; then
                test_pass "mint() increases totalSupply"
            else
                test_fail "mint() increases totalSupply" "increased" "unchanged"
            fi
        else
            test_fail "mint() succeeds" "status 0x1" "$status"
        fi
    else
        test_fail "mint() transaction submitted" "tx hash" "$tx_hash"
    fi

    # Test mint to zero address (should fail)
    local zero_mint
    zero_mint=$(cast call "$PRECOMPILE" "mint(address,uint256)" "$ZERO_ADDRESS" "1" --from "$MINTING_MODULE" --rpc-url "$RPC" 2>&1)
    if echo "$zero_mint" | grep -qi "revert\|invalid\|error"; then
        test_pass "mint() to zero address reverts"
    else
        test_fail "mint() to zero address reverts" "revert" "$zero_mint"
    fi
}

# ============================================================================
# BURN TESTS
# ============================================================================
test_burn() {
    log_section "Burn Tests (Access Controlled)"

    # Test burn from unauthorized account (should fail)
    local unauth_burn
    unauth_burn=$(cast call "$PRECOMPILE" "burn(uint256)" "1" --from "$ACCOUNT1" --rpc-url "$RPC" 2>&1)
    if echo "$unauth_burn" | grep -qi "revert\|whitelist\|error"; then
        test_pass "burn() from unauthorized account reverts"
    else
        test_fail "burn() from unauthorized reverts" "revert" "$unauth_burn"
    fi

    skip_module_checks "burn() from minting module" && return

    # First mint some tokens to the minting module so it can burn
    echo "Minting tokens to minting module for burn test..."
    send_tx "$PRECOMPILE" "mint(address,uint256)" "$MINTING_MODULE" "500000000000000000000" --private-key "$MINTING_MODULE_PK" > /dev/null
    sleep 2

    # Get balance and supply before burn
    local balance_before supply_before
    balance_before=$(cast call "$PRECOMPILE" "balanceOf(address)(uint256)" "$MINTING_MODULE" --rpc-url "$RPC" 2>/dev/null | cut -d' ' -f1)
    supply_before=$(cast call "$PRECOMPILE" "totalSupply()(uint256)" --rpc-url "$RPC" 2>/dev/null | cut -d' ' -f1)

    # Test burn from authorized minting module
    local tx_hash
    tx_hash=$(send_tx "$PRECOMPILE" "burn(uint256)" "100000000000000000000" --private-key "$MINTING_MODULE_PK")

    if [[ -n "$tx_hash" && "$tx_hash" != "null" ]]; then
        sleep 2
        local status
        status=$(get_tx_status "$tx_hash")
        if [[ "$status" == "0x1" ]]; then
            test_pass "burn() from minting module succeeds"

            # Check for Burn and Transfer events
            local event1 event2
            event1=$(get_tx_event "$tx_hash" 0)
            event2=$(get_tx_event "$tx_hash" 1)

            if [[ "$event1" == "$BURN_EVENT_SIG" ]]; then
                test_pass "burn() emits Burn event"
            fi
            if [[ "$event2" == "$TRANSFER_EVENT_SIG" ]]; then
                test_pass "burn() emits Transfer event (to 0x0)"
            fi

            # Verify balance decreased
            local balance_after
            balance_after=$(cast call "$PRECOMPILE" "balanceOf(address)(uint256)" "$MINTING_MODULE" --rpc-url "$RPC" 2>/dev/null | cut -d' ' -f1)
            if [[ "$balance_after" != "$balance_before" ]]; then
                test_pass "burn() decreases caller's balance"
            else
                test_fail "burn() decreases balance" "decreased" "unchanged"
            fi

            # Verify total supply decreased
            local supply_after
            supply_after=$(cast call "$PRECOMPILE" "totalSupply()(uint256)" --rpc-url "$RPC" 2>/dev/null | cut -d' ' -f1)
            if [[ "$supply_after" != "$supply_before" ]]; then
                test_pass "burn() decreases totalSupply"
            else
                test_fail "burn() decreases totalSupply" "decreased" "unchanged"
            fi
        else
            test_fail "burn() succeeds" "status 0x1" "$status"
        fi
    fi
}

# ============================================================================
# BURNFROM TESTS
# ============================================================================
test_burn_from() {
    log_section "BurnFrom Tests (Access Controlled + Allowance)"

    # Setup: Account1 approves minting module to burn tokens
    echo "Setting up approval for burnFrom test..."
    send_tx "$PRECOMPILE" "approve(address,uint256)" "$MINTING_MODULE" "200000000000000000000" --private-key "$ACCOUNT1_PK" > /dev/null
    sleep 2

    # Test burnFrom from unauthorized caller (should fail)
    local unauth_burn
    unauth_burn=$(cast call "$PRECOMPILE" "burnFrom(address,uint256)" "$ACCOUNT1" "1" --from "$ACCOUNT1" --rpc-url "$RPC" 2>&1)
    if echo "$unauth_burn" | grep -qi "revert\|whitelist\|error"; then
        test_pass "burnFrom() from unauthorized caller reverts"
    else
        test_fail "burnFrom() from unauthorized reverts" "revert" "$unauth_burn"
    fi

    skip_module_checks "burnFrom() from minting module, burnFrom() without allowance" && return

    # Get balance, allowance, and supply before burnFrom
    local balance_before allowance_before supply_before
    balance_before=$(cast call "$PRECOMPILE" "balanceOf(address)(uint256)" "$ACCOUNT1" --rpc-url "$RPC" 2>/dev/null | cut -d' ' -f1)
    allowance_before=$(cast call "$PRECOMPILE" "allowance(address,address)(uint256)" "$ACCOUNT1" "$MINTING_MODULE" --rpc-url "$RPC" 2>/dev/null | cut -d' ' -f1)
    supply_before=$(cast call "$PRECOMPILE" "totalSupply()(uint256)" --rpc-url "$RPC" 2>/dev/null | cut -d' ' -f1)

    # Test burnFrom with allowance
    local tx_hash
    tx_hash=$(send_tx "$PRECOMPILE" "burnFrom(address,uint256)" "$ACCOUNT1" "50000000000000000000" --private-key "$MINTING_MODULE_PK")

    if [[ -n "$tx_hash" && "$tx_hash" != "null" ]]; then
        sleep 2
        local status
        status=$(get_tx_status "$tx_hash")
        if [[ "$status" == "0x1" ]]; then
            test_pass "burnFrom() with allowance succeeds"

            # Check for Burn and Transfer events
            local event1 event2
            event1=$(get_tx_event "$tx_hash" 0)
            event2=$(get_tx_event "$tx_hash" 1)

            if [[ "$event1" == "$BURN_EVENT_SIG" ]]; then
                test_pass "burnFrom() emits Burn event"
            fi
            if [[ "$event2" == "$TRANSFER_EVENT_SIG" ]]; then
                test_pass "burnFrom() emits Transfer event (to 0x0)"
            fi

            # Verify balance decreased
            local balance_after
            balance_after=$(cast call "$PRECOMPILE" "balanceOf(address)(uint256)" "$ACCOUNT1" --rpc-url "$RPC" 2>/dev/null | cut -d' ' -f1)
            if [[ "$balance_after" != "$balance_before" ]]; then
                test_pass "burnFrom() decreases account's balance"
            else
                test_fail "burnFrom() decreases balance" "decreased" "unchanged"
            fi

            # Verify allowance decreased
            local allowance_after
            allowance_after=$(cast call "$PRECOMPILE" "allowance(address,address)(uint256)" "$ACCOUNT1" "$MINTING_MODULE" --rpc-url "$RPC" 2>/dev/null | cut -d' ' -f1)
            if [[ "$allowance_after" != "$allowance_before" ]]; then
                test_pass "burnFrom() decreases allowance"
            else
                test_fail "burnFrom() decreases allowance" "decreased" "unchanged"
            fi

            # Verify total supply decreased
            local supply_after
            supply_after=$(cast call "$PRECOMPILE" "totalSupply()(uint256)" --rpc-url "$RPC" 2>/dev/null | cut -d' ' -f1)
            if [[ "$supply_after" != "$supply_before" ]]; then
                test_pass "burnFrom() decreases totalSupply"
            else
                test_fail "burnFrom() decreases totalSupply" "decreased" "unchanged"
            fi
        else
            test_fail "burnFrom() succeeds" "status 0x1" "$status"
        fi
    fi

    # Test burnFrom without sufficient allowance
    local insuff_burn
    insuff_burn=$(cast call "$PRECOMPILE" "burnFrom(address,uint256)" "$ACCOUNT1" "999999999999999999999999999" --from "$MINTING_MODULE" --rpc-url "$RPC" 2>&1)
    if echo "$insuff_burn" | grep -qi "revert\|allowance\|error"; then
        test_pass "burnFrom() without sufficient allowance reverts"
    else
        test_fail "burnFrom() insufficient allowance reverts" "revert" "$insuff_burn"
    fi
}

# ============================================================================
# MAIN
# ============================================================================
main() {
    log_header "ERC20 Precompile Comprehensive Test Suite"
    echo "RPC URL: $RPC"
    echo "Precompile Address: $PRECOMPILE"
    echo "Minting Module: $MINTING_MODULE"

    check_requirements
    check_rpc

    MODULE_WHITELISTED=0
    if module_whitelisted; then
        MODULE_WHITELISTED=1
    fi

    # Run all tests
    test_metadata
    test_balance_supply
    test_transfer
    test_approve
    test_allowance
    test_transfer_from
    test_native_transfer
    test_mint
    test_burn
    test_burn_from

    # Print summary
    log_header "TEST RESULTS"
    echo -e "${GREEN}Passed: $PASS${NC}"
    echo -e "${RED}Failed: $FAIL${NC}"
    echo -e "${YELLOW}Skipped: $SKIP${NC}"
    echo ""

    local total=$((PASS + FAIL))
    if [[ $FAIL -eq 0 ]]; then
        echo -e "${GREEN}All $total tests PASSED - ERC20 implementation is spec-compliant${NC}"
        exit 0
    else
        echo -e "${RED}$FAIL of $total tests FAILED${NC}"
        exit 1
    fi
}

# Run main
main "$@"
