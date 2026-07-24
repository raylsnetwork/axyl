#!/bin/bash
#
# This script runs CI checks locally and submits a tx to testnet.
#
# This approach is adopted due to CI limitations using GitHub actions.

# Reset SECONDS to zero at the beginning of the script
SECONDS=0

set -e  # Exit immediately if a command exits with a non-zero status

# load environment variables
source .env

# Verify required variables are loaded
if [ -z "$GITHUB_ATTESTATION_PRIVATE_KEY" ]; then
    echo "Private key not set."
    exit 1
fi

# NOTE: this contract must match CI
CONTRACT_ADDRESS="0xba26a2c8e1c54d5bbb96e891fd4a9d853f438eb9"
RPC_ENDPOINT="https://rpc.testnet.rayls"
ATTEST_CALL="attestGitCommitHash(bytes20,bool)"
VERIFY_CALL="gitCommitHashAttested(bytes20)"
CHAIN_ID=2017
PRIVATE_KEY=${GITHUB_ATTESTATION_PRIVATE_KEY}
COMMIT_HASH=$(git rev-parse HEAD)
echo "attesting git hash: ${COMMIT_HASH}"

# Use cast to call the contract and return early if current HEAD attestation present
ALREADY_ATTESTED=$(cast call --rpc-url "${RPC_ENDPOINT}" \
    "${CONTRACT_ADDRESS}" "${VERIFY_CALL}" "${COMMIT_HASH}" )

# Check if the result is true (1) or false (0)
if [[ "${ALREADY_ATTESTED: -1}" == "1" ]]; then
    echo "Commit hash ${COMMIT_HASH} already attested on-chain."
    echo "Nothing to update."
    exit 0
fi

# Navigate to the project root directory for workspace, .rustfmt.toml, etc.
cd "$(dirname "$0")/.."

echo "executing bash script from $(pwd)"

# set environment
CARGO_INCREMENTAL=0 # disable incremental compilation
RUSTFLAGS="-D warnings -D unused_extern_crates"
CARGO_TERM_COLOR=always
RUST_BACKTRACE=1
CARGO_PROFILE_DEV_DEBUG=0

# Fetch the status of the git repository and filter for lines that indicate modified tracked files
MODIFIED_TRACKED_FILES=$(git status --porcelain --untracked-files=no)

# Check the output - ignore untracked files
if [ -n "$MODIFIED_TRACKED_FILES" ]; then
    echo "Error: please commit changes before attesting HEAD commit hash."
    echo "$MODIFIED_TRACKED_FILES"
    exit 1
fi

# check cargo fmt first
cargo +nightly-2025-11-04 fmt -- --check

echo "fmt passed"

#
# check clippy
#
# default features
cargo +nightly-2025-11-04 clippy --workspace -- -D warnings
# all features
cargo +nightly-2025-11-04 clippy --workspace --all-features -- -D warnings

echo "clippy for workspace: default and all features passed"

#
# run tests
#
# default features
cargo test --workspace --no-fail-fast -- --show-output
# Run tests that require credentials.
cargo test test_with_creds -- --ignored
# run the e2e restart and epoch tests, they are seperate to avoid any port/node confusion
cargo test -p e2e-tests -- --ignored
# faucet it test
cargo test -p e2e-tests --test it --features faucet --no-fail-fast -- --ignored faucet

echo "default tests and specific faucet it test passing"

#
# If we've reached this point, all checks have passed
#

# create and submit transaction
#
# Send the transaction using cast
output=$(cast send --private-key "${PRIVATE_KEY}" \
    --rpc-url "${RPC_ENDPOINT}" \
    --chain "2017" \
    --gas-limit 1000000 \
    "${CONTRACT_ADDRESS}" \
    "${ATTEST_CALL}" "${COMMIT_HASH}" "true")

# Check if the cast command was successful
if [ $? -ne 0 ]; then
    echo "failed to submit tx"
    exit 1
fi

echo "\nTransaction output: ${output}\n"

# Extract transaction hash using awk
TX_HASH=$(echo "$output" | grep 'transactionHash' | grep -v 'logs' | awk '{print $NF}')

echo "https://telscan.io/tx/${TX_HASH}"
echo "Contract state update initiated with commit hash: ${COMMIT_HASH}"
echo "Script took ${SECONDS}s to complete"
