#!/bin/bash
#
# Provision a new Rayls validator.
#
#   ./create-validator.sh --config-only   Generate keys + config files only (no on-chain calls).
#                                         Use this to prepare a validator that will run on
#                                         another host and be staked/allowlisted later.
#   ./create-validator.sh                 Full flow: generate config, fund, allowlist and stake.
#                                         Intended for local testing.

set -e

usage() {
    echo "Usage: $0 [--config-only] [--help]"
    echo ""
    echo "  --config-only   Only generate keys and config files into local-validator/"
    echo "                  (and validator-bundle.tar.gz). No private keys or RPC needed."
    echo "  (no flag)       Generate config, then fund, allowlist and stake on-chain."
}

CONFIG_ONLY=false
while [ "$1" != "" ]; do
    case $1 in
        --config-only )
                CONFIG_ONLY=true
                ;;
        --start )
                # legacy no-op, use activate-validator.sh --start
                ;;
        -h | --help )
                usage
                exit 0
                ;;
        * )     echo "Invalid option: $1"
                usage
                exit 1
    esac
    shift
done

directory=$(dirname "${BASH_SOURCE[0]}")
workingDir=$(cd "$directory" && pwd)
envPath="$workingDir/.env"
if [[ ! -e "$envPath" ]]; then
    echo "Error: .env file not found at $envPath"
    exit 1
fi
# export everything from .env so rayls-network picks up RL_* variables
# (RL_BLS_PASSPHRASE, RL_EXTERNAL_PRIMARY_ADDR, RL_EXTERNAL_WORKER_ADDRS, ...)
set -a
. "$envPath"
set +a

cd "$workingDir/../.."

BUILD_CONFIG="${BUILD_CONFIG:-debug}"

# BLS keystore passphrase - the same value is required when starting the node
if [ -z "$RL_BLS_PASSPHRASE" ]; then
    if [ "$CONFIG_ONLY" = true ]; then
        echo "Error: RL_BLS_PASSPHRASE must be set in .env for --config-only."
        echo "The node will need the same passphrase at startup."
        exit 1
    fi
    echo "Warning: RL_BLS_PASSPHRASE not set, defaulting to \"local\" (throwaway nodes only)."
    RL_BLS_PASSPHRASE="local"
fi
export RL_BLS_PASSPHRASE

# ADDRESS
if [ -z "$ADDRESS" ]; then
    echo "Enter validator address:"
    read ADDRESS
    if [ -z "$ADDRESS" ]; then
        echo "Error: Validator address is required."
        exit 1
    fi
fi

# GENESISDIR
if [ -z "$GENESISDIR" ]; then
    echo "Error: GENESISDIR is required."
    exit 1
fi
for f in "${GENESISDIR}/genesis.yaml" "${GENESISDIR}/committee.yaml" "${GENESISDIR}/../parameters.yaml"; do
    if [ ! -f "$f" ]; then
        echo "Error: $f not found."
        exit 1
    fi
done

# on-chain inputs, only needed for the full flow
if [ "$CONFIG_ONLY" = false ]; then
    # ADMIN PRIVATE KEY
    if [ -z "$ADMIN_PRIVATE_KEY" ]; then
        echo "Enter admin private key:"
        read ADMIN_PRIVATE_KEY
        if [ -z "$ADMIN_PRIVATE_KEY" ]; then
            echo "Error: Admin private key is required."
            exit 1
        fi
    fi

    # PRIVATE KEY
    if [ -z "$PRIVATE_KEY" ]; then
        echo "Enter private key:"
        read PRIVATE_KEY
        if [ -z "$PRIVATE_KEY" ]; then
            echo "Error: Private key is required."
            exit 1
        fi
    fi

    # RPC_URL
    if [ -z "$RPC_URL" ]; then
        echo "Enter RPC URL:"
        read RPC_URL
        if [ -z "$RPC_URL" ]; then
            echo "Error: RPC URL is required."
            exit 1
        fi
    fi

    # STAKE_AMOUNT
    if [ -z "$STAKE_AMOUNT" ]; then
        echo "Enter stake amount:"
        read STAKE_AMOUNT
        if [ -z "$STAKE_AMOUNT" ]; then
            echo "Error: Stake amount is required."
            exit 1
        fi
    fi

    # registry contract address - if not supplied, use default value
    if [ -z "$REGISTRY_CONTRACT_ADDRESS" ]; then
        REGISTRY_CONTRACT_ADDRESS="0x07E17e17E17e17E17e17E17E17E17e17e17E17e1"
    fi
fi

# root path for the validator
DATADIR="$workingDir/local-validator"
BUNDLE="$workingDir/validator-bundle.tar.gz"
RAYLS_BIN="${workingDir}/../../target/${BUILD_CONFIG}/rayls-network"

BUILD_ARGS=(
    "-p"
    "rayls-network"
    "--bin"
    "rayls-network"
)
if [[ -n "$COMPILER_THREADS" ]]; then
    BUILD_ARGS+=( "-j" "$COMPILER_THREADS" )
fi
if [[ "$BUILD_CONFIG" = "release" ]]; then
    BUILD_ARGS+=( "--release" )
fi
RUSTFLAGS="-C target-cpu=native" cargo build "${BUILD_ARGS[@]}"
# Example of using redb for the consensus DB
#cargo build --bin rayls-network --features redb --release

if [ -d "${DATADIR}" ]; then
    echo "The directory ${DATADIR} already exists -- skipping configuration"
    echo "Remove ${DATADIR} if you wish create a new configuration."
    echo ""
    exit 0
fi

echo "creating validator keys/info"
"${RAYLS_BIN}" keytool generate validator \
    --datadir "${DATADIR}" \
    --address "${ADDRESS}"

# Copy the genesis, committee and parameters to the validator.
mkdir "${DATADIR}/genesis"
echo "copying genesis files to ${DATADIR}"
cp "${GENESISDIR}/genesis.yaml" "${DATADIR}/genesis"
cp "${GENESISDIR}/committee.yaml" "${DATADIR}/genesis"
cp "${GENESISDIR}/../parameters.yaml" "${DATADIR}/"

# stake calldata only depends on node-info.yaml (public BLS key + proof of possession)
CALLDATA_RES=$("${RAYLS_BIN}" keytool stake-calldata --datadir "${DATADIR}")
CALLDATA=$(echo "$CALLDATA_RES" | grep 'Calldata:' | awk '{print $2}')
echo "$CALLDATA" > "${DATADIR}/stake-calldata.txt"
echo ""

if [ "$CONFIG_ONLY" = true ]; then
    tar -czf "${BUNDLE}" -C "${workingDir}" "$(basename "${DATADIR}")"
    echo "Validator config generated in ${DATADIR}"
    echo "  node-info.yaml        public validator info (share with the network operator)"
    echo "  node-keys/            BLS + network keys (keep private, BLS key is encrypted with RL_BLS_PASSPHRASE)"
    echo "  genesis/, parameters.yaml"
    echo "  stake-calldata.txt    calldata for ConsensusRegistry.stake(...)"
    echo ""
    echo "Bundle for upload: ${BUNDLE}"
    echo "Start the node on the target host with the same RL_BLS_PASSPHRASE."
    echo "Allowlisting, staking and activation are done later (see README.md)."
    exit 0
fi

echo "Funding address ${ADDRESS} with ${STAKE_AMOUNT} wei"
cast send --private-key "$ADMIN_PRIVATE_KEY" --rpc-url "$RPC_URL" --value "$STAKE_AMOUNT" "$ADDRESS"

echo "Adding validator to whitelist"
cast send "$REGISTRY_CONTRACT_ADDRESS" "allowlistValidator(address)" "$ADDRESS" --private-key "$ADMIN_PRIVATE_KEY" --rpc-url "$RPC_URL"

# Get RLS token contract address from registry
RLS_TOKEN=$(cast call --rpc-url "$RPC_URL" "$REGISTRY_CONTRACT_ADDRESS" "rlsToken()(address)" 2>&1)
echo "RLS token contract: $RLS_TOKEN"

# Get the required stake amount from the current stake config
STAKE_CONFIG=$(cast call --rpc-url "$RPC_URL" "$REGISTRY_CONTRACT_ADDRESS" "getCurrentStakeConfig()(uint256,uint256,uint32)" 2>&1)
REQUIRED_STAKE=$(echo "$STAKE_CONFIG" | head -1 | sed 's/\[.*\]//;s/ //g')
echo "Required stake amount: $REQUIRED_STAKE"

# Mint RLS tokens to validator address (admin has MINTER_ROLE)
echo "Minting $REQUIRED_STAKE RLS tokens to validator address ${ADDRESS}"
cast send "$RLS_TOKEN" \
  "mint(address,uint256)" \
  "$ADDRESS" \
  "$REQUIRED_STAKE" \
  --private-key "$ADMIN_PRIVATE_KEY" --rpc-url "$RPC_URL"

# Approve the registry to spend the RLS tokens
echo "Approving registry to spend $REQUIRED_STAKE RLS tokens"
cast send "$RLS_TOKEN" \
  "approve(address,uint256)(bool)" \
  "$REGISTRY_CONTRACT_ADDRESS" \
  "$REQUIRED_STAKE" \
  --private-key "$PRIVATE_KEY" --rpc-url "$RPC_URL"

echo "Submitting stake transaction to registry contract at address ${REGISTRY_CONTRACT_ADDRESS}"
echo "Stake: $REQUIRED_STAKE, CallData: $CALLDATA"

# send stake transaction
cast send "$REGISTRY_CONTRACT_ADDRESS" "$CALLDATA" --private-key "$PRIVATE_KEY" --rpc-url "$RPC_URL" -vvvv
