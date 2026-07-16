#!/bin/bash

set -e

directory=$(dirname "${BASH_SOURCE[0]}")
workingDir=$(cd "$directory" && pwd)
envPath="$workingDir/.env"
if [[ ! -e "$envPath" ]]; then
    echo "Error: .env file not found at $envPath"
    exit 1
fi
. "$envPath"

cd "$workingDir/../.."

# ---- flags -------------------------------------------------------------
# --config-only : only generate the validator keys and assemble the datadir
#                 (node-keys/, node-info.yaml, genesis/, parameters.yaml),
#                 then stop. No on-chain steps (fund / allowlist / stake) are
#                 run. Use this when the node will run on another host and/or
#                 staking will happen later. The datadir can be shipped to the
#                 target host as-is.
# --start       : (full mode only) legacy no-op, kept for backwards compat.
CONFIG_ONLY=false
START=false
while [ "$1" != "" ]; do
    case $1 in
        --config-only )
                CONFIG_ONLY=true
                ;;
        --start )
                START=true
                ;;
        * )     echo "Invalid option: $1"
                exit 1
    esac
    shift
done

# ---- BLS passphrase ----------------------------------------------------
# Sourced from .env (RL_BLS_PASSPHRASE) so each node can use its own strong
# passphrase. Falls back to "local" only as a convenience for throwaway local
# nodes -- set a real value in .env for anything you intend to keep. keytool
# reads it from the environment (default --bls-passphrase-source env).
if [ -z "$RL_BLS_PASSPHRASE" ]; then
    RL_BLS_PASSPHRASE="local"
fi
export RL_BLS_PASSPHRASE

# ADDRESS is always required: it is baked into node-info.yaml and is the
# operator address that will eventually hold the stake.
if [ -z "$ADDRESS" ]; then
    echo "Error: ADDRESS is required (the validator operator address)."
    exit 1
fi

# On-chain inputs are only needed for the full (staking) flow. In
# --config-only mode we never touch the chain, so don't prompt for them.
if [ "$CONFIG_ONLY" = false ]; then
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

# root path for all validators
DATADIR="$workingDir/local-validator"

# Use RELEASE="debug" below and remove the --release to use a debug build
RELEASE="release"
cargo build --bin rayls-network --release
# Example of using redb for the consensus DB
#cargo build --bin rayls-network --features redb --release

if [ -d "${DATADIR}" ]; then
    echo "The directory ${DATADIR} already exists -- skipping configuration"
    echo "Remove ${DATADIR} if you wish create a new configuration."
    echo ""
else
    echo "creating validator keys/info"

    # Optional externally-reachable p2p multiaddrs so other peers can dial
    # this validator once it runs on its own host. Passed through to keytool
    # exactly like the observer script does.
    KEYGEN_EXTRA_ARGS=""
    if [ -n "$RL_EXTERNAL_PRIMARY_ADDR" ]; then
        KEYGEN_EXTRA_ARGS="$KEYGEN_EXTRA_ARGS --external-primary-addr ${RL_EXTERNAL_PRIMARY_ADDR}"
    fi
    if [ -n "$RL_EXTERNAL_WORKER_ADDRS" ]; then
        KEYGEN_EXTRA_ARGS="$KEYGEN_EXTRA_ARGS --external-worker-addrs ${RL_EXTERNAL_WORKER_ADDRS}"
    fi

    ${workingDir}/../../target/${RELEASE}/rayls-network keytool generate validator \
        --datadir "${DATADIR}" \
        --address "${ADDRESS}" \
        ${KEYGEN_EXTRA_ARGS}

    # Copy the generated genesis, committee and parameters into the datadir.
    mkdir "${DATADIR}/genesis"
    echo "copying validator info to shared genesis dir"
    cp "${GENESISDIR}/genesis.yaml" "${DATADIR}/genesis"
    cp "${GENESISDIR}/committee.yaml" "${DATADIR}/genesis"
    cp "${GENESISDIR}/../parameters.yaml" "${DATADIR}/"
    echo ""
    echo ""

    if [ "$CONFIG_ONLY" = true ]; then
        echo "Config-only mode: keys and datadir prepared at:"
        echo "  ${DATADIR}"
        echo ""
        echo "Skipped funding, allowlisting and staking -- do these later"
        echo "(re-run without --config-only, or drive the on-chain steps"
        echo "manually, then activate-validator.sh)."
        echo ""
        echo "Upload the entire local-validator/ directory to the validator"
        echo "host and start the node there with the same RL_BLS_PASSPHRASE."
        exit 0
    fi

    echo "Funding address ${ADDRESS} with ${STAKE_AMOUNT} wei"
    cast send --private-key $ADMIN_PRIVATE_KEY --rpc-url $RPC_URL --value $STAKE_AMOUNT $ADDRESS

    echo "Adding validator to whitelist"
    cast send $REGISTRY_CONTRACT_ADDRESS "allowlistValidator(address)" $ADDRESS --private-key $ADMIN_PRIVATE_KEY --rpc-url $RPC_URL

    # extract stake calldata from output
    echo "Submitting stake transaction to registry contract at address ${REGISTRY_CONTRACT_ADDRESS}"
    CALLDATA_RES=$(${workingDir}/../../target/${RELEASE}/rayls-network keytool stake-calldata \
        --datadir "${DATADIR}")

    CALLDATA=$(echo "$CALLDATA_RES" | grep 'Calldata:' | awk '{print $2}')

    echo "Stake: $STAKE_AMOUNT, CallData: $CALLDATA"

    # send stake transaction
    cast send $REGISTRY_CONTRACT_ADDRESS $CALLDATA --private-key $PRIVATE_KEY --rpc-url $RPC_URL -vvvv

fi
