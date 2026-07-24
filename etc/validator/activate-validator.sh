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

RL_BLS_PASSPHRASE="local"

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

while [ "$1" != "" ]; do
    case $1 in
        --start )
                START=true
                ;;
        * )     echo "Invalid option: $1"
                exit 1
    esac
    shift
done

# root path for all validators
DATADIR="$workingDir/local-validator"

# Use RELEASE="debug" below and remove the --release to use a debug build
RELEASE="release"
cargo build --bin rayls-network --release
# Example of using redb for the consensus DB
#cargo build --bin rayls-network --features redb --release

# send activate transaction

echo "Stake transaction sent, sending activate transaction"

cast send $REGISTRY_CONTRACT_ADDRESS "activate()" --private-key $PRIVATE_KEY --rpc-url $RPC_URL -vvvv


if [ "$START" = true ]; then
    echo "Starting ${VALIDATOR} in background, rpc endpoint http://localhost:$RPC_PORT"
    # -vvv for INFO, -vvvvv for TRACE, etc
    # start validator
    RL_BLS_PASSPHRASE="local" ${workingDir}/../../target/${RELEASE}/rayls-network node \
        --datadir "${DATADIR}" \
        --instance 99 \
        --metrics "127.0.0.1:9109" \
        --log.stdout.format log-fmt \
        --storage.v2 \
        --txpool.pending-max-count 1000000 \
        --txpool.pending-max-size 1242880000 \
        --txpool.basefee-max-count 1000000 \
        --txpool.basefee-max-size 20971120000 \
        --txpool.queued-max-count 1000000 \
        --txpool.queued-max-size 20971120000 \
        --txpool.max-pending-txns 1000000 \
        --txpool.max-new-txns 1000000 \
        --txpool.minimal-protocol-fee 0 \
        --txpool.max-tx-input-bytes 999999999999 \
        -vvv \
        --http
fi
