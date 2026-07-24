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

# registry contract address - if not supplied, use default value
if [ -z "$REGISTRY_CONTRACT_ADDRESS" ]; then
    REGISTRY_CONTRACT_ADDRESS="0x07E17e17E17e17E17e17E17E17E17e17e17E17e1"
fi


# extract stake calldata from output
echo "Submitting beginExit transaction to registry contract at address ${REGISTRY_CONTRACT_ADDRESS}"

# send beginExit transaction
cast send "$REGISTRY_CONTRACT_ADDRESS" "beginExit()" --private-key "$PRIVATE_KEY" --rpc-url "$RPC_URL"

