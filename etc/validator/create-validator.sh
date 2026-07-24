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

if [ -d "${DATADIR}" ]; then
    echo "The directory ${DATADIR} already exists -- skipping configuration"
    echo "Remove ${DATADIR} if you wish create a new configuration."
    echo ""
else
    echo "creating validator keys/info"
    RL_BLS_PASSPHRASE="local" ${workingDir}/../../target/${RELEASE}/rayls-network keytool generate validator \
        --datadir "${DATADIR}" \
        --address "${ADDRESS}"

    # Copy the generated genesis, committee and parameters to each validator.
    mkdir "${DATADIR}/genesis"
    # cp validator info into shared genesis dir
    echo "copying validator info to shared genesis dir"
    cp "${GENESISDIR}/genesis.yaml" "${DATADIR}/genesis"
    cp "${GENESISDIR}/committee.yaml" "${DATADIR}/genesis"
    cp "${GENESISDIR}/../parameters.yaml" "${DATADIR}/"
    echo ""
    echo ""

    echo "Funding address ${ADDRESS} with ${STAKE_AMOUNT} wei"
    cast send --private-key $ADMIN_PRIVATE_KEY --rpc-url $RPC_URL --value $STAKE_AMOUNT $ADDRESS

    echo "Adding validator to whitelist"
    cast send $REGISTRY_CONTRACT_ADDRESS "allowlistValidator(address)" $ADDRESS --private-key $ADMIN_PRIVATE_KEY --rpc-url $RPC_URL

    # Get RLS token contract address from registry
    RLS_TOKEN=$(cast call --rpc-url $RPC_URL $REGISTRY_CONTRACT_ADDRESS "rlsToken()(address)" 2>&1)
    echo "RLS token contract: $RLS_TOKEN"

    # Get the required stake amount from the current stake config
    STAKE_CONFIG=$(cast call --rpc-url $RPC_URL $REGISTRY_CONTRACT_ADDRESS "getCurrentStakeConfig()(uint256,uint256,uint32)" 2>&1)
    REQUIRED_STAKE=$(echo "$STAKE_CONFIG" | head -1 | sed 's/\[.*\]//;s/ //g')
    echo "Required stake amount: $REQUIRED_STAKE"

    # Mint RLS tokens to validator address (admin has MINTER_ROLE)
    echo "Minting $REQUIRED_STAKE RLS tokens to validator address ${ADDRESS}"
    cast send $RLS_TOKEN \
      "mint(address,uint256)" \
      $ADDRESS \
      $REQUIRED_STAKE \
      --private-key $ADMIN_PRIVATE_KEY --rpc-url $RPC_URL

    # Approve the registry to spend the RLS tokens
    echo "Approving registry to spend $REQUIRED_STAKE RLS tokens"
    cast send $RLS_TOKEN \
      "approve(address,uint256)(bool)" \
      $REGISTRY_CONTRACT_ADDRESS \
      $REQUIRED_STAKE \
      --private-key $PRIVATE_KEY --rpc-url $RPC_URL

    # extract stake calldata from output
    echo "Submitting stake transaction to registry contract at address ${REGISTRY_CONTRACT_ADDRESS}"
    CALLDATA_RES=$(RL_BLS_PASSPHRASE="local" ${workingDir}/../../target/${RELEASE}/rayls-network keytool stake-calldata \
        --datadir "${DATADIR}")

    CALLDATA=$(echo "$CALLDATA_RES" | grep 'Calldata:' | awk '{print $2}')

    echo "Stake: $REQUIRED_STAKE, CallData: $CALLDATA"

    # send stake transaction
    cast send $REGISTRY_CONTRACT_ADDRESS $CALLDATA --private-key $PRIVATE_KEY --rpc-url $RPC_URL -vvvv

fi