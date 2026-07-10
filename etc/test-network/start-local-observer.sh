#!/bin/bash

directory=$(dirname "${BASH_SOURCE[0]}")
scriptDir=$(cd "$directory" && pwd)
envPath="$scriptDir/.env"
if [[ ! -e "$envPath" ]]; then
    echo "Error: .env file not found at $envPath"
    exit 1
fi
. "$envPath"
export RL_BLS_PASSPHRASE="$RL_BLS_PASSPHRASE"
export RAYLS_NETWORK="$RAYLS_NETWORK"

# `--full` prunes account/storage history to ~10k blocks. Set DISABLE_PRUNING=1
# (in .env or inline) to run this observer as a full archive so its datadir can
# seed `rayls-replay` with complete history.
FULL_FLAG="--full"
if [[ "$DISABLE_PRUNING" == "1" || "$DISABLE_PRUNING" == "true" ]]; then
    FULL_FLAG=""
    echo "DISABLE_PRUNING set: running observer as a full archive (no --full)"
fi

nodeNum="$1"
if [[ "$nodeNum" == "" ]]; then
    echo -e "Error: You must specify the validator as 'start-local-observer.sh 1'";
    exit 1
fi

logFile="$scriptDir/local-validators/observer-$nodeNum.log"
rm -rf "$logFile"

"$scriptDir/../../target/${BUILD_CONFIG}/rayls-network" node \
            --observer \
            --datadir "$scriptDir/local-validators/observer-$nodeNum" \
            --instance "$nodeNum" \
            --metrics "127.0.0.1:910$nodeNum" \
            --log.stdout.format log-fmt \
            ${FULL_FLAG} \
            --storage.v2 \
            --db.growth-step 1MB \
            --consensus-db.growth-step 1MB \
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
            --txpool.max-account-slots 410000006 \
            --http \
            --http.api all \
            --http.addr 0.0.0.0 \
            -vvv \
            | tee "$scriptDir/local-validators/observer-$nodeNum.log"
