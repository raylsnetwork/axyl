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

nodeNum="$1"
if [[ "$nodeNum" == "" ]]; then
    echo -e "Error: You must specify the validator as 'start-local-validator.sh 1'";
    exit 1
fi

logFile="$scriptDir/local-validators/validator-$nodeNum.log"
rm -rf "$logFile"

"$scriptDir/../../target/${BUILD_CONFIG}/rayls-network" node \
            --datadir "$scriptDir/local-validators/validator-$nodeNum" \
            --config-file ./config.yaml \
            --subnet local \
            --instance "$nodeNum" \
            --metrics "127.0.0.1:910$nodeNum" \
            --log.stdout.format log-fmt \
            --full \
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
            | tee "$scriptDir/local-validators/validator-$nodeNum.log"
