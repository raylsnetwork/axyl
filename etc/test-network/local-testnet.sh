#!/bin/bash

set -e

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

cd "$scriptDir/../.."

# Function to start a validator by sequence number (0-based index)
start_validator() {
    local seq_num=$1
    local ROOTDIR="$scriptDir/local-validators"

    if [[ ! "$seq_num" =~ ^[0-9]+$ ]]; then
        echo "Error: Sequence number must be a non-negative integer"
        return 1
    fi

    if [[ $seq_num -ge $NUM_VALIDATORS ]]; then
        echo "Error: Sequence number $seq_num is out of range (0-$((NUM_VALIDATORS-1)))"
        return 1
    fi

    local VALIDATOR="${VALIDATORS[$seq_num]}"
    local DATADIR="${ROOTDIR}/${VALIDATOR}"

    if [[ ! -d "$DATADIR" ]]; then
        echo "Error: Validator directory $DATADIR does not exist. Please run setup first."
        return 1
    fi

    # Check if validator is already running
    local PIDFILE="${ROOTDIR}/${VALIDATOR}.pid"
    if [[ -f "$PIDFILE" ]]; then
        local PID=$(cat "$PIDFILE")
        if kill -0 "$PID" 2>/dev/null; then
            echo "Error: Validator $VALIDATOR (seq: $seq_num) is already running with PID $PID"
            return 1
        fi
    fi

    local INSTANCE=$((seq_num+1))
    local RPC_PORT=$((8545-seq_num))
    local WS_PORT=$((8556-seq_num))
    local CONSENSUS_METRICS="127.0.0.1:910$seq_num"
    local RETH_METRICS="127.0.0.1:920$seq_num"
    local heaptrackProfiling=""

    if [[ "$seq_num" == "0" && "$HEAPTRACK" == "1" ]]; then
        heaptrackProfiling="heaptrack"
    fi

    echo "Starting ${VALIDATOR} (seq: $seq_num) in background, rpc http://localhost:$RPC_PORT ws ws://localhost:$WS_PORT"

    $heaptrackProfiling "$scriptDir/../../target/${BUILD_CONFIG}/rayls-network" node \
        --datadir "${DATADIR}" \
        --instance "${INSTANCE}" \
        --metrics "${CONSENSUS_METRICS}" \
        --reth-metrics "${RETH_METRICS}" \
        --log.stdout.format log-fmt \
        --full \
        --storage.v2 \
        ${FLAG_DB_GROW} \
        ${FLAG_CONSENSUS_DB_GROW} \
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
        --txpool.max-account-slots 999999999 \
        --gpo.default-suggested-fee 0 \
        -${LOG_LEVEL} \
        --http \
        --http.api all \
        ${FLAG_WS_API} \
        --ws.port "${WS_PORT}" \
        --ws.api all \
        >> "${ROOTDIR}/${VALIDATOR}.log" &

    local PID=$!
    echo $PID > "$PIDFILE"
    set_high_priority "$PID" "$VALIDATOR"
    echo "Started ${VALIDATOR} (seq: $seq_num) with PID $PID"
}

# Function to stop a validator by sequence number (0-based index)
stop_validator() {
    local seq_num=$1
    local ROOTDIR="$scriptDir/local-validators"

    if [[ ! "$seq_num" =~ ^[0-9]+$ ]]; then
        echo "Error: Sequence number must be a non-negative integer"
        return 1
    fi

    if [[ $seq_num -ge $NUM_VALIDATORS ]]; then
        echo "Error: Sequence number $seq_num is out of range (0-$((NUM_VALIDATORS-1)))"
        return 1
    fi

    local VALIDATOR="${VALIDATORS[$seq_num]}"
    local PIDFILE="${ROOTDIR}/${VALIDATOR}.pid"

    if [[ ! -f "$PIDFILE" ]]; then
        echo "Error: No PID file found for ${VALIDATOR} (seq: $seq_num). Validator may not be running."
        return 1
    fi

    local PID=$(cat "$PIDFILE")

    # Check if process exists (kill -0 sends no signal)
    if ! kill -0 "$PID" 2>/dev/null; then
        echo "Warning: Process $PID for ${VALIDATOR} (seq: $seq_num) is not running. Cleaning up PID file."
        rm "$PIDFILE"
        return 1
    fi

    echo "Stopping ${VALIDATOR} (seq: $seq_num) with PID $PID"

    # Send SIGTERM for graceful shutdown
    kill "$PID"

    # Wait for process to terminate (max 10 seconds)
    local count=0
    while kill -0 "$PID" 2>/dev/null && [[ $count -lt 10 ]]; do
        sleep 1
        count=$((count+1))
    done

    # Force kill if still running
    if kill -0 "$PID" 2>/dev/null; then
        echo "Process did not terminate gracefully, forcing kill..."
        kill -9 "$PID"
        sleep 1
    fi

    rm "$PIDFILE"
    echo "Stopped ${VALIDATOR} (seq: $seq_num)"
}

set_high_priority() {
    local pid=$1
    local name=$2
    if [[ "$USE_HIGH_PRIORITY" == "1" ]]; then
        sudo renice -n -20 -p "$pid" > /dev/null 2>&1 && \
            echo "Set high priority for ${name} (PID $pid)" || \
            echo "Warning: Failed to set high priority for ${name} (PID $pid). Try running with sudo."
    fi
}

while [ "$1" != "" ]; do
    case $1 in
        --start )
                START=true
                ;;
        --start-validator )
                shift
                START_VALIDATOR="$1"
                ;;
        --stop-validator )
                shift
                STOP_VALIDATOR="$1"
                ;;
        --dev-funds )
                shift
                DEV_FUNDS="$1"
                ;;
        --basefee-address )
                shift
                BASEFEE_ADDRESS="$1"
                ;;
        --gasless )
                GASLESS=true
                ;;
        --gas-limit )
                shift
                GAS_LIMIT="$1"
                ;;
        * )     echo "Invalid option: $1"
                exit 1
    esac
    shift
done

# if EPOCH_ROUNDS is not set or not number, set to default of 120
if ! [[ "$EPOCH_DURATION" =~ ^[0-9]+$ ]]; then
    EPOCH_DURATION=120
fi

if ! [[ "$NUM_VALIDATORS" =~ ^[0-9]+$ ]] || [[ "$NUM_VALIDATORS" -lt 3 ]] || [[ "$NUM_VALIDATORS" -gt 9 ]]; then
    echo "Error: NUM_VALIDATORS must be between 3 and 9"
    exit 1
fi

if ! [[ "$NUM_OBSERVERS" =~ ^[0-9]+$ ]] || [[ "$NUM_OBSERVERS" -lt 0 ]] || [[ "$NUM_OBSERVERS" -gt 9 ]]; then
    echo "Error: NUM_OBSERVERS must be between 0 and 9"
    exit 1
fi

# Gasless network flags for genesis command
GASLESS_FLAGS=""
if [ "$GASLESS" = true ]; then
    GASLESS_FLAGS="--base-fee 0 --min-base-fee 0"
    echo "Gasless mode enabled: base fee and min base fee set to 0"
fi

# Gas limit flag for genesis command
GAS_LIMIT_FLAGS=""
if [ -n "$GAS_LIMIT" ]; then
    GAS_LIMIT_FLAGS="--gas-limit $GAS_LIMIT"
    echo "Custom gas limit: $GAS_LIMIT"
fi

ANVIL_VALIDATOR_ADDRESSES=(
    "0x9965507D1a55bcC2695C58ba16FB37d819B0A4dc" # anvil idx 5
    "0x976EA74026E726554dB657fA54763abd0C3a0aa9" # anvil idx 6
    "0x14dC79964da2C08b23698B3D3cc7Ca32193d9955" # anvil idx 7
    "0x23618e81E3f5cdF7f54C3d65f7FBc0aBf5B21E8f" # anvil idx 8
)

declare -a VALIDATORS
declare -a ADDRESSES
for ((i=0; i<NUM_VALIDATORS; i++)); do
    VALIDATORS+=("validator-$((i+1))")
    if [[ $i -lt ${#ANVIL_VALIDATOR_ADDRESSES[@]} ]]; then
        ADDRESSES+=("${ANVIL_VALIDATOR_ADDRESSES[$i]}")
    else
        ADDR=""
        for ((c=0; c<40; c++)); do ADDR="${ADDR}$((i+1))"; done
        ADDRESSES+=("0x${ADDR}")
    fi
done

# variables for pulling
LOCAL_PATH="./genesis/validators/"
REMOTE_PATH="/home/share/validators/*"

# root path for all validators
ROOTDIR="$scriptDir/local-validators"
GENESISDIR="genesis"
VALIDATORSDIR="${GENESISDIR}/validators"
SHARED_GENESISDIR="${ROOTDIR}/${VALIDATORSDIR}"
COMMITTEE_PATH="${ROOTDIR}/${GENESISDIR}/committee.yaml"
GENESIS_JSON_PATH="${ROOTDIR}/${GENESISDIR}/genesis.json"
ACCOUNTS_YAML="$scriptDir/accounts.yaml"
RLS_ACCOUNTS_YAML="$scriptDir/rls-accounts.yaml"

FLAG_DB_GROW=""
FLAG_CONSENSUS_DB_GROW=""
FLAG_WS_API=""
if [[ -n "$DB_GROW_STEP" ]]; then
    FLAG_DB_GROW="--db.growth-step $DB_GROW_STEP"
    FLAG_CONSENSUS_DB_GROW="--consensus-db.growth-step $DB_GROW_STEP"
fi
if [[ "$EXPOSE_WS" == "1" ]]; then
    FLAG_WS_API="--ws"
fi
# Observers only: `--full` prunes account/storage history to ~10k blocks. Set
# DISABLE_PRUNING=1 to run observers as full archives (no pruning) so their
# datadir can seed `rayls-replay`. Validators are unaffected.
OBSERVER_FULL_FLAG="--full"
if [[ "$DISABLE_PRUNING" == "1" || "$DISABLE_PRUNING" == "true" ]]; then
    OBSERVER_FULL_FLAG=""
    echo "DISABLE_PRUNING set: observers will run as full archives (no --full)"
fi
FLAG_TX_POOL_MAX_COUNT="50000"
FLAG_TX_POOL_MAX_SIZE="1048556000"
if [[ "$TX_POOL_LARGE_LIMITS" == "1" ]]; then
    FLAG_TX_POOL_MAX_COUNT="1000000"
    FLAG_TX_POOL_MAX_SIZE="20971120000"
fi

BUILD_ARGS=(
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
# cargo build --bin rayls-network --features redb --release

if [ -d "${ROOTDIR}" ]; then
    echo "The directory ${ROOTDIR} already exists -- skipping configuration"
    echo "Remove ${ROOTDIR} if you wish create a new configuration."
    echo
else
    # Make sure we have a test account with funds if configuring.
    if [ "$DEV_FUNDS" == "" ]; then
        echo "Must use --dev-funds=[ADDRESS] to fund a test account and own the consensus registry."
        echo "For example: --dev-funds 0x1111111111111111111111111111111111111111"
        echo "This sould be an account you have the private key to allow access to RLS on the test network"
        exit 1
    fi

    # make local directory for all validators
    mkdir -p $SHARED_GENESISDIR

    # Loop through all the validators and generate their keys and validator infos.
    for ((i=0; i<$NUM_VALIDATORS; i++)); do
        VALIDATOR="${VALIDATORS[$i]}"
        ADDRESS="${ADDRESSES[$i]}"
        DATADIR="${ROOTDIR}/${VALIDATOR}"
        echo "creating validator keys/info for ${VALIDATOR}"
        "$scriptDir/../../target/${BUILD_CONFIG}/rayls-network" keytool generate validator \
            --datadir "${DATADIR}" \
            --address "${ADDRESS}"

        # cp validator info into shared genesis dir
        echo "copying validator info to shared genesis dir"
        cp "${DATADIR}/node-info.yaml" "${SHARED_GENESISDIR}/${VALIDATOR}.yaml"
        echo ""
        echo ""
    done

    # Optional prefund yamls — only pass the flag if the file is present.
    EXTRA_GENESIS_ARGS=()
    if [ -f "$ACCOUNTS_YAML" ]; then
        EXTRA_GENESIS_ARGS+=(--accounts "$ACCOUNTS_YAML")
    fi
    if [ -f "$RLS_ACCOUNTS_YAML" ]; then
        EXTRA_GENESIS_ARGS+=(--rls-accounts "$RLS_ACCOUNTS_YAML")
    fi

    # Use the validator infos to Create genesis, committee and worker cache yamls.
    # Speed up blocks for testing, use a bogus chain id
    if [ "$BASEFEE_ADDRESS" = "" ]; then
        "$scriptDir/../../target/${BUILD_CONFIG}/rayls-network" genesis \
            --datadir "${ROOTDIR}" \
            --chain-id 0x1e7 \
            --epoch-duration-in-secs $EPOCH_DURATION \
            --dev-funded-account $DEV_FUNDS \
            --max-header-delay-ms 1000 \
            --min-header-delay-ms 500 \
            --consensus-registry-owner $DEV_FUNDS \
            ${GASLESS_FLAGS} \
            ${GAS_LIMIT_FLAGS} \
            "${EXTRA_GENESIS_ARGS[@]}" \
            --network-admin $DEV_FUNDS
    else
        "$scriptDir/../../target/${BUILD_CONFIG}/rayls-network" genesis \
            --datadir "${ROOTDIR}" \
            --chain-id 0x1e7 \
            --epoch-duration-in-secs $EPOCH_DURATION \
            --dev-funded-account $DEV_FUNDS \
            --basefee-address $BASEFEE_ADDRESS \
            --max-header-delay-ms 1000 \
            --min-header-delay-ms 500 \
            --consensus-registry-owner $DEV_FUNDS \
            ${GASLESS_FLAGS} \
            ${GAS_LIMIT_FLAGS} \
            "${EXTRA_GENESIS_ARGS[@]}" \
            --network-admin $DEV_FUNDS
    fi

    # Copy the generated genesis, committee and parameters to each validator.
    for ((i=0; i<$NUM_VALIDATORS; i++)); do
        VALIDATOR="${VALIDATORS[$i]}"
        DATADIR="${ROOTDIR}/${VALIDATOR}"
        mkdir "${DATADIR}/genesis"
        # cp validator info into shared genesis dir
        echo "copying validator info to shared genesis dir"
        cp "${ROOTDIR}/${GENESISDIR}/genesis.yaml" "${DATADIR}/genesis"
        cp "${ROOTDIR}/${GENESISDIR}/committee.yaml" "${DATADIR}/genesis"
        cp "${ROOTDIR}/parameters.yaml" "${DATADIR}/"
        echo ""
        echo ""
    done

    if [[ "$NUM_OBSERVERS" -gt 0 ]]; then
        for ((o=0; o<NUM_OBSERVERS; o++)); do
            OBSERVER="observer-$((NUM_VALIDATORS+o+1))"
            echo "creating datadir for $OBSERVER"
            DATADIR="${ROOTDIR}/${OBSERVER}"
            mkdir -p "${DATADIR}/genesis"
            "$scriptDir/../../target/${BUILD_CONFIG}/rayls-network" keytool generate observer \
                --datadir "${DATADIR}" \
                --address "0x0000000000000000000000000000000000000000"
            cp "${ROOTDIR}/${GENESISDIR}/genesis.yaml" "${DATADIR}/genesis"
            cp "${ROOTDIR}/${GENESISDIR}/committee.yaml" "${DATADIR}/genesis"
            cp "${ROOTDIR}/parameters.yaml" "${DATADIR}/"
        done
    fi
fi

# Handle individual validator start/stop commands
if [[ -n "$START_VALIDATOR" ]]; then
    start_validator "$START_VALIDATOR"
    exit $?
fi

if [[ -n "$STOP_VALIDATOR" ]]; then
    stop_validator "$STOP_VALIDATOR"
    exit $?
fi

if [ "$START" = true ]; then
    for ((i=0; i<$NUM_VALIDATORS; i++)); do
        VALIDATOR="${VALIDATORS[$i]}"
        DATADIR="${ROOTDIR}/${VALIDATOR}"
        INSTANCE=$((i+1))
        RPC_PORT=$((8545-i))
        WS_PORT=$((8556-i))
        CONSENSUS_METRICS="127.0.0.1:910$i"
        heaptrackProfiling=""
        if [[ "$i" == "0" && "$HEAPTRACK" == "1" ]]; then
            heaptrackProfiling="heaptrack"
        fi

        echo "Starting ${VALIDATOR} in background, rpc http://localhost:$RPC_PORT ws ws://localhost:$WS_PORT"
        # start validator
        $heaptrackProfiling "$scriptDir/../../target/${BUILD_CONFIG}/rayls-network" node \
            --datadir "${DATADIR}" \
            --instance "${INSTANCE}" \
            --metrics "${CONSENSUS_METRICS}" \
            --log.stdout.format log-fmt \
            --full \
            --storage.v2 \
            ${FLAG_DB_GROW} \
            ${FLAG_CONSENSUS_DB_GROW} \
            --txpool.pending-max-count "$FLAG_TX_POOL_MAX_COUNT" \
            --txpool.pending-max-size "$FLAG_TX_POOL_MAX_SIZE" \
            --txpool.basefee-max-count "$FLAG_TX_POOL_MAX_COUNT" \
            --txpool.basefee-max-size "$FLAG_TX_POOL_MAX_SIZE" \
            --txpool.queued-max-count "$FLAG_TX_POOL_MAX_COUNT" \
            --txpool.queued-max-size "$FLAG_TX_POOL_MAX_SIZE" \
            --txpool.max-pending-txns "$FLAG_TX_POOL_MAX_COUNT" \
            --txpool.max-new-txns "$FLAG_TX_POOL_MAX_COUNT" \
            --txpool.minimal-protocol-fee 0 \
            --txpool.max-tx-input-bytes 999999999999 \
            --txpool.max-account-slots "$FLAG_TX_POOL_MAX_COUNT" \
            --gpo.default-suggested-fee 0 \
            -${LOG_LEVEL} \
            --http \
            --http.api all \
            ${FLAG_WS_API} \
            --ws.port "${WS_PORT}" \
            --ws.api all \
             >> "${ROOTDIR}/${VALIDATOR}.log" &

        PID=$!
        echo $PID > "${ROOTDIR}/${VALIDATOR}.pid"
        set_high_priority "$PID" "$VALIDATOR"
    done

    if [[ "$NUM_OBSERVERS" -gt 0 ]]; then
        for ((o=0; o<NUM_OBSERVERS; o++)); do
            OBSERVER="observer-$((NUM_VALIDATORS+o+1))"
            OBSERVER_INSTANCE=$((NUM_VALIDATORS+o+1))
            OBSERVER_RPC_PORT=$((8545-NUM_VALIDATORS-o))
            OBSERVER_WS_PORT=$((8556-NUM_VALIDATORS-o))
            OBSERVER_METRICS="127.0.0.1:910$((NUM_VALIDATORS+o))"
            echo "Starting $OBSERVER in background, rpc http://localhost:$OBSERVER_RPC_PORT ws ws://localhost:$OBSERVER_WS_PORT"
            "$scriptDir/../../target/${BUILD_CONFIG}/rayls-network" node \
                --datadir "${ROOTDIR}/${OBSERVER}" \
                --observer \
                --instance "${OBSERVER_INSTANCE}" \
                --metrics "${OBSERVER_METRICS}" \
                --log.stdout.format log-fmt \
                ${OBSERVER_FULL_FLAG} \
                --storage.v2 \
                ${FLAG_DB_GROW} \
                ${FLAG_CONSENSUS_DB_GROW} \
                --txpool.pending-max-count "$FLAG_TX_POOL_MAX_COUNT" \
                --txpool.pending-max-size "$FLAG_TX_POOL_MAX_SIZE" \
                --txpool.basefee-max-count "$FLAG_TX_POOL_MAX_COUNT" \
                --txpool.basefee-max-size "$FLAG_TX_POOL_MAX_SIZE" \
                --txpool.queued-max-count "$FLAG_TX_POOL_MAX_COUNT" \
                --txpool.queued-max-size "$FLAG_TX_POOL_MAX_SIZE" \
                --txpool.max-pending-txns "$FLAG_TX_POOL_MAX_COUNT" \
                --txpool.max-new-txns "$FLAG_TX_POOL_MAX_COUNT" \
                --txpool.minimal-protocol-fee 0 \
                --txpool.max-tx-input-bytes 999999999999 \
                --txpool.max-account-slots "$FLAG_TX_POOL_MAX_COUNT" \
                --gpo.default-suggested-fee 0 \
                -${LOG_LEVEL} \
                --http \
                --http.api all \
                ${FLAG_WS_API} \
                --ws.port "${OBSERVER_WS_PORT}" \
                --ws.api all \
                >> "${ROOTDIR}/${OBSERVER}.log" &
            PID=$!
            echo $PID > "${ROOTDIR}/${OBSERVER}.pid"
            set_high_priority "$PID" "$OBSERVER"
        done
    fi

    TOTAL_NODES=$((NUM_VALIDATORS+NUM_OBSERVERS))
    echo "$TOTAL_NODES nodes started in background, \
    use 'killall rayls-network' to bring the test network down"
fi
