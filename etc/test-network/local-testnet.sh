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

    # RELAY_ENV is populated by the dispatch above (start_relay_pair + build_relay_env) in relay
    # mode, empty otherwise. `env` with no assignments just runs the command normally.
    env "${RELAY_ENV[@]}" $heaptrackProfiling "$scriptDir/../../target/${BUILD_CONFIG}/rayls-network" node \
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
        --http.addr 0.0.0.0 \
        --http.api all \
        ${FLAG_WS_API} \
        --ws.addr 0.0.0.0 \
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

    # Wait for the process to exit on its own -- indefinitely, NO SIGKILL fallback. A graceful
    # SIGTERM shutdown that hangs is a real bug worth catching; forcing kill -9 would mask it (and
    # any shutdown errors would scroll past unnoticed unless you're watching the logs). If a stop
    # blocks here, inspect ${VALIDATOR}.log to see what shutdown step is stuck.
    local count=0
    while kill -0 "$PID" 2>/dev/null; do
        sleep 1
        count=$((count+1))
        (( count % 30 == 0 )) && echo "  ${VALIDATOR} still shutting down after ${count}s (SIGTERM sent; not forcing) -- check ${VALIDATOR}.log"
    done

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

# Derive the fixed relay ed25519 seed for a validator index (0-based): the byte (index+1) repeated
# 32x, as hex. Must stay in sync with RELAY_PEER_IDS / RELAY_KEYS.md and the rayls-relay identity.
relay_seed_hex() {
    local idx=$1
    local byte
    byte=$(printf '%02x' $((idx+1)))
    local seed="" c
    for ((c=0; c<32; c++)); do seed="${seed}${byte}"; done
    echo "$seed"
}

# True if the pid recorded in <pidfile> names a live process.
relay_alive() {
    local pf=$1 pid
    [[ -f "$pf" ]] || return 1
    pid=$(cat "$pf" 2>/dev/null) || return 1
    [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

# Ensure validator <i>'s primary + backup relays are running (start only if down) and populate
# RELAY_A_ADDR[i]/RELAY_B_ADDR[i]. Idempotent: a relay already up is reused, not restarted. Relay
# identities are deterministic (fixed seeds), so a restarted relay keeps the SAME peer id / dialable
# address -- the /dnsaddr records dnsmasq serves stay valid across relay restarts.
#  - primary relay-(i+1) on ${RELAY_BASE_PORT+i}: fixed identity matching the validators' baked
#    node-info addresses / RELAY_PEER_IDS;
#  - backup relay-(i+1)-b on ${RELAY_B_BASE_PORT+i}: distinct fixed identity, peer id read from log.
start_relay_pair() {
    local i=$1
    local ROOTDIR="$scriptDir/local-validators"

    # primary relay (identity must match the baked node-info addresses)
    local port=$((RELAY_BASE_PORT + i))
    local pidf="${ROOTDIR}/relay-$((i+1)).pid"
    if relay_alive "$pidf"; then
        echo "relay-$((i+1)) already running on ${RELAY_HOST}:${port} (peer ${RELAY_PEER_IDS[$i]})"
    else
        local seed
        seed=$(relay_seed_hex "$i")
        echo "Starting relay-$((i+1)) on ${RELAY_HOST}:${port} (peer ${RELAY_PEER_IDS[$i]})"
        RELAY_SEED_HEX="$seed" RELAY_PORT="$port" \
            "$scriptDir/../../target/${BUILD_CONFIG}/rayls-relay" \
            >> "${ROOTDIR}/relay-$((i+1)).log" 2>&1 &
        echo $! > "$pidf"
    fi
    # base dialable address of this primary relay (fixed peer id, so always known)
    RELAY_A_ADDR[$i]="/ip4/${RELAY_HOST}/udp/${port}/quic-v1/p2p/${RELAY_PEER_IDS[$i]}"

    # backup relay (distinct fixed seed byte 0xb0+i; peer id read back from its log)
    local b_port=$((RELAY_B_BASE_PORT + i))
    local b_pidf="${ROOTDIR}/relay-$((i+1))-b.pid"
    local b_log="${ROOTDIR}/relay-$((i+1))-b.log"
    if relay_alive "$b_pidf"; then
        echo "relay-$((i+1))-b (backup) already running on ${RELAY_HOST}:${b_port}"
    else
        local b_byte b_seed c
        b_byte=$(printf '%02x' $((0xb0 + i)))
        b_seed=""
        for ((c=0; c<32; c++)); do b_seed="${b_seed}${b_byte}"; done
        echo "Starting relay-$((i+1))-b (backup) on ${RELAY_HOST}:${b_port}"
        RELAY_SEED_HEX="$b_seed" RELAY_PORT="$b_port" \
            "$scriptDir/../../target/${BUILD_CONFIG}/rayls-relay" \
            >> "$b_log" 2>&1 &
        echo $! > "$b_pidf"
    fi
    # backup relay peer id: read from its log. The seed is fixed, so the peer id is stable across
    # restarts -- head -1 picks the first (possibly older) entry, which is identical either way.
    local b_peer=""
    for _ in $(seq 1 40); do
        b_peer=$(grep -ao '12D3KooW[A-Za-z0-9]*' "$b_log" 2>/dev/null | head -1 || true)
        [[ -n "$b_peer" ]] && break
        sleep 0.25
    done
    [[ -n "$b_peer" ]] || { echo "Error: backup relay-$((i+1))-b did not report a peer id (see $b_log)."; exit 1; }
    RELAY_B_ADDR[$i]="/ip4/${RELAY_HOST}/udp/${b_port}/quic-v1/p2p/${b_peer}"
}

# Stop validator <i>'s primary + backup relays (by pidfile) and clear their pidfiles.
stop_relay_pair() {
    local i=$1
    local ROOTDIR="$scriptDir/local-validators"
    local pf pid count
    for pf in "${ROOTDIR}/relay-$((i+1)).pid" "${ROOTDIR}/relay-$((i+1))-b.pid"; do
        if relay_alive "$pf"; then
            pid=$(cat "$pf")
            echo "stopping $(basename "${pf%.pid}") (pid $pid)"
            kill -TERM "$pid" 2>/dev/null || true
            # Relays are stateless -- if SIGTERM doesn't take within a few seconds, just rip it with
            # SIGKILL (unlike validators, there's no shutdown correctness to preserve).
            count=0
            while kill -0 "$pid" 2>/dev/null && [[ $count -lt 5 ]]; do sleep 1; count=$((count+1)); done
            if kill -0 "$pid" 2>/dev/null; then
                echo "  $(basename "${pf%.pid}") did not exit on SIGTERM, forcing kill -9"
                kill -9 "$pid" 2>/dev/null || true
            fi
        fi
        rm -f "$pf"
    done
}

# Spawn all validators' relays (primary + backup). Populates RELAY_A_ADDR[]/RELAY_B_ADDR[].
start_relays() {
    RELAY_B_ADDR=()
    RELAY_A_ADDR=()
    for ((i=0; i<NUM_VALIDATORS; i++)); do
        start_relay_pair "$i"
    done
    # Give relays a moment to bind before validators try to reserve/dial through them.
    sleep 1
}

# Build the global RELAY_ENV[] for validator <seq>: relay reservations (+ /dnsaddr resolver in
# --relay-dns mode) and, with MULTI_LISTEN=1, the direct 0.0.0.0 listeners. Requires RELAY_A_ADDR/
# RELAY_B_ADDR to be populated (by start_relays in --start, or start_relay_pair on restart).
# Shared by the --start loop and the single-validator (re)start path so both launch with the SAME
# env -- without RAYLS_DNS_SERVER a restarted node resolves committee /dnsaddr via the system/public
# resolver, finds no peers, and can't rejoin.
build_relay_env() {
    local i=$1
    RELAY_ENV=()
    if [[ "$RELAY_DNS_MODE" == "true" ]]; then
        # node-info advertises /dnsaddr (not listened on), so reserve on BOTH relays via env,
        # and resolve /dnsaddr against the local dnsmasq.
        RELAY_ENV=(
            "RAYLS_DNS_SERVER=127.0.0.1:${DNSMASQ_PRIVATE_PORT}"
            "PRIMARY_RELAY_MULTIADDRS=${RELAY_A_ADDR[$i]},${RELAY_B_ADDR[$i]}"
            "WORKER_RELAY_MULTIADDRS=${RELAY_A_ADDR[$i]},${RELAY_B_ADDR[$i]}"
        )
    elif [[ "$RELAY_MODE" == "true" ]]; then
        RELAY_ENV=(
            "PRIMARY_RELAY_MULTIADDRS=${RELAY_B_ADDR[$i]}"
            "WORKER_RELAY_MULTIADDRS=${RELAY_B_ADDR[$i]}"
        )
    fi
    # MULTI_LISTEN: also open a direct QUIC listener bound to MULTI_LISTEN_BIND (default 127.0.0.1),
    # so this validator listens on BOTH a direct address and its relay reservation(s) at once. The
    # node appends /p2p itself, so pass the bare base multiaddr.
    if [[ "$MULTI_LISTEN" == "1" ]]; then
        RELAY_ENV+=(
            "PRIMARY_LISTENER_MULTIADDR=/ip4/${MULTI_LISTEN_BIND}/udp/$((PRIMARY_DIRECT_BASE + i))/quic-v1"
            "WORKER_LISTENER_MULTIADDR=/ip4/${MULTI_LISTEN_BIND}/udp/$((WORKER_DIRECT_BASE + i))/quic-v1"
        )
        echo "  MULTI_LISTEN: direct ${MULTI_LISTEN_BIND}:$((PRIMARY_DIRECT_BASE + i)) (primary) / ${MULTI_LISTEN_BIND}:$((WORKER_DIRECT_BASE + i)) (worker), plus relay reservation"
    fi
}

# Launch a local dnsmasq serving ONE view of the committee /dnsaddr records (high port, no
# systemd-resolved/NetworkManager conflict). Args: <view> <port>.
#   relay  -> each validator's /dnsaddr resolves to its relay circuits (primary + backup). The
#             PUBLIC view: how an outsider reaches the committee, through the relays.
#   direct -> resolves to each validator's direct 127.0.0.1:<port> listener only. The INSIDE/private
#             view: co-located nodes connect directly and never touch a relay (needs MULTI_LISTEN,
#             i.e. the nodes actually opened those direct listeners).
# Requires RELAY_A_ADDR/RELAY_B_ADDR (set by start_relays). With MULTI_LISTEN the caller runs both
# views on different ports; otherwise a single relay view on $DNSMASQ_PRIVATE_PORT.
start_dnsmasq() {
    local view="$1" port="$2"
    local ROOTDIR="$scriptDir/local-validators"
    command -v dnsmasq >/dev/null 2>&1 || { echo "Error: dnsmasq not found; install it or run --relay (without -dns)."; exit 1; }
    # --conf-file=/dev/null + --no-hosts: ignore the system dnsmasq config and /etc/hosts so this
    # instance serves ONLY our TXT records (avoids interference from /etc/dnsmasq.conf and
    # /etc/dnsmasq.d/* on the host). --log-queries: show each lookup so failures are visible.
    local args=(
        --no-daemon --conf-file=/dev/null --no-resolv --no-hosts --log-queries
        --port="$port" --listen-address="$DNSMASQ_BIND" --bind-interfaces
    )
    for ((i=0; i<NUM_VALIDATORS; i++)); do
        local host="v$((i+1)).${RELAY_DNS_DOMAIN}"
        local ninfo="${ROOTDIR}/${VALIDATORS[$i]}/node-info.yaml"
        # primary + worker peer ids = the trailing /p2p/<id> of the two /dnsaddr addresses baked
        # into node-info (primary listed first, worker second).
        local ids
        mapfile -t ids < <(grep -oE '/p2p/12D3KooW[A-Za-z0-9]+' "$ninfo" | grep -oE '12D3KooW[A-Za-z0-9]+')
        # ids[0]=primary, ids[1]=worker; their direct ports differ (40000+i / 41000+i).
        local direct_ports=($((PRIMARY_DIRECT_BASE + i)) $((WORKER_DIRECT_BASE + i)))
        local j dst
        for j in 0 1; do
            dst="${ids[$j]}"
            [[ -n "$dst" ]] || { echo "Error: could not read peer id from $ninfo"; exit 1; }
            if [[ "$view" == "direct" ]]; then
                args+=(--txt-record="_dnsaddr.${host},dnsaddr=/ip4/127.0.0.1/udp/${direct_ports[$j]}/quic-v1/p2p/${dst}")
            else
                # Advertise the relay at RELAY_PUBLIC_HOST (default RELAY_HOST) so a cross-host
                # joiner resolves a reachable relay IP, not loopback. RELAY_{A,B}_ADDR bake in
                # RELAY_HOST for the validators' own (co-located) reservations; rewrite just the ip4
                # host for the public record.
                local relay_a="${RELAY_A_ADDR[$i]/\/ip4\/${RELAY_HOST}\//\/ip4\/${RELAY_PUBLIC_HOST}\/}"
                local relay_b="${RELAY_B_ADDR[$i]/\/ip4\/${RELAY_HOST}\//\/ip4\/${RELAY_PUBLIC_HOST}\/}"
                args+=(--txt-record="_dnsaddr.${host},dnsaddr=${relay_a}/p2p-circuit/p2p/${dst}")
                args+=(--txt-record="_dnsaddr.${host},dnsaddr=${relay_b}/p2p-circuit/p2p/${dst}")
            fi
        done
    done
    dnsmasq "${args[@]}" >> "${ROOTDIR}/dnsmasq-${view}.log" 2>&1 &
    echo $! > "${ROOTDIR}/dnsmasq-${view}.pid"
    echo "dnsmasq[${view}] on ${DNSMASQ_BIND}:${port} serving ${NUM_VALIDATORS} validators' /dnsaddr"
    sleep 1
}

# Bundle the three files a follower needs (genesis.yaml + committee.yaml + parameters.yaml) so they
# can be shipped to another host and dropped in where add-relay-node.sh expects them. Archiving with
# `-C local-validators` stores them as genesis/... + parameters.yaml, so extracting with the same
# `-C` on the other host restores the exact tree -- no path munging. Arg: optional output file.
export_join_bundle() {
    local ROOTDIR="$scriptDir/local-validators"
    local out="${1:-join-bundle.tgz}"
    local missing=0
    for f in genesis/genesis.yaml genesis/committee.yaml parameters.yaml; do
        [[ -f "${ROOTDIR}/${f}" ]] || { echo "Error: missing ${ROOTDIR}/${f}"; missing=1; }
    done
    [[ "$missing" == 0 ]] || { echo "Start the network first (--start) so the genesis exists."; exit 1; }
    tar -czf "$out" -C "$ROOTDIR" genesis/genesis.yaml genesis/committee.yaml parameters.yaml
    echo "wrote ${out} (genesis.yaml + committee.yaml + parameters.yaml)"
    echo "on the joining host, from the repo root:"
    echo "  mkdir -p etc/test-network/local-validators"
    echo "  tar -xzf $(basename "$out") -C etc/test-network/local-validators"
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
        --export-join-bundle )
                EXPORT_JOIN_BUNDLE=1
                # optional output filename follows
                if [[ -n "$2" && "$2" != --* ]]; then
                    JOIN_BUNDLE_FILE="$2"
                    shift
                fi
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
        --relay )
                RELAY_MODE=true
                ;;
        --relay-dns )
                # Full peer-failover variant: validators advertise a /dnsaddr name that a local
                # dnsmasq resolves to BOTH their relays, so peers fail over to the backup relay when
                # the primary dies. Implies --relay (primary + backup relays are still spawned).
                RELAY_MODE=true
                RELAY_DNS_MODE=true
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

# --- circuit-relay-v2 test setup -------------------------------------------------
# Pass --relay to generate validators that route p2p through a per-validator relay
# (their advertised primary/worker addresses become <relay>/p2p-circuit/p2p/<node-key>).
# Omit --relay for the normal direct-QUIC setup.
#
# The relay ip/port is derived from the validator index (0-based): validator-(i+1) uses
# ${RELAY_HOST}:$((RELAY_BASE_PORT + i)). The relay PEER ID cannot be derived (libp2p requires it
# in the circuit address and it is the hash of the relay's key), so the peer ids below are fixed to
# deterministic ed25519 keys. Your relay app MUST run with the matching identity for each port:
# the identity is ed25519 with a 32-byte seed equal to the byte (validator index + 1) repeated 32x
# (i.e. 0x01*32 for validator-1, 0x02*32 for validator-2, ...). See RELAY_KEYS.md for the secrets.
RELAY_HOST="127.0.0.1"
# The IP advertised for the relays in the PUBLIC (relay-circuit) dnsaddr records that outsiders
# resolve -- as opposed to RELAY_HOST, which is how co-located validators dial their own relay to
# reserve. Default RELAY_HOST (loopback, single-host testnet). To let a node on ANOTHER machine join,
# set RELAY_PUBLIC_HOST to this host's LAN/public IP: the relay server already listens on all
# interfaces, so it is reachable there, and the joiner then dials the relay at a real address instead
# of 127.0.0.1. Only rewrites the public-view records; the direct/inside view stays loopback.
RELAY_PUBLIC_HOST="${RELAY_PUBLIC_HOST:-$RELAY_HOST}"
RELAY_BASE_PORT=50000
# Each validator also gets a second "backup" relay (ports 51000+i) so it reserves on two relays.
# Kill a validator's primary relay (relay-N) and the validator should stay up on its backup
# (relay-N-b) instead of shutting down -- that's the multi-reservation failover test. (Peers won't
# re-reach it via the backup without /dnsaddr advertisement; the network continues on quorum.)
RELAY_B_BASE_PORT=51000
# --relay-dns only: validators advertise /dnsaddr/v<i>.${RELAY_DNS_DOMAIN}, resolved by a local
# dnsmasq on ${DNSMASQ_BIND}:${DNSMASQ_PRIVATE_PORT} (high port, no systemd-resolved/NetworkManager conflict).
RELAY_DNS_DOMAIN="rayls.test"
DNSMASQ_PRIVATE_PORT=5353
# MULTI_LISTEN only: the PUBLIC (outside) resolver. Serves the relay-circuit records that an outsider
# joining later (add-relay-node.sh) resolves, while the inside view (direct records) stays on
# DNSMASQ_PRIVATE_PORT. Distinct port so both dnsmasq instances can bind the same address.
DNSMASQ_PUBLIC_PORT=5354
# The interface the local dnsmasq resolver(s) bind. Default 127.0.0.1 (loopback only) keeps the
# resolver private to this host; set DNSMASQ_BIND=0.0.0.0 to serve the /dnsaddr records to other
# hosts (e.g. an outsider joining from another machine that points RAYLS_DNS_SERVER here).
DNSMASQ_BIND="${DNSMASQ_BIND:-127.0.0.1}"
# MULTI_LISTEN=1 (relay/relay-dns only): in addition to the relay reservation, open a DIRECT QUIC
# listener so each validator listens on BOTH a direct and a relayed address at once -- the
# private-direct + public-relay topology. The listener binds MULTI_LISTEN_BIND (default 127.0.0.1,
# loopback only): co-located nodes reach it directly (matching the direct dnsaddr records, which
# advertise 127.0.0.1) while it is never exposed on an external interface, so any cross-host reach
# must go through a relay. Set MULTI_LISTEN_BIND=0.0.0.0 to bind all interfaces instead.
# Direct ports mirror the relay scheme one band lower: primary 40000+i / worker 41000+i, i.e. exactly
# 10000 below the validator's relay ports (relay A 50000+i / relay B 51000+i). Clear of reth's
# 8545/9100 range. Each validator needs a unique port since <bind>:<port> is host-wide.
MULTI_LISTEN="${MULTI_LISTEN:-0}"
MULTI_LISTEN_BIND="${MULTI_LISTEN_BIND:-127.0.0.1}"
PRIMARY_DIRECT_BASE=40000
WORKER_DIRECT_BASE=41000
RELAY_PEER_IDS=(
    "12D3KooWK99VoVxNE7XzyBwXEzW7xhK7Gpv85r9F3V3fyKSUKPH5" # validator-1 relay @ 127.0.0.1:50000 (seed 0x01*32)
    "12D3KooWJWoaqZhDaoEFshF7Rh1bpY9ohihFhzcW6d69Lr2NASuq" # validator-2 relay @ 127.0.0.1:50001 (seed 0x02*32)
    "12D3KooWRndVhVZPCiQwHBBBdg769GyrPUW13zxwqQyf9r3ANaba" # validator-3 relay @ 127.0.0.1:50002 (seed 0x03*32)
    "12D3KooWPT98FXMfDQYavZm66EeVjTqP9Nnehn1gyaydqV8L8BQw" # validator-4 relay @ 127.0.0.1:50003 (seed 0x04*32)
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
# In relay mode also build the local relay-server binary.
if [[ "$RELAY_MODE" == "true" ]]; then
    BUILD_ARGS+=( "--bin" "rayls-relay" )
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

        # In relay mode, derive this validator's relay address from its index and pass it to keytool
        # so its advertised primary/worker addresses become <relay>/p2p-circuit/p2p/<node-key>.
        RELAY_ARGS=()
        if [[ "$RELAY_DNS_MODE" == "true" ]]; then
            # Advertise via /dnsaddr; the concrete relays to reserve on are supplied at runtime.
            RELAY_ARGS=(--advertise-dnsaddr "v$((i+1)).${RELAY_DNS_DOMAIN}")
            echo "creating validator keys/info for ${VALIDATOR} (advertise /dnsaddr/v$((i+1)).${RELAY_DNS_DOMAIN})"
        elif [[ "$RELAY_MODE" == "true" ]]; then
            RELAY_PEER_ID="${RELAY_PEER_IDS[$i]}"
            if [[ -z "$RELAY_PEER_ID" || "$RELAY_PEER_ID" == REPLACE_WITH_* ]]; then
                echo "Error: --relay set but RELAY_PEER_IDS[$i] is not filled in for ${VALIDATOR}."
                echo "Set the relay peer id near the top of this script (see RELAY_PEER_IDS)."
                exit 1
            fi
            RELAY_PORT=$((RELAY_BASE_PORT + i))
            RELAY_ADDR="/ip4/${RELAY_HOST}/udp/${RELAY_PORT}/quic-v1/p2p/${RELAY_PEER_ID}"
            RELAY_ARGS=(--relay "$RELAY_ADDR")
            echo "creating validator keys/info for ${VALIDATOR} (relay ${RELAY_ADDR})"
        else
            echo "creating validator keys/info for ${VALIDATOR}"
        fi

        "$scriptDir/../../target/${BUILD_CONFIG}/rayls-network" keytool generate validator \
            --datadir "${DATADIR}" \
            --address "${ADDRESS}" \
            "${RELAY_ARGS[@]}"

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
if [[ -n "$EXPORT_JOIN_BUNDLE" ]]; then
    export_join_bundle "$JOIN_BUNDLE_FILE"
    exit $?
fi

if [[ -n "$START_VALIDATOR" ]]; then
    # Rebuild the SAME relay/DNS env the --start loop uses, so a single-validator (re)start (e.g.
    # the chaos-kill loop) doesn't boot with a bare env and fail /dnsaddr resolution. Bring this
    # validator's relays up if they're down (idempotent) and populate its relay addresses. Requires
    # the same mode flags on this invocation, e.g.
    #   MULTI_LISTEN=1 ./local-testnet.sh --start-validator N --relay-dns
    RELAY_ENV=()
    if [[ "$RELAY_DNS_MODE" == "true" || "$RELAY_MODE" == "true" ]]; then
        start_relay_pair "$START_VALIDATOR"
        build_relay_env "$START_VALIDATOR"
    fi
    start_validator "$START_VALIDATOR"
    exit $?
fi

if [[ -n "$STOP_VALIDATOR" ]]; then
    stop_validator "$STOP_VALIDATOR"
    # Also scrap this validator's relays; a later --start-validator brings them back (identities are
    # deterministic, so peer ids / dnsmasq records stay valid). Harmless if no relays were running.
    stop_relay_pair "$STOP_VALIDATOR"
    exit $?
fi

if [ "$START" = true ]; then
    # MULTI_LISTEN is only meaningful with a relay mode, where the node otherwise has no direct
    # listener. The direct listener binds 0.0.0.0, so there's no interface/alias to set up.
    if [[ "$MULTI_LISTEN" == "1" && "$RELAY_MODE" != "true" && "$RELAY_DNS_MODE" != "true" ]]; then
        echo "MULTI_LISTEN=1 ignored: only meaningful with --relay/--relay-dns."
        MULTI_LISTEN=0
    fi
    # In relay mode, bring the relays up before validators so reservations/dials succeed.
    if [[ "$RELAY_MODE" == "true" ]]; then
        start_relays
    fi
    # In /dnsaddr mode, start the resolver(s). With MULTI_LISTEN, run TWO views: the inside/private
    # view (direct records) on $DNSMASQ_PRIVATE_PORT that the base validators use, and the public
    # view (relay records) on $DNSMASQ_PUBLIC_PORT that an outsider joining later (add-relay-node.sh)
    # points at. Without MULTI_LISTEN, a single relay view on $DNSMASQ_PRIVATE_PORT.
    if [[ "$RELAY_DNS_MODE" == "true" ]]; then
        if [[ "$MULTI_LISTEN" == "1" ]]; then
            start_dnsmasq direct "$DNSMASQ_PRIVATE_PORT"
            start_dnsmasq relay "$DNSMASQ_PUBLIC_PORT"
        else
            start_dnsmasq relay "$DNSMASQ_PRIVATE_PORT"
        fi
    fi

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
        # Build this validator's relay/DNS env (also reserves on the backup relay so it survives
        # losing its primary). Passed as env assignments via `env` so the big command below stays
        # unchanged. Shared with the single-validator restart path (see build_relay_env).
        build_relay_env "$i"
        # start validator
        env "${RELAY_ENV[@]}" $heaptrackProfiling "$scriptDir/../../target/${BUILD_CONFIG}/rayls-network" node \
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
            --http.addr 0.0.0.0 \
            --http.api all \
            ${FLAG_WS_API} \
            --ws.addr 0.0.0.0 \
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
            # In /dnsaddr mode the observer must also resolve committee `/dnsaddr` addresses against
            # the local dnsmasq -- otherwise it queries the system/public resolver, gets NXDomain
            # for *.rayls.test, resolves no circuits, and never connects to the committee. Observers
            # don't reserve on relays (peers don't dial them), so only the resolver env is needed.
            OBSERVER_ENV=()
            if [[ "$RELAY_DNS_MODE" == "true" ]]; then
                OBSERVER_ENV=("RAYLS_DNS_SERVER=127.0.0.1:${DNSMASQ_PRIVATE_PORT}")
            fi
            env "${OBSERVER_ENV[@]}" "$scriptDir/../../target/${BUILD_CONFIG}/rayls-network" node \
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
                --http.addr 0.0.0.0 \
                --http.api all \
                ${FLAG_WS_API} \
                --ws.addr 0.0.0.0 \
                --ws.port "${OBSERVER_WS_PORT}" \
                --ws.api all \
                >> "${ROOTDIR}/${OBSERVER}.log" &
            PID=$!
            echo $PID > "${ROOTDIR}/${OBSERVER}.pid"
            set_high_priority "$PID" "$OBSERVER"
        done
    fi

    TOTAL_NODES=$((NUM_VALIDATORS+NUM_OBSERVERS))
    if [[ "$RELAY_MODE" == "true" ]]; then
        echo "$TOTAL_NODES nodes + $((NUM_VALIDATORS * 2)) relays (primary + backup per validator) started in background."
        if [[ "$RELAY_DNS_MODE" == "true" ]]; then
            echo "Full-failover test (/dnsaddr via dnsmasq on :${DNSMASQ_PRIVATE_PORT}):"
            echo "  kill validator-1's PRIMARY relay: 'kill \$(cat ${ROOTDIR}/relay-1.pid)'"
            echo "  peers re-resolve /dnsaddr and reconnect to ${VALIDATORS[0]} via relay-1-b -- it stays in the committee."
            echo "Bring it all down with 'killall rayls-network rayls-relay dnsmasq'."
        else
            echo "Failover test: kill validator-1's PRIMARY relay with 'kill \$(cat ${ROOTDIR}/relay-1.pid)'"
            echo "  and confirm ${VALIDATORS[0]} stays up (it keeps its backup reservation on relay-1-b)."
            echo "Bring it all down with 'killall rayls-network rayls-relay'."
        fi
    else
        echo "$TOTAL_NODES nodes started in background, \
    use 'killall rayls-network' to bring the test network down"
    fi
fi
