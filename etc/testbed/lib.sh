# Shared settings, node control and RPC/assert helpers. Sourced by testbed.sh and every test.

TESTBED="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$TESTBED/../.." && pwd)"

# Settings (override via env)
DATA="${TESTBED_DATA:-$TESTBED/data}"
[[ $DATA == /* ]] || DATA=$PWD/$DATA
BIN="${RAYLS_BIN:-$ROOT/target/release/rayls-network}"
EPOCH_SECS="${EPOCH_SECS:-30}"
HTTP_ADDR="${HTTP_ADDR:-127.0.0.1}"
LOG_LEVEL="${LOG_LEVEL:-vvv}"
export RL_BLS_PASSPHRASE="${RL_BLS_PASSPHRASE:-testbed}"
export RAYLS_NETWORK=local

# Deterministic accounts (anvil test mnemonic): #0 funds/owns everything, #5..#9 operate validators 1..5
ADMIN=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
ADMIN_KEY=0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
OPERATORS=(0x9965507D1a55bcC2695C58ba16FB37d819B0A4dc 0x976EA74026E726554dB657fA54763abd0C3a0aa9
    0x14dC79964da2C08b23698B3D3cc7Ca32193d9955 0x23618e81E3f5cdF7f54C3d65f7FBc0aBf5B21E8f
    0xa0Ee7A142d267C1f36714E4a8F75612F20a79720)
V5_KEY=0x2a871d0798f97d79848a013d4936a73bf4cc922c825d33c1cf7073dff6d409c6
MNEMONIC="test test test test test test test test test test test junk"
REGISTRY=0x07E17e17E17e17E17e17E17E17E17e17e17E17e1
RLS=0x07E17e17E17e17E17e17E17E17E17e17e17E17eA

# --- nodes: validator-N runs as --instance N (rpc port 8546-N), the rpc node as instance 6 ---

instance() { if [[ $1 == rpc ]]; then echo 6; else echo "${1#validator-}"; fi; }
# clients connect to HTTP_ADDR, or to loopback when the nodes listen on every interface
url() { local h=$HTTP_ADDR; [[ $h != 0.0.0.0 ]] || h=127.0.0.1; echo "http://$h:$((8546 - $(instance "$1")))"; }
validators() { local d; for d in "$DATA"/validator-*; do [[ -d $d ]] && basename "$d"; done; return 0; }
nodes() { validators; if [[ -d $DATA/rpc ]]; then echo rpc; fi; }
quorum() { local n; n=$(validators | wc -l); echo $((2 * n / 3 + 1)); }
pid() { cat "$DATA/$1.pid" 2>/dev/null; }
# trust a pid file only while that pid is still this node (pids get reused over long soaks)
running() { local p; p=$(pid "$1") && [[ $(ps -ww -p "$p" -o command= 2>/dev/null) == *"--datadir $DATA/$1 "* ]]; }
running_nodes() { local n; for n in $(nodes); do running "$n" && echo "$n"; done; return 0; }

start_node() {
    local n=$1 i
    i=$(instance "$n")
    running "$n" && return 0
    # own process group, so Ctrl-C on the test runner does not reach the node
    set -m
    (
        set --
        if [[ $n == rpc ]]; then
            # observer keys advertise no dialable address, so it needs explicit listeners
            export PRIMARY_LISTENER_MULTIADDR=/ip4/127.0.0.1/udp/49006/quic-v1
            export WORKER_LISTENER_MULTIADDR=/ip4/127.0.0.1/udp/49106/quic-v1
            set -- --observer
        fi
        # txpool sized for load tests like local-testnet.sh (reth's default is 16 pending txs per sender)
        exec "$BIN" node "$@" --datadir "$DATA/$n" --instance "$i" --metrics "127.0.0.1:$((9099 + i))" \
            --http --http.addr "$HTTP_ADDR" --http.api all --full --storage.v2 --log.stdout.format log-fmt \
            --txpool.minimal-protocol-fee 0 --txpool.max-account-slots 50000 --txpool.pending-max-count 50000 \
            --txpool.queued-max-count 50000 --txpool.basefee-max-count 50000 -"$LOG_LEVEL" $NODE_ARGS
    ) >> "$DATA/$n.log" 2>&1 &
    echo $! > "$DATA/$n.pid"
    set +m
}

# SIGTERM only, never SIGKILL: a node must shut down cleanly, and one that hangs is a bug to fix.
# After STOP_TIMEOUT (default 120s) this fails and leaves the node running for inspection.
stop_node() {
    local p
    running "$1" || { rm -f "$DATA/$1.pid"; return 0; }
    p=$(pid "$1")
    kill -CONT "$p" 2>/dev/null || true
    kill "$p" 2>/dev/null || true
    if ! wait_exit "$p" "${STOP_TIMEOUT:-120}"; then
        echo "  $1 (pid $p) did not stop within ${STOP_TIMEOUT:-120}s of SIGTERM; left running, see $DATA/$1.log" >&2
        return 1
    fi
    rm -f "$DATA/$1.pid"
}
pause_node() { running "$1" || fail "$1 is not running"; kill -STOP "$(pid "$1")"; }
resume_node() { if running "$1"; then kill -CONT "$(pid "$1")"; fi; }
start_all() { local n; for n in $(nodes); do start_node "$n"; done; }
stop_all() { local n rc=0; for n in $(nodes); do stop_node "$n" || rc=1; done; return $rc; }
wait_exit() { local i; for ((i = 0; i < $2; i++)); do kill -0 "$1" 2>/dev/null || return 0; sleep 1; done; return 1; }

# --- network setup ---

copy_genesis() {
    mkdir -p "$DATA/$1/genesis"
    cp "$DATA/genesis/genesis.yaml" "$DATA/genesis/committee.yaml" "$DATA/$1/genesis/"
    cp "$DATA/parameters.yaml" "$DATA/$1/"
}

genesis() {
    local i
    mkdir -p "$DATA/genesis/validators"
    for i in 1 2 3 4; do
        "$BIN" keytool generate validator --datadir "$DATA/validator-$i" --address "${OPERATORS[i - 1]}" > /dev/null
        cp "$DATA/validator-$i/node-info.yaml" "$DATA/genesis/validators/validator-$i.yaml"
    done
    "$BIN" genesis --datadir "$DATA" --chain-id 487 --epoch-duration-in-secs "$EPOCH_SECS" \
        --dev-funded-account "$ADMIN" --consensus-registry-owner "$ADMIN" --network-admin "$ADMIN" \
        --fee-aggregator-admin "$ADMIN" --max-header-delay-ms 1000 --min-header-delay-ms 500 $GENESIS_ARGS > /dev/null
    for i in 1 2 3 4; do copy_genesis "validator-$i"; done
}

tx() { cast send --rpc-url "$(url validator-1)" "$@" > /dev/null; }

# validator-5 in the registry: 0 undefined, 1 staked, 2 pending activation, 3 active
v5_status() {
    cast call "$REGISTRY" 'getValidator(address)((bytes,address,uint32,uint32,uint8,bool,bool,uint8))' \
        "${OPERATORS[4]}" --rpc-url "$(url validator-1)" | cut -d, -f5 | tr -d ' '
}

# 5th validator: start it as a follower, then fund, allowlist, stake and activate it on-chain.
# It joins the committee at a later epoch boundary. Re-runnable: steps the registry already saw are skipped.
add_validator() {
    local n=validator-5 op=${OPERATORS[4]} status stake calldata
    if [[ ! -d $DATA/$n ]]; then
        "$BIN" keytool generate validator --datadir "$DATA/$n" --address "$op" > /dev/null
        copy_genesis "$n"
    fi
    start_node "$n"
    status=$(v5_status)
    [[ $status =~ ^[0-9]$ ]] || fail "could not read validator-5 status from the registry: '$status'"
    if [[ $status == 0 ]]; then
        stake=$(cast call "$REGISTRY" 'getCurrentStakeConfig()(uint256,uint256,uint32)' --rpc-url "$(url validator-1)" | head -1 | cut -d' ' -f1)
        calldata=$("$BIN" keytool stake-calldata --datadir "$DATA/$n" | awk '/Calldata:/ {print $2}')
        tx --private-key "$ADMIN_KEY" --value 10ether "$op"
        tx --private-key "$ADMIN_KEY" "$RLS" 'mint(address,uint256)' "$op" "$stake"
        tx --private-key "$ADMIN_KEY" "$REGISTRY" 'allowlistValidator(address)' "$op"
        tx --private-key "$V5_KEY" "$RLS" 'approve(address,uint256)' "$REGISTRY" "$stake"
        tx --private-key "$V5_KEY" "$REGISTRY" "$calldata"
        status=$(v5_status)
    fi
    if [[ $status == 1 ]]; then
        tx --private-key "$V5_KEY" "$REGISTRY" 'activate()'
    fi
}

# RPC node: an observer that follows the committee and serves JSON-RPC
add_rpc() {
    if [[ ! -d $DATA/rpc ]]; then
        "$BIN" keytool generate observer --datadir "$DATA/rpc" \
            --address 0x0000000000000000000000000000000000000000 --advertise-identity-only > /dev/null
        copy_genesis rpc
    fi
    start_node rpc
}

# --- load (tps-checker, github.com/raylsnetwork/tps-checker, checked out next to this repo) ---

TPS_CHECKER_DIR="${TPS_CHECKER_DIR:-$(dirname "$ROOT")/tps-checker}"
TPS_CHECKER="${TPS_CHECKER_BIN:-$TPS_CHECKER_DIR/target/release/tps-checker}"

# Use an existing tps-checker build, or clone and build it next to this repo
tps_setup() {
    [[ -x $TPS_CHECKER ]] && return 0
    [[ -d $TPS_CHECKER_DIR ]] || git clone -q git@github.com:raylsnetwork/tps-checker.git "$TPS_CHECKER_DIR" || return 1
    (cd "$TPS_CHECKER_DIR" && cargo build --release -q)
}

# tps-checker against this chain. Runs from the data dir so no stray .env is picked up. The gas
# price is twice the base fee: the chain never mines under its 48 gwei floor, and eth_gasPrice
# would ratchet up because it follows the prices the load itself pays.
tps_checker() {
    local fee
    fee=$(cast base-fee --rpc-url "$(url validator-1)") || return 1
    (cd "$DATA" && exec "$TPS_CHECKER" --chain-id 487 --gas-price $((fee * 2)) "$@")
}

# private keys of test-mnemonic wallets: wallet_keys <first index> <count>
wallet_keys() {
    local i
    for ((i = $1; i < $1 + $2; i++)); do cast wallet private-key --mnemonic "$MNEMONIC" --mnemonic-index "$i"; done | paste -sd, -
}
# load wallets start at index 10, clear of the admin (#0), test (#1-#4) and operator (#5-#9) accounts
load_keys() { wallet_keys 10 "${LOAD_WALLETS:-4}"; }

# with_timeout <secs> <cmd...>: kill cmd (and its children) if it runs longer
with_timeout() {
    local secs=$1 p w rc=0
    shift
    "$@" &
    p=$!
    # the watcher must not hold the caller's stdout, or $(with_timeout ...) waits for it
    (sleep "$secs" && { pkill -P "$p"; kill "$p"; }) > /dev/null 2>&1 &
    w=$!
    wait "$p" || rc=$?
    pkill -P "$w" 2> /dev/null || true
    kill "$w" 2> /dev/null || true
    return $rc
}

# Fund the load wallets once. validator-1 may be restarting, so fund only after a real balance read.
fund_load() {
    local key addr balance
    for key in $(load_keys | tr , ' '); do
        addr=$(cast wallet address --private-key "$key")
        until balance=$(cast balance "$addr" --rpc-url "$(url validator-1)" 2> /dev/null); do sleep 2; done
        [[ $balance != 0 ]] || tx --private-key "$ADMIN_KEY" --value 1000000ether "$addr"
    done
}

# Nonce collisions: at random intervals averaging COLLIDE_SECS (default 10, 0 = off), send one extra
# transfer from the first load wallet while tps-checker is using it, so both race for the same nonces.
# Rejections ("nonce too low", "replacement underpriced") are expected; the chain must not care.
collide_loop() {
    local secs=${COLLIDE_SECS:-10} key
    ((secs > 0)) || return 0
    key=$(wallet_keys 10 1)
    while sleep $((RANDOM % (2 * secs) + 1)); do
        echo "--- $(date '+%F %T') nonce collision: $(cast send --async --rpc-url "$(url validator-1)" --private-key "$key" \
            0x000000000000000000000000000000000000dEaD --value 1gwei 2>&1 | tail -1)"
    done
}

# Steady load: tps-checker in continuous mode at LOAD_TPS (default 200) through validator-1, logged to
# load.log, plus collide_loop. tps-checker is restarted whenever it exits (e.g. after validator-1 was
# restarted) and, with LOAD_CYCLE_SECS, on purpose every that many seconds (another source of nonce races).
load_loop() {
    set +m
    local keys
    keys=$(load_keys)
    collide_loop >> "$DATA/load.log" 2>&1 &
    while :; do
        echo "--- $(date '+%F %T') tps-checker at ${LOAD_TPS:-200} tps"
        tps_checker --private-keys "$keys" --rpc-url "$(url validator-1)" --continuous \
            --target-tps "${LOAD_TPS:-200}" --num-transactions 1000000000 --report-interval-secs 30 \
            --duration-secs "${LOAD_CYCLE_SECS:-0}" || true
        sleep 2
    done >> "$DATA/load.log" 2>&1
}

# Install and fund up front (so the admin nonce is settled before tests use it), then run
# load_loop in the background as its own process group, so stop_load ends the whole tree.
start_load() {
    tps_setup
    fund_load
    set -m
    load_loop &
    LOAD_PID=$!
    set +m
    log "load running (pid $LOAD_PID), see $DATA/load.log"
}
stop_load() { if [[ -n $LOAD_PID ]]; then kill -- -"$LOAD_PID" 2> /dev/null || true; fi; }
load_nonce() { cast nonce "$(cast wallet address --private-key "$(wallet_keys 10 1)")" --rpc-url "$(url validator-1)"; }

# --- RPC queries ---

rpc() {
    curl -sf -m 3 -H 'content-type: application/json' \
        --data "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$2\",\"params\":${3:-[]}}" "$(url "$1")"
}
height() { local h; h=$(rpc "$1" eth_blockNumber | grep -o '0x[0-9a-f]*') && printf '%d\n' "$h"; }
block_hash() { rpc "$1" eth_getBlockByNumber "[\"$(printf '0x%x' "$2")\",false]" | grep -o '"hash":"0x[0-9a-f]*"' | head -1 | cut -d'"' -f4; }
# field of rayls_nodeStatus: role, is_caught_up, epoch, committed_round, last_canonical_block
status_field() { rpc "$1" rayls_nodeStatus | grep -o "\"$2\":[^,}]*" | cut -d: -f2 | tr -d '"'; }
role() { status_field "$1" role; }
epoch() { status_field "$1" epoch; }
# members in the committee of a closed epoch (its record lists their BLS keys)
committee_size() { rpc validator-1 rayls_epochRecord "[$1]" | grep -o '"committee":\[[^]]*\]' | tr ',' '\n' | wc -l | tr -d ' '; }

# --- assertions (tests run with set -e; fail exits the test) ---

log() { echo "  $*"; }
fail() { echo "  FAIL: $*" >&2; exit 1; }
skip() { echo "  SKIP: $*"; exit 77; }

# wait_for <secs> <what> <cmd...>: poll cmd every 2s until it succeeds
wait_for() {
    local secs=$1 what=$2 end=$((SECONDS + $1))
    shift 2
    until "$@"; do
        ((SECONDS < end)) || fail "timed out after ${secs}s waiting for $what"
        sleep 2
    done
}

at_height() { local h; h=$(height "$1") && ((h >= $2)); }
at_epoch() { local e; e=$(epoch "$1") && ((e >= $2)); }
has_role() { [[ $(role "$1") == "$2" ]]; }

# wait_blocks <node> <n> [secs]: the chain grows by n blocks as seen by node
wait_blocks() {
    local h
    h=$(height "$1") || fail "$1 is not responding"
    wait_for "${3:-60}" "$1 to reach block $((h + $2))" at_height "$1" $((h + $2))
    log "$1 advanced $h -> $(height "$1")"
}

# <node> reports caught up and is within 2 blocks of another validator
synced() {
    local ref h r
    ref=$(validators | grep -vx "$1" | head -1)
    h=$(height "$1") && r=$(height "$ref") && ((r - h <= 2)) && [[ $(status_field "$1" is_caught_up) == true ]]
}
wait_synced() {
    local secs=$((EPOCH_SECS * 4))
    ((secs > 120)) || secs=120
    wait_for "${2:-$secs}" "$1 to catch up" synced "$1"
    log "$1 caught up at block $(height "$1") as $(role "$1")"
}

# assert_stalled <node> <secs>: no progress (one in-flight block allowed)
assert_stalled() {
    local a b
    a=$(height "$1") || fail "$1 is not responding"
    sleep "$2"
    b=$(height "$1") || fail "$1 is not responding"
    ((b <= a + 1)) || fail "chain advanced $a -> $b without quorum"
    log "chain stalled at block $b"
}

# all running nodes have the same block hashes at the last 3 common heights
check_consistency() {
    local n h min="" from hash ref
    for n in $(running_nodes); do
        h=$(height "$n") || fail "$n is not responding"
        if [[ -z $min ]] || ((h < min)); then min=$h; fi
    done
    from=$((min > 2 ? min - 2 : 0))
    for ((h = from; h <= min; h++)); do
        ref=""
        for n in $(running_nodes); do
            hash=$(block_hash "$n" "$h") || fail "$n has no block $h"
            ref=${ref:-$hash}
            [[ $hash == "$ref" ]] || fail "fork at block $h: $n has $hash, expected $ref"
        done
    done
    log "$(running_nodes | wc -l | tr -d ' ') nodes agree on blocks $from-$min"
}
