#!/usr/bin/env bash
# Catch-up benchmark: a local network of 4 validators and M observers. Run it through run.sh, which
# starts it in a container whose loopback adds a fixed delay, so every message between nodes pays a
# real round trip. Optional senders keep transactions flowing. For each round and each -b LABEL=BIN
# in turn, the observers stop, fall GAP blocks behind, and restart on that binary together.
# Per round it reports: time to the first new block, time until every observer reaches the
# validators' height at restart, transactions per block in the gap, the consensus-header requests
# the validators served, and whether every observer has the validators' block hash.
# See README.md.
set -uo pipefail

usage() { echo "usage: $0 -v VALIDATOR_BIN -b LABEL=BIN [-b ...] -d WORKDIR [-g GAP] [-r ROUNDS] [-m OBSERVERS] [-t TX_PER_SEC] [-e EPOCH_SECS] [-c CAST]"; exit 2; }
GAP=300; ROUNDS=1; M=1; TPS=0; EPOCH_SECS=36000; CAST=cast; INSTANCE_BASE=70; VBIN=""; RUN=""; LABELS=(); declare -A BINS
while getopts "v:b:d:g:r:m:t:e:c:" opt; do
    case "$opt" in
        v) VBIN=$OPTARG ;; b) LABELS+=("${OPTARG%%=*}"); BINS[${OPTARG%%=*}]=${OPTARG#*=} ;; d) RUN=$OPTARG ;;
        g) GAP=$OPTARG ;; r) ROUNDS=$OPTARG ;; m) M=$OPTARG ;; t) TPS=$OPTARG ;; e) EPOCH_SECS=$OPTARG ;;
        c) CAST=$OPTARG ;; *) usage ;;
    esac
done
[[ -x $VBIN && -n $RUN && ${#LABELS[@]} -gt 0 ]] || usage
for l in "${LABELS[@]}"; do [[ -x ${BINS[$l]} ]] || { echo "not executable: $l=${BINS[$l]}"; exit 2; }; done
[[ -d $RUN && -n $(ls -A "$RUN" 2>/dev/null) ]] && { echo "not empty: $RUN"; exit 2; }
[[ $TPS == 0 ]] || CAST=$(command -v "$CAST") || { echo "cast not found (part of Foundry); needed for -t"; exit 2; }

N=4
obs_instance() { echo $((INSTANCE_BASE + N + $1)); }
CHAIN_ID=0x1e7
DEV_ACCOUNT=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
# well-known local test key of DEV_ACCOUNT (anvil account 0); checked below
DEV_KEY=0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
export RL_BLS_PASSPHRASE="catchup-bench-local-only"
declare -A PIDS
declare -A OBS_PIDS
SENDER_PIDS=()
mkdir -p "$RUN"
log() { echo "$(date -u +%T) $*" | tee -a "$RUN/summary.txt"; }
fail() { log "INCONCLUSIVE: $*"; exit 2; }

rpc_port() { if [[ $1 == obs* ]]; then echo $((8546 - $(obs_instance ${1#obs}))); else echo $((8546 - (INSTANCE_BASE + $1))); fi; }
rpc() { curl -s -m 5 -H 'content-type: application/json' -d "$2" "http://127.0.0.1:$(rpc_port "$1")" 2>/dev/null; }
height() {
    local out
    out=$(rpc "$1" '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}') || return 0
    [[ $out =~ \"result\":\"0x([0-9a-fA-F]+)\" ]] && echo $((16#${BASH_REMATCH[1]}))
}
block_json() { local h; printf -v h '0x%x' "$2"; rpc "$1" "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_getBlockByNumber\",\"params\":[\"$h\",false]}"; }
block_hash() { local out; out=$(block_json "$1" "$2"); [[ $out =~ \"hash\":\"(0x[0-9a-fA-F]+)\" ]] && echo "${BASH_REMATCH[1]}"; }
block_txs() {
    local out list; out=$(block_json 1 "$1")
    [[ $out =~ \"transactions\":\[([^]]*)\] ]] || { echo 0; return; }
    list=${BASH_REMATCH[1]}; [[ -z $list ]] && { echo 0; return; }
    echo $(( $(grep -o '"0x' <<<"$list" | wc -l) ))
}
max_val_height() {
    local best=-1 h v
    for v in $(seq 1 "$N"); do h=$(height "$v"); [[ -n $h && $h -gt $best ]] && best=$h; done
    echo "$best"
}
start_validator() {
    local seq=$1
    "$VBIN" node --datadir "$RUN/validator-$seq" --network local --instance $((INSTANCE_BASE + seq)) \
        --log.stdout.format log-fmt --full --storage.v2 --db.growth-step 1MB --consensus-db.growth-step 1MB \
        --txpool.minimal-protocol-fee 0 --gpo.default-suggested-fee 0 -vvv --http --http.addr 127.0.0.1 \
        --log.stdout.filter "info,network=debug" --http.api all >>"$RUN/validator-$seq.log" 2>&1 &
    PIDS[$seq]=$!
}
start_observer() {
    local i=$1 bin=$2 logfile=$3 inst
    inst=$(obs_instance "$i")
    PRIMARY_LISTENER_MULTIADDR="/ip4/0.0.0.0/udp/$((49000 + inst))/quic-v1" \
    WORKER_LISTENER_MULTIADDR="/ip4/0.0.0.0/udp/$((49100 + inst))/quic-v1" \
        "$bin" node --datadir "$RUN/observer-$i" --network local --observer --instance "$inst" \
        --log.stdout.format log-fmt --full --storage.v2 --db.growth-step 1MB --consensus-db.growth-step 1MB \
        --txpool.minimal-protocol-fee 0 --gpo.default-suggested-fee 0 -vvv --http --http.addr 127.0.0.1 \
        --http.api all >>"$logfile" 2>&1 &
    OBS_PIDS[$i]=$!
}
stop_pid() {
    local pid=$1; [[ -z $pid ]] && return 0
    kill -TERM "$pid" 2>/dev/null
    for _ in $(seq 1 40); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
    kill -KILL "$pid" 2>/dev/null; return 0
}
stop_observers() {
    local i; for i in "${!OBS_PIDS[@]}"; do kill -TERM "${OBS_PIDS[$i]}" 2>/dev/null; done
    for i in "${!OBS_PIDS[@]}"; do stop_pid "${OBS_PIDS[$i]}"; done; OBS_PIDS=(); return 0
}
stop_all() {
    local p; for p in "${SENDER_PIDS[@]}"; do kill -TERM "$p" 2>/dev/null; done
    local s; for s in "${!PIDS[@]}"; do kill -TERM "${PIDS[$s]}" 2>/dev/null; done
    stop_observers
    for s in "${!PIDS[@]}"; do stop_pid "${PIDS[$s]}"; done; PIDS=(); return 0
}
# Consensus-header requests the validators received between two epoch-ms times, from their
# debug "request received" lines (target "network", crates/consensus/network/src/consensus/reqres.rs):
# "by-number by-hash latest". If that log line changes, these counts read 0.
served_requests() {
    local from to
    from=$(date -u -d "@$(awk "BEGIN{printf \"%.3f\", $1/1000}")" +%Y-%m-%dT%H:%M:%S.%3N)
    to=$(date -u -d "@$(awk "BEGIN{printf \"%.3f\", $2/1000}")" +%Y-%m-%dT%H:%M:%S.%3N)
    cat "$RUN"/validator-*.log | awk -v from="ts=$from" -v to="ts=$to" '
        $1 >= from && $1 <= to && /request received/ && /ConsensusHeader/ {
            if (/hash: Some/) h++; else if (/number: Some/) n++; else l++
        }
        END { printf "%d %d %d", n, h, l }'
}
trap stop_all EXIT
trap 'exit 2' INT TERM
wait_for() { local secs=$1; shift; for _ in $(seq 1 "$secs"); do "$@" && return 0; sleep 1; done; return 1; }
val_reached() { [[ $(max_val_height) -ge $1 ]]; }
obs_reached() { local h; h=$(height "obs$1"); [[ -n $h && $h -ge $2 ]]; }
all_obs_reached() { local i; for i in $(seq 1 "$M"); do obs_reached "$i" "$1" || return 1; done; }

# Transaction load: reth keeps few pool slots per sender, so the load comes from one funded account
# per PER_ACCOUNT tx/s, like many users. Each account signs PER_ACCOUNT transfers a second at its
# pending nonce and submits them to a validator in one JSON-RPC batch. Recipients cycle through
# fresh addresses so execution also grows state. Rejections are counted in tx-<account>.log.
PER_ACCOUNT=4
SENDER_PIDS=()
nonce_of() {
    local out; out=$(rpc "$2" "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_getTransactionCount\",\"params\":[\"$1\",\"pending\"]}")
    [[ $out =~ \"result\":\"0x([0-9a-fA-F]+)\" ]] && echo $((16#${BASH_REMATCH[1]}))
}
# sign COUNT transfers from KEY starting at NONCE; prints the JSON-RPC batch body
sign_batch() {
    local key=$1 nonce=$2 count=$3 value=$4 i to raw body=""
    for i in $(seq 0 $((count - 1))); do
        if [[ $value == fund ]]; then to=${ACCOUNTS[$i]}; v=1000ether; else to=$(printf '0x%040x' $((0xbe000000 + RANDOM * 32768 + RANDOM))); v=1; fi
        raw=$(HOME=/tmp "$CAST" mktx --private-key "$key" --chain $((CHAIN_ID)) --nonce $((nonce + i)) \
            --gas-limit 21000 --gas-price 100gwei --legacy --value "$v" "$to" 2>/dev/null) || continue
        body+="${body:+,}{\"jsonrpc\":\"2.0\",\"id\":$i,\"method\":\"eth_sendRawTransaction\",\"params\":[\"$raw\"]}"
    done
    echo "[$body]"
}
sender() {
    local key=$1 addr=$2 node=$3 nonce out sent=0 errs=0 cycles=0 t0 dt
    while true; do
        t0=$(date +%s%3N)
        if nonce=$(nonce_of "$addr" "$node") && [[ -n $nonce ]]; then
            out=$(rpc "$node" "$(sign_batch "$key" "$nonce" $PER_ACCOUNT send)")
            sent=$((sent + PER_ACCOUNT)); errs=$((errs + $(grep -o '"error"' <<<"$out" | wc -l)))
            (( cycles++ % 30 == 0 )) && echo "$(date -u +%T) sent $sent rejected $errs last: ${out:0:160}" >>"$RUN/tx-$addr.log"
        fi
        dt=$(( 1000 - ($(date +%s%3N) - t0) ))
        (( dt > 0 )) && sleep "$(awk "BEGIN{print $dt/1000}")"
    done
}
start_load() {
    local k=$(( (TPS + PER_ACCOUNT - 1) / PER_ACCOUNT )) i key nonce
    KEYS=(); ACCOUNTS=()
    for i in $(seq 1 "$k"); do
        key=$(printf '0x%064x' $((0x5eed00 + i))); KEYS+=("$key")
        ACCOUNTS+=("$(HOME=/tmp "$CAST" wallet address --private-key "$key")")
    done
    nonce=$(nonce_of "$DEV_ACCOUNT" 1)
    rpc 1 "$(sign_batch "$DEV_KEY" "$nonce" "$k" fund)" >"$RUN/tx-funding.log"
    wait_for 120 funded "${ACCOUNTS[-1]}" || fail "sender accounts not funded (see tx-funding.log)"
    for i in $(seq 0 $((k - 1))); do
        sender "${KEYS[$i]}" "${ACCOUNTS[$i]}" $(( i % N + 1 )) & SENDER_PIDS+=($!)
    done
}
funded() {
    local out; out=$(rpc 1 "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_getBalance\",\"params\":[\"$1\",\"latest\"]}")
    [[ $out =~ \"result\":\"0x([0-9a-fA-F]+)\" && ${BASH_REMATCH[1]} != 0 ]]
}

mkdir -p "$RUN/genesis/validators"
for seq in $(seq 1 "$N"); do
    "$VBIN" keytool generate validator --datadir "$RUN/validator-$seq" \
        --address "$(printf '0x%040x' $((0x5eed0000 + seq)))" >/dev/null 2>&1 || fail keytool
    cp "$RUN/validator-$seq/node-info.yaml" "$RUN/genesis/validators/validator-$seq.yaml"
done
"$VBIN" genesis --datadir "$RUN" --chain-id $CHAIN_ID --epoch-duration-in-secs $EPOCH_SECS \
    --dev-funded-account $DEV_ACCOUNT --max-header-delay-ms 1000 --min-header-delay-ms 500 \
    --consensus-registry-owner $DEV_ACCOUNT --network-admin $DEV_ACCOUNT >/dev/null 2>&1 || fail genesis
for seq in $(seq 1 "$N"); do
    mkdir -p "$RUN/validator-$seq/genesis"
    cp "$RUN/genesis/genesis.yaml" "$RUN/genesis/committee.yaml" "$RUN/validator-$seq/genesis/"
    cp "$RUN/parameters.yaml" "$RUN/validator-$seq/"
done
for i in $(seq 1 "$M"); do
    "$VBIN" keytool generate observer --datadir "$RUN/observer-$i" \
        --address 0x0000000000000000000000000000000000000000 --advertise-identity-only >/dev/null 2>&1 || fail keytool-obs
    mkdir -p "$RUN/observer-$i/genesis"
    cp "$RUN/genesis/genesis.yaml" "$RUN/genesis/committee.yaml" "$RUN/observer-$i/genesis/"
    cp "$RUN/parameters.yaml" "$RUN/observer-$i/"
done

rtt=$(ping -c 3 -q 127.0.0.1 2>/dev/null | awk -F/ '/rtt/{printf "%.0f", $5}')
for seq in $(seq 1 "$N"); do start_validator "$seq"; done
for i in $(seq 1 "$M"); do start_observer "$i" "${BINS[${LABELS[0]}]}" "$RUN/observer-$i-warmup.log"; done
wait_for 300 val_reached 20 || fail "chain did not reach 20"
wait_for 300 all_obs_reached 20 || fail "observers did not follow to 20"
if [[ $TPS != 0 ]]; then
    [[ $(HOME=/tmp "$CAST" wallet address --private-key "$DEV_KEY") == "$DEV_ACCOUNT" ]] || fail "DEV_KEY does not match DEV_ACCOUNT"
    start_load
    sleep 30
    s=$(max_val_height); busy=0
    for b in $(seq $((s - 9)) "$s"); do busy=$((busy + $(block_txs "$b"))); done
    [[ $busy -gt 0 ]] || fail "sender running but no transactions in blocks $((s - 9))..$s (see tx-*.log)"
fi
log "warm-up done: validators $(max_val_height), ~$((${busy:-0} / 10)) tx/block; $M observers; rtt ${rtt} ms; target $TPS tx/s; epoch ${EPOCH_SECS}s; gap $GAP, rounds $ROUNDS"

for round in $(seq 1 "$ROUNDS"); do
    for label in "${LABELS[@]}"; do
        bin=${BINS[$label]}
        stop_at=$(height obs1); stop_at=${stop_at:-$(max_val_height)}
        stop_observers
        t_stop=$(date +%s)
        wait_for 14400 val_reached $((stop_at + GAP)) || fail "validators did not advance"
        tip=$(max_val_height)
        # transactions per block across the gap, from 20 evenly spaced blocks
        txs=0; step=$(( (tip - stop_at) / 20 )); (( step < 1 )) && step=1; n=0
        for b in $(seq $((stop_at + 1)) "$step" "$tip"); do txs=$((txs + $(block_txs "$b"))); n=$((n + 1)); done
        t0=$(date +%s%3N)
        for i in $(seq 1 "$M"); do start_observer "$i" "$bin" "$RUN/observer-$i-$round-$label.log"; done
        trace="$RUN/progress-$round-$label.tsv"; echo -e "ms\tobs1_height" >"$trace"
        declare -A first=(); rc=1; last_trace=0
        for _ in $(seq 1 72000); do
            now=$(date +%s%3N)
            for i in $(seq 1 "$M"); do
                [[ -n ${first[$i]:-} ]] && continue
                obs_reached "$i" $((stop_at + 1)) && first[$i]=$((now - t0))
            done
            if (( now - last_trace >= 2000 )); then echo -e "$((now - t0))\t$(height obs1)" >>"$trace"; last_trace=$now; fi
            if all_obs_reached "$tip"; then
                # An observer can catch up before its RPC first answers; then its first new block
                # is no earlier than now.
                for i in $(seq 1 "$M"); do first[$i]=${first[$i]:-$((now - t0))}; done
                rc=0; break
            fi
            for i in $(seq 1 "$M"); do kill -0 "${OBS_PIDS[$i]}" 2>/dev/null || fail "round $round $label: observer $i exited"; done
            sleep 0.2
        done
        [[ $rc == 1 ]] && { log "round $round $label: not all observers reached $tip"; continue; }
        t1=$(date +%s%3N)
        read -r req_n req_h req_l <<<"$(served_requests "$t0" "$t1")"
        secs=$(awk "BEGIN{printf \"%.1f\", ($t1 - $t0)/1000}")
        firsts=""; for i in $(seq 1 "$M"); do firsts+="${firsts:+/}$(awk "BEGIN{printf \"%.1f\", ${first[$i]:-0}/1000}")"; done
        ok="same hash"
        for i in $(seq 1 "$M"); do [[ $(block_hash "obs$i" "$tip") == "$(block_hash 1 "$tip")" ]] || ok="HASH DIFFERS (obs $i)"; done
        log "round $round $label: $M obs, gap $((tip - stop_at)) blocks ($((t0 / 1000 - t_stop))s offline, ~$((txs / n)) tx/block), first new block ${firsts}s, all caught up in ${secs}s (boot included), served header requests $req_n by number/$req_h by hash/$req_l latest, $ok"
        unset first
        sleep 10
    done
done
log "done"
