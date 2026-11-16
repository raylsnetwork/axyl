#!/usr/bin/env bash
# Catch-up benchmark: a local network of 4 validators and M observers. Run it through run.sh, which
# starts it in a container whose loopback adds a fixed delay, so every message between nodes pays a
# real round trip. Optional senders keep transactions flowing. For each round and each -b LABEL=BIN
# in turn, the observers stop, fall GAP blocks behind, and restart on that binary together.
# Per round it reports: time to the first new block, time until every observer reaches the
# validators' height at restart, transactions per block in the gap, the consensus-header requests
# the validators served, and whether every observer has the validators' block hash.
# See ../README.md.
set -uo pipefail

usage() { echo "usage: $0 -v VALIDATOR_BIN -b LABEL=BIN [-b ...] [-x LABEL=VAR=VAL[,VAR=VAL...] ...] [-o LABEL=SECS@START ...] -d WORKDIR [-g GAP] [-r ROUNDS] [-m OBSERVERS] [-t TX_PER_SEC] [-e EPOCH_SECS] [-c CAST]"; exit 2; }
GAP=300; ROUNDS=1; M=1; TPS=0; EPOCH_SECS=36000; CAST=cast; INSTANCE_BASE=70; VBIN=""; RUN=""; LABELS=(); declare -A BINS ENVS OUTAGES
while getopts "v:b:x:o:d:g:r:m:t:e:c:" opt; do
    case "$opt" in
        v) VBIN=$OPTARG ;; b) LABELS+=("${OPTARG%%=*}"); BINS[${OPTARG%%=*}]=${OPTARG#*=} ;; d) RUN=$OPTARG ;;
        g) GAP=$OPTARG ;; r) ROUNDS=$OPTARG ;; m) M=$OPTARG ;; t) TPS=$OPTARG ;; e) EPOCH_SECS=$OPTARG ;;
        x) ENVS[${OPTARG%%=*}]=${OPTARG#*=} ;; o) OUTAGES[${OPTARG%%=*}]=${OPTARG#*=} ;;
        c) CAST=$OPTARG ;; *) usage ;;
    esac
done
[[ -x $VBIN && -n $RUN && ${#LABELS[@]} -gt 0 ]] || usage
for l in "${LABELS[@]}"; do [[ -x ${BINS[$l]} ]] || { echo "not executable: $l=${BINS[$l]}"; exit 2; }; done
for l in "${!ENVS[@]}"; do [[ -n ${BINS[$l]:-} ]] || { echo "-x for unknown label: $l"; exit 2; }; done
for l in "${!OUTAGES[@]}"; do
    [[ -n ${BINS[$l]:-} ]] || { echo "-o for unknown label: $l"; exit 2; }
    [[ ${OUTAGES[$l]} =~ ^[0-9]+@. ]] || { echo "-o $l: want SECS@START, START seconds or log:TEXT"; exit 2; }
done
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
declare -A OBS_PIDS OBS_LOGS
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
# start_observer I BIN LOGFILE [VAR=VAL,...]: the optional list (from -x) is set for this observer only.
start_observer() {
    local i=$1 bin=$2 logfile=$3 inst vars=()
    [[ -n ${4:-} ]] && IFS=, read -ra vars <<<"$4"
    inst=$(obs_instance "$i")
    env "${vars[@]}" PRIMARY_LISTENER_MULTIADDR="/ip4/0.0.0.0/udp/$((49000 + inst))/quic-v1" \
    WORKER_LISTENER_MULTIADDR="/ip4/0.0.0.0/udp/$((49100 + inst))/quic-v1" \
        "$bin" node --datadir "$RUN/observer-$i" --network local --observer --instance "$inst" \
        --log.stdout.format log-fmt --full --storage.v2 --db.growth-step 1MB --consensus-db.growth-step 1MB \
        --txpool.minimal-protocol-fee 0 --gpo.default-suggested-fee 0 -vvv --http --http.addr 127.0.0.1 \
        --http.api all >>"$logfile" 2>&1 &
    OBS_PIDS[$i]=$!
    OBS_LOGS[$i]=$logfile
}
# Height an observer stopped at: the head of the last output its log says it executed. Read after
# it stopped, so a block it executed while being stopped is counted (reading it over RPC before the
# stop missed that, and the restarted node then had a "new" block at once).
stopped_height() {
    sed -E 's/\x1b\[[0-9;]*m//g' "${OBS_LOGS[$1]}" 2>/dev/null | grep 'message="output executed"' |
        tail -1 | grep -oE 'head=[0-9]+' | cut -d= -f2
}
# CPU seconds (user + system) a process has used so far.
cpu_secs() {
    local stat; stat=$(cat "/proc/$1/stat" 2>/dev/null) || { echo 0; return; }
    awk -v tck="$CLK_TCK" '{ sub(/.*\) /, ""); printf "%.2f", ($12 + $13) / tck }' <<<"$stat"
}
CLK_TCK=$(getconf CLK_TCK)
# Outage (-o): cut the observers off the network. Every UDP packet between a validator's port and
# any other port is dropped, so the observers' connections go silent whatever sockets they use,
# while the validators keep talking to each other. Peers see nodes that stop answering, and their
# requests time out. The validators' ports are the ones genesis lists (8 for 4 validators, within
# multiport's limit of 15). Run in the background from the observers' restart; writes the dropped
# packet count to $3. START is seconds after the restart, or log:TEXT to start when TEXT first
# appears in observer 1's log.
outage_rules() {
    local op=$1 ports
    ports=$(grep -rhoE '/ip4/127\.0\.0\.1/udp/[0-9]+' "$RUN"/genesis/*.yaml | cut -d/ -f5 | sort -un | paste -sd,)
    iptables -w "$op" OUTPUT -p udp -m multiport --dports "$ports" -m multiport ! --sports "$ports" -j DROP
    iptables -w "$op" OUTPUT -p udp -m multiport --sports "$ports" -m multiport ! --dports "$ports" -j DROP
}
# Wait for the round's outage, if any, and describe it for the result line.
end_outage() {
    outage_note=""
    [[ -z $outage_pid ]] && return 0
    wait "$outage_pid"
    outage_note=", outage ${OUTAGES[$label]%%@*}s from ${OUTAGES[$label]#*@} ($(cat "$outage_file" 2>/dev/null || echo 0) packets dropped)"
}
run_outage() {
    local secs=$1 start=$2 out=$3
    if [[ $start == log:* ]]; then
        until grep -qF "${start#log:}" "${OBS_LOGS[1]}" 2>/dev/null; do sleep 0.05; done
    else
        sleep "$start"
    fi
    outage_rules -I
    sleep "$secs"
    iptables -w -L OUTPUT -v -n -x | awk '/DROP/ { n += $1 } END { print n + 0 }' >"$out"
    outage_rules -D
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
            sec[substr($1, 1, 22)]++
        }
        END { for (s in sec) if (sec[s] > peak) peak = sec[s]; printf "%d %d %d %d", n, h, l, peak }'
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
        before=$(height obs1)
        stop_observers
        declare -A STOP=()
        for i in $(seq 1 "$M"); do STOP[$i]=$(stopped_height "$i"); done
        stop_at=${STOP[1]:-${before:-$(max_val_height)}}
        for i in $(seq 1 "$M"); do STOP[$i]=${STOP[$i]:-$stop_at}; done
        t_stop=$(date +%s)
        wait_for 14400 val_reached $((stop_at + GAP)) || fail "validators did not advance"
        tip=$(max_val_height)
        # transactions per block across the gap, from 20 evenly spaced blocks
        txs=0; step=$(( (tip - stop_at) / 20 )); (( step < 1 )) && step=1; n=0
        for b in $(seq $((stop_at + 1)) "$step" "$tip"); do txs=$((txs + $(block_txs "$b"))); n=$((n + 1)); done
        declare -A cpu0=(); for s in "${!PIDS[@]}"; do cpu0[$s]=$(cpu_secs "${PIDS[$s]}"); done
        t0=$(date +%s%3N)
        for i in $(seq 1 "$M"); do start_observer "$i" "$bin" "$RUN/observer-$i-$round-$label.log" "${ENVS[$label]:-}"; done
        outage_pid=""; outage_file="$RUN/outage-$round-$label.txt"
        if [[ -n ${OUTAGES[$label]:-} ]]; then
            o=${OUTAGES[$label]}; run_outage "${o%%@*}" "${o#*@}" "$outage_file" &
            outage_pid=$!
        fi
        trace="$RUN/progress-$round-$label.tsv"; echo -e "ms\tobs1_height" >"$trace"
        declare -A first=(); rc=1; last_trace=0
        for _ in $(seq 1 72000); do
            now=$(date +%s%3N)
            for i in $(seq 1 "$M"); do
                [[ -n ${first[$i]:-} ]] && continue
                obs_reached "$i" $((STOP[$i] + 1)) && first[$i]=$((now - t0))
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
        [[ $rc == 1 ]] && { end_outage; log "round $round $label: not all observers reached $tip$outage_note"; continue; }
        t1=$(date +%s%3N)
        read -r req_n req_h req_l req_peak <<<"$(served_requests "$t0" "$t1")"
        cpu_sum=0; cpu_max=0
        for s in "${!PIDS[@]}"; do
            used=$(awk "BEGIN{printf \"%.1f\", $(cpu_secs "${PIDS[$s]}") - ${cpu0[$s]}}")
            cpu_sum=$(awk "BEGIN{printf \"%.1f\", $cpu_sum + $used}")
            cpu_max=$(awk "BEGIN{print ($used > $cpu_max) ? $used : $cpu_max}")
        done
        secs=$(awk "BEGIN{printf \"%.1f\", ($t1 - $t0)/1000}")
        firsts=""; for i in $(seq 1 "$M"); do firsts+="${firsts:+/}$(awk "BEGIN{printf \"%.1f\", ${first[$i]:-0}/1000}")"; done
        # Measured above; now let an outage finish (its rules come off at its end) before going on.
        end_outage
        ok="same hash"
        for i in $(seq 1 "$M"); do [[ $(block_hash "obs$i" "$tip") == "$(block_hash 1 "$tip")" ]] || ok="HASH DIFFERS (obs $i)"; done
        log "round $round $label: $M obs, gap $((tip - stop_at)) blocks ($((t0 / 1000 - t_stop))s offline, ~$((txs / n)) tx/block), first new block ${firsts}s, all caught up in ${secs}s (boot included), served header requests $req_n by number/$req_h by hash/$req_l latest (peak $req_peak/s), validator CPU ${cpu_sum}s (max ${cpu_max}s on one), $ok$outage_note"
        unset first STOP cpu0
        sleep 10
    done
done
log "done"
