#!/usr/bin/env bash
#
# Upper bound: when several validators join in the same epoch and an attacker keeps every validator's
# store full, the newly seated committee cannot form a quorum and the chain stalls. See README.md.
#
# Four genesis validators grow to seven (quorum 2f+1 = 5). A round needs five distinct certificates.
# Only a validator that can collect five votes can certify. The three new validators cannot: their
# vote requests to the genesis validators come back "requesting peer unknown" (the full stores never
# bind them). The four genesis validators can still certify, but four is below quorum. So at most four
# certificates form per round, and block production halts.
#
# Exit status: 0 the chain kept producing (upper bound NOT confirmed), 1 the chain stalled (confirmed),
# 2 inconclusive (including a genesis validator dying, which would confound the result).

set -uo pipefail

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
BIN="$REPO/target/debug/rayls-network"
ATTACKER="$REPO/target/debug/kadfill-attacker"

N=4                      # genesis validators (all attacked)
NEW_NODES=(5 6 7)        # post-genesis validators, staked, seated together
INSTANCE_BASE=30
EPOCH_SECS=${EPOCH_SECS:-120}
WARMUP_BLOCKS=${WARMUP_BLOCKS:-8}
ATTACK_PUTS=10
ATTACK_DURATION=${ATTACK_DURATION:-660}
CHAIN_ID=0x1e7
ADMIN_ADDR=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
ADMIN_KEY=0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
STAKE_AMOUNT=5000000000000000000000000
RLS_ADDRESS=0x07E17e17E17e17E17e17E17E17E17e17e17E17eA
REGISTRY=0x07E17e17E17e17E17e17E17E17E17e17e17E17e1
GAS_FUND=1000000000000000000
ERC1967_IMPL_SLOT=0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc
export RL_BLS_PASSPHRASE="${RL_BLS_PASSPHRASE:-kad-store-fill-local-only}"

RUN=${WORKDIR:-$REPO/target/kad-store-fill-stall/$(date -u +%Y%m%dT%H%M%SZ)}
PIDS=()
SCOPES=()
VAL_PID=()
log() { echo "$(date -u +%T) $*" | tee -a "$RUN/summary.txt"; }
fail() { log "INCONCLUSIVE: $*"; echo "logs: $RUN"; exit 2; }

command -v cast >/dev/null || { echo "needs Foundry 'cast'"; exit 2; }
command -v curl >/dev/null || { echo "needs curl"; exit 2; }
# The 1 GiB per-node cap uses Linux systemd user scopes; where unavailable (macOS, many containers)
# the nodes run uncapped. This run is memory-heavy (seven nodes); on an uncapped host make sure there
# is enough RAM, or lower the node count.
HAVE_SYSTEMD=0
if [[ -z ${FORCE_UNCAPPED:-} ]] && command -v systemd-run >/dev/null 2>&1 && systemctl --user show-environment >/dev/null 2>&1; then
    HAVE_SYSTEMD=1
else
    echo "note: running nodes without the ${MEM_MAX:-1G} memory cap (no systemd user scopes, or FORCE_UNCAPPED set)"
fi
if [[ ! -x $BIN ]]; then echo "building rayls-network"; (cd "$REPO" && cargo build -p rayls-network) || exit 2; fi
if [[ ! -x $ATTACKER ]]; then echo "building kadfill-attacker"; (cd "$REPO" && cargo build -p rayls-consensus-network --features kadfill-test-hooks --bin kadfill-attacker) || exit 2; fi

rpc_port() { echo $((8546 - (INSTANCE_BASE + $1))); }
height() {
    local port=$1 out
    out=$(curl -s -m 2 -H 'content-type: application/json' \
        -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' "http://127.0.0.1:$port" 2>/dev/null) || return 0
    [[ $out =~ \"result\":\"0x([0-9a-fA-F]+)\" ]] && echo $((16#${BASH_REMATCH[1]}))
}
max_val_height() { local best=-1 h v; for v in $(seq 1 "$N"); do h=$(height "$(rpc_port "$v")"); [[ -n $h && $h -gt $best ]] && best=$h; done; echo "$best"; }
primary_addr_of() { awk '/^  primary:/{p=1} p&&/network_address:/{print $2; exit}' "$1/node-info.yaml"; }
committee_size() { grep -aoE 'committee: \[[^]]*\]' "$RUN/validator-1.log" 2>/dev/null | tail -1 | grep -oE '0x[0-9a-fA-F]{40}' | wc -l | tr -d ' '; }
# Run a node, capped at MEM_MAX in a systemd user scope when available, else directly. Must be
# called with a trailing `&`; `exec` makes $! the node (or scope) process so cleanup can kill it.
capped() {
    local unit=$1; shift
    if [[ $HAVE_SYSTEMD -eq 1 ]]; then
        exec systemd-run --user --scope --unit="$unit" -p MemoryMax="${MEM_MAX:-1G}" -p MemorySwapMax=0 -p OOMPolicy=continue "$@"
    else
        exec "$@"
    fi
}

node_common=(--network local --log.stdout.format log-fmt --full --storage.v2
    --db.growth-step 1MB --consensus-db.growth-step 1MB --txpool.minimal-protocol-fee 0
    --gpo.default-suggested-fee 0 -vvv --http --http.addr 127.0.0.1 --http.api all)
start_validator() { local seq=$1 unit=ksf-val-$seq; capped "$unit" "$BIN" node --datadir "$RUN/validator-$seq" --instance $((INSTANCE_BASE + seq)) "${node_common[@]}" >>"$RUN/validator-$seq.log" 2>&1 &
    VAL_PID[$seq]=$!; PIDS+=("$!"); [[ $HAVE_SYSTEMD -eq 1 ]] && SCOPES+=("$unit"); }
start_node() { local n=$1 unit=ksf-node$n; capped "$unit" "$BIN" node --datadir "$RUN/node$n" --instance $((100 + n)) "${node_common[@]}" >>"$RUN/node$n.log" 2>&1 &
    PIDS+=("$!"); [[ $HAVE_SYSTEMD -eq 1 ]] && SCOPES+=("$unit"); }
alive_val() { local seq=$1; if [[ $HAVE_SYSTEMD -eq 1 ]]; then systemctl --user is-active "ksf-val-$seq.scope" >/dev/null 2>&1; else kill -0 "${VAL_PID[$seq]}" 2>/dev/null; fi; }
stop_all() {
    local u p
    if [[ ${#SCOPES[@]} -gt 0 ]]; then for u in "${SCOPES[@]}"; do systemctl --user stop "$u.scope" 2>/dev/null; done; fi
    if [[ ${#PIDS[@]} -gt 0 ]]; then for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null; done; fi
    pkill -f kadfill-attacker 2>/dev/null
    return 0
}
trap stop_all EXIT; trap 'exit 2' INT TERM
wait_for() { local secs=$1; shift; for _ in $(seq 1 "$secs"); do "$@" && return 0; sleep 1; done; return 1; }
val_reached() { [[ $(max_val_height) -ge $1 ]]; }

setup() {
    rm -rf "$RUN"; mkdir -p "$RUN/genesis/validators"
    local seq n
    for seq in $(seq 1 "$N"); do
        "$BIN" keytool generate validator --datadir "$RUN/validator-$seq" --address "$(printf '0x%040x' $((0x5eed0000 + seq)))" >/dev/null 2>&1 || return 1
        cp "$RUN/validator-$seq/node-info.yaml" "$RUN/genesis/validators/validator-$seq.yaml"
    done
    "$BIN" genesis --datadir "$RUN" --chain-id $CHAIN_ID --epoch-duration-in-secs $EPOCH_SECS \
        --dev-funded-account $ADMIN_ADDR --max-header-delay-ms 1000 --min-header-delay-ms 500 \
        --consensus-registry-owner $ADMIN_ADDR --network-admin $ADMIN_ADDR >/dev/null 2>&1 || return 1
    for seq in $(seq 1 "$N"); do
        mkdir -p "$RUN/validator-$seq/genesis"
        cp "$RUN/genesis/genesis.yaml" "$RUN/genesis/committee.yaml" "$RUN/validator-$seq/genesis/"
        cp "$RUN/parameters.yaml" "$RUN/validator-$seq/"
    done
    for n in "${NEW_NODES[@]}"; do
        "$BIN" keytool generate validator --datadir "$RUN/node$n" --address "$(cast wallet address --private-key 0x$(printf '%064x' $((1000+n))))" >/dev/null 2>&1 || return 1
        mkdir -p "$RUN/node$n/genesis"
        cp "$RUN/genesis/genesis.yaml" "$RUN/genesis/committee.yaml" "$RUN/node$n/genesis/"
        cp "$RUN/parameters.yaml" "$RUN/node$n/"
    done
}
stake_node() {
    local n=$1 opkey=0x$(printf '%064x' $((1000+n))) rpc="http://127.0.0.1:$(rpc_port 2)" opaddr
    opaddr=$(cast wallet address --private-key "$opkey")
    cast send --private-key "$ADMIN_KEY" --rpc-url "$rpc" --value "$GAS_FUND" "$opaddr" >/dev/null 2>&1 || return 1
    cast send "$RLS_ADDRESS" "mint(address,uint256)" "$opaddr" "$STAKE_AMOUNT" --private-key "$ADMIN_KEY" --rpc-url "$rpc" >/dev/null 2>&1 || return 1
    cast send "$REGISTRY" "allowlistValidator(address)" "$opaddr" --private-key "$ADMIN_KEY" --rpc-url "$rpc" >/dev/null 2>&1 || true
    cast send "$RLS_ADDRESS" "approve(address,uint256)" "$REGISTRY" "$STAKE_AMOUNT" --private-key "$opkey" --rpc-url "$rpc" >/dev/null 2>&1 || return 1
    local calldata; calldata=$("$BIN" keytool stake-calldata --datadir "$RUN/node$n" | grep 'Calldata:' | awk '{print $2}')
    [[ -n $calldata ]] || return 1
    cast send "$REGISTRY" "$calldata" --private-key "$opkey" --rpc-url "$rpc" >/dev/null 2>&1 || return 1
    cast send "$REGISTRY" "activate()" --private-key "$opkey" --rpc-url "$rpc" >/dev/null 2>&1 || return 1
}
wait_contracts() {
    local rpc="http://127.0.0.1:$(rpc_port 2)" rls_impl reg_owner
    for _ in $(seq 1 90); do
        rls_impl=$(cast storage "$RLS_ADDRESS" "$ERC1967_IMPL_SLOT" --rpc-url "$rpc" 2>/dev/null || true)
        reg_owner=$(cast call "$REGISTRY" "owner()(address)" --rpc-url "$rpc" 2>/dev/null || true)
        [[ $rls_impl =~ [1-9a-fA-F] && -n $reg_owner && $reg_owner != 0x0000000000000000000000000000000000000000 ]] && return 0
        sleep 2
    done
    return 1
}

# --- run ---
setup || fail "setup failed"
for seq in $(seq 1 "$N"); do start_validator "$seq"; done
log "started $N genesis validators; warming up to block $WARMUP_BLOCKS"
wait_for 300 val_reached "$WARMUP_BLOCKS" || fail "chain did not warm up"
log "warm-up done: height $(max_val_height)"

log "attacking all $N genesis stores (sustained ${ATTACK_DURATION}s)"
for seq in $(seq 1 "$N"); do
    "$ATTACKER" "$(primary_addr_of "$RUN/validator-$seq")" --duration-secs "$ATTACK_DURATION" --puts-per-id "$ATTACK_PUTS" --concurrency 10 >"$RUN/attacker-$seq.log" 2>&1 &
done
for seq in $(seq 1 "$N"); do
    wait_for 150 bash -c "grep -qa 'kad store rejected record' '$RUN/validator-$seq.log'" || fail "store $seq did not fill"
done
log "all $N genesis stores FULL (MaxRecords on each)"

for n in "${NEW_NODES[@]}"; do start_node "$n"; done
log "started new nodes: ${NEW_NODES[*]} (following)"
sleep 10
wait_contracts || fail "system contracts not live"
for n in "${NEW_NODES[@]}"; do stake_node "$n" || fail "staking node$n failed"; log "node$n staked+activated"; done

# wait until the committee grows to N+new, seen from a genesis validator (the registry computes the
# new committee there; the attacked validators log the new ones as "unknown", and a new node never
# sees the enlarged committee while the chain is stalled).
TARGET=$((N + ${#NEW_NODES[@]}))
log "waiting up to $((EPOCH_SECS * 3))s for the committee to grow to $TARGET"
seated=0
for _ in $(seq 1 $((EPOCH_SECS * 3))); do
    [[ "$(committee_size)" == "$TARGET" ]] && { seated=1; break; }
    sleep 1
done
[[ $seated -eq 1 ]] || fail "committee never grew to $TARGET (saw $(committee_size)); check staking/epoch timing"
H_SEAT=$(max_val_height)
log "committee now $TARGET (quorum 5); height at seating $H_SEAT; watching 120s for a stall"
sleep 120
H_END=$(max_val_height)
DELTA=$((H_END - H_SEAT))
rej_total=0; for seq in $(seq 1 "$N"); do rej_total=$((rej_total + $(grep -ca 'kad store rejected record' "$RUN/validator-$seq.log"))); done
# a stall only counts if every genesis validator is still alive (an OOM-killed node would reduce quorum on its own)
dead=0; for seq in $(seq 1 "$N"); do alive_val "$seq" || { log "genesis validator $seq is NOT alive"; dead=1; }; done
[[ $dead -eq 1 ]] && fail "a genesis validator died during the run; the stall would be confounded"

echo | tee -a "$RUN/summary.txt"
log "=== RESULT ==="
log "committee $TARGET (quorum 5); genesis certifiers: $N"
log "height at seating: $H_SEAT ; height +120s: $H_END ; advanced: $DELTA blocks"
log "stores kept full: $rej_total total MaxRecords rejections across the $N genesis validators"
if [[ $DELTA -le 3 ]]; then
    log "UPPER BOUND CONFIRMED: the chain STALLED after the new validators were seated (advanced only $DELTA blocks in 120s)"
    echo "logs: $RUN"; exit 1
else
    log "NOT CONFIRMED: the chain kept producing ($DELTA blocks in 120s) despite all stores full"
    echo "logs: $RUN"; exit 0
fi
