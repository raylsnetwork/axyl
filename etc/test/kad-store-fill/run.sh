#!/usr/bin/env bash
#
# Live reproduction: a filled peer-discovery record store stops a newly joined validator from being
# recognized. See README.md in this directory.
#
# Starts a local network of four validators, has an unstaked outside peer (kadfill-attacker) keep one
# validator's kad record store full, then stakes a fifth validator through the on-chain registry and
# lets it be seated at the next epoch boundary. The three un-attacked validators bind the new
# validator; the attacked one never does, so it rejects the new validator's requests and drops its
# gossip while its store stays full. The chain keeps producing.
#
# The attacker is a separate process, not a committee node, which matches the "any connectable peer"
# threat. It is compiled only with the `kadfill-test-hooks` cargo feature, so a normal build never
# contains it.
#
# Exit status: 0 the attacked validator bound the new validator anyway (healthy; bug absent or fixed),
# 1 the attacked validator never bound the seated new validator and rejected its traffic (reproduced),
# 2 the run was inconclusive or could not start.

set -uo pipefail

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
BIN="$REPO/target/debug/rayls-network"
ATTACKER="$REPO/target/debug/kadfill-attacker"

N=4                      # genesis validators
VICTIM=1                 # the attacked validator
HEALTHY=2                # a non-victim validator, for comparison
INSTANCE_BASE=30
NODE5_INSTANCE=105
EPOCH_SECS=${EPOCH_SECS:-120}
WARMUP_BLOCKS=${WARMUP_BLOCKS:-8}
ATTACK_PUTS=10
ATTACK_DURATION=${ATTACK_DURATION:-480}
CHAIN_ID=0x1e7           # 487 = the local network's chain-id
ADMIN_ADDR=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266   # anvil #0
ADMIN_KEY=0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
OPERATOR_KEY=0x$(printf '%064x' 1005)
STAKE_AMOUNT=5000000000000000000000000
RLS_ADDRESS=0x07E17e17E17e17E17e17E17E17E17e17e17E17eA
REGISTRY=0x07E17e17E17e17E17e17E17E17E17e17e17E17e1
GAS_FUND=1000000000000000000
ERC1967_IMPL_SLOT=0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc
export RL_BLS_PASSPHRASE="${RL_BLS_PASSPHRASE:-kad-store-fill-local-only}"

RUN=${WORKDIR:-$REPO/target/kad-store-fill/$(date -u +%Y%m%dT%H%M%SZ)}
PIDS=()
SCOPES=()
log() { echo "$(date -u +%T) $*" | tee -a "$RUN/summary.txt"; }
fail() { log "INCONCLUSIVE: $*"; echo "logs: $RUN"; exit 2; }

command -v cast >/dev/null || { echo "needs Foundry 'cast' on PATH"; exit 2; }
command -v curl >/dev/null || { echo "needs curl"; exit 2; }
# The 1 GiB per-node cap uses Linux systemd user scopes. Where they are unavailable (macOS, many
# containers, non-systemd distros) the nodes run uncapped, which is fine on a dev's own machine.
HAVE_SYSTEMD=0
if [[ -z ${FORCE_UNCAPPED:-} ]] && command -v systemd-run >/dev/null 2>&1 && systemctl --user show-environment >/dev/null 2>&1; then
    HAVE_SYSTEMD=1
else
    echo "note: running nodes without the ${MEM_MAX:-1G} memory cap (no systemd user scopes, or FORCE_UNCAPPED set)"
fi

if [[ ! -x $BIN ]]; then
    echo "building rayls-network (debug)"; (cd "$REPO" && cargo build -p rayls-network) || exit 2
fi
if [[ ! -x $ATTACKER ]]; then
    echo "building kadfill-attacker (debug, --features kadfill-test-hooks)"
    (cd "$REPO" && cargo build -p rayls-consensus-network --features kadfill-test-hooks --bin kadfill-attacker) || exit 2
fi

rpc_port() { echo $((8546 - (INSTANCE_BASE + $1))); }
height() {
    local port=$1 out
    out=$(curl -s -m 2 -H 'content-type: application/json' \
        -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' "http://127.0.0.1:$port" 2>/dev/null) || return 0
    [[ $out =~ \"result\":\"0x([0-9a-fA-F]+)\" ]] && echo $((16#${BASH_REMATCH[1]}))
}
max_val_height() { local best=-1 h v; for v in $(seq 1 "$N"); do h=$(height "$(rpc_port "$v")"); [[ -n $h && $h -gt $best ]] && best=$h; done; echo "$best"; }
bls_of() { grep '^bls_public_key:' "$1/node-info.yaml" | awk '{print $2}'; }
primary_addr_of() { awk '/^  primary:/{p=1} p&&/network_address:/{print $2; exit}' "$1/node-info.yaml"; }
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

start_validator() {
    local seq=$1 unit=ksf-val-$seq filter="info"
    [[ $seq -eq $VICTIM ]] && filter="info,network-kad=trace"
    capped "$unit" "$BIN" node --datadir "$RUN/validator-$seq" --instance $((INSTANCE_BASE + seq)) \
        --log.stdout.filter "$filter" "${node_common[@]}" >>"$RUN/validator-$seq.log" 2>&1 &
    PIDS+=("$!"); [[ $HAVE_SYSTEMD -eq 1 ]] && SCOPES+=("$unit")
}
start_node5() {
    capped ksf-node5 "$BIN" node --datadir "$RUN/node5" --instance "$NODE5_INSTANCE" \
        "${node_common[@]}" >>"$RUN/node5.log" 2>&1 &
    PIDS+=("$!"); [[ $HAVE_SYSTEMD -eq 1 ]] && SCOPES+=("ksf-node5")
}
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

setup_network() {
    rm -rf "$RUN"; mkdir -p "$RUN/genesis/validators"
    local seq
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
    # node 5: post-genesis, stakeable (operator address baked into its proof-of-possession). NOT in
    # committee.yaml, so a peer can bind it only through the runtime record path.
    "$BIN" keytool generate validator --datadir "$RUN/node5" --address "$(cast wallet address --private-key "$OPERATOR_KEY")" >/dev/null 2>&1 || return 1
    mkdir -p "$RUN/node5/genesis"
    cp "$RUN/genesis/genesis.yaml" "$RUN/genesis/committee.yaml" "$RUN/node5/genesis/"
    cp "$RUN/parameters.yaml" "$RUN/node5/"
}

stake_node5() {
    local rpc="http://127.0.0.1:$(rpc_port "$HEALTHY")" opaddr
    opaddr=$(cast wallet address --private-key "$OPERATOR_KEY")
    log "staking node5 (operator $opaddr) via $rpc"
    local ready=0 rls_impl reg_owner
    for _ in $(seq 1 90); do
        rls_impl=$(cast storage "$RLS_ADDRESS" "$ERC1967_IMPL_SLOT" --rpc-url "$rpc" 2>/dev/null || true)
        reg_owner=$(cast call "$REGISTRY" "owner()(address)" --rpc-url "$rpc" 2>/dev/null || true)
        [[ $rls_impl =~ [1-9a-fA-F] && -n $reg_owner && $reg_owner != 0x0000000000000000000000000000000000000000 ]] && { ready=1; break; }
        sleep 2
    done
    [[ $ready -eq 1 ]] || { log "genesis system contracts not live"; return 1; }
    cast send --private-key "$ADMIN_KEY" --rpc-url "$rpc" --value "$GAS_FUND" "$opaddr" >/dev/null 2>&1 || return 1
    cast send "$RLS_ADDRESS" "mint(address,uint256)" "$opaddr" "$STAKE_AMOUNT" --private-key "$ADMIN_KEY" --rpc-url "$rpc" >/dev/null 2>&1 || return 1
    cast send "$REGISTRY" "allowlistValidator(address)" "$opaddr" --private-key "$ADMIN_KEY" --rpc-url "$rpc" >/dev/null 2>&1 || log "  (allowlist may already be set)"
    cast send "$RLS_ADDRESS" "approve(address,uint256)" "$REGISTRY" "$STAKE_AMOUNT" --private-key "$OPERATOR_KEY" --rpc-url "$rpc" >/dev/null 2>&1 || return 1
    local calldata; calldata=$("$BIN" keytool stake-calldata --datadir "$RUN/node5" | grep 'Calldata:' | awk '{print $2}')
    [[ -n $calldata ]] || { log "no stake calldata"; return 1; }
    cast send "$REGISTRY" "$calldata" --private-key "$OPERATOR_KEY" --rpc-url "$rpc" >/dev/null 2>&1 || { log "stake tx failed"; return 1; }
    cast send "$REGISTRY" "activate()" --private-key "$OPERATOR_KEY" --rpc-url "$rpc" >/dev/null 2>&1 || { log "activate tx failed"; return 1; }
    log "node5 staked + activated; it promotes at the next epoch boundary"
}

# --- run ---
setup_network || fail "network setup failed"
VICTIM_ADDR=$(primary_addr_of "$RUN/validator-$VICTIM")
NODE5_BLS=$(bls_of "$RUN/node5")
NODE5_PID=$(primary_addr_of "$RUN/node5" | grep -oE '12D3Koo[A-Za-z0-9]+')
log "victim is validator $VICTIM; node5 (post-genesis) bls=${NODE5_BLS:0:16}..."

for seq in $(seq 1 "$N"); do start_validator "$seq"; done
log "started $N validators; warming up to block $WARMUP_BLOCKS"
wait_for 300 val_reached "$WARMUP_BLOCKS" || fail "chain did not reach block $WARMUP_BLOCKS"
log "warm-up done: validators at $(max_val_height)"

# Keep the victim's store full (sustained) so any freed slot is re-grabbed before node5 can take it.
log "attacking victim store (sustained ${ATTACK_DURATION}s)"
"$ATTACKER" "$VICTIM_ADDR" --duration-secs "$ATTACK_DURATION" --puts-per-id "$ATTACK_PUTS" --concurrency 12 >"$RUN/attacker.log" 2>&1 &
wait_for 150 bash -c "grep -qa 'kad store rejected record' '$RUN/validator-$VICTIM.log'" \
    || fail "victim store did not fill (no MaxRecords). attacker tail: $(tail -3 "$RUN/attacker.log" 2>/dev/null)"
log "victim store FULL ($(grep -ca 'kad store rejected record' "$RUN/validator-$VICTIM.log") MaxRecords rejections)"

start_node5
log "node5 started (following)"
sleep 10
stake_node5 || fail "staking node5 failed"

log "waiting up to $((EPOCH_SECS * 2 + 60))s for node5 to be seated"
seated=0
for _ in $(seq 1 $((EPOCH_SECS * 2 + 60))); do
    grep -qa "adding committee member $NODE5_BLS" "$RUN/validator-$HEALTHY.log" && { seated=1; break; }
    sleep 1
done
[[ $seated -eq 1 ]] || fail "healthy validator $HEALTHY never seated node5; check staking/epoch timing"
SEAT_TS=$(grep -a "adding committee member $NODE5_BLS" "$RUN/validator-$HEALTHY.log" | head -1 | cut -d. -f1 | sed 's/ts=//')
log "node5 seated at $SEAT_TS; observing 90s under sustained attack"
sleep 90

# The real binding is peer_id -> bls (known_peerids / peer_to_bls), not the "adding committee member"
# log, which every node emits from the replicated dial-hint. node5 is denied iff the victim never
# binds node5's peer id: node5's requests come back "requesting peer unknown" and the victim drops
# node5's gossip ("no BLS mapping" for node5's peer id).
after() { awk -v t="$SEAT_TS" '{ if (match($0,/ts=[0-9T:-]+/)) { s=substr($0,RSTART+3,RLENGTH-3); if (s >= t) print } }' "$1"; }
echo | tee -a "$RUN/summary.txt"
log "=== MEASUREMENTS (post-seating, under sustained attack) ==="
healthy_bound=$(grep -ca "adding committee member $NODE5_BLS" "$RUN/validator-$HEALTHY.log")
victim_peerids_max=$(grep -aoE 'known_peerids_len=[0-9]+' "$RUN/validator-$VICTIM.log" | grep -oE '[0-9]+' | sort -n | tail -1)
node5_reqrej_after=$(after "$RUN/node5.log" | grep -ca "requesting peer unknown")
victim_gossipdrop_node5=$(after "$RUN/validator-$VICTIM.log" | grep -a 'dropping valid gossip, no BLS mapping' | grep -ca "$NODE5_PID")
log "healthy validator $HEALTHY seated node5:                        $healthy_bound (>=1 means node5 is on the committee)"
log "victim max known_peerids_len (bound peer ids):                 ${victim_peerids_max:-?} (= $N genesis validators; excludes node5)"
log "node5 'requesting peer unknown' after seating:                 $node5_reqrej_after"
log "victim gossip-drops for node5's peer id after seating:         $victim_gossipdrop_node5"
log "chain height (validators): $(max_val_height)"

if [[ $healthy_bound -ge 1 && $node5_reqrej_after -ge 1 && $victim_gossipdrop_node5 -ge 1 && ${victim_peerids_max:-0} -le $N ]]; then
    log "ISSUE REPRODUCED: node5 is seated but the victim never binds it (known_peerids stays ${victim_peerids_max}); the victim rejects node5's requests ($node5_reqrej_after) and drops node5's gossip ($victim_gossipdrop_node5) while its store stays full"
    echo "logs: $RUN"; exit 1
elif [[ ${victim_peerids_max:-0} -gt $N ]]; then
    log "HEALTHY: the victim bound node5 (known_peerids grew to ${victim_peerids_max}) despite the full store"
    echo "logs: $RUN"; exit 0
else
    log "INCONCLUSIVE: healthy_bound=$healthy_bound peerids_max=${victim_peerids_max:-?} reqrej=$node5_reqrej_after gossipdrop=$victim_gossipdrop_node5"
    echo "logs: $RUN"; exit 2
fi
