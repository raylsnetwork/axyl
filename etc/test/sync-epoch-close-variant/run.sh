#!/bin/bash
# Live reproduction of the sync-epoch-close-variant fork.
#
# Builds the binary with byzantine-test-hooks, starts a 5-validator network with
# EpochCloseSeedV2 activated at block 50, forks an observer via a variant leader
# certificate served by an excluded committee member, then tests recovery.
#
# Exits 0 if the fork is confirmed and recovery fails (expected behavior).
# Exits 1 if the fork does not reproduce or the observer unexpectedly recovers.
#
# Usage:
#   etc/test/sync-epoch-close-variant/run.sh [--no-build]
#
# Requires: cargo, curl, python3.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
BIN="$REPO/target/debug/rayls-network"
RUNDIR=$(mktemp -d "${TMPDIR:-/tmp}/sync-variant-XXXXXX")
export RL_BLS_PASSPHRASE="sync-variant-local-only"
DEV=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
ADDRS=(0x9965507D1a55bcC2695C58ba16FB37d819B0A4dc 0x976EA74026E726554dB657fA54763abd0C3a0aa9
       0x14dC79964da2C08b23698B3D3cc7Ca32193d9955 0x23618e81E3f5cdF7f54C3d65f7FBc0aBf5B21E8f
       0x5555555555555555555555555555555555555555)
INSTANCE_BASE=10
OBS_INSTANCE=106
EPOCH_SECS=15
V2_ACTIVATION=50
PIDS=()

cleanup() {
    for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null || true; done
    wait 2>/dev/null || true
    echo "run dir: $RUNDIR"
}
trap cleanup EXIT

rpc_port() { echo $((8545 - (INSTANCE_BASE + $1) + 1)); }
obs_port() { echo $((8545 - (OBS_INSTANCE - 1))); }

blk() { # blk <port> <height-hex-or-tag> -> "number hash extra"
    curl -s -m 5 -H 'content-type: application/json' \
        -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_getBlockByNumber\",\"params\":[\"$2\",false]}" \
        "http://127.0.0.1:$1" | python3 -c 'import sys,json
try:
  b=json.load(sys.stdin)["result"]
  if not b: print("none"); raise SystemExit
  ex=b["extraData"]
  print(int(b["number"],16), b["hash"], ("EMPTY" if ex=="0x" else ex))
except SystemExit: pass
except Exception: print("down")' 2>/dev/null || echo "down"
}

# --- build ---
if [[ "${1:-}" != "--no-build" ]]; then
    echo "Building with byzantine-test-hooks..."
    (cd "$REPO" && nice -n 19 cargo build -j 4 -p rayls-network --features byzantine-test-hooks 2>&1 | tail -3)
fi
[[ -x "$BIN" ]] || { echo "FAIL: binary not found at $BIN"; exit 1; }

# --- setup: keys + genesis ---
echo "Setting up $RUNDIR..."
for n in 1 2 3 4 5; do
    "$BIN" keytool generate validator --datadir "$RUNDIR/validator-$n" --address "${ADDRS[$((n-1))]}" >/dev/null
    mkdir -p "$RUNDIR/genesis/validators"
    cp "$RUNDIR/validator-$n/node-info.yaml" "$RUNDIR/genesis/validators/validator-$n.yaml"
done
"$BIN" genesis --datadir "$RUNDIR" --chain-id 0x1e7 --epoch-duration-in-secs $EPOCH_SECS \
    --dev-funded-account $DEV --max-header-delay-ms 1000 --min-header-delay-ms 500 \
    --consensus-registry-owner $DEV --network-admin $DEV >/dev/null
for n in 1 2 3 4 5; do
    mkdir -p "$RUNDIR/validator-$n/genesis"
    cp "$RUNDIR/genesis/genesis.yaml" "$RUNDIR/genesis/committee.yaml" "$RUNDIR/validator-$n/genesis/"
    cp "$RUNDIR/parameters.yaml" "$RUNDIR/validator-$n/"
done

# --- hardfork schedule ---
cat > "$RUNDIR/schedule.yaml" <<EOF
networks:
  local:
    chain_id: 487
    hardforks:
      Eip1559: 0
      BatchDigestV2: 0
      AdminTransfer: 0
      PrecompileGasFix: 0
      RlsStorage: never
      Tokenomics: never
      Uups: never
      Erc20PrecompileBytecode: 1
      TransactionLoadBalancing: 0
      UsdrSupplyCorrection: 100
      EmptyOutputBlock: 0
      DynamicCommitteeSizing: 0
      HybridRewards: 1
      OutputSeqNormalization: 0
      SenderAffinityLoadBalancing: 0
      EpochCloseSeedV2: $V2_ACTIVATION
EOF

start_validator() { # start_validator <n> [ENV=VAL..]
    local n=$1; shift
    local inst=$((INSTANCE_BASE + n))
    env "$@" "$BIN" node --datadir "$RUNDIR/validator-$n" --config-file "$RUNDIR/schedule.yaml" \
        --subnet local --instance $inst --metrics "127.0.0.1:93$((10+n))" \
        --log.stdout.format log-fmt --full --storage.v2 \
        --db.growth-step 1MB --consensus-db.growth-step 1MB \
        --txpool.minimal-protocol-fee 0 --gpo.default-suggested-fee 0 \
        -vvv --http --http.addr 127.0.0.1 --http.api all \
        >> "$RUNDIR/validator-$n.log" 2>&1 &
    PIDS+=($!)
    echo "  validator-$n pid $! rpc $(rpc_port $n)"
}

stop_all() {
    for p in "${PIDS[@]}"; do kill -TERM "$p" 2>/dev/null || true; done
    for p in "${PIDS[@]}"; do
        for _ in $(seq 1 60); do kill -0 "$p" 2>/dev/null || break; sleep 1; done
    done
    PIDS=()
}

# ============================================================
# Phase 1: canonical history (v5 excluded from all certs)
# ============================================================
echo ""
echo "=== Phase 1: building canonical history ==="
start_validator 1
start_validator 2
start_validator 3
start_validator 4
start_validator 5 RL_BYZ_WITHHOLD_VOTE=1

echo "Waiting 90s for blocks..."
sleep 90

echo "Network tips:"
for n in 1 2 3 4 5; do
    echo "  v$n: $(blk "$(rpc_port $n)" latest)"
done

# Find a pre-activation epoch-closing block
CLOSE_HEIGHT=""
for h in $(seq 1 $((V2_ACTIVATION - 1))); do
    hx=$(printf '0x%x' "$h")
    line=$(blk "$(rpc_port 1)" "$hx")
    ex=$(echo "$line" | awk '{print $3}')
    if [[ "$ex" != "EMPTY" && "$ex" != "down" && "$ex" != "none" && -n "$ex" ]]; then
        CLOSE_HEIGHT=$h
        CANONICAL_HASH=$(echo "$line" | awk '{print $2}')
        echo "First pre-activation closing block: height=$h hash=$CANONICAL_HASH"
        break
    fi
done
[[ -n "$CLOSE_HEIGHT" ]] || { echo "FAIL: no pre-activation closing block found"; exit 1; }

# ============================================================
# Phase 2: fork the observer
# ============================================================
echo ""
echo "=== Phase 2: forking the observer ==="
stop_all

start_validator 5 RL_BYZ_WITHHOLD_VOTE=1 RL_BYZ_SERVE_VARIANT=1
sleep 3

# Start fresh observer syncing only from v5
"$BIN" keytool generate validator --datadir "$RUNDIR/observer-6" \
    --address 0x0000000000000000000000000000000000000000 >/dev/null
mkdir -p "$RUNDIR/observer-6/genesis"
cp "$RUNDIR/genesis/genesis.yaml" "$RUNDIR/genesis/committee.yaml" "$RUNDIR/observer-6/genesis/"
cp "$RUNDIR/parameters.yaml" "$RUNDIR/observer-6/"

"$BIN" node --datadir "$RUNDIR/observer-6" --config-file "$RUNDIR/schedule.yaml" --subnet local \
    --observer --instance $OBS_INSTANCE --metrics 127.0.0.1:9316 \
    --log.stdout.format log-fmt --full --storage.v2 \
    --db.growth-step 1MB --consensus-db.growth-step 1MB \
    --txpool.minimal-protocol-fee 0 --gpo.default-suggested-fee 0 \
    -vvv --http --http.addr 127.0.0.1 --http.api all \
    >> "$RUNDIR/observer-6.log" 2>&1 &
OBS_PID=$!
PIDS+=($OBS_PID)
echo "  observer-6 pid $OBS_PID rpc $(obs_port)"

echo "Waiting 45s for sync + fork..."
sleep 45

OBS_TIP=$(blk "$(obs_port)" latest)
OBS_HASH=$(blk "$(obs_port)" "$(printf '0x%x' "$CLOSE_HEIGHT")" | awk '{print $2}')
MISMATCH_COUNT=$(grep -c 'HASH MISMATCH' "$RUNDIR/observer-6.log" 2>/dev/null || echo 0)

echo "Observer tip: $OBS_TIP"
echo "Observer block $CLOSE_HEIGHT hash: $OBS_HASH"
echo "Canonical block $CLOSE_HEIGHT hash: $CANONICAL_HASH"
echo "HASH MISMATCH count: $MISMATCH_COUNT"

if [[ "$OBS_HASH" == "$CANONICAL_HASH" ]]; then
    echo "FAIL: observer has canonical hash — fork did not reproduce"
    exit 1
fi
echo "PASS: fork confirmed (different hash at closing block $CLOSE_HEIGHT)"

# ============================================================
# Phase 3: recovery test (bring honest validators back)
# ============================================================
echo ""
echo "=== Phase 3: recovery test ==="
# Stop v5, start all honest
for p in "${PIDS[@]}"; do
    [[ "$p" == "$OBS_PID" ]] && continue
    kill -TERM "$p" 2>/dev/null || true
done
sleep 5
PIDS=("$OBS_PID")

start_validator 1
start_validator 2
start_validator 3
start_validator 4
start_validator 5

echo "Waiting 60s for potential recovery..."
sleep 60

OBS_TIP_AFTER=$(blk "$(obs_port)" latest)
OBS_HASH_AFTER=$(blk "$(obs_port)" "$(printf '0x%x' "$CLOSE_HEIGHT")" | awk '{print $2}')
MISMATCH_AFTER=$(grep -c 'HASH MISMATCH' "$RUNDIR/observer-6.log" 2>/dev/null || echo 0)

echo "Observer tip after recovery attempt: $OBS_TIP_AFTER"
echo "Observer block $CLOSE_HEIGHT hash: $OBS_HASH_AFTER (canonical: $CANONICAL_HASH)"
echo "HASH MISMATCH count: $MISMATCH_AFTER"

if [[ "$OBS_HASH_AFTER" == "$CANONICAL_HASH" ]]; then
    echo "UNEXPECTED: observer recovered — the fix might already be in"
    exit 1
fi

echo ""
echo "=== RESULT ==="
echo "Fork reproduced: YES (observer block $CLOSE_HEIGHT differs from canonical)"
echo "Self-healing:    NO  (observer still forked after 60s with honest peers)"
echo "HASH MISMATCH:   $MISMATCH_AFTER (detected, not silent)"
exit 0
