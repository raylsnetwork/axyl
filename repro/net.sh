#!/bin/bash
# Live reproduction driver for the POWER-LOSS durability issue.
# N validators from debug binaries, one machine, no observer, no attacker.
#
# This file lives only on the reproduction branch and is never merged to main.
#
# Unlike a process crash (which loses a write that was never persisted), this models a POWER LOSS:
# the write IS persisted, committed and readable, and would survive `kill -9`. It is lost only
# because MDBX SafeNoSync has not fsynced it. The binary's
# env-gated hook, when a node is armed, force-syncs and snapshots the consensus datafile BEFORE the
# node's next header/vote write, so the snapshot excludes that round's record:
#   RL_REPRO_PL_ARM_FILE   per-node arm file; snapshot taken once, before the first armed write
#   RL_REPRO_PL_DBFILE     the node's consensus-db/mdbx.dat; snapshot written as <dbfile>.pl-snap
# A snapshotting node logs "RL_REPRO power-loss snapshot taken". The driver then SIGKILLs it and
# restores the snapshot over mdbx.dat (the power loss: the committed-but-unsynced tail is gone),
# then restarts it clean.
#
# Commands:
#   net.sh setup <N> [epoch_secs]     fresh keys + genesis for N validators
#   net.sh start-all <N>              start all N nodes clean
#   net.sh start <N> <n> [ENV=VAL..]  start node n (1..N)
#   net.sh stop|kill <N> <n|all>
#   net.sh power-loss <N> <n>          restore node n's pre-write snapshot over its consensus DB
#   net.sh tip <N>                    block number and hash per node
#   net.sh alive <N>                  pid, state, exit code per node
#   net.sh scan <N>                   grep logs for equivocation, aborts and conflicting headers
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOTDIR=$(cd "$HERE/.." && pwd)
BIN="$ROOTDIR/target/debug/rayls-network"
# Build the node once on a fresh checkout so `fetch branch -> ./repro/reproduce.sh N` just works.
if [ ! -x "$BIN" ]; then
    echo "[net.sh] $BIN missing, building it (one-time, a few minutes)…" >&2
    ( cd "$ROOTDIR" && nice -n 19 cargo build -p rayls-network ) >&2 \
        || { echo "[net.sh] failed to build rayls-network" >&2; exit 1; }
fi
RUNS="$HERE/runs"
RUN="live"
ROOT="$RUNS/$RUN"
export RL_BLS_PASSPHRASE="power-loss-durability-local-only"
DEV=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
# Deterministic funded addresses, one per validator, reused across sizes.
ADDRS=(0x9965507D1a55bcC2695C58ba16FB37d819B0A4dc 0x976EA74026E726554dB657fA54763abd0C3a0aa9
       0x14dC79964da2C08b23698B3D3cc7Ca32193d9955 0x23618e81E3f5cdF7f54C3d65f7FBc0aBf5B21E8f
       0x5555555555555555555555555555555555555555 0x6666666666666666666666666666666666666666
       0x7777777777777777777777777777777777777777 0x8888888888888888888888888888888888888888
       0x9999999999999999999999999999999999999999 0xaAAaAAaAAAAAaaaaAAaAaaAAaaAaAAAAAaaaaAAA)
INSTANCE_BASE=20
rpc_port() { echo $((8545 - (INSTANCE_BASE + $1) + 1)); }
name() { echo "validator-$1"; }
dbfile() { echo "$ROOT/$(name $1)/consensus-db/mdbx.dat"; }

cmd=${1:?command}; N=${2:?N}; shift 2 || true

case "$cmd" in
setup)
    epoch=${1:-120}
    rm -rf "$ROOT"; mkdir -p "$ROOT/genesis/validators"
    for n in $(seq 1 "$N"); do
        "$BIN" keytool generate validator --datadir "$ROOT/validator-$n" \
            --address "${ADDRS[$((n-1))]}" >/dev/null
        cp "$ROOT/validator-$n/node-info.yaml" "$ROOT/genesis/validators/validator-$n.yaml"
    done
    "$BIN" genesis --datadir "$ROOT" --chain-id 0x1e7 --epoch-duration-in-secs "$epoch" \
        --dev-funded-account $DEV --max-header-delay-ms ${MAXMS:-2000} --min-header-delay-ms ${MINMS:-1000} \
        --consensus-registry-owner $DEV --network-admin $DEV >/dev/null
    for n in $(seq 1 "$N"); do
        d="$ROOT/$(name $n)"; mkdir -p "$d/genesis"
        cp "$ROOT/genesis/genesis.yaml" "$ROOT/genesis/committee.yaml" "$d/genesis/"
        cp "$ROOT/parameters.yaml" "$d/"
    done
    echo "setup done: $N validators, epoch ${epoch}s, $ROOT"
    ;;
start)
    n=${1:?node}; shift || true
    nm=$(name $n); inst=$((INSTANCE_BASE + n))
    # Clean start: clear any arm file and leftover snapshot from a previous cycle.
    rm -f "$ROOT/$nm.armpowerloss" "$(dbfile $n).pl-snap"
    echo "--- start $(date -u +%FT%TZ) node=$n env=$* ---" >> "$ROOT/$nm.log"
    ( env RL_REPRO_PL_ARM_FILE="$ROOT/$nm.armpowerloss" RL_REPRO_PL_DBFILE="$(dbfile $n)" "$@" "$BIN" node --datadir "$ROOT/$nm" --network local \
        --instance $inst --metrics "127.0.0.1:94$((10+n))" --log.stdout.format log-fmt --log.stdout.filter "info,primary::certifier=debug,primary::proposer=debug,repro=warn" --full \
        --storage.v2 --db.growth-step 1MB --consensus-db.growth-step 1MB \
        --txpool.minimal-protocol-fee 0 --gpo.default-suggested-fee 0 \
        -vvv --http --http.addr 127.0.0.1 --http.api all >> "$ROOT/$nm.log" 2>&1
      echo "$(date -u +%FT%TZ) exit=$?" >> "$ROOT/$nm.exit" ) &
    for _ in $(seq 1 50); do
        pid=$(pgrep -f -- "--datadir $ROOT/$nm " | head -1 || true)
        [[ -n "$pid" ]] && break; sleep 0.2
    done
    echo "$pid" > "$ROOT/$nm.pid"
    echo "$nm pid $pid rpc $(rpc_port $n) env=$*"
    ;;
start-all)
    for n in $(seq 1 "$N"); do "$HERE/net.sh" start "$N" "$n"; sleep 0.3; done
    ;;
stop|kill)
    sig=TERM; [[ $cmd == kill ]] && sig=KILL
    targets=${1:?target}; [[ $targets == all ]] && targets="$(seq 1 "$N")"
    for n in $targets; do
        pf="$ROOT/$(name $n).pid"; [[ -f $pf ]] || continue
        pid=$(cat "$pf"); kill -$sig "$pid" 2>/dev/null || true
        for _ in $(seq 1 120); do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
        rm -f "$pf"; echo "$(name $n) $cmd (pid $pid)"
    done
    ;;
power-loss)
    # Restore node n's pre-write snapshot over its consensus datafile: the committed-but-unsynced
    # tail (this round's header/vote/cert) vanishes, exactly as a power loss would drop it. The node
    # must already be dead (kill it first). The snapshot is removed after use.
    n=${1:?node}; nm=$(name $n); df=$(dbfile $n); snap="$df.pl-snap"
    if [[ ! -f "$snap" ]]; then echo "$nm: no snapshot ($snap); node was not armed/snapshotted" >&2; exit 1; fi
    cp -f "$snap" "$df"
    rm -f "$(dirname "$df")/mdbx.lck" "$snap"
    echo "$nm power-loss: restored last-synced consensus DB (dropped the unsynced tail)"
    ;;
tip)
    for n in $(seq 1 "$N"); do
        p=$(rpc_port $n)
        r=$(curl -s -m 3 -H 'content-type: application/json' \
            -d '{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["latest",false]}' \
            "http://127.0.0.1:$p" | python3 -c 'import sys,json
try:
  b=json.load(sys.stdin)["result"]; print(int(b["number"],16), b["hash"][:18])
except Exception: print("down")' 2>/dev/null || echo down)
        echo "$(name $n) $r"
    done
    ;;
alive)
    for n in $(seq 1 "$N"); do
        nm=$(name $n); pid=$(cat "$ROOT/$nm.pid" 2>/dev/null || echo -)
        st=dead
        if [[ $pid != - ]] && kill -0 "$pid" 2>/dev/null; then st=alive; fi
        ex=$(tail -1 "$ROOT/$nm.exit" 2>/dev/null || echo -)
        echo "$nm $st last_exit=$ex"
    done
    ;;
scan)
    echo "== equivocation / abort / conflicting headers =="
    for n in $(seq 1 "$N"); do
        nm=$(name $n); log="$ROOT/$nm.log"
        eq=$(grep -c "CertificateEquivocation" "$log" 2>/dev/null || echo 0)
        ab=$(grep -c "critical task .* returned Err\|panicked\|aborting" "$log" 2>/dev/null || echo 0)
        df=$(grep -c "submitted different header" "$log" 2>/dev/null || echo 0)
        al=$(grep -c "Already voted for header" "$log" 2>/dev/null || echo 0)
        echo "$nm equivocation=$eq abort=$ab different_header=$df already_voted=$al"
    done
    ;;
*) echo "unknown command $cmd"; exit 1;;
esac
