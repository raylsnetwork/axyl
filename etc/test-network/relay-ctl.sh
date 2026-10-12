#!/usr/bin/env bash
#
# Stop / start / restart a single relay by index, using the same fixed-identity scheme as
# local-testnet.sh and add-relay-node.sh -- so the relay keeps its peer id across a bounce and the
# fronted validator re-reserves on its own (retry_relay_reservations, ~15s) with no node restart.
#
# Usage:
#   ./relay-ctl.sh stop    <N> [--backup]
#   ./relay-ctl.sh start   <N> [--backup]
#   ./relay-ctl.sh restart <N> [--backup]
#
# N = 1-based index (validator index, or the add-relay-node index, e.g. 6 for relay-node-6).
#   primary (default): port 50000+(N-1), seed = byte N repeated 32x,          pid/log relay-N{.pid,.log}
#   --backup:          port 51000+(N-1), seed = byte (0xb0+N-1) repeated 32x, pid/log relay-N-b{.pid,.log}
# (Added nodes have no backup relay; --backup is only meaningful for base validators.)
#
# Honors BUILD_CONFIG (default release), matching the other scripts.

set -uo pipefail

directory=$(dirname "${BASH_SOURCE[0]}")
scriptDir=$(cd "$directory" && pwd)
ROOTDIR="$scriptDir/local-validators"
BUILD_CONFIG="${BUILD_CONFIG:-release}"
RELAY_BIN="$scriptDir/../../target/${BUILD_CONFIG}/rayls-relay"

action="${1:-}"
N="${2:-}"
variant="${3:-}"

[[ -n "$action" && -n "$N" ]] || { echo "usage: $0 <start|stop|restart> <N> [--backup]"; exit 1; }
[[ "$N" =~ ^[0-9]+$ ]] || { echo "Error: N must be a number (got '$N')"; exit 1; }

idx=$((N - 1))
if [[ "$variant" == "--backup" ]]; then
    label="relay-${N}-b"
    port=$((51000 + idx))
    seed_byte=$(printf '%02x' $((0xb0 + idx)))
elif [[ -z "$variant" ]]; then
    label="relay-${N}"
    port=$((50000 + idx))
    seed_byte=$(printf '%02x' "$N")
else
    echo "Error: unknown option '$variant' (only --backup)"; exit 1
fi
pidfile="${ROOTDIR}/${label}.pid"
logfile="${ROOTDIR}/${label}.log"

alive() { [[ -f "$pidfile" ]] && kill -0 "$(cat "$pidfile" 2>/dev/null)" 2>/dev/null; }

stop_relay() {
    if ! alive; then echo "${label} not running"; return 0; fi
    local pid; pid=$(cat "$pidfile")
    echo "stopping ${label} (pid $pid, :$port)"
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 25); do kill -0 "$pid" 2>/dev/null || break; sleep 0.2; done
    if kill -0 "$pid" 2>/dev/null; then echo "  did not exit; force kill"; kill -9 "$pid" 2>/dev/null || true; fi
    rm -f "$pidfile"
}

start_relay() {
    if alive; then echo "${label} already running (pid $(cat "$pidfile"))"; return 0; fi
    [[ -x "$RELAY_BIN" ]] || { echo "Error: $RELAY_BIN not built (BUILD_CONFIG=$BUILD_CONFIG)"; exit 1; }
    local seed="" c
    for ((c = 0; c < 32; c++)); do seed="${seed}${seed_byte}"; done
    echo "starting ${label} on 0.0.0.0:${port} (seed 0x${seed_byte}*32)"
    RELAY_SEED_HEX="$seed" RELAY_PORT="$port" "$RELAY_BIN" >> "$logfile" 2>&1 &
    echo $! > "$pidfile"
    echo "  pid $(cat "$pidfile"), log ${logfile}"
}

case "$action" in
    stop)    stop_relay ;;
    start)   start_relay ;;
    restart) stop_relay; sleep 0.5; start_relay ;;
    *) echo "Error: unknown action '$action' (use start|stop|restart)"; exit 1 ;;
esac
