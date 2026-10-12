#!/bin/bash
# Stop a node previously added by add-relay-node.sh -- the inverse of that script's process layout:
#   - its consensus node (relay-node-N): SIGTERM, then wait INDEFINITELY for a clean shutdown. No
#     kill -9 -- a hung graceful shutdown is a real bug worth catching, and force-killing would mask
#     it (watch relay-node-N.log if it blocks here).
#   - its relay (relay-N): stateless, so SIGTERM then kill -9 if it lingers.
#
# A later `add-relay-node.sh N` brings both back (it's restart-safe: reuses the datadir, revives the
# relay). Relay identity is a fixed seed, so its peer id / dialable address survive the restart.
#
# Usage:
#   ./stop-relay-node.sh [INDEX]     # default INDEX=5; must match the add-relay-node.sh index

directory=$(dirname "${BASH_SOURCE[0]}")
scriptDir=$(cd "$directory" && pwd)

NODE_NUM="${1:-5}"
ROOTDIR="$scriptDir/local-validators"
NODE_NAME="relay-node-${NODE_NUM}"
NODE_PID_FILE="${ROOTDIR}/${NODE_NAME}.pid"
RELAY_PID_FILE="${ROOTDIR}/relay-${NODE_NUM}.pid"

alive() { [[ -f "$1" ]] && kill -0 "$(cat "$1" 2>/dev/null)" 2>/dev/null; }

# --- 1. consensus node: graceful, wait forever (no kill -9) ---
if alive "$NODE_PID_FILE"; then
    pid=$(cat "$NODE_PID_FILE")
    echo "stopping ${NODE_NAME} (pid $pid) -- waiting for graceful shutdown, no kill -9..."
    kill -TERM "$pid" 2>/dev/null || true
    count=0
    while kill -0 "$pid" 2>/dev/null; do
        sleep 1
        count=$((count + 1))
        (( count % 30 == 0 )) && echo "  ${NODE_NAME} still shutting down after ${count}s -- check ${NODE_NAME}.log"
    done
    echo "${NODE_NAME} stopped."
else
    echo "${NODE_NAME} not running."
fi
rm -f "$NODE_PID_FILE"

# --- 2. relay: stateless, SIGTERM then force -9 if it lingers ---
if alive "$RELAY_PID_FILE"; then
    pid=$(cat "$RELAY_PID_FILE")
    echo "stopping relay-${NODE_NUM} (pid $pid)..."
    kill -TERM "$pid" 2>/dev/null || true
    count=0
    while kill -0 "$pid" 2>/dev/null && [[ $count -lt 5 ]]; do sleep 1; count=$((count + 1)); done
    if kill -0 "$pid" 2>/dev/null; then
        echo "  relay-${NODE_NUM} did not exit on SIGTERM, forcing kill -9"
        kill -9 "$pid" 2>/dev/null || true
    fi
    echo "relay-${NODE_NUM} stopped."
else
    echo "relay-${NODE_NUM} not running."
fi
rm -f "$RELAY_PID_FILE"
