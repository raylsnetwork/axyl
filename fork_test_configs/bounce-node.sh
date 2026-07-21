#!/usr/bin/env bash
# Chaos-loop: wait until a node is caught up, then stop it (and its relays) and restart it, repeat.
# Run from the repo root.
#
#   base (genesis) validator:   ./fork_test_configs/bounce-node.sh <SEQ>
#                               SEQ is 0-based (1 = validator-2). Restart goes through
#                               local-testnet.sh --stop/--start-validator, which needs the SAME
#                               mode flags the net was started with (default off):
#                                   RELAY_DNS=1 MULTI_LISTEN=1 ./fork_test_configs/bounce-node.sh 1
#
#   dynamically-added node:     ADDED=1 ./fork_test_configs/bounce-node.sh <INDEX>
#                               INDEX = the add-relay-node.sh index (e.g. 6). Restart goes through
#                               stop-relay-node.sh + add-relay-node.sh, which carry their own
#                               relay/DNS env. DNSMASQ_PORT selects the resolver view the node uses
#                               to reach the committee (default 5353, the private/direct view;
#                               set DNSMASQ_PORT=5354 for the public/relay view). Matches
#                               add-relay-node.sh's default so add + bounce stay consistent.
#
# stop is graceful and waits INDEFINITELY for a clean shutdown (no kill -9), so a hung shutdown
# blocks the loop here on purpose -- inspect the node's log instead of losing the failure.

ADDED="${ADDED:-0}"
IDX="${1:-1}"

# Keep the node DOWN this long before restarting. Default 0 = restart immediately (tests a quick
# bounce, which usually rejoins via normal gossip). Set it above ~2 epoch durations to make the
# node fall behind across epoch boundaries: it comes back with a stale committee view, its
# consensus-output gossip mesh stays empty, and the forward streamer's proactive idle-probe
# catch-up path is exercised.
DOWN_SECS="${DOWN_SECS:-0}"

LOCAL_TESTNET_SCRIPT="./etc/test-network/local-testnet.sh"
ADD_RELAY_NODE_SCRIPT="./etc/test-network/add-relay-node.sh"
STOP_RELAY_NODE_SCRIPT="./etc/test-network/stop-relay-node.sh"

if [[ "$ADDED" == "1" ]]; then
    NODE_NUM="$IDX"
    LABEL="relay-node-${NODE_NUM}"
    # Mirror add-relay-node.sh's port scheme: INSTANCE=100+NODE_NUM, and reth shifts its rpc port
    # down by (instance-1) from 8545. Keep in sync with add-relay-node.sh if that formula changes.
    INSTANCE=$((100 + NODE_NUM))
    RPC_PORT=$((8545 - (INSTANCE - 1)))
    DNSMASQ_PORT="${DNSMASQ_PORT:-5353}"
    stop_node()  { bash "$STOP_RELAY_NODE_SCRIPT" "$NODE_NUM"; }
    start_node() { DNSMASQ_PORT="$DNSMASQ_PORT" bash "$ADD_RELAY_NODE_SCRIPT" "$NODE_NUM"; }
else
    SEQ="$IDX"
    LABEL="validator-$((SEQ + 1))"
    RPC_PORT=$((8545 - SEQ))
    # Match the mode the network was started with (default: plain, no relays).
    RELAY_DNS="${RELAY_DNS:-0}"
    export MULTI_LISTEN="${MULTI_LISTEN:-0}"   # local-testnet.sh reads this from the env
    start_flags=()
    [[ "$RELAY_DNS" == "1" ]] && start_flags+=(--relay-dns)
    stop_node()  { bash "$LOCAL_TESTNET_SCRIPT" --stop-validator "$SEQ"; }
    start_node() { bash "$LOCAL_TESTNET_SCRIPT" --start-validator "$SEQ" "${start_flags[@]}"; }
fi

is_caught_up() {
  curl -sS --max-time 2 -X POST "http://localhost:${RPC_PORT}" \
    -H 'content-type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"rayls_nodeStatus","params":[]}' 2>/dev/null \
    | grep -q '"is_caught_up":true'
}

wait_until_caught_up() {
  local waited=0
  until is_caught_up; do
    sleep 2; waited=$((waited + 2))
    (( waited % 30 == 0 )) && echo "  ${LABEL} still catching up (${waited}s)..."
  done
  echo "${LABEL} caught up after ${waited}s"
}

echo "chaos loop: ${LABEL} (rpc :${RPC_PORT}, added=${ADDED})"
while true; do
  wait_until_caught_up
  echo "caught up; waiting 30s before stopping ${LABEL}..."
  sleep 30

  echo "stopping ${LABEL}..."
  stop_node

  if [[ "$DOWN_SECS" -gt 0 ]]; then
    echo "keeping ${LABEL} down ${DOWN_SECS}s (to fall behind across epoch boundaries)..."
    sleep "$DOWN_SECS"
  fi

  echo "restarting ${LABEL}..."
  start_node
done
