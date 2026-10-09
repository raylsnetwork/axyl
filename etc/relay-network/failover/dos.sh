#!/usr/bin/env bash
# Simulate (or clear) a DoS on a pool relay by blackholing all inbound traffic to it. The relay
# container stays up but becomes unreachable - the dns-failover health loop pings it, sees it gone,
# and auto-flips the advertised TXT to the next warm standby.
#
# Usage: ./dos.sh <relay-service> [on|off]     (default on)
set -euo pipefail
cd "$(dirname "$0")"
COMPOSE="docker compose -f ../compose.failover.yaml"
svc="${1:?usage: dos.sh <relay-service> [on|off]}"
mode="${2:-on}"
case "$mode" in
    on)
        $COMPOSE exec -T "$svc" sh -c \
            'iptables -C INPUT -j DROP 2>/dev/null || iptables -I INPUT 1 -j DROP'
        echo "DoS ON: $svc blackholed (all inbound dropped)"
        ;;
    off)
        $COMPOSE exec -T "$svc" sh -c 'iptables -D INPUT -j DROP 2>/dev/null || true'
        echo "DoS OFF: $svc inbound restored"
        ;;
    *)
        echo "usage: dos.sh <relay-service> [on|off]"; exit 1
        ;;
esac
