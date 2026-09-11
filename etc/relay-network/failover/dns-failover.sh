#!/bin/sh
# Blue-green failover control plane: dnsmasq + an auto health-flip loop, in one box.
#
# The failover-subject validator advertises /dnsaddr/<V1_HOST>. The `_dnsaddr.<V1_HOST>` TXT record
# names exactly ONE pool relay (the ACTIVE one) - this is what makes it blue-green rather than
# active/active: peers only ever dial the advertised relay. The validator holds WARM reservations
# on every pool relay, so a cutover is just a TXT rewrite; peers re-resolve on their next redial and
# follow (the node re-resolves /dnsaddr per redial, so a flipped record moves them).
#
# The loop pings the active relay's public leg. On a DoS (simulated by blackholing that relay, see
# dos.sh) the ping fails, and we advance the TXT to the next HEALTHY pool member (rotation: a->b->c),
# regenerate the record, and restart dnsmasq. A real deployment would use a richer signal than ICMP
# (circuit success rate, packet-rate metrics); ICMP reachability is the POC proxy for "relay gone".
#
# Env:
#   V1_HOST           - the /dnsaddr host advertised by the failover subject (e.g. v1.rayls.test)
#   V1_NODEINFO_DIR   - mounted validator data dir; node-info.yaml carries the primary+worker peer ids
#   POOL              - space-separated "ip:relay_peer_id" pool members, active starts at the first
#   TTL               - TXT record TTL seconds (low, so peers re-resolve quickly). Default 2.
#   PROBE_INTERVAL    - seconds between health probes. Default 2.
#   FAIL_THRESHOLD    - consecutive failed probes before a cutover. Default 2.
#   STATIC_ARGS       - optional extra dnsmasq args (e.g. observer host-records), space-separated
set -eu

TTL="${TTL:-2}"
PROBE_INTERVAL="${PROBE_INTERVAL:-2}"
FAIL_THRESHOLD="${FAIL_THRESHOLD:-2}"
CONF=/tmp/failover-dnsmasq.conf

log() { printf '%s dns-failover: %s\n' "$(cat /proc/uptime | cut -d. -f1)s" "$*"; }

# --- discover the subject's primary + worker peer ids from its node-info.yaml -------------------
ninfo=""
for _ in $(seq 1 120); do
    ninfo=$(find "$V1_NODEINFO_DIR" -name node-info.yaml 2>/dev/null | head -1 || true)
    [ -n "$ninfo" ] && break
    sleep 1
done
[ -n "$ninfo" ] || { echo "FATAL: node-info.yaml not found under $V1_NODEINFO_DIR"; exit 1; }
# primary network_address is listed before worker; both are /dnsaddr/<host>/p2p/<id>
PRIMARY_ID=$(grep -oE '/p2p/12D3KooW[A-Za-z0-9]+' "$ninfo" | grep -oE '12D3KooW[A-Za-z0-9]+' | sed -n 1p)
WORKER_ID=$(grep -oE '/p2p/12D3KooW[A-Za-z0-9]+' "$ninfo" | grep -oE '12D3KooW[A-Za-z0-9]+' | sed -n 2p)
[ -n "$PRIMARY_ID" ] && [ -n "$WORKER_ID" ] || { echo "FATAL: could not read peer ids from $ninfo"; exit 1; }
log "subject $V1_HOST primary=$PRIMARY_ID worker=$WORKER_ID"

# --- pool as positional list --------------------------------------------------------------------
set -- $POOL
POOL_N=$#
member_at() { i=$1; shift; eval "echo \${$i}"; }   # member_at <1-based idx> $POOL
ACTIVE=1

active_ip() { member_at "$ACTIVE" $POOL | cut -d: -f1; }
active_rid() { member_at "$ACTIVE" $POOL | cut -d: -f2; }

write_conf() {
    ip=$(active_ip); rid=$(active_rid)
    circ="/ip4/$ip/udp/4001/quic-v1/p2p/$rid/p2p-circuit/p2p"
    {
        echo "txt-record=_dnsaddr.$V1_HOST,dnsaddr=$circ/$PRIMARY_ID"
        echo "txt-record=_dnsaddr.$V1_HOST,dnsaddr=$circ/$WORKER_ID"
    } > "$CONF"
}

DNSMASQ_PID=""
start_dnsmasq() {
    # shellcheck disable=SC2086
    dnsmasq --no-daemon --no-hosts --no-resolv --log-queries --port=53 --local-ttl="$TTL" \
        --conf-file="$CONF" ${STATIC_ARGS:-} >> /tmp/dnsmasq.log 2>&1 &
    DNSMASQ_PID=$!
}
restart_dnsmasq() {
    [ -n "$DNSMASQ_PID" ] && kill "$DNSMASQ_PID" 2>/dev/null || true
    wait "$DNSMASQ_PID" 2>/dev/null || true
    start_dnsmasq
}

healthy() { ping -c1 -W1 "$1" >/dev/null 2>&1; }

# pick the next healthy pool member after the current active (wraps); echoes its index or nothing
next_healthy() {
    n=1
    while [ "$n" -le "$POOL_N" ]; do
        cand=$(( (ACTIVE - 1 + n) % POOL_N + 1 ))
        ip=$(member_at "$cand" $POOL | cut -d: -f1)
        if healthy "$ip"; then echo "$cand"; return 0; fi
        n=$((n + 1))
    done
    return 1
}

write_conf
start_dnsmasq
log "ACTIVE = pool[$ACTIVE] $(active_ip) (of $POOL_N warm pool members); dnsmasq up"

fails=0
while true; do
    sleep "$PROBE_INTERVAL"
    if healthy "$(active_ip)"; then
        fails=0
        continue
    fi
    fails=$((fails + 1))
    [ "$fails" -lt "$FAIL_THRESHOLD" ] && continue
    log "active relay $(active_ip) unreachable ($fails probes) - seeking standby"
    if nxt=$(next_healthy); then
        old=$(active_ip)
        ACTIVE=$nxt
        write_conf
        restart_dnsmasq
        log "CUTOVER: $old -> $(active_ip) (pool[$ACTIVE]); TXT flipped, dnsmasq restarted"
        fails=0
    else
        log "no healthy standby available; staying on $(active_ip), will retry"
    fi
done
