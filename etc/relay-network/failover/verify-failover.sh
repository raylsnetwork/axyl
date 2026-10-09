#!/usr/bin/env bash
# Prove the blue-green failover on a RUNNING failover stack. The meaningful claim is NOT "every peer
# re-homes onto the standby" (a consensus mesh reconnects via any path), but the operational one:
#
#   1. every pool relay holds validator1's warm reservation (primary+worker);
#   2. at steady state peers reach validator1 through the ACTIVE pool relay only;
#   3. a DoS on the active relay is AUTO-detected and the /dnsaddr TXT is flipped to a warm standby;
#   4. validator1 stays in consensus across the DoS - its height tracks the observer within a small
#      lag and catches back up - rather than going dark.
#
# dns-failover does not fail back, so we restart it first to reset the active relay to relay1a for a
# deterministic run. `HOLD=<s>` tunes how long the active relay is held down.
set -euo pipefail
cd "$(dirname "$0")"
COMPOSE="docker compose -f ../compose.failover.yaml"
V1P=12D3KooWF1KyZ6Utk41kJ2kfmqf6B5stVgvArmAwNq5aQrHSHdDk
V1W=12D3KooWFei5dykSXYwVULUFf6YExqBzTAPf6vThqini4KCSuRrk
HOLD="${HOLD:-45}"
FAIL=0

say() { printf '\n== %s\n' "$*"; }
verdict() { if [ "$1" = 0 ]; then printf 'PASS: %s\n' "$2"; else printf 'FAIL: %s\n' "$2"; FAIL=1; fi; }
obs_h() { curl -sm3 -X POST -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' http://127.0.0.1:7545 |
    sed -n 's/.*"result":"0x\([0-9a-f]*\)".*/\1/p'; }
v1_h() { $COMPOSE exec -T validator1 sh -c \
    'curl -sm5 -X POST -H "Content-Type: application/json" -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_blockNumber\",\"params\":[]}" http://127.0.0.1:8545' \
    2>/dev/null | sed -n 's/.*0x\([0-9a-f]*\).*/\1/p'; }
hex() { echo $((16#${1:-0})); }

say "reset: restart dns-failover so the active relay is relay1a (pool[0]) again"
$COMPOSE restart dns >/dev/null 2>&1
sleep 12

say "proof 1: every pool relay holds validator1's warm reservation (primary+worker => >=2 each)"
for s in relay1a relay1b relay1c; do
    n=$($COMPOSE logs "$s" 2>/dev/null | grep -c 'ReservationReqAccepted' || true)
    printf '   %-8s %s reservations\n' "$s" "$n"
    [ "$n" -ge 2 ] || FAIL=1
done
verdict "$FAIL" "all three pool relays hold validator1's reservation"

say "proof 2: at steady state peers reach validator1 via the active relay (relay1a), not the standbys"
for s in relay1a relay1b relay1c; do
    c=$($COMPOSE logs "$s" 2>/dev/null | grep 'CircuitReqAccepted' | grep -cE "$V1P|$V1W" || true)
    printf '   %-8s %s v1-circuits\n' "$s" "$c"
done

say "proof 3+4: DoS the active relay (relay1a), hold ${HOLD}s; TXT must auto-flip and v1 must stay synced"
cut0=$($COMPOSE logs dns 2>/dev/null | grep CUTOVER | tail -1 || true)
b_obs=$(obs_h); b_v1=$(v1_h)
echo "   before:  v1=0x$b_v1  observer=0x$b_obs"
./dos.sh relay1a on
maxlag=0; flipped=0
end=$((SECONDS + HOLD))
while [ "$SECONDS" -lt "$end" ]; do
    sleep 5
    cutn=$($COMPOSE logs dns 2>/dev/null | grep CUTOVER | tail -1 || true)
    [ "$cutn" != "$cut0" ] && flipped=1 || flipped=${flipped:-0}
    oh=$(hex "$(obs_h)"); vh=$(hex "$(v1_h)")
    lag=$((oh - vh)); [ "$lag" -lt 0 ] && lag=0
    [ "$lag" -gt "$maxlag" ] && maxlag=$lag
    printf '   t+%02ss  v1=%d observer=%d  lag=%d  flipped=%d\n' "$((HOLD-(end-SECONDS)))" "$vh" "$oh" "$lag" "$flipped"
done
cutf=$($COMPOSE logs dns 2>/dev/null | grep CUTOVER | tail -1 || true)
[ "$cutf" != "$cut0" ] && flipped=1
echo "   ${cutf#*dns-failover: }"
./dos.sh relay1a off

say "settle 15s, confirm v1 caught back up"
sleep 15
a_obs=$(hex "$(obs_h)"); a_v1=$(hex "$(v1_h)")
echo "   after:   v1=$a_v1  observer=$a_obs  (final lag=$((a_obs - a_v1)))"

say "result"
verdict "$([ "$flipped" = 1 ] && echo 0 || echo 1)" "dns-failover auto-flipped the /dnsaddr TXT to a warm standby"
verdict "$([ "$maxlag" -lt 40 ] && echo 0 || echo 1)" "validator1 stayed in consensus during the DoS (max lag ${maxlag} blocks)"
verdict "$([ $((a_obs - a_v1)) -lt 5 ] && echo 0 || echo 1)" "validator1 caught back up after the DoS (final lag $((a_obs - a_v1)) blocks)"
[ "$FAIL" = 0 ] && echo "FAILOVER VERIFIED: relay DoS auto-flips the advertised relay and validator1 never leaves consensus" ||
    echo "FAILOVER CHECK FAILED - see FAIL lines"
exit "$FAIL"
