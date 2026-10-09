#!/usr/bin/env bash
# Deterministically verify the relay-isolation topology on a RUNNING stack (see README.md).
#
# Three independent proofs, each with a hard pass/fail verdict:
#
#   1. Relay-side positive accounting: every validator<->validator and observer->validator link
#      exists as a circuit on some relay, and every relay holds exactly the reservations the
#      design prescribes (own validator primary+worker via the DMZ leg, nothing else).
#   2. Kernel packet ledger (negative proof): iptables OUTPUT counters on every validator
#      partition ALL egress into {own relay DMZ leg, public relays, DNS, loopback, OTHER}.
#      After a soak under live consensus, OTHER must be exactly 0 packets - the kernel counts
#      every packet, so this is exhaustive, not sampled. The own-public-leg class must also be
#      0 (the reservation rides the DMZ leg; dialing the same relay under two address forms
#      would race the relay client's reservation arbitration).
#   3. Inbound: each validator's INPUT chain accepts only loopback and conntrack
#      RELATED,ESTABLISHED replies to flows it opened itself (printed; the policy-DROP counter
#      counts unsolicited probes).
#
# --causal additionally runs the counterfactual: stop ALL relays -> consensus and observer sync
# must freeze completely (any direct validator path would keep 3/4 quorum alive) while the
# validators stay up; restart -> reservations return (<=15s tick) and committee re-dials on the
# peer-manager heartbeat (<=30s). WARNING: this freezes the network for ~1-2 minutes.
#
# Usage: ./verify-topology.sh [--causal]   (SOAK=<secs> to change the ledger window, default 120)

set -euo pipefail
cd "$(dirname "$0")"
COMPOSE="docker compose -f compose.yaml"
SOAK="${SOAK:-120}"
FAIL=0

say() { printf '\n== %s\n' "$*"; }
verdict() { # verdict <ok:0|1> <label>
    if [ "$1" -eq 0 ]; then printf 'PASS: %s\n' "$2"; else printf 'FAIL: %s\n' "$2"; FAIL=1; fi
}

# Validator1's block height via its in-container RPC (validators publish no host ports).
# Prints 0 when the node is unreachable or mid-boot: sed exits 0 even on no-match, so the guard
# must default the EMPTY string, not rely on exit codes - `$((16#))` is a fatal arithmetic error.
v1_height() {
    local h
    h=$($COMPOSE exec validator1 sh -c \
        "curl -sm3 -X POST -H 'Content-Type: application/json' -d '{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_blockNumber\",\"params\":[]}' http://127.0.0.1:8545" |
        sed -n 's/.*0x\([0-9a-f]*\).*/\1/p' || true)
    echo $((16#${h:-0}))
}

# ---- peer-id map: each node logs its two swarm ids (primary + worker) at boot ------------------
# stored in IDS_<node> variables (macOS bash 3.2 has no associative arrays)
ids_of() { local var="IDS_$1"; echo "${!var}"; }
say "peer-id map"
for n in validator1 validator2 validator3 validator4 observer1 observer2; do
    # `|| true`: a node that has not logged its boot line yet (stack still starting) must
    # surface as a FAIL verdict below, not abort the whole script via set -e/pipefail
    ids=$($COMPOSE logs "$n" 2>/dev/null | grep 'network loop STARTING' |
        sed -n 's/.*PeerId(\\"\([A-Za-z0-9]*\)\\").*/\1/p' | sort -u | tail -2 | tr '\n' ' ' ||
        true)
    eval "IDS_$n=\"$ids\""
    echo "$n: $ids"
    [ "$(echo "$ids" | wc -w)" -eq 2 ] ||
        verdict 1 "$n: expected 2 peer ids (node not booted yet? stack starting?)"
done

# ---- proof 1: relay-side accounting ------------------------------------------------------------
say "proof 1: reservations (each relay serves exactly its own validator)"
for n in 1 2 3 4; do
    res=$($COMPOSE logs "relay$n" 2>/dev/null |
        grep -o 'ReservationReqAccepted { src_peer_id: PeerId("[A-Za-z0-9]*")' |
        sed 's/.*PeerId("\(.*\)")/\1/' | sort -u || true)
    ok=0
    own_ids=$(ids_of "validator$n")
    [ -n "$own_ids" ] || ok=1 # unknown ids cannot vacuously pass
    for id in $own_ids; do
        echo "$res" | grep -q "$id" || ok=1
    done
    verdict $ok "relay$n holds reservations for validator$n (primary+worker)"
    # only-own-relay restriction: no other validator may hold a reservation here
    foreign=0
    for m in 1 2 3 4; do
        [ "$m" -eq "$n" ] && continue
        for id in $(ids_of "validator$m"); do
            echo "$res" | grep -q "$id" && foreign=1
        done
    done
    verdict $foreign "relay$n holds no reservation from any other validator"
done

say "proof 1: every consensus link is a relay circuit"
CIRCUITS=$(for n in 1 2 3 4; do
    $COMPOSE logs "relay$n" 2>/dev/null |
        grep -o 'CircuitReqAccepted { src_peer_id: PeerId("[A-Za-z0-9]*"), dst_peer_id: PeerId("[A-Za-z0-9]*")' |
        sed 's/.*src_peer_id: PeerId("\([A-Za-z0-9]*\)"), dst_peer_id: PeerId("\([A-Za-z0-9]*\)")/\1 \2/'
done | sort -u || true)
pair_circuits() { # pair_circuits "<ids of A>" "<ids of B>" -> count of circuits between A and B
    local count=0 a b
    for a in $1; do for b in $2; do
        count=$((count + $(echo "$CIRCUITS" | grep -c -e "^$a $b$" -e "^$b $a$" || true)))
    done; done
    echo "$count"
}
# NOTE: no `seq $((a+1)) 4` here - BSD seq counts BACKWARDS when start > end instead of
# returning empty, which fabricates pairs like 4<->5 on macOS
for a in 1 2 3; do
    b=$((a + 1))
    while [ "$b" -le 4 ]; do
        c=$(pair_circuits "$(ids_of "validator$a")" "$(ids_of "validator$b")")
        verdict $([ "$c" -ge 2 ] && echo 0 || echo 1) \
            "validator$a<->validator$b via circuits (primary+worker: $c found)"
        b=$((b + 1))
    done
done
for o in observer1 observer2; do for v in 1 2 3 4; do
    c=$(pair_circuits "$(ids_of "$o")" "$(ids_of "validator$v")")
    verdict $([ "$c" -ge 1 ] && echo 0 || echo 1) "$o->validator$v via circuits ($c found)"
done; done

# ---- proof 2: kernel packet ledger -------------------------------------------------------------
say "proof 2: arming kernel ledger on all validators, soaking ${SOAK}s under live consensus"
for n in 1 2 3 4; do
    $COMPOSE exec "validator$n" sh -c "
        iptables -N TOPOAUDIT 2>/dev/null; iptables -F TOPOAUDIT;
        iptables -A TOPOAUDIT -o lo -j RETURN;
        iptables -A TOPOAUDIT -d 10.10.$n.3 -j RETURN;
        iptables -A TOPOAUDIT -d 10.20.0.11 -j RETURN;
        iptables -A TOPOAUDIT -d 10.20.0.12 -j RETURN;
        iptables -A TOPOAUDIT -d 10.20.0.13 -j RETURN;
        iptables -A TOPOAUDIT -d 10.20.0.14 -j RETURN;
        iptables -A TOPOAUDIT -d 10.20.0.53 -j RETURN;
        iptables -A TOPOAUDIT -j RETURN;
        iptables -C OUTPUT -j TOPOAUDIT 2>/dev/null || iptables -I OUTPUT 1 -j TOPOAUDIT;
        iptables -Z TOPOAUDIT" >/dev/null
done
sleep "$SOAK"
for n in 1 2 3 4; do
    table=$($COMPOSE exec "validator$n" iptables -L TOPOAUDIT -v -n -x | tail -7)
    other=$(echo "$table" | tail -1 | awk '{print $1}')
    own_public=$(echo "$table" | awk -v ip="10.20.0.1$n" '$NF == ip {print $1}')
    verdict $([ "$other" = "0" ] && echo 0 || echo 1) \
        "validator$n sent 0 packets anywhere but relays/DNS (OTHER=$other)"
    verdict $([ "$own_public" = "0" ] && echo 0 || echo 1) \
        "validator$n never dialed its own relay's public leg (pkts=$own_public; DMZ leg carries the reservation)"
done

# ---- proof 3: inbound is replies-only ----------------------------------------------------------
say "proof 3: inbound = loopback + replies to validator-initiated flows (INPUT policy DROP)"
for n in 1 2 3 4; do
    $COMPOSE exec "validator$n" sh -c "iptables -L INPUT -v -n -x | sed -n '1p;3,4p'"
done

# ---- proof 4: the restriction is kernel-enforced, not just observed ----------------------------
say "proof 4: default-drop enforcement on both ends of the validator<->relay pipe"
for n in 1 2 3 4; do
    pol=$($COMPOSE exec "validator$n" sh -c \
        "iptables -L OUTPUT -n | head -1 | grep -c 'policy DROP'" || echo 0)
    verdict $([ "$pol" = "1" ] && echo 0 || echo 1) "validator$n OUTPUT policy is DROP"
    fwd=$($COMPOSE exec "relay$n" sh -c \
        "iptables -L FORWARD -n | head -1 | grep -c 'policy DROP'" || echo 0)
    verdict $([ "$fwd" = "1" ] && echo 0 || echo 1) "relay$n FORWARD policy is DROP (gateway filter active)"
done

# ---- optional causal counterfactual ------------------------------------------------------------
if [ "${1:-}" = "--causal" ]; then
    say "causal: stopping ALL relays - consensus must freeze completely, validators must survive"
    before=$(v1_height)
    $COMPOSE stop relay1 relay2 relay3 relay4 >/dev/null 2>&1
    sleep 30
    h1=$(v1_height)
    sleep 20
    h2=$(v1_height)
    verdict $([ "$h1" -eq "$h2" ] && echo 0 || echo 1) \
        "consensus frozen with relays down (pre=$before, t+30s=$h1, t+50s=$h2)"
    up=$($COMPOSE ps validator1 validator2 validator3 validator4 --format '{{.Status}}' | grep -c '^Up' || true)
    verdict $([ "$up" -eq 4 ] && echo 0 || echo 1) "all 4 validators survived the blackout (up=$up)"
    say "causal: restarting relays - reservations (<=15s) + committee re-dial (<=30s heartbeat)"
    $COMPOSE start relay1 relay2 relay3 relay4 >/dev/null 2>&1
    recovered=1
    for _ in $(seq 1 24); do
        sleep 5
        now=$(v1_height)
        [ "$now" -gt $((h2 + 5)) ] && { recovered=0; break; }
    done
    verdict $recovered "consensus resumed after relays returned (height=$now)"
fi

say "result"
[ "$FAIL" -eq 0 ] && echo "TOPOLOGY VERIFIED: all traffic relay-mediated, no direct validator links" ||
    echo "TOPOLOGY VIOLATIONS FOUND - see FAIL lines above"
exit "$FAIL"
