#!/bin/sh
# Egress-gateway entrypoint for the blue-green failover harness.
#
# The validator's VPC carries no egress of its own (its default route is this box, installed by the
# validator entrypoint), so this box is its only egress: it MASQUERADEs the validator's traffic onto
# the public leg and forwards ONLY udp/4001 to relay
# public legs (circuit first hops) plus optional udp/53 to DNS. Everything else is dropped. This is
# the gateway role the base harness fused into the relay; here it is a SEPARATE box so a DoS on the
# active relay cannot take out egress or the warm standby reservations.
#
# Env:
#   SUBNET         - the VPC subnet to masquerade (e.g. 10.10.1.0/24)
#   VIP            - the validator IP whose forwarded flows are permitted
#   RELAY_PUBLICS  - space-separated relay public IPs the validator may reach on udp/4001
#   DNS_SERVER     - optional DNS server IP; when set, udp/53 to it is forwarded
#   RUN_RELAY      - when set, exec `rayls-relay` after installing rules (base-style gateway that is
#                    also its validator's relay); otherwise the box is a pure egress NAT (sleeps)
set -eu

iptables -t nat -A POSTROUTING -s "$SUBNET" ! -d "$SUBNET" -j MASQUERADE
iptables -A FORWARD -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
for r in $RELAY_PUBLICS; do
    iptables -A FORWARD -s "$VIP" -d "$r" -p udp --dport 4001 -j ACCEPT
done
if [ -n "${DNS_SERVER:-}" ]; then
    iptables -A FORWARD -s "$VIP" -d "$DNS_SERVER" -p udp --dport 53 -j ACCEPT
fi
iptables -P FORWARD DROP

if [ -n "${RUN_RELAY:-}" ]; then
    exec rayls-relay
fi
exec sleep infinity
