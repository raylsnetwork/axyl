#!/bin/sh
# Validator entrypoint for the blue-green failover harness.
#
# Installs the default route via a DEDICATED egress gateway (decoupled from the relays, so a DoS
# on the active relay cannot take out the validator's egress or its warm standby reservations),
# then the same default-drop firewall the base harness uses, then execs the node. The node reserves
# on every relay listed in PRIMARY/WORKER_RELAY_MULTIADDRS (the warm pool) and advertises whichever
# address keytool baked into its node-info (a /dnsaddr for the failover subject, concrete /ip4 for
# the others).
#
# Env:
#   GW             - gateway IP (the egress NAT's DMZ leg); default route + allowed OUTPUT dest
#   RELAY_PUBLICS  - space-separated relay public IPs this node may dial on udp/4001 (circuit hops)
#   DNS_SERVER     - optional DNS server IP; when set, udp/53 to it is allowed (needed to resolve a
#                    peer's /dnsaddr). Omitted for the failover subject, which dials only /ip4 peers.
set -eu

# `replace`, not `add`: the VPC is a normal (non-internal) bridge, so Docker Desktop already gives
# the container a default route via the bridge gateway. `ip route add default` would then fail with
# "File exists" and abort the script under `set -e`; `replace` installs ours whether or not one is
# present. (OrbStack leaves no pre-existing default, so `add` happened to work there.)
ip route replace default via "$GW"

# inbound: loopback + replies to our own outbound flows only (circuit-relay-v2 needs no inbound)
iptables -A INPUT -i lo -j ACCEPT
iptables -A INPUT -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
iptables -P INPUT DROP

# outbound: loopback, the egress gateway, udp/4001 to relay public legs, optional DNS, replies
iptables -A OUTPUT -o lo -j ACCEPT
iptables -A OUTPUT -d "$GW" -j ACCEPT
for r in $RELAY_PUBLICS; do
    iptables -A OUTPUT -d "$r" -p udp --dport 4001 -j ACCEPT
done
if [ -n "${DNS_SERVER:-}" ]; then
    iptables -A OUTPUT -d "$DNS_SERVER" -p udp --dport 53 -j ACCEPT
fi
iptables -A OUTPUT -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
iptables -P OUTPUT DROP

exec /usr/local/bin/rayls node --datadir /home/nonroot/data --metrics 127.0.0.1:9101 \
    --log.stdout.format terminal -vvvv --full --storage.v2 \
    --txpool.pending-max-count 1000000 --txpool.pending-max-size 20971120000 \
    --txpool.basefee-max-count 1000000 --txpool.basefee-max-size 20971120000 \
    --txpool.queued-max-count 1000000 --txpool.queued-max-size 20971120000 \
    --txpool.max-pending-txns 1000000 --txpool.max-new-txns 1000000 \
    --txpool.minimal-protocol-fee 0 --txpool.gas-limit 999999999999 \
    --txpool.max-tx-gas 999999999999 --txpool.max-tx-input-bytes 999999999999 \
    --txpool.max-account-slots 1000000 --gpo.default-suggested-fee 0 \
    --http --http.addr 0.0.0.0 --http.api all
