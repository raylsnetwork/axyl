#!/bin/bash
# Genesis/committee generation for the blue-green failover harness. Same as the base
# etc/relay-network/genesis.sh MINUS its built-in /dnsaddr TXT block: here the dns-failover service
# generates validator1's TXT at runtime (from node-info) and flips it on DoS, and validator2..4 use
# concrete /ip4 committee addresses (no TXT), so a static all-validator TXT step does not apply.
# Idempotent, so a plain `up` on an existing stack does not regenerate genesis under running nodes.
set -e

if [ -f /home/nonroot/data/genesis/committee.yaml ]; then
    echo "genesis already generated -- skipping"
else
    GASLESS_FLAGS=""
    if [ "$GASLESS" = "true" ]; then
        GASLESS_FLAGS="--base-fee 0 --min-base-fee 0"
        echo "Gasless mode enabled: base fee and min base fee set to 0"
    fi

    GAS_LIMIT_FLAGS=""
    if [ -n "$GAS_LIMIT" ]; then
        GAS_LIMIT_FLAGS="--gas-limit $GAS_LIMIT"
        echo "Custom gas limit: $GAS_LIMIT"
    fi

    mkdir -p /home/nonroot/data/genesis/validators
    for i in 1 2 3 4; do
        cp /home/nonroot/data/validator-$i/node-info.yaml \
            /home/nonroot/data/genesis/validators/validator-$i.yaml
    done

    # short epochs so committee re-dials (the relay-failover recovery path) happen quickly
    /usr/local/bin/rayls genesis \
        --datadir /home/nonroot/data/ \
        --chain-id 0x1e7 \
        --epoch-duration-in-secs 60 \
        --dev-funded-account 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 \
        --max-header-delay-ms 1000 \
        --min-header-delay-ms 500 \
        --consensus-registry-owner 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 \
        --network-admin 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 \
        --fee-aggregator-admin 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 \
        ${GASLESS_FLAGS} \
        ${GAS_LIMIT_FLAGS}

    for i in 1 2 3 4; do
        mkdir -p /home/nonroot/data/validator-$i/genesis/
        cp /home/nonroot/data/genesis/genesis.yaml /home/nonroot/data/genesis/committee.yaml \
            /home/nonroot/data/validator-$i/genesis/
        cp /home/nonroot/data/parameters.yaml /home/nonroot/data/validator-$i/
    done
    cp /home/nonroot/data/parameters.yaml /home/nonroot/data/genesis/
fi

# Observers: dial-out-only followers; their keys just give them a network identity.
for o in observer1 observer2; do
    if [ ! -d /home/nonroot/data/$o/node-keys ]; then
        /usr/local/bin/rayls keytool generate observer \
            --datadir /home/nonroot/data/$o \
            --address 0x0000000000000000000000000000000000000000
        mkdir -p /home/nonroot/data/$o/genesis
        cp /home/nonroot/data/genesis/genesis.yaml /home/nonroot/data/genesis/committee.yaml \
            /home/nonroot/data/$o/genesis/
        cp /home/nonroot/data/genesis/parameters.yaml /home/nonroot/data/$o/
    fi
done

chown -R 1101:1101 /home/nonroot/data
echo "done"
