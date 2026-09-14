#!/bin/bash
# Genesis ceremony for chaos testing.
# Uses a shorter epoch duration (15s) to exercise epoch transitions quickly.
set -e

mkdir -p /home/nonroot/data/genesis/validators
cp -r /home/nonroot/data/validator-1/node-info.yaml /home/nonroot/data/genesis/validators/validator-1.yaml
cp -r /home/nonroot/data/validator-2/node-info.yaml /home/nonroot/data/genesis/validators/validator-2.yaml
cp -r /home/nonroot/data/validator-3/node-info.yaml /home/nonroot/data/genesis/validators/validator-3.yaml
cp -r /home/nonroot/data/validator-4/node-info.yaml /home/nonroot/data/genesis/validators/validator-4.yaml

/usr/local/bin/rayls genesis \
    --datadir /home/nonroot/data/ \
    --chain-id 487 \
    --epoch-duration-in-secs 15 \
    --dev-funded-account 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 \
    --max-header-delay-ms 1000 \
    --min-header-delay-ms 500 \
    --consensus-registry-owner 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 \
    --network-admin 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 \
    --fee-aggregator-admin 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266

for i in {1..4}; do
    mkdir -p /home/nonroot/data/validator-$i/genesis/
    cp /home/nonroot/data/genesis/genesis.yaml \
       /home/nonroot/data/genesis/committee.yaml \
       /home/nonroot/data/validator-$i/genesis/
    cp /home/nonroot/data/parameters.yaml /home/nonroot/data/validator-$i/
done
chown -R 1101:1101 /home/nonroot/data

echo "Genesis complete (epoch=15s)"
