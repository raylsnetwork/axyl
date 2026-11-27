# Chain configs

The genesis and the node parameters of the real networks. A node has both files in its datadir
(`genesis/genesis.yaml` and `parameters.yaml`). A snapshot does not contain them, and
`rayls-replay` cannot run without them.

| Network | Chain id | Genesis block hash |
|---|---|---|
| mainnet | 72957 | `0x500426d91e3e8fa5cf39722f8c4ef5765865129b5efbcc25544b39e2acdc3b91` |
| testnet | 7295799 | `0x33b818f13cab5c2e3e7bb589cbd0612e78e1139f3add29f0d5f0d6a03a5c9545` |

## Use with a snapshot

```sh
rayls-replay \
  --snapshot-datadir /path/to/snapshot \
  --archive-out      /path/to/archive \
  --network          mainnet \
  --genesis          chain-configs/mainnet/genesis.yaml \
  --parameters       chain-configs/mainnet/parameters.yaml
```

The tool compares the genesis hash with block 0 of the snapshot before it replays anything.

## Where the files come from

- Both genesis files were checked by a full replay (#420): the hash above is block 0 of a
  snapshot of the network, and every later block gave the same hash too.
- `mainnet/parameters.yaml` is from a mainnet validator. Its line `network: testnet` is in the
  original file and is kept as it is.
- `testnet/parameters.yaml` is from May 2026 and has no `min_base_fee`. The replay does not need
  it. For a node, take the file of a running testnet node.

The files hold no keys. They contain chain settings, the accounts and contract code of block 0,
and two fee addresses.
