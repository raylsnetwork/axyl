# Local testnet relay identities

> See `RELAY_DESIGN.md` for the topology and its security tradeoffs (chokepoint/SPOF, eclipse,
> metadata/IP exposure) before relying on relay routing beyond local testing.

Fixed, deterministic relay identities used by `local-testnet.sh --relay`. Each validator routes its
p2p through the relay listening on the matching port. For a reservation to succeed, **your relay app
must run with the exact identity below** for each port — the peer id baked into the validators'
`node-info.yaml` (via `keytool generate --relay`) must match the relay's actual key.

> These are throwaway test keys (ed25519, seed = the byte `index+1` repeated 32×). Never use them
> outside a local test network.

| Validator  | Relay listen addr        | Peer ID                                                |
|------------|--------------------------|--------------------------------------------------------|
| validator-1| 127.0.0.1:50000 (quic-v1)| `12D3KooWK99VoVxNE7XzyBwXEzW7xhK7Gpv85r9F3V3fyKSUKPH5` |
| validator-2| 127.0.0.1:50001 (quic-v1)| `12D3KooWJWoaqZhDaoEFshF7Rh1bpY9ohihFhzcW6d69Lr2NASuq` |
| validator-3| 127.0.0.1:50002 (quic-v1)| `12D3KooWRndVhVZPCiQwHBBBdg769GyrPUW13zxwqQyf9r3ANaba` |
| validator-4| 127.0.0.1:50003 (quic-v1)| `12D3KooWPT98FXMfDQYavZm66EeVjTqP9Nnehn1gyaydqV8L8BQw` |

## Key material

Two encodings per relay — use whichever your relay app accepts:

- **seed_hex**: the raw 32-byte ed25519 secret seed.
- **proto_hex**: the libp2p protobuf-encoded private key (`Keypair::to_protobuf_encoding`), portable
  across rust-libp2p apps via `Keypair::from_protobuf_encoding`.

```
# validator-1  (127.0.0.1:50000)
seed_hex  = 0101010101010101010101010101010101010101010101010101010101010101
proto_hex = 0801124001010101010101010101010101010101010101010101010101010101010101018a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c

# validator-2  (127.0.0.1:50001)
seed_hex  = 0202020202020202020202020202020202020202020202020202020202020202
proto_hex = 0801124002020202020202020202020202020202020202020202020202020202020202028139770ea87d175f56a35466c34c7ecccb8d8a91b4ee37a25df60f5b8fc9b394

# validator-3  (127.0.0.1:50002)
seed_hex  = 0303030303030303030303030303030303030303030303030303030303030303
proto_hex = 080112400303030303030303030303030303030303030303030303030303030303030303ed4928c628d1c2c6eae90338905995612959273a5c63f93636c14614ac8737d1

# validator-4  (127.0.0.1:50003)
seed_hex  = 0404040404040404040404040404040404040404040404040404040404040404
proto_hex = 080112400404040404040404040404040404040404040404040404040404040404040404ca93ac1705187071d67b83c7ff0efe8108e8ec4530575d7726879333dbdabe7c
```

## Relay-side requirements (reminder)

- Listen on QUIC (`/ip4/127.0.0.1/udp/<port>/quic-v1`) so validators can dial over the same
  transport they use.
- Enable the **relay server** behaviour (`libp2p::relay::Behaviour`) and grant effectively
  unlimited reservations/data — default relay-v2 "limited relay" caps bytes/time per circuit, which
  will throttle a consensus network that hairpins all traffic through the relay.
