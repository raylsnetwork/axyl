# Testnet minters

**Rayls testnet (7295799) only.** [`TestnetMinterOps.s.sol`](TestnetMinterOps.s.sol) gives an address `MINTER_ROLE` on
the RLS token and on the NativeTokenController (USDr), so a testnet faucet can mint test tokens on request instead of
handing out a balance that runs out. Every action refuses any other chain id, mainnet (72957) included.

## The first minter: the Enygma relayer

The Enygma testnet wallet (https://enygma-testnet.rayls.com) runs one relayer, `0xa5f0C684EB49866a9f62f901Cd85f98a19DA0Ac8`.
It pays the wallet's gas in USDr and runs its faucet (5 USDr and 50 RLS per address every 3 hours). Today it hands out
its own balance and has to be topped up by hand. With both roles it mints the faucet's tokens, and its own gas, instead.

Checked on the testnet on 2026-11-18:

| | RLS `0x07e1…17ea` | USDr, via NativeTokenController `0x07e1…17e6` |
|---|---|---|
| `DEFAULT_ADMIN_ROLE` | `0x91ec7a2be07a79d2eab99135553b26f706099e9d` | `0x8d93de1ec49f212c77e969a5723e18cac22083ef` |
| Minters today | `ElevatedMinterBurner` `0x545b…be3d` (LayerZero) | 10 addresses |
| Enygma relayer a minter | No | No |
| Cap | `MAX_SUPPLY` 10B; supply ~1.001B | None |

`deployments.json`'s `admin` is not the testnet admin of either contract, so each grant is signed with that contract's
admin key, not `ADMIN_PK`.

## Functions

| `--sig` | Purpose | Signer | Env |
|---|---|---|---|
| `status()` | Whether `MINTER` can mint each token; RLS headroom | none | `MINTER` |
| `grantRls()` | `RLS.grantRole(MINTER_ROLE, MINTER)` | RLS `DEFAULT_ADMIN_ROLE` | `MINTER` |
| `grantUsdr()` | `NativeTokenController.addMinter(MINTER)` | NativeTokenController `DEFAULT_ADMIN_ROLE` | `MINTER` |
| `revokeRls()`, `revokeUsdr()` | Take the role back | the same | `MINTER` |

The grants do nothing when the role is already held, and check it took.

## Run it

```bash
export RPC_URL=https://testnet-rpc.rayls.com MINTER=0xa5f0C684EB49866a9f62f901Cd85f98a19DA0Ac8
S=script/interactions/testnet-minters/TestnetMinterOps.s.sol:TestnetMinterOps

forge script $S --sig "status()" --rpc-url $RPC_URL
forge script $S --sig "grantRls()"  --rpc-url $RPC_URL --broadcast --private-key $RLS_ADMIN_PK
forge script $S --sig "grantUsdr()" --rpc-url $RPC_URL --broadcast --private-key $NTC_ADMIN_PK
forge script $S --sig "status()" --rpc-url $RPC_URL
```

## Before granting

- **Testnet RLS bridges.** RLS's existing minter is a LayerZero `ElevatedMinterBurner`. Minted testnet RLS must only
  reach other testnets, never a chain where RLS has value.
- **A hot key.** The relayer's key signs on a server. If it leaked, it could mint testnet tokens without limit until the
  roles are revoked (`revokeRls()`, `revokeUsdr()`). The faucet still limits each address to one request every 3 hours.
