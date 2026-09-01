# RewardCurve Playground

A local dApp for interactively "playing with" `RewardCurve` (`../src/fees/RewardCurve.sol`) — the
POC contract for [issue #103](https://github.com/raylsnetwork/axyl/issues/103) ("Replace fixed APY
with a revenue-based reward curve for staking"). Deploys the real, unmodified contract to a local
`anvil` chain, and lets you move inputs / click buttons to fire real transactions and watch the
on-chain-derived APY, emission breakdown, yield estimate, and curve chart update live.

This is intended as a **PR reviewer aid**: check out this branch, follow the steps below, and you
can exercise the exact contract under review without writing any test code yourself.

Two curves are deployed, mirroring the Track A ("priority"/locked tier) vs Track B ("open tier")
split already proven in `../test/fees/RewardDistributorExtendedTest.t.sol`'s integration test:

- **Track A · Priority** — base-heavy policy (500k base + 100k revenue / month)
- **Track B · Open Tier** — revenue-leaning policy (100k base + 500k revenue / month)

## ⚠️ Local-only key

This app signs transactions with **Foundry's well-known anvil default account #0 private key**
(`0xac0974bec3...`) — the same key every `anvil`/`forge test` run in the world uses, printed in
Foundry's own docs. It has zero value outside a local anvil chain. **Never** put a real private
key in `.env.local` or reuse this setup against any real network.

## Running it

Requires: Node.js on this machine, and [Foundry](https://getfoundry.sh) (`anvil`) — this was built
against Foundry installed **in WSL**, with the frontend and scripts run from Windows (WSL2's
default networking forwards `localhost:8546` to Windows automatically).

**1. Start a local chain** (leave running in its own terminal):

If you're already inside a WSL shell (ran `wsl` first, or your terminal *is* WSL):

```bash
anvil --host 0.0.0.0 --port 8546
```

If you're running this from a **Windows** terminal (PowerShell, Git Bash / MINGW64, cmd) — plain
`anvil ...` won't be found there, since it's a Linux binary that only exists inside WSL. Launch it
into WSL explicitly instead:

```bash
wsl -e bash -lc "anvil --host 0.0.0.0 --port 8546"
```

(Plain `wsl anvil ...` without `-e bash -lc "..."` can also fail to find it — that form skips the
login-shell startup files that put `anvil` on `PATH`.)

Port `8546` (not anvil's usual `8545`) is the default here because `8545` may already be in use
by something else on your machine — check first (`curl -s -X POST -H 'Content-Type: application/json'
--data '{"jsonrpc":"2.0","method":"web3_clientVersion","params":[],"id":1}' http://127.0.0.1:8545`).
If `8545` is free for you, use it and set `RPC_URL=http://127.0.0.1:8545` for `npm run deploy` (the
frontend picks up whatever `VITE_RPC_URL` `deploy.mjs` writes to `.env.local`, so only the deploy
step's port needs to match what you passed to `anvil`).

**2. Build the contracts, then install/sync/deploy this playground** (from
`rayls-contracts/playground/`, in a Windows/PowerShell terminal or wherever Node runs):

```bash
# from rayls-contracts/ (one level up) — only needed if you haven't already:
forge build

# from rayls-contracts/playground/:
npm install
npm run sync-artifacts   # copies RewardCurve/ERC1967Proxy ABIs+bytecode from ../out
npm run deploy           # deploys both curve proxies to the running anvil, writes .env.local
```

`sync-artifacts` reads compiled artifacts from `../out` (i.e. `rayls-contracts/out/`, this
playground's parent's build output) by default — override with `AXYL_CONTRACTS_PATH` only if
you've copied this `playground/` folder somewhere else. Re-run `sync-artifacts` + `deploy` any
time `RewardCurve.sol` changes (including after `git pull`ing new commits on this branch) — the
copied artifacts go stale otherwise.

**3. Run the app:**

```bash
npm run dev
```

Open the printed `localhost` URL. If you restart `anvil` (which resets all chain state), re-run
`npm run deploy` and restart `npm run dev` so it picks up the freshly deployed addresses.

## What's real vs. simulated

- **Real:** the deployed contract, every read (`getCurrentApyBps`, `getApyBreakdown`,
  `estimateYield`, `previewCurve`, `getEmissionBreakdown`) and every write
  (`setBaseMonthlyEmission`, `recordRevenue`, `resetMonthlyRevenue`, `setPhase`) goes through an
  actual transaction against actual contract bytecode.
- **Simulated:** "total RLS staked" is a plain number you type in — `RewardCurve` takes it as a
  parameter by design (there's no on-chain "total staked" anywhere in the current codebase to read
  from), so this playground can't and doesn't pretend to track a real staking pool.
