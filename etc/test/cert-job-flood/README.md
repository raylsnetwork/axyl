# Live reproduction: unbounded missing-certificate jobs

This directory reproduces, on a running network, that a few observers can make a validator fall
behind consensus by sending it ordinary, valid missing-certificates requests. Observers have no
stake and no committee seat.

See the repository issue for the full analysis. In short: every missing-certificates request a
node receives starts a storage job on a background thread, with no limit per peer or in total, and
cancelling the request does not stop the job.

## What the script does

`run.sh`:

1. Builds `rayls-network` with the `cert-job-flood-hook` cargo feature. This feature compiles a
   test-only load generator that is never part of a production build.
2. Starts a local network of validators plus one observer and warms the chain up.
3. Picks the last validator as the victim and the one before it as a control, and starts several
   observers with the load armed and pointed at the victim. Each observer sends valid
   missing-certificates requests to the victim, on many concurrent streams, and gives up on each
   request quickly, as a node that times out would. The victim keeps running the storage jobs
   regardless. The validators themselves stay honest.
4. Samples every validator's peak memory, and at the end compares the victim against the control
   and checks whether the victim kept up with the chain.

The requests name every committee member with no rounds to skip, which asks the victim to return
every certificate it has. That is a normal, within-limits request; only the volume and the quick
give-up are unusual. A single peer is not enough, because the network layer caps how many requests
one connection can have in flight. Several peers together drive the victim's job count past that.

## Running it

```
etc/test/cert-job-flood/run.sh
```

Options: `-n` validators (default 5), `-w` seconds to run the load (default 180),
`-b` a prebuilt binary, `-d` a work directory, `-i` the first instance number (RPC ports).
The number of armed observers is set by `N_OBS` near the top of the script (default 8).

Build the binary yourself with:

```
cargo build -p rayls-network --features cert-job-flood-hook
```

## Reading the result

The script prints each validator's peak memory and the chain height over time, and exits:

- `1` the victim fell behind the chain while the control kept up, or used far more memory, or the
  chain stopped: the jobs piled up, so the issue is present.
- `0` the victim stayed level with the control and kept up: the issue is absent or fixed.
- `2` the run could not start or was inconclusive.

Logs, datadirs and a `summary.txt` are left under the work directory.

## Arming the hook by hand

The load is off unless `RL_CJFLOOD_TARGETS` is set. On a node built with the feature:

- `RL_CJFLOOD_TARGETS=<n>` send to the first n committee peers (arms the load)
- `RL_CJFLOOD_TARGET_BLS=<a,b>` send only to peers with these BLS keys, overriding the count
- `RL_CJFLOOD_STREAMS=<n>` concurrent requests per peer (default 32)
- `RL_CJFLOOD_GIVEUP_MS=<n>` how long before the requester drops each request (default 1000)
- `RL_CJFLOOD_DELAY_SECS=<n>` delay before the first request (default 0)
