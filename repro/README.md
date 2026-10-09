# Live reproduction: two certificates for one round from the durability gap

These scripts drive a real N-validator network into two certificates for one author and round,
which halts the chain. They exist only on the `repro/vote-durability-live` branch and are never
committed to the product. The nodes are honest builds from `origin/main`; a small env-gated hook in
this worktree only keeps a targeted write in memory instead of on disk, which is exactly what a
crash with a queued write leaves behind.

## Build

    flock /tmp/axyl-cargo.lock nice -n 19 cargo build -j 4 -p rayls-network --bin rayls-network

The debug binary is used so the node can recover by re-executing after a crash kill.

## Hook (this worktree only)

- `RL_REPRO_STALL_VOTE_FILE`, `RL_REPRO_STALL_HEADER_FILE`, `RL_REPRO_STALL_CERT_FILE`: when the named
  file exists, the node keeps its next vote / its next header / its certificates in memory only.
- The node keeps running and still sends the vote, header and certificate on the wire. Only the disk
  write is skipped, so a kill loses the record.

## Run

    ./repro/two_cert.sh 4           # one run at N validators
    ./repro/sweep.sh 4 5 7 10       # several sizes, one after another

`two_cert.sh` keeps the "fresh" nodes down during the first header, arms the author (header + its own
certificate) and the forgetful voters (vote + certificate), lets the author assemble certificate 1,
kills the author, the forgetful voters and the certificate holder, restarts everyone except the
holder, lets the author rebuild a different header and assemble certificate 2, then brings the holder
back so it meets the conflicting certificate.

## Reading the result

The reliable signal is **`nodes_equivocated`** in `sweep-results.txt`, or `CertificateEquivocation`
in the node logs. The author rebuilds on a round that is certified but not yet committed, which is
often a few rounds after certificate 1, so the "two certificates for the exact same round" line in
`two_cert.sh` can read 0 even when the equivocation and halt did happen. Confirm the halt by checking
that the block tip stops advancing across two samples.

## Evidence

A captured run is in `/home/ricardo/parfin/axyl-evidence/vote-durability-two-cert-2026-10-09`
(logs, the two-certificate pair, the equivocation lines).
