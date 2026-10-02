<!--
Thanks for contributing to Axyl. A few notes before you file:

- Keep the PR focused. Smaller PRs review faster and revert cleanly.
- If the change is security-sensitive, see SECURITY.md instead.
- The CI runs `cargo fmt --check`, `cargo clippy`, and the workspace tests on every push.
-->

## The problem

<!-- What is broken, missing, or painful today, and who hits it? A short paragraph in plain words, before any mention of the fix. Link the issue or incident if there is one. -->



## Summary

<!-- 1–3 bullets describing what this PR changes to solve the problem above. Link to the issue it closes or references. -->

-

Closes #

## Surface areas touched

<!-- Tick everything that materially changes in this PR. Anything ticked here is a signal for release notes and reviewer routing. Leave unticked if untouched. -->

- [ ] Consensus protocol (primary / worker / network / state-sync)
- [ ] Execution / EVM
- [ ] JSON-RPC (`eth_*`, `rayls_*`, faucet)
- [ ] Middleware (orchestrator / processor / bridge)
- [ ] Infrastructure (types / storage / config / network-cli)
- [ ] On-chain contracts (`rayls-contracts/`)
- [ ] Operations (`etc/`, scripts, Docker, compose)
- [ ] CI / build (`.github/workflows/`, `Makefile`)
- [ ] Documentation only (`doc/`, in-crate READMEs, root docs)
- [ ] Tests only

## Breaking / compatibility

<!-- Does this require a node restart, a network upgrade, a hardfork activation block, a contract migration, a config change, or any client coordination? If yes, describe. If no, write "None". -->

None.

## Test plan

<!-- What did you do to convince yourself this works? Commands, scenarios, or links to CI output. -->

- [ ]
- [ ]
