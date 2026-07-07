# Security Audits

Independent security audits of the Rayls Network protocol, performed by
[Halborn](https://www.halborn.com/) between February and March 2026, with
remediation reviews completed in April–May 2026.

| Report | Scope | Engagement | Findings | Outcome |
| --- | --- | --- | --- | --- |
| [Consensus Protocol](./halborn-2026-03-consensus-protocol.pdf) | Consensus-related Rust code paths, certificate synchronization, gossip handling, validator key management | Mar 2 – Mar 20, 2026 | 5 (1 critical, 3 medium, 1 low) | All findings solved |
| [Network Node](./halborn-2026-03-network-node.pdf) | Networking, worker, state-sync, execution, orchestrator, storage modules | Feb 18 – Mar 24, 2026 | 22 (1 critical, 2 high, 7 medium, 6 low, 6 informational) | 20 solved, 2 low risk-accepted |
| [Smart Contracts](./halborn-2026-03-smart-contracts.pdf) | `rayls-contracts/src/` — ConsensusRegistry, StakeManager, DelegationPool, fee distribution, and related contracts | Mar 2 – Mar 13, 2026 | 15 (1 critical, 2 high, 7 medium, 2 low, 3 informational) | 14 solved, 1 medium risk-accepted |

## A note on commit and pull-request references

These audits were performed against the pre-publication (private) repository.
When the project was open-sourced, the repository history was squashed into a
single initial commit. As a result, the commit hashes and pull-request links
cited in the reports as remediation evidence point to the private history and
do not resolve in this repository.

**All remediations verified by Halborn are included in the initial public
commit** of this repository, which postdates the remediation reviews.

The PDFs are published exactly as issued by Halborn and have not been modified.
