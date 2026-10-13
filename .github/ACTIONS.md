# GitHub Actions workflows

The CI for this repository runs on standard GitHub-hosted runners. The four workflows that
live in [`.github/workflows/`](workflows/) are described below.

## `pr.yaml` — Pull Request Checks

Triggered when the `run-ci` label is added to a pull request against `main` (and manually via
`workflow_dispatch`). Opening or pushing to a PR runs nothing; its required checks stay pending
until `run-ci` is added. `consume-run-ci-label.yml` removes the label again, so re-add it to
run CI on a newer commit.

- Skips immediately on draft PRs (the first step exits with an error if
  `github.event.pull_request.draft == true`).
- Pins the Rust toolchain to `1.91.0` via `dtolnay/rust-toolchain`.
- Runs the Rust workspace tests (`cargo test --workspace`, excluding `rayls-execution-faucet`
  and `e2e-tests`), `cargo fmt --check`, and `cargo clippy` against the workspace.

This workflow is what runs the "Tests" status check that gates merges.

## `build-docker.yml` — Build & Push Docker Image

Triggered by tag pushes that match `v*` (production releases like `v1.0.0`) or `dev-*` (dev
releases), and manually via `workflow_dispatch` with an `tag` input.

- Builds the Docker image defined by [`etc/docker-network/Dockerfile`](../etc/docker-network/Dockerfile).
- Pushes the image to the private AWS ECR registry under
  `IMAGE_NAME=rayls-stack-node-client`.
- Authenticates to ECR using the runner's `id-token` permission via OIDC; no long-lived AWS
  credentials are stored in repo secrets.

## `claude.yml` — Claude PR helper

Reacts to PR comments and lets reviewers offload investigation work (code review summaries,
documentation lookups, diff analysis) to an automated Claude assistant. The assistant runs in
a sandboxed environment scoped to the PR.

## `claude-code-review.yml` — Automated review pass

Runs an automated review pass against every PR using Claude. Findings are posted as a PR
review comment; merging is not gated on the review (the human-authored review and the `pr.yaml`
status checks are the gating signals).

## Local validation

Before pushing, contributors typically run the same checks `pr.yaml` will run:

```sh
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

There is **no** on-chain attestation step in CI; an earlier iteration of this document described
an attestation contract at `0xde9700e89e0999854e5bfd7357a803d8fc476bb0` and a
`test-and-attest.sh` script. The on-chain contract has been retired and the script is no longer
part of CI. The script (`etc/test/test-and-attest.sh`) and the `make attest` Makefile target
are kept in the tree for historical reference only and are not part of the active build or
release flow.
