# Historical-replay image: rebuilds a full archive datadir from a pruned Rayls
# snapshot by re-executing every block, leaving a datadir ready to boot in
# Observer mode. Mirrors etc/docker-network/Dockerfile (same bases, deps, user).
#
# Build (context = repo root):
#   docker build -f etc/docker-replay/Dockerfile -t rayls-replay:latest .
#
# Run (mount the snapshot read-only and the output read-write; pass parameters as
# CLI flags appended to the entrypoint):
#   docker run --rm \
#     -v /data/snapshot:/snapshot:ro \
#     -v /data/archive:/archive \
#     rayls-replay:latest \
#       --snapshot-datadir /snapshot --archive-out /archive --chain mainnet
#
# The mounted volumes must be writable by uid 1101 (the nonroot user); the output
# datadir and its parent need write access. Run `rayls-replay --help` for the full
# flag set (--consensus-db, --from-block, --to-block, --unwind-to, --storage-v2, ...).

FROM rust:1.91-slim-bookworm AS builder

WORKDIR /usr/src/rayls-network

# install dependencies
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    libclang-16-dev \
    pkg-config \
    libssl-dev \
    libapr1-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY Cargo* ./
COPY bin/ ./bin/
COPY crates/ ./crates/
COPY chain-configs/ ./chain-configs/
COPY rayls-contracts/ ./rayls-contracts/

# Must never expand to the empty string -- see etc/docker-network/Dockerfile.
ARG VERGEN_GIT_SHA
ENV VERGEN_GIT_SHA=${VERGEN_GIT_SHA:-0000000000000000000000000000000000000000}

# build the replay binary; the archive-replay feature is selected by
# bin/rayls-replay/Cargo.toml, so no extra --features flag is required.
# `-p` scopes feature resolution to this package -- see etc/docker-network/Dockerfile.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=./target \
    cargo build -p rayls-replay --bin rayls-replay --release \
    && mv ./target/release/rayls-replay /tmp/

# production image
FROM debian:bookworm-slim

# openssl
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    libapr1 \
    libssl3 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# specify user id for nonroot
ENV USER_ID=1101
ARG USER_NAME=nonroot

# Create a non-root user
RUN useradd -u $USER_ID -U -ms /bin/bash $USER_NAME

USER $USER_NAME

# copy binary
COPY --from=builder /tmp/rayls-replay /usr/local/bin/rayls-replay

# rayls-replay installs its own SIGTERM/SIGINT handler, so `docker stop` is caught
# directly (it finishes the current output group, flushes, and exits resumable) —
# no init wrapper needed even though the binary runs as PID 1.
# parameters are appended as CLI flags, e.g.
#   docker run <image> --snapshot-datadir /snapshot --archive-out /archive --chain mainnet
# a bare `docker run <image>` prints usage via the default CMD.
ENTRYPOINT ["rayls-replay"]
CMD ["--help"]
