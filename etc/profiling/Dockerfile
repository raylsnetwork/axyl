# Profiling build of rayls-network for the eBPF + tokio-metrics PoC (see doc/profiling-poc.md).
#
# Differs from etc/docker-network/Dockerfile only in how the binary is compiled:
# - `--profile profiling` (release + `debug = true`): symbols and DWARF stay in the binary, so the
#   eBPF profiler can name functions (including inlined ones) instead of raw addresses;
# - `--cfg tokio_unstable`: unlocks tokio's poll-time histogram, mean poll time, queue depths and
#   steal counts for `--tokio-metrics`.
FROM rust:1.91-slim-bookworm AS builder

WORKDIR /usr/src/rayls-network

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
COPY rayls-contracts/ ./rayls-contracts/

ARG VERGEN_GIT_SHA
ENV VERGEN_GIT_SHA=${VERGEN_GIT_SHA:-0000000000000000000000000000000000000000}

ARG BUILD_FEATURES=""
ENV RUSTFLAGS="--cfg tokio_unstable"

# Separate target cache: different RUSTFLAGS would otherwise invalidate the production build's.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,id=rayls-target-profiling,target=./target \
    cargo build -p rayls-network --bin rayls-network --profile profiling ${BUILD_FEATURES:+--features ${BUILD_FEATURES}} \
    && mv ./target/profiling/rayls-network /tmp/

FROM debian:bookworm-slim

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get upgrade -y && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    libapr1 \
    libssl3 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV USER_ID=1101
ARG USER_NAME=nonroot
RUN useradd -u $USER_ID -U -ms /bin/bash $USER_NAME
USER $USER_NAME

# not stripped: the profiler symbolizes from this file
COPY --from=builder /tmp/rayls-network /usr/local/bin/rayls

CMD ["rayls", "node", "--storage.v2"]
