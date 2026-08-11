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

ARG VERGEN_GIT_SHA
ENV VERGEN_GIT_SHA=${VERGEN_GIT_SHA:-0000000000000000000000000000000000000000}

# Optional cargo features compiled into the binary, e.g. `dev-single-node-setup` for single-validator local
# `node --dev` chains. Empty (default) -> a production build with no extra features.
ARG BUILD_FEATURES=""

# move binary out of cache for subsequent build steps
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=./target \
    cargo build -p rayls-network --bin rayls-network --release ${BUILD_FEATURES:+--features ${BUILD_FEATURES}} \
    && mv ./target/release/rayls-network /tmp/

# production image
FROM debian:bookworm-slim

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get upgrade -y && \
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
COPY --from=builder /tmp/rayls-network /usr/local/bin/rayls

# default
CMD ["rayls", "node", "--storage.v2"]
