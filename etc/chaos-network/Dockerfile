FROM rust:1.91-slim-bookworm AS builder

WORKDIR /usr/src/rayls-network

# install build dependencies
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

# Must never expand to the empty string -- see etc/docker-network/Dockerfile.
ARG VERGEN_GIT_SHA
ENV VERGEN_GIT_SHA=${VERGEN_GIT_SHA:-0000000000000000000000000000000000000000}

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=./target \
    cargo build -p rayls-network --bin rayls-network --release \
    && mv ./target/release/rayls-network /tmp/

# production image with network tools for chaos testing
FROM debian:bookworm-slim

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    libapr1 \
    libssl3 \
    iproute2 \
    iptables \
    procps \
    curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Build-time only (used by useradd below); ARG not ENV so it doesn't leak into
# the runtime environment. Consistent with USER_NAME.
ARG USER_ID=1101
ARG USER_NAME=nonroot

RUN useradd -u $USER_ID -U -ms /bin/bash $USER_NAME

# copy binary
COPY --from=builder /tmp/rayls-network /usr/local/bin/rayls

USER $USER_NAME

CMD ["rayls", "node", "--storage.v2"]
