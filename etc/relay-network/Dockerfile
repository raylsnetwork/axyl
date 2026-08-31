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
ENV VERGEN_GIT_SHA=${VERGEN_GIT_SHA}

# move binaries out of cache for subsequent build steps; unlike etc/docker-network this image also
# ships the circuit-relay-v2 server, since the relay containers run from the same image
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=./target \
    cargo build --bin rayls-network --bin rayls-relay --release \
    && mv ./target/release/rayls-network ./target/release/rayls-relay /tmp/

# production image
FROM debian:bookworm-slim

# Beyond the runtime libs: curl because validators sit on internal-only networks with no published
# ports (the only way to poke their RPC is `docker compose exec <validator> curl ...`), iproute2 so
# validators can point their default route at their relay's DMZ leg, and iptables for the
# validator firewalls and the relays' gateway FORWARD/MASQUERADE rules (relays run from this
# same image).
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get upgrade -y && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    libapr1 \
    libssl3 \
    curl \
    iproute2 \
    iptables \
    tcpdump \
    iputils-ping \
    netcat-traditional \
    dnsmasq && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# specify user id for nonroot
ENV USER_ID=1101
ARG USER_NAME=nonroot

# Create a non-root user
RUN useradd -u $USER_ID -U -ms /bin/bash $USER_NAME

USER $USER_NAME

# copy binaries
COPY --from=builder /tmp/rayls-network /usr/local/bin/rayls
COPY --from=builder /tmp/rayls-relay /usr/local/bin/rayls-relay

# default
CMD ["rayls", "node"]
