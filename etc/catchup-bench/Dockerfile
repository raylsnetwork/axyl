# Runtime for run.sh: the nodes run from host-built binaries mounted into the container, so the
# image only needs netem (iproute2) and the tools bench.sh calls.
FROM ubuntu:24.04
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      iproute2 curl ca-certificates procps util-linux iputils-ping && rm -rf /var/lib/apt/lists/*
