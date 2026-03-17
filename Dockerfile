FROM ubuntu:24.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    git \
    pkg-config \
    libssl-dev \
    ca-certificates \
    python3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app

RUN rm -rf build \
    && cmake -S . -B build -DHAMSAZ_WITH_NURAFT=ON -DHAMSAZ_BUILD_TESTS=OFF \
    && cmake --build build -j"$(nproc)"

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    python3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/build/raft_node_server /app/raft_node_server
COPY --from=builder /app/build/raft_node_cli /app/raft_node_cli
COPY --from=builder /app/bench /app/bench
COPY --from=builder /app/src /app/src
COPY scripts/start_node_server.sh /app/scripts/start_node_server.sh

RUN chmod +x /app/raft_node_server /app/raft_node_cli /app/scripts/start_node_server.sh

ENTRYPOINT ["/app/raft_node_server"]
