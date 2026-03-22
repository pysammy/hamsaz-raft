FROM ubuntu:24.04 AS builder

ARG CMAKE_BUILD_TYPE=Release
ARG BUILD_JOBS=1
ARG CXX_FLAGS_RELEASE="-O0 -DNDEBUG"
ARG HAMSAZ_NURAFT_LOW_MEM_BUILD=ON

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
    && cmake -S . -B build \
      -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} \
      -DCMAKE_CXX_FLAGS_RELEASE="${CXX_FLAGS_RELEASE}" \
      -DHAMSAZ_WITH_NURAFT=ON \
      -DHAMSAZ_NURAFT_LOW_MEM_BUILD=${HAMSAZ_NURAFT_LOW_MEM_BUILD} \
      -DHAMSAZ_BUILD_TESTS=OFF \
    && cmake --build build -j"${BUILD_JOBS}"

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
