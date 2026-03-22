# hamsaz-raft

Hybrid consistency prototype in C++:
- **Conflicting operations** -> ordered with **NuRaft**
- **Dependent/independent operations** -> local apply + causal gossip path

This repository is set up for research-style benchmarking (latency, throughput, convergence, and Raft pressure).

## Get started (from GitHub)

```bash
git clone https://github.com/pysammy/hamsaz-raft.git
cd hamsaz-raft
```

## What this project implements

- Operation routing by class:
  - `conflicting` (e.g., `delete`) -> Raft path
  - `dependent` / `independent` -> local + gossip path
- Conflict safety gate for conflicting operations:
  - queue until prerequisites are satisfied
  - append when safe, drop on TTL expiry
- Causal gossip mechanics:
  - vector clocks
  - deferred delivery until causal prerequisites exist
  - anti-entropy resend
- Benchmarks:
  - local multi-node harness
  - external multi-client benchmark
  - Docker and Kubernetes runners

## Repository layout

- `src/` - core implementation
  - `src/raft/` - NuRaft integration and routing logic
  - `src/runtime/` - routing, dependency tracking, gossip
  - `src/domain/` - courseware object model
  - `src/main/` - CLI and server entry points
- `tests/` - unit and integration tests
- `bench/` - benchmark drivers and plotting utilities
- `scripts/` - one-command run scripts (local, Docker, Kubernetes)
- `deploy/k8s/` - Kubernetes manifests
- `proto/` - operation protocol definitions

## Prerequisites

### macOS (Homebrew)

```bash
brew install cmake openssl pkg-config python
```

### Ubuntu/Debian

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake pkg-config libssl-dev python3 python3-pip
```

Optional plotting:

```bash
python3 -m pip install matplotlib
```

## Build

### 1) Core + tests

```bash
cmake -S . -B build -DHAMSAZ_BUILD_TESTS=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
```

### 2) Build with NuRaft (Raft binaries)

```bash
cmake -S . -B build -DHAMSAZ_WITH_NURAFT=ON -DHAMSAZ_BUILD_TESTS=ON
cmake --build build -j
```

This produces, among others:
- `build/raft_node_cli`
- `build/raft_node_server`

## Quick run options

### A) Local split-vs-all benchmark (single machine)

```bash
scripts/run_bench_compare.sh
```

Outputs:
- `artifacts/bench/<timestamp>/...`
- `artifacts/bench_compare/latency_cdf_compare.png`

### B) 3-node UDP demo

```bash
scripts/run_three_node_udp.sh
```

Logs:
- `artifacts/demo/node1.log`
- `artifacts/demo/node2.log`
- `artifacts/demo/node3.log`

### B1) Reproducible 3-node external benchmark (correctness-gated)

```bash
cmake -S . -B build -DHAMSAZ_WITH_NURAFT=ON -DHAMSAZ_BUILD_TESTS=ON
cmake --build build -j
scripts/repro_3node_external.sh
```

On Windows, run this in WSL2 from the Linux filesystem path (for example `~/hamsaz-raft`), not `/mnt/c` or `/mnt/e`.

Generated output:
- `analysis-results-<YYYY-MM-DD>/three-node-external/README.md`
- `analysis-results-<YYYY-MM-DD>/three-node-external/metrics_summary.csv`
- `analysis-results-<YYYY-MM-DD>/three-node-external/split/*`
- `analysis-results-<YYYY-MM-DD>/three-node-external/all_to_raft/*`
- `analysis-results-<YYYY-MM-DD>/three-node-external/latency_cdf_compare.png`
- `analysis-results-<YYYY-MM-DD>/three-node-external/throughput_bar_compare.png`
- `analysis-results-<YYYY-MM-DD>/three-node-external/raft_appends_bar_compare.png`

This command always runs with:
- `--verify-correctness`
- `--fail-on-correctness`
- `--fail-on-op-errors`

Detailed runbook:
- `repro/3node-external/README.md`

### C) External multi-client benchmark (Docker)

```bash
NODES=10 OPS=1500 CONCURRENCY=32 scripts/run_bench_docker.sh
```

### D) External multi-client benchmark (Kubernetes/kind)

Create a local cluster once:

```bash
kind create cluster --name hamsaz --config=- <<'EOF'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
  - role: worker
  - role: worker
EOF
```

Run benchmark:

```bash
LOAD_IMAGE=1 NODES=3 OPS=300 CONCURRENCY=8 scripts/run_bench_k8s.sh
```

## API protocol (for `raft_node_server`)

Line-based TCP protocol (tab-separated):

- `PING`
- `MEMBERS`
- `INVARIANT`
- `STATE_HASH`
- `EXEC\t<op_id>\t<cmd>\t<arg1>\t<arg2>`
- `SHUTDOWN`

Supported commands for `EXEC`:
- `register`
- `add`
- `enroll`
- `unenroll`
- `delete`
- `query`

## Metrics used

The benchmark outputs both forms:

- **Average latency**
  - per operation RTT measured from request send to response receive
  - `avg_latency = (t1 + t2 + ... + tn) / n`
- **RTT-based throughput**
  - `throughput_rtt = n / (t1 + t2 + ... + tn)`
- **Wall-clock throughput**
  - `throughput_wall = n / (last_finish_time - first_send_time)`

CSV + JSON outputs are written under `artifacts/`.

## Troubleshooting

- `OpenSSL not found` during CMake:
  - install `openssl` and `pkg-config` (see prerequisites)
- Port already in use:
  - pick different base ports in scripts/bench args
- K8s port-forward readiness failures:
  - check pod status and logs:
    - `kubectl get pods -l app=raft-node -o wide`
    - `kubectl logs raft-node-0`
  - rerun with restart:
    - `RESTART_BEFORE_BENCH=1 scripts/run_bench_k8s.sh`

## Notes

- This is an active research prototype; benchmarking and deployment scripts are evolving.
- For reproducible comparisons, keep workload mix (`OPS`, `CONFLICT_RATIO`, `DEPENDENT_RATIO`) and `CONCURRENCY` fixed between runs.
