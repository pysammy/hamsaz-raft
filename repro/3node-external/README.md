# 3-Node External Benchmark Reproduction

This is the canonical, copy-paste guide for reproducing the 3-node external benchmark with correctness checks enabled.

## Goal

Run two configurations on the same workload:

1. **split routing**: conflicting operations through Raft, dependent/independent through local+gossip
2. **all-to-raft**: all operations routed through Raft

and verify:

- convergence (`correctness.converged = true`)
- invariant safety (`correctness.all_invariants_ok = true`)
- no operation failures (`op_failures = 0`)

## Prerequisites

### macOS

```bash
brew install cmake openssl pkg-config python
```

### Ubuntu/Debian

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake pkg-config libssl-dev python3 python3-pip
```

## Build

From repo root:

```bash
cmake -S . -B build -DHAMSAZ_WITH_NURAFT=ON -DHAMSAZ_BUILD_TESTS=ON
cmake --build build -j
```

## Run (single command)

```bash
scripts/repro_3node_external.sh
```

On Windows + WSL2, run from the Linux filesystem (for example `~/hamsaz-raft`), not `/mnt/c` or `/mnt/e`, because mounted Windows paths can be much slower and can cause timeout-like behavior in convergence checks.

By default this writes outputs to:

```text
analysis-results-<today>/three-node-external/
```

To force a deterministic location:

```bash
RESULT_ROOT=analysis-results-2026-03-21/three-node-external scripts/repro_3node_external.sh
```

If your machine is slower, increase settle/drain wait without changing benchmark semantics:

```bash
SETTLE_TIMEOUT_SEC=240 DRAIN_TIMEOUT_SEC=20 scripts/repro_3node_external.sh
```

## What gets produced

- `split/summary.json`
- `split/latencies.csv`
- `split/latency_cdf.png`
- `split/latency_scatter.png`
- `all_to_raft/summary.json`
- `all_to_raft/latencies.csv`
- `all_to_raft/latency_cdf.png`
- `all_to_raft/latency_scatter.png`
- `metrics_summary.csv`
- `latency_cdf_compare.png`
- `throughput_bar_compare.png`
- `avg_latency_bar_compare.png`
- `raft_appends_bar_compare.png`
- `gate_counters_bar_compare.png`

## Correctness gates used

The script enforces:

- `--verify-correctness`
- `--fail-on-correctness`
- `--fail-on-op-errors`

If correctness fails, the script exits non-zero.

## How to read `metrics_summary.csv`

- `ops_requested`: target workload size (`--ops`)
- `ops_reported`: benchmarked operations (should match `ops_requested`)
- `attempts_total`: all internal attempts, including retries
- `retries_total`: `attempts_total - ops_reported`
- `raft_appends_total`: total Raft log pressure indicator
- `converged`, `invariants_ok`, `op_failures`: correctness and safety outcomes

For protocol comparisons, prioritize:

1. correctness fields (`converged`, `invariants_ok`, `op_failures`)
2. Raft pressure (`raft_appends_total`)
3. throughput/latency fields, with `retries_total` as context

## Reference expectation

On the same machine and similar load, split routing should show **much lower** `raft_appends_total` than all-to-raft while keeping correctness checks green.
