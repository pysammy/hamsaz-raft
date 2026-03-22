#!/usr/bin/env bash
set -euo pipefail

# Reproducible 3-node external benchmark runner:
# - split routing (conflicts -> Raft, others -> gossip)
# - all-to-raft baseline
# Both runs enforce correctness gates.

BUILD_DIR="${BUILD_DIR:-build}"
OPS="${OPS:-1500}"
CONCURRENCY="${CONCURRENCY:-8}"
CONFLICT_RATIO="${CONFLICT_RATIO:-0.25}"
DEPENDENT_RATIO="${DEPENDENT_RATIO:-0.25}"
DATE_TAG="${DATE_TAG:-$(date +%Y-%m-%d)}"
RESULT_ROOT="${RESULT_ROOT:-analysis-results-${DATE_TAG}/three-node-external}"
MAX_RETRIES="${MAX_RETRIES:-100}"
RETRY_DELAY_MS="${RETRY_DELAY_MS:-25}"
REQUEST_TIMEOUT_SEC="${REQUEST_TIMEOUT_SEC:-5}"

if [[ ! -x "${BUILD_DIR}/raft_node_server" ]]; then
  echo "missing binary: ${BUILD_DIR}/raft_node_server" >&2
  echo "build first: cmake -S . -B ${BUILD_DIR} -DHAMSAZ_WITH_NURAFT=ON -DHAMSAZ_BUILD_TESTS=ON && cmake --build ${BUILD_DIR} -j" >&2
  exit 1
fi

cleanup_pids() {
  local pids=("$@")
  for p in "${pids[@]}"; do
    kill "${p}" >/dev/null 2>&1 || true
  done
}

run_case() {
  local case_name="$1"      # split | all_to_raft
  local all_to_raft="$2"    # 0 | 1
  local raft_base="$3"
  local api_base="$4"
  local out_dir="${RESULT_ROOT}/raw/${case_name}"

  echo "[repro] running case=${case_name}" >&2
  mkdir -p "${out_dir}"

  rm -rf data/node1 data/node2 data/node3
  rm -f gossip_state_1.bin gossip_state_2.bin gossip_state_3.bin

  local -a all_flag=()
  if [[ "${all_to_raft}" == "1" ]]; then
    all_flag+=(--all-to-raft)
  fi

  "${BUILD_DIR}/raft_node_server" --id 1 --host 127.0.0.1 --port "${raft_base}" --api-port "${api_base}" \
    --inproc --gossip-udp --gossip-delay 2-8 "${all_flag[@]:-}" \
    --peer 2:127.0.0.1:$((raft_base + 1)) --peer 3:127.0.0.1:$((raft_base + 2)) \
    > "${BUILD_DIR}/srv1_${case_name}.log" 2>&1 &
  local p1=$!
  "${BUILD_DIR}/raft_node_server" --id 2 --host 127.0.0.1 --port "$((raft_base + 1))" --api-port "$((api_base + 1))" \
    --inproc --gossip-udp --gossip-delay 2-8 "${all_flag[@]:-}" \
    --peer 1:127.0.0.1:${raft_base} --peer 3:127.0.0.1:$((raft_base + 2)) \
    > "${BUILD_DIR}/srv2_${case_name}.log" 2>&1 &
  local p2=$!
  "${BUILD_DIR}/raft_node_server" --id 3 --host 127.0.0.1 --port "$((raft_base + 2))" --api-port "$((api_base + 2))" \
    --inproc --gossip-udp --gossip-delay 2-8 "${all_flag[@]:-}" \
    --peer 1:127.0.0.1:${raft_base} --peer 2:127.0.0.1:$((raft_base + 1)) \
    > "${BUILD_DIR}/srv3_${case_name}.log" 2>&1 &
  local p3=$!

  sleep 10

  set +e
  python3 bench/bench_multiclient_external.py \
    --hosts "127.0.0.1:${api_base},127.0.0.1:$((api_base + 1)),127.0.0.1:$((api_base + 2))" \
    --wait-members 3 \
    --ops "${OPS}" \
    --conflict-ratio "${CONFLICT_RATIO}" \
    --dependent-ratio "${DEPENDENT_RATIO}" \
    --concurrency "${CONCURRENCY}" \
    --verify-correctness \
    --fail-on-correctness \
    --fail-on-op-errors \
    --max-retries "${MAX_RETRIES}" \
    --retry-delay-ms "${RETRY_DELAY_MS}" \
    --request-timeout-sec "${REQUEST_TIMEOUT_SEC}" \
    --settle-timeout-sec 60 \
    --settle-poll-ms 200 \
    --drain-timeout-sec 3 \
    --drain-poll-ms 200 \
    --out "${out_dir}" >&2
  local rc=$?
  set -e

  cleanup_pids "${p1}" "${p2}" "${p3}"

  if [[ ${rc} -ne 0 ]]; then
    echo "benchmark failed for case=${case_name}" >&2
    exit ${rc}
  fi

  local latest
  latest="$(ls -1dt "${out_dir}"/* 2>/dev/null | head -n1)"
  if [[ -z "${latest}" ]]; then
    echo "no artifact generated for case=${case_name}" >&2
    exit 1
  fi
  echo "${latest}"
}

split_dir="$(run_case split 0 17001 36001)"
all_dir="$(run_case all_to_raft 1 17101 36101)"

mkdir -p "${RESULT_ROOT}/split" "${RESULT_ROOT}/all_to_raft"
cp -f "${split_dir}/summary.json" "${split_dir}/latencies.csv" "${split_dir}/latency_cdf.png" "${split_dir}/latency_scatter.png" "${RESULT_ROOT}/split/"
cp -f "${all_dir}/summary.json" "${all_dir}/latencies.csv" "${all_dir}/latency_cdf.png" "${all_dir}/latency_scatter.png" "${RESULT_ROOT}/all_to_raft/"

python3 - "${RESULT_ROOT}" "${OPS}" <<'PY'
import csv
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
ops_requested = int(sys.argv[2])
split = json.loads((root / "split" / "summary.json").read_text())
allr = json.loads((root / "all_to_raft" / "summary.json").read_text())

def row(label, s):
    gate = s.get("conflict_gate_stats", {})
    corr = s.get("correctness", {})
    return {
        "case": label,
        "ops_requested": ops_requested,
        "ops_reported": s.get("ops"),
        "concurrency": s.get("concurrency"),
        "avg_latency_ms": round(float(s.get("avg_latency_sec", 0.0)) * 1000.0, 3),
        "throughput_rtt_ops_sec": round(float(s.get("throughput_ops_per_sec", 0.0)), 3),
        "throughput_wall_ops_sec": round(float(s.get("wall_throughput_ops_per_sec", 0.0)), 3),
        "raft_appends_total": int(sum(s.get("raft_appends", []))),
        "gate_queued": int(gate.get("queued", 0)),
        "gate_appended_from_queue": int(gate.get("appended_from_queue", 0)),
        "gate_dropped": int(gate.get("dropped", 0)),
        "converged": bool(corr.get("converged", False)),
        "invariants_ok": bool(corr.get("all_invariants_ok", False)),
        "op_failures": int(s.get("op_failures", 0)),
    }

rows = [row("split_routing", split), row("all_to_raft", allr)]
out_csv = root / "metrics_summary.csv"
with out_csv.open("w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
    w.writeheader()
    w.writerows(rows)
print(f"[repro] wrote {out_csv}")
PY

python3 bench/plot_repro_compare.py "${RESULT_ROOT}" >/dev/null 2>&1 || true

cat > "${RESULT_ROOT}/README.md" <<EOF
# 3-Node External Benchmark Reproduction (${DATE_TAG})

This folder stores a reproducible 3-node external benchmark run with correctness checks enabled.

## Configuration

- Nodes: 3
- Ops requested per run: ${OPS}
- Concurrency: ${CONCURRENCY}
- Conflict ratio: ${CONFLICT_RATIO}
- Dependent ratio: ${DEPENDENT_RATIO}
- Correctness gates: ON
  - \`--verify-correctness\`
  - \`--fail-on-correctness\`
  - \`--fail-on-op-errors\`

## Included runs

1. **split routing**: conflicting -> Raft, dependent/independent -> local+gossip
2. **all-to-raft baseline**: all operations routed through Raft

## Files

- \`split/summary.json\`
- \`split/latencies.csv\`
- \`split/latency_cdf.png\`
- \`split/latency_scatter.png\`
- \`all_to_raft/summary.json\`
- \`all_to_raft/latencies.csv\`
- \`all_to_raft/latency_cdf.png\`
- \`all_to_raft/latency_scatter.png\`
- \`metrics_summary.csv\` (single table for direct comparison)
- \`latency_cdf_compare.png\` (common CDF, split vs all-to-raft)
- \`throughput_bar_compare.png\` (RTT + wall throughput bars)
- \`avg_latency_bar_compare.png\` (average latency bars)
- \`raft_appends_bar_compare.png\` (Raft pressure bars)
- \`gate_counters_bar_compare.png\` (queued/appended/dropped counters)

## How to reproduce exactly

From repo root:

\`\`\`bash
cmake -S . -B build -DHAMSAZ_WITH_NURAFT=ON -DHAMSAZ_BUILD_TESTS=ON
cmake --build build -j
scripts/repro_3node_external.sh
\`\`\`

If you want a fixed output location:

\`\`\`bash
RESULT_ROOT=analysis-results-${DATE_TAG}/three-node-external scripts/repro_3node_external.sh
\`\`\`

## What to verify

- In both summaries:
  - \`correctness.converged = true\`
  - \`correctness.all_invariants_ok = true\`
  - \`op_failures = 0\`
- In \`metrics_summary.csv\`:
  - \`split_routing\` should show significantly fewer \`raft_appends_total\` than \`all_to_raft\`.

## Interpreting \`ops_reported\`

- \`ops_requested\` is the intended workload size (fixed by \`--ops\`).
- \`ops_reported\` comes from current benchmark accounting and may include retry-attempt effects in all-to-raft mode under transient append rejections.
- For correctness comparisons, use \`converged\`, \`invariants_ok\`, and \`op_failures\`; for pressure comparisons, use \`raft_appends_total\`.

## Notes

- This run uses real external TCP API calls to three server processes on localhost.
- It is external benchmark mode (not the in-process harness benchmark client).
- Absolute latency/throughput can vary by machine, but convergence/invariant correctness and the relative Raft-pressure pattern should remain consistent.
EOF

echo "[repro] done: ${RESULT_ROOT}"
