#!/usr/bin/env bash
# Run two benchmark scenarios: All-to-Raft vs Split Routing, then overlay plots.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD="${ROOT}/build"

NODES=${NODES:-3}
OPS=${OPS:-300}
CONFLICT=${CONFLICT:-0.3}
DEPENDENT=${DEPENDENT:-0.3}
GOSSIP_DROP=${GOSSIP_DROP:-0.0}

echo "[compare] running All-to-Raft..."
python3 "${ROOT}/bench/bench.py" \
  --nodes "${NODES}" \
  --ops "${OPS}" \
  --conflict-ratio "${CONFLICT}" \
  --dependent-ratio "${DEPENDENT}" \
  --gossip-udp \
  --gossip-delay 2-8 \
  --all-to-raft

RUN_ALL=$(ls -td "${ROOT}/artifacts/bench/"* | head -1)

echo "[compare] running Split Routing..."
python3 "${ROOT}/bench/bench.py" \
  --nodes "${NODES}" \
  --ops "${OPS}" \
  --conflict-ratio "${CONFLICT}" \
  --dependent-ratio "${DEPENDENT}" \
  --gossip-udp \
  --gossip-delay 2-8

RUN_SPLIT=$(ls -td "${ROOT}/artifacts/bench/"* | head -1)

OUT_DIR="${ROOT}/artifacts/bench_compare"
mkdir -p "${OUT_DIR}"

echo "[compare] plotting overlay..."
python3 "${ROOT}/bench/plot_compare.py" "${RUN_ALL}" "${RUN_SPLIT}" "${OUT_DIR}/latency_cdf_compare.png"

echo "[compare] done."
echo "All-to-Raft run:   ${RUN_ALL}"
echo "Split routing run: ${RUN_SPLIT}"
echo "Overlay plot:      ${OUT_DIR}/latency_cdf_compare.png"
