#!/usr/bin/env bash
# Quick two-node demo (conflicts via Raft, non-conflicts via simulated gossip).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD="${ROOT}/build"

seed_port=12001
join_port=12002

echo "[demo] starting seed (id=1)..."
"${BUILD}/raft_node_cli" \
  --id 1 --port "${seed_port}" \
  --peer 2:127.0.0.1:${join_port} \
  --gossip-delay 2-8 \
  >"${ROOT}/artifacts/demo/seed.log" 2>&1 &
seed_pid=$!

sleep 2

echo "[demo] starting joiner (id=2)..."
"${BUILD}/raft_node_cli" \
  --id 2 --port "${join_port}" \
  --join \
  --peer 1:127.0.0.1:${seed_port} \
  --gossip-delay 2-8 \
  >"${ROOT}/artifacts/demo/join.log" 2>&1 &
join_pid=$!

sleep 5

echo "[demo] issuing commands to leader (assuming id=1 leads)"
{
  echo "register alice"
  echo "add CS101"
  echo "enroll alice CS101"
  echo "delete CS101"
  echo "query"
  sleep 1
  echo "exit"
} | "${BUILD}/raft_node_cli" --id 1 --port ${seed_port} --peer 2:127.0.0.1:${join_port} >/dev/null

echo "[demo] tail logs:"
echo "seed log: artifacts/demo/seed.log"
echo "join log: artifacts/demo/join.log"
echo "[demo] stop processes"
kill "${seed_pid}" "${join_pid}" || true
