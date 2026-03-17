#!/usr/bin/env bash
# Three-node demo with real UDP gossip (conflicts via Raft).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD="${ROOT}/build"

base=12010
ports=(${base} $((base+1)) $((base+2)))
udp_ports=($((base+1000)) $((base+1001)) $((base+1002)))

mkdir -p "${ROOT}/artifacts/demo"

start_node() {
  local id=$1 port=$2 udp_port=$3 join_flag=$4
  shift 4
  local peers=("$@")
  local peer_args=()
  for p in "${peers[@]}"; do peer_args+=(--peer "${p}"); done
  "${BUILD}/raft_node_cli" \
    --id "${id}" --port "${port}" \
    "${join_flag}" \
    --gossip-udp "${udp_port}" \
    "${peer_args[@]}" \
    >"${ROOT}/artifacts/demo/node${id}.log" 2>&1 &
  echo $!
}

echo "[demo] starting nodes..."
pid1=$(start_node 1 ${ports[0]} ${udp_ports[0]} "" \
  2:127.0.0.1:${ports[1]} 3:127.0.0.1:${ports[2]})
pid2=$(start_node 2 ${ports[1]} ${udp_ports[1]} "--join" \
  1:127.0.0.1:${ports[0]} 3:127.0.0.1:${ports[2]})
pid3=$(start_node 3 ${ports[2]} ${udp_ports[2]} "--join" \
  1:127.0.0.1:${ports[0]} 2:127.0.0.1:${ports[1]})

sleep 6

echo "[demo] issuing commands to leader (assume node1 leader; adjust if needed)"
{
  echo "register alice"
  echo "add CS101"
  echo "enroll alice CS101"
  echo "delete CS101"
  echo "query"
  sleep 1
  echo "exit"
} | "${BUILD}/raft_node_cli" --id 1 --port ${ports[0]} --peer 2:127.0.0.1:${ports[1]} --peer 3:127.0.0.1:${ports[2]} --gossip-udp ${udp_ports[0]} >/dev/null

echo "[demo] logs in artifacts/demo/node*.log"
echo "[demo] shutting down"
kill "${pid1}" "${pid2}" "${pid3}" || true
