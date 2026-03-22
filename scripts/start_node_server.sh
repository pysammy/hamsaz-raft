#!/usr/bin/env bash
set -euo pipefail

NODES="${NODES:-10}"
RAFT_PORT="${RAFT_PORT:-15000}"
API_PORT="${API_PORT:-25000}"
SERVICE_NAME="${SERVICE_NAME:-raft-headless}"
GOSSIP_DELAY="${GOSSIP_DELAY:-2-8}"
GOSSIP_DROP="${GOSSIP_DROP:-0.0}"
GOSSIP_UDP="${GOSSIP_UDP:-1}"
ALL_TO_RAFT="${ALL_TO_RAFT:-0}"
RESET_STATE_ON_START="${RESET_STATE_ON_START:-1}"

if [[ -z "${HOSTNAME:-}" ]]; then
  echo "HOSTNAME is not set" >&2
  exit 1
fi

ordinal="${HOSTNAME##*-}"
if ! [[ "${ordinal}" =~ ^[0-9]+$ ]]; then
  echo "Unable to parse pod ordinal from HOSTNAME=${HOSTNAME}" >&2
  exit 1
fi

id=$((ordinal + 1))

if [[ "${RESET_STATE_ON_START}" == "1" ]]; then
  rm -rf /app/data /app/logs
  rm -f /app/gossip_state_*.bin
fi

my_host="${HOSTNAME}.${SERVICE_NAME}"
args=(--id "${id}" --port "${RAFT_PORT}" --host "${my_host}" --api-port "${API_PORT}" --inproc \
      --gossip-delay "${GOSSIP_DELAY}" --gossip-drop "${GOSSIP_DROP}")
if [[ "${GOSSIP_UDP}" == "1" ]]; then
  args+=(--gossip-udp)
fi

if [[ "${ALL_TO_RAFT}" == "1" ]]; then
  args+=(--all-to-raft)
fi

for ((j = 1; j <= NODES; ++j)); do
  if [[ "${j}" -eq "${id}" ]]; then
    continue
  fi
  peer_ordinal=$((j - 1))
  peer_host="raft-node-${peer_ordinal}.${SERVICE_NAME}"
  args+=(--peer "${j}:${peer_host}:${RAFT_PORT}")
done

exec /app/raft_node_server "${args[@]}"
