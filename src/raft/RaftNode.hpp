// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "runtime/ReplicatedObject.hpp"
#include "runtime/Operation.hpp"
#include "runtime/OperationRouter.hpp"
#include "runtime/GossipEngine.hpp"
#include <atomic>
#include <deque>
#include <mutex>
#include <utility>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <thread>

#ifdef HAMSAZ_WITH_NURAFT
#include "raft/StateMachine.hpp"
#include <libnuraft/nuraft.hxx>
#include <libnuraft/launcher.hxx>
#endif
#include <cstdint>

namespace hamsaz::raft {

enum class RaftRoutingPolicy {
  ConflictsOnly, // send only conflicting (vertex-cover) ops to Raft
  AllOps         // send all ops to Raft (current safe default for demos/tests)
};

class RaftNode {
public:
  explicit RaftNode(runtime::ReplicatedObject& obj);

  // Submit an operation; if NuRaft server is not wired (or flag is off) it falls back to local apply.
  runtime::OperationResult submit(const runtime::Operation& op);

#ifdef HAMSAZ_WITH_NURAFT
  // Placeholder to wire a real NuRaft server; not initialized by default.
  void attachServer(nuraft::ptr<nuraft::raft_server> server) { server_ = server; }
  bool startSingleNode(int port);
  // Start with an explicit server ID (single-node config).
  bool startStandalone(int my_id, int port, bool skip_initial_election = false,
                       const std::string& advertise_host = "localhost");
  // Start as part of an in-process cluster with static config (id, port) pairs.
  bool startInProcCluster(int my_id, const std::vector<std::pair<int, int>>& cluster);
  // Host-aware static cluster config (id, host, port); useful for Docker/K8s.
  bool startInProcCluster(int my_id,
                          const std::vector<std::tuple<int, std::string, int>>& cluster);
  bool isLeader() const;
  size_t configSize() const {
    return (server_ && server_->get_config()) ? server_->get_config()->get_servers().size() : 0;
  }
  uint64_t lastCommitIndex() const { return sm_ ? sm_->last_commit_index() : 0; }
  uint64_t lastSnapshotIndex() const {
    if (!sm_) return 0;
    auto snap = sm_->last_snapshot();
    return snap ? snap->get_last_log_idx() : 0;
  }
  bool addServer(int id, const std::string& endpoint);
  void setPeers(const std::vector<int>& peers);
  void setRoutingPolicy(RaftRoutingPolicy policy) { policy_ = policy; }
  void setConflictGateEnabled(bool enabled) { conflict_gate_enabled_ = enabled; }
  void shutdown();
  bool hasServer() const { return static_cast<bool>(server_); }
#endif
  uint64_t totalRaftAppends() const {
#ifdef HAMSAZ_WITH_NURAFT
    return raft_appends_.load(std::memory_order_relaxed);
#else
    return 0;
#endif
  }
  uint64_t conflictsQueued() const {
#ifdef HAMSAZ_WITH_NURAFT
    return conflicts_queued_.load(std::memory_order_relaxed);
#else
    return 0;
#endif
  }
  uint64_t conflictsAppendedFromQueue() const {
#ifdef HAMSAZ_WITH_NURAFT
    return conflicts_from_queue_appended_.load(std::memory_order_relaxed);
#else
    return 0;
#endif
  }
  uint64_t conflictsDropped() const {
#ifdef HAMSAZ_WITH_NURAFT
    return conflicts_dropped_.load(std::memory_order_relaxed);
#else
    return 0;
#endif
  }

private:
  void registerGossipHandler();
  void storeEnvelope(const runtime::GossipEnvelope& env);
  void loadGossipState();
  void saveGossipState();
  void startAntiEntropy();
  void sendClockToPeers();
  std::vector<runtime::GossipEnvelope> missingFor(const std::unordered_map<std::string,uint64_t>& remote_vc);
  void processPendingConflicts();

  runtime::ReplicatedObject& obj_;
  [[maybe_unused]] runtime::OperationRouter router_;
#ifdef HAMSAZ_WITH_NURAFT
  static std::atomic<uint64_t> raft_appends_;
  nuraft::ptr<nuraft::raft_server> server_;
  nuraft::ptr<StateMachine> sm_;
  std::unique_ptr<nuraft::raft_launcher> launcher_;
  RaftRoutingPolicy policy_{RaftRoutingPolicy::ConflictsOnly};
  bool conflict_gate_enabled_{true};
  int node_id_{-1};
  std::unordered_set<std::string> seen_ops_;
  std::string node_name_;
  std::unordered_map<std::string, uint64_t> vc_; // vector clock
  std::deque<runtime::GossipEnvelope> pending_;
  std::deque<runtime::GossipEnvelope> recent_envelopes_;
  std::unordered_map<std::string, runtime::GossipEnvelope> envelope_by_op_;
  std::vector<int> peers_;
  std::mutex gossip_mu_;
  struct PendingConflict {
    runtime::Operation op;
    std::chrono::steady_clock::time_point deadline;
  };
  std::mutex pending_conflict_mu_;
  std::deque<PendingConflict> pending_conflicts_;
  std::chrono::milliseconds conflict_ttl_{std::chrono::milliseconds(1500)};
  std::atomic<uint64_t> conflicts_queued_{0};
  std::atomic<uint64_t> conflicts_from_queue_appended_{0};
  std::atomic<uint64_t> conflicts_dropped_{0};
  std::atomic<bool> stop_anti_{false};
  std::thread anti_entropy_thread_;
#endif
};

} // namespace hamsaz::raft
