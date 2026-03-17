#include "raft/RaftNode.hpp"
#include "runtime/ReplicatedObject.hpp"
#include "analysis/Method.hpp"
#include "runtime/GossipEngine.hpp"

#include <iostream>
#include <thread>
#include <chrono>
#include <vector>

using namespace hamsaz;
using hamsaz::analysis::Method;

struct NodeCtx {
  runtime::ReplicatedObject obj;
  std::unique_ptr<raft::RaftNode> node;
  int id;
  int port;
  bool up{true};
};

raft::RaftNode* waitLeader(std::vector<NodeCtx>& nodes, int attempts = 200) {
  for (int i = 0; i < attempts; ++i) {
    for (auto& n : nodes) {
      if (n.up && n.node && n.node->isLeader()) return n.node.get();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  return nullptr;
}

int main() {
#ifndef HAMSAZ_WITH_NURAFT
  std::cout << "NuRaft not enabled\n";
  return 0;
#else
  // Use ports that won't collide with the manual 2-node demo.
  const int base = 12100;
  std::vector<NodeCtx> nodes;
  nodes.reserve(3);
  nodes.push_back(NodeCtx{.id = 1, .port = base + 1});
  nodes.push_back(NodeCtx{.id = 2, .port = base + 2});
  nodes.push_back(NodeCtx{.id = 3, .port = base + 3});
  std::vector<int> peer_ids{2,3};

  // Start all nodes standalone (single-node clusters for bootstrap).
  for (auto& n : nodes) {
    n.node = std::make_unique<raft::RaftNode>(n.obj);
    n.node->setRoutingPolicy(raft::RaftRoutingPolicy::ConflictsOnly); // non-conflicts via gossip
    std::vector<int> others;
    for (auto& m : nodes) {
      if (m.id != n.id) others.push_back(m.id);
    }
    n.node->setPeers(others);
    if (!n.node->startStandalone(n.id, n.port)) {
      std::cerr << "Failed to start node " << n.id << "\n";
      return 1;
    }
  }
  {
    runtime::GossipConfig gcfg;
    gcfg.min_delay_ms = 2; gcfg.max_delay_ms = 8; gcfg.drop_chance = 0.0;
    gcfg.mode = runtime::GossipMode::Simulated;
    runtime::GossipEngine::instance().setConfig(gcfg);
  }

  // Let first leader add the others to its cluster.
  auto leader = waitLeader(nodes);
  if (!leader) {
    std::cerr << "No leader elected in initial phase\n";
    return 1;
  }
  for (auto& n : nodes) {
    if (n.node.get() != leader) {
      auto ok = leader->addServer(n.id, "localhost:" + std::to_string(n.port));
      if (!ok) {
        std::cerr << "addServer failed for peer " << n.id << "\n";
        return 1;
      }
    }
  }

  // Submit ops via leader.
  std::vector<runtime::Operation> ops{
      {"op-1", Method::Register, "alice", ""},
      {"op-2", Method::AddCourse, "CS101", ""},
      {"op-3", Method::Enroll, "alice", "CS101"},
  };
  for (auto& op : ops) {
    if (!leader->submit(op).ok) {
      std::cerr << "Submit failed before failover\n";
      return 1;
    }
  }

  // Simulate leader failure.
  for (auto& n : nodes) {
    if (n.node.get() == leader) {
      n.node->shutdown();
      n.up = false;
      break;
    }
  }

  // Wait for new leader among remaining nodes.
  auto newLeader = waitLeader(nodes);
  if (!newLeader) {
    std::cerr << "No leader after failover\n";
    return 1;
  }

  // Submit a conflicting op via new leader.
  runtime::Operation del{"op-4", Method::DeleteCourse, "CS101", ""};
  if (!newLeader->submit(del).ok) {
    std::cerr << "Submit failed after failover\n";
    return 1;
  }

  // Allow replication and convergence (wait up to 5s).
  auto check_state = [](const runtime::ReplicatedObject& o) {
    const auto& st = o.state();
    return st.hasStudent("alice") &&
           !st.hasCourse("CS101") &&
           !st.hasEnrollment("alice", "CS101");
  };
  bool ok = false;
  for (int i = 0; i < 100; ++i) { // 100 * 50ms = 5s
    ok = true;
    for (auto& n : nodes) {
      if (!n.up) continue;
      if (!check_state(n.obj)) {
        ok = false;
        break;
      }
    }
    if (ok) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  if (!ok) {
    for (auto& n : nodes) {
      if (!n.up) continue;
      std::cerr << "State mismatch on node " << n.id << "\n";
    }
    return 1;
  }
  // Clean shutdown of surviving nodes.
  for (auto& n : nodes) {
    if (n.up && n.node) n.node->shutdown();
  }
  std::cout << "Failover harness OK (3 nodes)\n";
  return 0;
#endif
}
