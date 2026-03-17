#include "raft/RaftNode.hpp"
#include "runtime/ReplicatedObject.hpp"
#include "runtime/GossipEngine.hpp"

#include <iostream>
#include <thread>
#include <chrono>
#include <vector>

int main() {
  hamsaz::runtime::ReplicatedObject obj;
#ifdef HAMSAZ_WITH_NURAFT
  // Two-node in-process cluster to show conflicting ops are ordered.
  hamsaz::runtime::ReplicatedObject obj2;
  hamsaz::raft::RaftNode node1(obj);
  hamsaz::raft::RaftNode node2(obj2);
  // Use ConflictsOnly policy; non-conflicting ops flow via gossip.
  node1.setRoutingPolicy(hamsaz::raft::RaftRoutingPolicy::ConflictsOnly);
  node2.setRoutingPolicy(hamsaz::raft::RaftRoutingPolicy::ConflictsOnly);
  node1.setPeers({2});
  node2.setPeers({1});
  hamsaz::runtime::GossipConfig gcfg;
  gcfg.min_delay_ms = 2; gcfg.max_delay_ms = 8; gcfg.drop_chance = 0.0;
  gcfg.mode = hamsaz::runtime::GossipMode::Simulated;
  hamsaz::runtime::GossipEngine::instance().setConfig(gcfg);
  const int port1 = 12001;
  const int port2 = 12002;

  // Boot both nodes as standalone, then ask node1 (leader) to add node2.
  bool clustered = node1.startStandalone(1, port1) && node2.startStandalone(2, port2);

  if (clustered) {
    auto waitLeader = [&]() -> hamsaz::raft::RaftNode* {
      for (int i = 0; i < 100; ++i) {
        if (node1.isLeader()) return &node1;
        if (node2.isLeader()) return &node2;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
      }
      return nullptr;
    };

    auto leader = waitLeader();
    if (!leader) {
      std::cerr << "No leader elected\n";
      return 1;
    }

    // If leader is node1, add node2; if leader is node2, add node1.
    if (leader == &node1) {
      clustered = node1.addServer(2, "localhost:" + std::to_string(port2));
    } else {
      clustered = node2.addServer(1, "localhost:" + std::to_string(port1));
    }
    if (!clustered) {
      std::cerr << "Failed to add peer to cluster\n";
      return 1;
    }

    using hamsaz::analysis::Method;
    std::vector<hamsaz::runtime::Operation> ops{
        {"demo-op-1", Method::Register, "alice", ""},
        {"demo-op-2", Method::AddCourse, "CS101", ""},
        {"demo-op-3", Method::Enroll, "alice", "CS101"},
        {"demo-op-4", Method::DeleteCourse, "CS101", ""} // conflicting op goes through Raft
    };
    for (const auto& op : ops) {
      auto res = leader->submit(op);
      if (!res.ok) {
        std::cerr << "Submit failed: " << res.message << "\n";
        return 1;
      }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto check_state = [](const hamsaz::runtime::ReplicatedObject& o) {
      const auto& st = o.state();
      return st.hasStudent("alice") &&
             !st.hasCourse("CS101") &&
             !st.hasEnrollment("alice", "CS101");
    };

    if (check_state(obj) && check_state(obj2)) {
      std::cout << "Harness demo OK (NuRaft replicated cluster)\n";
      return 0;
    }

    std::cerr << "Harness demo FAILED: states diverged\n";
    return 1;
  }

  // Fallback: single-node NuRaft instance if clustering is not ready yet.
  node1.shutdown();
  node2.shutdown();
  std::cerr << "Cluster start failed, running single-node Raft harness\n";
  if (!node1.startSingleNode(12001)) {
    std::cerr << "Single-node Raft start failed\n";
    return 1;
  }
  using hamsaz::analysis::Method;
  std::vector<hamsaz::runtime::Operation> ops{
      {"demo-op-1", Method::Register, "alice", ""},
      {"demo-op-2", Method::AddCourse, "CS101", ""},
      {"demo-op-3", Method::Enroll, "alice", "CS101"},
      {"demo-op-4", Method::DeleteCourse, "CS101", ""}};
  for (const auto& op : ops) {
    auto res = node1.submit(op);
    if (!res.ok) {
      std::cerr << "Submit failed: " << res.message << "\n";
      return 1;
    }
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  auto& st = obj.state();
  if (st.hasStudent("alice") && !st.hasCourse("CS101") && !st.hasEnrollment("alice", "CS101")) {
    std::cout << "Harness demo OK (single-node Raft fallback)\n";
    return 0;
  }
  std::cerr << "Harness demo FAILED in fallback path\n";
  return 1;
#else
  // Fallback: single-node local path.
  hamsaz::raft::RaftNode node(obj);
  using hamsaz::analysis::Method;
  hamsaz::runtime::Operation reg{"demo-op-1", Method::Register, "alice", ""};
  hamsaz::runtime::Operation add{"demo-op-2", Method::AddCourse, "CS101", ""};
  hamsaz::runtime::Operation en{"demo-op-3", Method::Enroll, "alice", "CS101"};

  node.submit(reg);
  node.submit(add);
  node.submit(en);

  if (obj.state().hasEnrollment("alice", "CS101")) {
    std::cout << "Harness demo OK (local apply path)\n";
    return 0;
  }
  std::cout << "Harness demo FAILED\n";
  return 1;
#endif
}
