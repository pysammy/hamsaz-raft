// SPDX-License-Identifier: Apache-2.0
// Minimal manual Raft node runner for multi-terminal testing.
// Start one process per node, e.g.:
//   ./build/raft_node_cli --id 1 --port 12001 --peer 2:127.0.0.1:12002
//   ./build/raft_node_cli --id 2 --port 12002 --peer 1:127.0.0.1:12001
// Submit ops from the elected leader terminal.

#include "raft/RaftNode.hpp"
#include "runtime/ReplicatedObject.hpp"
#include "analysis/Method.hpp"
#include "runtime/OperationRouter.hpp"
#include "runtime/Operation.hpp"

#include <libnuraft/nuraft.hxx>
#include "in_memory_state_mgr.hxx"

#include <chrono>
#include <cstring>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using namespace hamsaz;
using hamsaz::analysis::Method;

struct Args {
  int id = 0;
  int port = 0;
  std::string host{"127.0.0.1"};
  std::vector<std::tuple<int, std::string, int>> peers; // id, host, port
  bool join = false; // if true, start as non-electing follower (for joining node)
  bool all_to_raft = false; // replicate all ops (safer for demos)
  int gossip_min{5};
  int gossip_max{25};
  double gossip_drop{0.0};
  bool gossip_udp{false};
  int gossip_port{0};
  bool no_conflict_gate_requested{false}; // deprecated/no-op, gate is always ON
  bool inproc{false}; // start static in-process cluster (no add_srv)
};

Args parse_args(int argc, char** argv) {
  Args a;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--id" && i + 1 < argc) {
      a.id = std::stoi(argv[++i]);
    } else if (arg == "--port" && i + 1 < argc) {
      a.port = std::stoi(argv[++i]);
    } else if (arg == "--host" && i + 1 < argc) {
      a.host = argv[++i];
    } else if (arg == "--peer" && i + 1 < argc) {
      // format: id:host:port
      std::string p = argv[++i];
      auto first = p.find(':');
      auto second = p.find(':', first + 1);
      if (first == std::string::npos || second == std::string::npos) continue;
      int pid = std::stoi(p.substr(0, first));
      std::string host = p.substr(first + 1, second - first - 1);
      int pport = std::stoi(p.substr(second + 1));
      a.peers.emplace_back(pid, host, pport);
    } else if (arg == "--join") {
      a.join = true;
    } else if (arg == "--all-to-raft") {
      a.all_to_raft = true;
    } else if (arg == "--gossip-delay" && i + 1 < argc) {
      // format: min-max
      std::string v = argv[++i];
      auto dash = v.find('-');
      if (dash != std::string::npos) {
        a.gossip_min = std::stoi(v.substr(0, dash));
        a.gossip_max = std::stoi(v.substr(dash + 1));
      }
    } else if (arg == "--gossip-drop" && i + 1 < argc) {
      a.gossip_drop = std::stod(argv[++i]);
    } else if (arg == "--gossip-udp") {
      a.gossip_udp = true;
      if (i + 1 < argc && argv[i+1][0] != '-') {
        a.gossip_port = std::stoi(argv[++i]);
      }
    } else if (arg == "--no-conflict-gate") {
      a.no_conflict_gate_requested = true;
    } else if (arg == "--inproc") {
      a.inproc = true;
    }
  }
  return a;
}

runtime::Operation make_op(const std::string& op_id,
                           const std::string& cmd,
                           const std::string& a1,
                           const std::string& a2) {
  Method m = Method::Unknown;
  if (cmd == "register") m = Method::Register;
  else if (cmd == "add") m = Method::AddCourse;
  else if (cmd == "enroll") m = Method::Enroll;
  else if (cmd == "delete") m = Method::DeleteCourse;
  else if (cmd == "unenroll") m = Method::Unenroll;
  else m = Method::Unknown;
  return runtime::Operation{op_id, m, a1, a2};
}

void print_help() {
  std::cout << "Commands (leader only for writes):\n"
            << "  status              - show role and last index\n"
            << "  leader              - print leader? (bool)\n"
            << "  register <sid>\n"
            << "  add <cid>\n"
            << "  enroll <sid> <cid>\n"
            << "  unenroll <sid> <cid>\n"
            << "  delete <cid>\n"
            << "  query               - dump state\n"
            << "  help\n"
            << "  exit\n";
}

int main(int argc, char** argv) {
  Args args = parse_args(argc, argv);
  if (args.id == 0 || args.port == 0) {
    std::cerr << "Usage: " << argv[0]
              << " --id <n> --port <p> [--host <addr>] [--peer id:host:port ...]\n";
    return 1;
  }

  runtime::ReplicatedObject obj;
  raft::RaftNode node(obj);
  std::vector<int> peer_ids;
  for (auto& p : args.peers) {
    peer_ids.push_back(std::get<0>(p));
  }
  node.setPeers(peer_ids);
  node.setRoutingPolicy(args.all_to_raft ? hamsaz::raft::RaftRoutingPolicy::AllOps
                                         : hamsaz::raft::RaftRoutingPolicy::ConflictsOnly);
  node.setConflictGateEnabled(true);
  if (args.no_conflict_gate_requested) {
    std::cerr << "[raft_node_cli] --no-conflict-gate is ignored; conflict gate is always enabled\n";
  }

  runtime::GossipConfig gcfg;
  gcfg.min_delay_ms = args.gossip_min;
  gcfg.max_delay_ms = args.gossip_max;
  gcfg.drop_chance = args.gossip_drop;
  gcfg.mode = args.gossip_udp ? runtime::GossipMode::UDP : runtime::GossipMode::Simulated;
  if (args.gossip_udp) {
    gcfg.listen_port = args.gossip_port == 0 ? args.port + 1000 : args.gossip_port;
    for (auto& p : args.peers) {
      int pid; std::string host; int prt;
      std::tie(pid, host, prt) = p;
      gcfg.peers.push_back(runtime::GossipConfig::Peer{pid, host, prt + 1000});
    }
  }
  runtime::GossipEngine::instance().setConfig(gcfg);

  auto self_ep = args.host + ":" + std::to_string(args.port);
  bool started = false;
  if (args.inproc) {
    std::vector<std::tuple<int,std::string,int>> cluster;
    cluster.emplace_back(args.id, args.host, args.port);
    for (auto& p : args.peers) {
      int pid; std::string host; int prt;
      std::tie(pid, host, prt) = p;
      cluster.emplace_back(pid, host, prt);
    }
    started = node.startInProcCluster(args.id, cluster);
  } else {
    started = node.startStandalone(args.id, args.port, args.join, args.host);
  }
  if (!started) {
    std::cerr << "Failed to start Raft server\n";
    return 1;
  }

  std::cout << "Node " << args.id << " listening on " << self_ep
            << " (leader? " << (node.isLeader() ? "yes" : "no") << ")\n";
  print_help();

  auto try_add_peers = [&]() {
    if (args.inproc) return; // static config already contains all peers
    if (!node.isLeader()) return;
    std::vector<std::tuple<int,std::string,int>> remaining;
    for (auto& p : args.peers) {
      int pid; std::string host; int prt;
      std::tie(pid, host, prt) = p;
      if (!node.addServer(pid, host + ":" + std::to_string(prt))) {
        std::cout << "add_srv failed for peer " << pid << " (will retry)\n";
        remaining.push_back(p);
      } else {
        std::cout << "add_srv ok for peer " << pid << "\n";
      }
    }
    args.peers.swap(remaining);
  };

  // If we start as leader, add peers immediately (once).
  try_add_peers();

  // REPL
  std::string line;
  int op_counter = 0;
  while (std::cout << "> " && std::getline(std::cin, line)) {
    // Attempt to add peers once leadership is obtained.
    if (!args.peers.empty() && node.isLeader()) {
      try_add_peers();
    }
    std::istringstream iss(line);
    std::string cmd; iss >> cmd;
    if (cmd.empty()) continue;
    if (cmd == "exit" || cmd == "quit") break;
    if (cmd == "help") { print_help(); continue; }
    if (cmd == "leader") { std::cout << (node.isLeader() ? "yes\n" : "no\n"); continue; }
    if (cmd == "status") {
      std::cout << (node.isLeader() ? "leader" : "follower")
                << " last_idx=" << node.lastCommitIndex()
                << " snapshot_idx=" << node.lastSnapshotIndex()
                << "\n";
      continue;
    }
    if (cmd == "query") {
      const auto& st = obj.state();
      std::cout << "Students:";
      for (auto& s : st.students()) std::cout << " " << s;
      std::cout << "\nCourses:";
      for (auto& c : st.courses()) std::cout << " " << c;
      std::cout << "\nEnrollments:";
      for (auto& e : st.enrollmentList()) std::cout << " (" << e.first << "," << e.second << ")";
      std::cout << "\n";
      continue;
    }
    if (cmd == "members") {
      std::cout << "members " << node.configSize() << "\n";
      continue;
    }

    std::string a1, a2;
    iss >> a1 >> a2;
    auto op = make_op("cli-" + std::to_string(++op_counter), cmd, a1, a2);
    if (op.method == Method::Unknown) {
      std::cout << "Unknown command\n";
      continue;
    }
    // Decide whether this op must go through Raft (conflicting or all-to-raft).
    runtime::OperationRouter router;
    auto route = router.classify(op.method);
    bool requires_raft = args.all_to_raft || route == runtime::Route::Conflicting;
    if (requires_raft && !node.hasServer()) {
      std::cout << "Raft server unavailable\n";
      continue;
    }
    auto res = node.submit(op);
    if (!res.ok) {
      std::cout << res.message << "\n";
      continue;
    }
    std::cout << "ok\n";
  }

  node.shutdown();
  return 0;
}
