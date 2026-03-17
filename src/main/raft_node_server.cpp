// SPDX-License-Identifier: Apache-2.0
// Networked Raft node server for true multi-client testing.
// Line protocol (tab-separated):
//   PING
//   MEMBERS
//   EXEC\t<op_id>\t<cmd>\t<arg1>\t<arg2>
//   SHUTDOWN

#include "raft/RaftNode.hpp"
#include "runtime/ReplicatedObject.hpp"
#include "analysis/Method.hpp"
#include "runtime/OperationRouter.hpp"
#include "runtime/Operation.hpp"

#include <libnuraft/nuraft.hxx>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

using namespace hamsaz;
using hamsaz::analysis::Method;

struct Args {
  int id = 0;
  int port = 0; // NuRaft port
  std::string host{"127.0.0.1"}; // advertised raft host
  int api_port = 0; // client API tcp port
  std::vector<std::tuple<int, std::string, int>> peers; // id, host, raft_port
  bool join = false;
  bool all_to_raft = false;
  int gossip_min{5};
  int gossip_max{25};
  double gossip_drop{0.0};
  bool gossip_udp{false};
  int gossip_port{0};
  bool no_conflict_gate_requested{false}; // deprecated/no-op, gate is always ON
  bool inproc{false};
};

static std::vector<std::string> splitTabs(const std::string& s) {
  std::vector<std::string> out;
  std::string cur;
  for (char ch : s) {
    if (ch == '\t') {
      out.push_back(cur);
      cur.clear();
    } else {
      cur.push_back(ch);
    }
  }
  out.push_back(cur);
  return out;
}

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
    } else if (arg == "--api-port" && i + 1 < argc) {
      a.api_port = std::stoi(argv[++i]);
    } else if (arg == "--peer" && i + 1 < argc) {
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
      if (i + 1 < argc && argv[i + 1][0] != '-') {
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
  else if (cmd == "query") m = Method::Query;
  return runtime::Operation{op_id, m, a1 == "_" ? "" : a1, a2 == "_" ? "" : a2};
}

static bool writeAll(int fd, const std::string& data) {
  size_t off = 0;
  while (off < data.size()) {
    ssize_t n = send(fd, data.data() + off, data.size() - off, 0);
    if (n <= 0) return false;
    off += static_cast<size_t>(n);
  }
  return true;
}

static bool readLine(int fd, std::string& out) {
  out.clear();
  char ch;
  while (true) {
    ssize_t n = recv(fd, &ch, 1, 0);
    if (n <= 0) return false;
    if (ch == '\n') break;
    if (ch != '\r') out.push_back(ch);
    if (out.size() > 8192) return false;
  }
  return true;
}

int main(int argc, char** argv) {
  Args args = parse_args(argc, argv);
  if (args.id == 0 || args.port == 0 || args.api_port == 0) {
    std::cerr << "Usage: " << argv[0]
              << " --id <n> --port <raft_port> --host <advertise_host> --api-port <tcp_api_port>"
              << " [--peer id:host:port ...]\n";
    return 1;
  }

  runtime::ReplicatedObject obj;
  raft::RaftNode node(obj);

  std::vector<int> peer_ids;
  for (auto& p : args.peers) peer_ids.push_back(std::get<0>(p));
  node.setPeers(peer_ids);
  node.setRoutingPolicy(args.all_to_raft ? hamsaz::raft::RaftRoutingPolicy::AllOps
                                         : hamsaz::raft::RaftRoutingPolicy::ConflictsOnly);
  node.setConflictGateEnabled(true);
  if (args.no_conflict_gate_requested) {
    std::cerr << "[raft_node_server] --no-conflict-gate is ignored; conflict gate is always enabled\n";
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

  bool started = false;
  if (args.inproc) {
    std::vector<std::tuple<int, std::string, int>> cluster;
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

  std::cout << "Node " << args.id << " raft=" << args.host << ":" << args.port
            << " api=0.0.0.0:" << args.api_port
            << " leader? " << (node.isLeader() ? "yes" : "no") << "\n";

  if (!args.inproc) {
    auto try_add_peers = [&]() {
      if (!node.isLeader()) return;
      std::vector<std::tuple<int, std::string, int>> remaining;
      for (auto& p : args.peers) {
        int pid; std::string host; int prt;
        std::tie(pid, host, prt) = p;
        if (!node.addServer(pid, host + ":" + std::to_string(prt))) {
          remaining.push_back(p);
        } else {
          std::cout << "add_srv ok for peer " << pid << "\n";
        }
      }
      args.peers.swap(remaining);
    };
    try_add_peers();
  }

  int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd < 0) {
    std::cerr << "socket() failed\n";
    node.shutdown();
    return 1;
  }
  int opt = 1;
  setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(args.api_port));
  addr.sin_addr.s_addr = INADDR_ANY;
  if (bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    std::cerr << "bind() failed on api port " << args.api_port << "\n";
    close(listen_fd);
    node.shutdown();
    return 1;
  }
  if (listen(listen_fd, 128) < 0) {
    std::cerr << "listen() failed\n";
    close(listen_fd);
    node.shutdown();
    return 1;
  }

  std::atomic<bool> stop{false};
  std::mutex exec_mu;
  std::vector<std::thread> workers;

  auto process_line = [&](const std::string& line) -> std::string {
    if (line == "PING") return "PONG";
    if (line == "MEMBERS") return "MEMBERS\t" + std::to_string(node.configSize());
    if (line == "LEADER") return std::string("LEADER\t") + (node.isLeader() ? "1" : "0");
    if (line == "STATS") {
      return "STATS\t" + std::to_string(node.totalRaftAppends()) + "\t" +
             std::to_string(node.conflictsQueued()) + "\t" +
             std::to_string(node.conflictsAppendedFromQueue()) + "\t" +
             std::to_string(node.conflictsDropped());
    }
    if (line == "SHUTDOWN") {
      stop.store(true);
      return "BYE";
    }

    auto parts = splitTabs(line);
    if (parts.size() == 5 && parts[0] == "EXEC") {
      auto op = make_op(parts[1], parts[2], parts[3], parts[4]);
      if (op.method == Method::Unknown) {
        return "RES\t0\tUnknown command";
      }
      std::lock_guard<std::mutex> lock(exec_mu);
      runtime::OperationRouter router;
      auto route = router.classify(op.method);
      bool requires_raft = args.all_to_raft || route == runtime::Route::Conflicting;
      if (requires_raft && !node.hasServer()) {
        return "RES\t0\tRaft server unavailable";
      }
      auto res = node.submit(op);
      return std::string("RES\t") + (res.ok ? "1" : "0") + "\t" + res.message;
    }

    return "ERR\tBadRequest";
  };

  auto handle_client = [&](int fd) {
    std::string line;
    while (!stop.load()) {
      if (!readLine(fd, line)) break;
      auto reply = process_line(line);
      if (!writeAll(fd, reply + "\n")) break;
      if (line == "SHUTDOWN") break;
    }
    close(fd);
  };

  while (!stop.load()) {
    fd_set rfds;
    FD_ZERO(&rfds);
    FD_SET(listen_fd, &rfds);
    timeval tv{};
    tv.tv_sec = 0;
    tv.tv_usec = 200000; // 200ms
    int rc = select(listen_fd + 1, &rfds, nullptr, nullptr, &tv);
    if (rc < 0) {
      if (errno == EINTR) continue;
      break;
    }
    if (rc == 0) continue;
    int cfd = accept(listen_fd, nullptr, nullptr);
    if (cfd < 0) continue;
    workers.emplace_back(handle_client, cfd);
  }

  close(listen_fd);
  for (auto& t : workers) {
    if (t.joinable()) t.join();
  }

  node.shutdown();
  return 0;
}
