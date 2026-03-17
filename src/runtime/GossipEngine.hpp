// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "runtime/Operation.hpp"
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <random>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <netinet/in.h>

namespace hamsaz::runtime {

enum class GossipMode { Simulated, UDP };

struct GossipConfig {
  int min_delay_ms{5};
  int max_delay_ms{25};
  double drop_chance{0.0}; // 0.0 to 1.0
  GossipMode mode{GossipMode::Simulated};
  int listen_port{0}; // UDP mode only
  struct Peer {
    int id{0};
    std::string host;
    int port{0};
  };
  std::vector<Peer> peers;
};

struct GossipEnvelope {
  int sender{0};
  std::unordered_map<std::string, uint64_t> vc;
  Operation op;
};

// A lightweight in-process network simulator for gossip.
// When mode=Simulated: delay/drop + fanout to all registered peers except sender.
// When mode=UDP: use UDP datagrams; still supports drop simulation.
class GossipEngine {
public:
  static GossipEngine& instance();

  void setConfig(const GossipConfig& cfg);
  void registerNode(int node_id, std::function<void(const GossipEnvelope&)> handler);
  void unregisterNode(int node_id);
  // broadcast to all peers
  void gossip(const GossipEnvelope& env);
  // unicast to a single peer (used by anti-entropy)
  void gossipTo(int target_id, const GossipEnvelope& env);

private:
  GossipEngine();
  ~GossipEngine();
  GossipEngine(const GossipEngine&) = delete;
  GossipEngine& operator=(const GossipEngine&) = delete;

  void workerSim();
  void workerUdpRecv();
  void enqueueSim(int target_id, const GossipEnvelope& env);
  void sendUdp(int target_id, const GossipEnvelope& env);
  void rebuildPeerTableLocked();
  std::vector<int> peerIdsLocked() const;
  std::vector<int> broadcastTargetsLocked(int sender) const;

  struct Item {
    std::chrono::steady_clock::time_point due;
    int target_id{-1}; // -1 means broadcast to all except sender
    GossipEnvelope env;
  };

  std::mutex mu_;
  std::condition_variable cv_;
  std::unordered_map<int, std::function<void(const GossipEnvelope&)>> handlers_;
  std::priority_queue<Item, std::vector<Item>,
                      std::function<bool(const Item&, const Item&)>> queue_;
  GossipConfig cfg_;
  std::mt19937 rng_;
  std::uniform_real_distribution<double> drop_dist_{0.0, 1.0};
  std::uniform_int_distribution<int> delay_dist_{5, 25};
  bool stop_{false};
  std::thread sim_thread_;

  // UDP transport
  int udp_fd_{-1};
  std::thread udp_recv_thread_;
  std::unordered_map<int, sockaddr_in> peer_table_;
  int self_id_{-1};
};

} // namespace hamsaz::runtime
