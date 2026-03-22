// SPDX-License-Identifier: Apache-2.0
#include "runtime/GossipEngine.hpp"
#include "common/OperationCodec.hpp"

#include <arpa/inet.h>
#include <cstring>
#include <netdb.h>
#include <unistd.h>

namespace hamsaz::runtime {

namespace {

// --------- simple serialization helpers (network byte order) -----------
void put_u16(std::vector<uint8_t>& out, uint16_t v) {
  uint16_t n = htons(v);
  auto* p = reinterpret_cast<uint8_t*>(&n);
  out.insert(out.end(), p, p + sizeof(uint16_t));
}

void put_u32(std::vector<uint8_t>& out, uint32_t v) {
  uint32_t n = htonl(v);
  auto* p = reinterpret_cast<uint8_t*>(&n);
  out.insert(out.end(), p, p + sizeof(uint32_t));
}

void put_u64(std::vector<uint8_t>& out, uint64_t v) {
  uint64_t n =
      ((uint64_t)htonl(v & 0xFFFFFFFFULL) << 32) | htonl((v >> 32) & 0xFFFFFFFFULL);
  auto* p = reinterpret_cast<uint8_t*>(&n);
  out.insert(out.end(), p, p + sizeof(uint64_t));
}

uint16_t read_u16(const uint8_t*& p, const uint8_t* end) {
  if (p + 2 > end) return 0;
  uint16_t n;
  std::memcpy(&n, p, 2);
  p += 2;
  return ntohs(n);
}

uint32_t read_u32(const uint8_t*& p, const uint8_t* end) {
  if (p + 4 > end) return 0;
  uint32_t n;
  std::memcpy(&n, p, 4);
  p += 4;
  return ntohl(n);
}

uint64_t read_u64(const uint8_t*& p, const uint8_t* end) {
  if (p + 8 > end) return 0;
  uint64_t n;
  std::memcpy(&n, p, 8);
  p += 8;
  uint64_t hi = ntohl(static_cast<uint32_t>(n >> 32));
  uint64_t lo = ntohl(static_cast<uint32_t>(n & 0xFFFFFFFFULL));
  return (lo << 32) | hi;
}

std::vector<uint8_t> encodeEnvelope(const GossipEnvelope& env) {
  std::vector<uint8_t> out;
  put_u32(out, static_cast<uint32_t>(env.sender));
  put_u16(out, static_cast<uint16_t>(env.vc.size()));
  for (const auto& [k, v] : env.vc) {
    put_u16(out, static_cast<uint16_t>(k.size()));
    out.insert(out.end(), k.begin(), k.end());
    put_u64(out, v);
  }
  auto op_bytes = hamsaz::common::encodeOperation(env.op);
  put_u32(out, static_cast<uint32_t>(op_bytes.size()));
  out.insert(out.end(), op_bytes.begin(), op_bytes.end());
  return out;
}

bool decodeEnvelope(const uint8_t* data, size_t len, GossipEnvelope& out_env) {
  const uint8_t* p = data;
  const uint8_t* end = data + len;
  if (p + 4 > end) return false;
  out_env.sender = static_cast<int>(read_u32(p, end));
  uint16_t vc_sz = read_u16(p, end);
  for (uint16_t i = 0; i < vc_sz; ++i) {
    uint16_t klen = read_u16(p, end);
    if (p + klen > end) return false;
    std::string key(reinterpret_cast<const char*>(p), klen);
    p += klen;
    uint64_t val = read_u64(p, end);
    out_env.vc[key] = val;
  }
  uint32_t oplen = read_u32(p, end);
  if (p + oplen > end) return false;
  std::string op_bytes(reinterpret_cast<const char*>(p), oplen);
  p += oplen;
  auto op = hamsaz::common::decodeOperation(op_bytes);
  if (!op) return false;
  out_env.op = *op;
  return p == end;
}

bool resolvePeerAddr(const GossipConfig::Peer& peer, sockaddr_in& out_addr) {
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(peer.port);
  if (inet_pton(AF_INET, peer.host.c_str(), &addr.sin_addr) == 1) {
    out_addr = addr;
    return true;
  }
  addrinfo hints{};
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_DGRAM;
  addrinfo* res = nullptr;
  if (getaddrinfo(peer.host.c_str(), nullptr, &hints, &res) != 0 || res == nullptr) {
    if (res) freeaddrinfo(res);
    return false;
  }
  auto* sin = reinterpret_cast<sockaddr_in*>(res->ai_addr);
  addr.sin_addr = sin->sin_addr;
  freeaddrinfo(res);
  out_addr = addr;
  return true;
}

} // namespace

GossipEngine::GossipEngine()
  : queue_([](const Item& a, const Item& b){ return a.due > b.due; }) {
  std::random_device rd;
  rng_ = std::mt19937(rd());
  sim_thread_ = std::thread([this]{ workerSim(); });
}

GossipEngine::~GossipEngine() {
  {
    std::lock_guard<std::mutex> lock(mu_);
    stop_ = true;
    cv_.notify_all();
  }
  if (sim_thread_.joinable()) sim_thread_.join();
  if (udp_fd_ >= 0) {
    close(udp_fd_);
  }
  if (udp_recv_thread_.joinable()) udp_recv_thread_.join();
}

GossipEngine& GossipEngine::instance() {
  static GossipEngine eng;
  return eng;
}

void GossipEngine::setConfig(const GossipConfig& cfg) {
  std::lock_guard<std::mutex> lock(mu_);
  cfg_ = cfg;
  delay_dist_ = std::uniform_int_distribution<int>(cfg.min_delay_ms, cfg.max_delay_ms);
   // set self_id_ if only one registered handler so far
  if (handlers_.size() == 1 && self_id_ == -1) {
    self_id_ = handlers_.begin()->first;
  }
  if (cfg.mode == GossipMode::UDP && udp_fd_ < 0) {
    udp_fd_ = socket(AF_INET, SOCK_DGRAM, 0);
    if (udp_fd_ >= 0) {
      sockaddr_in addr{};
      addr.sin_family = AF_INET;
      addr.sin_addr.s_addr = INADDR_ANY;
      addr.sin_port = htons(cfg.listen_port);
      if (bind(udp_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        close(udp_fd_);
        udp_fd_ = -1;
      } else {
        udp_recv_thread_ = std::thread([this]{ workerUdpRecv(); });
      }
    }
  }
  rebuildPeerTableLocked();
}

void GossipEngine::registerNode(int node_id, std::function<void(const GossipEnvelope&)> handler) {
  std::lock_guard<std::mutex> lock(mu_);
  handlers_[node_id] = std::move(handler);
  if (self_id_ == -1) self_id_ = node_id;
  rebuildPeerTableLocked();
}

void GossipEngine::unregisterNode(int node_id) {
  std::lock_guard<std::mutex> lock(mu_);
  handlers_.erase(node_id);
  rebuildPeerTableLocked();
}

void GossipEngine::gossip(const GossipEnvelope& env) {
  gossipTo(-1, env);
}

void GossipEngine::gossipTo(int target_id, const GossipEnvelope& env) {
  if (cfg_.mode == GossipMode::UDP && udp_fd_ >= 0) {
    sendUdp(target_id, env);
    return;
  }
  enqueueSim(target_id, env);
}

void GossipEngine::enqueueSim(int target_id, const GossipEnvelope& env) {
  auto now = std::chrono::steady_clock::now();
  int delay = delay_dist_(rng_);
  Item item{now + std::chrono::milliseconds(delay), target_id, env};
  {
    std::lock_guard<std::mutex> lock(mu_);
    queue_.push(std::move(item));
    cv_.notify_all();
  }
}

void GossipEngine::workerSim() {
  std::unique_lock<std::mutex> lock(mu_);
  while (!stop_) {
    if (queue_.empty()) {
      cv_.wait(lock, [this]{ return stop_ || !queue_.empty(); });
      continue;
    }
    auto next = queue_.top();
    auto now = std::chrono::steady_clock::now();
    if (now < next.due) {
      cv_.wait_until(lock, next.due);
      continue;
    }
    queue_.pop();
    auto handlers_copy = handlers_; // copy under lock
    int target = next.target_id;
    lock.unlock();
    if (target == -1) {
      for (auto& [id, cb] : handlers_copy) {
        if (id == next.env.sender) continue;
        if (drop_dist_(rng_) < cfg_.drop_chance) continue;
        if (cb) cb(next.env);
      }
    } else {
      auto it = handlers_copy.find(target);
      if (it != handlers_copy.end()) {
        if (drop_dist_(rng_) >= cfg_.drop_chance && it->second) {
          it->second(next.env);
        }
      }
    }
    lock.lock();
  }
}

void GossipEngine::rebuildPeerTableLocked() {
  peer_table_.clear();
  for (const auto& peer : cfg_.peers) {
    sockaddr_in addr{};
    if (resolvePeerAddr(peer, addr)) {
      peer_table_[peer.id] = addr;
    }
  }
}

void GossipEngine::sendUdp(int target_id, const GossipEnvelope& env) {
  std::vector<sockaddr_in> targets;
  {
    std::lock_guard<std::mutex> lock(mu_);
    // Re-resolve peer hostnames each send to avoid stale pod IPs after restarts.
    rebuildPeerTableLocked();
    if (target_id == -1) {
      for (const auto& [peer_id, addr] : peer_table_) {
        if (peer_id == env.sender) continue;
        targets.push_back(addr);
      }
    } else {
      auto it = peer_table_.find(target_id);
      if (it != peer_table_.end()) {
        targets.push_back(it->second);
      }
    }
  }
  auto bytes = encodeEnvelope(env);
  for (const auto& addr : targets) {
    sendto(udp_fd_, bytes.data(), bytes.size(), 0,
           reinterpret_cast<const sockaddr*>(&addr), sizeof(sockaddr_in));
  }
}

std::vector<int> GossipEngine::peerIdsLocked() const {
  std::vector<int> ids;
  ids.reserve(peer_table_.size());
  for (auto& kv : peer_table_) ids.push_back(kv.first);
  return ids;
}

std::vector<int> GossipEngine::broadcastTargetsLocked(int sender) const {
  std::vector<int> ids;
  for (auto& kv : peer_table_) {
    if (kv.first == sender) continue;
    ids.push_back(kv.first);
  }
  return ids;
}

void GossipEngine::workerUdpRecv() {
  while (!stop_) {
    uint8_t buf[4096];
    sockaddr_in src{};
    socklen_t slen = sizeof(src);
    ssize_t n = recvfrom(udp_fd_, buf, sizeof(buf), 0,
                         reinterpret_cast<sockaddr*>(&src), &slen);
    if (n <= 0) {
      if (stop_) break;
      continue;
    }
    GossipEnvelope env;
    if (!decodeEnvelope(buf, static_cast<size_t>(n), env)) {
      continue;
    }
    std::function<void(const GossipEnvelope&)> cb;
    {
      std::lock_guard<std::mutex> lock(mu_);
      auto it = handlers_.find(self_id_);
      if (it != handlers_.end()) cb = it->second;
    }
    if (cb && drop_dist_(rng_) >= cfg_.drop_chance) {
      cb(env);
    }
  }
}

} // namespace hamsaz::runtime
