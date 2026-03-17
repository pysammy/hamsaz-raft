// SPDX-License-Identifier: Apache-2.0
#include "raft/RaftNode.hpp"

#ifdef HAMSAZ_WITH_NURAFT
#include <libnuraft/async.hxx>
#include <libnuraft/log_entry.hxx>
#include <libnuraft/launcher.hxx>
#include "raft/FileStateMgr.hpp"
#include <libnuraft/state_machine.hxx>
#include "in_memory_state_mgr.hxx"
#include "common/OperationCodec.hpp"
#include "runtime/GossipHub.hpp"
#include "runtime/GossipEngine.hpp"
#include <chrono>
#include <string>
#include <tuple>
#include <cstring>
#include <iostream>
#include <thread>
#include <fstream>
#include <filesystem>
#endif

namespace hamsaz::raft {

#ifdef HAMSAZ_WITH_NURAFT
std::atomic<uint64_t> RaftNode::raft_appends_{0};
namespace {
bool decodeEnvBytes(const std::string& bytes, runtime::GossipEnvelope& out_env) {
  const uint8_t* p = reinterpret_cast<const uint8_t*>(bytes.data());
  const uint8_t* end = p + bytes.size();
  auto read_u16 = [&](uint16_t& v)->bool{
    if (p + 2 > end) return false;
    uint16_t n; std::memcpy(&n, p, 2); p += 2; v = ntohs(n); return true;
  };
  auto read_u32 = [&](uint32_t& v)->bool{
    if (p + 4 > end) return false;
    uint32_t n; std::memcpy(&n, p, 4); p += 4; v = ntohl(n); return true;
  };
  auto read_u64 = [&](uint64_t& v)->bool{
    if (p + 8 > end) return false;
    uint64_t n; std::memcpy(&n, p, 8); p += 8;
    uint64_t hi = ntohl(static_cast<uint32_t>(n >> 32));
    uint64_t lo = ntohl(static_cast<uint32_t>(n & 0xFFFFFFFFULL));
    v = (lo << 32) | hi;
    return true;
  };
  uint32_t sender = 0; if (!read_u32(sender)) return false;
  out_env.sender = static_cast<int>(sender);
  uint16_t vc_sz = 0; if (!read_u16(vc_sz)) return false;
  for (uint16_t i = 0; i < vc_sz; ++i) {
    uint16_t klen = 0; if (!read_u16(klen)) return false;
    if (p + klen > end) return false;
    std::string key(reinterpret_cast<const char*>(p), klen); p += klen;
    uint64_t cnt = 0; if (!read_u64(cnt)) return false;
    out_env.vc[key] = cnt;
  }
  uint32_t oplen = 0; if (!read_u32(oplen)) return false;
  if (p + oplen > end) return false;
  std::string op_bytes(reinterpret_cast<const char*>(p), oplen);
  p += oplen;
  auto op = hamsaz::common::decodeOperation(op_bytes);
  if (!op) return false;
  out_env.op = *op;
  return p == end;
}

std::string encodeEnvBytes(const runtime::GossipEnvelope& env) {
  auto put_u16 = [](std::vector<uint8_t>& out, uint16_t v) {
    uint16_t n = htons(v);
    auto* p = reinterpret_cast<uint8_t*>(&n);
    out.insert(out.end(), p, p + sizeof(uint16_t));
  };
  auto put_u32 = [](std::vector<uint8_t>& out, uint32_t v) {
    uint32_t n = htonl(v);
    auto* p = reinterpret_cast<uint8_t*>(&n);
    out.insert(out.end(), p, p + sizeof(uint32_t));
  };
  auto put_u64 = [](std::vector<uint8_t>& out, uint64_t v) {
    uint64_t n = ((uint64_t)htonl(v & 0xFFFFFFFFULL) << 32) | htonl((v >> 32) & 0xFFFFFFFFULL);
    auto* p = reinterpret_cast<uint8_t*>(&n);
    out.insert(out.end(), p, p + sizeof(uint64_t));
  };
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
  return std::string(reinterpret_cast<const char*>(out.data()), out.size());
}
} // namespace
#endif
RaftNode::RaftNode(runtime::ReplicatedObject& obj)
#ifdef HAMSAZ_WITH_NURAFT
    : obj_(obj), sm_(nuraft::cs_new<StateMachine>(obj)) {}
#else
    : obj_(obj) {}
#endif

runtime::OperationResult RaftNode::submit(const runtime::Operation& op) {
#ifdef HAMSAZ_WITH_NURAFT
  auto route = router_.classify(op.method);
  bool send_to_raft = server_ &&
    (policy_ == RaftRoutingPolicy::AllOps || route == runtime::Route::Conflicting);
  if (send_to_raft) {
    // For conflicting ops, gate on prerequisites before appending.
    if (route == runtime::Route::Conflicting && conflict_gate_enabled_ && !obj_.prereqsSatisfied(op)) {
      // enqueue and return deferred
      PendingConflict pc{op, std::chrono::steady_clock::now() + conflict_ttl_};
      {
        std::lock_guard<std::mutex> lock(pending_conflict_mu_);
        pending_conflicts_.push_back(std::move(pc));
      }
      conflicts_queued_.fetch_add(1, std::memory_order_relaxed);
      return {false, "Deferred: conflict prerequisites not satisfied"};
    }
    auto bytes = hamsaz::common::encodeOperation(op);
    auto buf = nuraft::buffer::alloc(bytes.size());
    std::memcpy(buf->data_begin(), bytes.data(), bytes.size());
    auto append_res = server_->append_entries({buf});
    raft_appends_.fetch_add(1, std::memory_order_relaxed);
    if (!append_res) return {false, "Raft append failed to start"};
    append_res->get(); // block until committed
    if (!append_res->get_accepted() || append_res->get_result_code() != nuraft::cmd_result_code::OK) {
      return {false, "Raft append rejected"};
    }
    return {true, ""};
  }
#endif
  // Dependent/Independent or no server: apply locally and gossip.
  if (!seen_ops_.insert(op.op_id).second) {
    return {true, ""}; // already applied locally
  }
  // increment local clock for our node
  vc_[node_name_] += 1;
  auto res = obj_.apply(op);
  if (res.ok && node_id_ >= 0) {
    runtime::GossipHub::instance().broadcast(node_id_, op);
    runtime::GossipEnvelope env;
    env.sender = node_id_;
    env.op = op;
    env.vc = vc_;
    storeEnvelope(env);
    runtime::GossipEngine::instance().gossip(env);
    // A successful local apply may have satisfied prerequisites for queued conflicts.
    processPendingConflicts();
  }
  return res;
}

#ifdef HAMSAZ_WITH_NURAFT

bool RaftNode::startSingleNode(int port) {
  return startStandalone(1, port, false);
}

bool RaftNode::startStandalone(int my_id, int port, bool skip_initial_election,
                               const std::string& advertise_host) {
  if (!sm_) {
    sm_ = nuraft::cs_new<StateMachine>(obj_);
  }
  struct StderrLogger : public nuraft::logger {
    void put_details(int level, const char* source_file, const char* func_name,
                     size_t line_number, const std::string& log_line) override {
      std::cerr << "[NuRaft] " << log_line << "\n";
    }
    void debug(const std::string& log_line) override { std::cerr << "[NuRaft][D] " << log_line << "\n"; }
    void info(const std::string& log_line) override { std::cerr << "[NuRaft][I] " << log_line << "\n"; }
    void warn(const std::string& log_line) override { std::cerr << "[NuRaft][W] " << log_line << "\n"; }
    void err(const std::string& log_line) override { std::cerr << "[NuRaft][E] " << log_line << "\n"; }
    void set_level(int) override {}
  };
  auto lg = nuraft::cs_new<StderrLogger>();
  node_id_ = my_id;
  node_name_ = "n" + std::to_string(my_id);
  vc_[node_name_] = vc_[node_name_]; // ensure key exists (default 0)
  loadGossipState();
  launcher_ = std::make_unique<nuraft::raft_launcher>();
  std::string data_dir = "data/node" + std::to_string(my_id);
  auto smgr = nuraft::cs_new<FileStateMgr>(my_id, advertise_host + ":" + std::to_string(port), data_dir);
  nuraft::asio_service::options asio_opt;
  nuraft::raft_params params;
  params.client_req_timeout_ = 3000;
  params.with_auto_forwarding_req_timeout(4000);
  // Prevent an immediate self-election when this node is meant to join an existing cluster.
  // (We still allow election if it is the only node.)
  nuraft::raft_server::init_options init_opt;
  init_opt.skip_initial_election_timeout_ = skip_initial_election;

  server_ = launcher_->init(sm_, smgr, lg, port, asio_opt, params, init_opt);
  if (!server_) {
    std::cerr << "[RaftNode] launcher init returned null (check port/bind permissions)\n";
    return false;
  }
  for (int i = 0; i < 200 && !server_->is_initialized(); ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  if (server_) {
    registerGossipHandler();
    startAntiEntropy();
  }
  if (!server_->is_initialized()) {
    std::cerr << "[RaftNode] server not initialized after wait (continuing to allow join)\n";
  }
  return static_cast<bool>(server_);
}

bool RaftNode::startInProcCluster(int my_id, const std::vector<std::pair<int, int>>& cluster) {
  std::vector<std::tuple<int, std::string, int>> mapped;
  mapped.reserve(cluster.size());
  for (const auto& e : cluster) {
    mapped.emplace_back(e.first, "localhost", e.second);
  }
  return startInProcCluster(my_id, mapped);
}

bool RaftNode::startInProcCluster(int my_id, const std::vector<std::tuple<int, std::string, int>>& cluster) {
  if (!sm_) {
    sm_ = nuraft::cs_new<StateMachine>(obj_);
  }
  node_id_ = my_id;
  node_name_ = "n" + std::to_string(my_id);
  vc_[node_name_] = vc_[node_name_];
  loadGossipState();
  launcher_ = std::make_unique<nuraft::raft_launcher>();

  auto config = nuraft::cs_new<nuraft::cluster_config>();
  std::string my_endpoint;
  int my_port = 0;
  for (const auto& entry : cluster) {
    int id = std::get<0>(entry);
    const std::string& host = std::get<1>(entry);
    int port = std::get<2>(entry);
    std::string ep = host + ":" + std::to_string(port);
    config->get_servers().push_back(nuraft::cs_new<nuraft::srv_config>(id, ep));
    if (id == my_id) {
      my_endpoint = ep;
      my_port = port;
    }
  }
  if (my_endpoint.empty() || my_port == 0) {
    return false;
  }

  std::string data_dir = "data/node" + std::to_string(my_id);
  auto smgr = nuraft::cs_new<FileStateMgr>(my_id, my_endpoint, data_dir);
  smgr->save_config(*config); // share full cluster membership

  nuraft::asio_service::options asio_opt;
  nuraft::raft_params params;
  params.client_req_timeout_ = 3000;
  params.with_auto_forwarding_req_timeout(4000);

  server_ = launcher_->init(sm_, smgr, nullptr, my_port, asio_opt, params);
  if (!server_) return false;
  for (int i = 0; i < 200 && !server_->is_initialized(); ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  if (!server_->is_initialized()) {
    std::cerr << "[RaftNode] server " << my_id << " not initialized after wait\n";
  }
  if (server_) {
    registerGossipHandler();
    startAntiEntropy();
  }
  if (!server_->is_initialized()) {
    std::cerr << "[RaftNode] server " << my_id << " not initialized after wait (continuing)\n";
  }
  return static_cast<bool>(server_);
}

bool RaftNode::isLeader() const {
  return server_ && server_->is_leader();
}

bool RaftNode::addServer(int id, const std::string& endpoint) {
  if (!server_) return false;
  auto res = server_->add_srv(nuraft::srv_config(id, endpoint));
  if (!res) return false;
  res->get();
  return res->get_accepted() && res->get_result_code() == nuraft::cmd_result_code::OK;
}

void RaftNode::setPeers(const std::vector<int>& peers) {
  peers_ = peers;
}

void RaftNode::registerGossipHandler() {
  runtime::GossipHub::instance().registerNode(node_id_, [this](const runtime::Operation& incoming) {
    if (!seen_ops_.insert(incoming.op_id).second) return;
    obj_.apply(incoming);
    processPendingConflicts();
  });
  runtime::GossipEngine::instance().registerNode(node_id_, [this](const runtime::GossipEnvelope& env) {
    // Handle clock-only heartbeat: env.op.method Unknown and op_id "__clock__"
    if (env.op.method == hamsaz::analysis::Method::Unknown && env.op.op_id == "__clock__") {
      auto missing = missingFor(env.vc);
      for (const auto& e : missing) {
        runtime::GossipEngine::instance().gossipTo(env.sender, e);
      }
      return;
    }

    auto is_deliverable = [this](const runtime::GossipEnvelope& msg) {
      std::string sender_name = "n" + std::to_string(msg.sender);
      for (const auto& [node, cnt] : msg.vc) {
        uint64_t local = vc_[node];
        if (node == sender_name) {
          if (cnt != local + 1) return false;
        } else if (cnt > local) {
          return false;
        }
      }
      return true;
    };

    auto try_apply = [this,&is_deliverable](const runtime::GossipEnvelope& msg) {
      if (!is_deliverable(msg)) return false;
      bool first_time = seen_ops_.insert(msg.op.op_id).second;
      if (first_time) {
        obj_.apply(msg.op);
      }
      for (const auto& [n, c] : msg.vc) {
        vc_[n] = std::max(vc_[n], c);
      }
      storeEnvelope(msg);
      return true;
    };

    pending_.push_back(env);

    bool progress = true;
    while (progress) {
      progress = false;
      for (auto it = pending_.begin(); it != pending_.end();) {
        if (try_apply(*it)) {
          it = pending_.erase(it);
          progress = true;
        } else {
          ++it;
        }
      }
    }
    processPendingConflicts();
  });
}

void RaftNode::shutdown() {
  stop_anti_.store(true);
  if (anti_entropy_thread_.joinable()) {
    anti_entropy_thread_.join();
  }
  saveGossipState();
  if (server_) {
    std::cerr << "[RaftNode] node " << node_id_ << " total raft appends: "
              << raft_appends_.load() << "\n";
    std::cerr << "[RaftNode] conflict gate queued: " << conflicts_queued_.load()
              << " appended_from_queue: " << conflicts_from_queue_appended_.load()
              << " dropped: " << conflicts_dropped_.load() << "\n";
  }
  if (launcher_) {
    launcher_->shutdown(2);
    launcher_.reset();
  }
  server_.reset();
  if (node_id_ >= 0) {
    runtime::GossipHub::instance().unregisterNode(node_id_);
    node_id_ = -1;
  }
}

void RaftNode::loadGossipState() {
  std::filesystem::path path = "gossip_state_" + std::to_string(node_id_) + ".bin";
  if (!std::filesystem::exists(path)) return;
  std::ifstream in(path, std::ios::binary);
  if (!in) return;
  auto read_u16 = [&](uint16_t& v) { in.read(reinterpret_cast<char*>(&v), sizeof(v)); v = ntohs(v); };
  auto read_u32 = [&](uint32_t& v) { in.read(reinterpret_cast<char*>(&v), sizeof(v)); v = ntohl(v); };
  auto read_u64 = [&](uint64_t& v) {
    uint64_t n; in.read(reinterpret_cast<char*>(&n), sizeof(n));
    uint64_t hi = ntohl(static_cast<uint32_t>(n >> 32));
    uint64_t lo = ntohl(static_cast<uint32_t>(n & 0xFFFFFFFFULL));
    v = (lo << 32) | hi;
  };
  uint16_t vc_sz = 0;
  read_u16(vc_sz);
  for (uint16_t i = 0; i < vc_sz; ++i) {
    uint16_t klen = 0; read_u16(klen);
    std::string key(klen, '\0');
    in.read(key.data(), klen);
    uint64_t cnt = 0; read_u64(cnt);
    vc_[key] = cnt;
  }
  uint32_t seen_sz = 0;
  read_u32(seen_sz);
  for (uint32_t i = 0; i < seen_sz; ++i) {
    uint16_t len = 0; read_u16(len);
    std::string opid(len, '\0');
    in.read(opid.data(), len);
    seen_ops_.insert(opid);
  }
  uint32_t env_sz = 0;
  read_u32(env_sz);
  for (uint32_t i = 0; i < env_sz; ++i) {
    uint32_t blen = 0; read_u32(blen);
    std::string blob(blen, '\0');
    in.read(blob.data(), blen);
    runtime::GossipEnvelope env;
    if (decodeEnvBytes(blob, env)) {
      recent_envelopes_.push_back(env);
      envelope_by_op_[env.op.op_id] = env;
    }
  }
}

void RaftNode::saveGossipState() {
  if (node_id_ < 0) return;
  std::filesystem::path path = "gossip_state_" + std::to_string(node_id_) + ".bin";
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  if (!out) return;
  auto put_u16 = [&](uint16_t v){ uint16_t n = htons(v); out.write(reinterpret_cast<char*>(&n), sizeof(n)); };
  auto put_u32 = [&](uint32_t v){ uint32_t n = htonl(v); out.write(reinterpret_cast<char*>(&n), sizeof(n)); };
  auto put_u64 = [&](uint64_t v){
    uint64_t n = ((uint64_t)htonl(v & 0xFFFFFFFFULL) << 32) | htonl((v >> 32) & 0xFFFFFFFFULL);
    out.write(reinterpret_cast<char*>(&n), sizeof(n));
  };
  {
    put_u16(static_cast<uint16_t>(vc_.size()));
    for (const auto& [k, c] : vc_) {
      put_u16(static_cast<uint16_t>(k.size()));
      out.write(k.data(), k.size());
      put_u64(c);
    }
  }
  {
    put_u32(static_cast<uint32_t>(seen_ops_.size()));
    for (const auto& opid : seen_ops_) {
      put_u16(static_cast<uint16_t>(opid.size()));
      out.write(opid.data(), opid.size());
    }
  }
  {
    put_u32(static_cast<uint32_t>(recent_envelopes_.size()));
    for (const auto& env : recent_envelopes_) {
      auto s = encodeEnvBytes(env);
      put_u32(static_cast<uint32_t>(s.size()));
      out.write(s.data(), s.size());
    }
  }
}

void RaftNode::storeEnvelope(const runtime::GossipEnvelope& env) {
  std::lock_guard<std::mutex> lock(gossip_mu_);
  recent_envelopes_.push_back(env);
  envelope_by_op_[env.op.op_id] = env;
  constexpr size_t kMaxRecent = 256;
  if (recent_envelopes_.size() > kMaxRecent) {
    envelope_by_op_.erase(recent_envelopes_.front().op.op_id);
    recent_envelopes_.pop_front();
  }
}

void RaftNode::startAntiEntropy() {
  if (anti_entropy_thread_.joinable()) return;
  stop_anti_.store(false);
  anti_entropy_thread_ = std::thread([this] {
    while (!stop_anti_.load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
      std::vector<runtime::GossipEnvelope> batch;
      {
        std::lock_guard<std::mutex> lock(gossip_mu_);
        batch.assign(recent_envelopes_.begin(), recent_envelopes_.end());
      }
      for (const auto& env : batch) {
        runtime::GossipEngine::instance().gossip(env);
      }
      sendClockToPeers();
      processPendingConflicts();
    }
  });
}

void RaftNode::sendClockToPeers() {
  runtime::GossipEnvelope clock;
  clock.sender = node_id_;
  clock.vc = vc_;
  clock.op = runtime::Operation{"__clock__", hamsaz::analysis::Method::Unknown, "", ""};
  for (int pid : peers_) {
    runtime::GossipEngine::instance().gossipTo(pid, clock);
  }
}

std::vector<runtime::GossipEnvelope> RaftNode::missingFor(const std::unordered_map<std::string,uint64_t>& remote_vc) {
  std::vector<runtime::GossipEnvelope> missing;
  std::lock_guard<std::mutex> lock(gossip_mu_);
  for (const auto& [op_id, env] : envelope_by_op_) {
    // If any node's counter in env.vc exceeds remote's counter, remote may be missing it.
    bool newer = false;
    for (const auto& [node, cnt] : env.vc) {
      auto it = remote_vc.find(node);
      uint64_t remote_cnt = (it == remote_vc.end()) ? 0 : it->second;
      if (cnt > remote_cnt) {
        newer = true;
        break;
      }
    }
    if (newer) missing.push_back(env);
  }
  return missing;
}

void RaftNode::processPendingConflicts() {
  if (!server_) return;
  std::deque<PendingConflict> remaining;
  auto now = std::chrono::steady_clock::now();
  {
    std::lock_guard<std::mutex> lock(pending_conflict_mu_);
    remaining.swap(pending_conflicts_);
  }
  for (auto& pc : remaining) {
    if (pc.deadline < now) {
      conflicts_dropped_.fetch_add(1, std::memory_order_relaxed);
      continue; // expired, drop
    }
    if (!obj_.prereqsSatisfied(pc.op)) {
      // still not ready; keep it
      std::lock_guard<std::mutex> lock(pending_conflict_mu_);
      pending_conflicts_.push_back(pc);
      continue;
    }
    auto bytes = hamsaz::common::encodeOperation(pc.op);
    auto buf = nuraft::buffer::alloc(bytes.size());
    std::memcpy(buf->data_begin(), bytes.data(), bytes.size());
    auto append_res = server_->append_entries({buf});
    raft_appends_.fetch_add(1, std::memory_order_relaxed);
    conflicts_from_queue_appended_.fetch_add(1, std::memory_order_relaxed);
    if (!append_res) continue;
    append_res->get();
    if (!append_res->get_accepted() || append_res->get_result_code() != nuraft::cmd_result_code::OK) {
      // failed; drop silently
    }
  }
}

#endif

} // namespace hamsaz::raft
