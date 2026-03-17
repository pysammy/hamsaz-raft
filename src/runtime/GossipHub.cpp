// SPDX-License-Identifier: Apache-2.0
#include "runtime/GossipHub.hpp"

namespace hamsaz::runtime {

GossipHub& GossipHub::instance() {
  static GossipHub hub;
  return hub;
}

void GossipHub::registerNode(int node_id, std::function<void(const Operation&)> handler) {
  std::lock_guard<std::mutex> lock(mu_);
  handlers_[node_id] = std::move(handler);
}

void GossipHub::unregisterNode(int node_id) {
  std::lock_guard<std::mutex> lock(mu_);
  handlers_.erase(node_id);
}

void GossipHub::broadcast(int sender_id, const Operation& op) {
  std::unordered_map<int, std::function<void(const Operation&)>> copy;
  {
    std::lock_guard<std::mutex> lock(mu_);
    copy = handlers_;
  }
  for (auto& [id, cb] : copy) {
    if (id == sender_id) continue;
    if (cb) cb(op);
  }
}

} // namespace hamsaz::runtime
