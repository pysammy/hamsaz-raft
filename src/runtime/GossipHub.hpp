// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "runtime/Operation.hpp"
#include <functional>
#include <mutex>
#include <unordered_map>

namespace hamsaz::runtime {

// Very lightweight in-process gossip hub for demos/tests.
// Nodes register a callback; broadcast sends an op to all other registered nodes.
class GossipHub {
public:
  static GossipHub& instance();

  void registerNode(int node_id, std::function<void(const Operation&)> handler);
  void unregisterNode(int node_id);
  void broadcast(int sender_id, const Operation& op);

private:
  std::mutex mu_;
  std::unordered_map<int, std::function<void(const Operation&)>> handlers_;
};

} // namespace hamsaz::runtime
