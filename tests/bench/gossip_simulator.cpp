// SPDX-License-Identifier: Apache-2.0
#include "runtime/GossipEngine.hpp"
#include "runtime/ReplicatedObject.hpp"
#include "runtime/OperationRouter.hpp"
#include "analysis/Method.hpp"

#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

using namespace hamsaz;

struct SimNode {
  int id;
  runtime::ReplicatedObject obj;
  runtime::OperationRouter router;
  std::unordered_set<std::string> seen;
  std::unordered_map<std::string, uint64_t> vc;
};

runtime::Operation makeOp(int seq) {
  return runtime::Operation{
      "sim-op-" + std::to_string(seq),
      analysis::Method::Register,
      "user" + std::to_string(seq),
      ""};
}

int main(int argc, char** argv) {
  int n = 50;
  int ops = 200;
  if (argc > 1) n = std::stoi(argv[1]);
  if (argc > 2) ops = std::stoi(argv[2]);

  std::vector<SimNode> nodes;
  nodes.reserve(n);
  for (int i = 0; i < n; ++i) {
    nodes.push_back(SimNode{i, runtime::ReplicatedObject(), runtime::OperationRouter(), {}, {}});
  }

  runtime::GossipConfig cfg;
  cfg.mode = runtime::GossipMode::Simulated;
  cfg.min_delay_ms = 1;
  cfg.max_delay_ms = 3;
  runtime::GossipEngine::instance().setConfig(cfg);

  for (auto& node : nodes) {
    runtime::GossipEngine::instance().registerNode(node.id, [&node](const runtime::GossipEnvelope& env) {
      std::string sender_name = "n" + std::to_string(env.sender);
      // causal check
      for (const auto& [k, c] : env.vc) {
        uint64_t local = node.vc[k];
        if (k == sender_name) {
          if (c != local + 1) return; // wait for causal gap
        } else if (c > local) {
          return;
        }
      }
      if (!node.seen.insert(env.op.op_id).second) return;
      node.obj.apply(env.op);
      for (const auto& [k, c] : env.vc) node.vc[k] = std::max(node.vc[k], c);
    });
  }

  auto start = std::chrono::steady_clock::now();
  // inject ops from node 0
  for (int i = 0; i < ops; ++i) {
    auto& n0 = nodes[0];
    runtime::Operation op = makeOp(i);
    n0.seen.insert(op.op_id);
    n0.vc["n0"] += 1;
    runtime::GossipEnvelope env;
    env.sender = 0;
    env.op = op;
    env.vc = n0.vc;
    runtime::GossipEngine::instance().gossip(env);
  }

  // wait for convergence
  const int expected = ops;
  bool done = false;
  int waited_ms = 0;
  while (!done && waited_ms < 10000) {
    done = true;
    for (auto& n : nodes) {
      if (static_cast<int>(n.seen.size()) < expected) {
        done = false;
        break;
      }
    }
    if (!done) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
      waited_ms += 20;
    }
  }

  auto end = std::chrono::steady_clock::now();
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  if (!done) {
    std::cout << "Simulation did not converge within 10s (" << waited_ms << "ms)\n";
    return 1;
  }
  double throughput = (ops * 1000.0) / ms;
  std::cout << "Gossip simulator N=" << n << " ops=" << ops
            << " converged in " << ms << " ms, throughput ~"
            << throughput << " ops/sec\n";
  return 0;
}
