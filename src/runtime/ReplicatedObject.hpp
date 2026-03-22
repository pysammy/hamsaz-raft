// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "domain/CoursewareState.hpp"
#include "runtime/DependencyTracker.hpp"
#include "runtime/Operation.hpp"
#include "runtime/OperationRouter.hpp"
#include "common/SnapshotCodec.hpp"

#include <deque>
#include <vector>

namespace hamsaz::runtime {

// Single-node façade for Phase 1. In Phase 2 this will be driven by NuRaft.
class ReplicatedObject {
public:
  OperationResult apply(Operation op);

  const domain::CoursewareState& state() const { return state_; }
  common::SnapshotData createSnapshot() const;
  void restoreSnapshot(const common::SnapshotData& snap);
  bool prereqsSatisfied(const Operation& op) const;
  // Returns operations that became applied since last call, including
  // previously deferred operations that have just become executable.
  std::vector<Operation> consumeAppliedOperations();

private:
  OperationResult applyDirect(domain::CoursewareState& target, Operation& op) const;
  OperationResult applyAtomically(Operation op);

  domain::CoursewareState state_;
  OperationRouter router_;
  DependencyTracker tracker_;
  std::deque<Operation> deferredConflicts_;
  std::deque<Operation> recentlyApplied_;
};

} // namespace hamsaz::runtime
