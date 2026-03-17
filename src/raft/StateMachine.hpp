// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "runtime/ReplicatedObject.hpp"
#include "common/OperationCodec.hpp"
#include "common/SnapshotCodec.hpp"

#ifdef HAMSAZ_WITH_NURAFT
#include <libnuraft/nuraft.hxx>
#endif

namespace hamsaz::raft {

#ifdef HAMSAZ_WITH_NURAFT

// Minimal NuRaft state machine wrapper. Applies operations to ReplicatedObject.
class StateMachine : public nuraft::state_machine {
public:
  explicit StateMachine(runtime::ReplicatedObject& obj) : obj_(obj) {}

  nuraft::ptr<nuraft::buffer> commit(uint64_t log_idx, nuraft::buffer& data) override;
  bool apply_snapshot(nuraft::snapshot& s) override;
  nuraft::ptr<nuraft::snapshot> get_snapshot();
  uint64_t last_commit_index() override { return last_idx_; }
  nuraft::ptr<nuraft::snapshot> last_snapshot() override { return last_snapshot_; }
  void create_snapshot(nuraft::snapshot& s,
                       nuraft::async_result<bool>::handler_type& when_done) override;

private:
  runtime::ReplicatedObject& obj_;
  uint64_t last_idx_{0};
  nuraft::ptr<nuraft::snapshot> last_snapshot_;
};

#else

// Stub when NuRaft is disabled; keeps headers buildable.
class StateMachine {
public:
  explicit StateMachine(runtime::ReplicatedObject&) {}
};

#endif

} // namespace hamsaz::raft
