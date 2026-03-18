// SPDX-License-Identifier: Apache-2.0
#include "raft/StateMachine.hpp"
#include <cstring>

#ifdef HAMSAZ_WITH_NURAFT

using hamsaz::common::decodeOperation;
using hamsaz::common::encodeOperation;
using hamsaz::common::decodeSnapshot;
using hamsaz::common::encodeSnapshot;

namespace hamsaz::raft {

nuraft::ptr<nuraft::buffer> StateMachine::commit(uint64_t log_idx, nuraft::buffer& data) {
  last_idx_ = log_idx;
  std::string payload(reinterpret_cast<char*>(data.data_begin()), data.size());
  auto op = decodeOperation(payload);
  if (!op) {
    return nullptr;
  }
  auto res = apply_fn_ ? apply_fn_(*op) : obj_.apply(*op);
  if (!res.ok) {
    return nullptr;
  }
  return nuraft::buffer::alloc(0);
}

bool StateMachine::apply_snapshot(nuraft::snapshot& s) {
  auto buf = s.serialize();
  if (!buf) return false;
  std::string bytes(reinterpret_cast<char*>(buf->data_begin()), buf->size());
  auto snap = decodeSnapshot(bytes);
  if (snap) {
    obj_.restoreSnapshot(*snap);
  }
  last_idx_ = s.get_last_log_idx();
  last_snapshot_ = nuraft::ptr<nuraft::snapshot>(&s, [](nuraft::snapshot*) {});
  return true;
}

nuraft::ptr<nuraft::snapshot> StateMachine::get_snapshot() {
  auto snapData = obj_.createSnapshot();
  auto bytes = encodeSnapshot(snapData);
  auto buf = nuraft::buffer::alloc(bytes.size());
  std::memcpy(buf->data_begin(), bytes.data(), bytes.size());
  auto conf = nuraft::cs_new<nuraft::cluster_config>();
  auto snp = nuraft::cs_new<nuraft::snapshot>(last_idx_, last_idx_, conf, bytes.size(), nuraft::snapshot::logical_object);
  last_snapshot_ = snp;
  return snp;
}

void StateMachine::create_snapshot(nuraft::snapshot& s,
                                   nuraft::async_result<bool>::handler_type& when_done) {
  last_snapshot_ = nuraft::ptr<nuraft::snapshot>(&s, [](nuraft::snapshot*) {});
  nuraft::ptr<std::exception> err;
  bool ok = true;
  when_done(ok, err);
}

} // namespace hamsaz::raft

#endif // HAMSAZ_WITH_NURAFT
