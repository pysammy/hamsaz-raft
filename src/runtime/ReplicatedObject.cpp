// SPDX-License-Identifier: Apache-2.0
#include "runtime/ReplicatedObject.hpp"

namespace hamsaz::runtime {

OperationResult ReplicatedObject::apply(Operation op) {
  auto route = router_.classify(op.method);

  if (route == Route::Dependent && !tracker_.canExecute(op)) {
    tracker_.defer(op);
    return {true, "Deferred: dependencies not satisfied"};
  }

  if (route == Route::Conflicting && !op.prereq_op_ids.empty() && !prereqsSatisfied(op)) {
    deferredConflicts_.push_back(op);
    return {true, "Deferred: conflict prerequisites not satisfied"};
  }

  auto result = applyDirect(op);
  if (!result.ok) {
    return result;
  }

  // Record only on success.
  tracker_.record(op);
  recentlyApplied_.push_back(op);

  // Invariant check after application.
  if (!state_.satisfiesInvariant()) {
    return {false, "Invariant violated after apply"};
  }

  // Try to apply any deferred ops that just became ready.
  while (true) {
    auto ready = tracker_.takeReady();
    if (ready.empty()) break;
    for (auto& depOp : ready) {
      auto r = applyDirect(depOp);
      if (r.ok) {
        tracker_.record(depOp);
        recentlyApplied_.push_back(depOp);
        if (!state_.satisfiesInvariant()) {
          return {false, "Invariant violated after deferred apply"};
        }
      } else {
        // Drop failed deferred op but continue processing others.
      }
    }
  }

  // Process deferred conflicting operations once prerequisites become true.
  while (true) {
    std::deque<Operation> still_waiting;
    bool progress = false;
    while (!deferredConflicts_.empty()) {
      auto conf = deferredConflicts_.front();
      deferredConflicts_.pop_front();
      if (!prereqsSatisfied(conf)) {
        still_waiting.push_back(std::move(conf));
        continue;
      }
      auto r = applyDirect(conf);
      if (r.ok) {
        tracker_.record(conf);
        recentlyApplied_.push_back(conf);
        progress = true;
        if (!state_.satisfiesInvariant()) {
          return {false, "Invariant violated after deferred conflict apply"};
        }
      } else {
        // Keep waiting for state changes (e.g., missing course still absent).
        still_waiting.push_back(std::move(conf));
      }
    }
    deferredConflicts_.swap(still_waiting);
    if (!progress) break;
  }

  return result;
}

bool ReplicatedObject::prereqsSatisfied(const Operation& op) const {
  using analysis::Method;
  switch (op.method) {
  case Method::DeleteCourse:
    // Require course to exist and have no enrollments to safely delete.
    return state_.courseHasNoEnrollments(op.arg1);
  default:
    return true;
  }
}

OperationResult ReplicatedObject::applyDirect(Operation& op) {
  using analysis::Method;
  bool ok = false;
  std::string msg;
  switch (op.method) {
    case Method::Register:
      ok = state_.registerStudent(op.arg1);
      if (!ok) msg = "Student already exists";
      break;
    case Method::AddCourse:
      ok = state_.addCourse(op.arg1);
      if (!ok) msg = "Course already exists";
      break;
    case Method::Enroll:
      ok = state_.enroll(op.arg1, op.arg2);
      if (!ok) msg = "Enroll failed (missing prereqs or duplicate)";
      break;
    case Method::Unenroll:
      ok = state_.unenroll(op.arg1, op.arg2);
      if (!ok) msg = "Unenroll failed (not enrolled)";
      break;
    case Method::DeleteCourse:
      ok = state_.deleteCourse(op.arg1);
      if (!ok) msg = "Course not found";
      break;
    case Method::Query:
      // Query is read-only; treat as success.
      ok = true;
      break;
    default:
      return {false, "Unknown method"};
  }
  return {ok, ok ? "" : msg};
}

common::SnapshotData ReplicatedObject::createSnapshot() const {
  common::SnapshotData snap;
  for (const auto& s : state_.students()) snap.students.push_back(s);
  for (const auto& c : state_.courses()) snap.courses.push_back(c);
  snap.enrollments = state_.enrollmentList();
  snap.seenStudents = tracker_.seenStudents();
  snap.seenCourses = tracker_.seenCourses();
  return snap;
}

void ReplicatedObject::restoreSnapshot(const common::SnapshotData& snap) {
  state_.rebuild(snap.students, snap.courses, snap.enrollments);
  tracker_.loadSeen(snap.seenStudents, snap.seenCourses);
  tracker_.clearDeferred();
  deferredConflicts_.clear();
  recentlyApplied_.clear();
}

std::vector<Operation> ReplicatedObject::consumeAppliedOperations() {
  std::vector<Operation> out;
  out.reserve(recentlyApplied_.size());
  while (!recentlyApplied_.empty()) {
    out.push_back(std::move(recentlyApplied_.front()));
    recentlyApplied_.pop_front();
  }
  return out;
}

} // namespace hamsaz::runtime
