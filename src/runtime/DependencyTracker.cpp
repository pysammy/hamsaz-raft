// SPDX-License-Identifier: Apache-2.0
#include "runtime/DependencyTracker.hpp"

namespace hamsaz::runtime {

void DependencyTracker::record(const Operation& op) {
  using analysis::Method;
  switch (op.method) {
    case Method::Register:
      seenStudents_.insert(op.arg1);
      break;
    case Method::AddCourse:
      seenCourses_.insert(op.arg1);
      break;
    case Method::DeleteCourse:
      seenCourses_.erase(op.arg1);
      break;
    default:
      break;
  }
}

bool DependencyTracker::canExecute(const Operation& op) const {
  using analysis::Method;
  if (op.method == Method::Enroll) {
    return seenStudents_.count(op.arg1) > 0 && seenCourses_.count(op.arg2) > 0;
  }
  if (op.method == Method::Unenroll) {
    return seenStudents_.count(op.arg1) > 0 && seenCourses_.count(op.arg2) > 0;
  }
  return true;
}

void DependencyTracker::defer(const Operation& op) {
  deferred_.push_back(op);
}

std::vector<Operation> DependencyTracker::takeReady() {
  std::vector<Operation> ready;
  std::vector<Operation> stillWaiting;
  ready.reserve(deferred_.size());
  for (const auto& op : deferred_) {
    if (canExecute(op)) {
      ready.push_back(op);
    } else {
      stillWaiting.push_back(op);
    }
  }
  deferred_.swap(stillWaiting);
  return ready;
}

void DependencyTracker::loadSeen(const std::unordered_set<std::string>& students,
                                 const std::unordered_set<std::string>& courses) {
  seenStudents_ = students;
  seenCourses_ = courses;
}

} // namespace hamsaz::runtime
