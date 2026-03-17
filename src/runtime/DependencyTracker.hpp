// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "runtime/Operation.hpp"

#include <unordered_set>
#include <vector>

namespace hamsaz::runtime {

class DependencyTracker {
public:
  // Records successful execution of an operation to satisfy future dependencies.
  void record(const Operation& op);

  // Checks whether prerequisites for a dependent operation are met.
  bool canExecute(const Operation& op) const;

  // Store a dependent op until prerequisites are seen.
  void defer(const Operation& op);

  // Returns deferred ops that became ready; removes them from the waiting list.
  std::vector<Operation> takeReady();

  const std::unordered_set<std::string>& seenStudents() const { return seenStudents_; }
  const std::unordered_set<std::string>& seenCourses() const { return seenCourses_; }
  bool hasSeenCourse(const std::string& cid) const { return seenCourses_.count(cid) > 0; }
  void loadSeen(const std::unordered_set<std::string>& students, const std::unordered_set<std::string>& courses);
  void clearDeferred() { deferred_.clear(); }

private:
  std::unordered_set<std::string> seenStudents_;
  std::unordered_set<std::string> seenCourses_;
  std::vector<Operation> deferred_;
};

} // namespace hamsaz::runtime
