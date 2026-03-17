// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "domain/CoursewareState.hpp"
#include "runtime/DependencyTracker.hpp"

#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

namespace hamsaz::common {

struct SnapshotData {
  std::vector<std::string> students;
  std::vector<std::string> courses;
  std::vector<std::pair<std::string, std::string>> enrollments;
  std::unordered_set<std::string> seenStudents;
  std::unordered_set<std::string> seenCourses;
};

std::string encodeSnapshot(const SnapshotData& data);
std::optional<SnapshotData> decodeSnapshot(const std::string& bytes);

} // namespace hamsaz::common
