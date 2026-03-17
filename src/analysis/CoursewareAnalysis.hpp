// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "analysis/Method.hpp"

#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace hamsaz::analysis {

struct CoursewareAnalysis {
  using MethodSet = std::unordered_set<Method>;
  using DependencyMap = std::unordered_map<Method, MethodSet>;
  using ConflictEdges = std::vector<std::pair<Method, Method>>;

  // Hard-coded from POPL'19 Courseware example.
  static const ConflictEdges& conflicts();
  static const DependencyMap& dependencies();
  static const MethodSet& vertexCover();

  static bool isConflicting(Method m);
  static bool hasDependencies(Method m);
};

} // namespace hamsaz::analysis
