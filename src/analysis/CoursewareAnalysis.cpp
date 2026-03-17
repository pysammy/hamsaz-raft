// SPDX-License-Identifier: Apache-2.0
#include "analysis/CoursewareAnalysis.hpp"

namespace hamsaz::analysis {

const CoursewareAnalysis::ConflictEdges& CoursewareAnalysis::conflicts() {
  static const ConflictEdges edges = {
      {Method::AddCourse, Method::DeleteCourse},
      {Method::Enroll, Method::DeleteCourse},
      {Method::Unenroll, Method::DeleteCourse},
  };
  return edges;
}

const CoursewareAnalysis::DependencyMap& CoursewareAnalysis::dependencies() {
  static const DependencyMap deps = {
      {Method::Enroll, {Method::Register, Method::AddCourse}},
      {Method::Unenroll, {Method::Register, Method::AddCourse}},
  };
  return deps;
}

const CoursewareAnalysis::MethodSet& CoursewareAnalysis::vertexCover() {
  static const MethodSet cover = {Method::DeleteCourse};
  return cover;
}

bool CoursewareAnalysis::isConflicting(Method m) {
  return vertexCover().count(m) > 0;
}

bool CoursewareAnalysis::hasDependencies(Method m) {
  const auto& deps = dependencies();
  return deps.find(m) != deps.end();
}

} // namespace hamsaz::analysis
