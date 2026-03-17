// SPDX-License-Identifier: Apache-2.0
#include "runtime/OperationRouter.hpp"

namespace hamsaz::runtime {

Route OperationRouter::classify(analysis::Method method) const {
  if (analysis::CoursewareAnalysis::isConflicting(method)) {
    return Route::Conflicting;
  }
  if (analysis::CoursewareAnalysis::hasDependencies(method)) {
    return Route::Dependent;
  }
  return Route::Independent;
}

} // namespace hamsaz::runtime
