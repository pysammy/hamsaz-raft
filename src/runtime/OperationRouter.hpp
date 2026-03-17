// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "analysis/CoursewareAnalysis.hpp"
#include "runtime/Operation.hpp"

namespace hamsaz::runtime {

class OperationRouter {
public:
  Route classify(analysis::Method method) const;
};

} // namespace hamsaz::runtime
