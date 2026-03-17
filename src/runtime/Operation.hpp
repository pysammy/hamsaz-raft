// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "analysis/Method.hpp"

#include <string>

namespace hamsaz::runtime {

enum class Route { Conflicting, Dependent, Independent };

struct Operation {
  std::string op_id; // unique identifier for deduplication/replay safety
  analysis::Method method{analysis::Method::Unknown};
  std::string arg1; // sid or cid depending on method
  std::string arg2; // optional second argument (cid for enroll)
};

struct OperationResult {
  bool ok{false};
  std::string message;
};

} // namespace hamsaz::runtime
