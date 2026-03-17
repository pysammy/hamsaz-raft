// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "runtime/Operation.hpp"

#include <optional>
#include <string>

namespace hamsaz::common {

// Simple length-prefixed binary encoding for Operation.
// Format: [u32 id_len][id_bytes][u8 method][u32 arg1_len][arg1][u32 arg2_len][arg2]
std::string encodeOperation(const runtime::Operation& op);

std::optional<runtime::Operation> decodeOperation(const std::string& bytes);

} // namespace hamsaz::common
