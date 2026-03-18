// SPDX-License-Identifier: Apache-2.0
#include "common/OperationCodec.hpp"

#include <cstdint>
#include <cstring>
#include <stdexcept>

namespace {

using hamsaz::analysis::Method;

void appendUint32(std::string& out, uint32_t v) {
  char buf[4];
  buf[0] = static_cast<char>((v >> 24) & 0xFF);
  buf[1] = static_cast<char>((v >> 16) & 0xFF);
  buf[2] = static_cast<char>((v >> 8) & 0xFF);
  buf[3] = static_cast<char>(v & 0xFF);
  out.append(buf, 4);
}

bool readUint32(const std::string& in, size_t& offset, uint32_t& value) {
  if (offset + 4 > in.size()) return false;
  value = (static_cast<uint8_t>(in[offset]) << 24) |
          (static_cast<uint8_t>(in[offset + 1]) << 16) |
          (static_cast<uint8_t>(in[offset + 2]) << 8) |
          static_cast<uint8_t>(in[offset + 3]);
  offset += 4;
  return true;
}

bool readBytes(const std::string& in, size_t& offset, uint32_t len, std::string& out) {
  if (offset + len > in.size()) return false;
  out.assign(in.data() + offset, len);
  offset += len;
  return true;
}

} // namespace

namespace hamsaz::common {

std::string encodeOperation(const runtime::Operation& op) {
  std::string out;
  out.reserve(1 + op.op_id.size() + op.arg1.size() + op.arg2.size() + 12);
  appendUint32(out, static_cast<uint32_t>(op.op_id.size()));
  out.append(op.op_id);
  out.push_back(static_cast<char>(op.method));
  appendUint32(out, static_cast<uint32_t>(op.arg1.size()));
  out.append(op.arg1);
  appendUint32(out, static_cast<uint32_t>(op.arg2.size()));
  out.append(op.arg2);
  appendUint32(out, static_cast<uint32_t>(op.prereq_op_ids.size()));
  for (const auto& prereq : op.prereq_op_ids) {
    appendUint32(out, static_cast<uint32_t>(prereq.size()));
    out.append(prereq);
  }
  return out;
}

std::optional<hamsaz::runtime::Operation> decodeOperation(const std::string& bytes) {
  size_t offset = 0;
  uint32_t len = 0;
  hamsaz::runtime::Operation op;
  if (!readUint32(bytes, offset, len)) return std::nullopt;
  if (!readBytes(bytes, offset, len, op.op_id)) return std::nullopt;
  if (offset >= bytes.size()) return std::nullopt;
  op.method = static_cast<Method>(static_cast<uint8_t>(bytes[offset]));
  offset += 1;
  if (!readUint32(bytes, offset, len)) return std::nullopt;
  if (!readBytes(bytes, offset, len, op.arg1)) return std::nullopt;
  if (!readUint32(bytes, offset, len)) return std::nullopt;
  if (!readBytes(bytes, offset, len, op.arg2)) return std::nullopt;
  // Backward-compatible tail decode: prereqs are optional.
  if (offset == bytes.size()) return op;
  uint32_t prereq_count = 0;
  if (!readUint32(bytes, offset, prereq_count)) return std::nullopt;
  op.prereq_op_ids.reserve(prereq_count);
  for (uint32_t i = 0; i < prereq_count; ++i) {
    if (!readUint32(bytes, offset, len)) return std::nullopt;
    std::string prereq;
    if (!readBytes(bytes, offset, len, prereq)) return std::nullopt;
    op.prereq_op_ids.push_back(std::move(prereq));
  }
  if (offset != bytes.size()) return std::nullopt;
  return op;
}

} // namespace hamsaz::common
