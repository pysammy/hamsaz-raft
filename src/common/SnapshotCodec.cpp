// SPDX-License-Identifier: Apache-2.0
#include "common/SnapshotCodec.hpp"

#include <cstdint>

namespace {

using hamsaz::common::SnapshotData;

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

void appendStringVec(std::string& out, const std::vector<std::string>& vec) {
  appendUint32(out, static_cast<uint32_t>(vec.size()));
  for (const auto& s : vec) {
    appendUint32(out, static_cast<uint32_t>(s.size()));
    out.append(s);
  }
}

bool readStringVec(const std::string& in, size_t& offset, std::vector<std::string>& vec) {
  uint32_t count = 0;
  if (!readUint32(in, offset, count)) return false;
  vec.clear();
  vec.reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    uint32_t len = 0;
    std::string s;
    if (!readUint32(in, offset, len)) return false;
    if (!readBytes(in, offset, len, s)) return false;
    vec.push_back(std::move(s));
  }
  return true;
}

} // namespace

namespace hamsaz::common {

std::string encodeSnapshot(const SnapshotData& data) {
  std::string out;
  appendStringVec(out, data.students);
  appendStringVec(out, data.courses);
  // enrollments
  appendUint32(out, static_cast<uint32_t>(data.enrollments.size()));
  for (const auto& e : data.enrollments) {
    appendUint32(out, static_cast<uint32_t>(e.first.size()));
    out.append(e.first);
    appendUint32(out, static_cast<uint32_t>(e.second.size()));
    out.append(e.second);
  }
  // seen sets
  appendUint32(out, static_cast<uint32_t>(data.seenStudents.size()));
  for (const auto& s : data.seenStudents) {
    appendUint32(out, static_cast<uint32_t>(s.size()));
    out.append(s);
  }
  appendUint32(out, static_cast<uint32_t>(data.seenCourses.size()));
  for (const auto& c : data.seenCourses) {
    appendUint32(out, static_cast<uint32_t>(c.size()));
    out.append(c);
  }
  return out;
}

std::optional<SnapshotData> decodeSnapshot(const std::string& bytes) {
  SnapshotData data;
  size_t offset = 0;
  if (!readStringVec(bytes, offset, data.students)) return std::nullopt;
  if (!readStringVec(bytes, offset, data.courses)) return std::nullopt;
  uint32_t enrollCount = 0;
  if (!readUint32(bytes, offset, enrollCount)) return std::nullopt;
  data.enrollments.reserve(enrollCount);
  for (uint32_t i = 0; i < enrollCount; ++i) {
    uint32_t len1 = 0, len2 = 0;
    std::string s, c;
    if (!readUint32(bytes, offset, len1)) return std::nullopt;
    if (!readBytes(bytes, offset, len1, s)) return std::nullopt;
    if (!readUint32(bytes, offset, len2)) return std::nullopt;
    if (!readBytes(bytes, offset, len2, c)) return std::nullopt;
    data.enrollments.emplace_back(std::move(s), std::move(c));
  }
  uint32_t seenStudentsCount = 0;
  if (!readUint32(bytes, offset, seenStudentsCount)) return std::nullopt;
  for (uint32_t i = 0; i < seenStudentsCount; ++i) {
    uint32_t len = 0;
    std::string s;
    if (!readUint32(bytes, offset, len)) return std::nullopt;
    if (!readBytes(bytes, offset, len, s)) return std::nullopt;
    data.seenStudents.insert(std::move(s));
  }
  uint32_t seenCoursesCount = 0;
  if (!readUint32(bytes, offset, seenCoursesCount)) return std::nullopt;
  for (uint32_t i = 0; i < seenCoursesCount; ++i) {
    uint32_t len = 0;
    std::string c;
    if (!readUint32(bytes, offset, len)) return std::nullopt;
    if (!readBytes(bytes, offset, len, c)) return std::nullopt;
    data.seenCourses.insert(std::move(c));
  }
  return data;
}

} // namespace hamsaz::common
