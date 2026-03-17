#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common/OperationCodec.hpp"

using hamsaz::analysis::Method;
using hamsaz::common::decodeOperation;
using hamsaz::common::encodeOperation;
using hamsaz::runtime::Operation;
using ::testing::Eq;
using ::testing::Not;

TEST(OperationCodecTest, RoundTripSimple) {
  Operation op{"op-1", Method::Enroll, "alice", "CS101"};
  auto bytes = encodeOperation(op);
  auto decoded = decodeOperation(bytes);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_THAT(decoded->op_id, Eq("op-1"));
  EXPECT_THAT(decoded->method, Eq(Method::Enroll));
  EXPECT_THAT(decoded->arg1, Eq("alice"));
  EXPECT_THAT(decoded->arg2, Eq("CS101"));
}

TEST(OperationCodecTest, InvalidBufferFailsToDecode) {
  std::string bad = "\x00\x00"; // too short
  auto decoded = decodeOperation(bad);
  EXPECT_FALSE(decoded.has_value());
}
