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
  Operation op{"op-1", Method::Enroll, "alice", "CS101", {"p1", "p2"}};
  auto bytes = encodeOperation(op);
  auto decoded = decodeOperation(bytes);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_THAT(decoded->op_id, Eq("op-1"));
  EXPECT_THAT(decoded->method, Eq(Method::Enroll));
  EXPECT_THAT(decoded->arg1, Eq("alice"));
  EXPECT_THAT(decoded->arg2, Eq("CS101"));
  ASSERT_THAT(decoded->prereq_op_ids.size(), Eq(2u));
  EXPECT_THAT(decoded->prereq_op_ids[0], Eq("p1"));
  EXPECT_THAT(decoded->prereq_op_ids[1], Eq("p2"));
}

TEST(OperationCodecTest, InvalidBufferFailsToDecode) {
  std::string bad = "\x00\x00"; // too short
  auto decoded = decodeOperation(bad);
  EXPECT_FALSE(decoded.has_value());
}
