#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "runtime/ReplicatedObject.hpp"

using hamsaz::analysis::Method;
using hamsaz::runtime::Operation;
using hamsaz::runtime::ReplicatedObject;
using ::testing::Eq;
using ::testing::IsTrue;

// An end-to-end happy path exercising deferral, conflict, and invariant checks.
TEST(IntegrationTest, CourseLifecycle) {
  ReplicatedObject obj;

  // Enroll arrives first: should defer.
  auto r1 = obj.apply(Operation{"op1", Method::Enroll, "alice", "CS101"});
  EXPECT_FALSE(r1.ok);

  // Satisfy dependencies.
  EXPECT_TRUE(obj.apply(Operation{"op2", Method::Register, "alice", ""}).ok);
  EXPECT_TRUE(obj.apply(Operation{"op3", Method::AddCourse, "CS101", ""}).ok);

  // Deferred op should now apply automatically.
  EXPECT_THAT(obj.state().hasEnrollment("alice", "CS101"), IsTrue());

  // Delete course (conflicting op) should clean enrollments.
  auto r4 = obj.apply(Operation{"op4", Method::DeleteCourse, "CS101", ""});
  EXPECT_TRUE(r4.ok);
  EXPECT_THAT(obj.state().hasEnrollment("alice", "CS101"), Eq(false));
  EXPECT_THAT(obj.state().satisfiesInvariant(), IsTrue());
}
