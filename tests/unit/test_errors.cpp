#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "runtime/ReplicatedObject.hpp"

using hamsaz::analysis::Method;
using hamsaz::runtime::Operation;
using hamsaz::runtime::ReplicatedObject;
using ::testing::Eq;
using ::testing::IsFalse;

TEST(ErrorMessagesTest, DuplicateStudent) {
  ReplicatedObject obj;
  obj.apply(Operation{"op1", Method::Register, "alice", ""});
  auto res = obj.apply(Operation{"op2", Method::Register, "alice", ""});
  EXPECT_THAT(res.ok, IsFalse());
  EXPECT_THAT(res.message, Eq("Student already exists"));
}

TEST(ErrorMessagesTest, DuplicateCourse) {
  ReplicatedObject obj;
  obj.apply(Operation{"op1", Method::AddCourse, "CS101", ""});
  auto res = obj.apply(Operation{"op2", Method::AddCourse, "CS101", ""});
  EXPECT_THAT(res.ok, IsFalse());
  EXPECT_THAT(res.message, Eq("Course already exists"));
}

TEST(ErrorMessagesTest, DeleteMissingCourse) {
  ReplicatedObject obj;
  auto res = obj.apply(Operation{"op1", Method::DeleteCourse, "NOPE", ""});
  EXPECT_THAT(res.ok, IsFalse());
  EXPECT_THAT(res.message, Eq("Course not found"));
}

TEST(ErrorMessagesTest, UnenrollNotPresent) {
  ReplicatedObject obj;
  obj.apply(Operation{"op1", Method::Register, "alice", ""});
  obj.apply(Operation{"op2", Method::AddCourse, "CS101", ""});
  auto res = obj.apply(Operation{"op3", Method::Unenroll, "alice", "CS101"});
  EXPECT_THAT(res.ok, IsFalse());
  EXPECT_THAT(res.message, Eq("Unenroll failed (not enrolled)"));
}
