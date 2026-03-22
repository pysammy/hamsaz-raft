#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common/SnapshotCodec.hpp"
#include "runtime/ReplicatedObject.hpp"

using hamsaz::analysis::Method;
using hamsaz::common::SnapshotData;
using hamsaz::runtime::Operation;
using hamsaz::runtime::ReplicatedObject;
using ::testing::Eq;
using ::testing::IsFalse;
using ::testing::IsTrue;

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

TEST(ErrorMessagesTest, FailedApplyDoesNotMutateState) {
  ReplicatedObject obj;

  // Inject an invalid snapshot to force invariant failure on unrelated writes.
  SnapshotData bad;
  bad.students = {"alice"};
  bad.enrollments = {{"alice", "CS999"}};
  obj.restoreSnapshot(bad);
  ASSERT_THAT(obj.state().satisfiesInvariant(), IsFalse());

  auto before = obj.createSnapshot();
  auto res = obj.apply(Operation{"op4", Method::Register, "bob", ""});
  EXPECT_THAT(res.ok, IsFalse());
  EXPECT_THAT(res.message, Eq("Invariant violated after apply"));

  auto after = obj.createSnapshot();
  EXPECT_THAT(obj.state().hasStudent("bob"), IsFalse());
  EXPECT_THAT(after.students.size(), Eq(before.students.size()));
  EXPECT_THAT(after.courses.size(), Eq(before.courses.size()));
  EXPECT_THAT(after.enrollments.size(), Eq(before.enrollments.size()));
}
