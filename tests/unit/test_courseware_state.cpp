#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "domain/CoursewareState.hpp"

using namespace hamsaz::domain;
using ::testing::Eq;
using ::testing::IsTrue;
using ::testing::IsFalse;

TEST(CoursewareStateTest, StartsEmptyAndInvariantHolds) {
  CoursewareState state;
  EXPECT_THAT(state.studentCount(), Eq(0));
  EXPECT_THAT(state.courseCount(), Eq(0));
  EXPECT_THAT(state.enrollmentCount(), Eq(0));
  EXPECT_THAT(state.satisfiesInvariant(), IsTrue());
}

TEST(CoursewareStateTest, RegisterAddEnrollAndDeleteCourse) {
  CoursewareState state;

  EXPECT_THAT(state.registerStudent("alice"), IsTrue());
  EXPECT_THAT(state.addCourse("CS101"), IsTrue());
  EXPECT_THAT(state.enroll("alice", "CS101"), IsTrue());

  EXPECT_THAT(state.hasEnrollment("alice", "CS101"), IsTrue());
  EXPECT_THAT(state.satisfiesInvariant(), IsTrue());

  EXPECT_THAT(state.deleteCourse("CS101"), IsTrue());
  EXPECT_THAT(state.hasCourse("CS101"), IsFalse());
  EXPECT_THAT(state.hasEnrollment("alice", "CS101"), IsFalse());
  EXPECT_THAT(state.satisfiesInvariant(), IsTrue());
}

TEST(CoursewareStateTest, RejectsEnrollWithoutPrerequisites) {
  CoursewareState state;
  state.registerStudent("alice");
  // Missing course should block enrollment.
  EXPECT_THAT(state.enroll("alice", "CS101"), IsFalse());
  EXPECT_THAT(state.enrollmentCount(), Eq(0));
  EXPECT_THAT(state.satisfiesInvariant(), IsTrue());
}

TEST(CoursewareStateTest, DuplicateEntitiesDoNotBreakInvariant) {
  CoursewareState state;
  EXPECT_THAT(state.registerStudent("alice"), IsTrue());
  EXPECT_THAT(state.registerStudent("alice"), IsFalse()); // duplicate
  EXPECT_THAT(state.addCourse("CS101"), IsTrue());
  EXPECT_THAT(state.addCourse("CS101"), IsFalse()); // duplicate
  EXPECT_THAT(state.studentCount(), Eq(1));
  EXPECT_THAT(state.courseCount(), Eq(1));
  EXPECT_THAT(state.satisfiesInvariant(), IsTrue());
}

TEST(CoursewareStateTest, EnrollAfterCourseDeletionFails) {
  CoursewareState state;
  state.registerStudent("alice");
  state.addCourse("CS101");
  EXPECT_THAT(state.enroll("alice", "CS101"), IsTrue());
  EXPECT_THAT(state.deleteCourse("CS101"), IsTrue());
  EXPECT_THAT(state.enroll("alice", "CS101"), IsFalse());
  EXPECT_THAT(state.hasEnrollment("alice", "CS101"), IsFalse());
  EXPECT_THAT(state.satisfiesInvariant(), IsTrue());
}
