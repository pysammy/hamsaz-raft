#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common/SnapshotCodec.hpp"
#include "domain/CoursewareState.hpp"
#include "runtime/DependencyTracker.hpp"

using hamsaz::common::SnapshotData;
using hamsaz::common::decodeSnapshot;
using hamsaz::common::encodeSnapshot;
using hamsaz::domain::CoursewareState;
using hamsaz::runtime::DependencyTracker;
using ::testing::ElementsAre;
using ::testing::UnorderedElementsAre;
using ::testing::IsTrue;

TEST(SnapshotCodecTest, RoundTripStateAndSeenSets) {
  SnapshotData data;
  data.students = {"alice", "bob"};
  data.courses = {"CS101"};
  data.enrollments = {{"alice", "CS101"}};
  data.seenStudents = {"alice", "bob"};
  data.seenCourses = {"CS101"};

  auto bytes = encodeSnapshot(data);
  auto decoded = decodeSnapshot(bytes);
  ASSERT_TRUE(decoded.has_value());

  EXPECT_THAT(decoded->students, UnorderedElementsAre("alice", "bob"));
  EXPECT_THAT(decoded->courses, UnorderedElementsAre("CS101"));
  EXPECT_THAT(decoded->enrollments, UnorderedElementsAre(std::pair<std::string,std::string>{"alice", "CS101"}));
  EXPECT_THAT(decoded->seenStudents, UnorderedElementsAre("alice", "bob"));
  EXPECT_THAT(decoded->seenCourses, UnorderedElementsAre("CS101"));
}

TEST(SnapshotCodecTest, ApplyToStateAndTracker) {
  SnapshotData data;
  data.students = {"alice"};
  data.courses = {"CS101"};
  data.enrollments = {{"alice", "CS101"}};
  data.seenStudents = {"alice"};
  data.seenCourses = {"CS101"};

  auto bytes = encodeSnapshot(data);
  auto decoded = decodeSnapshot(bytes);
  ASSERT_TRUE(decoded.has_value());

  CoursewareState st;
  st.rebuild(decoded->students, decoded->courses, decoded->enrollments);
  EXPECT_THAT(st.hasEnrollment("alice", "CS101"), IsTrue());
  EXPECT_THAT(st.satisfiesInvariant(), IsTrue());

  DependencyTracker tracker;
  tracker.loadSeen(decoded->seenStudents, decoded->seenCourses);
  EXPECT_THAT(tracker.canExecute({.op_id="x", .method=hamsaz::analysis::Method::Enroll, .arg1="alice", .arg2="CS101"}), IsTrue());
}
