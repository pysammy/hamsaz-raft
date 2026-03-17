#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "analysis/CoursewareAnalysis.hpp"
#include "runtime/ReplicatedObject.hpp"

using hamsaz::analysis::CoursewareAnalysis;
using hamsaz::analysis::Method;
using hamsaz::runtime::Operation;
using hamsaz::runtime::ReplicatedObject;
using hamsaz::runtime::Route;
using ::testing::Eq;
using ::testing::IsFalse;
using ::testing::IsTrue;

TEST(AnalysisTest, RoutingCategoriesMatchDesign) {
  hamsaz::runtime::OperationRouter router;
  EXPECT_THAT(router.classify(Method::DeleteCourse), Eq(Route::Conflicting));
  EXPECT_THAT(router.classify(Method::Enroll), Eq(Route::Dependent));
  EXPECT_THAT(router.classify(Method::Register), Eq(Route::Independent));
  EXPECT_THAT(router.classify(Method::AddCourse), Eq(Route::Independent));
  EXPECT_THAT(router.classify(Method::Query), Eq(Route::Independent));
}

TEST(RuntimeTest, DependentOperationBlockedUntilPrereqsSeen) {
  ReplicatedObject obj;

  Operation enroll{"op1", Method::Enroll, "alice", "CS101"};
  auto r1 = obj.apply(enroll);
  EXPECT_THAT(r1.ok, IsFalse()); // deps missing
  EXPECT_THAT(r1.message, ::testing::HasSubstr("Deferred"));

  Operation reg{"op2", Method::Register, "alice", ""};
  Operation add{"op3", Method::AddCourse, "CS101", ""};
  EXPECT_THAT(obj.apply(reg).ok, IsTrue());
  EXPECT_THAT(obj.apply(add).ok, IsTrue());

  // The deferred enroll should have been applied once dependencies were satisfied.
  EXPECT_THAT(obj.state().hasEnrollment("alice", "CS101"), IsTrue());
}

TEST(RuntimeTest, ConflictingStillAppliesInPhaseOne) {
  ReplicatedObject obj;
  obj.apply(Operation{"op4", Method::AddCourse, "CS101", ""});
  auto res = obj.apply(Operation{"op5", Method::DeleteCourse, "CS101", ""});
  EXPECT_THAT(res.ok, IsTrue());
  EXPECT_THAT(res.message, ::testing::Eq(""));
  EXPECT_THAT(obj.state().hasCourse("CS101"), IsFalse());
}

TEST(RuntimeTest, MultipleDeferredEnrollsApplyWhenReady) {
  ReplicatedObject obj;
  // Defer two enrollments.
  obj.apply(Operation{"op6", Method::Enroll, "alice", "CS101"});
  obj.apply(Operation{"op7", Method::Enroll, "bob", "CS101"});
  // Satisfy prerequisites.
  obj.apply(Operation{"op8", Method::Register, "alice", ""});
  obj.apply(Operation{"op9", Method::Register, "bob", ""});
  obj.apply(Operation{"op10", Method::AddCourse, "CS101", ""});

  EXPECT_THAT(obj.state().hasEnrollment("alice", "CS101"), IsTrue());
  EXPECT_THAT(obj.state().hasEnrollment("bob", "CS101"), IsTrue());
}

TEST(RuntimeTest, DeferredEnrollFailsIfCourseRemovedBeforeReady) {
  ReplicatedObject obj;
  obj.apply(Operation{"op11", Method::Enroll, "alice", "CS101"}); // deferred
  obj.apply(Operation{"op12", Method::Register, "alice", ""});
  obj.apply(Operation{"op13", Method::AddCourse, "CS101", ""});
  // Remove the course before deferred replay.
  obj.apply(Operation{"op14", Method::DeleteCourse, "CS101", ""});

  // The deferred enroll should not exist; invariant still holds.
  EXPECT_THAT(obj.state().hasEnrollment("alice", "CS101"), IsFalse());
  EXPECT_THAT(obj.state().satisfiesInvariant(), IsTrue());
}
