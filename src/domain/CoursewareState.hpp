// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <string>
#include <unordered_set>
#include <vector>

namespace hamsaz::domain {

// Minimal sequential model for the Courseware object used in Phase 1.
class CoursewareState {
public:
  using StudentId = std::string;
  using CourseId = std::string;

  // Returns true if the student was newly registered.
  bool registerStudent(const StudentId& sid);

  // Returns true if the course was newly added.
  bool addCourse(const CourseId& cid);

  // Returns true if enrollment succeeded. Fails if student or course is missing.
  bool enroll(const StudentId& sid, const CourseId& cid);

  // Deletes a course and any related enrollments. Returns true if the course existed.
  bool deleteCourse(const CourseId& cid);

  // Removes a specific enrollment; returns true if it existed.
  bool unenroll(const StudentId& sid, const CourseId& cid);

  // Rebuild state from explicit collections (used for snapshots).
  void rebuild(const std::vector<StudentId>& students,
               const std::vector<CourseId>& courses,
               const std::vector<std::pair<StudentId, CourseId>>& enrollments);

  // Simple invariant: every enrollment refers to existing student and course.
  bool satisfiesInvariant() const;

  // Convenience query methods for tests.
  std::size_t studentCount() const { return students_.size(); }
  std::size_t courseCount() const { return courses_.size(); }
  std::size_t enrollmentCount() const { return enrollments_.size(); }

  bool hasStudent(const StudentId& sid) const;
  bool hasCourse(const CourseId& cid) const;
  bool hasEnrollment(const StudentId& sid, const CourseId& cid) const;
  bool courseHasNoEnrollments(const CourseId& cid) const;

  const std::unordered_set<StudentId>& students() const { return students_; }
  const std::unordered_set<CourseId>& courses() const { return courses_; }
  std::vector<std::pair<StudentId, CourseId>> enrollmentList() const;

private:
  static std::string makeEnrollmentKey(const StudentId& sid, const CourseId& cid);

  std::unordered_set<StudentId> students_;
  std::unordered_set<CourseId> courses_;
  std::unordered_set<std::string> enrollments_; // key = sid + '\x1f' + cid
};

} // namespace hamsaz::domain
