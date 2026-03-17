// SPDX-License-Identifier: Apache-2.0
#include "domain/CoursewareState.hpp"

#include <utility>

namespace hamsaz::domain {

std::string CoursewareState::makeEnrollmentKey(const StudentId& sid, const CourseId& cid) {
  constexpr char separator = '\x1f'; // unlikely to appear in IDs
  return sid + separator + cid;
}

bool CoursewareState::registerStudent(const StudentId& sid) {
  return students_.insert(sid).second;
}

bool CoursewareState::addCourse(const CourseId& cid) {
  return courses_.insert(cid).second;
}

bool CoursewareState::enroll(const StudentId& sid, const CourseId& cid) {
  if (!hasStudent(sid) || !hasCourse(cid)) {
    return false;
  }
  auto key = makeEnrollmentKey(sid, cid);
  auto inserted = enrollments_.insert(std::move(key)).second;
  return inserted && satisfiesInvariant();
}

bool CoursewareState::deleteCourse(const CourseId& cid) {
  auto erased = courses_.erase(cid);
  if (erased == 0) {
    return false;
  }
  // Remove related enrollments.
  for (auto it = enrollments_.begin(); it != enrollments_.end();) {
    const auto& key = *it;
    auto sep = key.find('\x1f');
    if (sep != std::string::npos) {
      const auto& coursePart = key.substr(sep + 1);
      if (coursePart == cid) {
        it = enrollments_.erase(it);
        continue;
      }
    }
    ++it;
  }
  return satisfiesInvariant();
}

bool CoursewareState::unenroll(const StudentId& sid, const CourseId& cid) {
  auto key = makeEnrollmentKey(sid, cid);
  auto erased = enrollments_.erase(key);
  return erased > 0 && satisfiesInvariant();
}

void CoursewareState::rebuild(const std::vector<StudentId>& students,
                              const std::vector<CourseId>& courses,
                              const std::vector<std::pair<StudentId, CourseId>>& enrollments) {
  students_.clear();
  courses_.clear();
  enrollments_.clear();
  for (const auto& s : students) {
    students_.insert(s);
  }
  for (const auto& c : courses) {
    courses_.insert(c);
  }
  for (const auto& e : enrollments) {
    enrollments_.insert(makeEnrollmentKey(e.first, e.second));
  }
  // Defensive check
  satisfiesInvariant();
}

bool CoursewareState::satisfiesInvariant() const {
  for (const auto& key : enrollments_) {
    auto sep = key.find('\x1f');
    if (sep == std::string::npos) return false;
    auto sid = key.substr(0, sep);
    auto cid = key.substr(sep + 1);
    if (!hasStudent(sid) || !hasCourse(cid)) {
      return false;
    }
  }
  return true;
}

bool CoursewareState::hasStudent(const StudentId& sid) const {
  return students_.find(sid) != students_.end();
}

bool CoursewareState::hasCourse(const CourseId& cid) const {
  return courses_.find(cid) != courses_.end();
}

bool CoursewareState::hasEnrollment(const StudentId& sid, const CourseId& cid) const {
  return enrollments_.find(makeEnrollmentKey(sid, cid)) != enrollments_.end();
}

bool CoursewareState::courseHasNoEnrollments(const CourseId& cid) const {
  // Early-out if course is absent.
  if (!hasCourse(cid)) return false;
  for (const auto& key : enrollments_) {
    auto sep = key.find('\x1f');
    if (sep == std::string::npos) continue;
    const auto& coursePart = key.substr(sep + 1);
    if (coursePart == cid) {
      return false;
    }
  }
  return true;
}

std::vector<std::pair<CoursewareState::StudentId, CoursewareState::CourseId>>
CoursewareState::enrollmentList() const {
  std::vector<std::pair<StudentId, CourseId>> result;
  result.reserve(enrollments_.size());
  for (const auto& key : enrollments_) {
    auto sep = key.find('\x1f');
    if (sep == std::string::npos) continue;
    result.emplace_back(key.substr(0, sep), key.substr(sep + 1));
  }
  return result;
}

} // namespace hamsaz::domain
