// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <string>

namespace hamsaz::analysis {

enum class Method {
  Register,
  AddCourse,
  Enroll,
  DeleteCourse,
  Query,
  Unenroll,
  Unknown,
};

inline std::string toString(Method m) {
  switch (m) {
    case Method::Register: return "register";
    case Method::AddCourse: return "addCourse";
    case Method::Enroll: return "enroll";
    case Method::DeleteCourse: return "deleteCourse";
    case Method::Query: return "query";
    case Method::Unenroll: return "unenroll";
    default: return "unknown";
  }
}

inline Method methodFromString(const std::string& name) {
  if (name == "register") return Method::Register;
  if (name == "addCourse") return Method::AddCourse;
  if (name == "enroll") return Method::Enroll;
  if (name == "deleteCourse") return Method::DeleteCourse;
  if (name == "query") return Method::Query;
  if (name == "unenroll") return Method::Unenroll;
  return Method::Unknown;
}

} // namespace hamsaz::analysis
