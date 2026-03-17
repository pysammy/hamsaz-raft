// SPDX-License-Identifier: Apache-2.0
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <cstdint>

#include "analysis/Method.hpp"
#include "runtime/ReplicatedObject.hpp"

using hamsaz::analysis::Method;
using hamsaz::analysis::methodFromString;
using hamsaz::runtime::Operation;
using hamsaz::runtime::ReplicatedObject;

namespace {

std::vector<std::string> splitWords(const std::string& line) {
  std::istringstream iss(line);
  std::vector<std::string> parts;
  std::string w;
  while (iss >> w) parts.push_back(w);
  return parts;
}

void printState(const ReplicatedObject& obj) {
  const auto& st = obj.state();
  std::cout << "Students (" << st.studentCount() << "):";
  for (const auto& s : st.students()) std::cout << " " << s;
  std::cout << "\nCourses (" << st.courseCount() << "):";
  for (const auto& c : st.courses()) std::cout << " " << c;
  std::cout << "\nEnrollments (" << st.enrollmentCount() << "):";
  for (const auto& e : st.enrollmentList()) std::cout << " (" << e.first << "," << e.second << ")";
  std::cout << "\n";
}

void printHelp() {
  std::cout << "Commands:\n"
            << "  register <student>\n"
            << "  add <course>\n"
            << "  enroll <student> <course>\n"
            << "  unenroll <student> <course>\n"
            << "  delete <course>\n"
            << "  query\n"
            << "  help\n"
            << "  exit\n";
}

} // namespace

int main() {
  ReplicatedObject obj;
  std::cout << "Hamsaz single-node CLI (Phase 1). Type 'help' for commands.\n";
  printHelp();

  std::string line;
  while (std::cout << "> " && std::getline(std::cin, line)) {
    auto parts = splitWords(line);
    if (parts.empty()) continue;
    const auto& cmd = parts[0];
    if (cmd == "exit" || cmd == "quit") break;
    if (cmd == "help") {
      printHelp();
      continue;
    }
    if (cmd == "query") {
      printState(obj);
      continue;
    }

    Operation op;
    static uint64_t counter = 0;
    auto nextId = [&]() { return "cli-op-" + std::to_string(++counter); };

    if (cmd == "register" && parts.size() == 2) {
      op = {nextId(), Method::Register, parts[1], ""};
    } else if (cmd == "add" && parts.size() == 2) {
      op = {nextId(), Method::AddCourse, parts[1], ""};
    } else if (cmd == "enroll" && parts.size() == 3) {
      op = {nextId(), Method::Enroll, parts[1], parts[2]};
    } else if (cmd == "unenroll" && parts.size() == 3) {
      op = {nextId(), Method::Unenroll, parts[1], parts[2]};
    } else if (cmd == "delete" && parts.size() == 2) {
      op = {nextId(), Method::DeleteCourse, parts[1], ""};
    } else {
      std::cout << "Unrecognized command or wrong arity. Type 'help'.\n";
      continue;
    }

    auto result = obj.apply(op);
    if (result.ok) {
      std::cout << "OK";
    } else {
      std::cout << "FAIL: " << result.message;
    }
    std::cout << "\n";
  }
  return 0;
}
