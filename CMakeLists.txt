cmake_minimum_required(VERSION 3.16)
project(hamsaz_raft LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(HAMSAZ_BUILD_TESTS "Build unit tests" ON)

if (MSVC)
  add_compile_options(/W4 /permissive- /utf-8)
else()
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# ---- Core library ----
add_library(hamsaz_core
  src/domain/CoursewareState.cpp
  src/analysis/CoursewareAnalysis.cpp
  src/runtime/OperationRouter.cpp
  src/runtime/DependencyTracker.cpp
  src/runtime/ReplicatedObject.cpp
  src/common/OperationCodec.cpp
  src/common/SnapshotCodec.cpp
  src/runtime/GossipEngine.cpp
  src/runtime/GossipHub.cpp
)
target_include_directories(hamsaz_core PUBLIC ${PROJECT_SOURCE_DIR}/src)

# ---- Tests ----
if (HAMSAZ_BUILD_TESTS)
  include(FetchContent)
  FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
  )
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)

  enable_testing()

add_executable(unit_tests
  tests/unit/test_courseware_state.cpp
  tests/unit/test_analysis_runtime.cpp
  tests/unit/test_operation_codec.cpp
  tests/unit/test_snapshot_codec.cpp
  tests/unit/test_errors.cpp
  tests/integration/test_end_to_end.cpp
)
  target_link_libraries(unit_tests PRIVATE hamsaz_core GTest::gtest_main GTest::gmock_main)

  add_executable(gossip_simulator tests/bench/gossip_simulator.cpp)
  target_link_libraries(gossip_simulator PRIVATE hamsaz_core)

  include(GoogleTest)
  gtest_discover_tests(unit_tests)
endif()

# ---- CLI driver (single-node) ----
add_executable(hamsaz_cli src/main/cli.cpp)
target_link_libraries(hamsaz_cli PRIVATE hamsaz_core)

option(HAMSAZ_WITH_NURAFT "Enable NuRaft integration (Phase 2)" OFF)
option(HAMSAZ_BUILD_RAFT_HARNESS "Build Raft harness demo (requires NuRaft)" OFF)
option(HAMSAZ_NURAFT_LOW_MEM_BUILD "Use lower-memory compile flags for NuRaft core objects" OFF)

if (HAMSAZ_WITH_NURAFT)
  include(FetchContent)
  # Avoid building NuRaft's own test binaries in Docker/K8s images.
  set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
  set(NURAFT_BUILD_TESTS OFF CACHE BOOL "" FORCE)
  FetchContent_Declare(
    nuraft
    GIT_REPOSITORY https://github.com/eBay/NuRaft.git
    GIT_TAG master
  )
  FetchContent_MakeAvailable(nuraft)
  find_package(OpenSSL REQUIRED)

  if (HAMSAZ_NURAFT_LOW_MEM_BUILD)
    if (TARGET RAFT_CORE_OBJ)
      # NuRaft's asio_service.cxx can be memory-heavy to compile in constrained Docker VMs.
      # Append low-memory flags after NuRaft's defaults so they take precedence.
      target_compile_options(RAFT_CORE_OBJ PRIVATE
        -g0
        -O0
        -fno-var-tracking
        -fno-inline-functions
      )
    endif()
  endif()

  add_library(hamsaz_raft
    src/raft/StateMachine.cpp
    src/raft/RaftNode.cpp
    ${nuraft_SOURCE_DIR}/examples/in_memory_log_store.cxx
  )
  target_compile_definitions(hamsaz_raft PRIVATE HAMSAZ_WITH_NURAFT=1)
  target_include_directories(hamsaz_raft PRIVATE
    ${nuraft_SOURCE_DIR}/include
    ${nuraft_SOURCE_DIR}/include/libnuraft
    ${nuraft_SOURCE_DIR}/examples)
  if (TARGET NuRaft::static_lib)
    set(HAMSAZ_NURAFT_LIB NuRaft::static_lib)
  elseif (TARGET static_lib)
    set(HAMSAZ_NURAFT_LIB static_lib)
  else()
    # Backward compatibility for older NuRaft layouts.
    set(HAMSAZ_NURAFT_LIB ${nuraft_BINARY_DIR}/libnuraft.a)
  endif()
  target_link_libraries(hamsaz_raft PUBLIC hamsaz_core ${HAMSAZ_NURAFT_LIB} OpenSSL::SSL OpenSSL::Crypto)

  if (HAMSAZ_BUILD_RAFT_HARNESS)
    add_executable(raft_harness_demo tests/raft/harness_demo.cpp)
    target_link_libraries(raft_harness_demo PRIVATE hamsaz_core hamsaz_raft OpenSSL::SSL OpenSSL::Crypto)
    target_include_directories(raft_harness_demo PRIVATE ${nuraft_SOURCE_DIR}/include)
    target_compile_definitions(raft_harness_demo PRIVATE HAMSAZ_WITH_NURAFT=1)

    add_executable(raft_harness_failover tests/raft/harness_failover.cpp)
    target_link_libraries(raft_harness_failover PRIVATE hamsaz_core hamsaz_raft OpenSSL::SSL OpenSSL::Crypto)
    target_include_directories(raft_harness_failover PRIVATE ${nuraft_SOURCE_DIR}/include)
    target_compile_definitions(raft_harness_failover PRIVATE HAMSAZ_WITH_NURAFT=1)
  endif()

  # Manual multi-process Raft node CLI
  add_executable(raft_node_cli src/main/raft_node_cli.cpp)
  target_link_libraries(raft_node_cli PRIVATE hamsaz_raft OpenSSL::SSL OpenSSL::Crypto)
  target_include_directories(raft_node_cli PRIVATE ${nuraft_SOURCE_DIR}/include ${nuraft_SOURCE_DIR}/include/libnuraft ${nuraft_SOURCE_DIR}/examples)
  target_compile_definitions(raft_node_cli PRIVATE HAMSAZ_WITH_NURAFT=1)

  # Networked Raft node server for true multi-client benchmarks.
  add_executable(raft_node_server src/main/raft_node_server.cpp)
  target_link_libraries(raft_node_server PRIVATE hamsaz_raft OpenSSL::SSL OpenSSL::Crypto)
  target_include_directories(raft_node_server PRIVATE ${nuraft_SOURCE_DIR}/include ${nuraft_SOURCE_DIR}/include/libnuraft ${nuraft_SOURCE_DIR}/examples)
  target_compile_definitions(raft_node_server PRIVATE HAMSAZ_WITH_NURAFT=1)
endif()
