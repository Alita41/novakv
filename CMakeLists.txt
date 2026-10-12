cmake_minimum_required(VERSION 3.16)
project(novakv VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(novakv_core
    src/store.cpp
    src/lru_cache.cpp
    src/wal.cpp
)
target_include_directories(novakv_core PUBLIC include)
target_compile_options(novakv_core PRIVATE -Wall -Wextra -Wpedantic)

add_executable(novakv-server src/server.cpp)
target_link_libraries(novakv-server PRIVATE novakv_core pthread)
target_compile_options(novakv-server PRIVATE -Wall -Wextra -Wpedantic)

add_executable(novakv-unit-tests tests/test_core.cpp)
target_link_libraries(novakv-unit-tests PRIVATE novakv_core pthread)
# Keep assertions active in test code even with Release/-DNDEBUG builds.
if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(novakv-unit-tests PRIVATE -UNDEBUG)
endif()

enable_testing()
add_test(NAME novakv_unit COMMAND novakv-unit-tests)
