# IMLane_CMake
enable_language(C CXX)
find_package(Arrow REQUIRED)
find_package(Boost REQUIRED)
# set(CONCURRENTQUEUE_INCLUDE_DIR "/workspace/deploy/duckdb/third_party/concurrentqueue")
set(CONCURRENTQUEUE_INCLUDE_DIR "/workspace/concurrentqueue")


execute_process(
  COMMAND "python" -c "import pyarrow; print(pyarrow.get_include())"
  OUTPUT_VARIABLE PYARROW_INCLUDE_DIR
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process(
  COMMAND "python" -c "import pyarrow; print(pyarrow.get_library_dirs()[0])"
  OUTPUT_VARIABLE PYARROW_LIB_DIR
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

set(CMAKE_CXX_STANDARD 14)

message(STATUS "Boost_INCLUDE_DIRS: ${Boost_INCLUDE_DIRS}")
message(STATUS "CONCURRENTQUEUE_INCLUDE_DIR: ${CONCURRENTQUEUE_INCLUDE_DIR}")
message(STATUS "PYARROW_INCLUDE_DIR: ${PYARROW_INCLUDE_DIR}")
message(STATUS "PYARROW_LIB_DIR: ${PYARROW_LIB_DIR}")


add_library(steve INTERFACE)
target_include_directories(
  steve INTERFACE
  ${Arrow_INCLUDE_DIRS}
  ${PYARROW_INCLUDE_DIR}
  ${Boost_INCLUDE_DIRS}
  ${CONCURRENTQUEUE_INCLUDE_DIR})
target_link_libraries(steve INTERFACE arrow_shared rt)