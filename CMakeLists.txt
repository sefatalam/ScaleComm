cmake_minimum_required(VERSION 3.7)
project(ScaleCommBenchmark CXX)
set(CMAKE_CXX_STANDARD 14)
add_subdirectory(lib)
add_executable(ScaleCommBenchmark mpi.cpp lib/ScaleComm.hpp)
add_executable(server server.cpp)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0")
find_package(PkgConfig REQUIRED)
include_directories(/usr/local/include)
pkg_check_modules(thallium REQUIRED IMPORTED_TARGET thallium)
find_package(MPI REQUIRED) 
target_link_libraries(ScaleCommBenchmark 
   MPI::MPI_CXX
   ScaleComm
)
target_link_libraries(server PkgConfig::thallium)