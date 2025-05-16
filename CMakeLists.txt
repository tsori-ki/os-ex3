cmake_minimum_required(VERSION 3.5)
project(barrierdemo)

# Use C++11 or newer
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Tell CMake where to find your headers (optional if headers are in the same dir)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# Define the executable and its sources
add_executable(barrierdemo
    Barrier.cpp
    barrierdemo.cpp
)

# Find & link the Threads (pthreads) library
find_package(Threads REQUIRED)
target_link_libraries(barrierdemo PRIVATE Threads::Threads)
