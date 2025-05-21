# Minimum CMake version required
cmake_minimum_required(VERSION 3.1)

# Project configuration
project(ex3 VERSION 1.0 LANGUAGES C CXX)

# Ensure system has pthreads
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# MapReduceFramework library
add_library(MapReduceFramework
        MapReduceClient.h
        MapReduceFramework.cpp MapReduceFramework.h
        Barrier.cpp Barrier.h
)

set_property(TARGET MapReduceFramework PROPERTY CXX_STANDARD 11)
target_compile_options(MapReduceFramework PUBLIC -Wall -Wextra)
target_include_directories(MapReduceFramework PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(MapReduceFramework PUBLIC Threads::Threads)

# Test 1: test1-1_thread_1_process
add_executable(test1 test1-1_thread_1_process.cpp)
target_link_libraries(test1 PRIVATE MapReduceFramework)

# Test 4: test4-1_thread_4_process
add_executable(test4 test4-1_thread_4_process.cpp)
target_link_libraries(test4 PRIVATE MapReduceFramework)

# Sample Client
add_executable(SampleClient SampleClient.cpp)
target_link_libraries(SampleClient PRIVATE MapReduceFramework)