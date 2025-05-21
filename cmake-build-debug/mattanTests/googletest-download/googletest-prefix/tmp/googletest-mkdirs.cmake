# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file LICENSE.rst or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION ${CMAKE_VERSION}) # this file comes with cmake

# If CMAKE_DISABLE_SOURCE_CHANGES is set to true and the source directory is an
# existing directory in our source tree, calling file(MAKE_DIRECTORY) on it
# would cause a fatal error, even though it would be a no-op.
if(NOT EXISTS "/Users/br/Desktop/OS_excersizes/os-ex3-1/cmake-build-debug/mattanTests/googletest-src")
  file(MAKE_DIRECTORY "/Users/br/Desktop/OS_excersizes/os-ex3-1/cmake-build-debug/mattanTests/googletest-src")
endif()
file(MAKE_DIRECTORY
  "/Users/br/Desktop/OS_excersizes/os-ex3-1/cmake-build-debug/mattanTests/googletest-build"
  "/Users/br/Desktop/OS_excersizes/os-ex3-1/cmake-build-debug/mattanTests/googletest-download/googletest-prefix"
  "/Users/br/Desktop/OS_excersizes/os-ex3-1/cmake-build-debug/mattanTests/googletest-download/googletest-prefix/tmp"
  "/Users/br/Desktop/OS_excersizes/os-ex3-1/cmake-build-debug/mattanTests/googletest-download/googletest-prefix/src/googletest-stamp"
  "/Users/br/Desktop/OS_excersizes/os-ex3-1/cmake-build-debug/mattanTests/googletest-download/googletest-prefix/src"
  "/Users/br/Desktop/OS_excersizes/os-ex3-1/cmake-build-debug/mattanTests/googletest-download/googletest-prefix/src/googletest-stamp"
)

set(configSubDirs )
foreach(subDir IN LISTS configSubDirs)
    file(MAKE_DIRECTORY "/Users/br/Desktop/OS_excersizes/os-ex3-1/cmake-build-debug/mattanTests/googletest-download/googletest-prefix/src/googletest-stamp/${subDir}")
endforeach()
if(cfgdir)
  file(MAKE_DIRECTORY "/Users/br/Desktop/OS_excersizes/os-ex3-1/cmake-build-debug/mattanTests/googletest-download/googletest-prefix/src/googletest-stamp${cfgdir}") # cfgdir has leading slash
endif()
