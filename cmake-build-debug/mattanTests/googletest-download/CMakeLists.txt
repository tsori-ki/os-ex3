cmake_minimum_required(VERSION 3.5)

project(googletest-download NONE)

include(ExternalProject)
ExternalProject_Add(googletest
  GIT_REPOSITORY    https://github.com/google/googletest.git
  GIT_TAG           main
  SOURCE_DIR        "/Users/br/Desktop/OS_excersizes/os-ex3-1/cmake-build-debug/mattanTests/googletest-src"
  BINARY_DIR        "/Users/br/Desktop/OS_excersizes/os-ex3-1/cmake-build-debug/mattanTests/googletest-build"
  CONFIGURE_COMMAND ""
  BUILD_COMMAND     ""
  INSTALL_COMMAND   ""
  TEST_COMMAND      ""
)
