include( ExternalProject )
include(FetchContent)

cmake_minimum_required(VERSION 3.11)
set(DEFAULT_BUILD_TYPE "Release")
option(BUILD_SHARED_LIBS "Build using shared libraries" ON)

# This ensures that things like gnu++11 get passed correctly
if(NOT DEFINED CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 17)
endif()

if(NOT DEFINED TARGET)
  set(TARGET DeltaSharingClient)
endif()


if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to '${DEFAULT_BUILD_TYPE}' as none was specified.")
  set(CMAKE_BUILD_TYPE "${DEFAULT_BUILD_TYPE}" CACHE STRING "Choose the type of build." FORCE)
  # Set the possible values of build type for cmake-gui
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif()



# Google TEST
FetchContent_Declare(googletest
  GIT_REPOSITORY https://github.com/google/googletest
  GIT_TAG release-1.12.1
)

FetchContent_MakeAvailable(googletest)

ExternalProject_Add(restclient
  GIT_REPOSITORY https://github.com/mrtazz/restclient-cpp
  SOURCE_DIR restclient
  BUILD_IN_SOURCE 1
  UPDATE_COMMAND ""
  PATCH_COMMAND ""
  CONFIGURE_COMMAND ./autogen.sh && ./configure
  BUILD_COMMAND make
  # INSTALL_COMMAND make install
  INSTALL_COMMAND sudo make install
)



if(NOT DEFINED PROJECT_VERSION)
    set(PROJECT_VERSION 0)
endif()
# We require a C++11 compliant compiler
set(CMAKE_CXX_STANDARD_REQUIRED ON)
list(APPEND CMAKE_MODULE_PATH "/opt/homebrew/Cellar/apache-arrow/8.0.0_4/lib/cmake/arrow/")
# set the project name
project(${TARGET} VERSION 0.0.1 DESCRIPTION "DeltaSharingClient for C++")

# for apache arrow
set(OPENSSL_ROOT_DIR /opt/homebrew/opt/openssl@1.1/)

# add the executable
add_library(${TARGET} SHARED ../DeltaSharingRestClient.cpp ../DeltaSharingClient.cpp ../functions.cpp)

find_package(nlohmann_json 3.2.0 REQUIRED)
find_package(Boost 1.79.0 REQUIRED COMPONENTS Program_options)
find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
find_package(ArrowDataset REQUIRED)
#include all necessary external includes
target_include_directories(${TARGET} PRIVATE /usr/local/include/)

target_link_libraries(${TARGET} PRIVATE nlohmann_json::nlohmann_json Boost::program_options /usr/local/lib/librestclient-cpp.dylib ${ARROW_SHARED_LIB} ${ARROW_DATASET_SHARED_LIB} ${PARQUET_SHARED_LIB})

target_compile_features(${TARGET} PRIVATE cxx_std_17)
set_target_properties(${TARGET} PROPERTIES VERSION ${PROJECT_VERSION})
set_target_properties(${TARGET} PROPERTIES SOVERSION 0)
set_target_properties(${TARGET} PROPERTIES PUBLIC_HEADER "../include/DeltaSharingClient.h;../include/DeltaSharingRestClient.h;../include/Protocol.h;../include/functions.h")

include(GNUInstallDirs)
install(TARGETS ${TARGET} EXPORT DeltaSharingConfig
    CONFIGURATIONS Release
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/DeltaSharing)

install(EXPORT DeltaSharingConfig DESTINATION share/DeltaSharing/cmake)
export(TARGETS ${TARGET} FILE DeltaSharingConfig.cmake)

enable_testing()

add_executable(
  TestRunner
  ../tests/functions_test.cpp
)

target_link_libraries(
  TestRunner
  GTest::gtest_main
  nlohmann_json::nlohmann_json 
  Boost::program_options 
  /usr/local/lib/librestclient-cpp.dylib 
  ${ARROW_SHARED_LIB} 
  ${ARROW_DATASET_SHARED_LIB} 
  ${PARQUET_SHARED_LIB}
  ${TARGET}
)

include(GoogleTest)
gtest_discover_tests(TestRunner)