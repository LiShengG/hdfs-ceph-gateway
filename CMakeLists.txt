cmake_minimum_required(VERSION 3.15)
project(hdfs_ceph_gateway CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 可以这里加 -Wall -Wextra 等
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

find_package(Protobuf REQUIRED)
message(STATUS "Found Protobuf: ${Protobuf_VERSION}")
add_subdirectory(proto)

add_subdirectory(src)
