set(SRC_ROOT ${CMAKE_CURRENT_SOURCE_DIR})

# 暂时先不强依赖 spdlog / protobuf / ceph 的 find_package
# 你可以后续再加 find_package(CEPHFS) 等

add_compile_definitions(_FILE_OFFSET_BITS=64)

include_directories(
    ${SRC_ROOT}
    ${PROJECT_SOURCE_DIR}/proto          # 源 proto 目录 (可选)
    ${PROJECT_BINARY_DIR}/proto          # 生成的 pb 头文件目录
)

add_library(hdfs_ceph_gateway_lib
    common/logging.cc
    fsal/cephfs/cephfs_adapter.cc
    meta/xattr_metadata_store.cc
    core/namespace/namespace_service.cc
    core/block/block_manager.cc
    core/lease/lease_manager.cc
    protocol/namenode/namenode_rpc_server.cc
    protocol/datanode/datanode_server.cc
    server/gateway.cc
    rpc/internal/internal_rpc_server.cc
    rpc/internal/internal_gateway_service_dummy.h
)



find_library(CEPHFS_LIB cephfs)
find_library(RADOS_LIB rados)

if (NOT CEPHFS_LIB)
    message(FATAL_ERROR "libcephfs not found")
endif()

target_link_libraries(hdfs_ceph_gateway_lib
    gateway_internal_proto
    ${CEPHFS_LIB}
    ${RADOS_LIB}
    protobuf::libprotobuf
)

add_executable(hdfs_ceph_gateway
    server/main.cc
)

add_executable(test_cephfs
    test_cephfs_main.cc
)

target_link_libraries(test_cephfs
    hdfs_ceph_gateway_lib
    ${CEPHFS_LIB}
    ${RADOS_LIB}
)


target_link_libraries(hdfs_ceph_gateway
    hdfs_ceph_gateway_lib
    # 这里后面会加:
    # cephfs
    # pthread
    # protobuf
    # 你的日志库等
)
