set(SRC_ROOT ${CMAKE_CURRENT_SOURCE_DIR})

# 暂时先不强依赖 spdlog / protobuf / ceph 的 find_package
# 你可以后续再加 find_package(CEPHFS) 等

include_directories(
    ${SRC_ROOT}
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
)

add_executable(hdfs_ceph_gateway
    server/main.cc
)

target_link_libraries(hdfs_ceph_gateway
    hdfs_ceph_gateway_lib
    # 这里后面会加:
    # cephfs
    # pthread
    # protobuf
    # 你的日志库等
)
