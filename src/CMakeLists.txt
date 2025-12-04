# 设置源码根目录（通常在文件开头）
set(SRC_ROOT ${CMAKE_CURRENT_SOURCE_DIR})

# 全局编译定义（若确实需全局生效；否则建议移到具体 target）
add_compile_definitions(_FILE_OFFSET_BITS=64)

# ===================================================================
# 定义可执行文件（必须在 target_link_libraries 之前！）
# ===================================================================

add_executable(hdfs_ceph_gateway
    server/main.cc
)

add_executable(test_cephfs
    test_cephfs_main.cc
)

add_executable(gwctl
    tools/gwctl.cc
)

# ===================================================================
# 定义库
# ===================================================================

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
    rpc/internal/internal_rpc_client.cc
    rpc/internal/internal_gateway_service_impl.cc
    # 注意：头文件通常不需要列在这里，除非是模板实现或需要安装
)

# ===================================================================
# 包含目录（推荐使用 target_include_directories 替代 include_directories）
# ===================================================================

target_include_directories(hdfs_ceph_gateway_lib PUBLIC
    $<BUILD_INTERFACE:${SRC_ROOT}>
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/proto>
    $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}/proto>
)

# 如果其他 target（如 test_cephfs）也需要这些头文件，
# 可通过链接 hdfs_ceph_gateway_lib 自动继承（PUBLIC / INTERFACE）

# ===================================================================
# 查找系统库
# ===================================================================

find_library(CEPHFS_LIB cephfs)
find_library(RADOS_LIB rados)

if(NOT CEPHFS_LIB)
    message(FATAL_ERROR "libcephfs not found")
endif()
if(NOT RADOS_LIB)
    message(FATAL_ERROR "librados not found")
endif()

# ===================================================================
# 链接依赖
# ===================================================================

target_link_libraries(hdfs_ceph_gateway_lib
    PUBLIC
        gateway_internal_proto
        ${CEPHFS_LIB}
        ${RADOS_LIB}
        protobuf::libprotobuf
)

target_link_libraries(test_cephfs
    PRIVATE
        hdfs_ceph_gateway_lib
)

target_link_libraries(hdfs_ceph_gateway
    PRIVATE
        hdfs_ceph_gateway_lib
)

target_link_libraries(gwctl
    PRIVATE
        hdfs_ceph_gateway_lib
)