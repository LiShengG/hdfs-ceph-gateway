# ---- 1. 先处理 gateway 内部 proto ----
set(GATEWAY_INTERNAL_PROTO
    ${CMAKE_CURRENT_SOURCE_DIR}/gateway_internal.proto
)
protobuf_generate_cpp(GATEWAY_PROTO_SRCS GATEWAY_PROTO_HDRS ${GATEWAY_INTERNAL_PROTO})

add_library(gateway_internal_proto STATIC
    ${GATEWAY_PROTO_SRCS}
    ${GATEWAY_PROTO_HDRS}
)
target_link_libraries(gateway_internal_proto PUBLIC protobuf::libprotobuf)
target_include_directories(gateway_internal_proto PUBLIC
    ${CMAKE_CURRENT_BINARY_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# ---- 2. 再处理 HDFS RPC proto（排除 gateway_internal.proto 避免重复） ----
file(GLOB_RECURSE HDFS_RPC_PROTOS
     "${CMAKE_CURRENT_SOURCE_DIR}/hadoop-common/*.proto"
     "${CMAKE_CURRENT_SOURCE_DIR}/hadoop-hdfs/*.proto"
     "${CMAKE_CURRENT_SOURCE_DIR}/hadoop-hdfs-client/*.proto"
)
# 把已经单独生成的 gateway_internal.proto 从列表里剔除
list(REMOVE_ITEM HDFS_RPC_PROTOS ${GATEWAY_INTERNAL_PROTO})

protobuf_generate_cpp(HDFS_RPC_SRCS HDFS_RPC_HDRS ${HDFS_RPC_PROTOS})

add_library(hdfs_rpc_proto STATIC ${HDFS_RPC_SRCS} ${HDFS_RPC_HDRS})
target_link_libraries(hdfs_rpc_proto PUBLIC protobuf::libprotobuf)
target_include_directories(hdfs_rpc_proto PUBLIC
    ${CMAKE_CURRENT_BINARY_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}
)