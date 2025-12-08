# ---- 1. 先处理 gateway 内部 proto ----
set(GENERATED_DIR ${CMAKE_CURRENT_SOURCE_DIR}/generated)

set(GATEWAY_INTERNAL_PROTO ${CMAKE_CURRENT_SOURCE_DIR}/gateway_internal.proto)
set(GATEWAY_PROTO_SRC ${GENERATED_DIR}/gateway_internal.pb.cc)
set(GATEWAY_PROTO_HDR ${GENERATED_DIR}/gateway_internal.pb.h)

add_custom_command(
    OUTPUT ${GATEWAY_PROTO_SRC} ${GATEWAY_PROTO_HDR}
    COMMAND protobuf::protoc
        --proto_path=${CMAKE_CURRENT_SOURCE_DIR}
        --cpp_out=${GENERATED_DIR}
        ${GATEWAY_INTERNAL_PROTO}
    DEPENDS ${GATEWAY_INTERNAL_PROTO}
    COMMENT "Generating gateway_internal.pb.cc/h from gateway_internal.proto"
    VERBATIM
)

add_library(gateway_internal_proto STATIC
    ${GATEWAY_PROTO_SRC}
    ${GATEWAY_PROTO_HDR}
)
target_link_libraries(gateway_internal_proto PUBLIC protobuf::libprotobuf)
target_include_directories(gateway_internal_proto PUBLIC
    ${GENERATED_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# ---- 2. 再处理 HDFS RPC proto（排除 gateway_internal.proto） ----
file(GLOB_RECURSE HDFS_RPC_PROTOS
     "${CMAKE_CURRENT_SOURCE_DIR}/hadoop-common/*.proto"
     "${CMAKE_CURRENT_SOURCE_DIR}/hadoop-hdfs/*.proto"
     "${CMAKE_CURRENT_SOURCE_DIR}/hadoop-hdfs-client/*.proto"
)
list(REMOVE_ITEM HDFS_RPC_PROTOS ${GATEWAY_INTERNAL_PROTO})

# 设置 proto 搜索路径（关键！解决 import 找不到的问题）
set(PROTO_INCLUDE_DIRS
    ${CMAKE_CURRENT_SOURCE_DIR}/hadoop-hdfs
    ${CMAKE_CURRENT_SOURCE_DIR}/hadoop-common
    ${CMAKE_CURRENT_SOURCE_DIR}/hadoop-hdfs-client
)

# 构建 --proto_path 参数列表
set(PROTO_PATH_ARGS "")
foreach(DIR IN LISTS PROTO_INCLUDE_DIRS)
    list(APPEND PROTO_PATH_ARGS "--proto_path=${DIR}")
endforeach()

# 收集生成的源文件和头文件
set(HDFS_RPC_SRCS "")
set(HDFS_RPC_HDRS "")

foreach(PROTO_FILE IN LISTS HDFS_RPC_PROTOS)
    # 获取文件名（不含路径）
    get_filename_component(FILE_NAME ${PROTO_FILE} NAME)
    get_filename_component(FILE_WE ${FILE_NAME} NAME_WE)

    # 平铺到 generated/（避免子目录问题，先测试能否工作）
    set(PB_CC ${GENERATED_DIR}/${FILE_WE}.pb.cc)
    set(PB_H  ${GENERATED_DIR}/${FILE_WE}.pb.h)

    list(APPEND HDFS_RPC_SRCS ${PB_CC})
    list(APPEND HDFS_RPC_HDRS ${PB_H})

    add_custom_command(
        OUTPUT ${PB_CC} ${PB_H}
        COMMAND protobuf::protoc
            ${PROTO_PATH_ARGS}
            --cpp_out=${GENERATED_DIR}
            ${PROTO_FILE}
        DEPENDS ${PROTO_FILE}
        COMMENT "Generating flat: ${FILE_NAME}"
        VERBATIM
    )
endforeach()

add_library(hdfs_rpc_proto STATIC ${HDFS_RPC_SRCS} ${HDFS_RPC_HDRS})
target_link_libraries(hdfs_rpc_proto PUBLIC protobuf::libprotobuf)
target_include_directories(hdfs_rpc_proto PUBLIC
    ${GENERATED_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}
)