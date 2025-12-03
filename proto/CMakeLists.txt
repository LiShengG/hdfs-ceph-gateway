# 使用 CMake 自带的 Protobuf 辅助函数
# 一般 FindProtobuf.cmake 会自动在 find_package(Protobuf) 时包含这些宏
# 如果你的环境有问题，可以手动 include(${Protobuf_DIR}/protobuf-generate.cmake) 之类。

set(GATEWAY_INTERNAL_PROTO
    ${CMAKE_CURRENT_SOURCE_DIR}/gateway_internal.proto
)

# 生成 C++ 源码和头文件
# PROTO_SRCS/PROTO_HDRS 会放在当前 binary dir 下
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${GATEWAY_INTERNAL_PROTO})

add_library(gateway_internal_proto STATIC
    ${PROTO_SRCS}
    ${PROTO_HDRS}
)

# 链接 protobuf
target_link_libraries(gateway_internal_proto
    PUBLIC
        protobuf::libprotobuf
)

# 让使用这个库的 target 能 include 到生成的头文件
target_include_directories(gateway_internal_proto
    PUBLIC
        ${CMAKE_CURRENT_BINARY_DIR}      # 生成的 *.pb.h
        ${CMAKE_CURRENT_SOURCE_DIR}      # 源 proto 所在路径（一般用不到，但保留）
)
