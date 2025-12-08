#pragma once

#include <cstdint>
#include <memory>

#include "protocol/namenode/hdfs_namenode_service.h"
#include "RpcHeader.pb.h"  // RpcRequestHeaderProto / RpcResponseHeaderProto
#include "ProtobufRpcEngine.pb.cc"

namespace hcg {

class HdfsRpcConnection {
public:
    HdfsRpcConnection(int fd, std::shared_ptr<IHdfsNamenodeService> service);
    ~HdfsRpcConnection();

    void serve();  // 处理握手 + 多个 RPC 调用

private:
    int fd_;
    std::shared_ptr<IHdfsNamenodeService> service_;

    bool read_preamble();        // 读取 "hrpc" + 版本 + auth
    bool handle_one_call();      // 读取一个 RPC 请求并回复

    static bool read_full(int fd, void* buf, size_t len);
    static bool write_full(int fd, const void* buf, size_t len);

    // 调度：根据 methodName 调用 service_
    bool dispatch(const ::hadoop::common::RpcRequestHeaderProto& rpc_header,
                  const ::hadoop::common::RequestHeaderProto& req_header,
                  const std::string& param_bytes,
                  std::string& out_response_bytes);
};

} // namespace hcg
