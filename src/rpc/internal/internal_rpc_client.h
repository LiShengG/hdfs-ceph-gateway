// src/rpc/internal/internal_rpc_client.h
#pragma once

#include <cstdint>
#include <string>

#include "rpc/internal/internal_rpc_defs.h"
#include "gateway_internal.pb.h"

namespace hcg {
namespace internal_rpc {

/**
 * 简单同步 client：
 * - 每个 InternalRpcClient 持有一个 TCP 连接
 * - 每次调用都是：序列化 req -> 发送一帧 -> 收一帧 -> parse rsp
 * - 不做并发、多路复用等复杂东西，Stage 0 够用
 */
class InternalRpcClient {
public:
    InternalRpcClient(const std::string& host, std::uint16_t port);
    ~InternalRpcClient();

    // 显式 connect / close
    bool connect();
    void close();

    bool is_connected() const { return sock_fd_ >= 0; }

    // ---- RPC 封装 ----
    bool CreateFile(const internal::CreateFileRequest& req,
                    internal::CreateFileResponse& rsp);

    bool GetFileInfo(const internal::GetFileInfoRequest& req,
                     internal::GetFileInfoResponse& rsp);

    bool ListStatus(const internal::ListStatusRequest& req,
                    internal::ListStatusResponse& rsp);

    bool DeletePath(const internal::DeleteRequest& req,
                    internal::DeleteResponse& rsp);

    bool AllocateBlock(const internal::AllocateBlockRequest& req,
                       internal::AllocateBlockResponse& rsp);

    bool GetBlockLocations(const internal::GetBlockLocationsRequest& req,
                           internal::GetBlockLocationsResponse& rsp);

    bool WriteBlock(const internal::WriteBlockRequest& req,
                    internal::WriteBlockResponse& rsp);

    bool ReadBlock(const internal::ReadBlockRequest& req,
                   internal::ReadBlockResponse& rsp);

    bool Complete(const internal::CompleteRequest& req,
                  internal::CompleteResponse& rsp);

private:
    std::string host_;
    std::uint16_t port_;
    int sock_fd_ {-1};

    static bool read_full(int fd, void* buf, size_t len);
    static bool write_full(int fd, const void* buf, size_t len);

    // 通用发送/接收 + 反序列化
    bool call(MethodId method,
              const ::google::protobuf::Message& req,
              ::google::protobuf::Message& rsp,
              MethodId expected_method);
};

} // namespace internal_rpc
} // namespace hcg
