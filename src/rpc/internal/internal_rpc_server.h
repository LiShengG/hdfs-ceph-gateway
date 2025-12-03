// src/rpc/internal/internal_rpc_server.h
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "rpc/internal/internal_gateway_service.h"

namespace hcg {
namespace internal_rpc {

class InternalRpcServer {
public:
    InternalRpcServer(std::shared_ptr<IInternalGatewayService> service);
    ~InternalRpcServer();

    // bind_addr 例如 "0.0.0.0", port 例如 19000
    int start(const std::string& bind_addr, std::uint16_t port);
    int stop();

private:
    std::shared_ptr<IInternalGatewayService> service_;
    int listen_fd_ {-1};
    std::atomic<bool> running_ {false};
    std::thread accept_thread_;
    std::vector<std::thread> worker_threads_;

    void accept_loop();

    void handle_client(int client_fd);

    // Tool: 读写固定长度数据
    static bool read_full(int fd, void* buf, size_t len);
    static bool write_full(int fd, const void* buf, size_t len);

    // 处理单个 RPC 请求帧
    void handle_one_request(int client_fd);
};

} // namespace internal_rpc
} // namespace hcg
