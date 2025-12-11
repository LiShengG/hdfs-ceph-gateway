#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "protocol/namenode/hdfs_namenode_service.h"

namespace hcg {

class NameRpcServer {
public:
    NameRpcServer(std::shared_ptr<IHdfsNamenodeService> nn_service);
    ~NameRpcServer();

    int start(const std::string& bind_addr, std::uint16_t port);
    int stop();

private:
    std::shared_ptr<IHdfsNamenodeService> nn_service_;
    int listen_fd_{-1};
    std::atomic<bool> running_{false};
    std::thread accept_thread_;
    std::vector<std::thread> worker_threads_;

    void accept_loop();
    void handle_client(int client_fd);
};

} // namespace hcg
