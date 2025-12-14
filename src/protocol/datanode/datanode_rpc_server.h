#pragma once

#include <string>
#include "core/block/block_manager.h"
#include "fsal/cephfs/cephfs_adapter.h"
#include "protocol/datanode/hdfs_datanode_service.h"
#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

// 前向声明，你需要定义这个接口
// #include "i_hdfs_datanode_service.h"

namespace hcg {

class DataRpcServer {
public:
    explicit DataRpcServer(std::shared_ptr<IHdfsDatanodeService> dn_service);
    ~DataRpcServer();

    // 禁止拷贝和移动
    DataRpcServer(const DataRpcServer&) = delete;
    DataRpcServer& operator=(const DataRpcServer&) = delete;
    DataRpcServer(DataRpcServer&&) = delete;
    DataRpcServer& operator=(DataRpcServer&&) = delete;

    int start(const std::string& bind_addr, std::uint16_t port);
    int stop();

    bool is_running() const { return running_.load(); }

private:
    std::shared_ptr<IHdfsDatanodeService> dn_service_;
    int listen_fd_{-1};
    std::atomic<bool> running_{false};
    std::thread accept_thread_;
    std::vector<std::thread> worker_threads_;
    std::mutex workers_mutex_;  // 保护 worker_threads_

    void accept_loop();
    void handle_client(int client_fd);
};

} // namespace hcg

