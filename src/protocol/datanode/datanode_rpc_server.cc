#include "protocol/datanode/datanode_rpc_server.h"
#include "common/logging.h"
#include "protocol/datanode/datanode_rpc_connection.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace hcg {

DataRpcServer::DataRpcServer(std::shared_ptr<IHdfsDatanodeService> dn_service)
    : dn_service_(std::move(dn_service)) {
}

DataRpcServer::~DataRpcServer() {
    stop();
}

int DataRpcServer::start(const std::string& bind_addr, std::uint16_t port) {
    if (running_) {
        log(LogLevel::ERROR, "DataRpcServer is already running");
        return 0;
    }

    // 创建 socket
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        log(LogLevel::ERROR, "DataRpcServer: socket() failed: %s", std::strerror(errno));
        return -1;
    }

    // 设置地址重用
    int opt = 1;
    if (::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        log(LogLevel::ERROR, "DataRpcServer: setsockopt(SO_REUSEADDR) failed: %s", 
            std::strerror(errno));
    }

    // 绑定地址
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    if (bind_addr.empty() || bind_addr == "0.0.0.0") {
        addr.sin_addr.s_addr = INADDR_ANY;
    } else {
        if (::inet_pton(AF_INET, bind_addr.c_str(), &addr.sin_addr) <= 0) {
            log(LogLevel::ERROR, "DataRpcServer: invalid bind_addr=%s", bind_addr.c_str());
            ::close(listen_fd_);
            listen_fd_ = -1;
            return -1;
        }
    }

    if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        log(LogLevel::ERROR, "DataRpcServer: bind(%s:%u) failed: %s",
            bind_addr.c_str(), port, std::strerror(errno));
        ::close(listen_fd_);
        listen_fd_ = -1;
        return -1;
    }

    // 开始监听
    if (::listen(listen_fd_, 128) < 0) {
        log(LogLevel::ERROR, "DataRpcServer: listen failed: %s", std::strerror(errno));
        ::close(listen_fd_);
        listen_fd_ = -1;
        return -1;
    }

    running_ = true;
    accept_thread_ = std::thread(&DataRpcServer::accept_loop, this);

    log(LogLevel::INFO, "DataRpcServer started at %s:%u", bind_addr.c_str(), port);
    return 0;
}

int DataRpcServer::stop() {
    if (!running_) {
        return 0;
    }

    log(LogLevel::INFO, "Stopping DataRpcServer...");
    running_ = false;

    // 关闭监听 socket 以唤醒 accept()
    if (listen_fd_ >= 0) {
        ::shutdown(listen_fd_, SHUT_RDWR);
        ::close(listen_fd_);
        listen_fd_ = -1;
    }

    // 等待 accept 线程结束
    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }

    // 等待所有 worker 线程结束
    {
        std::lock_guard<std::mutex> lock(workers_mutex_);
        for (auto& t : worker_threads_) {
            if (t.joinable()) {
                t.join();
            }
        }
        worker_threads_.clear();
    }

    log(LogLevel::INFO, "DataRpcServer stopped.");
    return 0;
}

void DataRpcServer::accept_loop() {
    while (running_) {
        sockaddr_in cli_addr{};
        socklen_t cli_len = sizeof(cli_addr);

        log(LogLevel::INFO, "DataNode: accept_loop ----------++");
        
        int client_fd = ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&cli_addr), &cli_len);
        if (client_fd < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (!running_) {
                break;
            }
            log(LogLevel::ERROR, "DataRpcServer: accept failed: %s", std::strerror(errno));
            continue;
        }

        // 为每个连接创建 worker 线程
        {
            std::lock_guard<std::mutex> lock(workers_mutex_);
            worker_threads_.emplace_back(&DataRpcServer::handle_client, this, client_fd);
        }
    }
}

void DataRpcServer::handle_client(int client_fd) {
    // 获取客户端地址信息
    char client_ip_str[INET_ADDRSTRLEN];
    sockaddr_in client_addr{};
    socklen_t client_len = sizeof(client_addr);
    
    if (getpeername(client_fd, reinterpret_cast<sockaddr*>(&client_addr), &client_len) == 0) {
        inet_ntop(AF_INET, &client_addr.sin_addr, client_ip_str, INET_ADDRSTRLEN);
        log(LogLevel::DEBUG, "DataNode: New client connected from %s:%d", 
            client_ip_str, ntohs(client_addr.sin_port));
    } else {
        log(LogLevel::WARN, "DataNode: Failed to get client address: %s", strerror(errno));
        strncpy(client_ip_str, "unknown", INET_ADDRSTRLEN - 1);
    }

    log(LogLevel::INFO, "DataNode: Handling client connection (fd=%d)", client_fd);

    // 使用 DataRpcConnection 处理 RPC 请求
    DataRpcConnection conn(client_fd, dn_service_);
    conn.serve();

    ::close(client_fd);
    log(LogLevel::DEBUG, "DataNode: Client disconnected (fd=%d)", client_fd);
}

} // namespace hcg
