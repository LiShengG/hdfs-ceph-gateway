
#include "protocol/namenode/hdfs_rpc_server.h"

#include "common/logging.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <iostream>


namespace hcg {


HdfsRpcServer::HdfsRpcServer(std::shared_ptr<IHdfsNamenodeService> nn_service)
    : nn_service_(std::move(nn_service)) {
    // 构造函数中可以初始化成员变量
}

HdfsRpcServer::~HdfsRpcServer() {
    stop(); // 析构时停止服务器
}

int HdfsRpcServer::start(const std::string& bind_addr, std::uint16_t port) {
    if (running_) {
        log(LogLevel::ERROR, "HdfsRpcServer is already running");
        return 0; // Already started
    }

    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        log(LogLevel::ERROR, "HdfsRpcServer: socket() failed: %s", std::strerror(errno));
        return -1;
    }

    int opt = 1;
    if (::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        log(LogLevel::ERROR, "HdfsRpcServer: setsockopt(SO_REUSEADDR) failed: %s", std::strerror(errno));
        // Not fatal, continue
    }

    sockaddr_in addr {};
    std::memset(&addr, 0, sizeof(addr)); // Zero-initialize
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    
    // Allow binding to any address if bind_addr is empty or "0.0.0.0"
    if (bind_addr.empty() || bind_addr == "0.0.0.0") {
        addr.sin_addr.s_addr = INADDR_ANY;
    } else {
        if (::inet_pton(AF_INET, bind_addr.c_str(), &addr.sin_addr) <= 0) {
            log(LogLevel::ERROR, "HdfsRpcServer: invalid bind_addr=%s", bind_addr.c_str());
            ::close(listen_fd_);
            listen_fd_ = -1;
            return -1;
        }
    }

    if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        log(LogLevel::ERROR, "HdfsRpcServer: bind(%s:%u) failed: %s",
                  bind_addr.c_str(), port, std::strerror(errno));
        ::close(listen_fd_);
        listen_fd_ = -1;
        return -1;
    }

    if (::listen(listen_fd_, 128) < 0) {
        log(LogLevel::ERROR, "HdfsRpcServer: listen failed: %s", std::strerror(errno));
        ::close(listen_fd_);
        listen_fd_ = -1;
        return -1;
    }

    running_ = true;
    accept_thread_ = std::thread(&HdfsRpcServer::accept_loop, this);

   log(LogLevel::INFO, "HdfsRpcServer started at %s:%u", bind_addr.c_str(), port);
    return 0;
}

int HdfsRpcServer::stop() {
    if (!running_) {
        return 0;
    }
   log(LogLevel::INFO, "Stopping HdfsRpcServer...");

    running_ = false;

    if (listen_fd_ >= 0) {
        // Shutdown can wake up blocking accept()
        ::shutdown(listen_fd_, SHUT_RDWR);
        ::close(listen_fd_);
        listen_fd_ = -1;
    }

    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }

    // Join worker threads
    for (auto& t : worker_threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
    worker_threads_.clear();

   log(LogLevel::INFO, "HdfsRpcServer stopped.");
    return 0;
}

void HdfsRpcServer::accept_loop() {
    while (running_) {
        sockaddr_in cli_addr {};
        socklen_t cli_len = sizeof(cli_addr);

        int client_fd = ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&cli_addr), &cli_len);
        if (client_fd < 0) {
            if (errno == EINTR) {
                continue; // Retry on interrupt
            }
            if (!running_) {
                // Server is shutting down
                break;
            }
            log(LogLevel::ERROR, "HdfsRpcServer: accept failed: %s", std::strerror(errno));
            continue;
        }

        // For simplicity, spawn a thread per connection.
        // In production, consider using a thread pool.
        worker_threads_.emplace_back(&HdfsRpcServer::handle_client, this, client_fd);
    }
}

void HdfsRpcServer::handle_client(int client_fd) {
    char client_ip_str[INET_ADDRSTRLEN];
    sockaddr_in client_addr{};
    socklen_t client_len = sizeof(client_addr);
    if (getpeername(client_fd, reinterpret_cast<sockaddr*>(&client_addr), &client_len) == 0) {
        inet_ntop(AF_INET, &client_addr.sin_addr, client_ip_str, INET_ADDRSTRLEN);
       log(LogLevel::DEBUG, "New client connected from %s:%d", client_ip_str, ntohs(client_addr.sin_port));
    } else {
       log(LogLevel::WARN, "Failed to get client address: %s", strerror(errno));
        strncpy(client_ip_str, "unknown", INET_ADDRSTRLEN - 1);
    }


    // TODO: Implement the actual HDFS RPC protocol handling loop here.
    // This typically involves:
    // 1. Reading the RPC header (e.g., RpcRequestHeaderProto)
    // 2. Reading the request body based on method name
    // 3. Dispatching to the appropriate method in nn_service_
    // 4. Writing back the response

    // Placeholder for now - just echo or close
   log(LogLevel::INFO, "Handling client connection (fd=%d)", client_fd);

    // Example placeholder logic:
    // You would replace this with real HDFS RPC parsing/dispatching
    char buffer[1024];
    ssize_t bytes_read;
    while ((bytes_read = read(client_fd, buffer, sizeof(buffer))) > 0) {
        // Echo back for testing
        write(client_fd, buffer, bytes_read);
    }

    if (bytes_read < 0) {
        log(LogLevel::ERROR, "Error reading from client fd=%d: %s", client_fd, strerror(errno));
    } else {
       log(LogLevel::DEBUG, "Client fd=%d disconnected gracefully.", client_fd);
    }

    ::close(client_fd);
}

} // namespace hcg