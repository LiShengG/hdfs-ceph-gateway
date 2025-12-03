// src/rpc/internal/internal_rpc_server.cc
#include "rpc/internal/internal_rpc_server.h"
#include "rpc/internal/internal_rpc_defs.h"
#include "common/logging.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace hcg {
namespace internal_rpc {

InternalRpcServer::InternalRpcServer(std::shared_ptr<IInternalGatewayService> service)
    : service_(std::move(service)) {}

InternalRpcServer::~InternalRpcServer() {
    stop();
}

int InternalRpcServer::start(const std::string& bind_addr, std::uint16_t port) {
    if (running_) {
        return 0;
    }

    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        log(LogLevel::ERROR, "InternalRpcServer: socket() failed: %s",
            std::strerror(errno));
        return -1;
    }

    int opt = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, bind_addr.c_str(), &addr.sin_addr) <= 0) {
        log(LogLevel::ERROR, "InternalRpcServer: invalid bind_addr=%s",
            bind_addr.c_str());
        ::close(listen_fd_);
        listen_fd_ = -1;
        return -1;
    }

    if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        log(LogLevel::ERROR, "InternalRpcServer: bind(%s:%u) failed: %s",
            bind_addr.c_str(), port, std::strerror(errno));
        ::close(listen_fd_);
        listen_fd_ = -1;
        return -1;
    }

    if (::listen(listen_fd_, 128) < 0) {
        log(LogLevel::ERROR, "InternalRpcServer: listen failed: %s",
            std::strerror(errno));
        ::close(listen_fd_);
        listen_fd_ = -1;
        return -1;
    }

    running_ = true;
    accept_thread_ = std::thread(&InternalRpcServer::accept_loop, this);

    log(LogLevel::INFO, "InternalRpcServer started at %s:%u",
        bind_addr.c_str(), port);
    return 0;
}

int InternalRpcServer::stop() {
    if (!running_) {
        return 0;
    }
    running_ = false;

    if (listen_fd_ >= 0) {
        ::shutdown(listen_fd_, SHUT_RDWR);
        ::close(listen_fd_);
        listen_fd_ = -1;
    }

    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }

    for (auto& t : worker_threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
    worker_threads_.clear();

    log(LogLevel::INFO, "InternalRpcServer stopped");
    return 0;
}

void InternalRpcServer::accept_loop() {
    while (running_) {
        sockaddr_in cli_addr {};
        socklen_t cli_len = sizeof(cli_addr);

        int client_fd = ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&cli_addr), &cli_len);
        if (client_fd < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (!running_) {
                break;
            }
            log(LogLevel::ERROR, "InternalRpcServer: accept failed: %s",
                std::strerror(errno));
            continue;
        }

        // 简单：每个连接一个线程
        worker_threads_.emplace_back(&InternalRpcServer::handle_client, this, client_fd);
    }
}

void InternalRpcServer::handle_client(int client_fd) {
    // 当前阶段：每个连接上可以处理多个 RPC，直到对端关闭
    for (;;) {
        if (!running_) {
            break;
        }

        // 如果客户端关闭连接，read_full 会失败
        handle_one_request(client_fd);
        // handle_one_request 内部如果读失败，会关闭 fd 并返回
        // 这里简单 break
        break;
    }
    ::close(client_fd);
}

bool InternalRpcServer::read_full(int fd, void* buf, size_t len) {
    std::uint8_t* p = static_cast<std::uint8_t*>(buf);
    size_t nread = 0;
    while (nread < len) {
        ssize_t r = ::read(fd, p + nread, len - nread);
        if (r < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (r == 0) {
            // 对端关闭
            return false;
        }
        nread += static_cast<size_t>(r);
    }
    return true;
}

bool InternalRpcServer::write_full(int fd, const void* buf, size_t len) {
    const std::uint8_t* p = static_cast<const std::uint8_t*>(buf);
    size_t nwritten = 0;
    while (nwritten < len) {
        ssize_t r = ::write(fd, p + nwritten, len - nwritten);
        if (r < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (r == 0) {
            return false;
        }
        nwritten += static_cast<size_t>(r);
    }
    return true;
}

void InternalRpcServer::handle_one_request(int client_fd) {
    // 1. 读取 4 字节 length
    std::uint32_t net_len = 0;
    if (!read_full(client_fd, &net_len, sizeof(net_len))) {
        // 连接关闭或错误
        return;
    }
    std::uint32_t len = ntohl(net_len);
    if (len < sizeof(std::uint16_t) * 2) {
        log(LogLevel::ERROR, "InternalRpcServer: invalid frame length=%u", len);
        return;
    }

    // 2. 读取 method_id + reserved + payload
    std::vector<std::uint8_t> buf(len);
    if (!read_full(client_fd, buf.data(), buf.size())) {
        return;
    }

    std::uint16_t net_mid = 0;
    std::uint16_t reserved = 0;
    std::memcpy(&net_mid, buf.data(), sizeof(net_mid));
    std::memcpy(&reserved, buf.data() + sizeof(net_mid), sizeof(reserved));
    (void)reserved;
    MethodId mid = method_from_uint16(ntohs(net_mid));

    const std::uint8_t* payload = buf.data() + sizeof(net_mid) + sizeof(reserved);
    size_t payload_len = buf.size() - sizeof(net_mid) - sizeof(reserved);

    log(LogLevel::DEBUG, "InternalRpcServer: received method=%s payload_len=%zu",
        to_string(mid), payload_len);

    using namespace hcg::internal;

    // 在栈上创建 request/response 对象
    // 先准备一个 status，默认 OK
    auto make_ok_status = []() {
        RpcStatus st;
        st.set_code(0);
        st.set_message("");
        return st;
    };

    // 根据 method 解析不同的 Request，并调用 service 再返回 Response
    switch (mid) {
    case MethodId::CREATE_FILE: {
        CreateFileRequest req;
        if (!req.ParseFromArray(payload, static_cast<int>(payload_len))) {
            log(LogLevel::ERROR, "Parse CreateFileRequest failed");
            return;
        }
        CreateFileResponse rsp;
        rsp.mutable_status()->CopyFrom(make_ok_status());
        service_->CreateFile(req, rsp);

        std::string out;
        if (!rsp.SerializeToString(&out)) {
            log(LogLevel::ERROR, "Serialize CreateFileResponse failed");
            return;
        }

        std::uint16_t out_mid = htons(to_uint16(MethodId::CREATE_FILE));
        std::uint16_t out_reserved = 0;
        std::uint32_t out_len = htonl(static_cast<std::uint32_t>(
            sizeof(out_mid) + sizeof(out_reserved) + out.size()));

        if (!write_full(client_fd, &out_len, sizeof(out_len))) return;
        if (!write_full(client_fd, &out_mid, sizeof(out_mid))) return;
        if (!write_full(client_fd, &out_reserved, sizeof(out_reserved))) return;
        if (!write_full(client_fd, out.data(), out.size())) return;
        break;
    }
    case MethodId::GET_FILE_INFO: {
        GetFileInfoRequest req;
        if (!req.ParseFromArray(payload, static_cast<int>(payload_len))) {
            log(LogLevel::ERROR, "Parse GetFileInfoRequest failed");
            return;
        }
        GetFileInfoResponse rsp;
        rsp.mutable_status()->CopyFrom(make_ok_status());
        service_->GetFileInfo(req, rsp);

        std::string out;
        if (!rsp.SerializeToString(&out)) {
            log(LogLevel::ERROR, "Serialize GetFileInfoResponse failed");
            return;
        }

        std::uint16_t out_mid = htons(to_uint16(MethodId::GET_FILE_INFO));
        std::uint16_t out_reserved = 0;
        std::uint32_t out_len = htonl(static_cast<std::uint32_t>(
            sizeof(out_mid) + sizeof(out_reserved) + out.size()));

        if (!write_full(client_fd, &out_len, sizeof(out_len))) return;
        if (!write_full(client_fd, &out_mid, sizeof(out_mid))) return;
        if (!write_full(client_fd, &out_reserved, sizeof(out_reserved))) return;
        if (!write_full(client_fd, out.data(), out.size())) return;
        break;
    }
    case MethodId::LIST_STATUS: {
        ListStatusRequest req;
        if (!req.ParseFromArray(payload, static_cast<int>(payload_len))) {
            log(LogLevel::ERROR, "Parse ListStatusRequest failed");
            return;
        }
        ListStatusResponse rsp;
        rsp.mutable_status()->CopyFrom(make_ok_status());
        service_->ListStatus(req, rsp);

        std::string out;
        if (!rsp.SerializeToString(&out)) {
            log(LogLevel::ERROR, "Serialize ListStatusResponse failed");
            return;
        }

        std::uint16_t out_mid = htons(to_uint16(MethodId::LIST_STATUS));
        std::uint16_t out_reserved = 0;
        std::uint32_t out_len = htonl(static_cast<std::uint32_t>(
            sizeof(out_mid) + sizeof(out_reserved) + out.size()));

        if (!write_full(client_fd, &out_len, sizeof(out_len))) return;
        if (!write_full(client_fd, &out_mid, sizeof(out_mid))) return;
        if (!write_full(client_fd, &out_reserved, sizeof(out_reserved))) return;
        if (!write_full(client_fd, out.data(), out.size())) return;
        break;
    }
    case MethodId::DELETE_PATH: {
        DeleteRequest req;
        if (!req.ParseFromArray(payload, static_cast<int>(payload_len))) {
            log(LogLevel::ERROR, "Parse DeleteRequest failed");
            return;
        }
        DeleteResponse rsp;
        rsp.mutable_status()->CopyFrom(make_ok_status());
        service_->DeletePath(req, rsp);

        std::string out;
        if (!rsp.SerializeToString(&out)) {
            log(LogLevel::ERROR, "Serialize DeleteResponse failed");
            return;
        }

        std::uint16_t out_mid = htons(to_uint16(MethodId::DELETE_PATH));
        std::uint16_t out_reserved = 0;
        std::uint32_t out_len = htonl(static_cast<std::uint32_t>(
            sizeof(out_mid) + sizeof(out_reserved) + out.size()));

        if (!write_full(client_fd, &out_len, sizeof(out_len))) return;
        if (!write_full(client_fd, &out_mid, sizeof(out_mid))) return;
        if (!write_full(client_fd, &out_reserved, sizeof(out_reserved))) return;
        if (!write_full(client_fd, out.data(), out.size())) return;
        break;
    }
    case MethodId::ALLOCATE_BLOCK: {
        AllocateBlockRequest req;
        if (!req.ParseFromArray(payload, static_cast<int>(payload_len))) {
            log(LogLevel::ERROR, "Parse AllocateBlockRequest failed");
            return;
        }
        AllocateBlockResponse rsp;
        rsp.mutable_status()->CopyFrom(make_ok_status());
        service_->AllocateBlock(req, rsp);

        std::string out;
        if (!rsp.SerializeToString(&out)) {
            log(LogLevel::ERROR, "Serialize AllocateBlockResponse failed");
            return;
        }

        std::uint16_t out_mid = htons(to_uint16(MethodId::ALLOCATE_BLOCK));
        std::uint16_t out_reserved = 0;
        std::uint32_t out_len = htonl(static_cast<std::uint32_t>(
            sizeof(out_mid) + sizeof(out_reserved) + out.size()));

        if (!write_full(client_fd, &out_len, sizeof(out_len))) return;
        if (!write_full(client_fd, &out_mid, sizeof(out_mid))) return;
        if (!write_full(client_fd, &out_reserved, sizeof(out_reserved))) return;
        if (!write_full(client_fd, out.data(), out.size())) return;
        break;
    }
    case MethodId::GET_BLOCK_LOCATIONS: {
        GetBlockLocationsRequest req;
        if (!req.ParseFromArray(payload, static_cast<int>(payload_len))) {
            log(LogLevel::ERROR, "Parse GetBlockLocationsRequest failed");
            return;
        }
        GetBlockLocationsResponse rsp;
        rsp.mutable_status()->CopyFrom(make_ok_status());
        service_->GetBlockLocations(req, rsp);

        std::string out;
        if (!rsp.SerializeToString(&out)) {
            log(LogLevel::ERROR, "Serialize GetBlockLocationsResponse failed");
            return;
        }

        std::uint16_t out_mid = htons(to_uint16(MethodId::GET_BLOCK_LOCATIONS));
        std::uint16_t out_reserved = 0;
        std::uint32_t out_len = htonl(static_cast<std::uint32_t>(
            sizeof(out_mid) + sizeof(out_reserved) + out.size()));

        if (!write_full(client_fd, &out_len, sizeof(out_len))) return;
        if (!write_full(client_fd, &out_mid, sizeof(out_mid))) return;
        if (!write_full(client_fd, &out_reserved, sizeof(out_reserved))) return;
        if (!write_full(client_fd, out.data(), out.size())) return;
        break;
    }
    case MethodId::WRITE_BLOCK: {
        WriteBlockRequest req;
        if (!req.ParseFromArray(payload, static_cast<int>(payload_len))) {
            log(LogLevel::ERROR, "Parse WriteBlockRequest failed");
            return;
        }
        WriteBlockResponse rsp;
        rsp.mutable_status()->CopyFrom(make_ok_status());
        service_->WriteBlock(req, rsp);

        std::string out;
        if (!rsp.SerializeToString(&out)) {
            log(LogLevel::ERROR, "Serialize WriteBlockResponse failed");
            return;
        }

        std::uint16_t out_mid = htons(to_uint16(MethodId::WRITE_BLOCK));
        std::uint16_t out_reserved = 0;
        std::uint32_t out_len = htonl(static_cast<std::uint32_t>(
            sizeof(out_mid) + sizeof(out_reserved) + out.size()));

        if (!write_full(client_fd, &out_len, sizeof(out_len))) return;
        if (!write_full(client_fd, &out_mid, sizeof(out_mid))) return;
        if (!write_full(client_fd, &out_reserved, sizeof(out_reserved))) return;
        if (!write_full(client_fd, out.data(), out.size())) return;
        break;
    }
    case MethodId::READ_BLOCK: {
        ReadBlockRequest req;
        if (!req.ParseFromArray(payload, static_cast<int>(payload_len))) {
            log(LogLevel::ERROR, "Parse ReadBlockRequest failed");
            return;
        }
        ReadBlockResponse rsp;
        rsp.mutable_status()->CopyFrom(make_ok_status());
        service_->ReadBlock(req, rsp);

        std::string out;
        if (!rsp.SerializeToString(&out)) {
            log(LogLevel::ERROR, "Serialize ReadBlockResponse failed");
            return;
        }

        std::uint16_t out_mid = htons(to_uint16(MethodId::READ_BLOCK));
        std::uint16_t out_reserved = 0;
        std::uint32_t out_len = htonl(static_cast<std::uint32_t>(
            sizeof(out_mid) + sizeof(out_reserved) + out.size()));

        if (!write_full(client_fd, &out_len, sizeof(out_len))) return;
        if (!write_full(client_fd, &out_mid, sizeof(out_mid))) return;
        if (!write_full(client_fd, &out_reserved, sizeof(out_reserved))) return;
        if (!write_full(client_fd, out.data(), out.size())) return;
        break;
    }
    case MethodId::COMPLETE: {
        CompleteRequest req;
        if (!req.ParseFromArray(payload, static_cast<int>(payload_len))) {
            log(LogLevel::ERROR, "Parse CompleteRequest failed");
            return;
        }
        CompleteResponse rsp;
        rsp.mutable_status()->CopyFrom(make_ok_status());
        service_->Complete(req, rsp);

        std::string out;
        if (!rsp.SerializeToString(&out)) {
            log(LogLevel::ERROR, "Serialize CompleteResponse failed");
            return;
        }

        std::uint16_t out_mid = htons(to_uint16(MethodId::COMPLETE));
        std::uint16_t out_reserved = 0;
        std::uint32_t out_len = htonl(static_cast<std::uint32_t>(
            sizeof(out_mid) + sizeof(out_reserved) + out.size()));

        if (!write_full(client_fd, &out_len, sizeof(out_len))) return;
        if (!write_full(client_fd, &out_mid, sizeof(out_mid))) return;
        if (!write_full(client_fd, &out_reserved, sizeof(out_reserved))) return;
        if (!write_full(client_fd, out.data(), out.size())) return;
        break;
    }
    default:
        log(LogLevel::ERROR, "InternalRpcServer: unknown method id=%u",
            static_cast<unsigned>(mid));
        break;
    }
}

} // namespace internal_rpc
} // namespace hcg
