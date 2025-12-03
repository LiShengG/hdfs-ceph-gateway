// src/rpc/internal/internal_rpc_client.cc
#include "rpc/internal/internal_rpc_client.h"
#include "common/logging.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace hcg {
namespace internal_rpc {

InternalRpcClient::InternalRpcClient(const std::string& host, std::uint16_t port)
    : host_(host), port_(port) {}

InternalRpcClient::~InternalRpcClient() {
    close();
}

bool InternalRpcClient::connect() {
    if (sock_fd_ >= 0) {
        return true;
    }

    sock_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd_ < 0) {
        log(LogLevel::ERROR, "InternalRpcClient: socket() failed: %s",
            std::strerror(errno));
        return false;
    }

    sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port_);
    if (::inet_pton(AF_INET, host_.c_str(), &addr.sin_addr) <= 0) {
        log(LogLevel::ERROR, "InternalRpcClient: invalid host=%s",
            host_.c_str());
        ::close(sock_fd_);
        sock_fd_ = -1;
        return false;
    }

    if (::connect(sock_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        log(LogLevel::ERROR, "InternalRpcClient: connect(%s:%u) failed: %s",
            host_.c_str(), port_, std::strerror(errno));
        ::close(sock_fd_);
        sock_fd_ = -1;
        return false;
    }

    log(LogLevel::INFO, "InternalRpcClient connected to %s:%u",
        host_.c_str(), port_);
    return true;
}

void InternalRpcClient::close() {
    if (sock_fd_ >= 0) {
        ::shutdown(sock_fd_, SHUT_RDWR);
        ::close(sock_fd_);
        sock_fd_ = -1;
    }
}

bool InternalRpcClient::read_full(int fd, void* buf, size_t len) {
    std::uint8_t* p = static_cast<std::uint8_t*>(buf);
    size_t nread = 0;
    while (nread < len) {
        ssize_t r = ::read(fd, p + nread, len - nread);
        if (r < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (r == 0) {
            // peer closed
            return false;
        }
        nread += static_cast<size_t>(r);
    }
    return true;
}

bool InternalRpcClient::write_full(int fd, const void* buf, size_t len) {
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

bool InternalRpcClient::call(MethodId method,
                             const ::google::protobuf::Message& req,
                             ::google::protobuf::Message& rsp,
                             MethodId expected_method) {
    if (sock_fd_ < 0 && !connect()) {
        return false;
    }

    std::string payload;
    if (!req.SerializeToString(&payload)) {
        log(LogLevel::ERROR, "InternalRpcClient: serialize request failed");
        return false;
    }

    std::uint16_t mid = htons(to_uint16(method));
    std::uint16_t reserved = 0;
    std::uint32_t len = htonl(static_cast<std::uint32_t>(
        sizeof(mid) + sizeof(reserved) + payload.size()));

    // 发送头+体
    if (!write_full(sock_fd_, &len, sizeof(len))) return false;
    if (!write_full(sock_fd_, &mid, sizeof(mid))) return false;
    if (!write_full(sock_fd_, &reserved, sizeof(reserved))) return false;
    if (!write_full(sock_fd_, payload.data(), payload.size())) return false;

    // 读响应 length
    std::uint32_t net_len = 0;
    if (!read_full(sock_fd_, &net_len, sizeof(net_len))) {
        log(LogLevel::ERROR, "InternalRpcClient: read response length failed");
        return false;
    }
    std::uint32_t resp_len = ntohl(net_len);
    if (resp_len < sizeof(std::uint16_t) * 2) {
        log(LogLevel::ERROR, "InternalRpcClient: invalid resp length=%u", resp_len);
        return false;
    }

    // 读 method_id + reserved + payload
    std::vector<std::uint8_t> buf(resp_len);
    if (!read_full(sock_fd_, buf.data(), buf.size())) {
        log(LogLevel::ERROR, "InternalRpcClient: read response body failed");
        return false;
    }

    std::uint16_t net_mid = 0;
    std::uint16_t resp_reserved = 0;
    std::memcpy(&net_mid, buf.data(), sizeof(net_mid));
    std::memcpy(&resp_reserved, buf.data() + sizeof(net_mid), sizeof(resp_reserved));
    (void)resp_reserved;
    MethodId resp_mid = method_from_uint16(ntohs(net_mid));

    if (resp_mid != expected_method) {
        log(LogLevel::ERROR,
            "InternalRpcClient: unexpected resp method=%u (expected %u)",
            static_cast<unsigned>(resp_mid),
            static_cast<unsigned>(expected_method));
        return false;
    }

    const std::uint8_t* resp_payload = buf.data() + sizeof(net_mid) + sizeof(resp_reserved);
    size_t resp_payload_len = buf.size() - sizeof(net_mid) - sizeof(resp_reserved);

    if (!rsp.ParseFromArray(resp_payload, static_cast<int>(resp_payload_len))) {
        log(LogLevel::ERROR, "InternalRpcClient: parse resp failed");
        return false;
    }

    return true;
}

// ---- 各个具体方法封装 ----

bool InternalRpcClient::CreateFile(const internal::CreateFileRequest& req,
                                   internal::CreateFileResponse& rsp) {
    return call(MethodId::CREATE_FILE, req, rsp, MethodId::CREATE_FILE);
}

bool InternalRpcClient::GetFileInfo(const internal::GetFileInfoRequest& req,
                                    internal::GetFileInfoResponse& rsp) {
    return call(MethodId::GET_FILE_INFO, req, rsp, MethodId::GET_FILE_INFO);
}

bool InternalRpcClient::ListStatus(const internal::ListStatusRequest& req,
                                   internal::ListStatusResponse& rsp) {
    return call(MethodId::LIST_STATUS, req, rsp, MethodId::LIST_STATUS);
}

bool InternalRpcClient::DeletePath(const internal::DeleteRequest& req,
                                   internal::DeleteResponse& rsp) {
    return call(MethodId::DELETE_PATH, req, rsp, MethodId::DELETE_PATH);
}

bool InternalRpcClient::AllocateBlock(const internal::AllocateBlockRequest& req,
                                      internal::AllocateBlockResponse& rsp) {
    return call(MethodId::ALLOCATE_BLOCK, req, rsp, MethodId::ALLOCATE_BLOCK);
}

bool InternalRpcClient::GetBlockLocations(const internal::GetBlockLocationsRequest& req,
                                          internal::GetBlockLocationsResponse& rsp) {
    return call(MethodId::GET_BLOCK_LOCATIONS, req, rsp, MethodId::GET_BLOCK_LOCATIONS);
}

bool InternalRpcClient::WriteBlock(const internal::WriteBlockRequest& req,
                                   internal::WriteBlockResponse& rsp) {
    return call(MethodId::WRITE_BLOCK, req, rsp, MethodId::WRITE_BLOCK);
}

bool InternalRpcClient::ReadBlock(const internal::ReadBlockRequest& req,
                                  internal::ReadBlockResponse& rsp) {
    return call(MethodId::READ_BLOCK, req, rsp, MethodId::READ_BLOCK);
}

bool InternalRpcClient::Complete(const internal::CompleteRequest& req,
                                 internal::CompleteResponse& rsp) {
    return call(MethodId::COMPLETE, req, rsp, MethodId::COMPLETE);
}

} // namespace internal_rpc
} // namespace hcg
