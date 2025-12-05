#include "protocol/namenode/hdfs_rpc_connection.h"

#include <arpa/inet.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include "RpcHeader.pb.h"             // hadoop.common.RpcRequestHeaderProto / RpcResponseHeaderProto
#include "ProtobufRpcEngine.pb.h"     // hadoop.common.RequestHeaderProto
#include "ClientNamenodeProtocol.pb.h"// hadoop.hdfs.*RequestProto / *ResponseProto

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

namespace hcg {

using google::protobuf::io::ArrayInputStream;
using google::protobuf::io::CodedInputStream;
using google::protobuf::io::CodedOutputStream;
using google::protobuf::io::StringOutputStream;

HdfsRpcConnection::HdfsRpcConnection(
    int fd,
    std::shared_ptr<IHdfsNamenodeService> service)
    : fd_(fd), service_(std::move(service)) {}

HdfsRpcConnection::~HdfsRpcConnection() {
    if (fd_ >= 0) {
        ::shutdown(fd_, SHUT_RDWR);
        ::close(fd_);
        fd_ = -1;
    }
}

void HdfsRpcConnection::serve() {
    // 1) 先做握手："hrpc" + version + service_class + auth_protocol
    if (!read_preamble()) {
        // 握手失败，直接关闭
        return;
    }

    // 2) 循环处理每一个 RPC 请求
    while (true) {
        if (!handle_one_call()) {
            break;
        }
    }
}

bool HdfsRpcConnection::read_preamble() {
    // Hadoop RPC preamble: 4 bytes "hrpc" + 1 byte version + 1 byte service class + 1 byte auth protocol
    std::uint8_t preamble[7];
    if (!read_full(fd_, preamble, sizeof(preamble))) {
        return false;
    }

    if (std::memcmp(preamble, "hrpc", 4) != 0) {
        std::cerr << "[HdfsRpcConnection] invalid magic in preamble\n";
        return false;
    }

    std::uint8_t version = preamble[4];
    std::uint8_t service_class = preamble[5];
    std::uint8_t auth_protocol = preamble[6];

    (void)service_class; // 目前用不到

    // 这里只做最小约束：协议版本一般是 0x09，auth 只能是 NONE(0)
    if (auth_protocol != 0) {
        std::cerr << "[HdfsRpcConnection] unsupported auth protocol=" << (int)auth_protocol << "\n";
        return false;
    }

    // version 不严格限制，先接受，后面如果需要可以根据版本做兼容处理
    std::cerr << "[HdfsRpcConnection] preamble ok, version=" << (int)version << "\n";
    return true;
}

bool HdfsRpcConnection::handle_one_call() {
    // 1) 先读 4 字节的 total length（network byte order）
    std::uint32_t net_len = 0;
    if (!read_full(fd_, &net_len, sizeof(net_len))) {
        // 正常关闭或读错，都视为连接结束
        return false;
    }

    std::uint32_t total_len = ntohl(net_len);
    if (total_len == 0) {
        std::cerr << "[HdfsRpcConnection] invalid packet length=0\n";
        return false;
    }

    std::vector<std::uint8_t> buf(total_len);
    if (!read_full(fd_, buf.data(), buf.size())) {
        return false;
    }

    ArrayInputStream ais(buf.data(), static_cast<int>(buf.size()));
    CodedInputStream cis(&ais);

    // 2) 解析 RpcRequestHeaderProto（带自己的 length 前缀）
    ::hadoop::common::RpcRequestHeaderProto rpc_header;
    {
        std::uint32_t header_len = 0;
        if (!cis.ReadVarint32(&header_len)) {
            std::cerr << "[HdfsRpcConnection] failed to read RpcRequestHeader length\n";
            return false;
        }
        auto limit = cis.PushLimit(static_cast<int>(header_len));
        if (!rpc_header.ParseFromCodedStream(&cis)) {
            std::cerr << "[HdfsRpcConnection] failed to parse RpcRequestHeader\n";
            return false;
        }
        cis.PopLimit(limit);
    }

    // 3) 解析 RequestHeaderProto（同样带 length 前缀）
    ::hadoop::common::RequestHeaderProto req_header;
    {
        std::uint32_t header_len = 0;
        if (!cis.ReadVarint32(&header_len)) {
            std::cerr << "[HdfsRpcConnection] failed to read RequestHeader length\n";
            return false;
        }
        auto limit = cis.PushLimit(static_cast<int>(header_len));
        if (!req_header.ParseFromCodedStream(&cis)) {
            std::cerr << "[HdfsRpcConnection] failed to parse RequestHeader\n";
            return false;
        }
        cis.PopLimit(limit);
    }

    // 4) 剩下的是 param（方法的 RequestProto），一般也有一个 varint32 length 前缀
    std::string param_bytes;
    if (!cis.ConsumedEntireMessage()) {
        std::uint32_t param_len = 0;
        if (!cis.ReadVarint32(&param_len)) {
            std::cerr << "[HdfsRpcConnection] failed to read param length\n";
            return false;
        }
        if (param_len > 0) {
            param_bytes.resize(param_len);
            if (!cis.ReadRaw(param_bytes.data(), param_len)) {
                std::cerr << "[HdfsRpcConnection] failed to read param bytes\n";
                return false;
            }
        }
    }

    // 5) 调度，调用 IHdfsNamenodeService
    std::string resp_param_bytes;
    bool ok = dispatch(rpc_header, req_header, param_bytes, resp_param_bytes);

    // 6) 构造 RpcResponseHeaderProto
    ::hadoop::common::RpcResponseHeaderProto resp_header;
    resp_header.set_callid(rpc_header.callid());
    resp_header.set_clientid(rpc_header.clientid());

    if (ok) {
        resp_header.set_status(::hadoop::common::RpcResponseHeaderProto::SUCCESS);
    } else {
        resp_header.set_status(::hadoop::common::RpcResponseHeaderProto::ERROR);
        resp_header.set_exceptionclassname("java.io.IOException");
        resp_header.set_errormsg("Unimplemented or failed RPC method in HdfsRpcConnection::dispatch");
        resp_header.set_errordetail(
            ::hadoop::common::RpcResponseHeaderProto::ERROR_APPLICATION);
    }

    // 7) 序列化 response： [len][RpcResponseHeader][RequestProto]
    std::string header_bytes;
    if (!resp_header.SerializeToString(&header_bytes)) {
        std::cerr << "[HdfsRpcConnection] failed to serialize RpcResponseHeader\n";
        return false;
    }

    std::string payload; // 不含前 4 字节 length
    {
        StringOutputStream sos(&payload);
        CodedOutputStream cos(&sos);

        // header 部分：length-delimited
        cos.WriteVarint32(static_cast<std::uint32_t>(header_bytes.size()));
        cos.WriteRaw(header_bytes.data(), header_bytes.size());

        // param 部分：也写一个 length（即使为 0）
        cos.WriteVarint32(static_cast<std::uint32_t>(resp_param_bytes.size()));
        if (!resp_param_bytes.empty()) {
            cos.WriteRaw(resp_param_bytes.data(), resp_param_bytes.size());
        }
    }

    std::uint32_t out_len = htonl(static_cast<std::uint32_t>(payload.size()));
    if (!write_full(fd_, &out_len, sizeof(out_len))) {
        return false;
    }
    if (!write_full(fd_, payload.data(), payload.size())) {
        return false;
    }

    return true;
}

bool HdfsRpcConnection::dispatch(
    const ::hadoop::common::RpcRequestHeaderProto& rpc_header,
    const ::hadoop::common::RequestHeaderProto& req_header,
    const std::string& param_bytes,
    std::string& out_response_bytes) {

    const std::string& proto_name = req_header.declaringclassprotocolname();
    const std::string& method     = req_header.methodname();

    // 目前只打算处理 ClientNamenodeProtocol，其它协议直接报错
    if (proto_name != "org.apache.hadoop.hdfs.protocol.ClientNamenodeProtocol") {
        std::cerr << "[HdfsRpcConnection] unsupported protocol: " << proto_name << "\n";
        return false;
    }

    std::cerr << "[HdfsRpcConnection] callId=" << rpc_header.callid()
              << " method=" << method << "\n";

    // ======= 下面是具体方法的分发骨架，全部用 TODO 标出来，后续你来补 =======

    if (method == "mkdirs") {
        // TODO:
        // 1. 解析 param_bytes 为 hadoop::hdfs::MkdirsRequestProto
        // 2. 调用 service_->mkdirs(req, rsp);
        // 3. 把 rsp SerializeToString 到 out_response_bytes
        //
        // 示例（实现时）：
        //
        // ::hadoop::hdfs::MkdirsRequestProto req;
        // req.ParseFromString(param_bytes);
        //
        // ::hadoop::hdfs::MkdirsResponseProto rsp;
        // service_->mkdirs(req, rsp);
        //
        // rsp.SerializeToString(&out_response_bytes);
        //
        // 这里先返回 false，让上层走 ERROR 流程，等你实现完再改为 true。
        return false;

    } else if (method == "getFileInfo") {
        // TODO: 类似 mkdirs:
        // ::hadoop::hdfs::GetFileInfoRequestProto req;
        // ::hadoop::hdfs::GetFileInfoResponseProto rsp;
        // req.ParseFromString(param_bytes);
        // service_->getFileInfo(req, rsp);
        // rsp.SerializeToString(&out_response_bytes);
        return false;

    } else if (method == "getServerDefaults") {
        // TODO: 建议优先实现这个，方便最小闭环测试
        // ::hadoop::hdfs::GetServerDefaultsRequestProto req;
        // ::hadoop::hdfs::GetServerDefaultsResponseProto rsp;
        // req.ParseFromString(param_bytes);
        // service_->getServerDefaults(req, rsp);
        // rsp.SerializeToString(&out_response_bytes);
        return false;

    } else if (method == "getFsStatus") {
        // TODO:
        // ::hadoop::hdfs::GetFsStatusRequestProto req;
        // ::hadoop::hdfs::GetFsStatusResponseProto rsp;
        // ...
        return false;

    } else if (method == "listStatus" || method == "getListing") {
        // HDFS 客户端常用 getListing，对应 ClientNamenodeProtocol 里的 GetListingRequestProto
        // TODO:
        // ::hadoop::hdfs::GetListingRequestProto req;
        // ::hadoop::hdfs::GetListingResponseProto rsp;
        // ...
        return false;

    } else if (method == "delete") {
        // TODO:
        // ::hadoop::hdfs::DeleteRequestProto req;
        // ::hadoop::hdfs::DeleteResponseProto rsp;
        // ...
        return false;

    } else if (method == "create") {
        // TODO:
        // ::hadoop::hdfs::CreateRequestProto req;
        // ::hadoop::hdfs::CreateResponseProto rsp;
        // ...
        return false;

    } else if (method == "addBlock") {
        // TODO:
        // ::hadoop::hdfs::AddBlockRequestProto req;
        // ::hadoop::hdfs::AddBlockResponseProto rsp;
        // ...
        return false;

    } else if (method == "complete") {
        // TODO:
        // ::hadoop::hdfs::CompleteRequestProto req;
        // ::hadoop::hdfs::CompleteResponseProto rsp;
        // ...
        return false;

    } else {
        std::cerr << "[HdfsRpcConnection] unsupported method: " << method << "\n";
        return false;
    }
}

// ================ 工具函数：读 / 写完整缓冲区 =================

bool HdfsRpcConnection::read_full(int fd, void* buf, size_t len) {
    std::uint8_t* p = static_cast<std::uint8_t*>(buf);
    size_t off = 0;
    while (off < len) {
        ssize_t n = ::read(fd, p + off, len - off);
        if (n == 0) {
            // 对端关闭
            return false;
        }
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::cerr << "[HdfsRpcConnection] read error: " << std::strerror(errno) << "\n";
            return false;
        }
        off += static_cast<size_t>(n);
    }
    return true;
}

bool HdfsRpcConnection::write_full(int fd, const void* buf, size_t len) {
    const std::uint8_t* p = static_cast<const std::uint8_t*>(buf);
    size_t off = 0;
    while (off < len) {
        ssize_t n = ::write(fd, p + off, len - off);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) {
                continue;
            }
            std::cerr << "[HdfsRpcConnection] write error: " << std::strerror(errno) << "\n";
            return false;
        }
        off += static_cast<size_t>(n);
    }
    return true;
}

} // namespace hcg
