#include "protocol/namenode/namenode_rpc_connection.h"

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
#include "IpcConnectionContext.pb.h"

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include "common/logging.h" // 假设有日志库

namespace hcg {

using google::protobuf::io::ArrayInputStream;
using google::protobuf::io::CodedInputStream;
using google::protobuf::io::CodedOutputStream;
using google::protobuf::io::StringOutputStream;

NameRpcConnection::NameRpcConnection(
    int fd,
    std::shared_ptr<IHdfsNamenodeService> service)
    : fd_(fd), service_(std::move(service)) {
        log(LogLevel::DEBUG, "NameRpcConnection ctor this=%p, fd=%d", this, fd_);

        // LOG_DEBUG("NameRpcConnection ctor this=%p, fd=%d", this, fd_);
    }

NameRpcConnection::~NameRpcConnection() {
    if (fd_ >= 0) {
        ::shutdown(fd_, SHUT_RDWR);
        ::close(fd_);
        fd_ = -1;
    }
}

void NameRpcConnection::serve() {
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

bool NameRpcConnection::read_preamble() {
    // Hadoop RPC preamble: 4 bytes "hrpc" + 1 byte version + 1 byte service class + 1 byte auth protocol
    std::uint8_t preamble[7];
    if (!read_full(fd_, preamble, sizeof(preamble))) {
        return false;
    }

    if (std::memcmp(preamble, "hrpc", 4) != 0) {
        std::cerr << "[NameRpcConnection] invalid magic in preamble\n";
        return false;
    }

    std::uint8_t version = preamble[4];
    std::uint8_t service_class = preamble[5];
    std::uint8_t auth_protocol = preamble[6];

    (void)service_class; // 目前用不到

    // 这里只做最小约束：协议版本一般是 0x09，auth 只能是 NONE(0)
    if (auth_protocol != 0) {
        std::cerr << "[NameRpcConnection] unsupported auth protocol=" << (int)auth_protocol << "\n";
        return false;
    }

    // version 不严格限制，先接受，后面如果需要可以根据版本做兼容处理
    std::cerr << "[NameRpcConnection] preamble ok, version=" << (int)version << "\n";
    return true;
}

bool NameRpcConnection::handle_one_call() {
    // 1) 读 total_len + buf
    std::uint32_t net_len = 0;
    if (!read_full(fd_, &net_len, sizeof(net_len))) {
        return false;
    }
    std::uint32_t total_len = ntohl(net_len);
    if (total_len == 0) {
        std::cerr << "[NameRpcConnection] invalid packet length=0\n";
        return false;
    }

    std::vector<std::uint8_t> buf(total_len);
    if (!read_full(fd_, buf.data(), buf.size())) {
        return false;
    }

    ArrayInputStream ais(buf.data(), static_cast<int>(buf.size()));
    CodedInputStream cis(&ais);

    // 2) 先解析 RpcRequestHeaderProto
    ::hadoop::common::RpcRequestHeaderProto rpc_header;
    {
        std::uint32_t header_len = 0;
        if (!cis.ReadVarint32(&header_len)) {
            std::cerr << "[NameRpcConnection] failed to read RpcRequestHeader length\n";
            return false;
        }
        auto limit = cis.PushLimit(static_cast<int>(header_len));
        if (!rpc_header.ParseFromCodedStream(&cis)) {
            std::cerr << "[NameRpcConnection] failed to parse RpcRequestHeader\n";
            return false;
        }
        cis.PopLimit(limit);
    }

    int32_t call_id = rpc_header.callid();  // 注意是有符号
    // std::cerr << "call_id=" << call_id << " rpc_kind=" << rpc_header.rpckind() << "\n";

    // 3) 如果是握手（ConnectionContext），第二个消息是 IpcConnectionContextProto
    if (!handshake_done_ && call_id == -3) {
        ::hadoop::common::IpcConnectionContextProto ctx;
        std::uint32_t ctx_len = 0;
        if (!cis.ReadVarint32(&ctx_len)) {
            std::cerr << "[NameRpcConnection] failed to read IpcConnectionContext length\n";
            return false;
        }
        auto limit = cis.PushLimit(static_cast<int>(ctx_len));
        if (!ctx.ParseFromCodedStream(&cis)) {
            std::cerr << "[NameRpcConnection] failed to parse IpcConnectionContext\n";
            return false;
        }
        cis.PopLimit(limit);

        // 可以从 ctx 里取出 user / protocol 信息，保存到连接状态
        if (ctx.has_userinfo() && ctx.userinfo().has_effectiveuser()) {
            user_ = ctx.userinfo().effectiveuser();
        }
        if (ctx.has_protocol()) {
            auto protocol_name_ = ctx.protocol();
        }

        handshake_done_ = true;
        std::cerr << "[NameRpcConnection] connection context ok, user=" << user_
                  << " protocol=" << protocol_name_ << "\n";
        return true;  // 握手完成即可
    }

    // 4) 普通 RPC 请求：第二个消息才是 RequestHeaderProto
    ::hadoop::common::RequestHeaderProto req_header;
    {
        std::uint32_t header_len = 0;
        if (!cis.ReadVarint32(&header_len)) {
            std::cerr << "[NameRpcConnection] failed to read RequestHeader length\n";
            return false;
        }
        auto limit = cis.PushLimit(static_cast<int>(header_len));
        if (!req_header.ParseFromCodedStream(&cis)) {
            std::cerr << "[NameRpcConnection] failed to parse RequestHeader\n";
            return false;
        }
        cis.PopLimit(limit);
    }

    // 5) 剩余是 param：varint32 长度 + RequestProto
    std::string param_bytes;
    {
        std::uint32_t param_len = 0;
        if (!cis.ReadVarint32(&param_len)) {
            std::cerr << "[NameRpcConnection] failed to read param length\n";
            return false;
        }
        if (param_len > 0) {
            param_bytes.resize(param_len);
            if (!cis.ReadRaw(param_bytes.data(), param_len)) {
                std::cerr << "[NameRpcConnection] failed to read param bytes\n";
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
        resp_header.set_errormsg("Unimplemented or failed RPC method in NameRpcConnection::dispatch");
        resp_header.set_errordetail(
            ::hadoop::common::RpcResponseHeaderProto::ERROR_APPLICATION);
    }

    // 7) 序列化 response： [len][RpcResponseHeader][RequestProto]
    std::string header_bytes;
    if (!resp_header.SerializeToString(&header_bytes)) {
        std::cerr << "[NameRpcConnection] failed to serialize RpcResponseHeader\n";
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

bool NameRpcConnection::dispatch(
    const ::hadoop::common::RpcRequestHeaderProto& rpc_header,
    const ::hadoop::common::RequestHeaderProto& req_header,
    const std::string& param_bytes,
    std::string& out_response_bytes) {

    // log(LogLevel::DEBUG, "dispatch this=%p", this);
    log(LogLevel::DEBUG, "dispatch enter this=%p, call_id=%d, method=%s, param_len=%zu",
            (void*)this,
            rpc_header.callid(),
            req_header.methodname().c_str(),
            param_bytes.size());


    const std::string& proto_name = req_header.declaringclassprotocolname();
    const std::string& method     = req_header.methodname();

    log(LogLevel::DEBUG, "dispatch: call_id=%d method=%s", rpc_header.callid(), method.c_str());

    // 目前只打算处理 ClientProtocol，其它协议直接报错
    if (proto_name != "org.apache.hadoop.hdfs.protocol.ClientProtocol") {
        std::cerr << "[NameRpcConnection] unsupported protocol: " << proto_name << "\n";
        return false;
    }

    std::cerr << "[NameRpcConnection] callId=" << rpc_header.callid()
              << " method=" << method << "\n";

    // ======= 下面是具体方法的分发骨架 =======

    if (method == "mkdirs") {
        ::hadoop::hdfs::MkdirsRequestProto req;
        req.ParseFromString(param_bytes);
        ::hadoop::hdfs::MkdirsResponseProto rsp;
        service_->mkdirs(req, rsp);
        
        rsp.SerializeToString(&out_response_bytes);
        return true;

    } else if (method == "getFileInfo") {
        ::hadoop::hdfs::GetFileInfoRequestProto req;
        ::hadoop::hdfs::GetFileInfoResponseProto rsp;
        req.ParseFromString(param_bytes);
        service_->getFileInfo(req, rsp);
        rsp.SerializeToString(&out_response_bytes);
        return true;

    } else if (method == "getServerDefaults") {
        ::hadoop::hdfs::GetServerDefaultsRequestProto req;
        ::hadoop::hdfs::GetServerDefaultsResponseProto rsp;
        req.ParseFromString(param_bytes);
        service_->getServerDefaults(req, rsp);
        rsp.SerializeToString(&out_response_bytes);
        return true;

    } else if (method == "getFsStats") {
        // TODO:
        ::hadoop::hdfs::GetFsStatusRequestProto req;
        ::hadoop::hdfs::GetFsStatsResponseProto rsp;
        req.ParseFromString(param_bytes);
        service_->getFsStatus(req, rsp);
        rsp.SerializeToString(&out_response_bytes);
        return true;

    } else if (method == "listStatus" || method == "getListing") {
        // HDFS 客户端常用 getListing，对应 ClientNamenodeProtocol 里的 GetListingRequestProto
        // TODO:
        ::hadoop::hdfs::GetListingRequestProto req;
        ::hadoop::hdfs::GetListingResponseProto rsp;
        req.ParseFromString(param_bytes);
        service_->listStatus(req, rsp);
        rsp.SerializeToString(&out_response_bytes);
        return true;

    } else if (method == "delete") {
        // TODO:
        ::hadoop::hdfs::DeleteRequestProto req;
        ::hadoop::hdfs::DeleteResponseProto rsp;
        req.ParseFromString(param_bytes);
        service_->deletePath(req, rsp);
        rsp.SerializeToString(&out_response_bytes);
        return true;

    } else if (method == "create") {
        // TODO:
        ::hadoop::hdfs::CreateRequestProto req;
        ::hadoop::hdfs::CreateResponseProto rsp;
        req.ParseFromString(param_bytes);
        service_->create(req, rsp);
        rsp.SerializeToString(&out_response_bytes);
        return true;

    } else if (method == "addBlock") {
        ::hadoop::hdfs::AddBlockRequestProto req;
        ::hadoop::hdfs::AddBlockResponseProto rsp;
        req.ParseFromString(param_bytes);
        service_->addBlock(req, rsp);
        rsp.SerializeToString(&out_response_bytes);
        return true;

    }  else if (method == "getBlockLocation") {
        ::hadoop::hdfs::GetBlockLocationsRequestProto req;
        ::hadoop::hdfs::GetBlockLocationsResponseProto rsp;
        req.ParseFromString(param_bytes);
        service_->getBlockLocation(req, rsp);
        rsp.SerializeToString(&out_response_bytes);
        return true;

    }  else if (method == "complete") {
        // TODO:
        ::hadoop::hdfs::CompleteRequestProto req;
        ::hadoop::hdfs::CompleteResponseProto rsp;
        req.ParseFromString(param_bytes);
        service_->complete(req, rsp);
        rsp.SerializeToString(&out_response_bytes);
        return true;

    }  else if (method == "abandonBlock")  {
        hadoop::hdfs::AbandonBlockRequestProto req;
        hadoop::hdfs::AbandonBlockResponseProto rsp;
        service_->abandonBlock(req, rsp);
        return true;
    } else {
        std::cerr << "[NameRpcConnection] unsupported method: " << method << "\n";
        return false;
    }
}

// ================ 工具函数：读 / 写完整缓冲区 =================

bool NameRpcConnection::read_full(int fd, void* buf, size_t len) {
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
            std::cerr << "[NameRpcConnection] read error: " << std::strerror(errno) << "\n";
            return false;
        }
        off += static_cast<size_t>(n);
    }
    return true;
}

bool NameRpcConnection::write_full(int fd, const void* buf, size_t len) {
    const std::uint8_t* p = static_cast<const std::uint8_t*>(buf);
    size_t off = 0;
    while (off < len) {
        ssize_t n = ::write(fd, p + off, len - off);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) {
                continue;
            }
            std::cerr << "[NameRpcConnection] write error: " << std::strerror(errno) << "\n";
            return false;
        }
        off += static_cast<size_t>(n);
    }
    return true;
}

} // namespace hcg
