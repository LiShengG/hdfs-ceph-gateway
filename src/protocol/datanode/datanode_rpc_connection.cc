#include "protocol/datanode/datanode_rpc_connection.h"
#include "common/logging.h" 

#include "datatransfer.pb.h"
#include "hdfs.pb.h"
#include "ClientDatanodeProtocol.pb.h"
#include "RpcHeader.pb.h"
#include "IpcConnectionContext.pb.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <unistd.h>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <sstream>   // 必须！用于 std::ostringstream
#include <iomanip>   // 必须！用于 std::setw, std::setfill
#include <algorithm> // 可能需要 std::min
#include <poll.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <errno.h>
#include <string.h>

using namespace google::protobuf::io;
using namespace hadoop::common;
using namespace hadoop::hdfs;

namespace hcg {

DataRpcConnection::DataRpcConnection(int fd, std::shared_ptr<IHdfsDatanodeService> service)
    : fd_(fd), service_(std::move(service)) {
}

DataRpcConnection::~DataRpcConnection() = default;

std::string hex_dump(const uint8_t* data, size_t len) {
    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    for (size_t i = 0; i < len; ++i) {
        if (i > 0) oss << ' ';
        oss << std::setw(2) << static_cast<int>(data[i]);
    }
    return oss.str();
}

static bool wait_fd(int fd, short events, int timeout_ms, short* revents_out=nullptr) {
    struct pollfd pfd{};
    pfd.fd = fd;
    pfd.events = events;

    for (;;) {
        int rc = ::poll(&pfd, 1, timeout_ms);
        if (rc > 0) break;
        if (rc == 0) { if (revents_out) *revents_out = pfd.revents; return false; }
        if (errno == EINTR) continue;
        if (revents_out) *revents_out = pfd.revents;
        return false;
    }

    if (revents_out) *revents_out = pfd.revents;

    // 关键：HUP/ERR/NVAL 直接判定连接不可用
    if (pfd.revents & (POLLHUP | POLLERR | POLLNVAL)) {
        return false;
    }

    return (pfd.revents & events) != 0;
}


// peek 至少 want 字节（MSG_PEEK，不消费），支持 non-blocking
bool DataRpcConnection::peek_at_least(size_t want, std::vector<uint8_t>& out, int timeout_ms) {
    out.clear();
    out.resize(want);

    int elapsed = 0;
    const int step = 500; // 0.5s step
    while (elapsed < timeout_ms) {
        ssize_t n = ::recv(fd_, out.data(), want, MSG_PEEK);
        // log(LogLevel::DEBUG, "peek: recv(MSG_PEEK) errno=%d(%s)", , strerror(errno));
        log(LogLevel::DEBUG, "peek_at_least) n = %d", n);
        if (n >= (ssize_t)want) {
            return true;
        }
        if (n > 0) {
            out.resize((size_t)n);
            // 还不够 want，继续等
        } else if (n == 0) {
            out.clear();
            log(LogLevel::INFO, "Client closed connection before sending data");
            return false;
        } else {
            if (errno == EINTR) continue;
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                log(LogLevel::DEBUG, "peek: recv(MSG_PEEK) errno=%d(%s)", errno, strerror(errno));
                out.clear();
                return false;
            }
        }

        short rev = 0;
        if (!wait_fd(fd_, POLLIN, step, &rev)) {
            // step 超时或关闭：继续累计 elapsed，看是否整体超时
            elapsed += step;
            continue;
        }
        elapsed += step;
    }

    // overall timeout
    // out 里可能有部分 bytes（如果 n>0 的情况发生过）
    return false;
}


// ==================== 主服务循环 ====================
void DataRpcConnection::serve() {
    // 检测协议类型
    protocol_type_ = detect_protocol();

    switch (protocol_type_) {
        case ProtocolType::DATA_TRANSFER:
            log(LogLevel::DEBUG, "Detected DataTransfer protocol");
            serve_data_transfer();
            break;
        case ProtocolType::IPC_RPC:
            log(LogLevel::DEBUG, "Detected IPC RPC protocol");
            serve_ipc_rpc();
            break;
        case ProtocolType::DATA_TRANSFER_SASL:
            log(LogLevel::DEBUG, "Detected SASL-wrapped DataTransfer protocol");
            // serve_data_transfer_sasl(); // 先只做打印/报错也行
            break;
        default:
            log(LogLevel::ERROR, "Unknown protocol");
            break;
    }
}

// ==================== 协议检测 ====================

DataRpcConnection::ProtocolType DataRpcConnection::detect_protocol() {
    std::uint8_t b[4] = {0};
    int n = ::recv(fd_, b, sizeof(b), MSG_PEEK);

    if (n <= 0) {
        if (n == 0) {
            log(LogLevel::INFO, "Connection closed by peer during protocol detection");
        } else {
            log(LogLevel::ERROR, "recv(MSG_PEEK) failed: %s", strerror(errno));
        }
        return ProtocolType::UNKNOWN;
    }

    if (static_cast<size_t>(n) < 2) {
        log(LogLevel::WARN, "Insufficient data for protocol detection (got %d bytes)", n);
        return ProtocolType::UNKNOWN;
    }

    // HDFS DataTransfer protocol: big-endian uint16 version
    std::uint16_t version = (static_cast<std::uint16_t>(b[0]) << 8) |
                             static_cast<std::uint16_t>(b[1]);

    // Known versions: 28 (standard), 29 (with encryption/SASL)
    if (version == 28) {
        return ProtocolType::DATA_TRANSFER;
    }
    if (version == 29) {
        return ProtocolType::DATA_TRANSFER_SASL;
    }

    // TODO: Add detection for HRPC (IPC_RPC) — usually starts with 'hrpc' magic
    // For now, assume non-versioned traffic is IPC (may need refinement)
    log(LogLevel::DEBUG, "Unrecognized protocol version: %u", version);
    return ProtocolType::IPC_RPC; // or UNKNOWN, depending on your design
}


static const char* dt_op_name(uint8_t op) {
    switch (op) {
        case 80: return "WRITE_BLOCK";
        case 81: return "READ_BLOCK";
        case 82: return "READ_METADATA";
        case 83: return "REPLACE_BLOCK";
        case 84: return "COPY_BLOCK";
        case 85: return "BLOCK_CHECKSUM";
        case 86: return "TRANSFER_BLOCK";
        case 87: return "REQUEST_SHORT_CIRCUIT_FDS";
        case 88: return "RELEASE_SHORT_CIRCUIT_FDS";
        case 89: return "REQUEST_SHORT_CIRCUIT_SHM";
        case 90: return "BLOCK_GROUP_CHECKSUM";
        case 127:return "CUSTOM";
        default: return "UNKNOWN_OP";
    }
}


// ==================== DataTransfer 协议处理 ====================
void DataRpcConnection::serve_data_transfer() {
    // 读 DataTransfer 握手：u16 version (big-endian) + u8 op
    uint16_t ver_be = 0;
    uint8_t op = 0;

    if (!read_full(&ver_be, sizeof(ver_be))) {
        log(LogLevel::ERROR, "DataTransfer: failed to read version");
        return;
    }
    uint16_t ver = ntohs(ver_be);

    if (!read_full(&op, sizeof(op))) {
        log(LogLevel::ERROR, "DataTransfer: failed to read op");
        return;
    }

    log(LogLevel::INFO, "DataTransfer handshake: version=%u op=%u (%s)",
        ver, op, dt_op_name(op));

    if (ver != DATA_TRANSFER_VERSION) {
        log(LogLevel::ERROR, "DataTransfer: version mismatch expect=%u got=%u",
            (unsigned)DATA_TRANSFER_VERSION, (unsigned)ver);
        return;
    }

    // 一个连接通常只处理一个 op
    if (!handle_data_transfer_op(op)) {
        log(LogLevel::ERROR, "Failed to handle data transfer operation op=%u (%s)",
            op, dt_op_name(op));
    }
}

bool DataRpcConnection::handle_data_transfer_op(uint8_t op_code) {
    log(LogLevel::DEBUG, "DataTransfer op=%u (%s)", op_code, dt_op_name(op_code));

    switch (static_cast<DataTransferOp>(op_code)) {
        case DataTransferOp::READ_BLOCK:
            return handle_read_block();
        case DataTransferOp::WRITE_BLOCK:
            return handle_write_block();
        case DataTransferOp::TRANSFER_BLOCK:
            return handle_transfer_block();
        case DataTransferOp::COPY_BLOCK:
            return handle_copy_block();
        case DataTransferOp::REPLACE_BLOCK:
            return handle_replace_block();
        case DataTransferOp::BLOCK_CHECKSUM:
            return handle_block_checksum();
        case DataTransferOp::REQUEST_SHORT_CIRCUIT_FDS:
            return handle_request_short_circuit();
        default:
            log(LogLevel::ERROR, "Unknown DataTransfer operation: %u (%s)",
                op_code, dt_op_name(op_code));
            return false;
    }
}

// ==================== 读取块处理 ====================
bool DataRpcConnection::handle_read_block() {
    // 读取 OpReadBlockProto
    OpReadBlockProto req;
    if (!read_proto_delimited(req)) {
        log(LogLevel::ERROR, "Failed to read OpReadBlockProto");
        return false;
    }

    const auto& header = req.header();
    const auto& block = header.baseheader().block();
    
    log(LogLevel::INFO, "READ_BLOCK: block_id=%lu, offset=%lu, len=%lu",
        block.blockid(), req.offset(), req.len());

    // 调用服务层
    BlockOpResponseProto rsp;
    std::vector<char> data;
    service_->readBlock(req, rsp, data);

    // 发送响应头
    if (!send_block_op_response(rsp)) {
        return false;
    }

    if (rsp.status() != SUCCESS) {
        return true;  // 错误响应已发送
    }

    // 发送数据包
    // 数据格式: [packet_len(4)] [header_len(2)] [PacketHeaderProto] [checksums] [data]
    return send_block_data(data, req.offset());
}

// ==================== 写入块处理 ====================
bool DataRpcConnection::handle_write_block() {
    // 读取 OpWriteBlockProto
    OpWriteBlockProto req;
    if (!read_proto_delimited(req)) {
        log(LogLevel::ERROR, "Failed to read OpWriteBlockProto");
        return false;
    }

    const auto& header = req.header();
    const auto& block = header.baseheader().block();
    
    log(LogLevel::INFO, "WRITE_BLOCK: block_id=%lu, stage=%d, targets=%d",
        block.blockid(), req.stage(), req.targets_size());

    // 发送初始响应（表示准备接收数据）
    BlockOpResponseProto initial_rsp;
    initial_rsp.set_status(SUCCESS);
    
    // 设置管道确认
    // if (req.targets_size() > 0) {
    //     auto* downstream_ack = initial_rsp.add_downstreamacknowledgment();
    //     // 填充下游确认信息
    // }
    
    if (!send_block_op_response(initial_rsp)) {
        return false;
    }

    // 接收数据包
    std::vector<char> block_data;
    uint64_t bytes_received = 0;
    
    while (true) {
        // 读取包头: packet_len(4) + header_len(2)
        uint32_t packet_len;
        uint16_t header_len;
        
        if (!read_full(&packet_len, sizeof(packet_len))) {
            log(LogLevel::ERROR, "Failed to read packet length");
            return false;
        }
        packet_len = ntohl(packet_len);
        
        if (!read_full(&header_len, sizeof(header_len))) {
            log(LogLevel::ERROR, "Failed to read header length");
            return false;
        }
        header_len = ntohs(header_len);

        // 读取 PacketHeaderProto
        std::vector<uint8_t> header_buf(header_len);
        if (!read_full(header_buf.data(), header_len)) {
            log(LogLevel::ERROR, "Failed to read packet header");
            return false;
        }

        PacketHeaderProto packet_header;
        if (!packet_header.ParseFromArray(header_buf.data(), header_len)) {
            log(LogLevel::ERROR, "Failed to parse PacketHeaderProto");
            return false;
        }

        int64_t offset_in_block = packet_header.offsetinblock();
        int64_t seq_no = packet_header.seqno();
        bool last_packet = packet_header.lastpacketinblock();
        int32_t data_len = packet_header.datalen();

        log(LogLevel::DEBUG, "Packet: seq=%ld, offset=%ld, len=%d, last=%d",
            seq_no, offset_in_block, data_len, last_packet);

        // 读取校验和和数据
        // 校验和大小取决于配置，这里简化处理
        uint32_t checksum_len = (data_len + 511) / 512 * 4;  // 假设 CRC32, 512字节一个
        
        std::vector<uint8_t> checksums(checksum_len);
        if (checksum_len > 0 && !read_full(checksums.data(), checksum_len)) {
            log(LogLevel::ERROR, "Failed to read checksums");
            return false;
        }

        // 读取数据
        if (data_len > 0) {
            size_t old_size = block_data.size();
            block_data.resize(old_size + data_len);
            if (!read_full(block_data.data() + old_size, data_len)) {
                log(LogLevel::ERROR, "Failed to read data");
                return false;
            }
            bytes_received += data_len;
        }

        // 发送 ACK
        PipelineAckProto ack;
        ack.set_seqno(seq_no);
        ack.add_reply(SUCCESS);
        
        // 序列化并发送
        std::string ack_bytes;
        ack.SerializeToString(&ack_bytes);
        uint32_t ack_len = htonl(ack_bytes.size());
        
        if (!write_full(&ack_len, sizeof(ack_len))) {
            return false;
        }
        if (!write_full(ack_bytes.data(), ack_bytes.size())) {
            return false;
        }

        if (last_packet) {
            break;
        }
    }

    // 调用服务层保存数据
    BlockOpResponseProto final_rsp;
    service_->writeBlock(req, block_data, final_rsp);

    log(LogLevel::INFO, "WRITE_BLOCK complete: received %zu bytes", block_data.size());
    return true;
}

// ==================== 传输块处理 ====================
bool DataRpcConnection::handle_transfer_block() {
    OpTransferBlockProto req;
    if (!read_proto_delimited(req)) {
        log(LogLevel::ERROR, "Failed to read OpTransferBlockProto");
        return false;
    }

    BlockOpResponseProto rsp;
    service_->transferBlock(req, rsp);

    return send_block_op_response(rsp);
}

// ==================== 复制块处理 ====================
bool DataRpcConnection::handle_copy_block() {
    OpCopyBlockProto req;
    if (!read_proto_delimited(req)) {
        log(LogLevel::ERROR, "Failed to read OpCopyBlockProto");
        return false;
    }

    BlockOpResponseProto rsp;
    service_->copyBlock(req, rsp);

    return send_block_op_response(rsp);
}

// ==================== 替换块处理 ====================
bool DataRpcConnection::handle_replace_block() {
    OpReplaceBlockProto req;
    if (!read_proto_delimited(req)) {
        log(LogLevel::ERROR, "Failed to read OpReplaceBlockProto");
        return false;
    }

    BlockOpResponseProto rsp;
    service_->replaceBlock(req, rsp);

    return send_block_op_response(rsp);
}

// ==================== 块校验和处理 ====================
bool DataRpcConnection::handle_block_checksum() {
    OpBlockChecksumProto req;
    if (!read_proto_delimited(req)) {
        log(LogLevel::ERROR, "Failed to read OpBlockChecksumProto");
        return false;
    }

    const auto& block = req.header().block();
    log(LogLevel::DEBUG, "BLOCK_CHECKSUM: block_id=%lu", block.blockid());

    // 构造响应
    BlockOpResponseProto rsp;
    rsp.set_status(SUCCESS);
    
    // 获取块校验和
    // GetBlockChecksumRequestProto checksum_req;
    // checksum_req.mutable_block()->CopyFrom(block);
    
    // GetBlockChecksumResponseProto checksum_rsp;
    // service_->getBlockChecksum(checksum_req, checksum_rsp);
    
    // 设置校验和信息
    auto* checksum_info = rsp.mutable_readopchecksuminfo();
    auto* checksum = checksum_info->mutable_checksum();
    checksum->set_type(CHECKSUM_CRC32);
    checksum->set_bytesperchecksum(512);

    return send_block_op_response(rsp);
}

// ==================== 短路读处理 ====================
bool DataRpcConnection::handle_request_short_circuit() {
    OpRequestShortCircuitAccessProto req;
    if (!read_proto_delimited(req)) {
        log(LogLevel::ERROR, "Failed to read OpRequestShortCircuitAccessProto");
        return false;
    }

    BlockOpResponseProto rsp;
    service_->requestShortCircuitAccess(req, rsp);

    return send_block_op_response(rsp);
}

// ==================== 发送块数据 ====================
bool DataRpcConnection::send_block_data(const std::vector<char>& data, uint64_t offset) {
    // 分包发送，每个包最大 64KB
    const size_t MAX_PACKET_SIZE = 64 * 1024;
    const size_t BYTES_PER_CHECKSUM = 512;
    
    size_t remaining = data.size();
    size_t data_offset = 0;
    int64_t seq_no = 0;

    while (remaining > 0 || seq_no == 0) {
        size_t chunk_size = std::min(remaining, MAX_PACKET_SIZE);
        bool last_packet = (remaining <= MAX_PACKET_SIZE);

        // 构造 PacketHeaderProto
        PacketHeaderProto packet_header;
        packet_header.set_offsetinblock(offset + data_offset);
        packet_header.set_seqno(seq_no);
        packet_header.set_lastpacketinblock(last_packet);
        packet_header.set_datalen(chunk_size);

        std::string header_bytes;
        packet_header.SerializeToString(&header_bytes);

        // 计算校验和
        size_t num_chunks = (chunk_size + BYTES_PER_CHECKSUM - 1) / BYTES_PER_CHECKSUM;
        std::vector<uint32_t> checksums(num_chunks);
        
        for (size_t i = 0; i < num_chunks; ++i) {
            size_t chunk_start = data_offset + i * BYTES_PER_CHECKSUM;
            size_t chunk_len = std::min(BYTES_PER_CHECKSUM, data.size() - chunk_start);
            
            // 简单 CRC32 计算
            uint32_t crc = 0xFFFFFFFF;
            for (size_t j = 0; j < chunk_len; ++j) {
                crc ^= static_cast<uint8_t>(data[chunk_start + j]);
                for (int k = 0; k < 8; ++k) {
                    crc = (crc >> 1) ^ (0xEDB88320 & -(crc & 1));
                }
            }
            checksums[i] = htonl(~crc);
        }

        // 计算包长度
        uint32_t packet_len = header_bytes.size() + checksums.size() * 4 + chunk_size;
        uint16_t header_len = header_bytes.size();

        // 发送: [packet_len(4)][header_len(2)][header][checksums][data]
        uint32_t net_packet_len = htonl(packet_len);
        uint16_t net_header_len = htons(header_len);

        if (!write_full(&net_packet_len, sizeof(net_packet_len))) return false;
        if (!write_full(&net_header_len, sizeof(net_header_len))) return false;
        if (!write_full(header_bytes.data(), header_bytes.size())) return false;
        if (!checksums.empty()) {
            if (!write_full(checksums.data(), checksums.size() * 4)) return false;
        }
        if (chunk_size > 0) {
            if (!write_full(data.data() + data_offset, chunk_size)) return false;
        }

        data_offset += chunk_size;
        remaining -= chunk_size;
        ++seq_no;
    }

    return true;
}

// ==================== 发送 BlockOpResponse ====================
bool DataRpcConnection::send_block_op_response(const BlockOpResponseProto& rsp) {
    return write_proto_delimited(rsp);
}

// ==================== IPC RPC 协议处理 ====================
void DataRpcConnection::serve_ipc_rpc() {
    // 先消费 IPC preamble： "hrpc" + version + serviceClass + authMethod
    char magic[4];
    if (!read_full(magic, sizeof(magic))) {
        log(LogLevel::ERROR, "[DataRpcConnection] failed to read IPC magic");
        return;
    }
    if (!(magic[0]=='h' && magic[1]=='r' && magic[2]=='p' && magic[3]=='c')) {
        log(LogLevel::ERROR, "[DataRpcConnection] invalid IPC magic: %s",
            hex_dump(reinterpret_cast<const uint8_t*>(magic), 4).c_str());
        return;
    }

    uint8_t ver = 0, service = 0, auth = 0;
    if (!read_full(&ver, 1) || !read_full(&service, 1) || !read_full(&auth, 1)) {
        log(LogLevel::ERROR, "[DataRpcConnection] failed to read IPC header");
        return;
    }
    log(LogLevel::DEBUG, "[DataRpcConnection] IPC preamble ok: ver=%u service=%u auth=%u",
        ver, service, auth);

    while (handle_one_ipc_call()) {
        // loop
    }
}

bool DataRpcConnection::handle_one_ipc_call() {
    // 读取 total_len + buf
    uint32_t net_len = 0;
    if (!read_full(&net_len, sizeof(net_len))) {
        return false;
    }
    uint32_t total_len = ntohl(net_len);
    if (total_len == 0) {
        log(LogLevel::ERROR, "[DataRpcConnection] invalid packet length=0");
        return false;
    }

    std::vector<uint8_t> buf(total_len);
    if (!read_full(buf.data(), buf.size())) {
        return false;
    }

    ArrayInputStream ais(buf.data(), static_cast<int>(buf.size()));
    CodedInputStream cis(&ais);

    // 解析 RpcRequestHeaderProto
    RpcRequestHeaderProto rpc_header;
    {
        uint32_t header_len = 0;
        if (!cis.ReadVarint32(&header_len)) {
            log(LogLevel::ERROR, "[DataRpcConnection] failed to read RpcRequestHeader length");
            return false;
        }
        auto limit = cis.PushLimit(static_cast<int>(header_len));
        if (!rpc_header.ParseFromCodedStream(&cis)) {
            log(LogLevel::ERROR, "[DataRpcConnection] failed to parse RpcRequestHeader");
            return false;
        }
        cis.PopLimit(limit);
    }

    int32_t call_id = rpc_header.callid();

    // 处理握手（ConnectionContext）
    if (!handshake_done_ && call_id == -3) {
        IpcConnectionContextProto ctx;
        uint32_t ctx_len = 0;
        if (!cis.ReadVarint32(&ctx_len)) {
            log(LogLevel::ERROR, "[DataRpcConnection] failed to read IpcConnectionContext length");
            return false;
        }
        auto limit = cis.PushLimit(static_cast<int>(ctx_len));
        if (!ctx.ParseFromCodedStream(&cis)) {
            log(LogLevel::ERROR, "[DataRpcConnection] failed to parse IpcConnectionContext");
            return false;
        }
        cis.PopLimit(limit);

        if (ctx.has_userinfo() && ctx.userinfo().has_effectiveuser()) {
            user_ = ctx.userinfo().effectiveuser();
        }
        if (ctx.has_protocol()) {
            protocol_name_ = ctx.protocol();
        }

        handshake_done_ = true;
        log(LogLevel::DEBUG, "[DataRpcConnection] connection context ok, user=%s protocol=%s",
            user_.c_str(), protocol_name_.c_str());
        return true;
    }

    // 普通 RPC 请求
    RequestHeaderProto req_header;
    {
        uint32_t header_len = 0;
        if (!cis.ReadVarint32(&header_len)) {
            log(LogLevel::ERROR, "[DataRpcConnection] failed to read RequestHeader length");
            return false;
        }
        auto limit = cis.PushLimit(static_cast<int>(header_len));
        if (!req_header.ParseFromCodedStream(&cis)) {
            log(LogLevel::ERROR, "[DataRpcConnection] failed to parse RequestHeader");
            return false;
        }
        cis.PopLimit(limit);
    }

    // 读取参数
    std::string param_bytes;
    {
        uint32_t param_len = 0;
        if (!cis.ReadVarint32(&param_len)) {
            log(LogLevel::ERROR, "[DataRpcConnection] failed to read param length");
            return false;
        }
        if (param_len > 0) {
            param_bytes.resize(param_len);
            if (!cis.ReadRaw(param_bytes.data(), param_len)) {
                log(LogLevel::ERROR, "[DataRpcConnection] failed to read param bytes");
                return false;
            }
        }
    }

    // 调度处理
    std::string resp_param_bytes;
    bool ok = dispatch_ipc(rpc_header, req_header, param_bytes, resp_param_bytes);

    // 构造响应
    RpcResponseHeaderProto resp_header;
    resp_header.set_callid(rpc_header.callid());
    resp_header.set_clientid(rpc_header.clientid());

    if (ok) {
        resp_header.set_status(RpcResponseHeaderProto::SUCCESS);
    } else {
        resp_header.set_status(RpcResponseHeaderProto::ERROR);
        resp_header.set_exceptionclassname("java.io.IOException");
        resp_header.set_errormsg("Unimplemented or failed RPC method");
        resp_header.set_errordetail(RpcResponseHeaderProto::ERROR_APPLICATION);
    }

    // 序列化响应
    std::string header_bytes;
    if (!resp_header.SerializeToString(&header_bytes)) {
        log(LogLevel::ERROR, "[DataRpcConnection] failed to serialize RpcResponseHeader");
        return false;
    }

    std::string payload;
    {
        StringOutputStream sos(&payload);
        CodedOutputStream cos(&sos);

        // 1) response header: delimited (varint32 len + bytes)
        cos.WriteVarint32(static_cast<uint32_t>(header_bytes.size()));
        cos.WriteRaw(header_bytes.data(), header_bytes.size());

        // 2) response body length: 4-byte int (big-endian)
        uint32_t body_len = ok ? static_cast<uint32_t>(resp_param_bytes.size()) : 0;
        uint32_t net_body_len = htonl(body_len);
        cos.WriteRaw(&net_body_len, sizeof(net_body_len));

        // 3) response body bytes (only when ok)
        if (body_len > 0) {
            cos.WriteRaw(resp_param_bytes.data(), body_len);
        }
    }

    // 外层 total length（4B big-endian）
    uint32_t out_len = htonl(static_cast<uint32_t>(payload.size()));
    if (!write_full(&out_len, sizeof(out_len))) return false;
    if (!write_full(payload.data(), payload.size())) return false;

        return true;
    }

// ==================== IPC RPC 调度 ====================
bool DataRpcConnection::dispatch_ipc(
    const RpcRequestHeaderProto& rpc_header,
    const RequestHeaderProto& req_header,
    const std::string& param_bytes,
    std::string& resp_param_bytes) {

    const std::string& method = req_header.methodname();
    log(LogLevel::DEBUG, "[DataRpcConnection] dispatch method=%s", method.c_str());

    // ClientDatanodeProtocol 方法
    if (method == "getReplicaVisibleLength") {
        GetReplicaVisibleLengthRequestProto req;
        GetReplicaVisibleLengthResponseProto rsp;
        if (!req.ParseFromString(param_bytes)) {
            return false;
        }
        service_->getReplicaVisibleLength(req, rsp);
        return rsp.SerializeToString(&resp_param_bytes);
    }
    else if (method == "getBlockLocalPathInfo") {
        GetBlockLocalPathInfoRequestProto req;
        GetBlockLocalPathInfoResponseProto rsp;
        if (!req.ParseFromString(param_bytes)) {
            return false;
        }
        service_->getBlockLocalPathInfo(req, rsp);
        return rsp.SerializeToString(&resp_param_bytes);
    }
    else if (method == "deleteBlockPool") {
        DeleteBlockPoolRequestProto req;
        DeleteBlockPoolResponseProto rsp;
        if (!req.ParseFromString(param_bytes)) {
            return false;
        }
        service_->deleteBlockPool(req, rsp);
        return rsp.SerializeToString(&resp_param_bytes);
    }
    else if (method == "shutdownDatanode") {
        ShutdownDatanodeRequestProto req;
        ShutdownDatanodeResponseProto rsp;
        if (!req.ParseFromString(param_bytes)) {
            return false;
        }
        service_->shutdownDatanode(req, rsp);
        return rsp.SerializeToString(&resp_param_bytes);
    }
    else if (method == "getDatanodeInfo") {
        GetDatanodeInfoRequestProto req;
        GetDatanodeInfoResponseProto rsp;
        if (!req.ParseFromString(param_bytes)) {
            return false;
        }
        service_->getDatanodeInfo(req, rsp);
        return rsp.SerializeToString(&resp_param_bytes);
    }
    // else if (method == "getBlockChecksum") {
    //     GetBlockChecksumRequestProto req;
    //     GetBlockChecksumResponseProto rsp;
    //     if (!req.ParseFromString(param_bytes)) {
    //         return false;
    //     }
    //     service_->getBlockChecksum(req, rsp);
    //     return rsp.SerializeToString(&resp_param_bytes);
    // }

    log(LogLevel::WARN, "[DataRpcConnection] unknown method: %s", method.c_str());
    return false;
}

// ==================== 工具函数 ====================

bool DataRpcConnection::read_full(void* buf, size_t len) {
    uint8_t* p = static_cast<uint8_t*>(buf);
    size_t left = len;

    while (left > 0) {
        ssize_t n = ::recv(fd_, p, left, 0);
        if (n > 0) {
            p += n;
            left -= static_cast<size_t>(n);
            continue;
        }
        if (n == 0) return false;
        if (errno == EINTR) continue;

        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            short rev = 0;
            if (!wait_fd(fd_, POLLIN, /*timeout_ms=*/30000, &rev)) {
                log(LogLevel::DEBUG, "read_full: wait POLLIN timeout/closed revents=0x%x", (int)rev);
                return false;
            }
            continue;
        }

        log(LogLevel::DEBUG, "read_full: recv error errno=%d(%s)", errno, strerror(errno));
        return false;
    }
    return true;
}

bool DataRpcConnection::write_full(const void* buf, size_t len) {
    const uint8_t* p = static_cast<const uint8_t*>(buf);
    size_t left = len;

    while (left > 0) {
        ssize_t n = ::send(fd_, p, left, 0);
        if (n > 0) {
            p += n;
            left -= n;
            continue;
        }
        if (n == 0) return false;
        if (errno == EINTR) continue;

        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            short rev = 0;
            if (!wait_fd(fd_, POLLOUT, /*timeout_ms=*/30000, &rev)) {
                log(LogLevel::DEBUG, "write_full: wait POLLOUT timeout/closed revents=0x%x", (int)rev);
                return false;
            }
            continue;
        }

        log(LogLevel::DEBUG, "write_full: send error errno=%d(%s)", errno, strerror(errno));
        return false;
    }
    return true;
}

bool DataRpcConnection::read_varint32(uint32_t& value) {
    value = 0;
    for (int i = 0; i < 5; ++i) {
        uint8_t byte;
        if (!read_full(&byte, 1)) {
            return false;
        }
        value |= (static_cast<uint32_t>(byte & 0x7F) << (7 * i));
        if ((byte & 0x80) == 0) {
            return true;
        }
    }
    return false;
}

bool DataRpcConnection::write_varint32(uint32_t value) {
    uint8_t buf[5];
    int len = 0;
    
    while (value > 0x7F) {
        buf[len++] = static_cast<uint8_t>((value & 0x7F) | 0x80);
        value >>= 7;
    }
    buf[len++] = static_cast<uint8_t>(value);
    
    return write_full(buf, len);
}

bool DataRpcConnection::read_proto_delimited(google::protobuf::Message& msg) {
    uint32_t len;
    if (!read_varint32(len)) {
        return false;
    }
    
    std::vector<uint8_t> buf(len);
    if (!read_full(buf.data(), len)) {
        return false;
    }
    
    return msg.ParseFromArray(buf.data(), len);
}

bool DataRpcConnection::write_proto_delimited(const google::protobuf::Message& msg) {
    std::string bytes;
    if (!msg.SerializeToString(&bytes)) {
        return false;
    }
    
    if (!write_varint32(bytes.size())) {
        return false;
    }
    
    return write_full(bytes.data(), bytes.size());
}

} // namespace hcg
