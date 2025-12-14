#pragma once
#include "protocol/datanode/hdfs_datanode_service.h"
#include "RpcHeader.pb.h"
#include "ProtobufRpcEngine.pb.h"
#include <memory>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

using namespace hadoop::common;

namespace hcg {

// DataTransfer 协议操作码
enum class DataTransferOp : uint8_t {
    WRITE_BLOCK = 80,
    READ_BLOCK = 81,
    READ_METADATA = 82,
    REPLACE_BLOCK = 83,
    COPY_BLOCK = 84,
    BLOCK_CHECKSUM = 85,
    TRANSFER_BLOCK = 86,
    REQUEST_SHORT_CIRCUIT_FDS = 87,
    RELEASE_SHORT_CIRCUIT_FDS = 88,
    REQUEST_SHORT_CIRCUIT_SHM = 89,
    BLOCK_GROUP_CHECKSUM = 90,
    CUSTOM = 127
};

// DataTransfer 协议版本
constexpr uint16_t DATA_TRANSFER_VERSION = 28;

class DataRpcConnection {
public:
    DataRpcConnection(int fd, std::shared_ptr<IHdfsDatanodeService> service);
    ~DataRpcConnection();

    // 主服务循环
    void serve();
    int fd_;

private:

    std::shared_ptr<IHdfsDatanodeService> service_;
    
    // IPC RPC 状态
    bool handshake_done_{false};
    std::string user_;
    std::string protocol_name_;

    // 协议类型
    enum class ProtocolType {
        UNKNOWN,
        DATA_TRANSFER,
        DATA_TRANSFER_SASL,
        IPC_RPC
    };
    ProtocolType protocol_type_{ProtocolType::UNKNOWN};

    // 检测协议类型
    ProtocolType detect_protocol();

    // ==================== DataTransfer 协议处理 ====================
    void serve_data_transfer();
    bool handle_data_transfer_op(uint8_t op_code);
    
    // 各操作处理函数
    bool handle_read_block();
    bool handle_write_block();
    bool handle_transfer_block();
    bool handle_copy_block();
    bool handle_replace_block();
    bool handle_block_checksum();
    bool handle_request_short_circuit();

    // 发送块数据（带校验和）
    bool send_block_data(const std::vector<char>& data, uint64_t offset);
    // 接收块数据（带校验和）
    bool recv_block_data(std::vector<char>& data, uint64_t expected_len);
    // 发送 BlockOpResponse
    bool send_block_op_response(const hadoop::hdfs::BlockOpResponseProto& rsp);

    // ==================== IPC RPC 协议处理 ====================
    void serve_ipc_rpc();
    bool handle_one_ipc_call();
    bool dispatch_ipc(
        const RpcRequestHeaderProto& rpc_header,
        const RequestHeaderProto& req_header,
        const std::string& param_bytes,
        std::string& resp_param_bytes);

    // ==================== 工具函数 ====================
    bool read_full(void* buf, size_t len);
    bool write_full(const void* buf, size_t len);
    bool read_varint32(uint32_t& value);
    bool write_varint32(uint32_t value);
    bool read_proto_delimited(google::protobuf::Message& msg);
    bool write_proto_delimited(const google::protobuf::Message& msg);
    bool peek_at_least(size_t want, std::vector<uint8_t>& out, int timeout_ms);
};

} // namespace hcg
