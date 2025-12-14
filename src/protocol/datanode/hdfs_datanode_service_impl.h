#pragma once

#include "protocol/datanode/hdfs_datanode_service.h"
#include "protocol/datanode/datanode_internal_service.h"
#include "rpc/internal/internal_gateway_service_impl.h"
#include "common/types.h"
#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

using namespace hadoop::hdfs;

namespace hcg {

// 块信息
// struct BlockInfo {
//     std::string block_id;
//     std::string block_pool_id;
//     uint64_t generation_stamp;
//     uint64_t num_bytes;
//     std::string local_path;
//     uint32_t checksum;
// };

class HdfsDatanodeServiceImpl : public IHdfsDatanodeService {
public:
    explicit HdfsDatanodeServiceImpl(
        std::shared_ptr<IInternalGatewayService> internal_service);

    ~HdfsDatanodeServiceImpl();

    // 初始化和启动
    int initialize();
    int start();
    int stop();

    // ==================== DataTransferProtocol ====================

    void readBlock(
        const OpReadBlockProto& req,
        BlockOpResponseProto& rsp,
        std::vector<char>& data) override;

    void writeBlock(
        const OpWriteBlockProto& req,
        const std::vector<char>& data,
        BlockOpResponseProto& rsp) override;

    void transferBlock(
        const OpTransferBlockProto& req,
        BlockOpResponseProto& rsp) override;

    void copyBlock(
        const OpCopyBlockProto& req,
        BlockOpResponseProto& rsp) override;

    void replaceBlock(
        const OpReplaceBlockProto& req,
        BlockOpResponseProto& rsp) override;

    void requestShortCircuitAccess(
        const OpRequestShortCircuitAccessProto& req,
        BlockOpResponseProto& rsp) override;

    // ==================== ClientDatanodeProtocol ====================

    void getReplicaVisibleLength(
        const GetReplicaVisibleLengthRequestProto& req,
        GetReplicaVisibleLengthResponseProto& rsp) override;

    void getBlockLocalPathInfo(
        const GetBlockLocalPathInfoRequestProto& req,
        GetBlockLocalPathInfoResponseProto& rsp) override;

    void deleteBlockPool(
        const DeleteBlockPoolRequestProto& req,
        DeleteBlockPoolResponseProto& rsp) override;

    void shutdownDatanode(
        const ShutdownDatanodeRequestProto& req,
        ShutdownDatanodeResponseProto& rsp) override;

    void getDatanodeInfo(
        const GetDatanodeInfoRequestProto& req,
        GetDatanodeInfoResponseProto& rsp) override;

private:
    // 配置
    std::string storage_dir_;
    std::string datanode_uuid_;
    std::string datanode_hostname_;
    uint32_t datanode_port_{50010};
    uint32_t ipc_port_{50020};
    std::shared_ptr<IInternalGatewayService> internal_;

    // 与 NameNode 通信的客户端
    std::shared_ptr<IDatanodeInternalService> nn_client_;

    // 块存储
    // std::unique_ptr<BlockStorage> block_storage_;

    // 块索引 (block_id -> BlockInfo)
    std::unordered_map<std::string, BlockInfo> block_map_;
    mutable std::mutex block_map_mutex_;

    // 待报告的块变更
    std::vector<std::string> received_blocks_;
    std::vector<std::string> deleted_blocks_;
    std::mutex pending_report_mutex_;

    // 后台线程
    std::atomic<bool> running_{false};
    std::thread heartbeat_thread_;
    std::thread block_report_thread_;

    // 心跳间隔（毫秒）
    uint32_t heartbeat_interval_ms_{3000};
    uint32_t block_report_interval_ms_{60000};

    // 私有方法
    void heartbeat_loop();
    void block_report_loop();
    void scan_storage_directory();
    std::string make_block_key(const std::string& pool_id, uint64_t block_id);
    std::string get_block_path(const std::string& pool_id, uint64_t block_id);
    uint32_t calculate_checksum(const std::vector<char>& data);
    void process_namenode_commands(
        const HeartbeatResponseProto& response);
};

} // namespace hcg
