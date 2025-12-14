#include "hdfs_datanode_service_impl.h"
#include "common/logging.h" 

#include <chrono>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <fstream>
#include <sys/stat.h>
#include <unistd.h>
#include <uuid/uuid.h>

namespace hcg {

HdfsDatanodeServiceImpl::HdfsDatanodeServiceImpl(
    std::shared_ptr<IInternalGatewayService> internal_service)
    : internal_(std::move(internal_service)) {
    // TODO: 初始化成员变量
}

HdfsDatanodeServiceImpl::~HdfsDatanodeServiceImpl() {
    // TODO: 清理资源
}

int HdfsDatanodeServiceImpl::initialize() {
    log(LogLevel::INFO, "Initializing HdfsDatanodeServiceImpl");
    // TODO: 实现初始化逻辑
    return 0;
}

int HdfsDatanodeServiceImpl::start() {
    log(LogLevel::INFO, "Starting HdfsDatanodeServiceImpl");
    // TODO: 启动服务、线程等
    return 0;
}

int HdfsDatanodeServiceImpl::stop() {
    log(LogLevel::INFO, "Stopping HdfsDatanodeServiceImpl");
    // TODO: 停止服务、等待线程退出等
    return 0;
}

// ==================== DataTransferProtocol ====================

void HdfsDatanodeServiceImpl::readBlock(
    const OpReadBlockProto& req,
    BlockOpResponseProto& rsp,
    std::vector<char>& data) {
    log(LogLevel::DEBUG, "Handling readBlock request");
    // TODO: Implement readBlock
}

void HdfsDatanodeServiceImpl::writeBlock(
    const OpWriteBlockProto& req,
    const std::vector<char>& data,
    BlockOpResponseProto& rsp) {
    log(LogLevel::DEBUG, "Handling writeBlock request");
    // TODO: Implement writeBlock
}

void HdfsDatanodeServiceImpl::transferBlock(
    const OpTransferBlockProto& req,
    BlockOpResponseProto& rsp) {
    log(LogLevel::DEBUG, "Handling transferBlock request");
    // TODO: Implement transferBlock
}

void HdfsDatanodeServiceImpl::copyBlock(
    const OpCopyBlockProto& req,
    BlockOpResponseProto& rsp) {
    log(LogLevel::DEBUG, "Handling copyBlock request");
    // TODO: Implement copyBlock
}

void HdfsDatanodeServiceImpl::replaceBlock(
    const OpReplaceBlockProto& req,
    BlockOpResponseProto& rsp) {
    log(LogLevel::DEBUG, "Handling replaceBlock request");
    // TODO: Implement replaceBlock
}

void HdfsDatanodeServiceImpl::requestShortCircuitAccess(
    const OpRequestShortCircuitAccessProto& req,
    BlockOpResponseProto& rsp) {
    log(LogLevel::DEBUG, "Handling requestShortCircuitAccess request");
    // TODO: Implement requestShortCircuitAccess
}

// ==================== ClientDatanodeProtocol ====================

void HdfsDatanodeServiceImpl::getReplicaVisibleLength(
    const GetReplicaVisibleLengthRequestProto& req,
    GetReplicaVisibleLengthResponseProto& rsp) {
    log(LogLevel::DEBUG, "Handling getReplicaVisibleLength request");
    // TODO: Implement getReplicaVisibleLength
}

void HdfsDatanodeServiceImpl::getBlockLocalPathInfo(
    const GetBlockLocalPathInfoRequestProto& req,
    GetBlockLocalPathInfoResponseProto& rsp) {
    log(LogLevel::DEBUG, "Handling getBlockLocalPathInfo request");
    // TODO: Implement getBlockLocalPathInfo
}

void HdfsDatanodeServiceImpl::deleteBlockPool(
    const DeleteBlockPoolRequestProto& req,
    DeleteBlockPoolResponseProto& rsp) {
    log(LogLevel::DEBUG, "Handling deleteBlockPool request");
    // TODO: Implement deleteBlockPool
}

void HdfsDatanodeServiceImpl::shutdownDatanode(
    const ShutdownDatanodeRequestProto& req,
    ShutdownDatanodeResponseProto& rsp) {
    log(LogLevel::INFO, "Handling shutdownDatanode request");
    // TODO: Implement shutdownDatanode
}

void HdfsDatanodeServiceImpl::getDatanodeInfo(
    const GetDatanodeInfoRequestProto& req,
    GetDatanodeInfoResponseProto& rsp) {
    log(LogLevel::DEBUG, "Handling getDatanodeInfo request");
    // TODO: Implement getDatanodeInfo
}

// ==================== Private Methods ====================

void HdfsDatanodeServiceImpl::heartbeat_loop() {
    log(LogLevel::DEBUG, "Heartbeat loop started");
    // TODO: Implement heartbeat loop
}

void HdfsDatanodeServiceImpl::block_report_loop() {
    log(LogLevel::DEBUG, "Block report loop started");
    // TODO: Implement block report loop
}

void HdfsDatanodeServiceImpl::scan_storage_directory() {
    log(LogLevel::DEBUG, "Scanning storage directory");
    // TODO: Implement scan_storage_directory
}

std::string HdfsDatanodeServiceImpl::make_block_key(const std::string& pool_id, uint64_t block_id) {
    log(LogLevel::DEBUG, "Making block key");
    // TODO: Implement make_block_key
    return "";
}

std::string HdfsDatanodeServiceImpl::get_block_path(const std::string& pool_id, uint64_t block_id) {
    log(LogLevel::DEBUG, "Getting block path");
    // TODO: Implement get_block_path
    return "";
}

uint32_t HdfsDatanodeServiceImpl::calculate_checksum(const std::vector<char>& data) {
    log(LogLevel::DEBUG, "Calculating checksum");
    // TODO: Implement calculate_checksum
    return 0;
}

void HdfsDatanodeServiceImpl::process_namenode_commands(
    const HeartbeatResponseProto& response) {
    log(LogLevel::DEBUG, "Processing NameNode commands");
    // TODO: Implement process_namenode_commands
}

} // namespace hcg


// namespace hcg {

// HdfsDatanodeServiceImpl::HdfsDatanodeServiceImpl(
//     const std::string& storage_dir,
//     std::shared_ptr<IDatanodeInternalService> nn_client)
//     : storage_dir_(storage_dir)
//     , nn_client_(std::move(nn_client)) {
    
//     // 生成 DataNode UUID
//     uuid_t uuid;
//     uuid_generate(uuid);
//     char uuid_str[37];
//     uuid_unparse(uuid, uuid_str);
//     datanode_uuid_ = uuid_str;

//     // 获取主机名
//     char hostname[256];
//     if (gethostname(hostname, sizeof(hostname)) == 0) {
//         datanode_hostname_ = hostname;
//     } else {
//         datanode_hostname_ = "localhost";
//     }
// }

// HdfsDatanodeServiceImpl::~HdfsDatanodeServiceImpl() {
//     stop();
// }

// int HdfsDatanodeServiceImpl::initialize() {
//     log(LogLevel::INFO, "Initializing DataNode, storage_dir=%s", storage_dir_.c_str());

//     // 创建存储目录
//     if (mkdir(storage_dir_.c_str(), 0755) < 0 && errno != EEXIST) {
//         log(LogLevel::ERROR, "Failed to create storage directory: %s", strerror(errno));
//         return -1;
//     }

//     // 创建块存储子目录
//     std::string block_dir = storage_dir_ + "/current";
//     if (mkdir(block_dir.c_str(), 0755) < 0 && errno != EEXIST) {
//         log(LogLevel::ERROR, "Failed to create block directory: %s", strerror(errno));
//         return -1;
//     }

//     // 扫描现有块
//     scan_storage_directory();

//     log(LogLevel::INFO, "DataNode initialized, found %zu blocks", block_map_.size());
//     return 0;
// }

// int HdfsDatanodeServiceImpl::start() {
//     if (running_) {
//         return 0;
//     }

//     log(LogLevel::INFO, "Starting DataNode...");

//     // 向 NameNode 注册
//     hadoop::hdfs::RegisterDatanodeRequestProto reg_req;
//     hadoop::hdfs::RegisterDatanodeResponseProto reg_rsp;

//     auto* registration = reg_req.mutable_registration();
//     auto* datanodeid = registration->mutable_datanodeid();
//     datanodeid->set_ipaddr(datanode_hostname_);
//     datanodeid->set_hostname(datanode_hostname_);
//     datanodeid->set_datanodeuuid(datanode_uuid_);
//     datanodeid->set_xferport(datanode_port_);
//     datanodeid->set_infoport(50075);
//     datanodeid->set_ipcport(ipc_port_);

//     auto* storage_info = registration->mutable_storageinfo();
//     storage_info->set_layoutversion(-65);
//     storage_info->set_namespaceid(1);
//     storage_info->set_clusterid("cluster1");
//     storage_info->set_ctime(0);

//     if (nn_client_->registerDatanode(reg_req, reg_rsp) != 0) {
//         log(LogLevel::ERROR, "Failed to register with NameNode");
//         return -1;
//     }

//     running_ = true;

//     // 启动心跳线程
//     heartbeat_thread_ = std::thread(&HdfsDatanodeServiceImpl::heartbeat_loop, this);

//     // 启动块报告线程
//     block_report_thread_ = std::thread(&HdfsDatanodeServiceImpl::block_report_loop, this);

//     log(LogLevel::INFO, "DataNode started, uuid=%s", datanode_uuid_.c_str());
//     return 0;
// }

// int HdfsDatanodeServiceImpl::stop() {
//     if (!running_) {
//         return 0;
//     }

//     log(LogLevel::INFO, "Stopping DataNode...");
//     running_ = false;

//     if (heartbeat_thread_.joinable()) {
//         heartbeat_thread_.join();
//     }
//     if (block_report_thread_.joinable()) {
//         block_report_thread_.join();
//     }

//     log(LogLevel::INFO, "DataNode stopped.");
//     return 0;
// }

// // ==================== 读取块 ====================
// void HdfsDatanodeServiceImpl::readBlock(
//     const hadoop::hdfs::OpReadBlockProto& req,
//     hadoop::hdfs::BlockOpResponseProto& rsp,
//     std::vector<char>& data) {
    
//     const auto& header = req.header();
//     const auto& block = header.baseheader().block();
//     uint64_t block_id = block.blockid();
//     std::string pool_id = block.poolid();
//     uint64_t offset = req.offset();
//     uint64_t len = req.len();

//     log(LogLevel::DEBUG, "readBlock: block_id=%lu, offset=%lu, len=%lu",
//         block_id, offset, len);

//     std::string block_key = make_block_key(pool_id, block_id);

//     // 查找块信息
//     BlockInfo block_info;
//     {
//         std::lock_guard<std::mutex> lock(block_map_mutex_);
//         auto it = block_map_.find(block_key);
//         if (it == block_map_.end()) {
//             log(LogLevel::ERROR, "Block not found: %s", block_key.c_str());
//             rsp.set_status(hadoop::hdfs::ERROR);
//             rsp.set_message("Block not found");
//             return;
//         }
//         block_info = it->second;
//     }

//     // 验证偏移量
//     if (offset >= block_info.num_bytes) {
//         log(LogLevel::ERROR, "Invalid offset %lu for block size %lu",
//             offset, block_info.num_bytes);
//         rsp.set_status(hadoop::hdfs::ERROR);
//         rsp.set_message("Invalid offset");
//         return;
//     }

//     // 计算实际读取长度
//     uint64_t actual_len = std::min(len, block_info.num_bytes - offset);

//     // 读取数据
//     std::string block_path = get_block_path(pool_id, block_id);
//     std::ifstream file(block_path, std::ios::binary);
//     if (!file) {
//         log(LogLevel::ERROR, "Failed to open block file: %s", block_path.c_str());
//         rsp.set_status(hadoop::hdfs::ERROR);
//         rsp.set_message("Failed to open block file");
//         return;
//     }

//     file.seekg(offset);
//     data.resize(actual_len);
//     file.read(data.data(), actual_len);

//     if (!file) {
//         log(LogLevel::ERROR, "Failed to read block data");
//         rsp.set_status(hadoop::hdfs::ERROR);
//         rsp.set_message("Failed to read block data");
//         return;
//     }

//     rsp.set_status(hadoop::hdfs::SUCCESS);
    
//     // 设置校验和信息
//     auto* checksum_rsp = rsp.mutable_readopchecksuminfo();
//     auto* checksum = checksum_rsp->mutable_checksum();
//     checksum->set_type(hadoop::hdfs::CHECKSUM_CRC32);
//     checksum->set_bytesperchecksum(512);
//     checksum_rsp->set_chunkoffset(offset);

//     log(LogLevel::DEBUG, "readBlock success: read %zu bytes", data.size());
// }

// // ==================== 写入块 ====================
// void HdfsDatanodeServiceImpl::writeBlock(
//     const hadoop::hdfs::OpWriteBlockProto& req,
//     const std::vector<char>& data,
//     hadoop::hdfs::BlockOpResponseProto& rsp) {
    
//     const auto& header = req.header();
//     const auto& block = header.baseheader().block();
//     uint64_t block_id = block.blockid();
//     std::string pool_id = block.poolid();
//     uint64_t gen_stamp = block.generationstamp();

//     log(LogLevel::DEBUG, "writeBlock: block_id=%lu, pool_id=%s, size=%zu",
//         block_id, pool_id.c_str(), data.size());

//     std::string block_key = make_block_key(pool_id, block_id);
//     std::string block_path = get_block_path(pool_id, block_id);

//     // 确保块池目录存在
//     std::string pool_dir = storage_dir_ + "/current/" + pool_id;
//     mkdir(pool_dir.c_str(), 0755);

//     // 写入数据文件
//     std::ofstream file(block_path, std::ios::binary | std::ios::trunc);
//     if (!file) {
//         log(LogLevel::ERROR, "Failed to create block file: %s", block_path.c_str());
//         rsp.set_status(hadoop::hdfs::ERROR);
//         rsp.set_message("Failed to create block file");
//         return;
//     }

//     file.write(data.data(), data.size());
//     if (!file) {
//         log(LogLevel::ERROR, "Failed to write block data");
//         rsp.set_status(hadoop::hdfs::ERROR);
//         rsp.set_message("Failed to write block data");
//         return;
//     }
//     file.close();

//     // 写入元数据文件
//     std::string meta_path = block_path + ".meta";
//     std::ofstream meta_file(meta_path, std::ios::binary);
//     if (meta_file) {
//         uint32_t checksum = calculate_checksum(data);
//         meta_file.write(reinterpret_cast<const char*>(&checksum), sizeof(checksum));
//         meta_file.close();
//     }

//     // 更新块索引
//     {
//         std::lock_guard<std::mutex> lock(block_map_mutex_);
//         BlockInfo& info = block_map_[block_key];
//         info.block_id = std::to_string(block_id);
//         info.block_pool_id = pool_id;
//         info.generation_stamp = gen_stamp;
//         info.num_bytes = data.size();
//         info.local_path = block_path;
//         info.checksum = calculate_checksum(data);
//     }

//     // 添加到待报告列表
//     {
//         std::lock_guard<std::mutex> lock(pending_report_mutex_);
//         received_blocks_.push_back(block_key);
//     }

//     rsp.set_status(hadoop::hdfs::SUCCESS);

//     // 如果有下游节点，转发数据（管道写入）
//     if (req.targets_size() > 0) {
//         log(LogLevel::DEBUG, "Forwarding block to %d downstream nodes",
//             req.targets_size());
//         // TODO: 实现管道转发
//     }

//     log(LogLevel::DEBUG, "writeBlock success: block_id=%lu", block_id);
// }

// // ==================== 传输块 ====================
// void HdfsDatanodeServiceImpl::transferBlock(
//     const hadoop::hdfs::OpTransferBlockProto& req,
//     hadoop::hdfs::BlockOpResponseProto& rsp) {
    
//     const auto& header = req.header();
//     const auto& block = header.baseheader().block();
    
//     log(LogLevel::DEBUG, "transferBlock: block_id=%lu", block.blockid());

//     // 读取本地块数据
//     std::string block_key = make_block_key(block.poolid(), block.blockid());
    
//     BlockInfo block_info;
//     {
//         std::lock_guard<std::mutex> lock(block_map_mutex_);
//         auto it = block_map_.find(block_key);
//         if (it == block_map_.end()) {
//             rsp.set_status(hadoop::hdfs::ERROR);
//             rsp.set_message("Block not found");
//             return;
//         }
//         block_info = it->second;
//     }

//     // 读取块数据
//     std::vector<char> data(block_info.num_bytes);
//     std::ifstream file(block_info.local_path, std::ios::binary);
//     if (!file) {
//         rsp.set_status(hadoop::hdfs::ERROR);
//         rsp.set_message("Failed to read block file");
//         return;
//     }
//     file.read(data.data(), data.size());

//     // 向目标节点发送数据
//     for (int i = 0; i < req.targets_size(); ++i) {
//         const auto& target = req.targets(i);
//         log(LogLevel::DEBUG, "Transferring block to %s:%d",
//             target.id().ipaddr().c_str(), target.id().xferport());
        
//         // TODO: 建立到目标 DataNode 的连接并发送数据
//     }

//     rsp.set_status(hadoop::hdfs::SUCCESS);
// }

// // ==================== 复制块 ====================
// void HdfsDatanodeServiceImpl::copyBlock(
//     const hadoop::hdfs::OpCopyBlockProto& req,
//     hadoop::hdfs::BlockOpResponseProto& rsp) {
    
//     const auto& header = req.header();
//     const auto& block = header.baseheader().block();
    
//     log(LogLevel::DEBUG, "copyBlock: block_id=%lu", block.blockid());

//     // 从源节点复制块
//     // TODO: 实现从源节点读取并本地保存

//     rsp.set_status(hadoop::hdfs::SUCCESS);
// }

// // ==================== 替换块 ====================
// void HdfsDatanodeServiceImpl::replaceBlock(
//     const hadoop::hdfs::OpReplaceBlockProto& req,
//     hadoop::hdfs::BlockOpResponseProto& rsp) {
    
//     const auto& header = req.header();
//     const auto& block = header.baseheader().block();
    
//     log(LogLevel::DEBUG, "replaceBlock: block_id=%lu", block.blockid());

//     // TODO: 实现块替换逻辑

//     rsp.set_status(hadoop::hdfs::SUCCESS);
// }

// // ==================== 短路访问 ====================
// void HdfsDatanodeServiceImpl::requestShortCircuitAccess(
//     const hadoop::hdfs::OpRequestShortCircuitAccessProto& req,
//     hadoop::hdfs::BlockOpResponseProto& rsp) {
    
//     const auto& block = req.header().block();
    
//     log(LogLevel::DEBUG, "requestShortCircuitAccess: block_id=%lu", block.blockid());

//     // 短路读允许客户端直接读取本地文件
//     std::string block_key = make_block_key(block.poolid(), block.blockid());
    
//     {
//         std::lock_guard<std::mutex> lock(block_map_mutex_);
//         auto it = block_map_.find(block_key);
//         if (it == block_map_.end()) {
//             rsp.set_status(hadoop::hdfs::ERROR);
//             rsp.set_message("Block not found");
//             return;
//         }
//     }

//     rsp.set_status(hadoop::hdfs::SUCCESS);
//     // 实际实现中需要传递文件描述符
// }

// // ==================== 获取副本可见长度 ====================
// void HdfsDatanodeServiceImpl::getReplicaVisibleLength(
//     const hadoop::hdfs::GetReplicaVisibleLengthRequestProto& req,
//     hadoop::hdfs::GetReplicaVisibleLengthResponseProto& rsp) {
    
//     const auto& block = req.block();
//     std::string block_key = make_block_key(block.poolid(), block.blockid());
    
//     log(LogLevel::DEBUG, "getReplicaVisibleLength: block=%s", block_key.c_str());

//     std::lock_guard<std::mutex> lock(block_map_mutex_);
//     auto it = block_map_.find(block_key);
//     if (it != block_map_.end()) {
//         rsp.set_length(it->second.num_bytes);
//     } else {
//         rsp.set_length(0);
//     }
// }

// // ==================== 获取块本地路径 ====================
// void HdfsDatanodeServiceImpl::getBlockLocalPathInfo(
//     const hadoop::hdfs::GetBlockLocalPathInfoRequestProto& req,
//     hadoop::hdfs::GetBlockLocalPathInfoResponseProto& rsp) {
    
//     const auto& block = req.block();
//     std::string block_key = make_block_key(block.poolid(), block.blockid());
    
//     log(LogLevel::DEBUG, "getBlockLocalPathInfo: block=%s", block_key.c_str());

//     std::lock_guard<std::mutex> lock(block_map_mutex_);
//     auto it = block_map_.find(block_key);
//     if (it != block_map_.end()) {
//         auto* loc_block = rsp.mutable_block();
//         loc_block->mutable_block()->CopyFrom(block);
//         rsp.set_localpath(it->second.local_path);
//         rsp.set_localmetapath(it->second.local_path + ".meta");
//     }
// }

// // ==================== 删除块池 ====================
// void HdfsDatanodeServiceImpl::deleteBlockPool(
//     const hadoop::hdfs::DeleteBlockPoolRequestProto& req,
//     hadoop::hdfs::DeleteBlockPoolResponseProto& rsp) {
    
//     std::string pool_id = req.blockpool();
//     bool force = req.force();
    
//     log(LogLevel::INFO, "deleteBlockPool: pool=%s, force=%d",
//         pool_id.c_str(), force);

//     // 删除块池目录下的所有块
//     std::string pool_dir = storage_dir_ + "/current/" + pool_id;

//     std::vector<std::string> blocks_to_remove;
//     {
//         std::lock_guard<std::mutex> lock(block_map_mutex_);
//         for (auto it = block_map_.begin(); it != block_map_.end(); ) {
//             if (it->second.block_pool_id == pool_id) {
//                 // 删除文件
//                 unlink(it->second.local_path.c_str());
//                 unlink((it->second.local_path + ".meta").c_str());
//                 it = block_map_.erase(it);
//             } else {
//                 ++it;
//             }
//         }
//     }

//     // 删除目录
//     rmdir(pool_dir.c_str());

//     log(LogLevel::INFO, "Block pool %s deleted", pool_id.c_str());
// }

// // ==================== 关闭 DataNode ====================
// void HdfsDatanodeServiceImpl::shutdownDatanode(
//     const hadoop::hdfs::ShutdownDatanodeRequestProto& req,
//     hadoop::hdfs::ShutdownDatanodeResponseProto& rsp) {
    
//     bool for_upgrade = req.forupgrade();
//     log(LogLevel::INFO, "Shutdown requested, for_upgrade=%d", for_upgrade);

//     // 触发异步关闭
//     std::thread([this]() {
//         std::this_thread::sleep_for(std::chrono::milliseconds(100));
//         this->stop();
//     }).detach();
// }

// // ==================== 获取 DataNode 信息 ====================
// void HdfsDatanodeServiceImpl::getDatanodeInfo(
//     const hadoop::hdfs::GetDatanodeInfoRequestProto& req,
//     hadoop::hdfs::GetDatanodeInfoResponseProto& rsp) {
    
//     log(LogLevel::DEBUG, "getDatanodeInfo");

//     auto* info = rsp.mutable_localdatanode();
//     info->set_softwareversion("3.3.0");
//     info->set_configversion("1");
//     info->set_uptime(0);  // TODO: 计算运行时间
// }

// // ==================== 获取块校验和 ====================
// void HdfsDatanodeServiceImpl::getBlockChecksum(
//     const hadoop::hdfs::GetBlockChecksumRequestProto& req,
//     hadoop::hdfs::GetBlockChecksumResponseProto& rsp) {
    
//     const auto& block = req.block();
//     std::string block_key = make_block_key(block.poolid(), block.blockid());
    
//     log(LogLevel::DEBUG, "getBlockChecksum: block=%s", block_key.c_str());

//     std::lock_guard<std::mutex> lock(block_map_mutex_);
//     auto it = block_map_.find(block_key);
//     if (it != block_map_.end()) {
//         rsp.set_bytespercrC(512);
//         rsp.set_crcperblock(it->second.num_bytes / 512 + 1);
//         rsp.set_md5(std::to_string(it->second.checksum));
//         rsp.set_crctype(hadoop::hdfs::CHECKSUM_CRC32);
//     }
// }

// // ==================== 心跳循环 ====================
// void HdfsDatanodeServiceImpl::heartbeat_loop() {
//     while (running_) {
//         hadoop::hdfs::HeartbeatRequestProto req;
//         hadoop::hdfs::HeartbeatResponseProto rsp;

//         // 填充注册信息
//         auto* registration = req.mutable_registration();
//         auto* datanodeid = registration->mutable_datanodeid();
//         datanodeid->set_ipaddr(datanode_hostname_);
//         datanodeid->set_hostname(datanode_hostname_);
//         datanodeid->set_datanodeuuid(datanode_uuid_);
//         datanodeid->set_xferport(datanode_port_);
//         datanodeid->set_infoport(50075);
//         datanodeid->set_ipcport(ipc_port_);

//         // 填充存储报告
//         auto* report = req.add_reports();
//         report->mutable_storage()->set_storageuuid(datanode_uuid_ + "-storage");
//         report->mutable_storage()->set_storagetype(hadoop::hdfs::DISK);
        
//         // 获取存储统计
//         struct statvfs stat;
//         if (statvfs(storage_dir_.c_str(), &stat) == 0) {
//             uint64_t capacity = stat.f_blocks * stat.f_frsize;
//             uint64_t used = (stat.f_blocks - stat.f_bfree) * stat.f_frsize;
//             uint64_t remaining = stat.f_bavail * stat.f_frsize;
            
//             report->set_capacity(capacity);
//             report->set_dfsused(used);
//             report->set_remaining(remaining);
//             report->set_blockpoolused(used);
//         }

//         // 发送心跳
//         if (nn_client_->sendHeartbeat(req, rsp) == 0) {
//             log(LogLevel::DEBUG, "Heartbeat sent successfully");
//             // 处理 NameNode 命令
//             process_namenode_commands(rsp);
//         } else {
//             log(LogLevel::WARN, "Failed to send heartbeat");
//         }

//         // 发送增量块报告
//         {
//             std::lock_guard<std::mutex> lock(pending_report_mutex_);
//             if (!received_blocks_.empty() || !deleted_blocks_.empty()) {
//                 hadoop::hdfs::BlockReceivedAndDeletedRequestProto report_req;
//                 hadoop::hdfs::BlockReceivedAndDeletedResponseProto report_rsp;
                
//                 report_req.mutable_registration()->CopyFrom(*registration);
//                 report_req.set_blockpoolid("BP-1");

//                 auto* storage_report = report_req.add_blocks();
//                 storage_report->mutable_storage()->set_storageuuid(datanode_uuid_ + "-storage");

//                 for (const auto& block_key : received_blocks_) {
//                     auto* block = storage_report->add_blocks();
//                     block->set_status(hadoop::hdfs::RECEIVED_BLOCK);
//                     // 填充块信息...
//                 }

//                 for (const auto& block_key : deleted_blocks_) {
//                     auto* block = storage_report->add_blocks();
//                     block->set_status(hadoop::hdfs::DELETED_BLOCK);
//                 }

//                 nn_client_->blockReceivedAndDeleted(report_req, report_rsp);
//                 received_blocks_.clear();
//                 deleted_blocks_.clear();
//             }
//         }

//         std::this_thread::sleep_for(std::chrono::milliseconds(heartbeat_interval_ms_));
//     }
// }

// // ==================== 块报告循环 ====================
// void HdfsDatanodeServiceImpl::block_report_loop() {
//     while (running_) {
//         std::this_thread::sleep_for(std::chrono::milliseconds(block_report_interval_ms_));
        
//         if (!running_) break;

//         log(LogLevel::INFO, "Sending block report...");

//         hadoop::hdfs::BlockReportRequestProto req;
//         hadoop::hdfs::BlockReportResponseProto rsp;

//         auto* registration = req.mutable_registration();
//         auto* datanodeid = registration->mutable_datanodeid();
//         datanodeid->set_ipaddr(datanode_hostname_);
//         datanodeid->set_hostname(datanode_hostname_);
//         datanodeid->set_datanodeuuid(datanode_uuid_);

//         req.set_blockpoolid("BP-1");

//         // 添加块报告
//         auto* report = req.add_reports();
//         report->mutable_storage()->set_storageuuid(datanode_uuid_ + "-storage");

//         {
//             std::lock_guard<std::mutex> lock(block_map_mutex_);
//             for (const auto& [key, info] : block_map_) {
//                 report->add_blocks(std::stoull(info.block_id));
//                 report->add_blocks(info.num_bytes);
//                 report->add_blocks(info.generation_stamp);
//                 report->add_blocks(0);  // 状态
//             }
//         }

//         if (nn_client_->blockReport(req, rsp) == 0) {
//             log(LogLevel::INFO, "Block report sent successfully");
//         } else {
//             log(LogLevel::ERROR, "Failed to send block report");
//         }
//     }
// }

// // ==================== 处理 NameNode 命令 ====================
// void HdfsDatanodeServiceImpl::process_namenode_commands(
//     const hadoop::hdfs::HeartbeatResponseProto& response) {
    
//     for (int i = 0; i < response.cmds_size(); ++i) {
//         const auto& cmd = response.cmds(i);
        
//         switch (cmd.action()) {
//             case hadoop::hdfs::DNA_TRANSFER:
//                 log(LogLevel::INFO, "Received DNA_TRANSFER command");
//                 // TODO: 处理块传输命令
//                 break;
                
//             case hadoop::hdfs::DNA_INVALIDATE:
//                 log(LogLevel::INFO, "Received DNA_INVALIDATE command");
//                 // 删除无效块
//                 if (cmd.has_blkcmd()) {
//                     for (int j = 0; j < cmd.blkcmd().blocks_size(); ++j) {
//                         const auto& block = cmd.blkcmd().blocks(j);
//                         std::string block_key = make_block_key(
//                             cmd.blkcmd().blockpoolid(), block.blockid());
                        
//                         std::lock_guard<std::mutex> lock(block_map_mutex_);
//                         auto it = block_map_.find(block_key);
//                         if (it != block_map_.end()) {
//                             unlink(it->second.local_path.c_str());
//                             unlink((it->second.local_path + ".meta").c_str());
//                             block_map_.erase(it);
//                             log(LogLevel::INFO, "Deleted block: %s", block_key.c_str());
//                         }
//                     }
//                 }
//                 break;
                
//             case hadoop::hdfs::DNA_SHUTDOWN:
//                 log(LogLevel::INFO, "Received DNA_SHUTDOWN command");
//                 running_ = false;
//                 break;
                
//             case hadoop::hdfs::DNA_RECOVERBLOCK:
//                 log(LogLevel::INFO, "Received DNA_RECOVERBLOCK command");
//                 // TODO: 处理块恢复命令
//                 break;
                
//             default:
//                 log(LogLevel::WARN, "Unknown command action: %d", cmd.action());
//                 break;
//         }
//     }
// }

// // ==================== 辅助方法 ====================

// void HdfsDatanodeServiceImpl::scan_storage_directory() {
//     std::string current_dir = storage_dir_ + "/current";
//     DIR* dir = opendir(current_dir.c_str());
//     if (!dir) {
//         return;
//     }

//     struct dirent* entry;
//     while ((entry = readdir(dir)) != nullptr) {
//         if (entry->d_type == DT_DIR && entry->d_name[0] != '.') {
//             // 这是一个块池目录
//             std::string pool_id = entry->d_name;
//             std::string pool_dir = current_dir + "/" + pool_id;
            
//             DIR* pool = opendir(pool_dir.c_str());
//             if (!pool) continue;

//             struct dirent* block_entry;
//             while ((block_entry = readdir(pool)) != nullptr) {
//                 std::string filename = block_entry->d_name;
//                 // 查找块文件（不是 .meta 文件）
//                 if (filename.find(".meta") == std::string::npos && 
//                     filename.substr(0, 4) == "blk_") {
                    
//                     std::string block_path = pool_dir + "/" + filename;
//                     struct stat st;
//                     if (stat(block_path.c_str(), &st) == 0) {
//                         std::string block_id = filename.substr(4);  // 去掉 "blk_" 前缀
//                         std::string block_key = make_block_key(pool_id, std::stoull(block_id));
                        
//                         BlockInfo info;
//                         info.block_id = block_id;
//                         info.block_pool_id = pool_id;
//                         info.num_bytes = st.st_size;
//                         info.local_path = block_path;
//                         info.generation_stamp = 0;  // TODO: 从元数据文件读取
                        
//                         block_map_[block_key] = info;
//                     }
//                 }
//             }
//             closedir(pool);
//         }
//     }
//     closedir(dir);
// }

// std::string HdfsDatanodeServiceImpl::make_block_key(
//     const std::string& pool_id, uint64_t block_id) {
//     return pool_id + ":" + std::to_string(block_id);
// }

// std::string HdfsDatanodeServiceImpl::get_block_path(
//     const std::string& pool_id, uint64_t block_id) {
//     return storage_dir_ + "/current/" + pool_id + "/blk_" + std::to_string(block_id);
// }

// uint32_t HdfsDatanodeServiceImpl::calculate_checksum(const std::vector<char>& data) {
//     // 简单的 CRC32 实现
//     uint32_t crc = 0xFFFFFFFF;
//     for (char byte : data) {
//         crc ^= static_cast<uint8_t>(byte);
//         for (int i = 0; i < 8; ++i) {
//             crc = (crc >> 1) ^ (0xEDB88320 & -(crc & 1));
//         }
//     }
//     return ~crc;
// }

// } // namespace hcg
