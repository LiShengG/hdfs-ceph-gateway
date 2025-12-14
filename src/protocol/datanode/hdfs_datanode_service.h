#pragma once

#include "hdfs.pb.h"
#include "datatransfer.pb.h"
#include "DatanodeProtocol.pb.h"
#include "ClientDatanodeProtocol.pb.h"

namespace hcg {

// DataNode 对外提供的服务接口（处理客户端和其他 DataNode 的请求）
class IHdfsDatanodeService {
public:
    virtual ~IHdfsDatanodeService() = default;

    // ==================== 数据传输协议 (DataTransferProtocol) ====================
    
    // 读取块数据
    virtual void readBlock(
        const hadoop::hdfs::OpReadBlockProto& req,
        hadoop::hdfs::BlockOpResponseProto& rsp,
        std::vector<char>& data) = 0;

    // 写入块数据
    virtual void writeBlock(
        const hadoop::hdfs::OpWriteBlockProto& req,
        const std::vector<char>& data,
        hadoop::hdfs::BlockOpResponseProto& rsp) = 0;

    // 传输块到另一个 DataNode（用于复制）
    virtual void transferBlock(
        const hadoop::hdfs::OpTransferBlockProto& req,
        hadoop::hdfs::BlockOpResponseProto& rsp) = 0;

    // 复制块
    virtual void copyBlock(
        const hadoop::hdfs::OpCopyBlockProto& req,
        hadoop::hdfs::BlockOpResponseProto& rsp) = 0;

    // 替换块
    virtual void replaceBlock(
        const hadoop::hdfs::OpReplaceBlockProto& req,
        hadoop::hdfs::BlockOpResponseProto& rsp) = 0;

    // 请求校验和
    virtual void requestShortCircuitAccess(
        const hadoop::hdfs::OpRequestShortCircuitAccessProto& req,
        hadoop::hdfs::BlockOpResponseProto& rsp) = 0;

    // ==================== 客户端协议 (ClientDatanodeProtocol) ====================

    // 获取副本可见长度
    virtual void getReplicaVisibleLength(
        const hadoop::hdfs::GetReplicaVisibleLengthRequestProto& req,
        hadoop::hdfs::GetReplicaVisibleLengthResponseProto& rsp) = 0;

    // 获取块本地路径信息
    virtual void getBlockLocalPathInfo(
        const hadoop::hdfs::GetBlockLocalPathInfoRequestProto& req,
        hadoop::hdfs::GetBlockLocalPathInfoResponseProto& rsp) = 0;

    // 删除块池
    virtual void deleteBlockPool(
        const hadoop::hdfs::DeleteBlockPoolRequestProto& req,
        hadoop::hdfs::DeleteBlockPoolResponseProto& rsp) = 0;

    // 关闭 DataNode
    virtual void shutdownDatanode(
        const hadoop::hdfs::ShutdownDatanodeRequestProto& req,
        hadoop::hdfs::ShutdownDatanodeResponseProto& rsp) = 0;

    // 获取 DataNode 信息
    virtual void getDatanodeInfo(
        const hadoop::hdfs::GetDatanodeInfoRequestProto& req,
        hadoop::hdfs::GetDatanodeInfoResponseProto& rsp) = 0;

};

} // namespace hcg
