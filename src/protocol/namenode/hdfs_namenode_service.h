#pragma once

#include <memory>
#include <string>

#include "gateway_internal.pb.h"              // internal::*
#include "ClientNamenodeProtocol.pb.h"        // 来自 Hadoop proto

namespace hcg {

// HDFS NameNode 协议服务接口
class IHdfsNamenodeService {
public:
    virtual ~IHdfsNamenodeService() = default;

    // 元数据类
    virtual void mkdirs(const hadoop::hdfs::MkdirsRequestProto& req,
                        hadoop::hdfs::MkdirsResponseProto& rsp) = 0;

    virtual void getFileInfo(const hadoop::hdfs::GetFileInfoRequestProto& req,
                             hadoop::hdfs::GetFileInfoResponseProto& rsp) = 0;

    virtual void listStatus(const hadoop::hdfs::GetListingRequestProto& req,
                            hadoop::hdfs::GetListingResponseProto& rsp) = 0;

    virtual void deletePath(const hadoop::hdfs::DeleteRequestProto& req,
                            hadoop::hdfs::DeleteResponseProto& rsp) = 0;

    // 写路径类
    virtual void create(const hadoop::hdfs::CreateRequestProto& req,
                        hadoop::hdfs::CreateResponseProto& rsp) = 0;

    virtual void addBlock(const hadoop::hdfs::AddBlockRequestProto& req,
                          hadoop::hdfs::AddBlockResponseProto& rsp) = 0;

    virtual void getBlockLocation(const hadoop::hdfs::GetBlockLocationsRequestProto& req,
                        hadoop::hdfs::GetBlockLocationsResponseProto& rsp) = 0;

    virtual void complete(const hadoop::hdfs::CompleteRequestProto& req,
                          hadoop::hdfs::CompleteResponseProto& rsp) = 0;

    // 其它辅助
    virtual void getServerDefaults(
        const hadoop::hdfs::GetServerDefaultsRequestProto& req,
        hadoop::hdfs::GetServerDefaultsResponseProto& rsp) = 0;

    virtual void getFsStatus(
        const hadoop::hdfs::GetFsStatusRequestProto& req,
        hadoop::hdfs::GetFsStatsResponseProto& rsp) = 0;

    virtual void abandonBlock(const hadoop::hdfs::AbandonBlockRequestProto& req,
                hadoop::hdfs::AbandonBlockResponseProto& rsp) = 0;
};

} // namespace hcg
