#pragma once

#include "protocol/namenode/hdfs_namenode_service.h"
#include "rpc/internal/internal_gateway_service.h"

namespace hcg {

class HdfsNamenodeServiceImpl : public IHdfsNamenodeService {
public:
    explicit HdfsNamenodeServiceImpl(
        std::shared_ptr<IInternalGatewayService> internal);

    void mkdirs(const hadoop::hdfs::MkdirsRequestProto& req,
                hadoop::hdfs::MkdirsResponseProto& rsp) override;

    void getFileInfo(const hadoop::hdfs::GetFileInfoRequestProto& req,
                     hadoop::hdfs::GetFileInfoResponseProto& rsp) override;

    void listStatus(const hadoop::hdfs::GetListingRequestProto& req,
                    hadoop::hdfs::GetListingResponseProto& rsp) override;

    void deletePath(const hadoop::hdfs::DeleteRequestProto& req,
                    hadoop::hdfs::DeleteResponseProto& rsp) override;

    void create(const hadoop::hdfs::CreateRequestProto& req,
                hadoop::hdfs::CreateResponseProto& rsp) override;

    void addBlock(const hadoop::hdfs::AddBlockRequestProto& req,
                  hadoop::hdfs::AddBlockResponseProto& rsp) override;

    void getBlockLocation(const hadoop::hdfs::GetBlockLocationsRequestProto& req,
                hadoop::hdfs::GetBlockLocationsResponseProto& rsp);

    void complete(const hadoop::hdfs::CompleteRequestProto& req,
                  hadoop::hdfs::CompleteResponseProto& rsp) override;

    void getServerDefaults(
        const hadoop::hdfs::GetServerDefaultsRequestProto& req,
        hadoop::hdfs::GetServerDefaultsResponseProto& rsp) override;

    void getFsStatus(
        const hadoop::hdfs::GetFsStatusRequestProto& req,
        hadoop::hdfs::GetFsStatsResponseProto& rsp) override;

private:
    std::shared_ptr<IInternalGatewayService> internal_;
    std::string block_pool_id_ = "BP-1";         // 简单写死一个 BlockPoolId
    std::string datanode_endpoint_;  
};

} // namespace hcg
