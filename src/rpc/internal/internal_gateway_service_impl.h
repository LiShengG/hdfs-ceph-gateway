#pragma once

#include "rpc/internal/internal_gateway_service.h"
#include "fsal/cephfs/cephfs_adapter.h"
#include "meta/metadata_store.h"
#include "meta/xattr_metadata_store.h"

namespace hcg {

class InternalGatewayServiceImpl : public IInternalGatewayService {
public:
    InternalGatewayServiceImpl(std::shared_ptr<CephFsAdapter> ceph,
                               std::shared_ptr<IMetadataStore> meta_store);

    // RPC 实现
    void CreateFile(const internal::CreateFileRequest& req,
                    internal::CreateFileResponse& rsp) override;

    void GetFileInfo(const internal::GetFileInfoRequest& req,
                     internal::GetFileInfoResponse& rsp) override;

    void ListStatus(const internal::ListStatusRequest& req,
                    internal::ListStatusResponse& rsp) override;

    void DeletePath(const internal::DeleteRequest& req,
                    internal::DeleteResponse& rsp) override;

    void AllocateBlock(const internal::AllocateBlockRequest& req,
                       internal::AllocateBlockResponse& rsp) override;

    void GetBlockLocations(const internal::GetBlockLocationsRequest& req,
                           internal::GetBlockLocationsResponse& rsp) override;

    void WriteBlock(const internal::WriteBlockRequest& req,
                    internal::WriteBlockResponse& rsp) override;

    void ReadBlock(const internal::ReadBlockRequest& req,
                   internal::ReadBlockResponse& rsp) override;

    void Complete(const internal::CompleteRequest& req,
                  internal::CompleteResponse& rsp) override;

private:
    std::shared_ptr<CephFsAdapter> ceph_;
    std::shared_ptr<IMetadataStore> meta_store_;

    // 工具方法
    void set_status_ok(internal::RpcStatus* st);
    void set_status_err(internal::RpcStatus* st, int code, const std::string& msg);

    // 简单 path -> file_id 映射：先用 hash
    std::uint64_t calc_file_id_from_path(const std::string& path);
};

} // namespace hcg
