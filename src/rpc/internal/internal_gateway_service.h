// src/rpc/internal/internal_gateway_service.h
#pragma once

#include "gateway_internal.pb.h" // 由 proto 生成
#include "common/status.h"
#include "common/types.h"

namespace hcg {

/**
 * 内部网关服务接口：
 * 每个方法对应一个 proto 里的 RPC。
 * 这里我们不使用 protobuf 生成的 service base class，而是自己定义一个简单接口，
 * 便于和 core 层解耦。
 */
class IInternalGatewayService {
public:
    virtual ~IInternalGatewayService() = default;

    virtual void MakeDir(const internal::MkdirRequest& req,
                            internal::MkdirResponse& rsp) = 0;

    virtual void CreateFile(const internal::CreateFileRequest& req,
                            internal::CreateFileResponse& rsp) = 0;

    virtual void GetFileInfo(const internal::GetFileInfoRequest& req,
                             internal::GetFileInfoResponse& rsp) = 0;

    virtual void ListStatus(const internal::ListStatusRequest& req,
                            internal::ListStatusResponse& rsp) = 0;

    virtual void DeletePath(const internal::DeleteRequest& req,
                            internal::DeleteResponse& rsp) = 0;

    virtual void AllocateBlock(const internal::AllocateBlockRequest& req,
                               internal::AllocateBlockResponse& rsp) = 0;

    virtual void GetBlockLocations(const internal::GetBlockLocationsRequest& req,
                                   internal::GetBlockLocationsResponse& rsp) = 0;

    virtual void WriteBlock(const internal::WriteBlockRequest& req,
                            internal::WriteBlockResponse& rsp) = 0;

    virtual void ReadBlock(const internal::ReadBlockRequest& req,
                           internal::ReadBlockResponse& rsp) = 0;

    virtual void Complete(const internal::CompleteRequest& req,
                          internal::CompleteResponse& rsp) = 0;

    virtual Status ResolveBlock(BlockId block_id,
                                std::string& out_path,
                                FileId& out_file_id,
                                BlockInfo& out_block,
                                std::uint64_t& out_block_size) = 0;
    
    virtual Status Rename(const std::string& src, const std::string& dst) = 0;
};

} // namespace hcg
