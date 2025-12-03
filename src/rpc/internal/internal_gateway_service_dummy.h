// src/rpc/internal/internal_gateway_service_dummy.h
#pragma once

#include "rpc/internal/internal_gateway_service.h"
#include "common/logging.h"

namespace hcg {

/**
 * 阶段0: 验证 RPC 框架用的 Dummy 实现
 * 不做任何真实 CephFS 操作，只简单回 status.code = 0，
 * 某些字段填一点固定值方便调试。
 */
class DummyInternalGatewayService : public IInternalGatewayService {
public:
    void CreateFile(const internal::CreateFileRequest& req,
                    internal::CreateFileResponse& rsp) override {
        log(LogLevel::INFO, "[Dummy] CreateFile path=%s", req.path().c_str());
        auto* st = rsp.mutable_status();
        st->set_code(0);
        st->set_message("");

        auto* fh = rsp.mutable_handle();
        fh->set_file_id(123);
        fh->set_path(req.path());
    }

    void GetFileInfo(const internal::GetFileInfoRequest& req,
                     internal::GetFileInfoResponse& rsp) override {
        log(LogLevel::INFO, "[Dummy] GetFileInfo path=%s", req.path().c_str());
        auto* st = rsp.mutable_status();
        st->set_code(0);
        st->set_message("");

        auto* fs = rsp.mutable_status_info();
        fs->set_path(req.path());
        fs->set_is_dir(false);
        fs->set_length(4096);
        fs->set_replication(1);
        fs->set_block_size(128 * 1024 * 1024);
        fs->set_mode(0644);
        fs->set_owner("dummy");
        fs->set_group("dummy");
    }

    void ListStatus(const internal::ListStatusRequest& req,
                    internal::ListStatusResponse& rsp) override {
        log(LogLevel::INFO, "[Dummy] ListStatus path=%s", req.path().c_str());
        auto* st = rsp.mutable_status();
        st->set_code(0);
        st->set_message("");

        auto* e = rsp.add_entries();
        e->set_path(req.path() + "/file1");
        e->set_is_dir(false);
        e->set_length(1234);
    }

    void DeletePath(const internal::DeleteRequest& req,
                    internal::DeleteResponse& rsp) override {
        log(LogLevel::INFO, "[Dummy] Delete path=%s recursive=%d",
            req.path().c_str(), static_cast<int>(req.recursive()));
        auto* st = rsp.mutable_status();
        st->set_code(0);
        st->set_message("");
    }

    void AllocateBlock(const internal::AllocateBlockRequest& req,
                       internal::AllocateBlockResponse& rsp) override {
        log(LogLevel::INFO, "[Dummy] AllocateBlock path=%s", req.path().c_str());
        auto* st = rsp.mutable_status();
        st->set_code(0);
        st->set_message("");

        auto* bh = rsp.mutable_block();
        bh->set_file_id(123);
        bh->set_index(0);
        rsp.set_block_size(128 * 1024 * 1024);
    }

    void GetBlockLocations(const internal::GetBlockLocationsRequest& req,
                           internal::GetBlockLocationsResponse& rsp) override {
        log(LogLevel::INFO, "[Dummy] GetBlockLocations path=%s", req.path().c_str());
        auto* st = rsp.mutable_status();
        st->set_code(0);
        st->set_message("");

        auto* bl = rsp.add_blocks();
        auto* bh = bl->mutable_block();
        bh->set_file_id(123);
        bh->set_index(0);
        bl->set_offset(0);
        bl->set_length(4096);
        bl->add_datanodes("127.0.0.1:50010");
    }

    void WriteBlock(const internal::WriteBlockRequest& req,
                    internal::WriteBlockResponse& rsp) override {
        log(LogLevel::INFO, "[Dummy] WriteBlock file_id=%lu index=%lu size=%zu",
            (unsigned long)req.block().file_id(),
            (unsigned long)req.block().index(),
            (size_t)req.data().size());
        auto* st = rsp.mutable_status();
        st->set_code(0);
        st->set_message("");
        rsp.set_bytes_written(req.data().size());
    }

    void ReadBlock(const internal::ReadBlockRequest& req,
                   internal::ReadBlockResponse& rsp) override {
        log(LogLevel::INFO, "[Dummy] ReadBlock file_id=%lu index=%lu len=%lu",
            (unsigned long)req.block().file_id(),
            (unsigned long)req.block().index(),
            (unsigned long)req.length());
        auto* st = rsp.mutable_status();
        st->set_code(0);
        st->set_message("");

        std::string data(req.length(), 'X');
        rsp.set_data(std::move(data));
    }

    void Complete(const internal::CompleteRequest& req,
                  internal::CompleteResponse& rsp) override {
        log(LogLevel::INFO, "[Dummy] Complete path=%s", req.path().c_str());
        auto* st = rsp.mutable_status();
        st->set_code(0);
        st->set_message("");
    }
};

} // namespace hcg
