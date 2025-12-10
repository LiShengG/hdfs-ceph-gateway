#include "rpc/internal/internal_gateway_service_impl.h"
#include "common/logging.h"

#include <algorithm>
#include <functional>

namespace hcg
{

    InternalGatewayServiceImpl::InternalGatewayServiceImpl(
        std::shared_ptr<CephFsAdapter> ceph,
        std::shared_ptr<IMetadataStore> meta_store)
        : ceph_(std::move(ceph)),
          meta_store_(std::move(meta_store)) {}

    void InternalGatewayServiceImpl::set_status_ok(internal::RpcStatus *st)
    {
        st->set_code(0);
        st->set_message("");
    }

    void InternalGatewayServiceImpl::set_status_err(internal::RpcStatus *st,
                                                    int code,
                                                    const std::string &msg)
    {
        st->set_code(code);
        st->set_message(msg);
    }

    std::uint64_t InternalGatewayServiceImpl::calc_file_id_from_path(
        const std::string &path)
    {
        // Stage 0 简单做：std::hash
        return std::hash<std::string>{}(path);
    }

    // ---------- Mkdir ----------

    void InternalGatewayServiceImpl::MakeDir(
        const internal::MkdirRequest &req,
        internal::MkdirResponse &rsp)
    {
        rsp.Clear();

        const std::string &path = req.path();
        const uint32_t mode = req.mode();
        const bool parents = req.parents();

        log(LogLevel::INFO, "Mkdir: path=%s, mode=%o, parents=%d",
            path.c_str(), mode, static_cast<int>(parents));

        if (path.empty())
        {
            rsp.set_status(-EINVAL);
            rsp.set_error_message("path is empty");
            return;
        }

        int rc = ceph_->mkdir(path, mode, parents);
        if (rc < 0)
        {
            rsp.set_status(rc);
            rsp.set_error_message("mkdir failed");
            log(LogLevel::ERROR, "Mkdir failed: path=%s, mode=%o, parents=%d, rc=%d",
                path.c_str(), mode, static_cast<int>(parents), rc);
            return;
        }

        rsp.set_status(0);
        log(LogLevel::INFO, "Mkdir success: path=%s, parents=%d",
            path.c_str(), static_cast<int>(parents));
    }

    // ---------- CreateFile ----------

    void InternalGatewayServiceImpl::CreateFile(
        const internal::CreateFileRequest &req,
        internal::CreateFileResponse &rsp)
    {
        auto *st = rsp.mutable_status();
        set_status_ok(st);

        const std::string &path = req.path();
        if (path.empty() || path[0] != '/')
        {
            set_status_err(st, -1, "path must be absolute");
            return;
        }

        // 1) 创建父目录（递归）
        auto pos = path.find_last_of('/');
        std::string parent = (pos == 0) ? "/" : path.substr(0, pos);
        if (!parent.empty())
        {
            int rc = ceph_->mkdir(parent, 0755, true);
            if (rc < 0 && rc != -EEXIST)
            {
                set_status_err(st, rc, "mkdir parent failed");
                return;
            }
        }

        // 2) 创建文件（如果存在，先覆盖）
        int rc = ceph_->create(path, req.mode(), /*overwrite*/ true);
        if (rc < 0)
        {
            set_status_err(st, rc, "create file failed");
            return;
        }

        // 3) 初始化 meta
        FileBlockMeta meta;
        meta.block_size = req.block_size() ? req.block_size()
                                           : 128ULL * 1024 * 1024;
        meta.replication = req.replication() ? req.replication() : 1;
        meta.length = 0;

        if (meta_store_->save_file_block_meta(path, meta) != Status::OK())
        {
            set_status_err(st, -1, "save_file_block_meta failed");
            return;
        }

        // 4) 返回 FileHandle
        auto *fh = rsp.mutable_handle();
        fh->set_file_id(calc_file_id_from_path(path));
        fh->set_path(path);

        log(LogLevel::INFO, "CreateFile path=%s block_size=%lu",
            path.c_str(), (unsigned long)meta.block_size);
    }

    // ---------- GetFileInfo ----------

    void InternalGatewayServiceImpl::GetFileInfo(
        const internal::GetFileInfoRequest &req,
        internal::GetFileInfoResponse &rsp)
    {
        auto *st = rsp.mutable_status();
        set_status_ok(st);

        const std::string &path = req.path();
        CephStat cs;
        int rc = ceph_->stat(path, cs);
        if (rc < 0)
        {
            set_status_err(st, rc, "stat failed");
            return;
        }

        FileBlockMeta meta;
        bool has_meta = meta_store_->load_file_block_meta(path, meta) != Status::OK();
        if (!has_meta)
        {
            // 对于目录或非 HDFS 产生的文件，meta 可能不存在
            meta.block_size = 128ULL * 1024 * 1024;
            meta.replication = 1;
            meta.length = cs.size;
        }

        auto *fs = rsp.mutable_status_info();
        fs->set_path(path);
        fs->set_is_dir(cs.is_dir);
        fs->set_length(cs.size);
        fs->set_replication(meta.replication);
        fs->set_block_size(meta.block_size);
        fs->set_mode(cs.mode);
        fs->set_owner("hdfs"); // Stage 0 简化
        fs->set_group("hdfs");
        fs->set_modification_time(cs.mtime_sec * 1000);
        fs->set_access_time(cs.atime_sec * 1000);
    }

    // ---------- ListStatus ----------

    void InternalGatewayServiceImpl::ListStatus(
        const internal::ListStatusRequest &req,
        internal::ListStatusResponse &rsp)
    {
        auto *st = rsp.mutable_status();
        set_status_ok(st);

        const std::string &path = req.path();

        // Readdir
        std::vector<std::string> names;
        int rc = ceph_->readdir(path, names);
        if (rc < 0)
        {
            set_status_err(st, rc, "readdir failed");
            return;
        }

        for (const auto &name : names)
        {
            std::string child_path = (path == "/") ? ("/" + name)
                                                   : (path + "/" + name);
            CephStat cs;
            rc = ceph_->stat(child_path, cs);
            if (rc < 0)
            {
                // 忽略坏条目
                continue;
            }

            FileBlockMeta meta;
            bool has_meta = meta_store_->load_file_block_meta(child_path, meta) != Status::OK();

            auto *e = rsp.add_entries();
            e->set_path(child_path);
            e->set_is_dir(cs.is_dir);
            e->set_length(cs.size);
            e->set_replication(has_meta ? meta.replication : 1);
            e->set_block_size(has_meta ? meta.block_size
                                       : 128ULL * 1024 * 1024);
            e->set_mode(cs.mode);
            e->set_owner("hdfs");
            e->set_group("hdfs");
        }
    }

    // ---------- DeletePath ----------

    void InternalGatewayServiceImpl::DeletePath(
        const internal::DeleteRequest &req,
        internal::DeleteResponse &rsp)
    {
        auto *st = rsp.mutable_status();
        set_status_ok(st);

        const std::string &path = req.path();

        CephStat cs;
        int rc = ceph_->stat(path, cs);
        if (rc < 0)
        {
            set_status_err(st, rc, "stat failed");
            return;
        }

        if (!cs.is_dir)
        {
            rc = ceph_->unlink(path);
            if (rc < 0)
            {
                set_status_err(st, rc, "unlink failed");
                return;
            }
        }
        else
        {
            // Stage 0：简单实现递归删除
            if (req.recursive())
            {
                std::vector<std::string> names;
                rc = ceph_->readdir(path, names);
                if (rc < 0)
                {
                    set_status_err(st, rc, "readdir failed");
                    return;
                }
                for (const auto &name : names)
                {
                    std::string child = (path == "/") ? ("/" + name)
                                                      : (path + "/" + name);
                    internal::DeleteRequest sub_req;
                    sub_req.set_path(child);
                    sub_req.set_recursive(true);
                    internal::DeleteResponse sub_rsp;
                    DeletePath(sub_req, sub_rsp);
                    // 忽略子错误，或者将最先的错误带出去，这里简化忽略
                }
            }

            // 目录本身
            // 你可以在 CephFsAdapter 里增加 rmdir 接口，这里先简单用 unlink
            rc = ceph_->unlink(path); // 如果使用 rmdir，请改成 ceph_->rmdir(path)
            if (rc < 0)
            {
                set_status_err(st, rc, "rmdir failed");
                return;
            }
        }
    }

    // ---------- AllocateBlock ----------

    void InternalGatewayServiceImpl::AllocateBlock(
        const internal::AllocateBlockRequest &req,
        internal::AllocateBlockResponse &rsp)
    {
        auto *st = rsp.mutable_status();
        set_status_ok(st);

        const std::string &path = req.path();

        FileBlockMeta meta;
        if (meta_store_->load_file_block_meta(path, meta) != Status::OK())
        {
            // 若没有 meta，尝试读一遍 stat 初始化 length
            CephStat cs;
            int rc = ceph_->stat(path, cs);
            if (rc < 0)
            {
                set_status_err(st, rc, "stat failed");
                return;
            }
            meta.length = cs.size;
            meta.block_size = 128ULL * 1024 * 1024;
            meta.replication = 1;
        }

        // 当前 length 决定下一个 block index
        std::uint64_t block_size = meta.block_size;
        std::uint64_t curr_len = meta.length;
        std::uint64_t next_index = curr_len / block_size;

        // 这里先不立即更新 length，等写入时再根据实际写入长度更新
        if (meta_store_->save_file_block_meta(path, meta) != Status::OK())
        {
            set_status_err(st, -1, "save_file_block_meta failed");
            return;
        }

        auto *bh = rsp.mutable_block();
        bh->set_file_id(calc_file_id_from_path(path));
        bh->set_index(next_index);
        bh->set_path(path);

        rsp.set_block_size(block_size);

        log(LogLevel::INFO, "AllocateBlock path=%s index=%lu block_size=%lu",
            path.c_str(),
            (unsigned long)next_index,
            (unsigned long)block_size);
    }

    // ---------- GetBlockLocations ----------

    void InternalGatewayServiceImpl::GetBlockLocations(
        const internal::GetBlockLocationsRequest &req,
        internal::GetBlockLocationsResponse &rsp)
    {
        auto *st = rsp.mutable_status();
        set_status_ok(st);

        const std::string &path = req.path();
        CephStat cs;
        int rc = ceph_->stat(path, cs);
        if (rc < 0)
        {
            set_status_err(st, rc, "stat failed");
            return;
        }

        FileBlockMeta meta;
        if (meta_store_->load_file_block_meta(path, meta) != Status::OK())
        {
            meta.block_size = 128ULL * 1024 * 1024;
            meta.replication = 1;
            meta.length = cs.size;
        }
        std::uint64_t file_len = cs.size;
        std::uint64_t block_size = meta.block_size;

        if (file_len == 0)
        {
            return; // 空文件，无 block
        }

        // 简单分块：0 .. file_len-1
        std::uint64_t num_blocks =
            (file_len + block_size - 1) / block_size;

        for (std::uint64_t i = 0; i < num_blocks; ++i)
        {
            auto *bl = rsp.add_blocks();
            auto *bh = bl->mutable_block();
            bh->set_file_id(calc_file_id_from_path(path));
            bh->set_index(i);
            bh->set_path(path);

            std::uint64_t off = i * block_size;
            std::uint64_t len = std::min(block_size, file_len - off);
            bl->set_offset(off);
            bl->set_length(len);

            // Stage 0：单 DataNode 地址
            bl->add_datanodes("127.0.0.1:50010");
        }
    }

    // ---------- WriteBlock ----------

    void InternalGatewayServiceImpl::WriteBlock(
        const internal::WriteBlockRequest &req,
        internal::WriteBlockResponse &rsp)
    {
        auto *st = rsp.mutable_status();
        set_status_ok(st);

        const auto &bh = req.block();
        const std::string &path = bh.path();
        std::uint64_t index = bh.index();
        std::uint64_t offset_in_block = req.offset_in_block();
        const std::string &data = req.data();

        FileBlockMeta meta;
        if (meta_store_->load_file_block_meta(path, meta) != Status::OK())
        {
            // 说明可能是非 HDFS 文件，这里给默认
            meta.block_size = 128ULL * 1024 * 1024;
            meta.replication = 1;
            meta.length = 0;
        }

        std::uint64_t base_offset = index * meta.block_size;
        std::uint64_t file_offset = base_offset + offset_in_block;

        int fd = -1;
        int rc = ceph_->open(path, O_WRONLY, 0644, fd);
        if (rc < 0)
        {
            set_status_err(st, rc, "open for write failed");
            return;
        }

        long written = ceph_->pwrite(fd, data.data(), data.size(), file_offset);
        ceph_->close(fd);
        if (written < 0)
        {
            set_status_err(st, (int)written, "pwrite failed");
            return;
        }

        rsp.set_bytes_written((std::uint64_t)written);

        // 更新 length
        std::uint64_t new_end = file_offset + (std::uint64_t)written;
        if (new_end > meta.length)
        {
            meta.length = new_end;
            meta_store_->save_file_block_meta(path, meta);
        }

        log(LogLevel::DEBUG, "WriteBlock path=%s index=%lu off=%lu len=%zu",
            path.c_str(),
            (unsigned long)index,
            (unsigned long)offset_in_block,
            data.size());
    }

    // ---------- ReadBlock ----------

    void InternalGatewayServiceImpl::ReadBlock(
        const internal::ReadBlockRequest &req,
        internal::ReadBlockResponse &rsp)
    {
        auto *st = rsp.mutable_status();
        set_status_ok(st);

        const auto &bh = req.block();
        const std::string &path = bh.path();
        std::uint64_t index = bh.index();
        std::uint64_t offset_in_block = req.offset_in_block();
        std::uint64_t length = req.length();

        FileBlockMeta meta;
        if (meta_store_->load_file_block_meta(path, meta) != Status::OK())
        {
            // 如果没有 meta，就从 stat 推 length 和默认 block_size
            CephStat cs;
            int rc = ceph_->stat(path, cs);
            if (rc < 0)
            {
                set_status_err(st, rc, "stat failed");
                return;
            }
            meta.block_size = 128ULL * 1024 * 1024;
            meta.replication = 1;
            meta.length = cs.size;
        }

        std::uint64_t base_offset = index * meta.block_size;
        std::uint64_t file_offset = base_offset + offset_in_block;

        int fd = -1;
        int rc = ceph_->open(path, O_RDONLY, 0, fd);
        if (rc < 0)
        {
            set_status_err(st, rc, "open for read failed");
            return;
        }

        std::string buf;
        buf.resize((size_t)length);

        long r = ceph_->pread(fd, buf.data(), buf.size(), file_offset);
        ceph_->close(fd);
        if (r < 0)
        {
            set_status_err(st, (int)r, "pread failed");
            return;
        }
        buf.resize((size_t)r);
        rsp.set_data(std::move(buf));
    }

    // ---------- Complete ----------

    void InternalGatewayServiceImpl::Complete(
        const internal::CompleteRequest &req,
        internal::CompleteResponse &rsp)
    {
        auto *st = rsp.mutable_status();
        set_status_ok(st);

        const std::string &path = req.path();
        // Stage 0：暂时不做额外操作。将来可以在这里做：
        // - Lease 释放
        // - 最终 meta 校验
        log(LogLevel::INFO, "Complete path=%s", path.c_str());
    }

} // namespace hcg
