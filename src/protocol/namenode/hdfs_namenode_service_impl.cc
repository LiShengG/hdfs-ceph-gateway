#include "protocol/namenode/hdfs_namenode_service_impl.h"

#include "common/logging.h" // 假设你有日志库

namespace hcg {

HdfsNamenodeServiceImpl::HdfsNamenodeServiceImpl(
    std::shared_ptr<IInternalGatewayService> internal)
    : internal_(std::move(internal)) {
    log(LogLevel::DEBUG, "HdfsNamenodeServiceImpl created.");
}

void HdfsNamenodeServiceImpl::mkdirs(const hadoop::hdfs::MkdirsRequestProto& req,
                                     hadoop::hdfs::MkdirsResponseProto& rsp) {
    uint32_t mode = 0644;
    if (req.has_masked() && req.masked().has_perm()) {
        mode = req.masked().perm() & 0777; // 只保留低 9 位 rwxrwxrwx
    }

    auto src = req.src();
    auto createParent = req.createparent();

    internal::MkdirRequest ireq;
    ireq.set_path(src);
    ireq.set_mode(mode);
    ireq.set_parents(createParent);

    internal::MkdirResponse ires;
    internal_->MakeDir(ireq, ires);

    log(LogLevel::INFO, "HdfsNamenodeServiceImpl::mkdirs called %s.", ires.status());
    rsp.set_result(ires.status()); 
}

void HdfsNamenodeServiceImpl::getFileInfo(
    const hadoop::hdfs::GetFileInfoRequestProto& req,
    hadoop::hdfs::GetFileInfoResponseProto& rsp) {

    log(LogLevel::DEBUG,
        "HdfsNamenodeServiceImpl::getFileInfo called for path: %s",
        req.src().c_str());

    // 1) 组装内部请求
    internal::GetFileInfoRequest ireq;
    ireq.set_path(req.src());

    // 2) 调用内部服务
    internal::GetFileInfoResponse irsp;
    internal_->GetFileInfo(ireq, irsp);

    // 3) 按 HDFS 语义处理“不存在”的情况：
    //    - 对 getFileInfo 来说，“不存在”= 返回 success 且 fs 字段为 null
    //    - proto 层就是：GetFileInfoResponseProto 中不设置 fs
    if (!irsp.has_status_info()) {
        // 这里不区分 ENOENT 和其它错误，Stage 0 先统一视为“not found”，
        // 后续如需区分，可以检查 irsp.status() 决定是否转成 RPC ERROR。
        log(LogLevel::DEBUG,
            "HdfsNamenodeServiceImpl::getFileInfo: path '%s' not found (no status_info)",
            req.src().c_str());
        return;  // 不调用 rsp.mutable_fs()，让 fs 字段缺省
    }

    // 4) 正常存在：填充 HdfsFileStatusProto
    const auto& status = irsp.status_info();
    auto* fs = rsp.mutable_fs();

    // 注意：HdfsFileStatusProto.path 是“本地名”（不带父目录）的 UTF8 bytes，
    // Stage 0 暂时可直接使用完整路径，后续再按需要裁剪为 basename。
    fs->set_path(status.path());

    fs->set_length(status.length());
    fs->set_owner(status.owner());
    fs->set_group(status.group());
    fs->set_modification_time(status.modification_time());
    fs->set_access_time(status.access_time());

    // 权限直接使用内部的 mode（假定已是 16bit POSIX 权限位）
    fs->mutable_permission()->set_perm(status.mode());

    if (status.is_dir()) {
        fs->set_filetype(::hadoop::hdfs::HdfsFileStatusProto::IS_DIR);
        // 目录的 length 通常为 0（或实现自定义），block 信息对目录意义不大
        fs->set_blocksize(0);
        fs->set_block_replication(0);
    } else {
        fs->set_filetype(::hadoop::hdfs::HdfsFileStatusProto::IS_FILE);
        // 使用内部 meta 中带出的 block_size / replication
        fs->set_blocksize(status.block_size());
        fs->set_block_replication(status.replication());
    }

    // TODO: 如果内部将 symlink 信息也放入 status，可在这里扩展：
    // if (status.is_symlink()) {
    //     fs->set_filetype(::hadoop::hdfs::HdfsFileStatusProto::IS_SYMLINK);
    //     fs->set_symlink(status.symlink_target());
    // }
}

void HdfsNamenodeServiceImpl::listStatus(
    const hadoop::hdfs::GetListingRequestProto& req,
    hadoop::hdfs::GetListingResponseProto& rsp) {

    log(LogLevel::DEBUG, "HdfsNamenodeServiceImpl::listStatus called for path: {}", req.src());

    internal::ListStatusRequest ireq;
    ireq.set_path(req.src());
    // TODO: internal::ListStatus 支持 startAfter 和 needLocation，也应传递：
    // ireq.set_start_after(req.startafter());  // 注意类型转换（bytes -> string）
    // ireq.set_need_location(req.needlocation());

    internal::ListStatusResponse irsp;
    internal_->ListStatus(ireq, irsp);

    auto* dirlist = rsp.mutable_dirlist(); 

    // 填充文件列表
    for (const auto& entry : irsp.entries()) {
        std::cout << entry.path() << " (dir: " << entry.is_dir() << ")\n";

        auto* status = dirlist->add_partiallisting();
        if (entry.is_dir()) {
            status->set_filetype(::hadoop::hdfs::HdfsFileStatusProto::IS_DIR);
        } else {
            status->set_filetype(::hadoop::hdfs::HdfsFileStatusProto::IS_FILE);
        }

        status->set_path(entry.path());
        status->set_length(entry.length());
        status->set_owner(entry.owner());
        status->set_group(entry.group());
        status->set_modification_time(entry.modification_time());
        status->set_access_time(entry.access_time());
        status->mutable_permission()->set_perm(entry.mode());
    }

    // 5. 设置 remainingEntries（示例：假设 irsp 有 has_more() 或 remaining 字段）
    // 如果 internal::ListStatusResponse 没有剩余信息，可设为 0
    dirlist->set_remainingentries(0);  
}

void HdfsNamenodeServiceImpl::deletePath(
    const hadoop::hdfs::DeleteRequestProto& req,
    hadoop::hdfs::DeleteResponseProto& rsp) {
    auto src = req.src();
    auto recursive = req.recursive();

    internal::DeleteRequest ireq;
    ireq.set_path(src);
    ireq.set_recursive(recursive);

    internal::DeleteResponse ires;
    internal_->DeletePath(ireq, ires);

    log(LogLevel::INFO, "HdfsNamenodeServiceImpl::deletePath called %d", ires.status().code());
    rsp.set_result(ires.status().code()); 
}

void HdfsNamenodeServiceImpl::create(
    const hadoop::hdfs::CreateRequestProto &req,
    hadoop::hdfs::CreateResponseProto &rsp) {
    
    rsp.Clear();

    const std::string &src = req.src();
    const std::string &client = req.clientname(); // 目前没用到，但先取出来

    // ---- 1. 解析 permission / flags / block size / replication ----
    // FsPermissionProto.perm 就是 UNIX mode bits（16 bit）
    uint32_t mode = 0644;
    if (req.has_masked() && req.masked().has_perm()) {
        mode = req.masked().perm() & 0777; // 只保留低 9 位 rwxrwxrwx
    }

    uint32_t replication = req.replication(); // HDFS 里是 uint32, 实际只用 16 bit
    uint64_t block_size = req.blocksize();    // 注意：auto-complete 看下是
                                            // blocksize() 还是 block_size()

    // createFlag 是位掩码：0x01 CREATE, 0x02 OVERWRITE, 0x04 APPEND
    uint32_t create_flag = req.createflag();
    bool overwrite = (create_flag & 0x02u) != 0;
    bool append = (create_flag & 0x04u) != 0;

    // 当前阶段不支持 append，直接打日志（严格一点可以映射成 RPC 异常）
    if (append) {
        log(LogLevel::ERROR,
            "HdfsNamenodeServiceImpl::create: APPEND not supported, src=%s "
            "client=%s",
            src.c_str(), client.c_str());
        // 当前阶段我们不走异常栈，保持与其它 RPC
        // 一致：返回一个“空响应”，上层一般会报错
        rsp.clear_fs();
        return;
    }

    bool create_parent = req.createparent();

    log(LogLevel::INFO,
        "HdfsNamenodeServiceImpl::create src=%s mode=%o repl=%u block_size=%llu "
        "overwrite=%d create_parent=%d",
        src.c_str(), mode, replication,
        static_cast<unsigned long long>(block_size), overwrite, create_parent);

    // ---- 2. 组装 internal::CreateFileRequest ----
    internal::CreateFileRequest ireq;
    ireq.set_path(src);
    ireq.set_mode(mode);
    ireq.set_replication(replication);
    ireq.set_block_size(block_size);
    ireq.set_overwrite(overwrite);
    ireq.set_create_parent(create_parent);

    internal::CreateFileResponse iresp;

    // ---- 3. 调用内部网关（CephFS 适配） ----
    internal_->CreateFile(ireq, iresp);
    if (iresp.status_code() != 0) {
        log(LogLevel::ERROR,
            "HdfsNamenodeServiceImpl::create: CreateFile error src=%s code=%d "
            "msg=%s",
            src.c_str(), iresp.status_code(), iresp.error_message().c_str());
        rsp.clear_fs();
        return;
    }

    // ---- 4. 把 internal FileStatus 映射成 HdfsFileStatusProto 填到 rsp.fs ----
    // 这里假设 CreateFileResponse 一定带回最终的文件状态（length=0 的普通文件）
    if (!iresp.has_status()) {
        // 内部没回 status，就按“无返回”处理（合法，对应 Hadoop 里的
        // VOID_CREATE_RESPONSE）
        rsp.clear_fs();
        return;
    }

    const internal::FileStatusProto& ist = iresp.filestatus();
    hadoop::hdfs::HdfsFileStatusProto *fs = rsp.mutable_fs();

    fs->set_length(ist.length()); // 文件长度，create 完为 0
    fs->set_filetype(ist.is_dir() ? hadoop::hdfs::HdfsFileStatusProto::IS_DIR
                                : hadoop::hdfs::HdfsFileStatusProto::IS_FILE);
    fs->set_block_replication(ist.replication());
    fs->set_blocksize(ist.block_size());
    fs->set_modification_time(ist.modification_time());
    fs->set_access_time(ist.access_time());

    // owner / group
    fs->set_owner(ist.owner());
    fs->set_group(ist.group());

    // permission：FsPermissionProto.perm = UNIX mode bits
    hadoop::hdfs::FsPermissionProto *perm_proto = fs->mutable_permission();
    perm_proto->set_perm(ist.mode() & 0777);

    // 路径：HdfsFileStatusProto 里 path 是 bytes，存 basename（不含父目录）
    // 这里按 HDFS 语义只存最后一段名字：
    std::string name = src;
    if (!name.empty() && name.back() == '/') {
        name.pop_back();
    }
    auto pos = name.find_last_of('/');
        if (pos != std::string::npos) {
        name = name.substr(pos + 1);
    }
    fs->set_path(name); // 注意：proto 里是 bytes path = 10; C++ 是 set_path(const
                        // std::string&)

    // 对于 Create 阶段，我们先不返回 block 位置信息（locations），HDFS
    // 客户端后续会走 addBlock 如果你已经实现 LocatedBlocks，可以在这里额外填
    // fs->mutable_locations()

    log(LogLevel::DEBUG,
        "HdfsNamenodeServiceImpl::create success src=%s length=%llu", src.c_str(),
        static_cast<unsigned long long>(fs->length()));
}

void HdfsNamenodeServiceImpl::addBlock(
    const hadoop::hdfs::AddBlockRequestProto& req,
    hadoop::hdfs::AddBlockResponseProto& rsp) {
    // TODO: 实现 addBlock 功能（为文件分配新块）
    // 1. 解析 req 获取文件路径、上次分配的块等
    // 2. 调用 internal_->allocateBlock(...) 分配新块
    // 3. 将新块信息设置到 rsp.mutable_block()
    log(LogLevel::DEBUG, "HdfsNamenodeServiceImpl::addBlock called (stub).");
    // rsp.mutable_block(); // 初始化块信息
}

void HdfsNamenodeServiceImpl::complete(
    const hadoop::hdfs::CompleteRequestProto& req,
    hadoop::hdfs::CompleteResponseProto& rsp) {
    // TODO: 实现 complete 功能（关闭文件写入）
    // 1. 解析 req 获取文件路径、最后一个块等
    // 2. 调用 internal_->completeFile(...) 完成文件写入
    // 3. 根据结果设置 rsp.set_result(true/false)
    log(LogLevel::DEBUG, "HdfsNamenodeServiceImpl::complete called (stub).");
    rsp.set_result(true); 
}

void HdfsNamenodeServiceImpl::getServerDefaults(
    const hadoop::hdfs::GetServerDefaultsRequestProto& req,
    hadoop::hdfs::GetServerDefaultsResponseProto& rsp) {
    
    auto* defaults = rsp.mutable_serverdefaults();

    // 必填字段（required）——必须全部设置！
    defaults->set_blocksize(134217728ULL);        // 128 MB (Hadoop 默认)
    defaults->set_bytesperchecksum(512);         // 校验块大小
    defaults->set_writepacketsize(65536);        // 写 packet 大小（64KB）
    defaults->set_replication(3);                // 副本数（实际用低16位）
    defaults->set_filebuffersize(4096);          // 文件缓冲区大小（4KB）

    defaults->set_encryptdatatransfer(false);    // 默认 false，可省略
    defaults->set_trashinterval(3600ULL);        // 回收站保留时间（秒），0=禁用
    defaults->set_checksumtype(hadoop::hdfs::CHECKSUM_CRC32); // 默认值，可省略
    // defaults->set_keyprovideruri("...");      // 如未配置加密，可不设
    // defaults->set_policyid(0);                // 默认 0，可省略

    log(LogLevel::DEBUG, "getServerDefaults: blockSize={}, replication={}",
        defaults->blocksize(), defaults->replication());
}

void HdfsNamenodeServiceImpl::getFsStatus(
    const hadoop::hdfs::GetFsStatusRequestProto& req,
    hadoop::hdfs::GetFsStatsResponseProto& rsp) {
    // TODO: 实现 getFsStatus 功能（获取文件系统统计信息）
    // 1. 调用 internal_->getFsStats(...) 获取容量、已用、剩余空间等
    // 2. 填充到 rsp
    log(LogLevel::DEBUG, "HdfsNamenodeServiceImpl::getFsStatus called (stub).");
    // rsp.set_capacity(0);
    // rsp.set_used(0);
    // rsp.set_remaining(0);
    // rsp.set_under_replicated(0);
    // rsp.set_corrupt_blocks(0);
    // rsp.set_missing_blocks(0);
}

} // namespace hcg