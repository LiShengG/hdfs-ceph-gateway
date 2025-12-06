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
    // TODO: 实现 mkdirs 功能
    // 1. 解析 req 获取路径、权限等信息
    // 2. 调用 internal_->createDirectory(...) 创建目录
    // 3. 根据结果设置 rsp.set_result(true/false)
    log(LogLevel::DEBUG, "HdfsNamenodeServiceImpl::mkdirs called (stub).");
    rsp.set_result(false); // 默认失败
}

void HdfsNamenodeServiceImpl::getFileInfo(
    const hadoop::hdfs::GetFileInfoRequestProto& req,
    hadoop::hdfs::GetFileInfoResponseProto& rsp) {
    // TODO: 实现 getFileInfo 功能
    // 1. 解析 req 获取文件路径
    // 2. 调用 internal_->getAttr(...) 获取文件元数据
    // 3. 将元数据填充到 rsp.mutable_fs()
    log(LogLevel::DEBUG, "HdfsNamenodeServiceImpl::getFileInfo called (stub).");
    // rsp.mutable_fs()->set_path(req.src()); // 示例：设置路径
    // rsp.mutable_fs()->set_length(0);
    // rsp.mutable_fs()->set_isdir(false);
    // rsp.mutable_fs()->set_blocksize(0);
    // rsp.mutable_fs()->set_mtime(0);
    // rsp.mutable_fs()->set_atime(0);
    // rsp.mutable_fs()->set_permission(0755);
    // rsp.mutable_fs()->set_owner("root");
    // rsp.mutable_fs()->set_group("root");
}

void HdfsNamenodeServiceImpl::listStatus(
    const hadoop::hdfs::GetListingRequestProto& req,
    hadoop::hdfs::GetListingResponseProto& rsp) {
    // TODO: 实现 listStatus 功能
    // 1. 解析 req 获取目录路径和是否递归
    // 2. 调用 internal_->listDirectory(...) 获取目录项列表
    // 3. 将列表填充到 rsp.mutable_entries()
    log(LogLevel::DEBUG, "HdfsNamenodeServiceImpl::listStatus called (stub).");
    // rsp.set_remainingentries(0); // 示例：设置剩余条目数
}

void HdfsNamenodeServiceImpl::deletePath(
    const hadoop::hdfs::DeleteRequestProto& req,
    hadoop::hdfs::DeleteResponseProto& rsp) {
    // TODO: 实现 deletePath 功能
    // 1. 解析 req 获取路径和是否递归删除
    // 2. 调用 internal_->remove(...) 删除文件或目录
    // 3. 根据结果设置 rsp.set_result(true/false)
    log(LogLevel::DEBUG, "HdfsNamenodeServiceImpl::deletePath called (stub).");
    rsp.set_result(false); // 默认失败
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
    bool ok = internal_->CreateFile(ireq, &iresp);
    if (!ok) {
    log(LogLevel::ERROR,
        "HdfsNamenodeServiceImpl::create: internal_->CreateFile RPC failed, "
        "src=%s",
        src.c_str());
        rsp.clear_fs();
        return;
    }

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

    const internal::FileStatus &ist = iresp.status();

    hadoop::hdfs::HdfsFileStatusProto *fs = rsp.mutable_fs();

    // 根据你 gateway_internal.proto 定义调整这些字段名字
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
    rsp.set_result(false); // 默认失败
}

void HdfsNamenodeServiceImpl::getServerDefaults(
    const hadoop::hdfs::GetServerDefaultsRequestProto& req,
    hadoop::hdfs::GetServerDefaultsResponseProto& rsp) {
    // TODO: 实现 getServerDefaults 功能（获取 HDFS 服务器默认配置）
    // 1. 从 internal_ 或配置中获取默认值（如 blocksize, replication 等）
    // 2. 填充到 rsp.mutable_stats()
    log(LogLevel::DEBUG, "HdfsNamenodeServiceImpl::getServerDefaults called (stub).");
    // rsp.mutable_stats(); // 初始化默认配置
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
    // rsp.set_num_files(0);
}

} // namespace hcg