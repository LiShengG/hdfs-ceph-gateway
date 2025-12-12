#include "protocol/namenode/hdfs_namenode_service_impl.h"

#include "common/logging.h" 
#include <string>
#include "common/types.h"              // FileId, BlockIndex, BlockId, make_block_id
#include "common/status.h"
#include "gateway_internal.pb.h"      // internal::*
#include "ClientNamenodeProtocol.pb.h"
#include "hdfs.pb.h"

// HDFS 协议请求/响应
using hadoop::hdfs::GetBlockLocationsRequestProto;
using hadoop::hdfs::GetBlockLocationsResponseProto;
using hadoop::hdfs::AddBlockRequestProto;
using hadoop::hdfs::AddBlockResponseProto;

// Block 相关
using hadoop::hdfs::LocatedBlocksProto;
using hadoop::hdfs::LocatedBlockProto;
using hadoop::hdfs::ExtendedBlockProto;

// DataNode 相关
using hadoop::hdfs::DatanodeInfoProto;
using hadoop::hdfs::DatanodeIDProto;

// 存储与安全
using hadoop::hdfs::StorageTypeProto;
using hadoop::common::TokenProto;

namespace {

inline bool is_rpc_status_ok(const ::hcg::internal::RpcStatus& st) {
    return st.code() == 0;
}

inline void split_host_port(const std::string& ep,
                            std::string& host,
                            std::uint32_t& port) {
    auto pos = ep.find(':');
    if (pos == std::string::npos) {
        host = ep;
        port = 0;
        return;
    }
    host = ep.substr(0, pos);
    port = static_cast<std::uint32_t>(std::stoul(ep.substr(pos + 1)));
}

// 把 "host:port" 转成 HDFS 的 DatanodeInfoProto
inline void fill_datanode_info(const std::string& endpoint,
                               DatanodeInfoProto* dn) {
    std::string host;
    std::uint32_t port = 0;
    split_host_port(endpoint, host, port);

    dn->mutable_id()->set_ipaddr(host);
    dn->mutable_id()->set_hostname(host);
    dn->mutable_id()->set_datanodeuuid(host);
    dn->mutable_id()->set_xferport(port);
    dn->mutable_id()->set_infoport(0);
    dn->mutable_id()->set_ipcport(0);

    // dn->set_capacity(0);
    // dn->set_dfsused(0);
    // dn->set_remaining(0);
    // dn->set_blockpoolused(0);
    // dn->set_lastupdate(0);
    // dn->set_location("");           // rack 信息，先留空
    // dn->set_nondfsused(0);
    // dn->set_adminstate(hadoop::hdfs::DatanodeInfoProto::NORMAL);

}

} // anonymous namespace

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
    // 客户端后续会走 addBlock 如果已经实现 LocatedBlocks，可以在这里额外填
    // fs->mutable_locations()

    log(LogLevel::DEBUG,
        "HdfsNamenodeServiceImpl::create success src=%s length=%llu", src.c_str(),
        static_cast<unsigned long long>(fs->length()));
}

void HdfsNamenodeServiceImpl::addBlock(
    const AddBlockRequestProto& req,
    AddBlockResponseProto& rsp) {
    const std::string path = req.src();
    log(LogLevel::DEBUG,
        "HdfsNamenodeServiceImpl::addBlock src=%s", path.c_str());

    // 1. 调用 internal AllocateBlock，分配一个新的逻辑块
    internal::AllocateBlockRequest ireq;
    internal::AllocateBlockResponse irsp;

    ireq.set_path(path);

    internal_->AllocateBlock(ireq, irsp);

    if (!is_rpc_status_ok(irsp.status())) {
        log(LogLevel::ERROR,
            "addBlock: internal AllocateBlock failed src=%s code=%d msg=%s",
            path.c_str(),
            irsp.status().code(),
            irsp.status().message().c_str());

        // TODO：这里可以按需要设置 RemoteException 到 rsp，目前先直接返回
        return;
    }

    // 2. 从 internal 的 BlockHandle 中取 file_id / index / path
    const internal::BlockHandle& bh = irsp.block();
    const std::uint64_t file_id     = bh.file_id();
    const std::uint64_t block_index = bh.index();
    const std::uint64_t block_size  = irsp.block_size();

    const std::uint64_t block_len    = 0;
    const std::uint64_t block_offset = block_index * block_size;

    // 3. 计算 HDFS 视角的 blockId
    hcg::BlockId bid = hcg::make_block_id(file_id, block_index);

    // 4. 构造 LocatedBlockProto（AddBlockResponse.block）
    LocatedBlockProto* lb = rsp.mutable_block();

    // 4.1 ExtendedBlockProto（必填）
    ExtendedBlockProto* eb = lb->mutable_b();
    eb->set_poolid(block_pool_id_);                
    eb->set_blockid(static_cast<std::uint64_t>(bid));
    eb->set_generationstamp(1);                     // Stage 2 简化：统一写 1
    eb->set_numbytes(block_len);                    // 当前块有效长度，初始为 0

    // 4.2 offset：块在整个文件中的起始偏移（必填）
    lb->set_offset(block_offset);

    // 4.3 locs：至少要有一个 DatanodeInfoProto（当前网关作为虚拟 DN）
    if (!datanode_endpoint_.empty()) {
        DatanodeInfoProto* dn = lb->add_locs();
        fill_datanode_info(datanode_endpoint_, dn);
    }

    // 4.4 corrupt：Stage 2 暂不做坏块检测，统一认为 false（必填）
    lb->set_corrupt(false);

    // 4.5 blockToken：必填，但 Stage 2 可给一个“空 token”
    TokenProto* tok = lb->mutable_blocktoken();
    tok->set_identifier("");      // 空 bytes
    tok->set_password("");        // 空 bytes
    tok->set_kind("");            // 或可填 "HDFS_BLOCK_TOKEN"
    tok->set_service("");         // 先留空

    // 4.6 isCached：可选，最小策略下与 locs 对齐，每个 false 即可
    // 这里只分配一个 loc，则添加一个 false
    if (lb->locs_size() > 0) {
        lb->add_iscached(false);
    }

    // 4.7 storageTypes：建议与 locs 对齐，每个填 DISK
    if (lb->locs_size() > 0) {
        lb->add_storagetypes(StorageTypeProto::DISK);
    }

    // 4.8 storageIDs / blockIndices / blockTokens 都是 EC/多介质相关，暂不填
}

void HdfsNamenodeServiceImpl::getBlockLocation(
    const GetBlockLocationsRequestProto& req,
    GetBlockLocationsResponseProto& rsp) {
    const std::string path      = req.src();
    const std::uint64_t offset  = req.offset();
    const std::uint64_t length  = req.length();

    log(LogLevel::DEBUG,
        "HdfsNamenodeServiceImpl::getBlockLocation src=%s offset=%lu length=%lu",
        path.c_str(),
        static_cast<unsigned long>(offset),
        static_cast<unsigned long>(length));

    // 1. 转成 internal::GetBlockLocations 请求
    internal::GetBlockLocationsRequest ireq;
    internal::GetBlockLocationsResponse irsp;

    ireq.set_path(path);
    ireq.set_offset(offset);
    ireq.set_length(length);

    internal_->GetBlockLocations(ireq, irsp);

    // 2. 检查 internal status
    if (!is_rpc_status_ok(irsp.status())) {
        log(LogLevel::ERROR,
            "getBlockLocation: internal GetBlockLocations failed src=%s code=%d msg=%s",
            path.c_str(),
            irsp.status().code(),
            irsp.status().message().c_str());

        // 最简单的处理：不设置 locations，客户端会得到 null，等价于 "没有块/文件不存在"
        return;
    }

    // 3. 构造 LocatedBlocksProto（GetBlockLocationsResponse.locations）
    LocatedBlocksProto* lbs = rsp.mutable_locations();
    std::uint64_t file_len = 0;
    file_len = irsp.file_length();
    lbs->set_filelength(file_len);

    // underConstruction（必填）：Stage 2 统一当成已完成文件
    lbs->set_underconstruction(false);

    // isLastBlockComplete（必填）：已完成文件统一填 true
    lbs->set_islastblockcomplete(true);

    // lastBlock / fileEncryptionInfo / ecPolicy 暂不填

    // 4. 把 internal::BlockLocationProto -> LocatedBlockProto
    for (int i = 0; i < irsp.blocks_size(); ++i) {
        const internal::BlockLocationProto& bloc = irsp.blocks(i);
        const internal::BlockHandle& bh          = bloc.block();

        const std::uint64_t file_id     = bh.file_id();
        const std::uint64_t block_index = bh.index();
        const std::uint64_t blk_offset  = bloc.offset();
        const std::uint64_t blk_length  = bloc.length();

        // 计算逻辑 BlockId（HDFS 视角）
        hcg::BlockId bid = hcg::make_block_id(file_id, block_index);

        LocatedBlockProto* lb = lbs->add_blocks();

        // 4.1 ExtendedBlockProto（必填）
        ExtendedBlockProto* eb = lb->mutable_b();
        eb->set_poolid(block_pool_id_);                     // 例如 "BP-1"
        eb->set_blockid(static_cast<std::uint64_t>(bid));
        eb->set_generationstamp(1);                         // Stage 2 简化：统一写 1
        eb->set_numbytes(blk_length);                       // 当前块有效长度

        // 4.2 offset（必填）：块在整个文件中的起始偏移
        lb->set_offset(blk_offset);

        // 4.3 locs + isCached + storageTypes
        //
        // 最小填充策略：
        //   - locs 至少有一个 DatanodeInfoProto
        //   - isCached 与 locs 数量对齐，每个 false
        //   - storageTypes 与 locs 数量对齐，每个 DISK
        for (int j = 0; j < bloc.datanodes_size(); ++j) {
            const std::string& ep = bloc.datanodes(j);

            DatanodeInfoProto* dn = lb->add_locs();
            fill_datanode_info(ep, dn);  // 使用和 addBlock 相同的 helper

            // isCached：对应这个 location 不是 cache
            lb->add_iscached(false);

            // storageType：先简单认为都是磁盘
            lb->add_storagetypes(StorageTypeProto::DISK);
        }

        // 4.4 corrupt（必填）：Stage 2 不做坏块检测，统一认为 false
        lb->set_corrupt(false);

        // 4.5 blockToken（必填）：未启用安全，给一个“空 token”即可
        TokenProto* tok = lb->mutable_blocktoken();
        tok->set_identifier("");   // 空 bytes
        tok->set_password("");     // 空 bytes
        tok->set_kind("");         // 或 "HDFS_BLOCK_TOKEN"，目前没用
        tok->set_service("");      // 先留空

        // 4.6 storageIDs / blockIndices / blockTokens
        // 这些主要用于多介质/EC 的场景，此处按最小策略保持空即可：
        // - storageIDs 留空
        // - blockIndices 不填
        // - blockTokens 不填
    }
}

void HdfsNamenodeServiceImpl::complete(
    const hadoop::hdfs::CompleteRequestProto& req,
    hadoop::hdfs::CompleteResponseProto& rsp) {
    // TODO: 实现 complete 功能（关闭文件写入）
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