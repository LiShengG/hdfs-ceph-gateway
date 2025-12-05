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
    const hadoop::hdfs::CreateRequestProto& req,
    hadoop::hdfs::CreateResponseProto& rsp) {
    // TODO: 实现 create 功能（创建文件）
    // 1. 解析 req 获取文件路径、副本数、块大小等
    // 2. 调用 internal_->createFile(...) 创建文件
    // 3. 如果成功，获取第一个 Block 信息并设置到 rsp.mutable_fs()
    log(LogLevel::DEBUG, "HdfsNamenodeServiceImpl::create called (stub).");
    // rsp.mutable_fs(); // 初始化文件状态
    rsp.clear_fs(); // 明确表示未设置
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