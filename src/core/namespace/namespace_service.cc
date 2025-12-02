#include "core/namespace/namespace_service.h"
#include "common/logging.h"

namespace hcg {

NamespaceService::NamespaceService() = default;

Status NamespaceService::mkdirs(const std::string& path, std::uint32_t mode) {
    (void)path; (void)mode;
    // TODO: 调用 CephFsAdapter::mkdir
    return Status::OK();
}

Status NamespaceService::delete_path(const std::string& path, bool recursive) {
    (void)path; (void)recursive;
    // TODO: unlink / rmdir
    return Status::OK();
}

Status NamespaceService::get_file_status(const std::string& path,
                                         FileStatus& status) {
    (void)path;
    // TODO: ceph_stat + xattr
    status.path = path;
    return Status::OK();
}

Status NamespaceService::list_status(const std::string& path,
                                     std::vector<FileStatus>& out) {
    (void)path;
    out.clear();
    // TODO: readdir + get_file_status
    return Status::OK();
}

Status NamespaceService::create_file(const std::string& path,
                                     const FileStatus& initial,
                                     FileId& out_file_id) {
    (void)initial;
    (void)path;
    out_file_id = 0;
    // TODO: 创建 CephFS 文件 + 写 xattr
    return Status::OK();
}

} // namespace hcg
