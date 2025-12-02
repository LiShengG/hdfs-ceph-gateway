#include "fsal/cephfs/cephfs_adapter.h"
#include "common/logging.h"

namespace hcg {

CephFsAdapter::CephFsAdapter() = default;
CephFsAdapter::~CephFsAdapter() = default;

int CephFsAdapter::init(const CephFsConfig& cfg) {
    cfg_ = cfg;
    // TODO: 初始化 libcephfs, mount 集群, 设置 root_prefix 等
    inited_ = true;
    log(LogLevel::INFO, "CephFsAdapter init with root_prefix=%s",
        cfg.root_prefix.c_str());
    return 0;
}

int CephFsAdapter::shutdown() {
    // TODO: 关闭 cephfs 连接
    inited_ = false;
    log(LogLevel::INFO, "CephFsAdapter shutdown");
    return 0;
}

int CephFsAdapter::create(const std::string& path, int mode, bool overwrite) {
    // TODO: 调用 ceph_open(..., O_CREAT | O_EXCL/ O_TRUNC)
    (void)path; (void)mode; (void)overwrite;
    return 0;
}

int CephFsAdapter::open(const std::string& path, int flags, int mode, int& out_fd) {
    // TODO: ceph_open
    (void)path; (void)flags; (void)mode;
    out_fd = -1;
    return 0;
}

int CephFsAdapter::close(int fd) {
    // TODO: ceph_close
    (void)fd;
    return 0;
}

long CephFsAdapter::pread(int fd, void* buf, size_t len, long offset) {
    // TODO: ceph_read
    (void)fd; (void)buf; (void)len; (void)offset;
    return 0;
}

long CephFsAdapter::pwrite(int fd, const void* buf, size_t len, long offset) {
    // TODO: ceph_write
    (void)fd; (void)buf; (void)len; (void)offset;
    return 0;
}

int CephFsAdapter::mkdir(const std::string& path, int mode, bool recursive) {
    (void)path; (void)mode; (void)recursive;
    // TODO
    return 0;
}

int CephFsAdapter::readdir(const std::string& path,
                           std::vector<std::string>& names) {
    (void)path;
    names.clear();
    // TODO
    return 0;
}

int CephFsAdapter::unlink(const std::string& path) {
    (void)path;
    // TODO
    return 0;
}

int CephFsAdapter::rename(const std::string& src, const std::string& dst) {
    (void)src; (void)dst;
    // TODO
    return 0;
}

int CephFsAdapter::get_xattr(const std::string& path,
                             const std::string& name,
                             std::string& value) {
    (void)path; (void)name;
    value.clear();
    // TODO
    return 0;
}

int CephFsAdapter::set_xattr(const std::string& path,
                             const std::string& name,
                             const std::string& value) {
    (void)path; (void)name; (void)value;
    // TODO
    return 0;
}

} // namespace hcg
