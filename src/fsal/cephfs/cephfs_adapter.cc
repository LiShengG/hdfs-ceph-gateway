#include "fsal/cephfs/cephfs_adapter.h"
#include "common/logging.h"

#include <cerrno>
#include <cstring>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>

namespace hcg {

CephFsAdapter::CephFsAdapter() = default;
CephFsAdapter::~CephFsAdapter() {
    if (inited_) {
        shutdown();
    }
}

std::string CephFsAdapter::full_path(const std::string& path) {
    // 简单拼：root_prefix + path
    if (path.empty() || path[0] != '/') {
        // 你可以再细化错误处理
        return cfg_.root_prefix + "/" + path;
    }
    return cfg_.root_prefix + path;
}


int CephFsAdapter::init(const CephFsConfig& cfg) {
    cfg_ = cfg;
    // TODO: 初始化 libcephfs, mount 集群, 设置 root_prefix 等
    int ret = ceph_create(&cm_, cfg_.user.c_str());
    if (ret < 0)
    {
        log(LogLevel::INFO, "[ERROR] ceph_create failed, %s", ret);
        return 1;
    }

    ceph_conf_set(cm_, "mon_host", cfg_.mon_hosts.c_str());
    ceph_conf_set(cm_, "keyring", cfg_.keyring_path.c_str());

    ceph_conf_read_file(cm_, NULL);

    ceph_conf_set(cm_, "auth_supported", "cephx");
    ceph_conf_set(cm_, "ms_auth_supported", "cephx");

    // 开启详细日志
    ceph_conf_set(cm_, "log_to_stderr", "true");
    // ceph_conf_set(cm_, "debug_client", "10");
    // ceph_conf_set(cm_, "debug_auth", "10");
    // ceph_conf_set(cm_, "debug_ms", "10");

    ret = ceph_init(cm_);
    if (ret < 0)
    {
        log(LogLevel::INFO, "ceph_init failed");
        ceph_shutdown(cm_);
        return 1;
    }

    ret = ceph_mount(cm_, "/hdfs_root");
    if (ret < 0)
    {
        log(LogLevel::INFO, "ceph_mount failed");
        ceph_shutdown(cm_);
        return 1;
    }

    inited_ = true;
    log(LogLevel::INFO, "CephFsAdapter init success, root_prefix=%s",
        cfg_.root_prefix.c_str());
    return 0;
}

int CephFsAdapter::shutdown() {
    if (!inited_) return 0;
    int rc = ceph_unmount(cm_);
    if (rc < 0) {
        log(LogLevel::ERROR, "ceph_unmount failed rc=%d", rc);
    }
    ceph_release(cm_);
    cm_ = nullptr;
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
