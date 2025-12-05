// fsal/cephfs/cephfs_adapter.cc
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

    // int rc = ceph_create(&cm_, nullptr);
    int ret = ceph_create(&cm_, cfg_.user.c_str());
    if (ret < 0) {
        log(LogLevel::ERROR, "ceph_create failed rc=%d (%s)", ret, strerror(-ret));
        return 1;
    }

    ceph_conf_set(cm_, "mon_host", cfg_.mon_hosts.c_str());
    ceph_conf_set(cm_, "keyring", cfg_.keyring_path.c_str());

    ceph_conf_read_file(cm_, NULL);

    ceph_conf_set(cm_, "auth_supported", "cephx");
    ceph_conf_set(cm_, "ms_auth_supported", "cephx");

    // 开启详细日志
    // ceph_conf_set(cm_, "log_to_stderr", "true");
    // ceph_conf_set(cm_, "debug_client", "20");
    // ceph_conf_set(cm_, "debug_auth", "20");
    // ceph_conf_set(cm_, "debug_ms", "20");

    ret = ceph_init(cm_);
    if (ret < 0)
    {
        log(LogLevel::INFO, "ceph_init failed");
        ceph_shutdown(cm_);
        return 1;
    }

    ret = ceph_mount(cm_, "/");
    if (ret < 0)
    {
        log(LogLevel::INFO, "ceph_mount failed");
        ceph_shutdown(cm_);
        return 1;
    }

    std::string root_prefix_ = cfg.root_prefix.empty() ? "/" : cfg.root_prefix;

    // 确保 root_prefix 本身存在且是目录
    if (root_prefix_ != "/") {
        int rc = mkdir(root_prefix_, 0755, true); // 这里 recursive=true
        if (rc < 0) {
            log(LogLevel::ERROR, "failed to ensure root_prefix=%s rc=%d",
                root_prefix_.c_str(), rc);
            return rc;
        }
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
    std::string p = full_path(path);
    int flags = O_CREAT | O_WRONLY;
    if (overwrite) {
        flags |= O_TRUNC;
    } else {
        flags |= O_EXCL;
    }

    int fd = ceph_open(cm_, p.c_str(), flags, mode);
    if (fd < 0) {
        log(LogLevel::ERROR, "ceph_open(create) path=%s rc=%d (%s)",
            p.c_str(), fd, strerror(-fd));
        return fd;
    }
    ceph_close(cm_, fd);
    return 0;
}

int CephFsAdapter::open(const std::string& path, int flags, int mode, int& out_fd) {
    std::string p = full_path(path);
    int fd = ceph_open(cm_, p.c_str(), flags, mode);
    if (fd < 0) {
        log(LogLevel::ERROR, "ceph_open path=%s rc=%d (%s)",
            p.c_str(), fd, strerror(-fd));
        return fd;
    }
    out_fd = fd;
    return 0;
}

int CephFsAdapter::close(int fd) {
    int rc = ceph_close(cm_, fd);
    if (rc < 0) {
        log(LogLevel::ERROR, "ceph_close fd=%d rc=%d (%s)",
            fd, rc, strerror(-rc));
        return rc;
    }
    return 0;
}

long CephFsAdapter::pread(int fd, void* buf, size_t len, long offset) {
    ssize_t ret = ceph_read(cm_, fd, static_cast<char*>(buf), len, offset);
    if (ret < 0) {
        log(LogLevel::ERROR, "ceph_read fd=%d rc=%zd (%s)",
            fd, ret, strerror(-ret));
        return ret;
    }
    return ret;
}

long CephFsAdapter::pwrite(int fd, const void* buf, size_t len, long offset) {
    ssize_t ret = ceph_write(cm_, fd, static_cast<const char*>(buf), len, offset);
    if (ret < 0) {
        log(LogLevel::ERROR, "ceph_write fd=%d rc=%zd (%s)",
            fd, ret, strerror(-ret));
        return ret;
    }
    return ret;
}

int CephFsAdapter::mkdir(const std::string& path, int mode, bool recursive) {
    std::string p = full_path(path);
    int rc = 0;

    if (!recursive) {
        rc = ceph_mkdir(cm_, p.c_str(), mode);
    } else {
        // 简单递归 mkdir -p 实现
        std::string cur;
        if (cfg_.root_prefix.size()) {
            cur = cfg_.root_prefix;
        }
        // 假设 path 是绝对路径 /a/b/c
        size_t start = 0;
        while (start < path.size()) {
            size_t pos = path.find('/', start);
            if (pos == std::string::npos) pos = path.size();
            std::string part = path.substr(start, pos - start);
            if (!part.empty()) {
                cur += "/" + part;
                rc = ceph_mkdir(cm_, cur.c_str(), mode);
                if (rc < 0 && rc != -EEXIST) {
                    log(LogLevel::ERROR, "ceph_mkdir path=%s rc=%d (%s)",
                        cur.c_str(), rc, strerror(-rc));
                    return rc;
                }
            }
            start = pos + 1;
        }
    }

    if (rc < 0 && rc != -EEXIST) {
        log(LogLevel::ERROR, "ceph_mkdir path=%s rc=%d (%s)",
            p.c_str(), rc, strerror(-rc));
        return rc;
    }
    return 0;
}

int CephFsAdapter::readdir(const std::string& path,
                           std::vector<std::string>& names) {
    std::string p = full_path(path);

    struct ceph_dir_result* dirp = nullptr;
    int rc = ceph_opendir(cm_, p.c_str(), &dirp);
    if (rc < 0) {
        // ceph_* 系列一般返回 -errno
        log(LogLevel::ERROR, "ceph_opendir path=%s rc=%d (%s)",
            p.c_str(), rc, std::strerror(-rc));
        return rc;
    }

    names.clear();

    while (true) {
        struct dirent* de = ceph_readdir(cm_, dirp);
        if (!de) {
            // NULL 表示读到目录末尾
            break;
        }

        const char* name = de->d_name;
        if (std::strcmp(name, ".") == 0 || std::strcmp(name, "..") == 0)
            continue;

        names.emplace_back(name);
    }

    rc = ceph_closedir(cm_, dirp);
    if (rc < 0) {
        log(LogLevel::ERROR, "ceph_closedir path=%s rc=%d (%s)",
            p.c_str(), rc, std::strerror(-rc));
        return rc;
    }

    return 0;
}


int CephFsAdapter::unlink(const std::string& path) {
    std::string p = full_path(path);
    int rc = ceph_unlink(cm_, p.c_str());
    if (rc < 0) {
        log(LogLevel::ERROR, "ceph_unlink path=%s rc=%d (%s)",
            p.c_str(), rc, strerror(-rc));
        return rc;
    }
    return 0;
}

int CephFsAdapter::rename(const std::string& src, const std::string& dst) {
    std::string s = full_path(src);
    std::string d = full_path(dst);
    int rc = ceph_rename(cm_, s.c_str(), d.c_str());
    if (rc < 0) {
        log(LogLevel::ERROR, "ceph_rename %s -> %s rc=%d (%s)",
            s.c_str(), d.c_str(), rc, strerror(-rc));
        return rc;
    }
    return 0;
}


int CephFsAdapter::get_xattr(const std::string& path,
                             const std::string& name,
                             std::string& value) {
    std::string p = full_path(path);

    // Step 1: 获取 xattr 的大小
    ssize_t sz = ceph_getxattr(cm_, p.c_str(), name.c_str(), nullptr, 0);
    if (sz < 0) {
        if (sz == -ENODATA) {
            return static_cast<int>(sz); // 属性不存在，由调用方处理
        }
        log(LogLevel::ERROR, "ceph_getxattr(size) path=%s name=%s rc=%zd (%s)",
            p.c_str(), name.c_str(), sz, strerror(-static_cast<int>(sz)));
        return static_cast<int>(sz);
    }

    // Step 2: 预分配 buffer 并读取值
    value.resize(static_cast<size_t>(sz));
    ssize_t sz2 = ceph_getxattr(cm_, p.c_str(), name.c_str(),
                                value.data(), value.size());

    if (sz2 < 0) {
        log(LogLevel::ERROR, "ceph_getxattr(read) path=%s name=%s rc=%zd (%s)",
            p.c_str(), name.c_str(), sz2, strerror(-static_cast<int>(sz2)));
        return static_cast<int>(sz2);
    }

    if (static_cast<size_t>(sz2) != value.size()) {
        value.resize(static_cast<size_t>(sz2)); // 以防万一
    }

    return 0;
}

int CephFsAdapter::set_xattr(const std::string& path,
                             const std::string& name,
                             const std::string& value) {
    std::string p = full_path(path);
    int rc = ceph_setxattr(cm_, p.c_str(), name.c_str(),
                           value.data(), value.size(), 0);
    if (rc < 0) {
        log(LogLevel::ERROR, "ceph_setxattr path=%s name=%s rc=%d (%s)",
            p.c_str(), name.c_str(), rc, strerror(-rc));
        return rc;
    }
    return 0;
}

int CephFsAdapter::stat(const std::string& path, CephStat& st) {
    std::string p = full_path(path);

    struct stat buf {};
    // libcephfs: ceph_statx 或 ceph_lstat
    int rc = ceph_stat(cm_, p.c_str(), &buf);
    if (rc < 0) {
        log(LogLevel::ERROR, "ceph_stat path=%s rc=%d (%s)",
            p.c_str(), rc, strerror(-rc));
        return rc;
    }

    st.is_dir = S_ISDIR(buf.st_mode);
    st.size = buf.st_size;
    st.mode = buf.st_mode & 0777;
    st.mtime_sec = buf.st_mtime;
    st.atime_sec = buf.st_atime;
    return 0;
}


}