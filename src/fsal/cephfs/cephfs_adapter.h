#pragma once

#include <string>
#include <vector>
#include <cephfs/libcephfs.h> 

namespace hcg {

struct CephStat {
    bool is_dir {false};
    std::uint64_t size {0};
    std::uint32_t mode {0};
    std::uint64_t mtime_sec {0};
    std::uint64_t atime_sec {0};
};
struct CephFsConfig {
    std::string mon_hosts;
    std::string user;
    std::string keyring_path;
    std::string root_prefix; // 例如 "/hdfs_root"
};

class ICephFsAdapter {
public:
    virtual ~ICephFsAdapter() = default;

    virtual int init(const CephFsConfig& cfg) = 0;
    virtual int shutdown() = 0;

    virtual int create(const std::string& path, int mode, bool overwrite) = 0;
    virtual int open(const std::string& path, int flags, int mode, int& out_fd) = 0;
    virtual int close(int fd) = 0;

    virtual long pread(int fd, void* buf, size_t len, long offset) = 0;
    virtual long pwrite(int fd, const void* buf, size_t len, long offset) = 0;

    virtual int mkdir(const std::string& path, int mode, bool recursive) = 0;
    virtual int readdir(const std::string& path,
                        std::vector<std::string>& names) = 0;
    virtual int unlink(const std::string& path) = 0;
    virtual int rename(const std::string& src, const std::string& dst) = 0;

    virtual int get_xattr(const std::string& path,
                          const std::string& name,
                          std::string& value) = 0;
    virtual int set_xattr(const std::string& path,
                          const std::string& name,
                          const std::string& value) = 0;
};

class CephFsAdapter : public ICephFsAdapter {
public:
    CephFsAdapter();
    ~CephFsAdapter() override;

    int init(const CephFsConfig& cfg) override;
    int shutdown() override;

    int create(const std::string& path, int mode, bool overwrite) override;
    int open(const std::string& path, int flags, int mode, int& out_fd) override;
    int close(int fd) override;

    long pread(int fd, void* buf, size_t len, long offset) override;
    long pwrite(int fd, const void* buf, size_t len, long offset) override;

    int mkdir(const std::string& path, int mode, bool recursive) override;
    int readdir(const std::string& path,
                std::vector<std::string>& names) override;
    int unlink(const std::string& path) override;
    int rename(const std::string& src, const std::string& dst) override;

    int get_xattr(const std::string& path,
                  const std::string& name,
                  std::string& value) override;
    int set_xattr(const std::string& path,
                  const std::string& name,
                  const std::string& value) override;

private:
    CephFsConfig cfg_;
    bool inited_ {false};
    ceph_mount_info* cm_ = nullptr;
    std::string mount_root_;     // 如果用了 mount_root_ 也要声明
    std::string full_path(const std::string& path);

public:
    int stat(const std::string& path, CephStat& st);
    // TODO: 这里以后加 libcephfs 相关句柄，如 struct ceph_mount_info* mount_;
};

} // namespace hcg
