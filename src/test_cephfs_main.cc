#include "fsal/cephfs/cephfs_adapter.h"
#include "common/logging.h"

#include <cstring>

int main(int argc, char** argv) {
    (void)argc; (void)argv;

    hcg::CephFsConfig cfg;
    // cfg.mon_hosts = "10.20.40.85";  // 换成你的 mon 地址
    cfg.mon_hosts = "v2:10.20.40.85:3300";
    cfg.user = "hdfs";             // 对应 ceph.client.hdfs
    cfg.keyring_path = "/etc/ceph/ceph.client.hdfs.keyring";
    cfg.root_prefix = "/";

    hcg::CephFsAdapter ceph;
    int rc = ceph.init(cfg);
    if (rc != 0) {
        hcg::log(hcg::LogLevel::ERROR, "CephFsAdapter init failed rc=%d", rc);
        return rc;
    }

    // mkdir
    rc = ceph.mkdir("/test_dir", 0755, true);
    hcg::log(hcg::LogLevel::INFO, "mkdir /test_dir rc=%d", rc);

    // create + write
    rc = ceph.create("/test_dir/hello.txt", 0644, true);
    hcg::log(hcg::LogLevel::INFO, "create /test_dir/hello.txt rc=%d", rc);

    int fd = -1;
    rc = ceph.open("/test_dir/hello.txt", O_WRONLY, 0644, fd);
    if (rc != 0) {
        hcg::log(hcg::LogLevel::ERROR, "open for write rc=%d", rc);
        return rc;
    }
    const char* msg = "hello from hdfs-ceph-gateway\n";
    long w = ceph.pwrite(fd, msg, std::strlen(msg), 0);
    hcg::log(hcg::LogLevel::INFO, "write bytes=%ld", w);
    ceph.close(fd);

    // read back
    rc = ceph.open("/test_dir/hello.txt", O_RDONLY, 0, fd);
    if (rc != 0) {
        hcg::log(hcg::LogLevel::ERROR, "open for read rc=%d", rc);
        return rc;
    }
    char buf[4096] = {0};
    long r = ceph.pread(fd, buf, sizeof(buf) - 1, 0);
    hcg::log(hcg::LogLevel::INFO, "read bytes=%ld content=%s", r, buf);
    ceph.close(fd);

    // xattr
    std::string meta = "{\"version\":1,\"block_size\":134217728}";
    rc = ceph.set_xattr("/test_dir/hello.txt", "user.hdfs.meta", meta);
    hcg::log(hcg::LogLevel::INFO, "set_xattr rc=%d", rc);

    std::string meta_out;
    rc = ceph.get_xattr("/test_dir/hello.txt", "user.hdfs.meta", meta_out);
    hcg::log(hcg::LogLevel::INFO, "get_xattr rc=%d value=%s", rc, meta_out.c_str());

    ceph.shutdown();
    return 0;
}
