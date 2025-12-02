#include "server/gateway.h"
#include "common/logging.h"
#include <unistd.h>

int main(int argc, char** argv) {
    (void)argc; (void)argv;

    hcg::GatewayConfig cfg;
    cfg.nn_bind = "0.0.0.0";
    cfg.nn_port = 9000;
    cfg.dn_bind = "0.0.0.0";
    cfg.dn_port = 50010;
    cfg.datanode_endpoint = "127.0.0.1:50010";

    cfg.ceph.mon_hosts = "10.0.0.1,10.0.0.2";
    cfg.ceph.user = "client.hdfs";
    cfg.ceph.keyring_path = "/etc/ceph/ceph.client.hdfs.keyring";
    cfg.ceph.root_prefix = "/hdfs_root";

    hcg::HdfsCephGateway gateway(cfg);
    int rc = gateway.init();
    if (rc != 0) {
        hcg::log(hcg::LogLevel::ERROR, "gateway init failed rc=%d", rc);
        return rc;
    }

    rc = gateway.start();
    if (rc != 0) {
        hcg::log(hcg::LogLevel::ERROR, "gateway start failed rc=%d", rc);
        return rc;
    }

    // 第一版简单阻塞在这里, 可以后面加 signal 处理
    while (true) {
        // TODO: sleep & wait for signal
        ::sleep(10);
    }

    gateway.stop();
    return 0;
}
