#include "server/gateway.h"
#include "common/logging.h"
#include "server/gateway.h"
#include "rpc/internal/internal_gateway_service_dummy.h"
#include "common/logging.h"
#include "rpc/internal/internal_gateway_service_impl.h"
#include "protocol/namenode/hdfs_namenode_service_impl.h"
#include "protocol/datanode/hdfs_datanode_service_impl.h"
#include "meta/xattr_metadata_store.h"

namespace hcg {

HdfsCephGateway::HdfsCephGateway(const GatewayConfig& cfg)
    : cfg_(cfg) {}

HdfsCephGateway::~HdfsCephGateway() {
    if (running_) {
        stop();
    }
}

int HdfsCephGateway::init() {
    ceph_ = std::make_shared<CephFsAdapter>();
    int rc = ceph_->init(cfg_.ceph);
    if (rc != 0) {
        log(LogLevel::ERROR, "Failed to init CephFsAdapter rc=%d", rc);
        return rc;
    }

    auto meta_store = std::make_shared<XattrMetadataStore>(ceph_);
    auto block_mgr = std::make_shared<BlockManager>(meta_store, cfg_.datanode_endpoint);

    // 创建 InternalGatewayServiceImpl
    internal_service_ = std::make_shared<InternalGatewayServiceImpl>(ceph_, meta_store, block_mgr);
    internal_rpc_server_ =
        std::make_unique<internal_rpc::InternalRpcServer>(internal_service_);

    // Stage 1: HDFS NN 服务实现（基于 internal_service_）
    hdfs_nn_svc_ = std::make_shared<HdfsNamenodeServiceImpl>(internal_service_);
    nn_server = std::make_unique<NameRpcServer>(hdfs_nn_svc_);

    hdfs_dn_svc_ = std::make_shared<HdfsDatanodeServiceImpl>(internal_service_);
    dn_server = std::make_unique<DataRpcServer>(hdfs_dn_svc_);

    return 0;
}

int HdfsCephGateway::start() {
    // int rc = nn_server_->start(cfg_.nn_bind, cfg_.nn_port);
    // if (rc != 0) return rc;

    // rc = dn_server_->start(cfg_.dn_bind, cfg_.dn_port);
    // if (rc != 0) return rc;

    // running_ = true;
    // log(LogLevel::INFO, "HdfsCephGateway started");
    // return 0;

    int rc = internal_rpc_server_->start("0.0.0.0", 19000);
    if (rc != 0) {
        log(LogLevel::ERROR, "start internal RPC server failed rc=%d", rc);
        return rc;
    }
    rc = nn_server->start(cfg_.nn_bind, cfg_.nn_port); 
    if (rc != 0) {
        log(LogLevel::ERROR, "start nn_server RPC server failed rc=%d", rc);
        return rc;
    }
    running_ = true;

    rc = dn_server->start(cfg_.dn_bind, cfg_.dn_port);
    if (rc != 0) {
        log(LogLevel::ERROR, "start dn_server RPC server failed rc=%d", rc);
        return rc;
    }

    log(LogLevel::INFO, "HdfsCephGateway started (internal rpc only)");
    return 0;
}

int HdfsCephGateway::stop() {
    // if (!running_) return 0;

    // dn_server_->stop();
    // nn_server_->stop();
    // ceph_->shutdown();
    // running_ = false;
    // log(LogLevel::INFO, "HdfsCephGateway stopped");
    // return 0;
    if (!running_) return 0;

    if (internal_rpc_server_) {
        internal_rpc_server_->stop();
    }

    if (nn_server) {
        nn_server->stop();
    }

    if (dn_server) {
        dn_server->stop();
    }

    if (ceph_) {
        ceph_->shutdown();
    }

    log(LogLevel::INFO, "HdfsCephGateway stopped");
    return 0;
}

} // namespace hcg
