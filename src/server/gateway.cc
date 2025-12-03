#include "server/gateway.h"
#include "common/logging.h"
#include "server/gateway.h"
#include "rpc/internal/internal_gateway_service_dummy.h"
#include "common/logging.h"

namespace hcg {

HdfsCephGateway::HdfsCephGateway(const GatewayConfig& cfg)
    : cfg_(cfg) {}

HdfsCephGateway::~HdfsCephGateway() {
    if (running_) {
        stop();
    }
}

int HdfsCephGateway::init() {
    ceph_ = std::make_unique<CephFsAdapter>();
    int rc = ceph_->init(cfg_.ceph);
    if (rc != 0) {
        log(LogLevel::ERROR, "Failed to init CephFsAdapter rc=%d", rc);
        return rc;
    }

    meta_ = std::make_unique<XattrMetadataStore>(ceph_.get());
    ns_ = std::make_unique<NamespaceService>();
    bm_ = std::make_unique<BlockManager>(meta_.get(), cfg_.datanode_endpoint);
    lm_ = std::make_unique<LeaseManager>();

    nn_server_ = std::make_unique<NameNodeRpcServer>(
        ns_.get(), bm_.get(), lm_.get());
    dn_server_ = std::make_unique<DataNodeServer>(
        bm_.get(), ceph_.get());


        
    // internal RPC 部分
    internal_service_ = std::make_shared<DummyInternalGatewayService>();
    internal_rpc_server_ = std::make_unique<internal_rpc::InternalRpcServer>(internal_service_);

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

    running_ = true;
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

    // 原来的 nn_server_ / dn_server_ stop...
    if (ceph_) {
        ceph_->shutdown();
    }

    running_ = false;
    log(LogLevel::INFO, "HdfsCephGateway stopped");
    return 0;
}

} // namespace hcg
