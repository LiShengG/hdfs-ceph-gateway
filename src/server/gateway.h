#pragma once

#include <memory>
#include <string>

#include "fsal/cephfs/cephfs_adapter.h"
#include "meta/xattr_metadata_store.h"
#include "core/namespace/namespace_service.h"
#include "core/block/block_manager.h"
#include "core/lease/lease_manager.h"
#include "protocol/namenode/namenode_rpc_server.h"
#include "protocol/datanode/datanode_server.h"
#include "protocol/namenode/hdfs_namenode_service.h"
#include "protocol/namenode/namenode_rpc_server.h"
#include "rpc/internal/internal_rpc_server.h"
#include "rpc/internal/internal_gateway_service.h"

namespace hcg {

struct GatewayConfig {
    std::string nn_bind {"0.0.0.0"};
    std::uint16_t nn_port {9000};

    std::string dn_bind {"0.0.0.0"};
    std::uint16_t dn_port {50010};

    std::string datanode_endpoint {"127.0.0.1:50010"};

    CephFsConfig ceph;
};

class HdfsCephGateway {
public:
    explicit HdfsCephGateway(const GatewayConfig& cfg);
    ~HdfsCephGateway();

    int init();
    int start();
    int stop();

private:
    GatewayConfig cfg_;

    std::shared_ptr<CephFsAdapter> ceph_;
    std::unique_ptr<XattrMetadataStore> meta_;
    std::unique_ptr<NamespaceService> ns_;
    std::unique_ptr<BlockManager> bm_;
    std::unique_ptr<LeaseManager> lm_;

    // std::unique_ptr<NameNodeRpcServer> nn_server_;
    std::unique_ptr<DataNodeServer> dn_server_;
    std::unique_ptr<internal_rpc::InternalRpcServer> internal_rpc_server_;
    std::shared_ptr<IInternalGatewayService> internal_service_;

    std::unique_ptr<NameRpcServer> hdfs_rpc_;
    std::shared_ptr<IHdfsNamenodeService> hdfs_nn_svc_;
    

    bool running_ {false};
};

} // namespace hcg
