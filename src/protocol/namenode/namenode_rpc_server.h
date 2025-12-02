#pragma once

#include <string>
#include "core/namespace/namespace_service.h"
#include "core/block/block_manager.h"
#include "core/lease/lease_manager.h"

namespace hcg {

class NameNodeRpcServer {
public:
    NameNodeRpcServer(INamespaceService* ns,
                      IBlockManager* bm,
                      ILeaseManager* lm);

    int start(const std::string& bind_addr, std::uint16_t port);
    int stop();

private:
    INamespaceService* ns_;
    IBlockManager* bm_;
    ILeaseManager* lm_;

    // TODO: RPC server 实现细节，比如监听 socket, thread pool

    // 示例: create() 的 handler
    void handle_create(/* Request */);
    void handle_get_block_locations(/* Request */);
    void handle_add_block(/* Request */);
    void handle_complete(/* Request */);
};

} // namespace hcg
