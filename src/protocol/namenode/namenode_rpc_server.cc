#include "protocol/namenode/namenode_rpc_server.h"
#include "common/logging.h"

namespace hcg {

NameNodeRpcServer::NameNodeRpcServer(INamespaceService* ns,
                                     IBlockManager* bm,
                                     ILeaseManager* lm)
    : ns_(ns),
      bm_(bm),
      lm_(lm) {}

int NameNodeRpcServer::start(const std::string& bind_addr, std::uint16_t port) {
    (void)bind_addr; (void)port;
    // TODO: 启动 RPC 监听
    log(LogLevel::INFO, "NameNodeRpcServer start at %s:%u",
        bind_addr.c_str(), port);
    return 0;
}

int NameNodeRpcServer::stop() {
    // TODO: 停止 RPC
    log(LogLevel::INFO, "NameNodeRpcServer stop");
    return 0;
}

void NameNodeRpcServer::handle_create(/* Request */) {
    // TODO: 解析 request -> path, perms, client_id
    // 调 ns_->create_file(...) + lm_->add_lease(...)
}

void NameNodeRpcServer::handle_get_block_locations(/* Request */) {
    // TODO: 调 bm_->get_block_locations(...)
}

void NameNodeRpcServer::handle_add_block(/* Request */) {
    // TODO: 调 bm_->allocate_block(...)
}

void NameNodeRpcServer::handle_complete(/* Request */) {
    // TODO: 调 lm_->remove_lease(...) + bm_->finalize_file_blocks(...)
}

} // namespace hcg
