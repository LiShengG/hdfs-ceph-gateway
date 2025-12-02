#include "protocol/datanode/datanode_server.h"
#include "common/logging.h"

namespace hcg {

DataNodeServer::DataNodeServer(IBlockManager* bm, ICephFsAdapter* ceph)
    : bm_(bm), ceph_(ceph) {}

int DataNodeServer::start(const std::string& bind_addr, std::uint16_t port) {
    (void)bind_addr; (void)port;
    // TODO: 启动 TCP 监听
    log(LogLevel::INFO, "DataNodeServer start at %s:%u",
        bind_addr.c_str(), port);
    return 0;
}

int DataNodeServer::stop() {
    // TODO: 停止
    log(LogLevel::INFO, "DataNodeServer stop");
    return 0;
}

void DataNodeServer::handle_write_block(/* header + stream */) {
    // TODO: blockId -> path, offset [通过 BlockManager],
    // 再调用 ceph_->pwrite(...)
}

void DataNodeServer::handle_read_block(/* header + stream */) {
    // TODO: 对应地 p_read -> send packets
}

} // namespace hcg
