#pragma once

#include <string>
#include "core/block/block_manager.h"
#include "fsal/cephfs/cephfs_adapter.h"

namespace hcg {

class DataNodeServer {
public:
    DataNodeServer(IBlockManager* bm, ICephFsAdapter* ceph);

    int start(const std::string& bind_addr, std::uint16_t port);
    int stop();

private:
    IBlockManager* bm_;
    ICephFsAdapter* ceph_;

    // TODO: DataTransfer 协议收包/发包

    void handle_write_block(/* header + stream */);
    void handle_read_block(/* header + stream */);
};

} // namespace hcg
