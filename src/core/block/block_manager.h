#pragma once

#include <string>
#include <vector>
#include "common/types.h"
#include "common/status.h"
#include "meta/metadata_store.h"

namespace hcg {

struct BlockLocation {
    BlockInfo block;
    std::vector<std::string> datanodes; // "host:port"
};

class IBlockManager {
public:
    virtual ~IBlockManager() = default;

    virtual Status get_block_locations(const std::string& path,
                                       std::uint64_t start,
                                       std::uint64_t length,
                                       std::vector<BlockLocation>& out) = 0;

    virtual Status allocate_block(const std::string& path,
                                  BlockInfo& out_block,
                                  BlockLocation& out_location) = 0;

    virtual Status finalize_file_blocks(const std::string& path) = 0;
};

class BlockManager : public IBlockManager {
public:
    BlockManager(IMetadataStore* meta, std::string datanode_endpoint);

    Status get_block_locations(const std::string& path,
                               std::uint64_t start,
                               std::uint64_t length,
                               std::vector<BlockLocation>& out) override;

    Status allocate_block(const std::string& path,
                          BlockInfo& out_block,
                          BlockLocation& out_location) override;

    Status finalize_file_blocks(const std::string& path) override;

private:
    IMetadataStore* meta_;
    std::string datanode_endpoint_;
};

} // namespace hcg
