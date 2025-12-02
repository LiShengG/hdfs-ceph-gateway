#include "core/block/block_manager.h"
#include "common/logging.h"

namespace hcg {

BlockManager::BlockManager(IMetadataStore* meta, std::string datanode_endpoint)
    : meta_(meta),
      datanode_endpoint_(std::move(datanode_endpoint)) {}

Status BlockManager::get_block_locations(const std::string& path,
                                         std::uint64_t start,
                                         std::uint64_t length,
                                         std::vector<BlockLocation>& out) {
    (void)start; (void)length;
    out.clear();

    FileBlockMeta meta;
    auto st = meta_->load_file_block_meta(path, meta);
    if (!st.ok()) return st;

    // 简版: 返回所有 block, datanode 固定为本节点
    for (auto& b : meta.blocks) {
        BlockLocation loc;
        loc.block = b;
        loc.datanodes.push_back(datanode_endpoint_);
        out.push_back(std::move(loc));
    }
    return Status::OK();
}

Status BlockManager::allocate_block(const std::string& path,
                                    BlockInfo& out_block,
                                    BlockLocation& out_location) {
    FileBlockMeta meta;
    auto st = meta_->load_file_block_meta(path, meta);
    if (!st.ok()) return st;

    std::uint64_t index = meta.blocks.size();
    out_block.block_index = index;
    out_block.offset = index * meta.block_size;
    out_block.length = 0;

    meta.blocks.push_back(out_block);
    st = meta_->save_file_block_meta(path, meta);
    if (!st.ok()) return st;

    out_location.block = out_block;
    out_location.datanodes = { datanode_endpoint_ };

    return Status::OK();
}

Status BlockManager::finalize_file_blocks(const std::string& path) {
    (void)path;
    // 第一版可以不做任何事
    return Status::OK();
}

} // namespace hcg
