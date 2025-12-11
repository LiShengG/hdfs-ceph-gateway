#pragma once
#include "common/types.h"
#include "meta/metadata_store.h"

#include <mutex>
#include <unordered_map>

namespace hcg {

struct BlockLocation {
    BlockInfo block;
    std::vector<std::string> datanodes; // "host:port"
};

// 运行时 Block 映射：只存在于内存，用于 DN 根据 blockId 找到 Ceph 文件位置
struct RuntimeBlockLocation {
    std::string  path;        // CephFS 路径（用于 ICephFsAdapter::open）
    FileId       file_id;
    BlockInfo    block;
    std::uint64_t block_size; // 对应 meta.block_size
};

class IBlockManager {
public:
    virtual ~IBlockManager() = default;

    // NN: 查询 [start, start+length) 范围内的 Block 列表
    virtual Status get_block_locations(const std::string& path,
                                       std::uint64_t start,
                                       std::uint64_t length,
                                       std::vector<BlockLocation>& out) = 0;

    // NN: 为指定文件分配下一个 Block（通常用于 create/addBlock）
    virtual Status allocate_block(const std::string& path,
                                  BlockInfo& out_block,
                                  BlockLocation& out_location) = 0;

    // NN: 在 complete 时，依据 CephFS 实际文件长度，回填 FileBlockMeta.blocks[i].length & meta.length
    virtual Status finalize_file_blocks(const std::string& path) = 0;

    // DN: 根据逻辑 BlockId 反查文件路径和 BlockInfo（Stage 2 新增）
    virtual Status resolve_block(BlockId block_id,
                                 std::string& out_path,
                                 BlockInfo& out_block,
                                 std::uint64_t& out_block_size) = 0;
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

    Status resolve_block(BlockId block_id,
                         std::string& out_path,
                         BlockInfo& out_block,
                         std::uint64_t& out_block_size) override;

private:
    IMetadataStore* meta_;
    std::string datanode_endpoint_;

    // 运行时：按 path 缓存 FileBlockMeta（blocks 只存在这里，不写入 xattr）
    std::mutex mu_;
    std::unordered_map<std::string, FileBlockMeta> runtime_files_;

    // 运行时：BlockId -> 位置，用于 DN 读写时根据 blockId 查路径和偏移
    struct RuntimeBlockLocation {
        std::string  path;
        FileId       file_id;
        BlockInfo    block;
        std::uint64_t block_size;
    };
    std::unordered_map<BlockId, RuntimeBlockLocation> runtime_blocks_;

    // 在持锁前提下获取或加载 meta，保证 file_id 有效，并放入 runtime_files_
    Status get_or_load_meta_locked(const std::string& path, FileBlockMeta*& out_meta);
};

} // namespace hcg
