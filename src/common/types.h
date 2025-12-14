#pragma once

#include <cstdint>
#include <string>

namespace hcg { // hdfs-ceph-gateway

using FileId = std::uint64_t;
using BlockIndex = std::uint64_t;   // 语义上标识块序号，等同于 block_index 字段, 某个文件内部的块序号，从 0 开始
using BlockId = std::int64_t;       // 暴露给 HDFS 的逻辑 BlockId（保持非负）
                                    // 是 HDFS 中每个数据块的 全局唯一 ID。由 NameNode 在创建 Block 时分配
struct BlockInfo {
    std::uint64_t block_index;
    std::uint64_t offset;
    std::uint64_t length;
    std::string block_id;
    std::string block_pool_id;
    uint64_t generation_stamp;
    uint64_t num_bytes;
    std::string local_path;
    uint32_t checksum;
};

// 用多少 bit 存 block_index（剩余 bit 给 file_id）
// 2^24 = 16,777,216 blocks；128MB/block 时，单文件上限 ~ 2PB，足够
inline constexpr std::uint32_t kBlockIndexBits = 24;
inline constexpr std::uint64_t kBlockIndexMask = (1ULL << kBlockIndexBits) - 1;

// 将 (file_id, block_index) 编码为逻辑 BlockId
inline BlockId make_block_id(FileId fid, BlockIndex index) {
    std::uint64_t raw = (fid << kBlockIndexBits) | (index & kBlockIndexMask);
    // 最高位清 0，保证是非负的 int64_t
    raw &= 0x7FFFFFFFFFFFFFFFULL;
    return static_cast<BlockId>(raw);
}

// 从 BlockId 反解出 file_id
inline FileId blockid_to_fileid(BlockId bid) {
    std::uint64_t raw = static_cast<std::uint64_t>(bid);
    return raw >> kBlockIndexBits;
}

// 从 BlockId 反解出 block_index
inline BlockIndex blockid_to_index(BlockId bid) {
    std::uint64_t raw = static_cast<std::uint64_t>(bid);
    return (raw & kBlockIndexMask);
}

} // namespace hcg
