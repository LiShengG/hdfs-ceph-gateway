#include "core/block/block_manager.h"
#include "common/logging.h"
#include "common/types.h"           // FileId, BlockId, make_block_id
#include "meta/metadata_store.h"    // FileBlockMeta, IMetadataStore
#include <algorithm>
#include <limits>

namespace hcg {

BlockManager::BlockManager(std::shared_ptr<IMetadataStore> meta, std::string datanode_endpoint)
    : meta_(meta),
      datanode_endpoint_(std::move(datanode_endpoint)) {
}

// 在持锁条件下，优先从 runtime_files_ 中取 FileBlockMeta，
// 没有的话通过 metadata_store 加载，并确保 file_id 有值（必要时 alloc_file_id + 回写 xattr）
Status BlockManager::get_or_load_meta_locked(const std::string& path,
                                             FileBlockMeta*& out_meta) {
    auto it = runtime_files_.find(path);
    if (it != runtime_files_.end()) {
        out_meta = &it->second;
        return Status::OK();
    }

    FileBlockMeta m;
    Status st = meta_->load_file_block_meta(path, m);
    if (st != Status::OK()) {
        log(LogLevel::ERROR,
            "BlockManager::get_or_load_meta_locked: load_file_block_meta failed path=%s",
            path.c_str());
        return st;
    }

    // blocks 在 Xattr 中不持久化，这里保持空；对于已完成文件，
    // 读路径会通过 length+block_size 计算 block 布局；
    // 对于新文件/写入中的文件，blocks 在本进程内逐步填充。
    m.blocks.clear();

    auto [ins, inserted] = runtime_files_.emplace(path, std::move(m));
    out_meta = &ins->second;
    return Status::OK();
}

/**
 * get_block_locations
 *
 * 语义：
 *  - 输入：path + 读取区间 [start, start+length)
 *  - 输出：覆盖该区间的逻辑块列表（BlockLocation）
 *  - 位置信息由 FileBlockMeta 的 {block_size, length} 推导：
 *      numBlocks = ceil(length / block_size)
 *      每个块：
 *          block_index = i
 *          offset      = i * block_size
 *          length      = full or partial (最后一个块)
 *
 * 注意：
 *  - Xattr 不存 blocks 数组，这里运行时按需计算。
 *  - 同时填充 runtime_blocks_，供后续 resolve_block(blockId) 使用。
 */
Status BlockManager::get_block_locations(const std::string& path,
                                         std::uint64_t start,
                                         std::uint64_t length,
                                         std::vector<BlockLocation>& out) {
    std::lock_guard<std::mutex> lock(mu_);

    FileBlockMeta* meta = nullptr;
    Status st = get_or_load_meta_locked(path, meta);
    if (st != Status::OK()) {
        return st;
    }

    out.clear();

    // 文件总长度：优先使用 meta->length；
    // 若 length==0 而本进程内已经有 blocks（写入中场景），则尝试用 blocks 推导；
    // Stage 2 简化：主要支持 complete 之后的读取，即 meta->length > 0。
    std::uint64_t total_len = meta->length;
    if (total_len == 0) {
        if (!meta->blocks.empty()) {
            const auto& last = meta->blocks.back();
            total_len = last.offset + last.length;
        } else {
            // 空文件，没有任何 block
            return Status::OK();
        }
    }

    if (start >= total_len || length == 0) {
        return Status::OK();
    }

    std::uint64_t end = start + length;
    if (end > total_len) {
        end = total_len;
    }

    const std::uint64_t block_size = meta->block_size;
    if (block_size == 0) {
        log(LogLevel::ERROR,
            "BlockManager::get_block_locations: invalid block_size=0 path=%s",
            path.c_str());
        return Status::Error(-1, "invalid block_size");
    }

    // 总 block 数
    std::uint64_t num_blocks =
        (total_len + block_size - 1) / block_size; // 向上取整

    // 需要覆盖 [start, end) 的块区间
    std::uint64_t first_index = start / block_size;
    std::uint64_t last_index  = (end - 1) / block_size;

    if (first_index >= num_blocks) {
        return Status::OK();
    }
    if (last_index >= num_blocks) {
        last_index = num_blocks - 1;
    }

    for (std::uint64_t idx = first_index; idx <= last_index; ++idx) {
        std::uint64_t block_offset = idx * block_size;
        std::uint64_t block_len;

        if (idx == num_blocks - 1) {
            // 最后一个块，长度可能小于 block_size
            block_len = total_len - block_offset;
        } else {
            block_len = block_size;
        }

        BlockInfo bi;
        bi.block_index = idx;
        bi.offset      = block_offset;
        bi.length      = block_len;

        BlockLocation loc;
        loc.block = bi;
        loc.datanodes.clear();
        loc.datanodes.push_back(datanode_endpoint_);

        out.push_back(loc);

        // 同步更新 runtime_blocks_ 映射，用于 DN 的 resolve_block()
        BlockId bid = make_block_id(meta->file_id, bi.block_index);

        RuntimeBlockLocation rt;
        rt.path       = path;
        rt.file_id    = meta->file_id;
        rt.block      = bi;
        rt.block_size = block_size;
        runtime_blocks_[bid] = std::move(rt);
    }

    return Status::OK();
}

/**
 * allocate_block
 *
 * 语义：
 *  - 为指定文件分配下一个逻辑块：
 *      block_index = 当前 runtime FileBlockMeta.blocks.size()
 *      offset      = index * block_size
 *      length      = 0（由写路径逐步更新，或在 complete 时一次性确定）
 *
 *  - 对于已经 complete 的文件（meta->length > 0 且 blocks 为空），
 *    Stage 2 不支持 append，直接返回错误。
 *
 * 持久化：
 *  - 只通过 metadata_store 保存 {file_id, block_size, replication, length}，
 *    blocks 不持久化。
 *  - length 一般在 finalize_file_blocks() 时基于实际文件长度回填。
 */
Status BlockManager::allocate_block(const std::string& path,
                                    BlockInfo& out_block,
                                    BlockLocation& out_location) {
    std::lock_guard<std::mutex> lock(mu_);

    FileBlockMeta* meta = nullptr;
    Status st = get_or_load_meta_locked(path, meta);
    if (st != Status::OK()) {
        return st;
    }

    // 简单防御：对于已经有 length 的文件，认为其已完成，不支持继续分配 block（即 append）
    if (meta->length > 0 && meta->blocks.empty()) {
        log(LogLevel::ERROR,
            "BlockManager::allocate_block: file already finalized (path=%s, length=%lu)",
            path.c_str(), static_cast<unsigned long>(meta->length));
        return Status::Error(-1, "allocate_block on finalized file not supported");
    }

    const std::uint64_t block_size = meta->block_size;
    if (block_size == 0) {
        log(LogLevel::ERROR,
            "BlockManager::allocate_block: invalid block_size=0 path=%s",
            path.c_str());
        return Status::Error(-1, "invalid block_size");
    }

    BlockIndex index = static_cast<BlockIndex>(meta->blocks.size());
    std::uint64_t offset = index * block_size;

    BlockInfo bi;
    bi.block_index = index;
    bi.offset      = offset;
    bi.length      = 0;

    meta->blocks.push_back(bi);

    // 注意：Xattr 只存 file_id / block_size / replication / length，
    // 这里的 save 主要是为了确保 file_id/bs/rep 的持久化，
    // length 一般在 finalize_file_blocks() 中回填。
    st = meta_->save_file_block_meta(path, *meta);
    if (st != Status::OK()) {
        log(LogLevel::ERROR,
            "BlockManager::allocate_block: save_file_block_meta failed path=%s",
            path.c_str());
        // 暂不回滚 meta->blocks，认为调用方的重试逻辑会处理
        return st;
    }

    // 填充输出参数
    out_block = bi;
    out_location.block = bi;
    out_location.datanodes.clear();
    out_location.datanodes.push_back(datanode_endpoint_);

    // 建立 BlockId -> RuntimeBlockLocation 映射
    BlockId bid = make_block_id(meta->file_id, bi.block_index);

    RuntimeBlockLocation rt;
    rt.path       = path;
    rt.file_id    = meta->file_id;
    rt.block      = bi;
    rt.block_size = block_size;
    runtime_blocks_[bid] = std::move(rt);

    return Status::OK();
}

/**
 * finalize_file_blocks
 *
 * 语义：
 *  - 在 complete 流程中由 NN 调用，用于“定稿”该文件的块信息。
 *  - 在当前设计下，Xattr 中只需要一个最终 length 即可：
 *      - block_size / replication 来自已有 meta；
 *      - blocks 列表不持久化。
 *
 * 假设：
 *  - 上层在调用本函数前，已经根据 CephFS 实际文件大小更新了 meta->length，
 *    或者本函数可以从 runtime blocks 计算（见下）。
 *
 * 实现策略（Stage 2 简化版）：
 *  - 如果 meta->length == 0 且 runtime blocks 非空，
 *    用最后一个 block 的 offset + length 推导 length；
 *  - 然后保存 meta 到 Xattr。
 *
 * 注意：
 *  - 真正正确的做法是：在 complete 阶段从 CephFS stat 获取文件长度，
 *    再把 length 设置到 FileBlockMeta 中；这可以在上层或未来扩展中完成。
 */
// TODO: DN 写入时更新 BlockInfo.length 的问题
// 当前基于 meta->blocks.back().length 推导 meta->length 的，这隐含一个前提：
// DN 在写入过程中会把“当前块已经写了多少字节”同步到 BlockInfo.length
Status BlockManager::finalize_file_blocks(const std::string& path) {
    std::lock_guard<std::mutex> lock(mu_);

    FileBlockMeta* meta = nullptr;
    Status st = get_or_load_meta_locked(path, meta);
    if (st != Status::OK()) {
        return st;
    }

    if (meta->length == 0 && !meta->blocks.empty()) {
        const auto& last = meta->blocks.back();
        meta->length = last.offset + last.length;
    }

    // 如果 length 仍然为 0，说明这是空文件或者上层尚未设置长度，
    // 对于空文件这没问题；对于非空文件，建议在上层补充 stat 逻辑。
    st = meta_->save_file_block_meta(path, *meta);
    if (st != Status::OK()) {
        log(LogLevel::ERROR,
            "BlockManager::finalize_file_blocks: save_file_block_meta failed path=%s",
            path.c_str());
        return st;
    }

    return Status::OK();
}

/**
 * resolve_block
 *
 * 语义：
 *  - DN 模块根据客户端传来的 BlockId，反查：
 *      - 对应的 HDFS 路径 path
 *      - BlockInfo（block_index, offset, length）
 *      - block_size（便于计算偏移）
 *
 * 依赖：
 *  - runtime_blocks_ 由 get_block_locations / allocate_block 填充；
 *    即：NN 在分配/查询 block 位置时，会提前把这些信息放进去。
 */
Status BlockManager::resolve_block(BlockId block_id,
                                   std::string& out_path,
                                   BlockInfo& out_block,
                                   std::uint64_t& out_block_size) {
    std::lock_guard<std::mutex> lock(mu_);

    auto it = runtime_blocks_.find(block_id);
    if (it == runtime_blocks_.end()) {
        log(LogLevel::ERROR,
            "BlockManager::resolve_block: unknown block_id=%ld",
            static_cast<long>(block_id));
        return Status::Error(-1, "block_id not found");
    }

    const RuntimeBlockLocation& rt = it->second;
    out_path       = rt.path;
    out_block      = rt.block;
    out_block_size = rt.block_size;

    return Status::OK();
}

} // namespace hcg
