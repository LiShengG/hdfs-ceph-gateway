#pragma once

#include <string>
#include <vector>
#include "common/types.h"
#include "common/status.h"

namespace hcg {

struct FileBlockMeta {
    FileId file_id;
    std::uint64_t block_size;
    std::vector<BlockInfo> blocks;
};

class IMetadataStore {
public:
    virtual ~IMetadataStore() = default;

    virtual Status load_file_block_meta(const std::string& path,
                                        FileBlockMeta& out_meta) = 0;

    virtual Status save_file_block_meta(const std::string& path,
                                        const FileBlockMeta& meta) = 0;

    virtual FileId alloc_file_id(const std::string& path) = 0;
};

} // namespace hcg
