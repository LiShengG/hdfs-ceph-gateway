#pragma once

#include "meta/metadata_store.h"
#include "fsal/cephfs/cephfs_adapter.h"

namespace hcg {

// 简单用 xattr 实现的 MetadataStore
class XattrMetadataStore : public IMetadataStore {
public:
    explicit XattrMetadataStore(ICephFsAdapter* ceph);

    Status load_file_block_meta(const std::string& path,
                                FileBlockMeta& out_meta) override;

    Status save_file_block_meta(const std::string& path,
                                const FileBlockMeta& meta) override;

    FileId alloc_file_id(const std::string& path) override;

private:
    ICephFsAdapter* ceph_;
};

} // namespace hcg

