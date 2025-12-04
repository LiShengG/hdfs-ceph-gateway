#pragma once

#include "meta/metadata_store.h"
#include "fsal/cephfs/cephfs_adapter.h"
#include <memory>

namespace hcg {

// 简单用 xattr 实现的 MetadataStore
class XattrMetadataStore : public IMetadataStore {
public:
    explicit XattrMetadataStore(std::shared_ptr<ICephFsAdapter> ceph);

    Status load_file_block_meta(const std::string& path,
                                FileBlockMeta& out_meta) override;

    Status save_file_block_meta(const std::string& path,
                                const FileBlockMeta& meta) override;

    FileId alloc_file_id(const std::string& path) override;

private:
    // ICephFsAdapter* ceph_;
    std::shared_ptr<ICephFsAdapter> ceph_;
    std::string xattr_key_ = "user.hdfs.meta";

    Status parse_meta(const std::string& s, FileBlockMeta& out);
    std::string format_meta(const FileBlockMeta& meta);
};

} // namespace hcg

