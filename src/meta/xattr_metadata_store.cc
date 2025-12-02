#include "meta/xattr_metadata_store.h"
#include "common/logging.h"
#include <functional> // std::hash
// 你后面可以加 JSON 库来编码/解码 meta

namespace hcg {

XattrMetadataStore::XattrMetadataStore(ICephFsAdapter* ceph)
    : ceph_(ceph) {}

Status XattrMetadataStore::load_file_block_meta(const std::string& path,
                                                FileBlockMeta& out_meta) {
    std::string value;
    int rc = ceph_->get_xattr(path, "user.hdfs.meta", value);
    if (rc != 0) {
        // 暂时简单处理: 当作 "没有 meta"
        log(LogLevel::DEBUG, "No block meta for path=%s", path.c_str());
        out_meta.file_id = alloc_file_id(path);
        out_meta.block_size = 128 * 1024 * 1024; // 默认 128MB
        out_meta.blocks.clear();
        return Status::OK();
    }
    // TODO: decode value -> out_meta
    (void)value;
    return Status::OK();
}

Status XattrMetadataStore::save_file_block_meta(const std::string& path,
                                                const FileBlockMeta& meta) {
    // TODO: encode meta -> string
    std::string encoded = "{}";

    int rc = ceph_->set_xattr(path, "user.hdfs.meta", encoded);
    if (rc != 0) {
        return Status::Error(rc, "set_xattr failed");
    }
    return Status::OK();
}

FileId XattrMetadataStore::alloc_file_id(const std::string& path) {
    // 简单版: hash(path)
    std::hash<std::string> h;
    return static_cast<FileId>(h(path));
}

} // namespace hcg

