#include "meta/xattr_metadata_store.h"
#include "meta/metadata_store.h"
#include "common/logging.h"
#include <functional> // std::hash
#include <sstream>
#include <memory>
// 你后面可以加 JSON 库来编码/解码 meta

namespace hcg {

XattrMetadataStore::XattrMetadataStore(std::shared_ptr<ICephFsAdapter> ceph)
    : ceph_(std::move(ceph)) {}

Status XattrMetadataStore::load_file_block_meta(const std::string& path,
                                                FileBlockMeta& out_meta) {
    std::string value;
    int rc = ceph_->get_xattr(path, xattr_key_, value);
    if (rc != 0) {
        log(LogLevel::DEBUG, "No block meta for path=%s", path.c_str());
        out_meta.file_id = alloc_file_id(path);
        out_meta.block_size = 128 * 1024 * 1024; // 默认 128MB
        out_meta.blocks.clear();
        return Status::OK();
    }

    return parse_meta(value, out_meta);
    // TODO: decode value -> out_meta
    // (void)value;
    // return Status::OK();
}

Status XattrMetadataStore::save_file_block_meta(const std::string& path,
                                                const FileBlockMeta& meta) {
    // TODO: encode meta -> string
    // std::string encoded = "{}";

    // int rc = ceph_->set_xattr(path, "user.hdfs.meta", encoded);
    // if (rc != 0) {
    //     return Status::Error(rc, "set_xattr failed");
    // }
    // return Status::OK();


    std::string s = format_meta(meta);
    int rc = ceph_->set_xattr(path, xattr_key_, s);
    if (rc < 0) {
        log(LogLevel::ERROR, "save_file_block_meta: set_xattr failed path=%s rc=%d",
            path.c_str(), rc);
        return Status::Error(rc, "set_xattr failed");
    }
    return Status::OK();;
}

// 简单文本格式：bs=134217728;rep=1;len=12345
std::string XattrMetadataStore::format_meta(const FileBlockMeta& meta) {
    std::ostringstream oss;
    oss << "bs=" << meta.block_size
        << ";rep=" << meta.replication
        << ";len=" << meta.length;
    return oss.str();
}

Status XattrMetadataStore::parse_meta(const std::string& s, FileBlockMeta& out) {
    FileBlockMeta m;
    std::size_t pos_bs = s.find("bs=");
    std::size_t pos_rep = s.find("rep=");
    std::size_t pos_len = s.find("len=");

    if (pos_bs == std::string::npos ||
        pos_rep == std::string::npos ||
        pos_len == std::string::npos) {
        log(LogLevel::ERROR, "parse_meta: invalid format '%s'", s.c_str());
        return Status::Error(-1, "parse_meta failed");
    }

    auto parse_value = [&](std::size_t pos) -> std::uint64_t {
        std::size_t start = s.find('=', pos);
        if (start == std::string::npos) return 0;
        ++start;
        std::size_t end = s.find_first_of(";", start);
        std::string v = s.substr(start, end == std::string::npos ? std::string::npos : end - start);
        return std::stoull(v);
    };

    m.block_size = parse_value(pos_bs);
    m.replication = static_cast<std::uint32_t>(parse_value(pos_rep));
    m.length = parse_value(pos_len);

    if (!m.is_valid()) {
        log(LogLevel::ERROR, "parse_meta: invalid values in '%s'", s.c_str());
        return Status::Error(-1, "parse_meta failed");
    }
    out = m;
    return Status::OK();
}

FileId XattrMetadataStore::alloc_file_id(const std::string& path) {
    // 简单版: hash(path)
    std::hash<std::string> h;
    return static_cast<FileId>(h(path));
}

} // namespace hcg

