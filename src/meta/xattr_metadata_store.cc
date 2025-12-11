#include "meta/xattr_metadata_store.h"
#include "meta/metadata_store.h"
#include "common/logging.h"
#include <functional> // std::hash
#include <sstream>
#include <memory>
// 后面可以加 JSON 库来编码/解码 meta

namespace hcg {

XattrMetadataStore::XattrMetadataStore(std::shared_ptr<ICephFsAdapter> ceph)
    : ceph_(std::move(ceph)) {}

// 根据 Ceph getxattr 的约定：
//   成功返回 0，失败返回 -errno
Status XattrMetadataStore::load_file_block_meta(const std::string& path,
                                                FileBlockMeta& out_meta) {
    std::string value;
    int rc = ceph_->get_xattr(path, xattr_key_, value);
    if (rc < 0) {
        // 区分“没有该 xattr”和真正错误
        if (rc == -ENODATA) {
            log(LogLevel::DEBUG,
                "No block meta xattr for path=%s, init default meta",
                path.c_str());

            FileBlockMeta m;
            m.file_id     = alloc_file_id(path);
            m.block_size  = 128ULL * 1024 * 1024; // 默认 128MB
            m.replication = 1;
            m.length      = 0;
            m.blocks.clear();

            out_meta = m;
            return Status::OK();
        }

        log(LogLevel::ERROR,
            "load_file_block_meta: get_xattr failed path=%s rc=%d",
            path.c_str(), rc);
        return Status::Error(rc, "get_xattr failed");
    }

    // rc == 0，说明读取到了 xattr
    return parse_meta(value, out_meta);
}

// 保存时将 file_id 也写进去
std::string XattrMetadataStore::format_meta(const FileBlockMeta& meta) {
    std::ostringstream oss;
    oss << "fid=" << meta.file_id
        << ";bs=" << meta.block_size
        << ";rep=" << meta.replication
        << ";len=" << meta.length;
    return oss.str();
}

Status XattrMetadataStore::save_file_block_meta(const std::string& path,
                                                const FileBlockMeta& meta) {
    std::string s = format_meta(meta);
    int rc = ceph_->set_xattr(path, xattr_key_, s);
    if (rc < 0) {
        log(LogLevel::ERROR,
            "save_file_block_meta: set_xattr failed path=%s rc=%d",
            path.c_str(), rc);
        return Status::Error(rc, "set_xattr failed");
    }
    return Status::OK();
}

Status XattrMetadataStore::parse_meta(const std::string& s, FileBlockMeta& out) {
    FileBlockMeta m;

    // 兼容旧格式：可能没有 fid=
    std::size_t pos_fid = s.find("fid=");
    std::size_t pos_bs  = s.find("bs=");
    std::size_t pos_rep = s.find("rep=");
    std::size_t pos_len = s.find("len=");

    if (pos_bs == std::string::npos ||
        pos_rep == std::string::npos ||
        pos_len == std::string::npos ||
        pos_fid == std::string::npos) {
        log(LogLevel::ERROR, "parse_meta: invalid format '%s'", s.c_str());
        return Status::Error(-EINVAL, "parse_meta failed: missing required fields");
    }

    auto parse_value = [&](std::size_t pos) -> std::optional<std::uint64_t> {
        std::size_t start = s.find('=', pos);
        if (start == std::string::npos) {
            return std::nullopt;
        }
        ++start;
        std::size_t end = s.find_first_of(";", start);
        std::string v = s.substr(start,
                                 end == std::string::npos
                                     ? std::string::npos
                                     : end - start);

        if (v.empty()) {
            return std::nullopt;
        }

        try {
            // stoull may throw std::invalid_argument or std::out_of_range
            size_t processed = 0;
            unsigned long long val = std::stoull(v, &processed, 10);
            if (processed != v.size()) {
                // 比如 "123abc" 这种部分匹配的情况
                return std::nullopt;
            }
            return static_cast<std::uint64_t>(val);
        } catch (const std::exception&) {
            return std::nullopt;
        }
    };

    auto fid_opt = parse_value(pos_fid);
    auto bs_opt  = parse_value(pos_bs);
    auto rep_opt = parse_value(pos_rep);
    auto len_opt = parse_value(pos_len);

    if (!fid_opt || !bs_opt || !rep_opt || !len_opt) {
        log(LogLevel::ERROR, "parse_meta: failed to parse numeric values in '%s'", s.c_str());
        return Status::Error(-EINVAL, "parse_meta failed: invalid number format");
    }

    m.file_id     = *fid_opt;
    m.block_size  = *bs_opt;
    m.replication = static_cast<std::uint32_t>(*rep_opt);
    m.length      = *len_opt;

    if (!m.is_valid()) {
        log(LogLevel::ERROR, "parse_meta: invalid values in '%s'", s.c_str());
        return Status::Error(-EINVAL, "parse_meta failed: invalid meta values");
    }

    // blocks 向量由 BlockManager 按需要构造，这里保持 empty
    out = m;
    m.blocks.clear();
    return Status::OK();
}
FileId XattrMetadataStore::alloc_file_id(const std::string& path) {
    std::hash<std::string> h;
    return static_cast<FileId>(h(path));
}

} // namespace hcg
