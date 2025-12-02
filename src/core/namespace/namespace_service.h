#pragma once

#include <string>
#include <vector>
#include "common/types.h"
#include "common/status.h"

namespace hcg {

struct FileStatus {
    std::string path;
    bool is_dir {false};
    std::uint64_t length {0};
    std::uint16_t replication {3};
    std::uint64_t block_size {128 * 1024 * 1024};
    std::string owner;
    std::string group;
    std::uint32_t mode {0644};
    std::uint64_t modification_time {0};
    std::uint64_t access_time {0};
};

class INamespaceService {
public:
    virtual ~INamespaceService() = default;

    virtual Status mkdirs(const std::string& path, std::uint32_t mode) = 0;

    virtual Status delete_path(const std::string& path, bool recursive) = 0;

    virtual Status get_file_status(const std::string& path,
                                   FileStatus& status) = 0;

    virtual Status list_status(const std::string& path,
                               std::vector<FileStatus>& out) = 0;

    virtual Status create_file(const std::string& path,
                               const FileStatus& initial,
                               FileId& out_file_id) = 0;
};

class NamespaceService : public INamespaceService {
public:
    NamespaceService();

    Status mkdirs(const std::string& path, std::uint32_t mode) override;
    Status delete_path(const std::string& path, bool recursive) override;
    Status get_file_status(const std::string& path,
                           FileStatus& status) override;
    Status list_status(const std::string& path,
                       std::vector<FileStatus>& out) override;
    Status create_file(const std::string& path,
                       const FileStatus& initial,
                       FileId& out_file_id) override;
};

} // namespace hcg
