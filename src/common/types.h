#pragma once

#include <cstdint>
#include <string>

namespace hcg { // hdfs-ceph-gateway

using FileId = std::uint64_t;

struct BlockInfo {
    std::uint64_t block_index;
    std::uint64_t offset;
    std::uint64_t length;
};

} // namespace hcg
