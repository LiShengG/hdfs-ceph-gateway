// src/rpc/internal/internal_rpc_defs.h
#pragma once

#include <cstdint>
#include <string>

namespace hcg {
namespace internal_rpc {

enum class MethodId : std::uint16_t {
    UNKNOWN             = 0,
    CREATE_FILE         = 1,
    GET_FILE_INFO       = 2,
    LIST_STATUS         = 3,
    DELETE_PATH         = 4,
    ALLOCATE_BLOCK      = 5,
    GET_BLOCK_LOCATIONS = 6,
    WRITE_BLOCK         = 7,
    READ_BLOCK          = 8,
    COMPLETE            = 9,
};

inline const char* to_string(MethodId m) {
    switch (m) {
    case MethodId::CREATE_FILE:         return "CreateFile";
    case MethodId::GET_FILE_INFO:       return "GetFileInfo";
    case MethodId::LIST_STATUS:         return "ListStatus";
    case MethodId::DELETE_PATH:         return "Delete";
    case MethodId::ALLOCATE_BLOCK:      return "AllocateBlock";
    case MethodId::GET_BLOCK_LOCATIONS: return "GetBlockLocations";
    case MethodId::WRITE_BLOCK:         return "WriteBlock";
    case MethodId::READ_BLOCK:          return "ReadBlock";
    case MethodId::COMPLETE:            return "Complete";
    default:                            return "Unknown";
    }
}

/**
 * 把 methodId 转成枚举，非法值返回 UNKNOWN
 */
inline MethodId method_from_uint16(std::uint16_t v) {
    switch (v) {
    case 1: return MethodId::CREATE_FILE;
    case 2: return MethodId::GET_FILE_INFO;
    case 3: return MethodId::LIST_STATUS;
    case 4: return MethodId::DELETE_PATH;
    case 5: return MethodId::ALLOCATE_BLOCK;
    case 6: return MethodId::GET_BLOCK_LOCATIONS;
    case 7: return MethodId::WRITE_BLOCK;
    case 8: return MethodId::READ_BLOCK;
    case 9: return MethodId::COMPLETE;
    default: return MethodId::UNKNOWN;
    }
}

inline std::uint16_t to_uint16(MethodId m) {
    return static_cast<std::uint16_t>(m);
}

} // namespace internal_rpc
} // namespace hcg
