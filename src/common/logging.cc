// common/logging.cc
#include "common/logging.h"
#include <cstdarg>

namespace hcg {

void log(LogLevel level, const char* fmt, ...) {
    const char* prefix = nullptr;
    switch (level) {
    case LogLevel::DEBUG: prefix = "[DEBUG]"; break;
    case LogLevel::INFO:  prefix = "[INFO ]"; break;
    case LogLevel::WARN:  prefix = "[WARN ]"; break;
    case LogLevel::ERROR: prefix = "[ERROR]"; break;
    }

    std::fprintf(stderr, "%s ", prefix);

    va_list args;
    va_start(args, fmt);
    std::vfprintf(stderr, fmt, args);
    va_end(args);

    std::fprintf(stderr, "\n");
}

} // namespace hcg
