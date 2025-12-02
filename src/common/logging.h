// common/logging.h
#pragma once

#include <cstdio>
#include <string>

namespace hcg {

enum class LogLevel {
    DEBUG,
    INFO,
    WARN,
    ERROR,
};

void log(LogLevel level, const char* fmt, ...);

} // namespace hcg
