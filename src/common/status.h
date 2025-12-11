#pragma once

#include <string>

namespace hcg {

class Status {
public:
    static Status OK() { return Status(); }
    static Status Error(int code, const std::string& msg) {
        return Status(code, msg);
    }

    static Status NotFound(const std::string& msg = "Not found") {
        return Status(kNotFound, msg);
    }
    // 可选：暴露错误码常量，便于外部比较
    static constexpr int kOk = 0;
    static constexpr int kNotFound = -1; 

    bool ok() const { return code_ == kOk; }
    int code() const { return code_; }
    const std::string& message() const { return msg_; }

    bool operator==(const Status& other) const {
        return code_ == other.code_ && msg_ == other.msg_;
    }

    bool operator!=(const Status& other) const {
        return !(*this == other);
    }

private:
    int code_ {kOk};
    std::string msg_;

    Status() = default;
    Status(int c, std::string m) : code_(c), msg_(std::move(m)) {}
};

} // namespace hcg
