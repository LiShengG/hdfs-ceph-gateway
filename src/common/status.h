#pragma once

#include <string>

namespace hcg {

class Status {
public:
    static Status OK() { return Status(); }
    static Status Error(int code, const std::string& msg) {
        return Status(code, msg);
    }

    bool ok() const { return code_ == 0; }
    int code() const { return code_; }
    const std::string& message() const { return msg_; }

    bool operator==(const Status& other) const {
        return code_ == other.code_ && msg_ == other.msg_;
    }

    bool operator!=(const Status& other) const {
        return !(*this == other);
    }

private:
    int code_ {0};
    std::string msg_;

    Status() = default;
    Status(int c, std::string m) : code_(c), msg_(std::move(m)) {}
};

} // namespace hcg
