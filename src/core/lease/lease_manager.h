// core/lease/lease_manager.h
#pragma once

#include <string>
#include <vector>
#include <mutex>
#include <unordered_map>
#include "common/status.h"

namespace hcg {

struct Lease {
    std::string client_id;
    std::string path;
    bool is_write {false};
    std::uint64_t last_heartbeat_ms {0};
};

class ILeaseManager {
public:
    virtual ~ILeaseManager() = default;

    virtual Status add_lease(const std::string& client_id,
                             const std::string& path,
                             bool write) = 0;
    virtual Status renew_lease(const std::string& client_id,
                               const std::string& path) = 0;
    virtual Status remove_lease(const std::string& client_id,
                                const std::string& path) = 0;

    virtual void scan_expired_leases() = 0;
};

class LeaseManager : public ILeaseManager {
public:
    LeaseManager();

    Status add_lease(const std::string& client_id,
                     const std::string& path,
                     bool write) override;
    Status renew_lease(const std::string& client_id,
                       const std::string& path) override;
    Status remove_lease(const std::string& client_id,
                        const std::string& path) override;

    void scan_expired_leases() override;

private:
    std::mutex mu_;
    std::unordered_map<std::string, Lease> leases_; // key = client_id+path

    std::string make_key(const std::string& client_id,
                         const std::string& path);
};

} // namespace hcg
