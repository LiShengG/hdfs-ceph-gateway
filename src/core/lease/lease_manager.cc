// core/lease/lease_manager.cc
#include "core/lease/lease_manager.h"
#include "common/logging.h"

namespace hcg {

LeaseManager::LeaseManager() = default;

std::string LeaseManager::make_key(const std::string& client_id,
                                   const std::string& path) {
    return client_id + ":" + path;
}

Status LeaseManager::add_lease(const std::string& client_id,
                               const std::string& path,
                               bool write) {
    std::lock_guard<std::mutex> g(mu_);
    Lease l;
    l.client_id = client_id;
    l.path = path;
    l.is_write = write;
    l.last_heartbeat_ms = 0; // TODO: 获取当前时间
    leases_[make_key(client_id, path)] = l;
    return Status::OK();
}

Status LeaseManager::renew_lease(const std::string& client_id,
                                 const std::string& path) {
    std::lock_guard<std::mutex> g(mu_);
    auto key = make_key(client_id, path);
    auto it = leases_.find(key);
    if (it == leases_.end()) {
        return Status::Error(-1, "lease not found");
    }
    it->second.last_heartbeat_ms = 0; // TODO: 当前时间
    return Status::OK();
}

Status LeaseManager::remove_lease(const std::string& client_id,
                                  const std::string& path) {
    std::lock_guard<std::mutex> g(mu_);
    leases_.erase(make_key(client_id, path));
    return Status::OK();
}

void LeaseManager::scan_expired_leases() {
    std::lock_guard<std::mutex> g(mu_);
    // TODO: 根据当前时间扫描过期 lease, 做恢复
    log(LogLevel::DEBUG, "scan_expired_leases: leases=%zu", leases_.size());
}

} // namespace hcg
