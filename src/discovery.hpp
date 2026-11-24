#pragma once
#include "config.hpp"
#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <unordered_map>
#include <utility>

struct PeerEvent {
    enum class Type { Joined, Lost };
    Type type;
    uint32_t node_id;
    std::string ip;
};

class PeerDiscovery {
public:
    explicit PeerDiscovery(const Config& config);
    ~PeerDiscovery();
    PeerDiscovery(const PeerDiscovery&) = delete;
    PeerDiscovery& operator=(const PeerDiscovery&) = delete;

    std::vector<PeerEvent> take_events();
    std::vector<std::pair<uint32_t, std::string>> peers() const;

private:
    void run();
    Config config_;
    int socket_ = -1;
    std::atomic<bool> running_{false};
    std::thread worker_;
    mutable std::mutex mutex_;
    struct Peer { std::string ip; uint64_t last_seen_ms; };
    std::unordered_map<uint32_t, Peer> peers_;
    std::vector<PeerEvent> events_;
};
