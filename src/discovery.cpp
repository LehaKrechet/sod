#include "discovery.hpp"
#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <poll.h>
#include <stdexcept>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

namespace {
#pragma pack(push, 1)
struct Beacon {
    uint32_t magic;
    uint16_t version;
    uint16_t reserved;
    uint32_t node_id;
};
#pragma pack(pop)
constexpr uint32_t DISCOVERY_MAGIC = 0x534F4448; // "SODH"

uint64_t monotonic_ms() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());
}
}

PeerDiscovery::PeerDiscovery(const Config& config) : config_(config) {
    socket_ = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (socket_ < 0)
        throw std::runtime_error("Discovery socket(): " + std::string(std::strerror(errno)));

    int reuse = 1;
    if (setsockopt(socket_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0)
        throw std::runtime_error("Discovery SO_REUSEADDR failed");

    sockaddr_in local{};
    local.sin_family = AF_INET;
    local.sin_port = htons(config_.multicast_port);
    local.sin_addr.s_addr = htonl(INADDR_ANY);
    if (bind(socket_, reinterpret_cast<sockaddr*>(&local), sizeof(local)) < 0)
        throw std::runtime_error("Discovery bind(): " + std::string(std::strerror(errno)));

    ip_mreq membership{};
    if (inet_pton(AF_INET, config_.multicast_group.c_str(), &membership.imr_multiaddr) != 1 ||
        inet_pton(AF_INET, config_.interface_ip.c_str(), &membership.imr_interface) != 1)
        throw std::runtime_error("Некорректный адрес multicast/interface в discovery");

    if (setsockopt(socket_, IPPROTO_IP, IP_ADD_MEMBERSHIP, &membership, sizeof(membership)) < 0)
        throw std::runtime_error("Discovery IP_ADD_MEMBERSHIP: " + std::string(std::strerror(errno)));

    unsigned char loop = 1, ttl = 1;
    setsockopt(socket_, IPPROTO_IP, IP_MULTICAST_LOOP, &loop, sizeof(loop));
    setsockopt(socket_, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl));
    in_addr outgoing{};
    outgoing.s_addr = membership.imr_interface.s_addr;
    if (setsockopt(socket_, IPPROTO_IP, IP_MULTICAST_IF, &outgoing, sizeof(outgoing)) < 0)
        throw std::runtime_error("Discovery IP_MULTICAST_IF failed");

    running_ = true;
    worker_ = std::thread(&PeerDiscovery::run, this);
}

PeerDiscovery::~PeerDiscovery() {
    running_ = false;
    if (worker_.joinable()) worker_.join();
    if (socket_ >= 0) close(socket_);
}

void PeerDiscovery::run() {
    const auto heartbeat = std::chrono::milliseconds(config_.heartbeat_interval_ms);
    auto next_beacon = std::chrono::steady_clock::now();

    sockaddr_in destination{};
    destination.sin_family = AF_INET;
    destination.sin_port = htons(config_.multicast_port);
    inet_pton(AF_INET, config_.multicast_group.c_str(), &destination.sin_addr);

    while (running_) {
        const auto now = std::chrono::steady_clock::now();
        if (now >= next_beacon) {
            Beacon b{};
            b.magic = htonl(DISCOVERY_MAGIC);
            b.version = htons(1);
            b.node_id = htonl(config_.node_id);
            (void)sendto(socket_, &b, sizeof(b), 0,
                         reinterpret_cast<sockaddr*>(&destination), sizeof(destination));
            next_beacon = now + heartbeat;
        }

        pollfd pfd{socket_, POLLIN, 0};
        const int ready = poll(&pfd, 1, 25);
        if (ready > 0 && (pfd.revents & POLLIN)) {
            for (;;) {
                Beacon b{};
                sockaddr_in source{};
                socklen_t source_len = sizeof(source);
                const auto n = recvfrom(socket_, &b, sizeof(b), MSG_DONTWAIT,
                    reinterpret_cast<sockaddr*>(&source), &source_len);
                if (n < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                    if (errno == EINTR) continue;
                    break;
                }
                if (n != static_cast<ssize_t>(sizeof(b)) ||
                    ntohl(b.magic) != DISCOVERY_MAGIC || ntohs(b.version) != 1)
                    continue;

                const uint32_t id = ntohl(b.node_id);
                if (id == 0 || id == config_.node_id) continue;

                char ipbuf[INET_ADDRSTRLEN]{};
                inet_ntop(AF_INET, &source.sin_addr, ipbuf, sizeof(ipbuf));
                const uint64_t stamp = monotonic_ms();

                std::lock_guard<std::mutex> lock(mutex_);
                auto it = peers_.find(id);
                if (it == peers_.end()) {
                    peers_[id] = {ipbuf, stamp};
                    events_.push_back({PeerEvent::Type::Joined, id, ipbuf});
                } else {
                    it->second.ip = ipbuf;
                    it->second.last_seen_ms = stamp;
                }
            }
        }

        const uint64_t stamp = monotonic_ms();
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto it = peers_.begin(); it != peers_.end();) {
            if (stamp - it->second.last_seen_ms > config_.peer_timeout_ms) {
                events_.push_back({PeerEvent::Type::Lost, it->first, it->second.ip});
                it = peers_.erase(it);
            } else {
                ++it;
            }
        }
    }
}

std::vector<PeerEvent> PeerDiscovery::take_events() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<PeerEvent> out;
    out.swap(events_);
    return out;
}

std::vector<std::pair<uint32_t, std::string>> PeerDiscovery::peers() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::pair<uint32_t, std::string>> out;
    out.reserve(peers_.size());
    for (const auto& [id, peer] : peers_) out.emplace_back(id, peer.ip);
    return out;
}
