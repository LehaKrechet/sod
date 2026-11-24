#include "multicast.hpp"
#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h>
#include <time.h>
#include <unordered_set>

namespace {
#pragma pack(push, 1)
struct WirePacket {
    uint32_t magic;
    uint16_t version;
    uint16_t reserved;
    uint32_t node_id;
    uint64_t sequence;
    uint32_t value;
};
#pragma pack(pop)

constexpr uint32_t MAGIC = 0x534F4431; // "SOD1"

uint64_t monotonic_ns() {
    timespec ts{};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return uint64_t(ts.tv_sec) * 1'000'000'000ULL + uint64_t(ts.tv_nsec);
}
}

Multicast::Multicast(const Config& config) : config_(config) {
    socket_ = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (socket_ < 0) throw std::runtime_error("socket(): " + std::string(std::strerror(errno)));

    int reuse = 1;
    if (setsockopt(socket_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0)
        throw std::runtime_error("setsockopt(SO_REUSEADDR) failed");

    sockaddr_in local{};
    local.sin_family = AF_INET;
    local.sin_port = htons(config_.multicast_port);
    local.sin_addr.s_addr = htonl(INADDR_ANY);
    if (bind(socket_, reinterpret_cast<sockaddr*>(&local), sizeof(local)) < 0)
        throw std::runtime_error("bind(): " + std::string(std::strerror(errno)));

    ip_mreq membership{};
    if (inet_pton(AF_INET, config_.multicast_group.c_str(), &membership.imr_multiaddr) != 1)
        throw std::runtime_error("Некорректный multicast_group");
    if (inet_pton(AF_INET, config_.interface_ip.c_str(), &membership.imr_interface) != 1)
        throw std::runtime_error("Некорректный interface IPv4");

    if (setsockopt(socket_, IPPROTO_IP, IP_ADD_MEMBERSHIP, &membership, sizeof(membership)) < 0)
        throw std::runtime_error("IP_ADD_MEMBERSHIP: " + std::string(std::strerror(errno)));

    unsigned char loop = 1;
    unsigned char ttl = 1;
    setsockopt(socket_, IPPROTO_IP, IP_MULTICAST_LOOP, &loop, sizeof(loop));
    setsockopt(socket_, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl));

    in_addr outgoing{};
    outgoing.s_addr = membership.imr_interface.s_addr;
    if (setsockopt(socket_, IPPROTO_IP, IP_MULTICAST_IF, &outgoing, sizeof(outgoing)) < 0)
        throw std::runtime_error("IP_MULTICAST_IF: " + std::string(std::strerror(errno)));
}

Multicast::~Multicast() {
    if (socket_ >= 0) close(socket_);
}

void Multicast::send(uint64_t sequence, uint32_t value) {
    WirePacket packet{};
    packet.magic = htonl(MAGIC);
    packet.version = htons(1);
    packet.node_id = htonl(config_.node_id);
    packet.sequence = htobe64(sequence);
    packet.value = htonl(value);

    sockaddr_in destination{};
    destination.sin_family = AF_INET;
    destination.sin_port = htons(config_.multicast_port);
    if (inet_pton(AF_INET, config_.multicast_group.c_str(), &destination.sin_addr) != 1)
        throw std::runtime_error("Некорректный multicast_group");

    const auto sent = sendto(socket_, &packet, sizeof(packet), 0,
        reinterpret_cast<sockaddr*>(&destination), sizeof(destination));
    if (sent != static_cast<ssize_t>(sizeof(packet)))
        throw std::runtime_error("sendto(): " + std::string(std::strerror(errno)));
}

std::vector<ReceivedPacket> Multicast::receive_cycle(
    uint64_t sequence, uint32_t expected_nodes, uint32_t timeout_us) {
    std::vector<ReceivedPacket> result;
    result.reserve(expected_nodes);
    std::unordered_set<uint32_t> seen;
    seen.reserve(expected_nodes);
    const uint64_t deadline = monotonic_ns() + uint64_t(timeout_us) * 1000ULL;

    // Короткое активное ожидание: ppoll() на интервалах в десятки микросекунд
    // часто добавляет задержку пробуждения планировщика, сопоставимую с самим бюджетом.
    // Здесь поток опрашивает неблокирующий UDP-сокет до дедлайна.
    while (seen.size() < expected_nodes) {
        for (;;) {
            WirePacket wire{};
            const auto n = recvfrom(socket_, &wire, sizeof(wire), MSG_DONTWAIT, nullptr, nullptr);
            if (n < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                if (errno == EINTR) continue;
                throw std::runtime_error("recvfrom(): " + std::string(std::strerror(errno)));
            }
            if (n != static_cast<ssize_t>(sizeof(wire))) continue;
            if (ntohl(wire.magic) != MAGIC || ntohs(wire.version) != 1) continue;

            const uint32_t node = ntohl(wire.node_id);
            const uint64_t seq = be64toh(wire.sequence);
            if (seq != sequence || node == 0 || node > 65535 || seen.count(node)) continue;

            seen.insert(node);
            result.push_back({node, seq, monotonic_ns(), ntohl(wire.value)});
            if (seen.size() >= expected_nodes) break;
        }
        if (seen.size() >= expected_nodes || monotonic_ns() >= deadline) break;
        // Не делаем sleep/yield: это измерительный участок с бюджетом в микросекунды.
    }
    return result;
}
