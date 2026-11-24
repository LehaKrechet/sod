#pragma once
#include "config.hpp"
#include <cstdint>
#include <vector>

struct ReceivedPacket {
    uint32_t node_id{};
    uint64_t sequence{};
    uint64_t arrival_ns{}; // локальное монотонное время приёма
    uint32_t value{};
};

class Multicast {
public:
    explicit Multicast(const Config& config);
    ~Multicast();
    Multicast(const Multicast&) = delete;
    Multicast& operator=(const Multicast&) = delete;

    void send(uint64_t sequence, uint32_t value);
    std::vector<ReceivedPacket> receive_cycle(
        uint64_t sequence, uint32_t expected_nodes, uint32_t timeout_us);

private:
    int socket_ = -1;
    Config config_;
};
