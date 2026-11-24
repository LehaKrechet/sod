#pragma once
#include <cstdint>
#include <string>

struct Config {
    uint32_t node_id = 1;
    uint32_t expected_nodes = 2;
    std::string multicast_group = "239.10.10.10";
    uint16_t multicast_port = 50000;
    std::string interface_ip = "0.0.0.0";
    uint32_t receive_timeout_us = 150;
    uint32_t heartbeat_interval_ms = 250;
    uint32_t peer_timeout_ms = 1500;
    uint32_t peer_status_interval_ms = 1000;
    uint64_t cycles = 10000;
    std::string cycle_log = "timing_cycles.csv";
    std::string packet_log = "timing_packets.csv";
};

Config load_config(const std::string& path);
