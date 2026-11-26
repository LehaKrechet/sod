#pragma once

#include <string>

// Параметры работы узла. Значения по умолчанию подходят для тестового запуска.
struct Config {
    int node_id = 1;
    int expected_nodes = 2;
    int data_port = 5000;
    int discovery_port = 5001;

    std::string multicast_group = "239.10.10.10";
    std::string interface_ip = "0.0.0.0";

    size_t payload_size_bytes = 2048;
    size_t chunk_payload_bytes = 1200;

    int cycle_period_us = 1000;
    int receive_timeout_us = 140;
    int heartbeat_interval_ms = 250;
    int peer_timeout_ms = 1500;
    int peer_status_interval_ms = 1000;
    int graph_width = 48;
    int graph_refresh_ms = 100;

    bool terminal_graph = true;
    bool csv_logging = true;
};

Config load_config(const std::string& path);
