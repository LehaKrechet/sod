#pragma once
#include <string>
struct Config {
 int node_id=1, expected_nodes=2, data_port=5000, discovery_port=5001;
 std::string multicast_group="239.10.10.10", interface_ip="0.0.0.0";
 size_t payload_size_bytes=2048, chunk_payload_bytes=1200;
 int cycle_period_us=1000, receive_timeout_us=140, heartbeat_interval_ms=250, peer_timeout_ms=1500, peer_status_interval_ms=1000, graph_width=48, graph_refresh_ms=100;
 bool terminal_graph=true, csv_logging=true;
};
Config load_config(const std::string& path);
