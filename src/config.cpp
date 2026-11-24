#include "config.hpp"
#include <fstream>
#include <stdexcept>
#include <string>
#include <unordered_map>

static std::string trim(const std::string& s) {
    const auto first = s.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) return {};
    const auto last = s.find_last_not_of(" \t\r\n");
    return s.substr(first, last - first + 1);
}

Config load_config(const std::string& path) {
    std::ifstream in(path);
    if (!in) throw std::runtime_error("Не удалось открыть конфигурацию: " + path);

    std::unordered_map<std::string, std::string> values;
    std::string line;
    while (std::getline(in, line)) {
        line = trim(line);
        if (line.empty() || line[0] == '#' || line[0] == ';') continue;
        const auto pos = line.find('=');
        if (pos == std::string::npos) continue;
        values[trim(line.substr(0, pos))] = trim(line.substr(pos + 1));
    }

    Config c;
    auto get = [&](const char* key, const std::string& fallback) {
        const auto it = values.find(key);
        return it == values.end() ? fallback : it->second;
    };
    try {
        c.node_id = static_cast<uint32_t>(std::stoul(get("node_id", "1")));
        c.expected_nodes = static_cast<uint32_t>(std::stoul(get("expected_nodes", "2")));
        c.multicast_group = get("multicast_group", c.multicast_group);
        c.multicast_port = static_cast<uint16_t>(std::stoul(get("multicast_port", "50000")));
        c.interface_ip = get("interface", "0.0.0.0");
        c.receive_timeout_us = static_cast<uint32_t>(std::stoul(get("receive_timeout_us", "150")));
        c.heartbeat_interval_ms = static_cast<uint32_t>(std::stoul(get("heartbeat_interval_ms", "250")));
        c.peer_timeout_ms = static_cast<uint32_t>(std::stoul(get("peer_timeout_ms", "1500")));
        c.peer_status_interval_ms = static_cast<uint32_t>(std::stoul(get("peer_status_interval_ms", "1000")));
        c.cycles = std::stoull(get("cycles", "10000"));
        c.cycle_log = get("cycle_log", c.cycle_log);
        c.packet_log = get("packet_log", c.packet_log);
    } catch (const std::exception&) {
        throw std::runtime_error("Ошибка преобразования значения в config.ini");
    }

    if (c.expected_nodes < 2 || c.expected_nodes > 10)
        throw std::runtime_error("expected_nodes должен быть в диапазоне 2..10");
    if (c.node_id == 0 || c.node_id > 65535)
        throw std::runtime_error("node_id должен быть в диапазоне 1..65535");
    if (c.multicast_port == 0 || c.receive_timeout_us == 0 ||
        c.heartbeat_interval_ms == 0 || c.peer_status_interval_ms == 0 ||
        c.peer_timeout_ms <= c.heartbeat_interval_ms * 2)
        throw std::runtime_error("Порт/тайм-ауты должны быть положительными; peer_timeout_ms должен быть больше двух heartbeat-интервалов");
    return c;
}
