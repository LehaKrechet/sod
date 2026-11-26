#include "config.hpp"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <sstream>
#include <stdexcept>

namespace {
// Убирает пробелы по краям строки, чтобы значения из INI читались без лишнего.
void trim(std::string& value) {
    const auto first = value.find_first_not_of(" \t\r");
    if (first == std::string::npos) {
        value.clear();
        return;
    }

    const auto last = value.find_last_not_of(" \t\r");
    value = value.substr(first, last - first + 1);
}
}

Config load_config(const std::string& path) {
    Config config;
    std::ifstream file(path);

    if (!file) {
        throw std::runtime_error("Cannot open config: " + path);
    }

    std::string line;
    while (std::getline(file, line)) {
        // Комментарии в конфигурации начинаются с символа #.
        const auto comment_pos = line.find('#');
        if (comment_pos != std::string::npos) {
            line.resize(comment_pos);
        }

        const auto equal_pos = line.find('=');
        if (equal_pos == std::string::npos) {
            continue;
        }

        std::string key = line.substr(0, equal_pos);
        std::string value = line.substr(equal_pos + 1);
        trim(key);
        trim(value);

        if (value.empty()) {
            continue;
        }

        // Для числовых параметров преобразование выполняется здесь же,
        // поэтому ошибка будет привязана к конкретному ключу конфигурации.
        try {
            if (key == "node_id") config.node_id = std::stoi(value);
            else if (key == "expected_nodes") config.expected_nodes = std::stoi(value);
            else if (key == "data_port") config.data_port = std::stoi(value);
            else if (key == "discovery_port") config.discovery_port = std::stoi(value);
            else if (key == "multicast_group") config.multicast_group = value;
            else if (key == "interface_ip") config.interface_ip = value;
            else if (key == "payload_size_bytes") config.payload_size_bytes = std::stoul(value);
            else if (key == "chunk_payload_bytes") config.chunk_payload_bytes = std::stoul(value);
            else if (key == "cycle_period_us") config.cycle_period_us = std::stoi(value);
            else if (key == "receive_timeout_us") config.receive_timeout_us = std::stoi(value);
            else if (key == "heartbeat_interval_ms") config.heartbeat_interval_ms = std::stoi(value);
            else if (key == "peer_timeout_ms") config.peer_timeout_ms = std::stoi(value);
            else if (key == "peer_status_interval_ms") config.peer_status_interval_ms = std::stoi(value);
            else if (key == "graph_width") config.graph_width = std::stoi(value);
            else if (key == "graph_refresh_ms") config.graph_refresh_ms = std::stoi(value);
            else if (key == "terminal_graph") config.terminal_graph = (value == "true" || value == "1");
            else if (key == "csv_logging") config.csv_logging = (value == "true" || value == "1");
        } catch (...) {
            throw std::runtime_error("Invalid value for config key: " + key);
        }
    }

    // Проверяем параметры, которые влияют на размер UDP-пакетов и работу обмена.
    if (config.payload_size_bytes < 1024 || config.payload_size_bytes > 102400) {
        throw std::runtime_error("payload_size_bytes must be 1024..102400");
    }
    if (config.chunk_payload_bytes < 256 || config.chunk_payload_bytes > 1400) {
        throw std::runtime_error("chunk_payload_bytes must be 256..1400 (safe MTU)");
    }
    if (config.expected_nodes < 1) {
        throw std::runtime_error("expected_nodes must be >=1");
    }

    return config;
}
