#pragma once

#include "config.hpp"

#include <atomic>
#include <map>
#include <string>
#include <thread>

// Отдельный поток отправляет маяки и собирает список соседних узлов.
class Discovery {
    int sock = -1;
    std::atomic<bool> stop{false};
    std::thread th;
    int id, port, heartbeat, timeout;
    std::string group, iface;

public:
    explicit Discovery(const Config& config);
    ~Discovery();

    std::map<int, std::string> peers();

private:
    void run();
};
