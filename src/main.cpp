#include "config.hpp"
#include "multicast.hpp"
#include "discovery.hpp"
#include <algorithm>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <string>
#include <unordered_set>
#include <iostream>
#include <stdexcept>
#include <time.h>

static uint64_t now_ns() {
    timespec ts{};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return uint64_t(ts.tv_sec) * 1'000'000'000ULL + uint64_t(ts.tv_nsec);
}


static bool config_bool(const std::string& path, const std::string& key, bool fallback) {
    std::ifstream in(path);
    std::string line;
    while (std::getline(in, line)) {
        const auto first = line.find_first_not_of(" \t\r\n");
        if (first == std::string::npos) continue;
        line = line.substr(first);
        if (line.empty() || line[0] == '#' || line[0] == ';') continue;
        const auto pos = line.find('=');
        if (pos == std::string::npos) continue;
        std::string k = line.substr(0, pos);
        k.erase(k.find_last_not_of(" \t\r\n") + 1);
        if (k != key) continue;
        std::string v = line.substr(pos + 1);
        const auto vf = v.find_first_not_of(" \t\r\n");
        if (vf == std::string::npos) return fallback;
        v = v.substr(vf);
        return v == "true" || v == "1" || v == "yes" || v == "on";
    }
    return fallback;
}

static unsigned config_uint(const std::string& path, const std::string& key, unsigned fallback) {
    std::ifstream in(path);
    std::string line;
    while (std::getline(in, line)) {
        const auto first = line.find_first_not_of(" \t\r\n");
        if (first == std::string::npos) continue;
        line = line.substr(first);
        if (line.empty() || line[0] == '#' || line[0] == ';') continue;
        const auto pos = line.find('=');
        if (pos == std::string::npos || line.substr(0, pos) != key) continue;
        try { return static_cast<unsigned>(std::stoul(line.substr(pos + 1))); }
        catch (...) { return fallback; }
    }
    return fallback;
}

static std::string bar(double value_us, double limit_us, unsigned width) {
    if (width == 0) width = 1;
    const auto count = static_cast<unsigned>(
        std::min<double>(width, value_us / limit_us * width));
    return "[" + std::string(count, '#') + std::string(width - count, ' ') + "]";
}

static void print_timing_graph(uint64_t seq, double send_us, double receive_us,
                               double compute_us, double total_us, unsigned width) {
    constexpr double LIMIT_US = 200.0;
    std::cout << "\nЦикл " << seq << " | шкала: 0.." << LIMIT_US << " мкс\n"
              << " TX   " << bar(send_us, LIMIT_US, width) << ' '
              << std::fixed << std::setprecision(2) << send_us << " мкс\n"
              << " RX   " << bar(receive_us, LIMIT_US, width) << ' '
              << receive_us << " мкс\n"
              << " CPU  " << bar(compute_us, LIMIT_US, width) << ' '
              << compute_us << " мкс\n"
              << " ALL  " << bar(total_us, LIMIT_US, width) << ' '
              << total_us << " мкс";
    if (total_us > LIMIT_US) std::cout << "  <--- ПРЕВЫШЕНИЕ 200 мкс";
    std::cout << "\n";
}

int main(int argc, char** argv) {
    try {
        const std::string config_path = argc > 1 ? argv[1] : "config.ini";
        const Config cfg = load_config(config_path);
        const bool terminal_graph = config_bool(config_path, "terminal_graph", false);
        const unsigned graph_width = std::max(10U, std::min(100U,
            config_uint(config_path, "graph_width", 50)));
        Multicast network(cfg);
        PeerDiscovery discovery(cfg);

        std::ofstream cycles(cfg.cycle_log);
        std::ofstream packets(cfg.packet_log);
        if (!cycles || !packets) throw std::runtime_error("Не удалось открыть CSV-файлы");

        cycles << "sequence,node_id,received_nodes,missing_nodes,send_us,receive_wait_us,compute_us,total_us,complete\n";
        packets << "sequence,local_node_id,remote_node_id,arrival_from_cycle_start_us,value\n";
        cycles << std::fixed << std::setprecision(3);
        packets << std::fixed << std::setprecision(3);

        std::cout << "SOD multicast: node " << cfg.node_id << ", ожидается узлов: "
                  << cfg.expected_nodes << "\n";

        std::cout << "Discovery запущен: heartbeat каждые "
                  << cfg.heartbeat_interval_ms << " мс; тайм-аут узла "
                  << cfg.peer_timeout_ms << " мс\n";
        std::cout << "Ожидание обнаружения соседних узлов...\n";
        std::cout << "Период сводки по узлам: " << cfg.peer_status_interval_ms << " мс\n";

        auto print_peer_events = [&]() {
            for (const auto& event : discovery.take_events()) {
                if (event.type == PeerEvent::Type::Joined) {
                    std::cout << "\n[ONLINE] Обнаружен узел " << event.node_id
                              << " с IP " << event.ip << "\n";
                } else {
                    std::cout << "\n[OFFLINE] Узел " << event.node_id
                              << " (" << event.ip << ") не присылал heartbeat "
                              << cfg.peer_timeout_ms << " мс\n";
                }
            }
        };

        uint64_t seq = 0;
        auto next_peer_status = std::chrono::steady_clock::now() +
            std::chrono::milliseconds(cfg.peer_status_interval_ms);
        while (cfg.cycles == 0 || seq < cfg.cycles) {
            ++seq;
            const uint64_t cycle_start = now_ns();

            // Тестовая полезная нагрузка. Каждый узел публикует своё значение.
            const uint32_t local_value = cfg.node_id;
            const uint64_t send_start = now_ns();
            network.send(seq, local_value);
            const uint64_t send_end = now_ns();

            const auto received = network.receive_cycle(
                seq, cfg.expected_nodes, cfg.receive_timeout_us);
            const uint64_t receive_end = now_ns();

            const uint64_t compute_start = now_ns();
            volatile uint32_t calculation = 1U + 1U; // Заглушка вычисления
            (void)calculation;
            const uint64_t compute_end = now_ns();
            const uint64_t cycle_end = compute_end;

            for (const auto& p : received) {
                packets << seq << ',' << cfg.node_id << ',' << p.node_id << ','
                        << (p.arrival_ns - cycle_start) / 1000.0 << ',' << p.value << '\n';
            }

            const auto us = [](uint64_t ns) { return ns / 1000.0; };
            const bool complete = received.size() == cfg.expected_nodes;
            const double send_us = us(send_end - send_start);
            const double receive_us = us(receive_end - send_end);
            const double compute_us = us(compute_end - compute_start);
            const double total_us = us(cycle_end - cycle_start);

            cycles << seq << ',' << cfg.node_id << ',' << received.size() << ','
                   << (cfg.expected_nodes - received.size()) << ','
                   << send_us << ',' << receive_us << ',' << compute_us << ','
                   << total_us << ',' << (complete ? 1 : 0) << '\n';

            if (terminal_graph) {
                print_timing_graph(seq, send_us, receive_us, compute_us,
                                   total_us, graph_width);
            }

            // Сводка о составе сети печатается по настенным часам, а не по числу циклов.
            const auto status_now = std::chrono::steady_clock::now();
            if (status_now >= next_peer_status) {
                print_peer_events();
                const auto active = discovery.peers();
                std::cout << "\n[PEERS] Активных удалённых узлов: " << active.size()
                          << "; всего видимых с этим узлом: " << active.size() + 1
                          << " (включая текущий node_id=" << cfg.node_id << ")";
                for (const auto& peer : active)
                    std::cout << " | node_id=" << peer.first << " ip=" << peer.second;
                std::cout << "\n";
                next_peer_status = status_now +
                    std::chrono::milliseconds(cfg.peer_status_interval_ms);
            }
            if (seq % 100 == 0) {
                cycles.flush();
                packets.flush();
            }
        }
        cycles.flush();
        packets.flush();
        std::cout << "Завершено. CSV: " << cfg.cycle_log << ", " << cfg.packet_log << "\n";
    } catch (const std::exception& e) {
        std::cerr << "Ошибка: " << e.what() << '\n';
        return 1;
    }
    return 0;
}
