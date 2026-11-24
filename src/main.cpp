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

static void print_dashboard(uint64_t seq, double send_us, double receive_us,
                           double compute_us, double total_us, unsigned width,
                           const std::vector<std::pair<uint32_t, std::string>>& peers,
                           uint32_t local_node_id, bool redraw) {
    constexpr double LIMIT_US = 200.0;
    constexpr unsigned PEER_ROWS = 8;
    constexpr unsigned DASHBOARD_ROWS = 5 + 1 + PEER_ROWS;
    if (redraw) std::cout << "\033[" << DASHBOARD_ROWS << "A";
    const auto line = [](const std::string& text) { std::cout << "\033[2K" << text << "\n"; };
    line("Цикл " + std::to_string(seq) + " | шкала: 0..200 мкс");
    std::ostringstream tx, rx, cpu, all;
    tx << " TX   " << bar(send_us, LIMIT_US, width) << ' ' << std::fixed << std::setprecision(2) << send_us << " мкс";
    rx << " RX   " << bar(receive_us, LIMIT_US, width) << ' ' << std::fixed << std::setprecision(2) << receive_us << " мкс";
    cpu << " CPU  " << bar(compute_us, LIMIT_US, width) << ' ' << std::fixed << std::setprecision(2) << compute_us << " мкс";
    all << " ALL  " << bar(total_us, LIMIT_US, width) << ' ' << std::fixed << std::setprecision(2) << total_us << " мкс";
    if (total_us > LIMIT_US) all << "  <--- ПРЕВЫШЕНИЕ 200 мкс";
    line(tx.str()); line(rx.str()); line(cpu.str()); line(all.str());
    line("Активные устройства (обновление по heartbeat): включая node_id=" + std::to_string(local_node_id));
    std::ostringstream peers_line;
    peers_line << "Всего: " << peers.size() + 1 << " | удалённых: " << peers.size();
    for (const auto& peer : peers) peers_line << " | " << peer.first << " (" << peer.second << ')';
    line(peers_line.str());
    for (unsigned i = 0; i < PEER_ROWS - 1; ++i) line("");
    std::cout.flush();
}

int main(int argc, char** argv) {
    try {
        const std::string config_path = argc > 1 ? argv[1] : "config.ini";
        const Config cfg = load_config(config_path);
        const bool terminal_graph = config_bool(config_path, "terminal_graph", false);
        const unsigned graph_width = std::max(10U, std::min(100U,
            config_uint(config_path, "graph_width", 50)));
        const unsigned graph_refresh_ms = std::max(20U, config_uint(config_path, "graph_refresh_ms", 100));
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
                // При активном dashboard список peer отображается в фиксированной панели.
                if (terminal_graph) continue;
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
        bool graph_drawn = false;
        auto next_graph_refresh = std::chrono::steady_clock::now();
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

            // Состав сети отображается в панели графика, без вывода в новую строку.
            const auto status_now = std::chrono::steady_clock::now();
            if (status_now >= next_peer_status) {
                print_peer_events();
                next_peer_status = status_now + std::chrono::milliseconds(cfg.peer_status_interval_ms);
            }
            if (terminal_graph && std::chrono::steady_clock::now() >= next_graph_refresh) {
                print_dashboard(seq, send_us, receive_us, compute_us, total_us, graph_width,
                               discovery.peers(), cfg.node_id, graph_drawn);
                graph_drawn = true;
                next_graph_refresh = std::chrono::steady_clock::now() + std::chrono::milliseconds(graph_refresh_ms);
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
