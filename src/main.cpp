#include "config.hpp"
#include "network.hpp"
#include "discovery.hpp"

#include <chrono>
#include <thread>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <random>
#include <map>
#include <vector>
#include <cstring>
#include <algorithm>
#include <numeric>
#include <string>

using Clock = std::chrono::steady_clock;

// Считаем разницу между двумя отметками времени в микросекундах.
static double us(Clock::time_point start, Clock::time_point end) {
    return std::chrono::duration<double, std::micro>(end - start).count();
}

// Выводит статистику и гистограмму последних измерений обмена.
static void histogram(const std::vector<double>& values, int width,
                     const std::map<int, std::string>& peers) {
    // Важный для нас участок 100-200 мкс разбит на интервалы по 10 мкс.
    // На остальных участках оставлены более широкие интервалы, чтобы
    // гистограмма не занимала слишком много места по высоте.
    const double edges[] = {
        50, 100,
        110, 120, 130, 140, 150, 160, 170, 180, 190, 200,
        250, 300, 400
    };
    const char* labels[] = {
        "0-50", "50-100",
        "100-110", "110-120", "120-130", "130-140", "140-150",
        "150-160", "160-170", "170-180", "180-190", "190-200",
        "200-250", "250-300", "300-400", ">400"
    };

    constexpr int bin_count = 16;
    int bins[bin_count] = {};
    for (double value : values) {
        int bin = 0;
        while (bin < bin_count - 1 && value >= edges[bin]) {
            ++bin;
        }
        ++bins[bin];
    }

    // Самый заполненный интервал задаёт масштаб длины полос.
    int max_count = *std::max_element(std::begin(bins), std::end(bins));
    if (max_count < 1) {
        max_count = 1;
    }

    const double average = values.empty()
        ? 0.0
        : std::accumulate(values.begin(), values.end(), 0.0) / values.size();

    std::cout << "Average ALL: " << std::fixed << std::setprecision(1)
              << average << " us | samples: " << values.size()
              << " | threshold: 200 us\n";

    std::cout << "Connected devices: "
              << (peers.empty() ? "(none)" : "") << "\n";
    for (const auto& peer : peers) {
        std::cout << "  Node " << peer.first << " @ " << peer.second << "\n";
    }

    std::cout << "\nDynamic histogram | last " << values.size() << " cycles\n";
    for (int i = 0; i < bin_count; ++i) {
        const int bar_length = static_cast<int>(
            static_cast<double>(bins[i]) / max_count * width);
        std::cout << std::setw(8) << labels[i] << " us | "
                  << std::string(bar_length, '#') << " (" << bins[i] << " )\n";
    }
}

int main(int argc, char* argv[]) {
    try {
        // Можно передать путь к конфигурации аргументом; иначе ищется config.ini.
        const std::string config_path = (argc > 1) ? argv[1] : "config.ini";
        Config config = load_config(config_path);

        std::cout << "SOD multicast node=" << config.node_id
                  << " payload=" << config.payload_size_bytes
                  << " bytes chunk=" << config.chunk_payload_bytes << "\n";

        DataNetwork network(config);
        Discovery discovery(config);
        std::ofstream cycles_file;
        std::ofstream packets_file;

        // CSV нужен для последующего анализа измерений, но не для отображения.
        if (config.csv_logging) {
            cycles_file.open("timing_cycles.csv");
            cycles_file << "seq,payload_bytes,tx_us,rx_us,compute_us,total_us,"
                          "peers_received,expected_remote,over_200\n";

            packets_file.open("timing_packets.csv");
            packets_file << "seq,node_id,chunk_index,chunk_count\n";
        }

        // Для теста создаём буфер данных с постоянным размером из конфигурации.
        std::vector<uint8_t> payload(config.payload_size_bytes);
        std::mt19937 random_generator(static_cast<unsigned>(config.node_id));
        for (auto& byte : payload) {
            byte = static_cast<uint8_t>(random_generator());
        }

        uint64_t sequence = 0;
        auto last_dashboard_update = Clock::now();
        std::vector<double> history;
        history.reserve(1000);
        const int remote_expected = config.expected_nodes - 1;

        while (true) {
            ++sequence;
            const auto cycle_start = Clock::now();

            // Сначала отправляем данные, затем ждём фрагменты от остальных узлов.
            network.send_message(sequence, payload, config.node_id);
            const auto tx_end = Clock::now();

            std::map<int, std::vector<uint8_t>> assembled;
            std::map<int, std::vector<bool>> received_chunks;
            std::map<int, uint16_t> chunk_counts;

            const auto deadline = cycle_start +
                std::chrono::microseconds(config.receive_timeout_us);

            // Сколько узлов уже передали все фрагменты текущего цикла.
            auto fully_received = [&]() {
                int complete_nodes = 0;
                for (const auto& entry : received_chunks) {
                    if (std::all_of(entry.second.begin(), entry.second.end(),
                                    [](bool received) { return received; })) {
                        ++complete_nodes;
                    }
                }
                return complete_nodes;
            };

            while (Clock::now() < deadline && fully_received() < remote_expected) {
                int node;
                uint64_t received_sequence;
                uint32_t total_size;
                uint16_t chunk_index, chunk_count;
                std::vector<uint8_t> chunk;

                const int remaining_us = static_cast<int>(
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        deadline - Clock::now()).count());

                if (!network.receive_chunk(remaining_us, node, received_sequence,
                                          total_size, chunk_index, chunk_count, chunk)) {
                    continue;
                }

                // Не принимаем собственные пакеты, чужие циклы и неподходящий размер.
                if (node == config.node_id || received_sequence != sequence ||
                    total_size != config.payload_size_bytes ||
                    chunk_index >= chunk_count || chunk_count == 0) {
                    continue;
                }

                // Для нового узла заранее выделяем буфер и отмечаем фрагменты.
                if (!chunk_counts.count(node)) {
                    chunk_counts[node] = chunk_count;
                    assembled[node].resize(total_size);
                    received_chunks[node].assign(chunk_count, false);
                }

                if (chunk_counts[node] != chunk_count ||
                    chunk_index >= received_chunks[node].size()) {
                    continue;
                }

                const size_t offset = static_cast<size_t>(chunk_index) *
                                      config.chunk_payload_bytes;
                if (offset + chunk.size() > assembled[node].size()) {
                    continue;
                }

                memcpy(assembled[node].data() + offset, chunk.data(), chunk.size());
                received_chunks[node][chunk_index] = true;
            }

            const auto rx_end = Clock::now();
            int complete_nodes = 0;

            // Считаем узлы, у которых получены все фрагменты, и записываем CSV.
            for (const auto& entry : received_chunks) {
                if (std::all_of(entry.second.begin(), entry.second.end(),
                                [](bool received) { return received; })) {
                    ++complete_nodes;
                    if (config.csv_logging) {
                        for (uint16_t i = 0; i < chunk_counts[entry.first]; ++i) {
                            packets_file << sequence << "," << entry.first << ","
                                        << i << "," << chunk_counts[entry.first] << "\n";
                        }
                    }
                }
            }

            // Здесь пока стоит простая заглушка вместо полезной обработки данных.
            volatile int result = 1 + 1;
            (void)result;
            const auto compute_end = Clock::now();

            const double tx_time = us(cycle_start, tx_end);
            const double rx_time = us(tx_end, rx_end);
            const double compute_time = us(rx_end, compute_end);
            const double total_time = us(cycle_start, compute_end);
            const bool over_limit = total_time > 200.0;

            history.push_back(total_time);
            if (history.size() > 1000) {
                history.erase(history.begin());
            }

            if (config.csv_logging) {
                cycles_file << sequence << "," << config.payload_size_bytes << ","
                            << tx_time << "," << rx_time << "," << compute_time << ","
                            << total_time << "," << complete_nodes << ","
                            << remote_expected << "," << (over_limit ? 1 : 0) << "\n";
            }

            // Обновляем терминал не на каждом цикле, а с заданной частотой.
            if (config.terminal_graph &&
                us(last_dashboard_update, Clock::now()) >=
                    config.graph_refresh_ms * 1000.0) {
                last_dashboard_update = Clock::now();
                std::cout << "\033[2J\033[H";
                histogram(history, config.graph_width, discovery.peers());
                std::cout << std::flush;
            }

            // Если задан период, ждём до начала следующего цикла.
            if (config.cycle_period_us > 0) {
                const auto next_cycle = cycle_start +
                    std::chrono::microseconds(config.cycle_period_us);
                if (Clock::now() < next_cycle) {
                    std::this_thread::sleep_until(next_cycle);
                }
            }
        }
    } catch (const std::exception& error) {
        std::cerr << "ERROR: " << error.what() << "\n";
        return 1;
    }
}
