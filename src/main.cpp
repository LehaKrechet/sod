#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <net/if.h>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <numeric>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

namespace {
using Clock = std::chrono::steady_clock;
constexpr uint32_t MAGIC = 0x534F4431; // "SOD1"
constexpr uint32_t VERSION = 1;
constexpr uint32_t HEARTBEAT = 1;
constexpr uint32_t REQUEST = 2;
constexpr uint32_t RESPONSE = 3;
constexpr double TARGET_US = 200.0;
constexpr auto PEER_TIMEOUT = std::chrono::seconds(3);

#pragma pack(push, 1)
struct Packet {
    uint32_t magic;
    uint32_t version;
    uint32_t type;
    uint32_t id_hi;
    uint32_t id_lo;
    uint32_t target_hi;
    uint32_t target_lo;
    uint32_t seq;
    uint32_t value;
};
#pragma pack(pop)

std::atomic<bool> running{true};

void on_signal(int) { running.store(false); }

uint64_t make_id() {
    std::random_device rd;
    uint64_t v = (static_cast<uint64_t>(rd()) << 32) ^ rd();
    v ^= (static_cast<uint64_t>(getpid()) << 16);
    v ^= static_cast<uint64_t>(
        std::chrono::high_resolution_clock::now().time_since_epoch().count());
    return v ? v : 1;
}

uint32_t hi(uint64_t v) { return static_cast<uint32_t>(v >> 32); }
uint32_t lo(uint64_t v) { return static_cast<uint32_t>(v & 0xffffffffu); }
uint64_t join_id(uint32_t h, uint32_t l) {
    return (static_cast<uint64_t>(h) << 32) | l;
}

Packet make_packet(uint32_t type, uint64_t id, uint64_t target,
                   uint32_t seq, uint32_t value = 0) {
    Packet p{};
    p.magic = htonl(MAGIC);
    p.version = htonl(VERSION);
    p.type = htonl(type);
    p.id_hi = htonl(hi(id));
    p.id_lo = htonl(lo(id));
    p.target_hi = htonl(hi(target));
    p.target_lo = htonl(lo(target));
    p.seq = htonl(seq);
    p.value = htonl(value);
    return p;
}

uint32_t field(uint32_t v) { return ntohl(v); }
uint64_t packet_id(const Packet& p) {
    return join_id(field(p.id_hi), field(p.id_lo));
}
uint64_t packet_target(const Packet& p) {
    return join_id(field(p.target_hi), field(p.target_lo));
}

struct Peer {
    Clock::time_point last_seen;
};

struct Config {
    std::string group = "239.0.0.1";
    std::string iface = "0.0.0.0";
    uint16_t port = 5000;
    unsigned interval_us = 10000;
};

class Node {
public:
    explicit Node(Config cfg) : cfg_(std::move(cfg)), id_(make_id()) {}

    bool init() {
        sock_ = ::socket(AF_INET, SOCK_DGRAM, 0);
        if (sock_ < 0) { perror("socket"); return false; }

        int yes = 1;
        if (setsockopt(sock_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0) {
            perror("SO_REUSEADDR"); return false;
        }

        timeval tv{};
        tv.tv_sec = 0;
        tv.tv_usec = 100000;
        setsockopt(sock_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        sockaddr_in local{};
        local.sin_family = AF_INET;
        local.sin_port = htons(cfg_.port);
        local.sin_addr.s_addr = htonl(INADDR_ANY);
        if (bind(sock_, reinterpret_cast<sockaddr*>(&local), sizeof(local)) < 0) {
            perror("bind"); return false;
        }

        ip_mreq mreq{};
        if (inet_pton(AF_INET, cfg_.group.c_str(), &mreq.imr_multiaddr) != 1) {
            std::cerr << "Некорректный multicast-адрес: " << cfg_.group << "\n";
            return false;
        }
        if (inet_pton(AF_INET, cfg_.iface.c_str(), &mreq.imr_interface) != 1) {
            std::cerr << "Некорректный IPv4 интерфейса: " << cfg_.iface << "\n";
            return false;
        }
        if (setsockopt(sock_, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0) {
            perror("IP_ADD_MEMBERSHIP"); return false;
        }

        unsigned char ttl = 1;
        unsigned char loop = 1;
        setsockopt(sock_, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl));
        setsockopt(sock_, IPPROTO_IP, IP_MULTICAST_LOOP, &loop, sizeof(loop));
        if (mreq.imr_interface.s_addr != htonl(INADDR_ANY)) {
            setsockopt(sock_, IPPROTO_IP, IP_MULTICAST_IF,
                       &mreq.imr_interface, sizeof(mreq.imr_interface));
        }

        dest_.sin_family = AF_INET;
        dest_.sin_port = htons(cfg_.port);
        if (inet_pton(AF_INET, cfg_.group.c_str(), &dest_.sin_addr) != 1) return false;
        return true;
    }

    void run() {
        std::signal(SIGINT, on_signal);
        std::signal(SIGTERM, on_signal);

        receiver_ = std::thread(&Node::receive_loop, this);
        auto next_heartbeat = Clock::now();
        auto next_cycle = Clock::now();
        auto next_draw = Clock::now();

        while (running.load()) {
            const auto now = Clock::now();

            if (now >= next_heartbeat) {
                send_packet(make_packet(HEARTBEAT, id_, 0, 0));
                next_heartbeat = now + std::chrono::seconds(1);
            }

            if (now >= next_cycle) {
                run_cycle();
                next_cycle = Clock::now() + std::chrono::microseconds(cfg_.interval_us);
            }

            if (now >= next_draw) {
                draw_dashboard();
                next_draw = now + std::chrono::milliseconds(250);
            }
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }

        if (receiver_.joinable()) receiver_.join();
        if (sock_ >= 0) close(sock_);
        std::cout << "\nЗавершение узла.\n";
    }

private:
    void send_packet(const Packet& p) {
        const auto n = sendto(sock_, &p, sizeof(p), 0,
                              reinterpret_cast<const sockaddr*>(&dest_), sizeof(dest_));
        (void)n;
    }

    void receive_loop() {
        while (running.load()) {
            Packet p{};
            sockaddr_in from{};
            socklen_t from_len = sizeof(from);
            const ssize_t n = recvfrom(sock_, &p, sizeof(p), 0,
                                       reinterpret_cast<sockaddr*>(&from), &from_len);
            if (n < static_cast<ssize_t>(sizeof(Packet))) continue;
            if (field(p.magic) != MAGIC || field(p.version) != VERSION) continue;

            const uint64_t sender = packet_id(p);
            if (sender == id_) continue; // multicast loopback: ignore own packet

            const uint32_t type = field(p.type);
            const auto now = Clock::now();

            if (type == HEARTBEAT || type == REQUEST || type == RESPONSE) {
                {
                    std::lock_guard<std::mutex> lock(state_mtx_);
                    peers_[sender].last_seen = now;
                }
            }

            if (type == REQUEST) {
                // Каждый узел отвечает непосредственно multicast-ответом.
                const uint32_t seq = field(p.seq);
                const uint32_t input = field(p.value);
                const uint32_t result = input + 1; // тестовая заглушка: 1 + 1
                send_packet(make_packet(RESPONSE, id_, sender, seq, result));
            } else if (type == RESPONSE && packet_target(p) == id_) {
                const uint32_t seq = field(p.seq);
                {
                    std::lock_guard<std::mutex> lock(cycle_mtx_);
                    if (pending_ && seq == pending_seq_) {
                        received_.insert(sender);
                    }
                }
                cycle_cv_.notify_one();
            }
        }
    }

    std::set<uint64_t> active_peers() {
        std::set<uint64_t> result;
        const auto now = Clock::now();
        std::lock_guard<std::mutex> lock(state_mtx_);
        for (auto it = peers_.begin(); it != peers_.end();) {
            if (now - it->second.last_seen > PEER_TIMEOUT) {
                it = peers_.erase(it);
            } else {
                result.insert(it->first);
                ++it;
            }
        }
        return result;
    }

    void run_cycle() {
        const uint32_t seq = ++sequence_;
        const auto expected = active_peers();
        const auto start = Clock::now();

        {
            std::lock_guard<std::mutex> lock(cycle_mtx_);
            pending_ = true;
            pending_seq_ = seq;
            expected_ = expected;
            received_.clear();
        }

        // value=1; обработчик каждого узла вычисляет 1+1 и возвращает 2.
        send_packet(make_packet(REQUEST, id_, 0, seq, 1));

        const auto deadline = start + std::chrono::microseconds(200);
        {
            std::unique_lock<std::mutex> lock(cycle_mtx_);
            cycle_cv_.wait_until(lock, deadline, [&] {
                return !running.load() ||
                       std::includes(received_.begin(), received_.end(),
                                     expected_.begin(), expected_.end());
            });
            pending_ = false;
        }

        const auto finish = Clock::now();
        const double elapsed = std::chrono::duration<double, std::micro>(finish - start).count();

        size_t got = 0;
        {
            std::lock_guard<std::mutex> lock(cycle_mtx_);
            got = received_.size();
        }
        const size_t wanted = expected.size();
        const bool complete = (got >= wanted);
        {
            std::lock_guard<std::mutex> lock(stats_mtx_);
            samples_.push_back(elapsed);
            if (samples_.size() > 80) samples_.pop_front();
            ++cycles_;
            if (!complete) ++missed_cycles_;
            if (elapsed > TARGET_US) ++over_limit_;
            last_us_ = elapsed;
            last_expected_ = wanted;
            last_received_ = got;
            last_complete_ = complete;
        }
    }

    void draw_dashboard() {
        std::vector<double> samples;
        uint64_t cycles, over, missed;
        double last;
        size_t expected, received;
        bool complete;
        {
            std::lock_guard<std::mutex> lock(stats_mtx_);
            samples.assign(samples_.begin(), samples_.end());
            cycles = cycles_;
            over = over_limit_;
            missed = missed_cycles_;
            last = last_us_;
            expected = last_expected_;
            received = last_received_;
            complete = last_complete_;
        }
        auto peers = active_peers();

        double avg = 0, minv = 0, maxv = 0;
        if (!samples.empty()) {
            avg = std::accumulate(samples.begin(), samples.end(), 0.0) / samples.size();
            auto mm = std::minmax_element(samples.begin(), samples.end());
            minv = *mm.first;
            maxv = *mm.second;
        }

        std::cout << "\033[2J\033[H";
        std::cout << "============================================================\n";
        std::cout << "             ПО «СОД» — P2P UDP MULTICAST MONITOR\n";
        std::cout << "============================================================\n";
        std::cout << "Узел ID: " << std::hex << id_ << std::dec
                  << "   Группа: " << cfg_.group << ":" << cfg_.port
                  << "   Интервал: " << cfg_.interval_us << " мкс\n";
        std::cout << "Режим: все узлы равноправны; центрального координатора нет.\n\n";
        std::cout << "Циклов: " << cycles << "   Активных соседей: " << peers.size()
                  << "   Последний обмен: " << received << "/" << expected
                  << (complete ? " [OK]" : " [TIMEOUT]") << "\n";
        std::cout << std::fixed << std::setprecision(2);
        std::cout << "Последний цикл: " << last << " мкс"
                  << "   Среднее: " << avg << " мкс"
                  << "   Мин: " << minv << " мкс"
                  << "   Макс: " << maxv << " мкс\n";
        std::cout << "Превышений 200 мкс: " << over
                  << "   Циклов без всех ответов: " << missed << "\n\n";

        std::cout << "ДИАГРАММА ВРЕМЕНИ ПОСЛЕДНИХ ЦИКЛОВ (каждый # ≈ 5 мкс)\n";
        std::cout << "250 |";
        for (size_t i = 0; i < samples.size(); ++i) std::cout << ' ';
        std::cout << "\n200 |";
        for (double v : samples) {
            int bars = std::min(50, static_cast<int>(v / 5.0));
            for (int j = 0; j < bars; ++j) std::cout << '#';
            if (v > TARGET_US) std::cout << '!';
            std::cout << "\n    |";
        }
        std::cout << "\n     ";
        for (size_t i = 0; i < samples.size(); ++i) std::cout << "-";
        std::cout << "\n     старые ------------------------------------> новые\n\n";

        std::cout << "УЗЛЫ В СЕТИ (таймаут обнаружения: 3 секунды)\n";
        if (peers.empty()) std::cout << "  Пока нет обнаруженных соседей. Запустите программу на другом ПК.\n";
        else {
            std::lock_guard<std::mutex> lock(state_mtx_);
            for (uint64_t peer : peers) {
                auto it = peers_.find(peer);
                if (it != peers_.end()) {
                    auto age = std::chrono::duration_cast<std::chrono::milliseconds>(
                        Clock::now() - it->second.last_seen).count();
                    std::cout << "  Узел " << std::hex << peer << std::dec
                              << " — доступен, последнее сообщение " << age << " мс назад\n";
                }
            }
        }
        std::cout << "\nCtrl+C — выход\n";
        std::cout.flush();
    }

    Config cfg_;
    uint64_t id_;
    int sock_ = -1;
    sockaddr_in dest_{};
    std::thread receiver_;
    std::atomic<uint32_t> sequence_{0};

    std::mutex state_mtx_;
    std::unordered_map<uint64_t, Peer> peers_;

    std::mutex cycle_mtx_;
    std::condition_variable cycle_cv_;
    bool pending_ = false;
    uint32_t pending_seq_ = 0;
    std::set<uint64_t> expected_;
    std::set<uint64_t> received_;

    std::mutex stats_mtx_;
    std::deque<double> samples_;
    uint64_t cycles_ = 0;
    uint64_t over_limit_ = 0;
    uint64_t missed_cycles_ = 0;
    double last_us_ = 0;
    size_t last_expected_ = 0;
    size_t last_received_ = 0;
    bool last_complete_ = false;
};

void usage(const char* name) {
    std::cout << "Использование: " << name << " [параметры]\n"
              << "  --group IPv4       Multicast-группа (по умолчанию 239.0.0.1)\n"
              << "  --port PORT        UDP-порт (по умолчанию 5000)\n"
              << "  --iface IPv4       IPv4 сетевого интерфейса (по умолчанию 0.0.0.0)\n"
              << "  --interval-us N    Пауза между циклами, мкс (по умолчанию 10000)\n"
              << "  --help             Показать эту справку\n";
}
}

int main(int argc, char** argv) {
    Config cfg;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--help") { usage(argv[0]); return 0; }
        else if (a == "--group" && i + 1 < argc) cfg.group = argv[++i];
        else if (a == "--iface" && i + 1 < argc) cfg.iface = argv[++i];
        else if (a == "--port" && i + 1 < argc) {
            int p = std::stoi(argv[++i]);
            if (p < 1 || p > 65535) { std::cerr << "Некорректный порт\n"; return 2; }
            cfg.port = static_cast<uint16_t>(p);
        } else if (a == "--interval-us" && i + 1 < argc) {
            int v = std::stoi(argv[++i]);
            if (v < 1000 || v > 10000000) {
                std::cerr << "Интервал должен быть от 1000 до 10000000 мкс\n";
                return 2;
            }
            cfg.interval_us = static_cast<unsigned>(v);
        } else {
            std::cerr << "Неизвестный параметр: " << a << "\n";
            usage(argv[0]);
            return 2;
        }
    }

    Node node(cfg);
    if (!node.init()) return 1;
    std::cout << "Запуск узла ПО «Сод»...\n";
    node.run();
    return 0;
}
