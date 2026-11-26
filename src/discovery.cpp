#include "discovery.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cstring>
#include <chrono>
#include <mutex>
#include <stdexcept>

// Короткий пакет обнаружения: маркер, версия и идентификатор узла.
#pragma pack(push, 1)
struct Beacon {
    uint32_t magic;
    uint16_t version;
    uint16_t pad;
    uint32_t id;
};
#pragma pack(pop)

static constexpr uint32_t BEACON_MAGIC = 0x534f4448;
static std::mutex peers_mutex;
static std::map<int, std::string> shared_peers;

Discovery::Discovery(const Config& config)
    : id(config.node_id), port(config.discovery_port),
      heartbeat(config.heartbeat_interval_ms), timeout(config.peer_timeout_ms),
      group(config.multicast_group), iface(config.interface_ip) {
    sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        throw std::runtime_error("discovery socket failed");
    }

    int yes = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes);

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(sock, reinterpret_cast<sockaddr*>(&address), sizeof address) < 0) {
        throw std::runtime_error("discovery bind failed");
    }

    ip_mreq membership{};
    membership.imr_multiaddr.s_addr = inet_addr(group.c_str());
    membership.imr_interface.s_addr = inet_addr(iface.c_str());

    if (setsockopt(sock, IPPROTO_IP, IP_ADD_MEMBERSHIP,
                   &membership, sizeof membership) < 0) {
        throw std::runtime_error("discovery join failed");
    }

    // После настройки сокета запускаем фоновую проверку соседей.
    th = std::thread([this] { run(); });
}

Discovery::~Discovery() {
    stop = true;
    if (sock >= 0) {
        shutdown(sock, SHUT_RDWR);
    }
    if (th.joinable()) {
        th.join();
    }
    if (sock >= 0) {
        close(sock);
    }
}

std::map<int, std::string> Discovery::peers() {
    std::lock_guard<std::mutex> lock(peers_mutex);
    return shared_peers;
}

void Discovery::run() {
    std::map<int, std::chrono::steady_clock::time_point> last_seen;

    sockaddr_in destination{};
    destination.sin_family = AF_INET;
    destination.sin_port = htons(port);
    destination.sin_addr.s_addr = inet_addr(group.c_str());

    sockaddr_in sender{};
    socklen_t sender_length = sizeof sender;
    char buffer[128];

    while (!stop) {
        // Отправляем маяк, чтобы другие компьютеры могли обнаружить этот узел.
        Beacon beacon{htonl(BEACON_MAGIC), htons(1), 0, htonl(static_cast<uint32_t>(id))};
        sendto(sock, &beacon, sizeof beacon, 0,
               reinterpret_cast<sockaddr*>(&destination), sizeof destination);

        // Не ждём бесконечно: таймаут соответствует интервалу маяков.
        timeval receive_timeout{};
        receive_timeout.tv_usec = heartbeat * 1000;
        setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO,
                   &receive_timeout, sizeof receive_timeout);

        const ssize_t received = recvfrom(sock, buffer, sizeof buffer, 0,
                                         reinterpret_cast<sockaddr*>(&sender),
                                         &sender_length);
        if (received == static_cast<ssize_t>(sizeof beacon)) {
            Beacon incoming{};
            memcpy(&incoming, buffer, sizeof incoming);

            if (ntohl(incoming.magic) == BEACON_MAGIC && ntohs(incoming.version) == 1) {
                const int peer_id = static_cast<int>(ntohl(incoming.id));
                if (peer_id != id) {
                    last_seen[peer_id] = std::chrono::steady_clock::now();
                    std::lock_guard<std::mutex> lock(peers_mutex);
                    shared_peers[peer_id] = inet_ntoa(sender.sin_addr);
                }
            }
        }

        // Если от узла давно не было маяков, убираем его из списка.
        const auto now = std::chrono::steady_clock::now();
        for (auto it = last_seen.begin(); it != last_seen.end();) {
            if (now - it->second > std::chrono::milliseconds(timeout)) {
                std::lock_guard<std::mutex> lock(peers_mutex);
                shared_peers.erase(it->first);
                it = last_seen.erase(it);
            } else {
                ++it;
            }
        }
    }
}
