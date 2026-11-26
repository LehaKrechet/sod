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
    // Если IP интерфейса задан в config.ini, слушаем multicast через него.
    // 0.0.0.0 оставляет выбор интерфейса операционной системе.
    membership.imr_interface.s_addr = inet_addr(iface.c_str());

    if (setsockopt(sock, IPPROTO_IP, IP_ADD_MEMBERSHIP,
                   &membership, sizeof membership) < 0) {
        throw std::runtime_error(std::string("discovery join failed for group ") + group + " on interface " + iface + ": " + std::strerror(errno));
    }

    // Указываем тот же интерфейс и для исходящих multicast-маяков.
    if (iface != "0.0.0.0") {
        in_addr outgoing_interface{};
        outgoing_interface.s_addr = inet_addr(iface.c_str());
        if (setsockopt(sock, IPPROTO_IP, IP_MULTICAST_IF,
                       &outgoing_interface, sizeof outgoing_interface) < 0) {
            throw std::runtime_error("discovery multicast interface failed");
        }
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

    char buffer[128];

    while (!stop) {
        // Периодически отправляем маяк, сообщая соседям, что узел активен.
        Beacon beacon{htonl(BEACON_MAGIC), htons(1), 0,
                      htonl(static_cast<uint32_t>(id))};
        sendto(sock, &beacon, sizeof beacon, 0,
               reinterpret_cast<sockaddr*>(&destination), sizeof destination);

        // За этот интервал принимаем все доступные маяки, а не только один.
        timeval receive_timeout{};
        receive_timeout.tv_sec = heartbeat / 1000;
        receive_timeout.tv_usec = (heartbeat % 1000) * 1000;
        setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO,
                   &receive_timeout, sizeof receive_timeout);

        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(heartbeat);
        while (!stop && std::chrono::steady_clock::now() < deadline) {
            sockaddr_in sender{};
            socklen_t sender_length = sizeof sender;
            const ssize_t received = recvfrom(sock, buffer, sizeof buffer, 0,
                                             reinterpret_cast<sockaddr*>(&sender),
                                             &sender_length);
            if (received < 0) {
                break; // Таймаут — пора отправить следующий маяк.
            }
            if (received != static_cast<ssize_t>(sizeof(Beacon))) {
                continue;
            }

            Beacon incoming{};
            memcpy(&incoming, buffer, sizeof incoming);
            if (ntohl(incoming.magic) != BEACON_MAGIC || ntohs(incoming.version) != 1) {
                continue;
            }

            const int peer_id = static_cast<int>(ntohl(incoming.id));
            if (peer_id == id) {
                continue; // Игнорируем собственный маяк.
            }

            last_seen[peer_id] = std::chrono::steady_clock::now();
            char address_text[INET_ADDRSTRLEN]{};
            inet_ntop(AF_INET, &sender.sin_addr, address_text, sizeof address_text);
            {
                std::lock_guard<std::mutex> lock(peers_mutex);
                shared_peers[peer_id] = address_text;
            }
        }

        // Удаляем узлы, от которых больше не приходят маяки.
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
