#include "network.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <poll.h>
#include <time.h>
#include <cstring>
#include <stdexcept>
#include <algorithm>

// Заголовок добавляется к каждой части сообщения, чтобы получатель мог
// определить узел-отправитель, номер цикла и положение фрагмента.
#pragma pack(push, 1)
struct Header {
    uint32_t magic;
    uint16_t ver;
    uint16_t reserved;
    uint32_t node;
    uint64_t seq;
    uint32_t total;
    uint16_t idx;
    uint16_t count;
    uint16_t len;
};
#pragma pack(pop)

static constexpr uint32_t MAGIC = 0x534f4432; // Маркер протокола SOD2.

// В сетевом заголовке 64-битное число тоже должно быть в сетевом порядке байт.
static uint64_t htonll(uint64_t value) {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return (static_cast<uint64_t>(htonl(static_cast<uint32_t>(value))) << 32) |
           htonl(static_cast<uint32_t>(value >> 32));
#else
    return value;
#endif
}

static uint64_t ntohll(uint64_t value) {
    return htonll(value);
}

DataNetwork::DataNetwork(const Config& config)
    : group(config.multicast_group), port(config.data_port),
      iface(config.interface_ip), nodeid(config.node_id),
      chunksize(config.chunk_payload_bytes) {
    sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        throw std::runtime_error("data socket failed");
    }

    int yes = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes);

    // Слушаем заданный UDP-порт на всех локальных адресах.
    sockaddr_in local{};
    local.sin_family = AF_INET;
    local.sin_port = htons(port);
    local.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(sock, reinterpret_cast<sockaddr*>(&local), sizeof local) < 0) {
        throw std::runtime_error("data bind failed");
    }

    // Подписываем сокет на multicast-группу.
    ip_mreq membership{};
    membership.imr_multiaddr.s_addr = inet_addr(group.c_str());
    membership.imr_interface.s_addr = inet_addr(iface.c_str());

    if (setsockopt(sock, IPPROTO_IP, IP_ADD_MEMBERSHIP,
                   &membership, sizeof membership) < 0) {
        throw std::runtime_error("data multicast join failed");
    }

    // Если интерфейс задан явно, отправляем multicast через него.
    if (iface != "0.0.0.0") {
        setsockopt(sock, IPPROTO_IP, IP_MULTICAST_IF,
                   &membership.imr_interface, sizeof membership.imr_interface);
    }
}

DataNetwork::~DataNetwork() {
    if (sock >= 0) {
        close(sock);
    }
}

void DataNetwork::send_message(uint64_t seq, const std::vector<uint8_t>& data,
                               int node_id) {
    sockaddr_in destination{};
    destination.sin_family = AF_INET;
    destination.sin_port = htons(port);
    destination.sin_addr.s_addr = inet_addr(group.c_str());

    const size_t chunk_max = chunksize;
    const uint16_t chunk_count = static_cast<uint16_t>(
        (data.size() + chunk_max - 1) / chunk_max);
    uint8_t packet[1500];

    // Большое сообщение отправляется несколькими UDP-пакетами.
    for (uint16_t i = 0; i < chunk_count; ++i) {
        const size_t offset = static_cast<size_t>(i) * chunk_max;
        const size_t length = std::min(chunk_max, data.size() - offset);

        Header header{};
        header.magic = htonl(MAGIC);
        header.ver = htons(2);
        header.node = htonl(static_cast<uint32_t>(node_id));
        header.seq = htonll(seq);
        header.total = htonl(static_cast<uint32_t>(data.size()));
        header.idx = htons(i);
        header.count = htons(chunk_count);
        header.len = htons(static_cast<uint16_t>(length));

        memcpy(packet, &header, sizeof header);
        memcpy(packet + sizeof header, data.data() + offset, length);
        sendto(sock, packet, sizeof header + length, 0,
               reinterpret_cast<sockaddr*>(&destination), sizeof destination);
    }
}

bool DataNetwork::receive_chunk(int timeout_us, int& node, uint64_t& seq,
                               uint32_t& total, uint16_t& idx, uint16_t& count,
                               std::vector<uint8_t>& chunk) {
    // ppoll позволяет задать короткий таймаут в микросекундах.
    pollfd descriptor{sock, POLLIN, 0};
    timespec timeout{};
    timeout.tv_sec = timeout_us / 1000000;
    timeout.tv_nsec = (timeout_us % 1000000) * 1000;

    const int result = ppoll(&descriptor, 1, &timeout, nullptr);
    if (result <= 0) {
        return false;
    }

    uint8_t buffer[1500];
    const ssize_t received = recv(sock, buffer, sizeof buffer, 0);
    if (received < static_cast<ssize_t>(sizeof(Header))) {
        return false;
    }

    Header header{};
    memcpy(&header, buffer, sizeof header);

    // Отбрасываем пакеты, которые не относятся к нашему протоколу.
    if (ntohl(header.magic) != MAGIC || ntohs(header.ver) != 2) {
        return false;
    }

    node = static_cast<int>(ntohl(header.node));
    seq = ntohll(header.seq);
    total = ntohl(header.total);
    idx = ntohs(header.idx);
    count = ntohs(header.count);
    const uint16_t length = ntohs(header.len);

    if (length != static_cast<size_t>(received) - sizeof header) {
        return false;
    }

    chunk.assign(buffer + sizeof header, buffer + received);
    return true;
}
