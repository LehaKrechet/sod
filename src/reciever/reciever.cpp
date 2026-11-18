#include <iostream>
#include <cstring>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

int main()
{
    int sock = socket(AF_INET, SOCK_DGRAM, 0);

    if (sock < 0) {
        perror("socket");
        return 1;
    }

    int reuse = 1;

    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR,
               &reuse, sizeof(reuse));

    sockaddr_in local{};
    local.sin_family = AF_INET;
    local.sin_port = htons(5000);
    local.sin_addr.s_addr = INADDR_ANY;

    if (bind(sock, (sockaddr*)&local,
             sizeof(local)) < 0) {
        perror("bind");
        return 1;
    }

    ip_mreq mreq{};

    inet_pton(AF_INET, "239.10.10.10",
              &mreq.imr_multiaddr);

    mreq.imr_interface.s_addr = INADDR_ANY;

    if (setsockopt(sock, IPPROTO_IP,
                   IP_ADD_MEMBERSHIP,
                   &mreq, sizeof(mreq)) < 0) {
        perror("IP_ADD_MEMBERSHIP");
        return 1;
    }

    char buffer[1500];

    while (true) {
        ssize_t size = recvfrom(
            sock, buffer, sizeof(buffer) - 1,
            0, nullptr, nullptr);

        if (size > 0) {
            buffer[size] = '\0';

            std::cout << "Received: "
                      << buffer << std::endl;
        }
    }

    close(sock);
    return 0;
}