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

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(5000);

    inet_pton(AF_INET, "239.10.10.10",
              &addr.sin_addr);

    const char* message = "Hello Multicast!";

    while (true) {
        sendto(sock,
               message,
               strlen(message),
               0,
               (sockaddr*)&addr,
               sizeof(addr));

        std::cout << "Message sent: "
                  << message << std::endl;

        sleep(1);
    }

    close(sock);
    return 0;
}