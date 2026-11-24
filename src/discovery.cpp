#include "discovery.hpp"
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cstring>
#include <chrono>
#include <mutex>
#include <stdexcept>
#pragma pack(push,1)
struct Beacon{uint32_t magic;uint16_t version;uint16_t pad;uint32_t id;};
#pragma pack(pop)
static constexpr uint32_t BM=0x534f4448; static std::mutex pm; static std::map<int,std::string> sharedpeers;
Discovery::Discovery(const Config&c):id(c.node_id),port(c.discovery_port),heartbeat(c.heartbeat_interval_ms),timeout(c.peer_timeout_ms),group(c.multicast_group),iface(c.interface_ip){sock=socket(AF_INET,SOCK_DGRAM,0);if(sock<0)throw std::runtime_error("discovery socket failed");int yes=1;setsockopt(sock,SOL_SOCKET,SO_REUSEADDR,&yes,sizeof yes);sockaddr_in a{};a.sin_family=AF_INET;a.sin_port=htons(port);a.sin_addr.s_addr=htonl(INADDR_ANY);if(bind(sock,(sockaddr*)&a,sizeof a)<0)throw std::runtime_error("discovery bind failed");ip_mreq m{};m.imr_multiaddr.s_addr=inet_addr(group.c_str());m.imr_interface.s_addr=inet_addr(iface.c_str());if(setsockopt(sock,IPPROTO_IP,IP_ADD_MEMBERSHIP,&m,sizeof m)<0)throw std::runtime_error("discovery join failed");th=std::thread([this]{run();});}
Discovery::~Discovery(){stop=true;if(sock>=0)shutdown(sock,SHUT_RDWR);if(th.joinable())th.join();if(sock>=0)close(sock);}
std::map<int,std::string> Discovery::peers(){std::lock_guard<std::mutex>l(pm);return sharedpeers;}
void Discovery::run(){std::map<int,std::chrono::steady_clock::time_point>seen; sockaddr_in dst{};dst.sin_family=AF_INET;dst.sin_port=htons(port);dst.sin_addr.s_addr=inet_addr(group.c_str());sockaddr_in from{};socklen_t fl=sizeof from;char buf[128];while(!stop){Beacon b{htonl(BM),htons(1),0,htonl((uint32_t)id)};sendto(sock,&b,sizeof b,0,(sockaddr*)&dst,sizeof dst);timeval tv{0,heartbeat*1000};setsockopt(sock,SOL_SOCKET,SO_RCVTIMEO,&tv,sizeof tv);ssize_t n=recvfrom(sock,buf,sizeof buf,0,(sockaddr*)&from,&fl);if(n==(ssize_t)sizeof b){Beacon q;memcpy(&q,buf,sizeof q);if(ntohl(q.magic)==BM&&ntohs(q.version)==1){int peer=ntohl(q.id);if(peer!=id){seen[peer]=std::chrono::steady_clock::now();std::lock_guard<std::mutex>l(pm);sharedpeers[peer]=inet_ntoa(from.sin_addr);}}}auto now=std::chrono::steady_clock::now();for(auto it=seen.begin();it!=seen.end();)if(now-it->second>std::chrono::milliseconds(timeout)){std::lock_guard<std::mutex>l(pm);sharedpeers.erase(it->first);it=seen.erase(it);}else ++it;}}
