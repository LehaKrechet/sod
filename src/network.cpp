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
#pragma pack(push,1)
struct Header { uint32_t magic; uint16_t ver; uint16_t reserved; uint32_t node; uint64_t seq; uint32_t total; uint16_t idx; uint16_t count; uint16_t len; };
#pragma pack(pop)
static constexpr uint32_t MAGIC=0x534f4432; // SOD2
static uint64_t htonll(uint64_t x){
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
 return ((uint64_t)htonl((uint32_t)x)<<32)|htonl((uint32_t)(x>>32));
#else
 return x;
#endif
}
static uint64_t ntohll(uint64_t x){return htonll(x);}
DataNetwork::DataNetwork(const Config&c):group(c.multicast_group),port(c.data_port),iface(c.interface_ip),nodeid(c.node_id),chunksize(c.chunk_payload_bytes){ sock=socket(AF_INET,SOCK_DGRAM,0); if(sock<0)throw std::runtime_error("data socket failed"); int yes=1; setsockopt(sock,SOL_SOCKET,SO_REUSEADDR,&yes,sizeof yes); sockaddr_in local{};local.sin_family=AF_INET;local.sin_port=htons(port);local.sin_addr.s_addr=htonl(INADDR_ANY); if(bind(sock,(sockaddr*)&local,sizeof local)<0)throw std::runtime_error("data bind failed"); ip_mreq m{};m.imr_multiaddr.s_addr=inet_addr(group.c_str());m.imr_interface.s_addr=inet_addr(iface.c_str()); if(setsockopt(sock,IPPROTO_IP,IP_ADD_MEMBERSHIP,&m,sizeof m)<0)throw std::runtime_error("data multicast join failed"); if(iface!="0.0.0.0")setsockopt(sock,IPPROTO_IP,IP_MULTICAST_IF,&m.imr_interface,sizeof m.imr_interface); }
DataNetwork::~DataNetwork(){if(sock>=0)close(sock);}
void DataNetwork::send_message(uint64_t seq,const std::vector<uint8_t>& data,int node_id){ sockaddr_in dst{};dst.sin_family=AF_INET;dst.sin_port=htons(port);dst.sin_addr.s_addr=inet_addr(group.c_str()); size_t chunkmax=chunksize;
 uint16_t count=(uint16_t)((data.size()+chunkmax-1)/chunkmax); for(uint16_t i=0;i<count;i++){size_t off=(size_t)i*chunkmax,len=std::min(chunkmax,data.size()-off); Header h{};h.magic=htonl(MAGIC);h.ver=htons(2);h.node=htonl((uint32_t)node_id);h.seq=htonll(seq);h.total=htonl((uint32_t)data.size());h.idx=htons(i);h.count=htons(count);h.len=htons((uint16_t)len); std::vector<uint8_t> packet(sizeof h+len);memcpy(packet.data(),&h,sizeof h);memcpy(packet.data()+sizeof h,data.data()+off,len);sendto(sock,packet.data(),packet.size(),0,(sockaddr*)&dst,sizeof dst);}}
bool DataNetwork::receive_chunk(int timeout,int&node,uint64_t&seq,uint32_t&total,uint16_t&idx,uint16_t&count,std::vector<uint8_t>&chunk){pollfd p{sock,POLLIN,0};timespec ts{timeout/1000000,(timeout%1000000)*1000};int r=ppoll(&p,1,&ts,nullptr);if(r<=0)return false;uint8_t buf[1500];ssize_t n=recv(sock,buf,sizeof buf,0);if(n<(ssize_t)sizeof(Header))return false;Header h;memcpy(&h,buf,sizeof h);if(ntohl(h.magic)!=MAGIC||ntohs(h.ver)!=2)return false;node=(int)ntohl(h.node);seq=ntohll(h.seq);total=ntohl(h.total);idx=ntohs(h.idx);count=ntohs(h.count);uint16_t len=ntohs(h.len);if(len!=(size_t)n-sizeof h)return false;chunk.assign(buf+sizeof h,buf+n);return true;}
