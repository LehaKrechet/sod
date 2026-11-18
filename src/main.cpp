#include <arpa/inet.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdint>
#include <deque>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <numeric>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <sys/socket.h>
#include <unistd.h>

namespace {
using Clock = std::chrono::steady_clock;
constexpr uint32_t MAGIC=0x534F4431, VERSION=1, HEARTBEAT=1, REQUEST=2, RESPONSE=3;
constexpr double LIMIT_US=200.0;
constexpr auto PEER_TIMEOUT=std::chrono::seconds(3);
#pragma pack(push,1)
struct Packet { uint32_t magic,version,type,id_hi,id_lo,target_hi,target_lo,seq,value; };
#pragma pack(pop)
std::atomic<bool> running{true};
void stop(int){running=false;}
uint64_t make_id(){std::random_device r; uint64_t x=(uint64_t(r())<<32)^r()^(uint64_t(getpid())<<16)^Clock::now().time_since_epoch().count(); return x?x:1;}
uint32_t hi(uint64_t x){return uint32_t(x>>32);} uint32_t lo(uint64_t x){return uint32_t(x);}
uint64_t join(uint32_t h,uint32_t l){return (uint64_t(h)<<32)|l;}
uint32_t H(uint32_t x){return ntohl(x);} uint32_t N(uint32_t x){return htonl(x);}
Packet make_packet(uint32_t t,uint64_t id,uint64_t target,uint32_t seq,uint32_t val=0){return {N(MAGIC),N(VERSION),N(t),N(hi(id)),N(lo(id)),N(hi(target)),N(lo(target)),N(seq),N(val)};}
uint64_t sender(const Packet&p){return join(H(p.id_hi),H(p.id_lo));}
uint64_t target(const Packet&p){return join(H(p.target_hi),H(p.target_lo));}
struct Config{std::string group="239.0.0.1",iface="0.0.0.0"; uint16_t port=5000; unsigned interval_us=1000;};
struct Peer{Clock::time_point seen;};
class Node{
public:
 explicit Node(Config c):cfg(std::move(c)),id(make_id()){}
 bool init(){
  fd=socket(AF_INET,SOCK_DGRAM,0); if(fd<0){perror("socket");return false;}
  int yes=1,buf=1<<20; setsockopt(fd,SOL_SOCKET,SO_REUSEADDR,&yes,sizeof yes); setsockopt(fd,SOL_SOCKET,SO_RCVBUF,&buf,sizeof buf); setsockopt(fd,SOL_SOCKET,SO_SNDBUF,&buf,sizeof buf);
  timeval tv{0,100000}; setsockopt(fd,SOL_SOCKET,SO_RCVTIMEO,&tv,sizeof tv);
  sockaddr_in local{}; local.sin_family=AF_INET;local.sin_port=htons(cfg.port);local.sin_addr.s_addr=htonl(INADDR_ANY);
  if(bind(fd,(sockaddr*)&local,sizeof local)<0){perror("bind");return false;}
  ip_mreq m{}; if(inet_pton(AF_INET,cfg.group.c_str(),&m.imr_multiaddr)!=1||inet_pton(AF_INET,cfg.iface.c_str(),&m.imr_interface)!=1){std::cerr<<"Invalid group/interface\n";return false;}
  if(setsockopt(fd,IPPROTO_IP,IP_ADD_MEMBERSHIP,&m,sizeof m)<0){perror("IP_ADD_MEMBERSHIP");return false;}
  unsigned char ttl=1,loop=1;setsockopt(fd,IPPROTO_IP,IP_MULTICAST_TTL,&ttl,sizeof ttl);setsockopt(fd,IPPROTO_IP,IP_MULTICAST_LOOP,&loop,sizeof loop);
  if(m.imr_interface.s_addr!=htonl(INADDR_ANY))setsockopt(fd,IPPROTO_IP,IP_MULTICAST_IF,&m.imr_interface,sizeof m.imr_interface);
  dest.sin_family=AF_INET;dest.sin_port=htons(cfg.port);inet_pton(AF_INET,cfg.group.c_str(),&dest.sin_addr);return true;
 }
 void run(){
  signal(SIGINT,stop);signal(SIGTERM,stop);rx=std::thread(&Node::receive,this);ui=std::thread(&Node::dashboard,this);
  auto hb=Clock::now(),next=hb;
  while(running){auto now=Clock::now();if(now>=hb){send(make_packet(HEARTBEAT,id,0,0));hb=now+std::chrono::seconds(1);}if(now>=next){cycle();next=Clock::now()+std::chrono::microseconds(cfg.interval_us);}std::this_thread::sleep_until(std::min(next,hb));}
  if (rx.joinable()) rx.join();
  if (ui.joinable()) ui.join();
  close(fd);
  std::cout << "\nStopped.\n";
 }
private:
 void send(const Packet&p){(void)sendto(fd,&p,sizeof p,0,(sockaddr*)&dest,sizeof dest);}
 void receive(){while(running){Packet p{};sockaddr_in from{};socklen_t n=sizeof from;auto got=recvfrom(fd,&p,sizeof p,0,(sockaddr*)&from,&n);if(got!=sizeof p||H(p.magic)!=MAGIC||H(p.version)!=VERSION)continue;uint64_t who=sender(p);if(who==id)continue;uint32_t type=H(p.type);{std::lock_guard<std::mutex>l(peers_m);peers[who].seen=Clock::now();}
  if(type==REQUEST){send(make_packet(RESPONSE,id,who,H(p.seq),H(p.value)+1));}
  else if(type==RESPONSE&&target(p)==id){std::lock_guard<std::mutex>l(cycle_m);if(pending&&H(p.seq)==pending_seq)received.insert(who);cv.notify_one();}
 }}
 std::set<uint64_t> active(){std::set<uint64_t> out;auto now=Clock::now();std::lock_guard<std::mutex>l(peers_m);for(auto i=peers.begin();i!=peers.end();)if(now-i->second.seen>PEER_TIMEOUT)i=peers.erase(i);else{out.insert(i->first);++i;}return out;}
 void cycle(){uint32_t seq=++sequence;auto expected=active();auto start=Clock::now();{std::lock_guard<std::mutex>l(cycle_m);pending=true;pending_seq=seq;expected_set=expected;received.clear();}
  send(make_packet(REQUEST,id,0,seq,1));auto deadline=start+std::chrono::microseconds(200);{std::unique_lock<std::mutex>l(cycle_m);cv.wait_until(l,deadline,[&]{return !running||std::includes(received.begin(),received.end(),expected_set.begin(),expected_set.end());});pending=false;}
  double us=std::chrono::duration<double,std::micro>(Clock::now()-start).count();size_t got;{std::lock_guard<std::mutex>l(cycle_m);got=received.size();}bool ok=got>=expected.size();{std::lock_guard<std::mutex>l(stats_m);samples.push_back(us);if(samples.size()>80)samples.pop_front();++cycles;if(us>LIMIT_US)++over;if(!ok)++incomplete;last=us;wanted=expected.size();answered=got;complete=ok;}
 }
 void dashboard(){while(running){draw();std::this_thread::sleep_for(std::chrono::milliseconds(250));}draw();}
 void draw(){std::deque<double>s;uint64_t c,o,inc;double l;size_t w,a;bool ok;{std::lock_guard<std::mutex>g(stats_m);s=samples;c=cycles;o=over;inc=incomplete;l=last;w=wanted;a=answered;ok=complete;}auto ps=active();double avg=0,mn=0,mx=0;if(!s.empty()){avg=std::accumulate(s.begin(),s.end(),0.0)/s.size();auto mm=std::minmax_element(s.begin(),s.end());mn=*mm.first;mx=*mm.second;}
  std::cout<<"\033[2J\033[H\033[1;36m==================== S O D | LIVE MONITOR ====================\033[0m\n";
  std::cout<<"Node "<<std::hex<<id<<std::dec<<"   Multicast "<<cfg.group<<":"<<cfg.port<<"   Period "<<cfg.interval_us<<" us\n";
  std::cout<<"P2P mesh: all peers equal | no coordinator | update 4 times/sec\n\n";
  std::cout<<"\033[1mPERFORMANCE — last 80 cycles\033[0m\n"<<std::fixed<<std::setprecision(1);
  std::cout<<"  LAST "<<std::setw(7)<<l<<" us   AVG "<<std::setw(7)<<avg<<" us   MIN "<<std::setw(7)<<mn<<" us   MAX "<<std::setw(7)<<mx<<" us\n";
  std::cout<<"  Cycles "<<c<<"   >200 us "<<o<<"   incomplete "<<inc<<"   Replies "<<a<<"/"<<w<<"  ";
  std::cout<<(ok?"\033[1;32mOK\033[0m":"\033[1;31mWAIT/TIMEOUT\033[0m")<<"\n\n";
  std::cout<<"\033[1mLATENCY HISTORY\033[0m   80 most recent cycles; scale 0..200 us\n  ";
  const char* levels[] = {"_", "▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"};
  for (double v : s) {
      int level = std::clamp(static_cast<int>(v / 25.0) + 1, 1, 8);
      std::cout << (v > LIMIT_US ? "\033[1;31m" : "\033[1;32m")
                << levels[level] << "\033[0m";
  }
  std::cout<<"\n  older ------------------------------------------------> latest\n\n";
  std::cout<<"\033[1mACTIVE PEERS\033[0m ("<<ps.size()<<")\n";
  if(ps.empty())std::cout<<"  Waiting for multicast neighbours...\n";else{std::lock_guard<std::mutex>g(peers_m);for(auto p:ps){auto i=peers.find(p);if(i!=peers.end()){auto age=std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now()-i->second.seen).count();std::cout<<"  \033[32m●\033[0m "<<std::hex<<p<<std::dec<<"   last packet "<<age<<" ms ago\n";}}}
  std::cout<<"\n\033[90mCtrl+C to exit\033[0m\n"<<std::flush;
 }
 Config cfg;uint64_t id;int fd=-1;sockaddr_in dest{};std::thread rx,ui;std::atomic<uint32_t>sequence{0};
 std::mutex peers_m;std::unordered_map<uint64_t,Peer>peers;
 std::mutex cycle_m;std::condition_variable cv;bool pending=false;uint32_t pending_seq=0;std::set<uint64_t>expected_set,received;
 std::mutex stats_m;std::deque<double>samples;uint64_t cycles=0,over=0,incomplete=0;double last=0;size_t wanted=0,answered=0;bool complete=false;
};
void usage(const char*n){std::cout<<"Usage: "<<n<<" [--group IPv4] [--port N] [--iface IPv4] [--interval-us N]\nDefault interval: 1000 us\n";}
}
int main(int argc,char**argv){Config c;for(int i=1;i<argc;i++){std::string a=argv[i];if(a=="--help"){usage(argv[0]);return 0;}else if(a=="--group"&&i+1<argc)c.group=argv[++i];else if(a=="--iface"&&i+1<argc)c.iface=argv[++i];else if(a=="--port"&&i+1<argc){int p=std::stoi(argv[++i]);if(p<1||p>65535)return 2;c.port=p;}else if(a=="--interval-us"&&i+1<argc){int v=std::stoi(argv[++i]);if(v<100||v>10000000){std::cerr<<"Interval must be 100..10000000 us\n";return 2;}c.interval_us=v;}else{usage(argv[0]);return 2;}}Node n(c);if(!n.init())return 1;n.run();}
