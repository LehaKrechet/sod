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
using Clock=std::chrono::steady_clock;
static double us(Clock::time_point a,Clock::time_point b){return std::chrono::duration<double,std::micro>(b-a).count();}
static void bar(const char*label,double v,int width){int n=(int)std::clamp(v/200.0*width,0.0,(double)width);std::cout<<label<<" ["<<std::string(n,'#')<<std::string(width-n,' ')<<"] "<<std::fixed<<std::setprecision(1)<<v<<" us\n";}
int main(int argc,char*argv[]){try{
 const std::string config_path=(argc>1)?argv[1]:"config.ini";Config c=load_config(config_path);
 std::cout<<"SOD multicast node="<<c.node_id<<" payload="<<c.payload_size_bytes<<" bytes chunk="<<c.chunk_payload_bytes<<"\n";
 DataNetwork net(c);Discovery discovery(c);std::ofstream cyc,pack;
 if(c.csv_logging){cyc.open("timing_cycles.csv");cyc<<"seq,payload_bytes,tx_us,rx_us,compute_us,total_us,peers_received,expected_remote,over_200\n";pack.open("timing_packets.csv");pack<<"seq,node_id,chunk_index,chunk_count\n";}
 std::vector<uint8_t> payload(c.payload_size_bytes);std::mt19937 rng((unsigned)c.node_id);for(auto&b:payload)b=(uint8_t)rng();
 uint64_t seq=0;auto lastdash=Clock::now();std::vector<double>hist;int remote_expected=c.expected_nodes-1;
 while(true){++seq;auto t0=Clock::now();net.send_message(seq,payload,c.node_id);auto ttx=Clock::now();
  std::map<int,std::vector<uint8_t>>assembled;std::map<int,std::vector<bool>>got;std::map<int,uint16_t>counts;
  auto deadline=t0+std::chrono::microseconds(c.receive_timeout_us);
  auto fully_received=[&](){int n=0;for(auto&kv:got)if(std::all_of(kv.second.begin(),kv.second.end(),[](bool x){return x;}))n++;return n;};
  while(Clock::now()<deadline&&fully_received()<remote_expected){int node;uint64_t s;uint32_t total;uint16_t idx,count;std::vector<uint8_t>chunk;int left=(int)std::chrono::duration_cast<std::chrono::microseconds>(deadline-Clock::now()).count();if(!net.receive_chunk(left,node,s,total,idx,count,chunk))continue;
   if(node==c.node_id||s!=seq||total!=c.payload_size_bytes||idx>=count||count==0)continue;
   if(!counts.count(node)){counts[node]=count;assembled[node].resize(total);got[node].assign(count,false);}if(counts[node]!=count||idx>=got[node].size())continue;
   size_t offset=(size_t)idx*c.chunk_payload_bytes;if(offset+chunk.size()>assembled[node].size())continue;memcpy(assembled[node].data()+offset,chunk.data(),chunk.size());got[node][idx]=true;
  }
  auto trx=Clock::now();int complete=0;for(auto&kv:got)if(std::all_of(kv.second.begin(),kv.second.end(),[](bool x){return x;})){complete++;if(c.csv_logging)for(uint16_t i=0;i<counts[kv.first];i++)pack<<seq<<","<<kv.first<<","<<i<<","<<counts[kv.first]<<"\n";}
  volatile int result=1+1;(void)result;auto tc=Clock::now();double tx=us(t0,ttx),rx=us(ttx,trx),compute=us(trx,tc),total=us(t0,tc);bool over=total>200;hist.push_back(total);if(hist.size()>1000)hist.erase(hist.begin());if(c.csv_logging)cyc<<seq<<","<<c.payload_size_bytes<<","<<tx<<","<<rx<<","<<compute<<","<<total<<","<<complete<<","<<remote_expected<<","<<(over?1:0)<<"\n";
  if(c.terminal_graph&&us(lastdash,Clock::now())>=c.graph_refresh_ms*1000.0){lastdash=Clock::now();std::cout<<"\033[2J\033[H"<<"Node "<<c.node_id<<" | seq "<<seq<<" | payload "<<c.payload_size_bytes<<" bytes | target 200 us\n";bar("TX  ",tx,c.graph_width);bar("RX  ",rx,c.graph_width);bar("CPU ",compute,c.graph_width);bar("ALL ",total,c.graph_width);std::cout<<(over?"[OVER 200 us]":"[within 200 us]")<<" | completed peers: "<<complete<<"/"<<remote_expected<<"\n\nActive peers: ";auto peers=discovery.peers();if(peers.empty())std::cout<<"(none)";for(auto&p:peers)std::cout<<p.first<<"("<<p.second<<") ";std::cout<<"\nRecent cycles: "<<hist.size()<<" | max="<<*std::max_element(hist.begin(),hist.end())<<" us\n"<<std::flush;}
  if(c.cycle_period_us>0){auto next=t0+std::chrono::microseconds(c.cycle_period_us);if(Clock::now()<next)std::this_thread::sleep_until(next);}
 }
}catch(const std::exception&e){std::cerr<<"ERROR: "<<e.what()<<"\n";return 1;}}
