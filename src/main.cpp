#include "config.hpp"
#include "network.hpp"
#include "discovery.hpp"

#include <chrono>
#include <thread>
#include <iostream>
#include <iomanip>
#include <random>
#include <vector>
#include <cstring>
#include <algorithm>
#include <numeric>
#include <string>
#include <array>
#include <map>
#include <cerrno>
#include <cstdlib>
#include <sched.h>
#include <time.h>
#include <sys/mman.h>

using Clock = std::chrono::steady_clock;
static double us(Clock::time_point a, Clock::time_point b) {
    return std::chrono::duration<double, std::micro>(b-a).count();
}

static void apply_realtime_settings(const Config& config) {
    if (config.cpu_affinity >= 0) {
        cpu_set_t set; CPU_ZERO(&set); CPU_SET(config.cpu_affinity, &set);
        if (sched_setaffinity(0, sizeof(set), &set) != 0)
            std::cerr << "WARN: CPU affinity failed: " << std::strerror(errno) << '\n';
    }
    if (config.lock_memory && mlockall(MCL_CURRENT | MCL_FUTURE) != 0)
        std::cerr << "WARN: mlockall failed: " << std::strerror(errno) << '\n';
    if (config.realtime_priority > 0) {
        sched_param p{}; p.sched_priority = config.realtime_priority;
        if (sched_setscheduler(0, SCHED_FIFO, &p) != 0)
            std::cerr << "WARN: SCHED_FIFO failed: " << std::strerror(errno) << '\n';
    }
}

static void histogram(const std::vector<double>& values, int width,
                     const std::map<int, std::string>& peers) {
    const double edges[] = {50,100,110,120,130,140,150,160,170,180,190,200,250,300,400};
    const char* labels[] = {"0-50","50-100","100-110","110-120","120-130","130-140","140-150","150-160","160-170","170-180","180-190","190-200","200-250","250-300","300-400",">400"};
    constexpr int n=16; int bins[n]{};
    for (double v:values) { int b=0; while(b<n-1 && v>=edges[b]) ++b; ++bins[b]; }
    int maximum=*std::max_element(std::begin(bins),std::end(bins)); if(maximum<1)maximum=1;
    double average=values.empty()?0.0:std::accumulate(values.begin(),values.end(),0.0)/values.size();
    size_t over=std::count_if(values.begin(),values.end(),[](double v){return v>200.0;});
    std::cout<<"Average ALL: "<<std::fixed<<std::setprecision(1)<<average<<" us | samples: "<<values.size()<<" | >200 us: "<<over<<" | threshold: 200 us\n";
    std::cout<<"Connected devices: "<<(peers.empty()?"(none)":"")<<"\n";
    for(const auto& p:peers) std::cout<<"  Node "<<p.first<<" @ "<<p.second<<"\n";
    std::cout<<"\nDynamic histogram | last "<<values.size()<<" cycles\n";
    for(int i=0;i<n;++i){int len=static_cast<int>(static_cast<double>(bins[i])/maximum*width);std::cout<<std::setw(8)<<labels[i]<<" us | "<<std::string(len,'#')<<" ("<<bins[i]<<")\n";}
}

struct NodeState {
    std::vector<uint8_t> payload;
    std::vector<uint8_t> seen;
    uint64_t sequence=0;
    uint16_t chunk_count=0;
    bool active=false;
    bool complete=false;
    size_t received_count=0;
};

int main(int argc,char* argv[]) {
 try {
    const std::string path=(argc>1)?argv[1]:"config.ini";
    Config config=load_config(path);
    apply_realtime_settings(config);
    std::cout<<"SOD multicast node="<<config.node_id<<" payload="<<config.payload_size_bytes<<" bytes chunk="<<config.chunk_payload_bytes<<"\n";
    DataNetwork network(config); Discovery discovery(config);
    std::vector<uint8_t> payload(config.payload_size_bytes);
    std::mt19937 rng(static_cast<unsigned>(config.node_id));for(auto& b:payload)b=static_cast<uint8_t>(rng());
    const size_t chunks=(payload.size()+config.chunk_payload_bytes-1)/config.chunk_payload_bytes;
    std::array<NodeState,256> nodes;
    // Буферы заранее выделяются один раз. Сбрасывать все 256 состояний
    // на каждом цикле не требуется: состояние обновляется при первом
    // пакете с новым sequence от конкретного узла.
    for(auto& n:nodes){n.payload.resize(config.payload_size_bytes);n.seen.resize(chunks);}
    std::vector<uint8_t> chunk;chunk.reserve(config.chunk_payload_bytes);
    std::vector<double> history;history.reserve(config.max_cycles > 0 ? static_cast<size_t>(config.max_cycles) : 1000);
    uint64_t sequence=0;auto dashboard=Clock::now();
    const int remote_expected=config.expected_nodes-1;
    while(true){
      if(config.max_cycles > 0 && sequence >= static_cast<uint64_t>(config.max_cycles)) break;
      ++sequence;const auto start=Clock::now();
      network.send_message(sequence,payload,config.node_id);
      const auto deadline=start+std::chrono::microseconds(config.receive_timeout_us);
      int complete_count=0;
      while(Clock::now()<deadline && complete_count<remote_expected){
        int node=0;uint64_t seq=0;uint32_t total=0;uint16_t idx=0,count=0;
        const auto now=Clock::now();if(now>=deadline)break;
        const int remaining=std::max(0,static_cast<int>(std::chrono::duration_cast<std::chrono::microseconds>(deadline-now).count()));
        if(!network.receive_chunk(remaining,node,seq,total,idx,count,chunk))continue;
        if(node<=0||node>=static_cast<int>(nodes.size())||node==config.node_id||seq!=sequence||total!=config.payload_size_bytes||count==0||idx>=count||count!=chunks)continue;
        auto& state=nodes[node];
        // Состояние узла сбрасывается только при смене номера цикла,
        // а не массово для всего массива перед каждым измерением.
        if(state.sequence!=sequence){
          state.sequence=sequence;state.active=true;state.complete=false;
          state.chunk_count=count;state.received_count=0;
          std::fill(state.seen.begin(),state.seen.end(),0);
        }
        if(!state.active || state.chunk_count!=count || state.complete)continue;
        const size_t offset=static_cast<size_t>(idx)*config.chunk_payload_bytes;
        if(offset+chunk.size()>state.payload.size())continue;
        std::memcpy(state.payload.data()+offset,chunk.data(),chunk.size());
        if(!state.seen[idx]){state.seen[idx]=1;++state.received_count;}
        if(!state.complete && state.received_count==count){state.complete=true;++complete_count;}
      }
      const auto rx_end=Clock::now();
      const double total_time=us(start,rx_end);
      if(config.max_cycles==0 && history.size()==1000) history.erase(history.begin());
      history.push_back(total_time);
      if(config.terminal_graph && us(dashboard,Clock::now())>=config.graph_refresh_ms*1000.0){dashboard=Clock::now();std::cout<<"\033[2J\033[H";histogram(history,config.graph_width,discovery.peers());std::cout<<std::flush;}
      if(config.cycle_period_us>0){const auto next=start+std::chrono::microseconds(config.cycle_period_us);if(Clock::now()<next)std::this_thread::sleep_until(next);}
    }
    if(config.max_cycles>0){
      std::cout<<"\nRun finished: "<<sequence<<" cycles. Final histogram (all measured cycles):\n";
      histogram(history,config.graph_width,discovery.peers());

    }
 }catch(const std::exception& e){std::cerr<<"ERROR: "<<e.what()<<"\n";return 1;}
}
