#include "config.hpp"
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <algorithm>
#include <cctype>
Config load_config(const std::string& path){ Config c; std::ifstream f(path); if(!f) throw std::runtime_error("Cannot open config: "+path); std::string line;
 while(std::getline(f,line)){ auto p=line.find('#'); if(p!=std::string::npos) line.resize(p); p=line.find('='); if(p==std::string::npos) continue; auto k=line.substr(0,p),v=line.substr(p+1); auto trim=[](std::string&s){s.erase(0,s.find_first_not_of(" \t\r")); auto q=s.find_last_not_of(" \t\r"); if(q!=std::string::npos)s.resize(q+1);}; trim(k);trim(v); if(v.empty())continue;
 try { if(k=="node_id")c.node_id=stoi(v); else if(k=="expected_nodes")c.expected_nodes=stoi(v); else if(k=="data_port")c.data_port=stoi(v); else if(k=="discovery_port")c.discovery_port=stoi(v); else if(k=="multicast_group")c.multicast_group=v; else if(k=="interface_ip")c.interface_ip=v; else if(k=="payload_size_bytes")c.payload_size_bytes=stoul(v); else if(k=="chunk_payload_bytes")c.chunk_payload_bytes=stoul(v); else if(k=="cycle_period_us")c.cycle_period_us=stoi(v); else if(k=="receive_timeout_us")c.receive_timeout_us=stoi(v); else if(k=="heartbeat_interval_ms")c.heartbeat_interval_ms=stoi(v); else if(k=="peer_timeout_ms")c.peer_timeout_ms=stoi(v); else if(k=="peer_status_interval_ms")c.peer_status_interval_ms=stoi(v); else if(k=="graph_width")c.graph_width=stoi(v); else if(k=="graph_refresh_ms")c.graph_refresh_ms=stoi(v); else if(k=="terminal_graph")c.terminal_graph=(v=="true"||v=="1"); else if(k=="csv_logging")c.csv_logging=(v=="true"||v=="1"); } catch(...) { throw std::runtime_error("Invalid value for config key: "+k); }
 }

 if (c.payload_size_bytes < 1024 || c.payload_size_bytes > 102400) {
     throw std::runtime_error("payload_size_bytes must be 1024..102400");
 }
 if (c.chunk_payload_bytes < 256 || c.chunk_payload_bytes > 1400) {
     throw std::runtime_error("chunk_payload_bytes must be 256..1400 (safe MTU)");
 }
 if (c.expected_nodes < 1) {
     throw std::runtime_error("expected_nodes must be >=1");
 }
 return c;
}
