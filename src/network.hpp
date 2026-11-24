#pragma once
#include "config.hpp"
#include <cstdint>
#include <string>
#include <vector>
struct Received { int node_id; uint64_t seq; size_t total; std::vector<uint8_t> data; };
class DataNetwork {
 int sock=-1; std::string group; int port; std::string iface; int nodeid; size_t chunksize;
 public: DataNetwork(const Config&); ~DataNetwork(); void send_message(uint64_t seq,const std::vector<uint8_t>& data,int node_id); bool receive_chunk(int timeout_us,int& node,uint64_t&seq,uint32_t&total,uint16_t&idx,uint16_t&count,std::vector<uint8_t>&chunk);
};
