# SOD Multicast test program

Linux C++17 UDP multicast test prototype. Supports configured payload size from 1 KiB to 100 KiB and segments each message into MTU-safe chunks (default 1200 bytes). The primary test target is 2048 bytes and a cycle threshold of 200 microseconds. Threshold is recorded as a measured result, not guaranteed by ordinary Linux.

## Build / run

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
./build/sod_multicast
```
Run on both hosts. Set unique `node_id` and `expected_nodes` to the total number of participating nodes. `interface_ip` can be set to a local Ethernet IPv4 address (or `0.0.0.0` to use the default interface). Ensure firewall permits UDP multicast on data/discovery ports.

## Configuration

`payload_size_bytes`: 1024..102400; `chunk_payload_bytes`: 256..1400 (defaults 1200, conservatively MTU-safe); `receive_timeout_us` bounds wait for all expected remote messages. The dashboard refresh is separate from cycle cadence. CSVs: `timing_cycles.csv`, `timing_packets.csv`.

## Important limits

This is a benchmark prototype, not a certified hard-real-time implementation. The UDP protocol uses chunk indices and cycle sequence IDs; no retransmission or FEC is implemented. Missing chunks or peers are recorded as incomplete, not recovered. With this simple round-cycle scheme, host start skew and scheduling can cause missed sequence matching. Linux scheduling, NIC, switch, link speed, and CPU load determine whether 2 KiB can meet 200 us. For hard guarantees evaluate PREEMPT_RT, affinity, IRQ tuning, and hardware under specified conditions. Multicast beacon discovery shares the configured multicast group but uses its own UDP port.
