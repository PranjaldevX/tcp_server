#pragma once
#include <cstddef>

#include "message_handler.h"

struct ServerConfig {
    int port = 9000;
    ServerMode mode = ServerMode::ECHO;
    int worker_threads = 1;                    // 1 = a single epoll loop (v1), more = v2 multi-reactor

    size_t max_connections = 100000;           // per worker thread; extra clients are accepted and dropped at once
    int idle_timeout_seconds = 60;             // 0 = never close idle connections
    size_t write_buffer_cap_bytes = 1 << 20;   // stop reading from a client once this much reply data is waiting
    int stats_interval_seconds = 5;            // 0 = no periodic stats line
    size_t initial_pool_buffers = 64;          // reusable buffers each worker creates at startup

    // v2 tuning switches. All off by default so each one can be measured before/after.
    bool tcp_nodelay = false;
    bool use_writev = false;
    bool pin_workers_to_cpus = false;
};

// Fills `config` from the command line. Returns false (after printing why) if something is wrong
// or --help was asked for.
bool parse_command_line(int argc, char** argv, ServerConfig& config);
