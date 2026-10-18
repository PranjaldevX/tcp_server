#include "server_config.h"

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace {

void print_usage(const char* program_name) {
    fprintf(stderr,
            "usage: %s [options]\n"
            "  --port N              port to listen on (default 9000)\n"
            "  --mode echo|ack       send payloads back, or reply with a sequence-id ack (default echo)\n"
            "  --workers N           epoll worker threads, each with its own listener (default 1)\n"
            "  --max-connections N   limit per worker thread (default 100000)\n"
            "  --idle-timeout SEC    close connections silent for this long, 0 = never (default 60)\n"
            "  --write-buffer-cap B  stop reading from a client with this many reply bytes waiting (default 1048576)\n"
            "  --stats-interval SEC  print a stats line every SEC seconds, 0 = off (default 5)\n"
            "  --pool-buffers N      reusable buffers per worker created at startup (default 64)\n"
            "  --tcp-nodelay         set TCP_NODELAY on accepted sockets\n"
            "  --writev              send queued + new reply bytes with one writev call\n"
            "  --pin-cpus            pin worker i to cpu i\n"
            "  --help                this text\n"
            "(the baseline server only looks at --port and --mode)\n",
            program_name);
}

// Reads the number after argv[i] and moves i onto it. Returns false (with a message) if the
// number is missing, not a number, or outside min_value..max_value.
bool read_number(int argc, char** argv, int& i, long long min_value, long long max_value,
                 long long& result) {
    const char* option = argv[i];
    if (i + 1 >= argc) {
        fprintf(stderr, "%s needs a value\n", option);
        return false;
    }
    const char* text = argv[++i];

    char* end = nullptr;
    errno = 0;
    long long value = strtoll(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0' || value < min_value || value > max_value) {
        fprintf(stderr, "%s: '%s' is not a number between %lld and %lld\n", option, text, min_value,
                max_value);
        return false;
    }
    result = value;
    return true;
}

}  // namespace

bool parse_command_line(int argc, char** argv, ServerConfig& config) {
    for (int i = 1; i < argc; i++) {
        const char* option = argv[i];
        long long number = 0;

        if (strcmp(option, "--help") == 0) {
            print_usage(argv[0]);
            return false;
        } else if (strcmp(option, "--port") == 0) {
            if (!read_number(argc, argv, i, 1, 65535, number)) return false;
            config.port = (int)number;
        } else if (strcmp(option, "--mode") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--mode needs a value\n");
                return false;
            }
            const char* mode = argv[++i];
            if (strcmp(mode, "echo") == 0) {
                config.mode = ServerMode::ECHO;
            } else if (strcmp(mode, "ack") == 0) {
                config.mode = ServerMode::ACK;
            } else {
                fprintf(stderr, "--mode: '%s' is not echo or ack\n", mode);
                return false;
            }
        } else if (strcmp(option, "--workers") == 0) {
            if (!read_number(argc, argv, i, 1, 256, number)) return false;
            config.worker_threads = (int)number;
        } else if (strcmp(option, "--max-connections") == 0) {
            if (!read_number(argc, argv, i, 1, 10000000, number)) return false;
            config.max_connections = (size_t)number;
        } else if (strcmp(option, "--idle-timeout") == 0) {
            if (!read_number(argc, argv, i, 0, 86400, number)) return false;
            config.idle_timeout_seconds = (int)number;
        } else if (strcmp(option, "--write-buffer-cap") == 0) {
            if (!read_number(argc, argv, i, 0, 1LL << 30, number)) return false;
            config.write_buffer_cap_bytes = (size_t)number;
        } else if (strcmp(option, "--stats-interval") == 0) {
            if (!read_number(argc, argv, i, 0, 3600, number)) return false;
            config.stats_interval_seconds = (int)number;
        } else if (strcmp(option, "--pool-buffers") == 0) {
            if (!read_number(argc, argv, i, 0, 1000000, number)) return false;
            config.initial_pool_buffers = (size_t)number;
        } else if (strcmp(option, "--tcp-nodelay") == 0) {
            config.tcp_nodelay = true;
        } else if (strcmp(option, "--writev") == 0) {
            config.use_writev = true;
        } else if (strcmp(option, "--pin-cpus") == 0) {
            config.pin_workers_to_cpus = true;
        } else {
            fprintf(stderr, "unknown option '%s'\n\n", option);
            print_usage(argv[0]);
            return false;
        }
    }
    return true;
}
