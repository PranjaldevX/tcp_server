// epoll_server_main.cpp - starts the workers and watches over them.
//
// The workers do all the network work. This thread only
//   - sets up signals and the file descriptor limit,
//   - prints the stats line every few seconds,
//   - waits for SIGINT/SIGTERM, then lets the workers finish and exits.

#include <sys/resource.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <memory>
#include <thread>
#include <vector>

#include "allocation_tracking.h"
#include "server_config.h"
#include "server_stats.h"
#include "worker.h"

std::atomic<bool> shutdown_requested{false};
static_assert(std::atomic<bool>::is_always_lock_free, "needs to be safe to set from a signal handler");

static void on_shutdown_signal(int) { shutdown_requested.store(true); }

// Every client needs a file descriptor, and the default limit is often 1024.
// Raise the soft limit as far as the hard limit allows, up to what the configured
// connection limits could need.
static void raise_open_file_limit(const ServerConfig& config) {
    rlimit limit{};
    if (getrlimit(RLIMIT_NOFILE, &limit) != 0) return;

    rlim_t needed = (rlim_t)config.max_connections * (rlim_t)config.worker_threads + 1024;
    rlim_t target = needed;
    if (limit.rlim_max != RLIM_INFINITY && target > limit.rlim_max) target = limit.rlim_max;
    if (limit.rlim_cur != RLIM_INFINITY && limit.rlim_cur >= target) return;

    limit.rlim_cur = target;
    if (setrlimit(RLIMIT_NOFILE, &limit) != 0) {
        perror("setrlimit(RLIMIT_NOFILE)");
        return;
    }
    if (target < needed) {
        fprintf(stderr, "note: open file limit is %llu, lower than --max-connections needs\n",
                (unsigned long long)target);
    }
}

static void print_stats(const char* label, const StatsTotals& now, const StatsTotals& before,
                        double seconds) {
    print_stats_line(label, now, before, seconds);
#ifdef TRACK_ALLOCATIONS
    fprintf(stderr, "[%s] heap allocations on worker threads so far: %llu\n", label,
            (unsigned long long)tracked_allocation_count());
#endif
}

int main(int argc, char** argv) {
    ServerConfig config;
    if (!parse_command_line(argc, argv, config)) return 1;

    // A client that disconnects while we write must not kill the server with SIGPIPE.
    signal(SIGPIPE, SIG_IGN);

    struct sigaction action{};
    action.sa_handler = on_shutdown_signal;
    sigemptyset(&action.sa_mask);
    sigaction(SIGINT, &action, nullptr);
    sigaction(SIGTERM, &action, nullptr);

    raise_open_file_limit(config);

    // Create every worker (and so bind every listening socket) before any thread starts,
    // so a bind error shows up right away instead of from inside a thread.
    std::vector<std::unique_ptr<Worker>> workers;
    std::vector<const WorkerStats*> worker_stats;
    for (int i = 0; i < config.worker_threads; i++) {
        workers.push_back(std::make_unique<Worker>(i, config));
        worker_stats.push_back(&workers.back()->stats());
    }

    fprintf(stderr, "epoll_server: listening on port %d, %d worker thread(s), mode %s\n", config.port,
            config.worker_threads, config.mode == ServerMode::ECHO ? "echo" : "ack");

    std::vector<std::thread> threads;
    for (auto& worker : workers) {
        threads.emplace_back([&worker] { worker->run(); });
    }

    const auto report_interval = std::chrono::seconds(config.stats_interval_seconds);
    auto last_report_time = Clock::now();
    StatsTotals last_report = collect_totals(worker_stats);

    while (!shutdown_requested.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        auto now = Clock::now();
        if (config.stats_interval_seconds > 0 && now - last_report_time >= report_interval) {
            StatsTotals totals = collect_totals(worker_stats);
            print_stats("stats", totals, last_report,
                        std::chrono::duration<double>(now - last_report_time).count());
            last_report = totals;
            last_report_time = now;
        }
    }

    for (auto& thread : threads) thread.join();
    print_stats("final", collect_totals(worker_stats), StatsTotals(), 0);
    return 0;
}
