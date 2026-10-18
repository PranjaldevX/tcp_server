#pragma once
#include <atomic>
#include <chrono>
#include <vector>

#include "buffer_pool.h"
#include "connection.h"
#include "server_config.h"
#include "server_stats.h"

// Set by the signal handler in epoll_server_main.cpp. Every worker checks it each loop pass.
extern std::atomic<bool> shutdown_requested;

// One reactor: a listening socket, an epoll instance, and every connection that listener accepted.
// It shares nothing with other workers, so it needs no locks.
class Worker {
public:
    // Creates the listening socket and epoll instance (exits the program if that fails).
    Worker(int worker_index, const ServerConfig& config);
    ~Worker();
    Worker(const Worker&) = delete;
    Worker& operator=(const Worker&) = delete;

    // The event loop. Returns after shutdown_requested is set and all connections are closed.
    void run();

    const WorkerStats& stats() const { return stats_; }

private:
    int create_listening_socket();
    void pin_to_cpu();

    void accept_new_connections();
    bool shed_one_waiting_client();

    void handle_readable(Connection& connection);
    void handle_writable(Connection& connection);
    void send_or_queue(Connection& connection, const unsigned char* data, size_t size);
    void update_epoll_interest(Connection& connection);

    void close_connection(Connection& connection, CloseReason reason);
    void close_idle_connections();
    void close_all_connections();

    int worker_index_;
    const ServerConfig& config_;

    WorkerStats stats_;  // declared before buffer_pool_, which keeps a reference to a counter in it
    BufferPool buffer_pool_;

    int epoll_fd_ = -1;
    int listening_fd_ = -1;
    int spare_fd_ = -1;  // see shed_one_waiting_client()

    std::vector<Connection> connections_;  // indexed by file descriptor
    size_t open_connection_count_ = 0;

    std::vector<unsigned char> receive_scratch_;  // recv() lands here
    ByteVector reply_scratch_;                    // replies for one recv() are built here, then sent

    Clock::time_point loop_time_;  // refreshed once per loop pass instead of per event
};
