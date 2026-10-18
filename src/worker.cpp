#include "worker.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <sched.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>

#include "allocation_tracking.h"
#include "message_handler.h"

namespace {

constexpr size_t RECEIVE_BATCH_BYTES = 64 * 1024;  // most we read from one socket per recv()
constexpr int EPOLL_BATCH_SIZE = 256;              // events fetched per epoll_wait
constexpr int LOOP_WAKEUP_MS = 100;                // wake up this often even when idle (shutdown, idle sweep)
constexpr auto IDLE_SWEEP_INTERVAL = std::chrono::seconds(1);

void exit_with_error(const char* what_failed) {
    perror(what_failed);
    exit(1);
}

bool would_block(int error) { return error == EAGAIN || error == EWOULDBLOCK; }

}  // namespace

Worker::Worker(int worker_index, const ServerConfig& config)
    : worker_index_(worker_index),
      config_(config),
      buffer_pool_(config.initial_pool_buffers, HEADER_SIZE + MAX_PAYLOAD_SIZE,
                   stats_.buffer_pool_allocations) {
    receive_scratch_.resize(RECEIVE_BATCH_BYTES);

    // A reply is never bigger than the bytes it answers. One recv() plus the biggest possible
    // leftover partial frame bounds that, so replies never outgrow this reserve.
    reply_scratch_.reserve(RECEIVE_BATCH_BYTES + HEADER_SIZE + MAX_PAYLOAD_SIZE);

    listening_fd_ = create_listening_socket();

    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) exit_with_error("epoll_create1");

    epoll_event event{};
    event.events = EPOLLIN;
    event.data.fd = listening_fd_;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listening_fd_, &event) < 0) {
        exit_with_error("epoll_ctl add listener");
    }

    spare_fd_ = open("/dev/null", O_RDONLY | O_CLOEXEC);
}

Worker::~Worker() {
    if (listening_fd_ >= 0) close(listening_fd_);
    if (epoll_fd_ >= 0) close(epoll_fd_);
    if (spare_fd_ >= 0) close(spare_fd_);
}

int Worker::create_listening_socket() {
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) exit_with_error("socket");

    // lets us restart the server right away without "address already in use"
    int enable = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof enable);

    if (config_.worker_threads > 1) {
        // every worker listens on the same port and the kernel spreads new connections between them
        if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &enable, sizeof enable) < 0) {
            exit_with_error("setsockopt SO_REUSEPORT");
        }
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(config_.port);
    if (bind(fd, (sockaddr*)&address, sizeof address) < 0) exit_with_error("bind");
    if (listen(fd, SOMAXCONN) < 0) exit_with_error("listen");
    return fd;
}

void Worker::pin_to_cpu() {
    unsigned cpu_count = std::thread::hardware_concurrency();
    if (cpu_count == 0) return;

    cpu_set_t cpus;
    CPU_ZERO(&cpus);
    CPU_SET(worker_index_ % cpu_count, &cpus);
    int error = pthread_setaffinity_np(pthread_self(), sizeof cpus, &cpus);
    if (error != 0) {
        fprintf(stderr, "worker %d: could not pin to a cpu: %s\n", worker_index_, strerror(error));
    }
}

void Worker::run() {
    track_allocations_on_this_thread(true);
    if (config_.pin_workers_to_cpus) pin_to_cpu();

    epoll_event events[EPOLL_BATCH_SIZE];
    Clock::time_point last_idle_sweep = Clock::now();

    while (!shutdown_requested.load()) {
        int ready = epoll_wait(epoll_fd_, events, EPOLL_BATCH_SIZE, LOOP_WAKEUP_MS);
        if (ready < 0) {
            if (errno == EINTR) continue;
            perror("epoll_wait");
            break;
        }
        loop_time_ = Clock::now();

        for (int i = 0; i < ready; i++) {
            int fd = events[i].data.fd;
            uint32_t happened = events[i].events;

            if (fd == listening_fd_) {
                accept_new_connections();
                continue;
            }

            Connection& connection = connections_[fd];
            if (!connection.is_open) continue;

            if (happened & EPOLLERR) {
                close_connection(connection, CloseReason::IO_ERROR);
                continue;
            }
            // Writing first: it can drain the backlog and turn reading back on.
            if (happened & EPOLLOUT) handle_writable(connection);
            if (connection.is_open && (happened & (EPOLLIN | EPOLLHUP))) handle_readable(connection);
        }

        if (loop_time_ - last_idle_sweep >= IDLE_SWEEP_INTERVAL) {
            close_idle_connections();
            last_idle_sweep = loop_time_;
        }
    }

    // shutting down: stop accepting, then close everything
    close(listening_fd_);
    listening_fd_ = -1;
    close_all_connections();
    track_allocations_on_this_thread(false);
}

void Worker::accept_new_connections() {
    for (;;) {
        int fd = accept4(listening_fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd < 0) {
            if (errno == EINTR) continue;
            if (would_block(errno)) return;  // nobody else is waiting
            if (errno == EMFILE || errno == ENFILE) {
                if (!shed_one_waiting_client()) return;
                continue;
            }
            stats_.accept_errors.add();  // e.g. the client gave up before we got to it
            return;
        }

        if (open_connection_count_ >= config_.max_connections) {
            close(fd);
            stats_.connections_rejected_over_limit.add();
            continue;
        }

        if (config_.tcp_nodelay) {
            int enable = 1;
            setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &enable, sizeof enable);
        }

        epoll_event event{};
        event.events = EPOLLIN;
        event.data.fd = fd;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &event) < 0) {
            stats_.accept_errors.add();
            close(fd);
            continue;
        }

        if ((size_t)fd >= connections_.size()) connections_.resize((size_t)fd + 1);
        Connection& connection = connections_[fd];
        connection = Connection();
        connection.is_open = true;
        connection.fd = fd;
        connection.last_activity = loop_time_;
        connection.registered_events = EPOLLIN;

        open_connection_count_++;
        stats_.connections_accepted.add();
    }
}

// We ran out of file descriptors, so accept() fails but the client stays in the kernel's queue and
// epoll keeps reporting the listener as readable: the loop would spin. The trick: give up the
// spare descriptor we kept for this, accept that one client, hang up on it, take the spare back.
// Returns false if that did not work (then the caller stops trying for this pass).
bool Worker::shed_one_waiting_client() {
    stats_.accept_errors.add();
    if (spare_fd_ >= 0) close(spare_fd_);
    int fd = accept4(listening_fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (fd >= 0) close(fd);
    spare_fd_ = open("/dev/null", O_RDONLY | O_CLOEXEC);
    return fd >= 0;
}

void Worker::handle_readable(Connection& connection) {
    ssize_t received = recv(connection.fd, receive_scratch_.data(), receive_scratch_.size(), 0);
    if (received == 0) {
        close_connection(connection, CloseReason::PEER_CLOSED);
        return;
    }
    if (received < 0) {
        if (errno == EINTR || would_block(errno)) return;
        close_connection(connection, CloseReason::IO_ERROR);
        return;
    }
    connection.last_activity = loop_time_;
    stats_.bytes_received.add((uint64_t)received);

    // Frames are parsed straight out of the receive scratch buffer. Only when a frame arrived
    // in pieces is its first part kept in a pooled buffer, and the new bytes are added behind it.
    const unsigned char* bytes = receive_scratch_.data();
    size_t byte_count = (size_t)received;
    if (connection.unparsed_bytes != nullptr) {
        connection.unparsed_bytes->insert(connection.unparsed_bytes->end(), bytes, bytes + received);
        bytes = connection.unparsed_bytes->data();
        byte_count = connection.unparsed_bytes->size();
    }

    // Peel off every complete frame and build the replies into reply_scratch_.
    // A protocol error closes the connection at once, replies not yet sent are dropped.
    reply_scratch_.clear();
    size_t consumed = 0;
    uint64_t frames_handled = 0;
    for (;;) {
        uint32_t payload_length = 0;
        FrameStatus status = check_frame(bytes + consumed, byte_count - consumed, &payload_length);
        if (status == FRAME_INCOMPLETE) break;
        if (status == FRAME_TOO_LARGE ||
            !append_reply(config_.mode, bytes + consumed + HEADER_SIZE, payload_length, reply_scratch_)) {
            close_connection(connection, CloseReason::PROTOCOL_ERROR);
            return;
        }
        consumed += HEADER_SIZE + payload_length;
        frames_handled++;
    }
    stats_.messages_received.add(frames_handled);
    stats_.messages_sent.add(frames_handled);

    // Whatever is left is the beginning of a frame that has not fully arrived yet.
    size_t leftover = byte_count - consumed;
    if (connection.unparsed_bytes != nullptr) {
        if (leftover == 0) {
            buffer_pool_.release(connection.unparsed_bytes);
            connection.unparsed_bytes = nullptr;
        } else {
            connection.unparsed_bytes->erase(connection.unparsed_bytes->begin(),
                                             connection.unparsed_bytes->begin() + (long)consumed);
        }
    } else if (leftover > 0) {
        connection.unparsed_bytes = buffer_pool_.acquire();
        connection.unparsed_bytes->assign(bytes + consumed, bytes + byte_count);
    }

    if (!reply_scratch_.empty()) {
        send_or_queue(connection, reply_scratch_.data(), reply_scratch_.size());
    }
}

// Sends the reply now if the socket has room, otherwise keeps the rest in a pooled buffer
// and asks epoll to tell us when the socket becomes writable again.
// Replies must leave in order, so while older bytes are still waiting, new ones queue behind them.
void Worker::send_or_queue(Connection& connection, const unsigned char* data, size_t size) {
    bool bytes_already_waiting = connection.unsent_bytes != nullptr;
    ssize_t sent = 0;

    if (!bytes_already_waiting) {
        sent = send(connection.fd, data, size, MSG_NOSIGNAL);
    } else if (config_.use_writev) {
        // one syscall for "what is waiting" + "the new reply", no copying unless the socket fills up again
        iovec parts[2];
        parts[0].iov_base = connection.unsent_bytes->data() + connection.unsent_offset;
        parts[0].iov_len = connection.unsent_byte_count();
        parts[1].iov_base = const_cast<unsigned char*>(data);
        parts[1].iov_len = size;
        sent = writev(connection.fd, parts, 2);
    }
    // else: with older bytes waiting we don't try now, EPOLLOUT will tell us when there is room

    if (sent < 0) {
        if (errno != EINTR && !would_block(errno)) {
            close_connection(connection, CloseReason::IO_ERROR);
            return;
        }
        sent = 0;
    }
    if (sent > 0) {
        stats_.bytes_sent.add((uint64_t)sent);
        connection.last_activity = loop_time_;
    }

    // `sent` bytes were taken from [waiting bytes][new data], in that order
    size_t waiting = connection.unsent_byte_count();
    size_t taken = (size_t)sent;
    if (taken < waiting) {
        connection.unsent_offset += taken;  // only part of the old backlog went out
        connection.unsent_bytes->insert(connection.unsent_bytes->end(), data, data + size);
    } else {
        size_t new_data_sent = taken - waiting;  // backlog is gone, maybe some of the new data too
        if (new_data_sent < size) {
            if (connection.unsent_bytes == nullptr) connection.unsent_bytes = buffer_pool_.acquire();
            connection.unsent_bytes->clear();
            connection.unsent_offset = 0;
            connection.unsent_bytes->assign(data + new_data_sent, data + size);
        } else if (connection.unsent_bytes != nullptr) {
            buffer_pool_.release(connection.unsent_bytes);
            connection.unsent_bytes = nullptr;
            connection.unsent_offset = 0;
        }
    }
    update_epoll_interest(connection);
}

// epoll said the socket can take more data: push out what is waiting.
void Worker::handle_writable(Connection& connection) {
    while (connection.unsent_bytes != nullptr && connection.unsent_byte_count() > 0) {
        ssize_t sent = send(connection.fd, connection.unsent_bytes->data() + connection.unsent_offset,
                            connection.unsent_byte_count(), MSG_NOSIGNAL);
        if (sent < 0) {
            if (errno == EINTR) continue;
            if (would_block(errno)) break;
            close_connection(connection, CloseReason::IO_ERROR);
            return;
        }
        stats_.bytes_sent.add((uint64_t)sent);
        connection.last_activity = loop_time_;
        connection.unsent_offset += (size_t)sent;
    }

    if (connection.unsent_bytes != nullptr && connection.unsent_byte_count() == 0) {
        buffer_pool_.release(connection.unsent_bytes);
        connection.unsent_bytes = nullptr;
        connection.unsent_offset = 0;
    }
    update_epoll_interest(connection);
}

// Decides what epoll should watch for this connection:
//   EPOLLOUT while reply bytes are waiting (we want to know when the socket has room)
//   EPOLLIN  unless too many reply bytes are waiting: then we stop reading from this client
//            until the backlog drains. This is the backpressure (SRS FR-12).
void Worker::update_epoll_interest(Connection& connection) {
    uint32_t wanted = 0;
    if (connection.unsent_byte_count() <= config_.write_buffer_cap_bytes) wanted |= EPOLLIN;
    if (connection.unsent_bytes != nullptr) wanted |= EPOLLOUT;
    if (wanted == connection.registered_events) return;

    if ((connection.registered_events & EPOLLIN) && !(wanted & EPOLLIN)) {
        stats_.reads_paused_by_backpressure.add();
    }

    epoll_event event{};
    event.events = wanted;
    event.data.fd = connection.fd;
    epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, connection.fd, &event);
    connection.registered_events = wanted;
}

void Worker::close_connection(Connection& connection, CloseReason reason) {
    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, connection.fd, nullptr);
    close(connection.fd);

    if (connection.unparsed_bytes != nullptr) buffer_pool_.release(connection.unparsed_bytes);
    if (connection.unsent_bytes != nullptr) buffer_pool_.release(connection.unsent_bytes);
    connection = Connection();
    open_connection_count_--;

    switch (reason) {
        case CloseReason::PEER_CLOSED: stats_.closed_by_peer.add(); break;
        case CloseReason::IO_ERROR: stats_.closed_by_io_error.add(); break;
        case CloseReason::IDLE_TIMEOUT: stats_.closed_by_idle_timeout.add(); break;
        case CloseReason::PROTOCOL_ERROR: stats_.closed_by_protocol_error.add(); break;
        case CloseReason::SERVER_SHUTDOWN: stats_.closed_by_shutdown.add(); break;
    }
}

// "Idle" = nothing received from the client and nothing we could send to it for the whole timeout.
void Worker::close_idle_connections() {
    if (config_.idle_timeout_seconds <= 0) return;
    const auto limit = std::chrono::seconds(config_.idle_timeout_seconds);
    for (Connection& connection : connections_) {
        if (connection.is_open && loop_time_ - connection.last_activity > limit) {
            close_connection(connection, CloseReason::IDLE_TIMEOUT);
        }
    }
}

void Worker::close_all_connections() {
    for (Connection& connection : connections_) {
        if (connection.is_open) close_connection(connection, CloseReason::SERVER_SHUTDOWN);
    }
}
