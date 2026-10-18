// baseline_server_main.cpp - the naive server the epoll server has to beat (SRS FR-19).
//
// One thread per connection, plain blocking recv/send. It speaks the same frame protocol and
// supports the same --mode echo|ack, so benchmarks compare like with like.
// Deliberately simple. Everything else the epoll server does (timeouts, backpressure, stats,
// connection limits) is not here.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <system_error>
#include <thread>

#include "message_handler.h"
#include "server_config.h"

// Reads exactly `count` bytes. Returns false if the client left or something failed.
static bool receive_exactly(int fd, unsigned char* buffer, size_t count) {
    size_t received = 0;
    while (received < count) {
        ssize_t n = recv(fd, buffer + received, count - received, 0);
        if (n == 0) return false;
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        received += (size_t)n;
    }
    return true;
}

static bool send_everything(int fd, const unsigned char* data, size_t size) {
    size_t sent = 0;
    while (sent < size) {
        ssize_t n = send(fd, data + sent, size - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        sent += (size_t)n;
    }
    return true;
}

// Runs in its own thread, for one client, until that client leaves or misbehaves.
static void serve_connection(int fd, ServerMode mode) {
    ByteVector payload;
    ByteVector reply;
    for (;;) {
        unsigned char header[HEADER_SIZE];
        if (!receive_exactly(fd, header, HEADER_SIZE)) break;

        uint32_t payload_length = read_length_prefix(header);
        if (payload_length > MAX_PAYLOAD_SIZE) break;

        payload.resize(payload_length);
        if (!receive_exactly(fd, payload.data(), payload_length)) break;

        reply.clear();
        if (!append_reply(mode, payload.data(), payload_length, reply)) break;
        if (!send_everything(fd, reply.data(), reply.size())) break;
    }
    close(fd);
}

int main(int argc, char** argv) {
    ServerConfig config;
    if (!parse_command_line(argc, argv, config)) return 1;
    signal(SIGPIPE, SIG_IGN);

    int listening_fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (listening_fd < 0) {
        perror("socket");
        return 1;
    }
    int enable = 1;
    setsockopt(listening_fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof enable);

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(config.port);
    if (bind(listening_fd, (sockaddr*)&address, sizeof address) < 0) {
        perror("bind");
        return 1;
    }
    if (listen(listening_fd, SOMAXCONN) < 0) {
        perror("listen");
        return 1;
    }
    fprintf(stderr, "baseline_server: listening on port %d, mode %s\n", config.port,
            config.mode == ServerMode::ECHO ? "echo" : "ack");

    for (;;) {
        int fd = accept4(listening_fd, nullptr, nullptr, SOCK_CLOEXEC);
        if (fd < 0) {
            if (errno == EINTR) continue;
            perror("accept4");
            continue;
        }
        try {
            std::thread(serve_connection, fd, config.mode).detach();
        } catch (const std::system_error& error) {
            // too many threads: this is exactly where thread-per-connection hits its wall
            fprintf(stderr, "could not start a thread: %s\n", error.what());
            close(fd);
        }
    }
}
