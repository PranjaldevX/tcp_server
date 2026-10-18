// load_generator.cpp - drives a server with framed requests and measures latency (SRS FR-20).
//
// Open loop (default): requests are scheduled at a fixed rate. Each request has an INTENDED send
// time, and its latency is measured from that moment, not from when the bytes actually left.
// If the server stalls and the generator can't send on time, that waiting still counts as latency.
// Otherwise a stalled server would also slow the generator down and its worst latencies would never
// show up (this flaw is called coordinated omission).
//
// Closed loop (--closed-loop): every connection keeps exactly one request in flight and sends the
// next as soon as the reply arrives. Handy for comparison, but it hides tail latency.
//
// usage: load_generator --port N --rate R [--connections C] [--threads T] [--mode echo|ack]
//                       [--payload-bytes B] [--warmup S] [--duration S] [--closed-loop] [--csv]

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "../src/message_handler.h"

using Clock = std::chrono::steady_clock;
using TimePoint = Clock::time_point;
using Duration = Clock::duration;

namespace {

struct Options {
    std::string host = "127.0.0.1";
    int port = 9000;
    ServerMode mode = ServerMode::ECHO;
    int connections = 100;
    int threads = 1;
    double target_rate = 0;  // requests per second over all connections (open loop)
    bool closed_loop = false;
    uint32_t payload_bytes = 64;  // payload size of every request, at least MESSAGE_PREFIX_SIZE
    double warmup_seconds = 2;
    double measure_seconds = 10;
    bool csv = false;
};

const char* CSV_HEADER =
    "connections,threads,mode,payload_bytes,target_rate,closed_loop,sent,received,lost,invalid,"
    "throughput_msgs_per_s,p50_us,p90_us,p99_us,p999_us,max_us";

Duration to_duration(double seconds) {
    return std::chrono::duration_cast<Duration>(std::chrono::duration<double>(seconds));
}

// ---------------------------------------------------------------- per-connection state

struct LoadConnection {
    int fd = -1;
    int global_index = 0;
    ByteVector out_bytes;               // request frames waiting to be sent
    size_t out_offset = 0;
    ByteVector in_bytes;                // reply bytes that are not a whole frame yet
    std::deque<TimePoint> in_flight;    // intended send time of every request without a reply yet
    uint32_t next_request_id = 0;
    uint32_t next_reply_id = 0;         // the id the next reply must carry (replies come back in order)
    uint32_t registered_events = 0;
    bool broken = false;
};

struct ThreadResult {
    std::vector<uint32_t> latencies_ns;  // for requests inside the measured window (saturates at ~4.3 s)
    uint64_t sent_in_window = 0;
    uint64_t received_in_window = 0;
    uint64_t invalid_replies = 0;
    uint64_t broken_connections = 0;
    bool could_not_connect = false;
};

// all threads connect first, then start together
struct StartGate {
    std::mutex mutex;
    std::condition_variable changed;
    int threads_ready = 0;
    bool released = false;
    TimePoint start_time;
};

struct DueRequest {
    TimePoint due;
    size_t connection_index;
    bool operator>(const DueRequest& other) const { return due > other.due; }
};

int connect_to_server(const Options& options) {
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        perror("socket");
        return -1;
    }
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(options.port);
    if (inet_pton(AF_INET, options.host.c_str(), &address.sin_addr) != 1) {
        fprintf(stderr, "bad address '%s'\n", options.host.c_str());
        close(fd);
        return -1;
    }
    if (connect(fd, (sockaddr*)&address, sizeof address) < 0) {
        perror("connect");
        close(fd);
        return -1;
    }
    // Our side always uses TCP_NODELAY so Nagle on the generator never adds to the latency we measure.
    int enable = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &enable, sizeof enable);
    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);
    return fd;
}

// ---------------------------------------------------------------- one generator thread

class LoadThread {
public:
    LoadThread(const Options& options, int thread_index, StartGate& gate, ThreadResult& result)
        : options_(options), thread_index_(thread_index), gate_(gate), result_(result) {}

    void run() {
        epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
        receive_scratch_.resize(64 * 1024);

        // this thread handles connections thread_index, thread_index + threads, ...
        for (int index = thread_index_; index < options_.connections; index += options_.threads) {
            int fd = connect_to_server(options_);
            if (fd < 0) {
                result_.could_not_connect = true;
                break;
            }
            connections_.emplace_back();
            connections_.back().fd = fd;
            connections_.back().global_index = index;
        }

        // wait for every thread to finish connecting
        {
            std::unique_lock<std::mutex> lock(gate_.mutex);
            gate_.threads_ready++;
            gate_.changed.notify_all();
            gate_.changed.wait(lock, [this] { return gate_.released; });
        }
        if (result_.could_not_connect) return;

        for (size_t i = 0; i < connections_.size(); i++) {
            epoll_event event{};
            event.events = EPOLLIN;
            event.data.u32 = (uint32_t)i;
            epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, connections_[i].fd, &event);
            connections_[i].registered_events = EPOLLIN;
        }

        const TimePoint start = gate_.start_time;
        measure_start_ = start + to_duration(options_.warmup_seconds);
        measure_end_ = measure_start_ + to_duration(options_.measure_seconds);
        const TimePoint stop_time = measure_end_ + std::chrono::seconds(2);  // grace for late replies

        reserve_latency_storage();

        // Open loop: spread the connections' first requests evenly over one interval so they
        // don't all fire at the same instant.
        std::priority_queue<DueRequest, std::vector<DueRequest>, std::greater<DueRequest>> schedule;
        if (options_.closed_loop) {
            for (size_t i = 0; i < connections_.size(); i++) schedule.push({start, i});
        } else {
            interval_ = to_duration((double)options_.connections / options_.target_rate);
            for (size_t i = 0; i < connections_.size(); i++) {
                double position = (double)connections_[i].global_index / options_.connections;
                schedule.push({start + std::chrono::duration_cast<Duration>(interval_ * position), i});
            }
        }

        epoll_event events[256];
        for (;;) {
            TimePoint now = Clock::now();
            if (now >= stop_time) break;
            if (now >= measure_end_ && schedule.empty() && in_flight_total_ == 0) break;

            // send everything that is due
            while (!schedule.empty() && schedule.top().due <= now) {
                DueRequest request = schedule.top();
                schedule.pop();
                LoadConnection& connection = connections_[request.connection_index];
                if (!connection.broken) queue_request(connection, request.due);
                if (!options_.closed_loop) {
                    TimePoint following = request.due + interval_;
                    if (following < measure_end_) schedule.push({following, request.connection_index});
                }
            }

            // wait for replies, but not past the next scheduled request
            int timeout_ms = 10;
            if (!schedule.empty()) {
                auto until_due = std::chrono::duration_cast<std::chrono::milliseconds>(schedule.top().due - now);
                timeout_ms = (int)std::max<long long>(0, std::min<long long>(until_due.count(), 10));
            }
            int ready = epoll_wait(epoll_fd_, events, 256, timeout_ms);
            if (ready < 0 && errno != EINTR) break;

            TimePoint after_wait = Clock::now();  // reply timestamps are taken here, right after waking
            for (int i = 0; i < ready; i++) {
                LoadConnection& connection = connections_[events[i].data.u32];
                if (connection.broken) continue;
                if (events[i].events & EPOLLOUT) {
                    if (!flush_requests(connection)) mark_broken(connection);
                }
                if (!connection.broken && (events[i].events & (EPOLLIN | EPOLLHUP | EPOLLERR))) {
                    if (!handle_replies(connection, after_wait)) mark_broken(connection);
                }
            }
        }

        for (LoadConnection& connection : connections_) close(connection.fd);
        close(epoll_fd_);
    }

private:
    void reserve_latency_storage() {
        size_t expected = 1 << 20;
        if (!options_.closed_loop) {
            double per_thread_rate = options_.target_rate / options_.threads;
            expected = (size_t)(per_thread_rate * options_.measure_seconds * 1.2) + 1024;
        }
        result_.latencies_ns.reserve(expected);
    }

    // Builds one request frame: [length][REQUEST][request id][padding...]
    void queue_request(LoadConnection& connection, TimePoint intended_send_time) {
        size_t start = connection.out_bytes.size();
        connection.out_bytes.resize(start + HEADER_SIZE + options_.payload_bytes, 0x5A);
        unsigned char* frame = connection.out_bytes.data() + start;
        write_length_prefix(frame, options_.payload_bytes);
        frame[HEADER_SIZE] = MESSAGE_TYPE_REQUEST;
        write_sequence_id(frame + HEADER_SIZE + 1, connection.next_request_id++);

        connection.in_flight.push_back(intended_send_time);
        in_flight_total_++;
        if (intended_send_time >= measure_start_ && intended_send_time < measure_end_) {
            result_.sent_in_window++;
        }
        if (!flush_requests(connection)) mark_broken(connection);
    }

    // Sends what the socket takes. Asks epoll for EPOLLOUT only while something is left over.
    bool flush_requests(LoadConnection& connection) {
        while (connection.out_offset < connection.out_bytes.size()) {
            ssize_t sent = send(connection.fd, connection.out_bytes.data() + connection.out_offset,
                                connection.out_bytes.size() - connection.out_offset, MSG_NOSIGNAL);
            if (sent < 0) {
                if (errno == EINTR) continue;
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                return false;
            }
            connection.out_offset += (size_t)sent;
        }
        bool leftover = connection.out_offset < connection.out_bytes.size();
        if (!leftover) {
            connection.out_bytes.clear();
            connection.out_offset = 0;
        }
        uint32_t wanted = leftover ? (EPOLLIN | EPOLLOUT) : EPOLLIN;
        if (wanted != connection.registered_events) {
            epoll_event event{};
            event.events = wanted;
            event.data.u32 = (uint32_t)(&connection - connections_.data());
            epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, connection.fd, &event);
            connection.registered_events = wanted;
        }
        return true;
    }

    bool handle_replies(LoadConnection& connection, TimePoint received_at) {
        ssize_t received = recv(connection.fd, receive_scratch_.data(), receive_scratch_.size(), 0);
        if (received == 0) return false;  // the server hung up on us
        if (received < 0) return errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK;
        connection.in_bytes.insert(connection.in_bytes.end(), receive_scratch_.begin(),
                                   receive_scratch_.begin() + received);

        size_t consumed = 0;
        for (;;) {
            uint32_t payload_length = 0;
            FrameStatus status = check_frame(connection.in_bytes.data() + consumed,
                                             connection.in_bytes.size() - consumed, &payload_length);
            if (status == FRAME_INCOMPLETE) break;
            if (status == FRAME_TOO_LARGE) return false;

            const unsigned char* payload = connection.in_bytes.data() + consumed + HEADER_SIZE;
            consumed += HEADER_SIZE + payload_length;

            if (connection.in_flight.empty()) {  // a reply we never asked for
                result_.invalid_replies++;
                continue;
            }
            TimePoint intended = connection.in_flight.front();
            connection.in_flight.pop_front();
            in_flight_total_--;

            bool valid = reply_is_valid(connection, payload, payload_length);
            connection.next_reply_id++;
            if (!valid) result_.invalid_replies++;

            if (intended >= measure_start_ && intended < measure_end_) {
                long long nanoseconds =
                    std::chrono::duration_cast<std::chrono::nanoseconds>(received_at - intended).count();
                if (nanoseconds < 0) nanoseconds = 0;
                result_.latencies_ns.push_back((uint32_t)std::min<long long>(nanoseconds, UINT32_MAX));
                result_.received_in_window++;
            }

            // closed loop: the reply is the signal to send the next request
            if (options_.closed_loop && received_at < measure_end_) {
                queue_request(connection, received_at);
                if (connection.broken) return true;
            }
        }
        connection.in_bytes.erase(connection.in_bytes.begin(), connection.in_bytes.begin() + (long)consumed);
        return true;
    }

    bool reply_is_valid(const LoadConnection& connection, const unsigned char* payload, uint32_t length) {
        if (options_.mode == ServerMode::ECHO) {
            return length == options_.payload_bytes && payload[0] == MESSAGE_TYPE_REQUEST &&
                   read_sequence_id(payload + 1) == connection.next_reply_id;
        }
        return length == MESSAGE_PREFIX_SIZE && payload[0] == MESSAGE_TYPE_ACK &&
               read_sequence_id(payload + 1) == connection.next_reply_id;
    }

    void mark_broken(LoadConnection& connection) {
        if (connection.broken) return;
        connection.broken = true;
        result_.broken_connections++;
        in_flight_total_ -= connection.in_flight.size();
        connection.in_flight.clear();
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, connection.fd, nullptr);
    }

    const Options& options_;
    int thread_index_;
    StartGate& gate_;
    ThreadResult& result_;

    int epoll_fd_ = -1;
    std::vector<LoadConnection> connections_;
    std::vector<unsigned char> receive_scratch_;
    size_t in_flight_total_ = 0;
    Duration interval_{};  // time between requests on one connection (open loop)
    TimePoint measure_start_;
    TimePoint measure_end_;
};

// ---------------------------------------------------------------- command line and report

bool parse_options(int argc, char** argv, Options& options) {
    for (int i = 1; i < argc; i++) {
        std::string option = argv[i];
        auto next_value = [&](const char* name) -> const char* {
            if (i + 1 >= argc) {
                fprintf(stderr, "%s needs a value\n", name);
                return nullptr;
            }
            return argv[++i];
        };

        if (option == "--closed-loop") {
            options.closed_loop = true;
        } else if (option == "--csv") {
            options.csv = true;
        } else if (option == "--csv-header") {
            puts(CSV_HEADER);
            exit(0);
        } else {
            const char* value = next_value(option.c_str());
            if (value == nullptr) return false;
            if (option == "--host") options.host = value;
            else if (option == "--port") options.port = atoi(value);
            else if (option == "--connections") options.connections = atoi(value);
            else if (option == "--threads") options.threads = atoi(value);
            else if (option == "--rate") options.target_rate = atof(value);
            else if (option == "--payload-bytes") options.payload_bytes = (uint32_t)atoi(value);
            else if (option == "--warmup") options.warmup_seconds = atof(value);
            else if (option == "--duration") options.measure_seconds = atof(value);
            else if (option == "--mode") {
                if (strcmp(value, "echo") == 0) options.mode = ServerMode::ECHO;
                else if (strcmp(value, "ack") == 0) options.mode = ServerMode::ACK;
                else {
                    fprintf(stderr, "--mode must be echo or ack\n");
                    return false;
                }
            } else {
                fprintf(stderr, "unknown option '%s'\n", option.c_str());
                return false;
            }
        }
    }

    if (options.connections < 1 || options.threads < 1 || options.port < 1 || options.port > 65535 ||
        options.payload_bytes < MESSAGE_PREFIX_SIZE || options.payload_bytes > MAX_PAYLOAD_SIZE ||
        options.measure_seconds <= 0 || options.warmup_seconds < 0) {
        fprintf(stderr, "bad option value (payload must be %zu..%u bytes)\n", MESSAGE_PREFIX_SIZE, MAX_PAYLOAD_SIZE);
        return false;
    }
    if (!options.closed_loop && options.target_rate <= 0) {
        fprintf(stderr, "open loop needs --rate R (requests per second), or use --closed-loop\n");
        return false;
    }
    if (options.threads > options.connections) options.threads = options.connections;
    return true;
}

// p between 0 and 100, `sorted` must be sorted ascending
double percentile_us(const std::vector<uint32_t>& sorted, double p) {
    if (sorted.empty()) return 0;
    size_t rank = (size_t)std::ceil(p / 100.0 * (double)sorted.size());
    size_t index = rank == 0 ? 0 : std::min(rank - 1, sorted.size() - 1);
    return sorted[index] / 1000.0;
}

}  // namespace

int main(int argc, char** argv) {
    Options options;
    if (!parse_options(argc, argv, options)) return 1;

    StartGate gate;
    std::vector<ThreadResult> results(options.threads);
    std::vector<std::unique_ptr<LoadThread>> load_threads;
    std::vector<std::thread> threads;
    for (int i = 0; i < options.threads; i++) {
        load_threads.push_back(std::make_unique<LoadThread>(options, i, gate, results[i]));
        threads.emplace_back([&load_threads, i] { load_threads[i]->run(); });
    }

    {
        std::unique_lock<std::mutex> lock(gate.mutex);
        gate.changed.wait(lock, [&] { return gate.threads_ready == options.threads; });
        gate.start_time = Clock::now() + std::chrono::milliseconds(200);
        gate.released = true;
        gate.changed.notify_all();
    }
    for (auto& thread : threads) thread.join();

    // merge what the threads measured
    std::vector<uint32_t> latencies;
    uint64_t sent = 0, received = 0, invalid = 0, broken = 0;
    bool could_not_connect = false;
    for (ThreadResult& result : results) {
        latencies.insert(latencies.end(), result.latencies_ns.begin(), result.latencies_ns.end());
        sent += result.sent_in_window;
        received += result.received_in_window;
        invalid += result.invalid_replies;
        broken += result.broken_connections;
        could_not_connect = could_not_connect || result.could_not_connect;
    }
    if (could_not_connect) {
        fprintf(stderr, "could not connect all connections, no result\n");
        return 1;
    }
    std::sort(latencies.begin(), latencies.end());

    uint64_t lost = sent > received ? sent - received : 0;
    double throughput = (double)received / options.measure_seconds;
    double p50 = percentile_us(latencies, 50), p90 = percentile_us(latencies, 90);
    double p99 = percentile_us(latencies, 99), p999 = percentile_us(latencies, 99.9);
    double max = latencies.empty() ? 0 : latencies.back() / 1000.0;
    const char* mode_name = options.mode == ServerMode::ECHO ? "echo" : "ack";

    fprintf(stderr, "load_generator results\n");
    fprintf(stderr, "  server            %s:%d, mode %s\n", options.host.c_str(), options.port, mode_name);
    fprintf(stderr, "  connections       %d (on %d thread(s)), payload %u bytes\n", options.connections,
            options.threads, options.payload_bytes);
    if (options.closed_loop) {
        fprintf(stderr, "  pattern           closed loop, one request in flight per connection\n");
    } else {
        fprintf(stderr, "  pattern           open loop, target %.0f requests/s\n", options.target_rate);
    }
    fprintf(stderr, "  measured window   %.1f s (after %.1f s warm-up)\n", options.measure_seconds, options.warmup_seconds);
    fprintf(stderr, "  sent / received   %llu / %llu  (lost or unanswered: %llu)\n", (unsigned long long)sent,
            (unsigned long long)received, (unsigned long long)lost);
    fprintf(stderr, "  invalid replies   %llu, broken connections %llu\n", (unsigned long long)invalid,
            (unsigned long long)broken);
    fprintf(stderr, "  throughput        %.0f replies/s\n", throughput);
    fprintf(stderr, "  latency (us)      p50 %.1f  p90 %.1f  p99 %.1f  p99.9 %.1f  max %.1f\n", p50, p90, p99, p999, max);

    if (options.csv) {
        printf("%d,%d,%s,%u,%.0f,%d,%llu,%llu,%llu,%llu,%.0f,%.1f,%.1f,%.1f,%.1f,%.1f\n", options.connections,
               options.threads, mode_name, options.payload_bytes, options.closed_loop ? 0.0 : options.target_rate,
               options.closed_loop ? 1 : 0, (unsigned long long)sent, (unsigned long long)received,
               (unsigned long long)lost, (unsigned long long)invalid, throughput, p50, p90, p99, p999, max);
    }
    return invalid > 0 || broken > 0 ? 2 : 0;
}
