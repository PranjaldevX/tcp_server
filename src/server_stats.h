#pragma once
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <vector>

// One worker thread writes a Counter, the supervisor thread reads it.
// Relaxed atomics keep the reads race-free, and since there is only ever one writer the
// increment is a plain load + store: no locked instruction on the hot path.
class Counter {
public:
    void add(uint64_t amount = 1) {
        value_.store(value_.load(std::memory_order_relaxed) + amount, std::memory_order_relaxed);
    }
    uint64_t get() const { return value_.load(std::memory_order_relaxed); }

private:
    std::atomic<uint64_t> value_{0};
};

struct WorkerStats {
    Counter connections_accepted;
    Counter connections_rejected_over_limit;
    Counter accept_errors;
    Counter messages_received;  // complete request frames
    Counter messages_sent;      // replies handed to the socket layer (sent or queued)
    Counter bytes_received;
    Counter bytes_sent;
    Counter closed_by_peer;
    Counter closed_by_io_error;
    Counter closed_by_idle_timeout;
    Counter closed_by_protocol_error;
    Counter closed_by_shutdown;
    Counter reads_paused_by_backpressure;
    Counter buffer_pool_allocations;  // buffers created after startup, should stay flat
};

// The same numbers, summed over all workers, as plain integers.
struct StatsTotals {
    uint64_t connections_accepted = 0;
    uint64_t connections_rejected_over_limit = 0;
    uint64_t accept_errors = 0;
    uint64_t messages_received = 0;
    uint64_t messages_sent = 0;
    uint64_t bytes_received = 0;
    uint64_t bytes_sent = 0;
    uint64_t closed_by_peer = 0;
    uint64_t closed_by_io_error = 0;
    uint64_t closed_by_idle_timeout = 0;
    uint64_t closed_by_protocol_error = 0;
    uint64_t closed_by_shutdown = 0;
    uint64_t reads_paused_by_backpressure = 0;
    uint64_t buffer_pool_allocations = 0;

    uint64_t total_closed() const {
        return closed_by_peer + closed_by_io_error + closed_by_idle_timeout +
               closed_by_protocol_error + closed_by_shutdown;
    }
    uint64_t active_connections() const {
        uint64_t closed = total_closed();
        return connections_accepted > closed ? connections_accepted - closed : 0;
    }
};

inline void add_to_totals(const WorkerStats& worker, StatsTotals& totals) {
    // The closed counters are read BEFORE the accepted counter on purpose: a connection is
    // always counted as accepted before it can be counted as closed, so reading in this order
    // never shows more closed than accepted, even though the worker keeps running meanwhile.
    totals.closed_by_peer += worker.closed_by_peer.get();
    totals.closed_by_io_error += worker.closed_by_io_error.get();
    totals.closed_by_idle_timeout += worker.closed_by_idle_timeout.get();
    totals.closed_by_protocol_error += worker.closed_by_protocol_error.get();
    totals.closed_by_shutdown += worker.closed_by_shutdown.get();
    totals.connections_accepted += worker.connections_accepted.get();

    totals.connections_rejected_over_limit += worker.connections_rejected_over_limit.get();
    totals.accept_errors += worker.accept_errors.get();
    totals.messages_received += worker.messages_received.get();
    totals.messages_sent += worker.messages_sent.get();
    totals.bytes_received += worker.bytes_received.get();
    totals.bytes_sent += worker.bytes_sent.get();
    totals.reads_paused_by_backpressure += worker.reads_paused_by_backpressure.get();
    totals.buffer_pool_allocations += worker.buffer_pool_allocations.get();
}

inline StatsTotals collect_totals(const std::vector<const WorkerStats*>& workers) {
    StatsTotals totals;
    for (const WorkerStats* worker : workers) add_to_totals(*worker, totals);
    return totals;
}

// `before` is the previous snapshot, `seconds` the time between the two (for the msgs/s rate).
inline void print_stats_line(const char* label, const StatsTotals& now, const StatsTotals& before,
                             double seconds) {
    auto n = [](uint64_t value) { return static_cast<unsigned long long>(value); };
    double messages_per_second =
        seconds > 0 ? (double)(now.messages_received - before.messages_received) / seconds : 0;

    fprintf(stderr,
            "[%s] active=%llu accepted=%llu msgs_in=%llu msgs_out=%llu (%.0f msgs/s) "
            "bytes_in=%llu bytes_out=%llu | closed: peer=%llu error=%llu idle=%llu protocol=%llu "
            "shutdown=%llu | rejected=%llu accept_errors=%llu reads_paused=%llu pool_allocs=%llu\n",
            label, n(now.active_connections()), n(now.connections_accepted), n(now.messages_received),
            n(now.messages_sent), messages_per_second, n(now.bytes_received), n(now.bytes_sent),
            n(now.closed_by_peer), n(now.closed_by_io_error), n(now.closed_by_idle_timeout),
            n(now.closed_by_protocol_error), n(now.closed_by_shutdown),
            n(now.connections_rejected_over_limit), n(now.accept_errors),
            n(now.reads_paused_by_backpressure), n(now.buffer_pool_allocations));
}
