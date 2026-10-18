#pragma once
#include <chrono>
#include <cstddef>
#include <cstdint>

#include "message_handler.h"  // ByteVector

using Clock = std::chrono::steady_clock;

enum class CloseReason { PEER_CLOSED, IO_ERROR, IDLE_TIMEOUT, PROTOCOL_ERROR, SERVER_SHUTDOWN };

// Everything a worker remembers about one client.
// A worker keeps these in a vector indexed by file descriptor, so lookup is a plain array access.
struct Connection {
    bool is_open = false;
    int fd = -1;
    Clock::time_point last_activity;

    // Start of a frame that has only partly arrived. Null while nothing is waiting, and only
    // then does an idle connection cost no buffer memory at all.
    ByteVector* unparsed_bytes = nullptr;

    // Reply bytes the socket would not take yet. Null while nothing is waiting.
    ByteVector* unsent_bytes = nullptr;
    size_t unsent_offset = 0;  // how much of unsent_bytes already went out

    uint32_t registered_events = 0;  // what epoll was last told we care about

    size_t unsent_byte_count() const {
        return unsent_bytes ? unsent_bytes->size() - unsent_offset : 0;
    }
};
