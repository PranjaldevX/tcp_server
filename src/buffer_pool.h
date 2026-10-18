#pragma once
#include <cstddef>
#include <memory>
#include <vector>

#include "message_handler.h"  // ByteVector
#include "server_stats.h"     // Counter

// Hands out reusable byte buffers, so the steady-state path doesn't call malloc.
// A released buffer is emptied but keeps its capacity, so the next user doesn't allocate either.
// Not thread safe: every worker owns its own pool.
class BufferPool {
public:
    // Creates `initial_buffers` buffers up front, each with room for `buffer_capacity` bytes.
    // `allocation_counter` counts buffers created AFTER startup (should stay at 0 in steady state).
    BufferPool(size_t initial_buffers, size_t buffer_capacity, Counter& allocation_counter)
        : buffer_capacity_(buffer_capacity), allocation_counter_(allocation_counter) {
        for (size_t i = 0; i < initial_buffers; i++) add_new_buffer();
    }

    ByteVector* acquire() {
        if (free_buffers_.empty()) {
            allocation_counter_.add();
            add_new_buffer();
        }
        ByteVector* buffer = free_buffers_.back();
        free_buffers_.pop_back();
        return buffer;
    }

    void release(ByteVector* buffer) {
        buffer->clear();  // size goes to 0, capacity stays
        free_buffers_.push_back(buffer);
    }

private:
    void add_new_buffer() {
        all_buffers_.push_back(std::make_unique<ByteVector>());
        all_buffers_.back()->reserve(buffer_capacity_);
        // free_buffers_ can never hold more entries than there are buffers, so reserving here means
        // release() never has to grow it later
        free_buffers_.reserve(all_buffers_.size());
        free_buffers_.push_back(all_buffers_.back().get());
    }

    size_t buffer_capacity_;
    Counter& allocation_counter_;
    std::vector<std::unique_ptr<ByteVector>> all_buffers_;  // owns every buffer
    std::vector<ByteVector*> free_buffers_;                 // the ones nobody is using right now
};
