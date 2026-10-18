#include "allocation_tracking.h"

#ifdef TRACK_ALLOCATIONS

#include <atomic>
#include <cstdlib>
#include <new>

namespace {
thread_local bool tracking_this_thread = false;
std::atomic<uint64_t> tracked_allocations{0};
}  // namespace

void track_allocations_on_this_thread(bool enabled) { tracking_this_thread = enabled; }

uint64_t tracked_allocation_count() { return tracked_allocations.load(std::memory_order_relaxed); }

// Every `new` in the program goes through here. The array forms and std::allocator call
// this one too, so std::vector growth is counted as well.
void* operator new(std::size_t size) {
    if (tracking_this_thread) tracked_allocations.fetch_add(1, std::memory_order_relaxed);
    void* memory = std::malloc(size ? size : 1);
    if (memory == nullptr) throw std::bad_alloc();
    return memory;
}

void operator delete(void* memory) noexcept { std::free(memory); }
void operator delete(void* memory, std::size_t) noexcept { std::free(memory); }

#endif  // TRACK_ALLOCATIONS
