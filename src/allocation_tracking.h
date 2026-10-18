#pragma once
#include <cstdint>

// Debug aid for SRS FR-16 ("no heap allocation on the steady-state hot path").
// Only does something in the build made with -DTRACK_ALLOCATIONS (make build/epoll_server_tracking).
// There, every operator new called on a thread that opted in is counted.
// In every other build these are empty functions and cost nothing.
#ifdef TRACK_ALLOCATIONS
void track_allocations_on_this_thread(bool enabled);
uint64_t tracked_allocation_count();
#else
inline void track_allocations_on_this_thread(bool) {}
inline uint64_t tracked_allocation_count() { return 0; }
#endif
