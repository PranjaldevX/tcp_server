#include <cassert>
#include <cstdio>

#include "../src/buffer_pool.h"

int main() {
    Counter allocations;
    BufferPool pool(2, 100, allocations);

    // the two startup buffers are free and don't count as "allocated later"
    ByteVector* first = pool.acquire();
    ByteVector* second = pool.acquire();
    assert(first != second);
    assert(allocations.get() == 0);
    assert(first->capacity() >= 100);

    // a third one has to be created, and that is counted
    ByteVector* third = pool.acquire();
    assert(allocations.get() == 1);

    // released buffers come back empty but keep their capacity, and are reused (no new allocation)
    first->assign(50, 0x11);
    pool.release(first);
    ByteVector* again = pool.acquire();
    assert(again == first);
    assert(again->empty());
    assert(again->capacity() >= 100);
    assert(allocations.get() == 1);

    // release everything, then take everything again: still nothing new allocated
    pool.release(again);
    pool.release(second);
    pool.release(third);
    for (int i = 0; i < 3; i++) pool.acquire();
    assert(allocations.get() == 1);

    puts("test_buffer_pool: all checks passed");
    return 0;
}
