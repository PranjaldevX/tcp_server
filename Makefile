CXX      ?= g++
BASE     := -std=c++17 -Wall -Wextra -pthread
OPTIMIZE := -O2
ADDRESS  := -fsanitize=address,undefined -fno-omit-frame-pointer -g -O1
THREAD   := -fsanitize=thread -g -O1

EPOLL_SOURCES    := src/epoll_server_main.cpp src/worker.cpp src/server_config.cpp src/allocation_tracking.cpp
BASELINE_SOURCES := src/baseline_server_main.cpp src/server_config.cpp
HEADERS          := $(wildcard src/*.h)

all: build/epoll_server build/baseline_server build/load_generator

# ---- the programs

build/epoll_server: $(EPOLL_SOURCES) $(HEADERS)
	@mkdir -p build
	$(CXX) $(BASE) $(OPTIMIZE) -o $@ $(EPOLL_SOURCES)

build/baseline_server: $(BASELINE_SOURCES) $(HEADERS)
	@mkdir -p build
	$(CXX) $(BASE) $(OPTIMIZE) -o $@ $(BASELINE_SOURCES)

build/load_generator: tools/load_generator.cpp $(HEADERS)
	@mkdir -p build
	$(CXX) $(BASE) $(OPTIMIZE) -o $@ tools/load_generator.cpp

# ---- special builds of the epoll server

# with AddressSanitizer + UBSan (finds memory bugs, leaks, undefined behaviour)
build/epoll_server_asan: $(EPOLL_SOURCES) $(HEADERS)
	@mkdir -p build
	$(CXX) $(BASE) $(ADDRESS) -o $@ $(EPOLL_SOURCES)

# with ThreadSanitizer (finds data races between the worker threads)
build/epoll_server_tsan: $(EPOLL_SOURCES) $(HEADERS)
	@mkdir -p build
	$(CXX) $(BASE) $(THREAD) -o $@ $(EPOLL_SOURCES)

# counts heap allocations made on worker threads (for the "no malloc on the hot path" check)
build/epoll_server_tracking: $(EPOLL_SOURCES) $(HEADERS)
	@mkdir -p build
	$(CXX) $(BASE) $(OPTIMIZE) -DTRACK_ALLOCATIONS -o $@ $(EPOLL_SOURCES)

# ---- unit tests (always built with ASan + UBSan, and with asserts on)

build/test_frame_protocol: tests/test_frame_protocol.cpp src/frame_protocol.h
	@mkdir -p build
	$(CXX) $(BASE) $(ADDRESS) -o $@ tests/test_frame_protocol.cpp

build/test_message_handler: tests/test_message_handler.cpp src/message_handler.h src/frame_protocol.h
	@mkdir -p build
	$(CXX) $(BASE) $(ADDRESS) -o $@ tests/test_message_handler.cpp

build/test_buffer_pool: tests/test_buffer_pool.cpp $(HEADERS)
	@mkdir -p build
	$(CXX) $(BASE) $(ADDRESS) -o $@ tests/test_buffer_pool.cpp

# ---- running the tests

test: test-unit test-integration

test-unit: build/test_frame_protocol build/test_message_handler build/test_buffer_pool
	./build/test_frame_protocol
	./build/test_message_handler
	./build/test_buffer_pool

# integration tests against the real binaries: normal build, then the ASan build
test-integration: build/epoll_server build/epoll_server_asan build/baseline_server
	SERVER_BINARY=build/epoll_server BASELINE_BINARY=build/baseline_server python3 tests/test_server.py
	SANITIZED_BUILD=1 SERVER_BINARY=build/epoll_server_asan python3 tests/test_server.py

# the same tests against the ThreadSanitizer build (skips the 10k connection test, TSan is slow)
test-tsan: build/epoll_server_tsan
	SANITIZED_BUILD=1 SKIP_HEAVY=1 SERVER_BINARY=build/epoll_server_tsan python3 tests/test_server.py

# checks that steady-state traffic causes no heap allocations on the worker threads
test-allocations: build/epoll_server_tracking
	python3 tests/test_allocations.py build/epoll_server_tracking

# ---- benchmarks (see README): numbers only mean something on a native Linux machine
benchmark: all
	scripts/run_benchmark.sh

clean:
	rm -rf build

.PHONY: all test test-unit test-integration test-tsan test-allocations benchmark clean
