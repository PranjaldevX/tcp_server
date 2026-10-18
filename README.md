# tcpd

Single-node, Linux-only, epoll-based TCP server in C++17, built against the SRS
("Software Requirements Specification: High-Performance TCP Server").
IPv4 only, no third-party libraries, nothing but the standard library and Linux system calls.

## Layout

```
tcpd/
├── Makefile
├── README.md
├── src/
│   ├── frame_protocol.h          frame = 4-byte length + payload, check_frame()
│   ├── message_handler.h         echo / request-ack reply logic (shared by both servers)
│   ├── server_config.h / .cpp    all command-line options
│   ├── server_stats.h            per-worker counters, totals, stats line
│   ├── buffer_pool.h             reusable byte buffers (no malloc on the hot path)
│   ├── connection.h              state kept for each client
│   ├── worker.h / .cpp           ONE epoll reactor thread  <- the heart of the project
│   ├── epoll_server_main.cpp     starts workers, signals, stats printing, shutdown
│   ├── baseline_server_main.cpp  thread-per-connection baseline (FR-19)
│   └── allocation_tracking.h/.cpp  debug build that counts heap allocations (FR-16 check)
├── tools/
│   └── load_generator.cpp        open-loop / closed-loop load generator (FR-20)
├── scripts/
│   ├── collect_environment.sh    machine details for the benchmark report (PR-5)
│   ├── run_benchmark.sh          baseline vs epoll, several runs, CSV out
│   └── summarize_results.py      median / min / max per server
└── tests/
    ├── test_frame_protocol.cpp   unit tests
    ├── test_message_handler.cpp  unit tests
    ├── test_buffer_pool.cpp      unit tests
    ├── test_server.py            integration tests against the real binaries (30 tests)
    └── test_allocations.py       "no heap allocation in steady state" check
```

## Build and run (Linux, WSL2 is fine for development)

```
sudo apt install -y build-essential python3
make                                   # build/epoll_server, build/baseline_server, build/load_generator
./build/epoll_server --port 9000       # echo mode, 1 worker
./build/epoll_server --port 9000 --mode ack --workers 4 --stats-interval 2
```

Options (`./build/epoll_server --help`):

| Option | Meaning | Default |
| --- | --- | --- |
| `--port N` | listening port | 9000 |
| `--mode echo\|ack` | send payloads back, or reply with a sequence-id ack | echo |
| `--workers N` | epoll worker threads, each with its own `SO_REUSEPORT` listener | 1 |
| `--max-connections N` | limit **per worker**; extra clients are accepted and dropped | 100000 |
| `--idle-timeout SEC` | close connections silent this long, 0 = never | 60 |
| `--write-buffer-cap B` | stop reading from a client once this many reply bytes wait | 1048576 |
| `--stats-interval SEC` | stats line every SEC seconds, 0 = off | 5 |
| `--pool-buffers N` | reusable buffers per worker created at startup | 64 |
| `--tcp-nodelay` / `--writev` / `--pin-cpus` | the v2 tuning switches (off by default so each can be measured) | off |

Ctrl+C (SIGINT) or SIGTERM: stop accepting, close every connection, print a `[final]` stats line, exit 0.

## Wire format

One frame = 4-byte payload length (big endian) + payload. Maximum payload 64 KiB (`MAX_PAYLOAD_SIZE`);
a bigger length closes the connection.

- **echo mode:** the reply is the same frame.
- **ack mode:** request payload = `[type 1][sequence id, 4 bytes][optional body]`, reply payload = `[type 2][same sequence id]`.
  A request that is too short or has the wrong type closes the connection.

## How to read the code (suggested order)

1. `frame_protocol.h`, then `tests/test_frame_protocol.cpp`: how a byte stream becomes messages.
2. `message_handler.h`: what the server does with a message.
3. `connection.h`: what we remember about one client.
4. `worker.cpp`, in this order: `run()`, `accept_new_connections()`, `handle_readable()`,
   `send_or_queue()`, `handle_writable()`, `update_epoll_interest()`, `close_connection()`.
5. `epoll_server_main.cpp`: how workers are started and stopped.
6. `baseline_server_main.cpp`: the same job done the simple blocking way. Compare it with step 4.
7. `tools/load_generator.cpp`.

One request, start to finish: epoll says a socket is readable -> `recv()` into the scratch buffer ->
`check_frame()` finds whole frames -> `append_reply()` builds the replies -> `send()` right away.
Only if the socket is full are the unsent bytes kept in a pooled buffer, `EPOLLOUT` is switched on, and
(if too many bytes pile up) `EPOLLIN` is switched off until they drain.

## Tests

```
make test              # unit tests + integration tests (normal build, then ASan/UBSan build)
make test-tsan         # integration tests under ThreadSanitizer (skips the 10k connection test)
make test-allocations  # steady state must cause 0 heap allocations on worker threads
```

Every integration test also requires the server to exit with code 0 on SIGTERM, so under a sanitizer
build a leak, memory error or data race anywhere in a test fails that test.

## Benchmarking

```
make
scripts/run_benchmark.sh 5 100 20000 64 10      # runs, connections, requests/s, payload bytes, seconds
python3 scripts/summarize_results.py benchmark_results/results_<timestamp>.csv
```

- Final numbers must come from **native Linux** (or a cloud VM), not WSL2: it is virtualized and noisy (SRS 2.4).
  `scripts/collect_environment.sh` records the machine details; the benchmark script saves them next to the CSV.
- Server and load generator share the machine, so give them different cores (`taskset`) or use a second machine.
  Add `--threads` to the generator if one thread can't produce the rate.
- The generator is open loop by default: latency counts from the *intended* send time, which avoids coordinated omission.
  Run it once with `--closed-loop` to see how much a closed-loop test would hide.
- To measure a v2 switch before/after (PR-3), edit the `SERVERS` list in `run_benchmark.sh`
  (e.g. add `epoll_writev|build/epoll_server --writev`).

## SRS traceability

| Requirement | Where | Checked by |
| --- | --- | --- |
| FR-1 non-blocking accept | `Worker::accept_new_connections` | all integration tests |
| FR-2 epoll reactor | `Worker::run` | all integration tests |
| FR-3 per-connection state | `connection.h` | |
| FR-4 partial reads | `Worker::handle_readable` (`unparsed_bytes`) | byte-by-byte and split-header tests |
| FR-5 partial writes | `Worker::send_or_queue`, `handle_writable` | slow-reader tests (also with `--writev`) |
| FR-6 clean close | `Worker::close_connection` | client-leaves-mid-frame test, sanitizer exit codes |
| FR-7 idle timeout | `Worker::close_idle_connections` | idle / keeps-talking tests |
| FR-8 framing | `frame_protocol.h` | `test_frame_protocol` |
| FR-9 oversize frame | `check_frame` -> `PROTOCOL_ERROR` | oversize test |
| FR-10 echo, FR-11 ack | `message_handler.h` | `test_message_handler`, echo and ack tests |
| FR-12 backpressure | `Worker::update_epoll_interest` | does-not-read test (client blocks, memory bounded, `reads_paused` > 0) |
| FR-13 counters | `server_stats.h` | stats test |
| FR-14 graceful shutdown | `epoll_server_main.cpp`, `Worker::run` | SIGTERM test |
| FR-15 multi-reactor | one `Worker` per thread, `SO_REUSEPORT` | 2- and 4-worker tests, TSan run |
| FR-16 buffer pool | `buffer_pool.h` | `test_buffer_pool`, `test_allocations.py` |
| FR-17 `TCP_NODELAY`, `writev`, pinning | `--tcp-nodelay`, `--writev`, `--pin-cpus` | functional tests only (see below) |
| FR-18 order-book gateway (optional) | **not implemented** | |
| FR-19 baseline server | `baseline_server_main.cpp` | baseline tests |
| FR-20 load generator | `tools/load_generator.cpp` | smoke-tested against both servers |
| PR-1 10,000 idle connections | | `test_ten_thousand_idle_connections` |
| PR-2 .. PR-6 benchmark rules | `scripts/` | **needs your measurements** |

## What has and has not been verified

Verified (in a Linux sandbox with 1 CPU core): everything compiles without warnings under `-Wall -Wextra`;
all unit tests pass; all 30 integration tests pass on the normal build, the ASan/UBSan build, and
(minus the 10k test) the ThreadSanitizer build; steady-state traffic caused no heap allocations on worker
threads; the load generator and benchmark scripts run end to end.

**Not** done, because it can only be done on your machine:
- No performance numbers are claimed. The sandbox has one core and runs inside a container, so any
  throughput or latency from it is meaningless. PR-2 (epoll beats baseline) and PR-3 (each v2 switch is a
  measured gain) are open until you run the benchmark on native Linux.
- FR-17 is only checked for *working*, not for being *faster*. It may turn out to make no difference or
  to be slower. That is what the before/after run is for.
- The buffer pool is always on, so there is no switch to compare "with" and "without".

## Design notes and known limits

- Level-triggered epoll only. Edge-triggered was left out on purpose (more ways to lose events).
- `--max-connections` is per worker, and `SO_REUSEPORT` spreads connections by hash, so with several
  workers one can fill up before the others.
- A protocol error closes the connection at once; replies built in the same batch but not yet sent are dropped.
- `messages_sent` counts replies handed to the socket layer (sent or queued).
- The backpressure cap is soft: the backlog can exceed it by at most one `recv()` batch (64 KiB) of replies.
- Pooled buffers keep their capacity, so a few buffers that once held a big backlog stay big.
- The idle sweep walks all connection slots once per second, which is fine at 10k connections.
- `test_allocations.py` exercises the common path (parse from the scratch buffer, send directly). The
  slow-client path (queued bytes, partial frames) allocates only while pool buffers are still growing.
- Linux only, IPv4 only, no TLS, no authentication: run it on trusted networks.
