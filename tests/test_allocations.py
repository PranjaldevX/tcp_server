#!/usr/bin/env python3
"""Checks SRS FR-16: after warm-up, serving traffic must not allocate heap memory on worker threads.

    python3 tests/test_allocations.py build/epoll_server_tracking

Needs the server built with -DTRACK_ALLOCATIONS (make build/epoll_server_tracking). That build
prints "heap allocations on worker threads so far: N" after every stats line. The test sends
steady traffic, notes N, sends more traffic, and N must not have changed.
"""
import re
import socket
import struct
import subprocess
import sys
import tempfile
import time

BINARY = sys.argv[1] if len(sys.argv) > 1 else "build/epoll_server_tracking"
CONNECTIONS = 50


def make_frame(payload):
    return struct.pack(">I", len(payload)) + payload


def allocation_counts(log_path):
    with open(log_path) as log_file:
        return [int(n) for n in re.findall(r"heap allocations on worker threads so far: (\d+)", log_file.read())]


def send_traffic(sockets, seconds):
    """Round-robin ping-pong over all connections for the given time. Mixed payload sizes."""
    payloads = [b"s" * 10, b"m" * 1000, b"l" * 20000]
    end = time.time() + seconds
    rounds = 0
    while time.time() < end:
        frame = make_frame(payloads[rounds % len(payloads)])
        for sock in sockets:
            sock.sendall(frame)
        for sock in sockets:
            received = 0
            while received < len(frame):
                chunk = sock.recv(len(frame) - received)
                if not chunk:
                    raise RuntimeError("server closed a connection")
                received += len(chunk)
        rounds += 1
    return rounds


def main():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".log", delete=False) as log:
        log_path = log.name
        server = subprocess.Popen([BINARY, "--port", str(free_port()), "--stats-interval", "1"],
                                  stderr=log, stdout=subprocess.DEVNULL)
    try:
        port = None
        deadline = time.time() + 10
        while time.time() < deadline and port is None:
            with open(log_path) as log_file:
                match = re.search(r"listening on port (\d+)", log_file.read())
            if match:
                port = int(match.group(1))
            else:
                time.sleep(0.05)
        if port is None:
            raise SystemExit("server did not start")

        sockets = [socket.create_connection(("127.0.0.1", port), timeout=10) for _ in range(CONNECTIONS)]

        send_traffic(sockets, 3)   # warm-up: buffers grow to the sizes they need, once
        time.sleep(1.5)            # let a stats line with the post-warm-up count appear
        before = allocation_counts(log_path)[-1]

        rounds = send_traffic(sockets, 4)   # steady state
        time.sleep(1.5)
        after = allocation_counts(log_path)[-1]

        print("steady-state rounds: %d (x %d connections), worker-thread allocations: before=%d after=%d"
              % (rounds, CONNECTIONS, before, after))
        if after != before:
            raise SystemExit("FAIL: %d heap allocations on the hot path during steady state" % (after - before))
        print("test_allocations: ok, no heap allocations during steady state")
    finally:
        server.terminate()
        server.wait()


def free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


if __name__ == "__main__":
    main()
