#!/usr/bin/env python3
"""Integration tests for the epoll server (and a few for the baseline server).

    SERVER_BINARY=build/epoll_server BASELINE_BINARY=build/baseline_server python3 tests/test_server.py

Environment variables:
    SERVER_BINARY     the epoll server to test (default build/epoll_server)
    BASELINE_BINARY   also run a few tests against the baseline server
    SANITIZED_BUILD=1 skip checks about memory size (sanitizers change it a lot)
    SKIP_HEAVY=1      skip the 10,000 connection test
"""
import contextlib
import os
import re
import resource
import signal
import socket
import struct
import subprocess
import tempfile
import threading
import time
import unittest

SERVER_BINARY = os.environ.get("SERVER_BINARY", "build/epoll_server")
BASELINE_BINARY = os.environ.get("BASELINE_BINARY")
SANITIZED_BUILD = os.environ.get("SANITIZED_BUILD") == "1"
SKIP_HEAVY = os.environ.get("SKIP_HEAVY") == "1"

MAX_PAYLOAD = 64 * 1024
MESSAGE_TYPE_REQUEST = 1
MESSAGE_TYPE_ACK = 2


# ---------------------------------------------------------------- helpers

def make_frame(payload):
    return struct.pack(">I", len(payload)) + payload


def make_request(sequence_id, body=b""):
    """A request as ack mode wants it: [REQUEST][sequence id][body], inside a frame."""
    return make_frame(bytes([MESSAGE_TYPE_REQUEST]) + struct.pack(">I", sequence_id) + body)


def make_ack(sequence_id):
    return make_frame(bytes([MESSAGE_TYPE_ACK]) + struct.pack(">I", sequence_id))


def find_free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class RunningServer:
    """A server process started with the given arguments, listening on a free port."""

    def __init__(self, binary, args):
        self.port = find_free_port()
        self._log = tempfile.NamedTemporaryFile(mode="w", suffix=".log", delete=False)
        self.process = subprocess.Popen(
            [binary, "--port", str(self.port)] + list(args),
            stderr=self._log, stdout=subprocess.DEVNULL)
        self._wait_until_listening()

    def _wait_until_listening(self):
        deadline = time.time() + 15
        while time.time() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError("server exited at startup:\n" + self.log())
            if "listening" in self.log():
                return
            time.sleep(0.02)
        raise RuntimeError("server did not start:\n" + self.log())

    def log(self):
        # read through a separate handle so we never move the file offset the server writes at
        with open(self._log.name) as log_file:
            return log_file.read()

    def rss_megabytes(self):
        with open("/proc/%d/status" % self.process.pid) as status:
            for line in status:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024.0
        raise RuntimeError("no VmRSS")

    def stop(self):
        """SIGTERM, wait for exit. Returns the exit code."""
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        return self.process.returncode

    def cleanup(self):
        """Stops the server and removes its log. Returns (exit code, the log text)."""
        code = self.stop()
        log_text = self.log()
        self._log.close()
        os.unlink(self._log.name)
        return code, log_text


@contextlib.contextmanager
def running_server(*args, binary=None):
    """Starts a server for the duration of a test. When the test passes, the server must also
    exit with code 0 on SIGTERM: under a sanitizer build that is how a leak or a memory error
    anywhere in the test shows up."""
    is_epoll_server = binary is None
    server = RunningServer(binary or SERVER_BINARY, args)
    try:
        yield server
    except BaseException:
        server.cleanup()
        raise
    code, log_text = server.cleanup()
    if is_epoll_server and code != 0:
        raise AssertionError("server exited with code %s, log:\n%s" % (code, log_text))


def connect(port, receive_buffer=None, timeout=10):
    sock = socket.socket()
    if receive_buffer:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, receive_buffer)
    sock.settimeout(timeout)
    sock.connect(("127.0.0.1", port))
    return sock


def read_exactly(sock, count):
    data = b""
    while len(data) < count:
        chunk = sock.recv(count - len(data))
        if not chunk:
            raise EOFError("connection closed after %d of %d bytes" % (len(data), count))
        data += chunk
    return data


def closed_by_server(sock, wait_seconds=10):
    """True if the server closes the connection within wait_seconds."""
    sock.settimeout(wait_seconds)
    try:
        return sock.recv(1) == b""
    except ConnectionResetError:
        return True
    except socket.timeout:
        return False


# ---------------------------------------------------------------- echo mode

class EchoModeTests(unittest.TestCase):
    def test_single_frame_comes_back(self):
        with running_server() as server:
            sock = connect(server.port)
            sock.sendall(make_frame(b"hello"))
            self.assertEqual(read_exactly(sock, 9), make_frame(b"hello"))

    def test_empty_payload(self):
        with running_server() as server:
            sock = connect(server.port)
            sock.sendall(make_frame(b""))
            self.assertEqual(read_exactly(sock, 4), make_frame(b""))

    def test_frame_arriving_one_byte_at_a_time(self):
        with running_server() as server:
            sock = connect(server.port)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            data = make_frame(b"split me up")
            for i in range(len(data)):
                sock.sendall(data[i:i + 1])
                time.sleep(0.003)
            self.assertEqual(read_exactly(sock, len(data)), data)

    def test_many_frames_in_one_send(self):
        with running_server() as server:
            sock = connect(server.port)
            frames = [make_frame(b"a" * n) for n in (1, 10, 100, 0, 5)]
            sock.sendall(b"".join(frames))
            for frame in frames:
                self.assertEqual(read_exactly(sock, len(frame)), frame)

    def test_frame_split_across_two_sends_in_the_middle_of_the_header(self):
        with running_server() as server:
            sock = connect(server.port)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            data = make_frame(b"0123456789")
            sock.sendall(data[:2])
            time.sleep(0.1)
            sock.sendall(data[2:])
            self.assertEqual(read_exactly(sock, len(data)), data)

    def test_largest_allowed_frame(self):
        with running_server() as server:
            sock = connect(server.port)
            payload = bytes(range(256)) * (MAX_PAYLOAD // 256)
            sock.sendall(make_frame(payload))
            self.assertEqual(read_exactly(sock, 4 + MAX_PAYLOAD), make_frame(payload))

    def test_oversize_frame_closes_the_connection_and_server_survives(self):
        with running_server() as server:
            sock = connect(server.port)
            sock.sendall(struct.pack(">I", MAX_PAYLOAD + 1))
            self.assertTrue(closed_by_server(sock))

            other = connect(server.port)
            other.sendall(make_frame(b"still alive"))
            self.assertEqual(read_exactly(other, 15), make_frame(b"still alive"))
            self.assertIn("protocol=1", self._wait_for_final_stats(server))

    def _wait_for_final_stats(self, server):
        server.stop()
        return server.log()

    def test_client_leaving_in_the_middle_of_a_frame_does_not_hurt_the_server(self):
        with running_server() as server:
            for _ in range(20):
                sock = connect(server.port)
                sock.sendall(make_frame(b"x" * 1000)[:500])  # only half of it
                sock.close()
            other = connect(server.port)
            other.sendall(make_frame(b"ok"))
            self.assertEqual(read_exactly(other, 6), make_frame(b"ok"))

    def test_two_hundred_clients_at_once(self):
        with running_server() as server:
            sockets = [connect(server.port) for _ in range(200)]
            for i, sock in enumerate(sockets):
                sock.sendall(make_frame(b"client %d" % i))
            for i, sock in enumerate(sockets):
                expected = make_frame(b"client %d" % i)
                self.assertEqual(read_exactly(sock, len(expected)), expected)

    def test_slow_reader_gets_everything_back(self):
        # a tiny receive buffer and a reader that sleeps makes the server's send() hit
        # "socket full" and finish the job later through EPOLLOUT
        self._run_slow_reader_test([])

    def _run_slow_reader_test(self, server_args):
        with running_server(*server_args) as server:
            sock = connect(server.port, receive_buffer=4096)
            frames = [make_frame(bytes([i % 256]) * 60000) for i in range(300)]
            errors = []

            def sender():
                try:
                    for frame in frames:
                        sock.sendall(frame)
                except Exception as error:  # noqa
                    errors.append(error)

            thread = threading.Thread(target=sender)
            thread.start()
            time.sleep(0.5)
            for frame in frames:
                self.assertEqual(read_exactly(sock, len(frame)), frame)
            thread.join()
            self.assertEqual(errors, [])


# ---------------------------------------------------------------- ack mode

class AckModeTests(unittest.TestCase):
    def test_ack_carries_the_sequence_id(self):
        with running_server("--mode", "ack") as server:
            sock = connect(server.port)
            for sequence_id in (0, 1, 0xDEADBEEF):
                sock.sendall(make_request(sequence_id, body=b"some body bytes"))
                self.assertEqual(read_exactly(sock, 9), make_ack(sequence_id))

    def test_many_requests_in_one_send_get_acks_in_order(self):
        with running_server("--mode", "ack") as server:
            sock = connect(server.port)
            sock.sendall(b"".join(make_request(i) for i in range(50)))
            for i in range(50):
                self.assertEqual(read_exactly(sock, 9), make_ack(i))

    def test_wrong_message_type_closes_the_connection(self):
        with running_server("--mode", "ack") as server:
            sock = connect(server.port)
            sock.sendall(make_frame(bytes([MESSAGE_TYPE_ACK]) + struct.pack(">I", 1)))
            self.assertTrue(closed_by_server(sock))

    def test_request_too_short_for_a_sequence_id_closes_the_connection(self):
        with running_server("--mode", "ack") as server:
            sock = connect(server.port)
            sock.sendall(make_frame(bytes([MESSAGE_TYPE_REQUEST, 0, 0])))
            self.assertTrue(closed_by_server(sock))


# ---------------------------------------------------------------- limits, timeouts, backpressure, shutdown

class RobustnessTests(unittest.TestCase):
    def test_idle_connection_is_closed_after_the_timeout(self):
        with running_server("--idle-timeout", "1") as server:
            sock = connect(server.port)
            self.assertTrue(closed_by_server(sock, wait_seconds=5))

    def test_connection_that_keeps_talking_is_not_closed(self):
        with running_server("--idle-timeout", "2") as server:
            sock = connect(server.port)
            for _ in range(8):  # 4 seconds, twice the timeout
                sock.sendall(make_frame(b"ping"))
                self.assertEqual(read_exactly(sock, 8), make_frame(b"ping"))
                time.sleep(0.5)

    def test_clients_over_the_connection_limit_are_dropped(self):
        with running_server("--max-connections", "5") as server:
            sockets = [connect(server.port) for _ in range(8)]
            for sock in sockets:
                sock.sendall(make_frame(b"hi"))
            answered = 0
            for sock in sockets:
                sock.settimeout(3)
                try:
                    if sock.recv(6) == make_frame(b"hi"):
                        answered += 1
                except (ConnectionResetError, socket.timeout):
                    pass
            self.assertEqual(answered, 5)

    def test_server_stops_reading_from_a_client_that_does_not_read(self):
        # With a small write buffer cap the server must stop reading once replies pile up, so
        # the client's sending blocks. Without backpressure the server would swallow all of it.
        with running_server("--write-buffer-cap", "65536") as server:
            sock = connect(server.port, receive_buffer=4096)
            frame = make_frame(b"x" * 60000)
            total_frames = 1500  # about 90 MB
            progress = {"sent": 0}

            def sender():
                for _ in range(total_frames):
                    sock.sendall(frame)
                    progress["sent"] += 1

            memory_before = server.rss_megabytes()
            thread = threading.Thread(target=sender)
            thread.start()
            time.sleep(2)
            self.assertLess(progress["sent"], total_frames, "client was never blocked: no backpressure")
            if not SANITIZED_BUILD:
                growth = server.rss_megabytes() - memory_before
                self.assertLess(growth, 40, "server memory grew by %.0f MB" % growth)

            # now read everything: it all has to arrive, intact, once reading resumes
            for _ in range(total_frames):
                self.assertEqual(read_exactly(sock, len(frame)), frame)
            thread.join()

            # the final stats line must show that reading really was paused at least once
            server.stop()
            pauses = re.findall(r"reads_paused=(\d+)", server.log())
            self.assertTrue(pauses and int(pauses[-1]) > 0, "no reads_paused recorded: " + server.log())

    def test_sigterm_shuts_down_cleanly(self):
        with running_server() as server:
            sockets = [connect(server.port) for _ in range(3)]
            for sock in sockets:
                sock.sendall(make_frame(b"x"))
                read_exactly(sock, 5)
            started = time.time()
            exit_code = server.stop()
            self.assertEqual(exit_code, 0)
            self.assertLess(time.time() - started, 5)
            for sock in sockets:
                self.assertTrue(closed_by_server(sock, wait_seconds=3))
            self.assertIn("[final]", server.log())
            self.assertIn("shutdown=3", server.log())

    def test_stats_are_reported(self):
        with running_server("--stats-interval", "1") as server:
            for _ in range(3):
                sock = connect(server.port)
                for _ in range(5):
                    sock.sendall(make_frame(b"count me"))
                    read_exactly(sock, 12)
            time.sleep(2.5)
            log = server.log()
            self.assertIn("[stats]", log)
            self.assertIn("accepted=3", log)
            self.assertIn("msgs_in=15", log)
            self.assertIn("msgs_out=15", log)

    @unittest.skipIf(SKIP_HEAVY, "SKIP_HEAVY is set")
    def test_ten_thousand_idle_connections(self):
        needed = 10500
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        target = needed if hard == resource.RLIM_INFINITY else min(hard, max(soft, needed))
        if target < needed:
            self.skipTest("open file limit too low (%d)" % target)
        resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))

        with running_server("--max-connections", "20000") as server:
            sockets = [connect(server.port, timeout=30) for _ in range(10000)]
            self.assertIsNone(server.process.poll(), "server died")
            for sock in sockets[::100]:  # every 100th one still gets its echo
                sock.sendall(make_frame(b"still here"))
                self.assertEqual(read_exactly(sock, 14), make_frame(b"still here"))
            for sock in sockets:
                sock.close()
            self.assertIsNone(server.process.poll(), "server died")


# ---------------------------------------------------------------- more than one worker, and the tuning switches

class MultiWorkerTests(unittest.TestCase):
    def test_four_workers_serve_many_clients(self):
        with running_server("--workers", "4") as server:
            sockets = [connect(server.port) for _ in range(100)]
            for i, sock in enumerate(sockets):
                sock.sendall(make_frame(b"client %d" % i) * 3)
            for i, sock in enumerate(sockets):
                expected = make_frame(b"client %d" % i) * 3
                self.assertEqual(read_exactly(sock, len(expected)), expected)

    def test_four_workers_ack_mode(self):
        with running_server("--workers", "4", "--mode", "ack") as server:
            sockets = [connect(server.port) for _ in range(40)]
            for sock in sockets:
                sock.sendall(b"".join(make_request(i) for i in range(10)))
            for sock in sockets:
                for i in range(10):
                    self.assertEqual(read_exactly(sock, 9), make_ack(i))

    def test_all_tuning_switches_together(self):
        with running_server("--workers", "2", "--tcp-nodelay", "--writev", "--pin-cpus") as server:
            sockets = [connect(server.port) for _ in range(30)]
            for i, sock in enumerate(sockets):
                sock.sendall(make_frame(b"tuned %d" % i))
            for i, sock in enumerate(sockets):
                expected = make_frame(b"tuned %d" % i)
                self.assertEqual(read_exactly(sock, len(expected)), expected)

    def test_slow_reader_with_writev(self):
        # the same slow-reader test, but through the writev code path
        EchoModeTests()._run_slow_reader_test(["--writev"])

    def test_slow_reader_with_two_workers_and_writev(self):
        EchoModeTests()._run_slow_reader_test(["--workers", "2", "--writev"])


# ---------------------------------------------------------------- baseline server

@unittest.skipUnless(BASELINE_BINARY, "BASELINE_BINARY is not set")
class BaselineServerTests(unittest.TestCase):
    def test_echo(self):
        with running_server(binary=BASELINE_BINARY) as server:
            sock = connect(server.port)
            frames = [make_frame(b"abc"), make_frame(b""), make_frame(b"z" * MAX_PAYLOAD)]
            sock.sendall(b"".join(frames))
            for frame in frames:
                self.assertEqual(read_exactly(sock, len(frame)), frame)

    def test_ack(self):
        with running_server("--mode", "ack", binary=BASELINE_BINARY) as server:
            sock = connect(server.port)
            sock.sendall(make_request(41, body=b"body") + make_request(42))
            self.assertEqual(read_exactly(sock, 9), make_ack(41))
            self.assertEqual(read_exactly(sock, 9), make_ack(42))

    def test_oversize_frame_closes_the_connection(self):
        with running_server(binary=BASELINE_BINARY) as server:
            sock = connect(server.port)
            sock.sendall(struct.pack(">I", MAX_PAYLOAD + 1))
            self.assertTrue(closed_by_server(sock))

    def test_fifty_clients_at_once(self):
        with running_server(binary=BASELINE_BINARY) as server:
            sockets = [connect(server.port) for _ in range(50)]
            for i, sock in enumerate(sockets):
                sock.sendall(make_frame(b"client %d" % i))
            for i, sock in enumerate(sockets):
                expected = make_frame(b"client %d" % i)
                self.assertEqual(read_exactly(sock, len(expected)), expected)


if __name__ == "__main__":
    unittest.main(verbosity=2, warnings="ignore")
