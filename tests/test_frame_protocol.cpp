#include <cassert>
#include <cstdio>
#include <vector>

#include "../src/frame_protocol.h"

using Bytes = std::vector<unsigned char>;

// Builds a buffer of HEADER_SIZE + payload_bytes_present bytes whose header
// says "length_field". The two can differ on purpose, to fake a frame that
// has only partly arrived.
static Bytes build_frame(uint32_t length_field, size_t payload_bytes_present) {
    Bytes frame(HEADER_SIZE + payload_bytes_present, 0xAB);
    write_length_prefix(frame.data(), length_field);
    return frame;
}

int main() {
    uint32_t payload_length = 0;

    // length prefix: write then read gives the same number, big endian on the wire
    unsigned char header[HEADER_SIZE];
    write_length_prefix(header, 0x01020304);
    assert(header[0] == 1 && header[1] == 2 && header[2] == 3 && header[3] == 4);
    assert(read_length_prefix(header) == 0x01020304);

    // not even a full header yet
    Bytes frame = build_frame(5, 5);
    for (size_t bytes_available = 0; bytes_available < HEADER_SIZE; bytes_available++) {
        assert(check_frame(frame.data(), bytes_available, &payload_length) == FRAME_INCOMPLETE);
    }

    // header is there, payload is one byte short
    assert(check_frame(frame.data(), HEADER_SIZE + 4, &payload_length) == FRAME_INCOMPLETE);

    // exactly one full frame
    assert(check_frame(frame.data(), HEADER_SIZE + 5, &payload_length) == FRAME_COMPLETE);
    assert(payload_length == 5);

    // one full frame followed by the start of the next: only the first one counts
    Bytes two_frames = build_frame(3, 3);
    Bytes next_frame_start = build_frame(7, 2);
    two_frames.insert(two_frames.end(), next_frame_start.begin(), next_frame_start.end());
    assert(check_frame(two_frames.data(), two_frames.size(), &payload_length) == FRAME_COMPLETE);
    assert(payload_length == 3);

    // empty payload is legal
    Bytes empty_frame = build_frame(0, 0);
    assert(check_frame(empty_frame.data(), empty_frame.size(), &payload_length) == FRAME_COMPLETE);
    assert(payload_length == 0);

    // limit: MAX_PAYLOAD_SIZE is fine, one more is too large,
    // and we say so before the payload even arrives
    Bytes largest_frame = build_frame(MAX_PAYLOAD_SIZE, MAX_PAYLOAD_SIZE);
    assert(check_frame(largest_frame.data(), largest_frame.size(), &payload_length) == FRAME_COMPLETE);
    assert(payload_length == MAX_PAYLOAD_SIZE);

    Bytes too_large_header_only = build_frame(MAX_PAYLOAD_SIZE + 1, 0);
    assert(check_frame(too_large_header_only.data(), too_large_header_only.size(), &payload_length) ==
           FRAME_TOO_LARGE);

    // garbage header (all 0xFF) is rejected too
    unsigned char garbage_header[HEADER_SIZE] = {0xFF, 0xFF, 0xFF, 0xFF};
    assert(check_frame(garbage_header, HEADER_SIZE, &payload_length) == FRAME_TOO_LARGE);

    puts("test_frame_protocol: all checks passed");
    return 0;
}
