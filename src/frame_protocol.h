#pragma once
#include <cstddef>
#include <cstdint>

// each frame = 4-byte length (big endian) + payload
constexpr size_t HEADER_SIZE = 4;

// biggest payload we accept, 64 KiB for now
constexpr uint32_t MAX_PAYLOAD_SIZE = 64 * 1024;

// 4 bytes (big endian) -> number
inline uint32_t read_length_prefix(const unsigned char* bytes) {
    return (uint32_t)bytes[0] << 24 | (uint32_t)bytes[1] << 16 |
           (uint32_t)bytes[2] << 8  | (uint32_t)bytes[3];
}

// number -> 4 bytes (big endian)
inline void write_length_prefix(unsigned char* bytes, uint32_t length) {
    bytes[0] = length >> 24;
    bytes[1] = length >> 16;
    bytes[2] = length >> 8;
    bytes[3] = length;
}

enum FrameStatus {
    FRAME_INCOMPLETE,  // wait for more bytes
    FRAME_COMPLETE,    // a full frame is at the front of the buffer
    FRAME_TOO_LARGE    // length prefix is above MAX_PAYLOAD_SIZE, drop the connection
};

// Look at the front of a buffer that holds bytes_available bytes.
// On FRAME_COMPLETE, *payload_length is filled in and the whole frame
// takes HEADER_SIZE + *payload_length bytes.
inline FrameStatus check_frame(const unsigned char* buffer, size_t bytes_available,
                               uint32_t* payload_length) {
    if (bytes_available < HEADER_SIZE) return FRAME_INCOMPLETE;

    uint32_t length = read_length_prefix(buffer);
    if (length > MAX_PAYLOAD_SIZE) return FRAME_TOO_LARGE;
    if (bytes_available < HEADER_SIZE + (size_t)length) return FRAME_INCOMPLETE;

    *payload_length = length;
    return FRAME_COMPLETE;
}
