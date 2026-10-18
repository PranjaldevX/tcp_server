#pragma once
#include <cstddef>
#include <cstdint>
#include <vector>

#include "frame_protocol.h"

using ByteVector = std::vector<unsigned char>;

// Two things a server can do with a request frame:
//   ECHO: send the payload straight back, unchanged
//   ACK:  send back a small acknowledgement carrying the request's sequence id
enum class ServerMode { ECHO, ACK };

// In ACK mode every payload starts with a type byte and a 4-byte sequence id:
//   request: [type = REQUEST][sequence id, big endian][optional body ...]
//   ack:     [type = ACK    ][sequence id, big endian]
constexpr unsigned char MESSAGE_TYPE_REQUEST = 1;
constexpr unsigned char MESSAGE_TYPE_ACK = 2;
constexpr size_t SEQUENCE_ID_SIZE = 4;
constexpr size_t MESSAGE_PREFIX_SIZE = 1 + SEQUENCE_ID_SIZE;  // type byte + sequence id

inline uint32_t read_sequence_id(const unsigned char* bytes) { return read_length_prefix(bytes); }
inline void write_sequence_id(unsigned char* bytes, uint32_t id) { write_length_prefix(bytes, id); }

inline void append_frame_header(ByteVector& out, uint32_t payload_length) {
    unsigned char header[HEADER_SIZE];
    write_length_prefix(header, payload_length);
    out.insert(out.end(), header, header + HEADER_SIZE);
}

// Appends the reply frame for one request to `out`.
// Returns false if the request breaks the protocol, then the caller drops the connection.
inline bool append_reply(ServerMode mode, const unsigned char* payload, uint32_t payload_length,
                         ByteVector& out) {
    if (mode == ServerMode::ECHO) {
        append_frame_header(out, payload_length);
        out.insert(out.end(), payload, payload + payload_length);
        return true;
    }

    if (payload_length < MESSAGE_PREFIX_SIZE || payload[0] != MESSAGE_TYPE_REQUEST) return false;
    append_frame_header(out, MESSAGE_PREFIX_SIZE);
    out.push_back(MESSAGE_TYPE_ACK);
    out.insert(out.end(), payload + 1, payload + 1 + SEQUENCE_ID_SIZE);  // sequence id goes back as it came
    return true;
}
