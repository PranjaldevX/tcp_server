#include <cassert>
#include <cstdio>
#include <vector>

#include "../src/message_handler.h"

// a request payload as ack mode expects it: [REQUEST][sequence id][body...]
static ByteVector make_request_payload(uint32_t sequence_id, size_t body_size) {
    ByteVector payload(MESSAGE_PREFIX_SIZE + body_size, 0x77);
    payload[0] = MESSAGE_TYPE_REQUEST;
    write_sequence_id(payload.data() + 1, sequence_id);
    return payload;
}

int main() {
    // ---- echo mode: the reply is the same frame, byte for byte
    {
        ByteVector payload = {1, 2, 3, 4, 5, 6};
        ByteVector reply;
        assert(append_reply(ServerMode::ECHO, payload.data(), (uint32_t)payload.size(), reply));

        assert(reply.size() == HEADER_SIZE + payload.size());
        assert(read_length_prefix(reply.data()) == payload.size());
        assert(ByteVector(reply.begin() + HEADER_SIZE, reply.end()) == payload);
    }

    // ---- echo mode accepts an empty payload
    {
        ByteVector reply;
        unsigned char nothing = 0;
        assert(append_reply(ServerMode::ECHO, &nothing, 0, reply));
        assert(reply.size() == HEADER_SIZE && read_length_prefix(reply.data()) == 0);
    }

    // ---- ack mode: reply is [ACK][same sequence id], whatever the body was
    {
        ByteVector payload = make_request_payload(0xDEADBEEF, 100);
        ByteVector reply;
        assert(append_reply(ServerMode::ACK, payload.data(), (uint32_t)payload.size(), reply));

        assert(reply.size() == HEADER_SIZE + MESSAGE_PREFIX_SIZE);
        assert(read_length_prefix(reply.data()) == MESSAGE_PREFIX_SIZE);
        assert(reply[HEADER_SIZE] == MESSAGE_TYPE_ACK);
        assert(read_sequence_id(reply.data() + HEADER_SIZE + 1) == 0xDEADBEEF);
    }

    // ---- ack mode with no body at all is fine
    {
        ByteVector payload = make_request_payload(7, 0);
        ByteVector reply;
        assert(append_reply(ServerMode::ACK, payload.data(), (uint32_t)payload.size(), reply));
        assert(read_sequence_id(reply.data() + HEADER_SIZE + 1) == 7);
    }

    // ---- ack mode rejects a payload that is too short or has the wrong type
    {
        ByteVector too_short = make_request_payload(1, 0);
        too_short.pop_back();
        ByteVector reply;
        assert(!append_reply(ServerMode::ACK, too_short.data(), (uint32_t)too_short.size(), reply));

        ByteVector wrong_type = make_request_payload(1, 4);
        wrong_type[0] = MESSAGE_TYPE_ACK;  // acks are not requests
        assert(!append_reply(ServerMode::ACK, wrong_type.data(), (uint32_t)wrong_type.size(), reply));
    }

    // ---- several replies pile up in one buffer, in order
    {
        ByteVector reply;
        for (uint32_t id = 0; id < 3; id++) {
            ByteVector payload = make_request_payload(id, 3);
            assert(append_reply(ServerMode::ACK, payload.data(), (uint32_t)payload.size(), reply));
        }
        size_t one_reply = HEADER_SIZE + MESSAGE_PREFIX_SIZE;
        assert(reply.size() == 3 * one_reply);
        for (uint32_t id = 0; id < 3; id++) {
            assert(read_sequence_id(reply.data() + id * one_reply + HEADER_SIZE + 1) == id);
        }
    }

    puts("test_message_handler: all checks passed");
    return 0;
}
