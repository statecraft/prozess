
#ifndef connection_h
#define connection_h

#include "common.h"

struct topic_t;

typedef enum {
    IN_MSG_EVENT = 1,
    IN_MSG_SUB = 3,
} in_msg_t;

enum {
    OUT_MSG_HELLO = 0,
    OUT_MSG_EVENT = 1,
    OUT_MSG_EVENT_CONFIRM = 2,
    OUT_MSG_SUB = 3,
    OUT_MSG_SUB_END = 4,
};
typedef uint8_t out_msg_t;

#pragma pack(push, 1)

typedef struct {
    uint32_t size;
    uint32_t crc32;
    
    // Everything below is included in the checksum.
    uint16_t batch_size; // varint would be better
    uint8_t proto_version;
    uint8_t flags;
    uint8_t data[];
} batch_block_t;

typedef struct {
    out_msg_t msg_type; // always OUT_MSG_EVENT
    batch_block_t b;
} out_block_msg;

typedef struct {
    out_msg_t msg_type; // OUT_MSG_EVENT_CONFIRM
    uint8_t proto_version;
    version_t v_start; // First version of batch.
} event_response_t;


enum sub_flags_t {
    SUB_FLAG_ONESHOT = 1, // Shared.
    
    SUB_OUT_FLAG_COMPLETE = 2, // There's no ongoing subscription
    SUB_OUT_FLAG_CURRENT = 4, // Caught up to the current version
};

typedef struct {
    uint8_t flags;
    version_t start;
    uint32_t maxbytes;
} in_sub_req_t;

typedef struct {
    out_msg_t msg_type; // always OUT_MSG_SUB
    uint8_t proto_version;
    version_t v_start; // varint would be better
    uint64_t size; // varint would be better
    uint8_t flags;
    uint8_t data[];
} out_sub_response_t;

typedef struct {
    out_msg_t msg_type; // always OUT_MSG_SUB_END
    uint8_t proto_version;
} out_sub_end_t;

typedef struct {
    out_msg_t msg_type; // always OUT_MSG_HELLO
    uint8_t proto_version;
    source_id_t source;
    // EPOCH & current version?
} out_msg_hello_t;

#pragma pack(pop)


typedef enum {
    READ_LEN = 0,
    READ_BODY,
} message_state;

#define SMALL_BUF_SIZE 1000
typedef struct connection_t {
    int fd;
    message_state state;
    uint32_t msg_len;
    
    uint32_t chunk_bytes_read;
    uint8_t *msg;
    uint8_t small_buf[SMALL_BUF_SIZE];
    
    size_t sub_bytes_remaining;
    size_t sub_idx;
} connection_t;

connection_t *kc_new(int fd);
void kc_free(connection_t *c, struct topic_t *topic);

data_buffer kc_current_buf(connection_t *c);

// Returns whether to read again.
bool kc_did_read(connection_t *c, size_t bytesread, struct topic_t *topic);

#endif
