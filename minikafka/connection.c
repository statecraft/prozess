#include "common.h"

#include "connection.h"
#include "topic.h"
#include "cmp.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>

const version_t VERSION_CONFLICT = UINT64_MAX;

// TODO: optimize for zero-allocate.
connection_t *kc_new(int fd) {
    connection_t *c = malloc(sizeof(connection_t));
    *c = (connection_t){
        .fd = fd,
        .sub_idx = -1,
    };
    return c;
}

void kc_free(connection_t *c, topic_t *topic) {
    if (c->sub_idx != -1) db_unsubscribe(topic, c);
    if (c->msg && c->msg != c->small_buf) free(c->msg);
    close(c->fd);
    free(c);
}

data_buffer kc_current_buf(connection_t *c) {
    switch (c->state) {
        case READ_LEN:
            return (data_buffer){
                .size = 4 - c->chunk_bytes_read,
                .bytes = &c->msg_len + c->chunk_bytes_read
            };
        case READ_BODY:
            return (data_buffer){
                .size = c->msg_len - c->chunk_bytes_read,
                .bytes = c->msg + c->chunk_bytes_read
            };
    }
}

int process_client_message(connection_t *conn, in_msg_t msgtype, void *msg, uint32_t len, struct topic_t *topic);

// Returns whether to read again.
bool kc_did_read(connection_t *c, size_t bytesread, topic_t *topic) {
    c->chunk_bytes_read += bytesread;
    
    assert(c->chunk_bytes_read <= (c->state == READ_LEN ? 4 : c->msg_len));
    
    if (c->state == READ_LEN && c->chunk_bytes_read == 4) {
        c->chunk_bytes_read = 0;
        c->state = READ_BODY;
        // TODO: Consider removing this allocation on the hot path.
        c->msg = c->msg_len <= SMALL_BUF_SIZE ? c->small_buf : malloc(c->msg_len);
        //        printf("length read. buffer primed with length %zd\n", c->msg_len);
        return true;
    } else if (c->state == READ_BODY && c->chunk_bytes_read == c->msg_len) {
        uint8_t msgtype = *c->msg;
        int result = process_client_message(c, (in_msg_t)msgtype, c->msg+1, c->msg_len-1, topic);
        if (result != 0) {
            fprintf(stderr, "Error handling client message: %d\n", result);
        }
        c->chunk_bytes_read = 0;
        c->state = READ_LEN;
        if (c->msg != c->small_buf) free(c->msg);
        c->msg = NULL;
        //        printf("packet read of size %zd\n", c->msg_len);
        return true;
    }
    return false;
}

static bool mp_reader(cmp_ctx_t *ctx, void *data, size_t limit) {
    data_buffer *buf = (data_buffer *)ctx->buf;
    if (buf->size < limit) return false;
    memcpy(data, buf->bytes, limit);
    buf->bytes += limit;
    buf->size -= limit;
    return true;
}

static bool mp_skipper(cmp_ctx_t *ctx, size_t count) {
    data_buffer *buf = (data_buffer *)ctx->buf;
    if (buf->size < count) return false;
    buf->bytes += count;
    buf->size -= count;
    return true;
}

enum process_message_err {
    ERR_OK = 0,
    ERR_INVALID_DATA = 1,
    ERR_CONFLICT = 2,
    ERR_UNKNOWN_MSG_TYPE = 3,
};
// Messagepack messages are

int process_client_message(connection_t *conn, in_msg_t msgtype, void *msg, uint32_t len, topic_t *topic) {
#define CHK(expr) if (!expr) {\
    fprintf(stderr, "Msgpack error %s\n", cmp_strerror(&cmp));\
    return ERR_INVALID_DATA;\
}
    
//    printf("process client message of type %d, %d bytes\n", msgtype, len);

//    void *_orig = msg;
    
    data_buffer buf = {.size=len, .bytes=msg};
    cmp_ctx_t cmp;
    cmp_init(&cmp, &buf, mp_reader, mp_skipper, NULL);

    
    switch (msgtype) {
        case IN_MSG_EVENT: {
            // array of version, keys, blob.
            uint32_t size;
            CHK(cmp_read_array(&cmp, &size))
            if (size != 3) return ERR_INVALID_DATA;
            
            // Read fields.
            
            // Target version is read signed because -1 means we ignore conflicts.
            int64_t target_version;
            CHK(cmp_read_integer(&cmp, &target_version))
            
            // List of interception keys
            uint32_t conflict_keys;
            CHK(cmp_read_array(&cmp, &conflict_keys))
            data_buffer conflict_key_marker = buf;
            for (int i = 0; i < conflict_keys; i++) {
                char buf[1000];
                uint32_t str_size = sizeof(buf);
                cmp_read_str(&cmp, buf, &str_size);
                if (target_version >= 0 && db_key_in_conflict(topic, target_version, buf, str_size)) {
                    printf("abort - key conflict in %s\n", buf);
                    
                    event_response_t client_msg = {
                        .msg_type = OUT_MSG_EVENT_CONFIRM,
                        .proto_version = 0,
                        .v_start = VERSION_CONFLICT,
                    };
                    write(conn->fd, &client_msg, sizeof(client_msg));

                    return 0;
                }
            }
        
            // Binary blob. If happy, memcpy this straight to the pool room.

            CHK(cmp_read_bin_size(&cmp, &size))
//            printf("%d bytes in data blob\n", size);
            
//            printf("byte %x at %ld\n", ((uint8_t *)buf.bytes)[0], buf.bytes - _orig);
            version_t resulting_version = db_append_event(topic, buf);
            
            event_response_t client_msg = {
                .msg_type = OUT_MSG_EVENT_CONFIRM,
                .proto_version = 0,
                .v_start = resulting_version,
            };
            write(conn->fd, &client_msg, sizeof(client_msg));
            
            // Then go through and tag all the keys.
            cmp.buf = &conflict_key_marker;
            for (int i = 0; i < conflict_keys; i++) {
                char buf[1000];
                uint32_t str_size = sizeof(buf);
                cmp_read_str(&cmp, buf, &str_size);
                db_mark_conflict_key(topic, resulting_version, buf, str_size);
            }
            
            return 0;
        }
            
        case IN_MSG_SUB: {
            // array of flags, start version (or -1), max bytes.
            uint32_t size;
            CHK(cmp_read_array(&cmp, &size))
            if (size != 3) return ERR_INVALID_DATA;

            in_sub_req_t req;
            CHK(cmp_read_uchar(&cmp, &req.flags))
            
            int64_t start;
            CHK(cmp_read_integer(&cmp, &start))
            // A start of -1 means we want to subscribe from the current version.
            req.start = start >= 0 ? start : UINT64_MAX;
            
            CHK(cmp_read_uint(&cmp, &req.maxbytes))
            // ... TODO: Check max_bytes is valid. Shouldn't be bigger than 1M or something.

            db_subscribe(topic, conn, req);
            return 0;
        }
            
        default: return ERR_UNKNOWN_MSG_TYPE;
    }
    
#undef CHK
}
