//
//  db.c
//  minikafka
//
//  Created by Joseph Gentle on 27/8/17.
//  Copyright © 2017 Joseph Gentle. All rights reserved.
//

#include "common.h"

#include "topic.h"
#include <stdio.h>
#include <assert.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <errno.h>

#include "khash.h"
#include "kvec.h"
#include "crc32.h"
#include "connection.h"


KHASH_MAP_INIT_STR(kvmap, version_t)

static const size_t MAX_ENTRY_SIZE = 100*1024*1024; // 100M.
static const size_t INDEX_SIZE = 10*1024*1024; // 10M.
typedef struct {
    uint32_t v_offset; // actually the version for now.
    uint32_t data_ptr;
} index_entry_t;

struct topic_t {
    index_entry_t *index_map;
    uint32_t index_pos;
    
    int data_fd;
    uint32_t data_pos;
    
    source_id_t source_id;
    
    version_t v_base;
    version_t next_version;
    kh_kvmap_t *key_lastmod;
    
    kvec_t(connection_t *) subs;
    // ... And topic name (/source).
};

const char BASE64_ALPHABET[64] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
source_id_t gen_source_id() {
    uint8_t id_bytes[sizeof(source_id_t)];
    arc4random_buf(id_bytes, sizeof(id_bytes));
    for (int i = 0; i < sizeof(id_bytes); i++) {
        id_bytes[i] = BASE64_ALPHABET[id_bytes[i] % 64];
    }
    return *(source_id_t *)id_bytes;
}

void preallocate(int fd, size_t size) {
#if defined(__APPLE__)
    fcntl(index_fd, F_PREALLOCATE, INDEX_SIZE);
#else
    uint8_t *zeroes = malloc(size);
    bzero(zeroes, size);
    ssize_t written = pwrite(fd, zeroes, size, 0);
    assert(written == size);
    free(zeroes);
#endif
}

const char MAGIC_BYTES[4] = "UKDF"; // microkafka data file
const size_t DATA_START_POS = sizeof(MAGIC_BYTES) + sizeof(source_id_t);

topic_t *db_new(char *name) {
#define CHK(expr) if ((expr) < 0) { perror(#expr); return NULL; }
    // TODO: Add magic bytes and packed format version.
    int index_fd, data_fd = open("data", O_EXLOCK | O_RDWR);
//    bool db_is_new;
    
    source_id_t source;
    
    uint32_t index_pos;
    uint32_t data_pos = DATA_START_POS, next_v_offset = 1;
    index_entry_t *index_map;
    
    if (data_fd == -1 && errno == ENOENT) {
        // Making a new database.
        data_fd = open("data", O_CREAT | O_EXLOCK | O_RDWR, 0640);
        CHK(data_fd)
        
        // Generate ID and add magic bytes at the start of the file.
        write(data_fd, &MAGIC_BYTES, 4);
        source = gen_source_id();
        write(data_fd, &source, sizeof(source));

        index_fd = open("index", O_CREAT | O_EXLOCK | O_RDWR, 0640);
        CHK(index_fd)

        ftruncate(index_fd, INDEX_SIZE); // 0-fill the index.
        preallocate(index_fd, INDEX_SIZE);
//        db_is_new = true;
        
        index_map = mmap(NULL, INDEX_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, index_fd, 0);
        if (index_map == MAP_FAILED) { perror("map index"); return NULL; }

        index_pos = 0;
        
    } else {
        // Db exists.
        CHK(data_fd)
        char magic_bytes[4];
        CHK(pread(data_fd, magic_bytes, 4, 0));
        assert(memcmp(magic_bytes, MAGIC_BYTES, 4) == 0);
        CHK(pread(data_fd, &source, sizeof(source), 4));
        
        index_fd = open("index", O_EXLOCK | O_RDWR);
        // If the data_fd exists, the index must exist too.
        CHK(index_fd)
        
        index_map = mmap(NULL, INDEX_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, index_fd, 0);
        if (index_map == MAP_FAILED) { perror("map index"); return NULL; }

        // Figure out the current version by scanning the index file for the most recent entry,
        // then seeking through the data file.
        
        // TODO: Replace this with a binary search.
        for (index_pos = 0; index_pos < (INDEX_SIZE / sizeof(index_entry_t)); index_pos++) {
            if (index_map[index_pos].data_ptr == 0) break;
        }
        
        printf("index pos %u\n", index_pos);
        
        // Ok, now scan the data file from the last index.
        if (index_pos) {
            data_pos = index_map[index_pos - 1].data_ptr;
            next_v_offset = index_map[index_pos - 1].v_offset;
        }
        
        //    printf("idx scan -> %d\n", v_offset);
        
        while (true) { // TODO: Add better error handling here
            batch_block_t block;
            ssize_t bytes_read = pread(data_fd, &block, sizeof(block), data_pos);
            CHK(bytes_read)
            if (bytes_read > 0 && bytes_read < sizeof(block)) {
                fprintf(stderr, "Warning: trailing %zd bytes in data file\n", bytes_read);
                break;
            }
            
            if (bytes_read == 0 || block.size == 0) break;
            if (block.size > MAX_ENTRY_SIZE) {
                fprintf(stderr, "Error: Invalid size in block.\n");
                break;
            }
            
            assert(block.proto_version == 0);
            
            // TODO: Also check CRC32 in entries.
            
            next_v_offset += block.batch_size;
            data_pos += sizeof(block) + block.size;
        }
    }
    
    printf("source %.8s next v offset %u, data pos %u, index pos %u\n", (char *)&source, next_v_offset, data_pos, index_pos);
    
    topic_t *topic = malloc(sizeof(topic_t));
    *topic = (topic_t){
        .index_map = index_map,
        .index_pos = index_pos,
        .data_fd = data_fd,
        .data_pos = data_pos,
        
        .source_id = source,
        
        .v_base = 0,
        .next_version = next_v_offset,
        .key_lastmod = kh_init_kvmap(),
        .subs = {},
    };
    return topic;
#undef CHK
}

void db_close(topic_t *topic) {
    if (munmap(topic->index_map, INDEX_SIZE) == -1) { perror("munmap index"); }
    close(topic->data_fd);
    kv_destroy(topic->subs);
}

int db_send_hello(topic_t *topic, connection_t *conn) {
    out_msg_hello_t msg = {
        .msg_type = OUT_MSG_HELLO,
        .proto_version = 0,
        .source = topic->source_id,
    };
    
    return (int)write(conn->fd, &msg, sizeof(msg));
}


bool db_key_in_conflict(topic_t *topic, version_t ok_at_v, char *key, size_t keylen) {
    if (ok_at_v < topic->v_base) return true;
    if (ok_at_v == topic->next_version) return false;
    
    kh_kvmap_t *h = topic->key_lastmod;
    khint_t iter = kh_get_kvmap(h, key);
    // Key missing from db.
    if (iter == kh_end(h)) return false;
    else {
//        printf("key %s found with value %llu\n", key, kh_val(h, iter));
        return (kh_val(h, iter) >= ok_at_v); // TODO: < or <=?
    }
}

void db_mark_conflict_key(topic_t *topic, version_t v, char *key, size_t keylen) {
//    printf("mark conflict key %s = %llu\n", key, v);
    kh_kvmap_t *h = topic->key_lastmod;
    int ret;
    khint_t iter = kh_put_kvmap(h, key, &ret);
    assert(ret != -1);
    if (ret != 0) {
        // Clone the key. TODO: Allocate this in a reusable memory arena.
        char *keyclone = malloc(keylen + 1);
        strcpy(keyclone, key);
        kh_key(h, iter) = keyclone;
    }
    kh_val(h, iter) = v;
}

version_t db_append_event(topic_t *topic, data_buffer event) {
    //    printf("xxx append %d\n", event.size);

    out_block_msg msg = {
        .msg_type = OUT_MSG_EVENT,
        .b = {
            .size = event.size,
            .crc32 = 0,
            
            .batch_size = 1,
            .proto_version = 0,
            .flags = 0,
        }
    };
    
    msg.b.crc32 = calculate_crc32c(~0, &msg.b.proto_version, sizeof(msg.b) - offsetof(batch_block_t, proto_version));
    msg.b.crc32 = calculate_crc32c(msg.b.crc32, event.bytes, event.size);
    msg.b.crc32 = ~msg.b.crc32;
    
    pwrite(topic->data_fd, &msg.b, sizeof(msg.b), topic->data_pos);
    pwrite(topic->data_fd, event.bytes, event.size, topic->data_pos + sizeof(msg.b));
    
    version_t version = topic->next_version; // + base? ???
    uint32_t v_offset = (uint32_t)(version - topic->v_base);
    
    // TODO: maybe X bytes instead of X operations.
    if (v_offset % 10 == 0) { // Add to the index file every 100 entries or so.
        topic->index_map[topic->index_pos++] = (index_entry_t) {
            .v_offset = v_offset,
            .data_ptr = topic->data_pos,
        };
//    msync(&db->index_map[v_offset], sizeof(index_entry_t), MS_ASYNC);
    }
    
//    printf("Recv message at version %llu\n", version);
    
    topic->next_version += msg.b.batch_size;
    topic->data_pos += sizeof(msg.b) + event.size;
    
    // ... And send message to each subscription.
    for (int i = 0; i < kv_size(topic->subs);) {
        connection_t *conn = kv_A(topic->subs, i);
        write(conn->fd, &msg, sizeof(msg));
        write(conn->fd, event.bytes, event.size);
        
        if (conn->sub_bytes_remaining <= event.size) {
            db_unsubscribe(topic, conn);
        } else {
            i++;
        }
    }
    
    return version;
}

typedef struct { version_t v; uint32_t pos; } v_pos_tuple;
v_pos_tuple data_pos_at_v(topic_t *topic, version_t target_v) {
    assert(target_v <= topic->next_version);
    if (target_v == topic->next_version) return (v_pos_tuple){
        .v = target_v, .pos = topic->data_pos
    };
    
    // TODO: Specialcase 1.
    
    uint32_t target_offset = (uint32_t)(target_v - topic->v_base);
    
    uint32_t scan_ptr = DATA_START_POS, scan_v = 1;
    // TODO: Replace with biased binary search.
    for (uint32_t i = 0; i < topic->index_pos; i++) {
        uint32_t idx_v_off = topic->index_map[i].v_offset;
        if (idx_v_off == 0 || idx_v_off > target_offset) break;
        
        scan_ptr = topic->index_map[i].data_ptr;
        scan_v = idx_v_off;
    }
    
    // Ok we have our offset to start scanning through the data file.
    
    while (scan_v < target_offset) { // TODO: Add better error handling here
        batch_block_t block;
        ssize_t bytes_read = pread(topic->data_fd, &block, sizeof(block), scan_ptr);
        assert(bytes_read == sizeof(block));
        assert(block.size > 0 && block.size < MAX_ENTRY_SIZE);
        
        assert(block.proto_version == 0);
        
        // Awkward. The block skips past our target. Return the single location.
        if (scan_v + block.batch_size > target_offset) break;
        scan_v += block.batch_size;
        scan_ptr += sizeof(batch_block_t) + block.size;
    }

    return (v_pos_tuple){
        .v = scan_v + topic->v_base,
        .pos = scan_ptr
    };
}


uint32_t data_pos_after_byte(topic_t *topic, uint32_t pos) {
    // TODO: Is > correct? Not >=?
    if (pos > topic->data_pos) return topic->data_pos;
    
    uint32_t scan_ptr = DATA_START_POS;
    // TODO: Replace with biased binary search.
    for (uint32_t i = 0; i < topic->index_pos; i++) {
        uint32_t ptr = topic->index_map[i].data_ptr;
        if (ptr > pos) break;
        else scan_ptr = ptr;
    }
    
    // Now slurp up more operations.
    
    while (1) { // TODO: Add better error handling here
        uint32_t size;
        ssize_t bytes_read = pread(topic->data_fd, &size, sizeof(size), scan_ptr);
        assert(bytes_read == sizeof(size));
        assert(size > 0 && size < MAX_ENTRY_SIZE);
        
        scan_ptr += sizeof(batch_block_t) + size;
        if (scan_ptr > pos) break;
    }
    
    return scan_ptr;
}


static void db_subscribe_raw(topic_t *topic, connection_t *connection, size_t bytes_remaining) {
    if (connection->sub_idx != -1) {
        // Already subscribed. Cancel the existing subscription.
        db_unsubscribe(topic, connection);
        // TODO: Message the client when this happens.
    }
    
    connection->sub_idx = kv_size(topic->subs);
    connection->sub_bytes_remaining = bytes_remaining;
    kv_push(connection_t *, topic->subs, connection);
}

int sendfile_wrap(int fd, int socket, off_t offset, off_t *len_inout) {
#if defined(__FreeBSD__)
return sendfile(fd, socket, offset, *len_inout, NULL, len_inout, 0);
#elif defined(__APPLE__)
return sendfile(fd, socket, offset, len_inout, NULL, 0);
#else
#error("Sendfile wrap does not know how to call sendfile on host system")
#endif
}

void db_subscribe(topic_t *topic, connection_t *connection, in_sub_req_t req) {
    if (req.start == 0) req.start = 1;
    // start == UINT64_MAX if we just want to subscribe to current.
    
    out_sub_response_t res = {
        .msg_type = OUT_MSG_SUB,
        .proto_version = 0,
        .flags = (req.flags & SUB_FLAG_ONESHOT),
    };
    
    // TODO: Read maxbytes from start, then...
    if (req.start <= topic->next_version) {
        res.v_start = req.start;
        // What do we do if start > current?
        v_pos_tuple v_pos = data_pos_at_v(topic, req.start);
        if (!(req.flags & SUB_FLAG_ONESHOT) &&
                (req.maxbytes == UINT32_MAX || v_pos.pos + req.maxbytes > topic->data_pos)) {
            // Catchup mode. We'll send some data now and also subscribe them.
//            printf("Subscription catchup from v%llu to v%llu\n", req.start, topic->next_version-1);
            
            // Send everything remaining and subscribe.
            off_t len = topic->data_pos - v_pos.pos;
            // TODO: Hoist this write into the event loop.
            res.flags = SUB_OUT_FLAG_CURRENT;
            res.size = len;
            
            write(connection->fd, &res, sizeof(res));
            sendfile_wrap(topic->data_fd, connection->fd, v_pos.pos, &len);
            db_subscribe_raw(topic, connection, req.maxbytes - (topic->data_pos - v_pos.pos));
        } else {
            // Completion mode. Everything the client needs is in the subscription response.
//            printf("Subscription completion\n");
            // Stop at an operation boundary. Ugh.
            uint32_t end = data_pos_after_byte(topic, (uint32_t)(v_pos.pos + req.maxbytes));
            // TODO: Hoist this write into the event loop.
            off_t len = end - v_pos.pos;
            
            // In oneshot mode, the complete flag indicates that we're caught up to current.
            res.flags = (req.flags & SUB_FLAG_ONESHOT) // Copy oneshot flag
            | SUB_OUT_FLAG_COMPLETE // We aren't subscribing
            | (end == topic->data_pos ? SUB_OUT_FLAG_CURRENT : 0);
            
            res.size = len;
            write(connection->fd, &res, sizeof(res));
            if (len > 0) { // 0 is specialcased in sendfile. If no bytes to send, just skip call.
                sendfile_wrap(topic->data_fd, connection->fd, v_pos.pos, &len);
            }
        }
    } else if (req.flags & SUB_FLAG_ONESHOT) {
        // There's no data to send. The client asked for a catchup but doesn't want to actually get
        // subscribed.
//        printf("Empty catchup\n");
        res.v_start = topic->next_version;
        res.flags = SUB_FLAG_ONESHOT | SUB_OUT_FLAG_COMPLETE | SUB_OUT_FLAG_CURRENT;
        res.size = 0;
        write(connection->fd, &res, sizeof(res));
    } else {
//        printf("Subscription raw\n");
        res.v_start = topic->next_version;
        res.flags = SUB_OUT_FLAG_CURRENT;
        res.size = 0;
        
        write(connection->fd, &res, sizeof(res));
        db_subscribe_raw(topic, connection, req.maxbytes);
    }
}

void db_unsubscribe(topic_t *topic, connection_t *connection) {
    size_t i = connection->sub_idx;
    if (i == -1) return;
    size_t n = kv_size(topic->subs) - 1;
    
    connection_t *replacement = kv_A(topic->subs, n);
    kv_A(topic->subs, i) = replacement;
    replacement->sub_idx = i;
    connection->sub_idx = -1;
    kv_size(topic->subs)--;
    
    out_sub_end_t msg = {
        .msg_type = OUT_MSG_SUB_END,
        .proto_version = 0
    };
    write(connection->fd, &msg, sizeof(msg));
}
