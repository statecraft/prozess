#include <sys/socket.h>
#include <sys/un.h>
#include <sys/event.h>
#include <netdb.h>
#include <assert.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>

#include <stdlib.h>

#include <sys/types.h>
#include <sys/stat.h>

#include "cmp.h"

#define ARR_SIZE(arrexpr) sizeof(arrexpr) / sizeof(arrexpr[0])

struct sockaddr_in6 get_socket() {
    // Macos automatically binds both ipv4 and 6 when you do this.
    struct sockaddr_in6 addr = {};
    addr.sin6_len = sizeof(addr);
    addr.sin6_family = AF_INET6;
    addr.sin6_addr = in6addr_any; //(struct in6_addr){}; // 0.0.0.0 / ::
    addr.sin6_port = htons(9999);
    
    return addr;
}

void print_kevent(struct kevent *evt) {
    printf("Event ident %lu filter %d data %ld flags %x\n",
           evt->ident,
           evt->filter,
           evt->data,
           evt->flags);
}

typedef enum {
    READ_LEN = 0,
    READ_BODY,
} message_state;

const size_t SMALL_BUF_SIZE = 1000;
typedef struct {
    message_state state;
    uint32_t msg_len;
    
    uint32_t chunk_bytes_read;
    uint8_t *msg;
    uint8_t small_buf[SMALL_BUF_SIZE];
} kafka_connection;

// TODO: optimize for zero-allocate.
static kafka_connection *kc_new() {
    kafka_connection *c = malloc(sizeof(kafka_connection));
    *c = (kafka_connection){};
    return c;
}

static void kc_free(kafka_connection *c) {
    if (c->msg && c->msg != c->small_buf) free(c->msg);
    free(c);
}

typedef struct {
    uint32_t size;
    void *bytes;
} data_buffer;

static data_buffer kc_current_buf(kafka_connection *c) {
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

void process_client_message(void *msg, uint32_t len) {
    printf("process client message of %d bytes\n", len);
}

// Returns whether to read again.
bool kc_did_read(kafka_connection *c, size_t bytesread) {
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
        process_client_message(c->msg, c->msg_len);
        c->chunk_bytes_read = 0;
        c->state = READ_LEN;
        if (c->msg != c->small_buf) free(c->msg);
        c->msg = NULL;
//        printf("packet read of size %zd\n", c->msg_len);
        return true;
    }
    return false;
}

// https://gist.github.com/josephg/6c078a241b0e9e538ac04ef28be6e787
int main(int argc, const char * argv[]) {
#define CHK(expr) if (expr < 0) { perror(#expr); return 1; }
    struct sockaddr_in6 addr = get_socket();
    
    int localFd = socket(addr.sin6_family, SOCK_STREAM, 0);
    CHK(localFd);
    
    {
        // Steal the socket if we didn't shutdown cleanly before.
        int on = 1;
        CHK(setsockopt(localFd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)));
    }
    CHK(bind(localFd, (struct sockaddr *)&addr, sizeof(addr)));
    // 5 = backlog size. Check manual.
    CHK(listen(localFd, 5));

    int kq = kqueue();
    
    struct kevent evSet;
    // Read events on the listening fd === incoming connections.
    EV_SET(&evSet, localFd, EVFILT_READ, EV_ADD, 0, 0, NULL);
    CHK(kevent(kq, &evSet, 1, NULL, 0, NULL));
    
//    uint64_t bytes_written = 0;

    struct kevent evList[32];
    while (1) {
        // returns number of events
        int nev = kevent(kq, NULL, 0, evList, ARR_SIZE(evList), NULL);
        printf("kqueue got %d events\n", nev);
        
        for (int i = 0; i < nev; i++) {
            print_kevent(&evList[i]);
            int fd = (int)evList[i].ident;
            
            switch (evList[i].filter) {
                case EVFILT_READ: {
                    if (fd == localFd) {
                        struct sockaddr_storage addr;
                        socklen_t socklen = sizeof(addr);
                        int connfd = accept(fd, (struct sockaddr *)&addr, &socklen);
                        assert(connfd != -1);
                        
                        // Listen on the new socket
                        kafka_connection *kc = kc_new();
                        EV_SET(&evSet, connfd, EVFILT_READ, EV_ADD, 0, 0, kc);
                        kevent(kq, &evSet, 1, NULL, 0, NULL);
                        printf("Got connection!\n");
                        
                        int flags = fcntl(connfd, F_GETFL, 0);
                        assert(flags >= 0);
                        fcntl(connfd, F_SETFL, flags | O_NONBLOCK);
                        
                        // schedule to send the file when we can write (first chunk should happen immediately)
    //                    EV_SET(&evSet, connfd, EVFILT_WRITE, EV_ADD | EV_ONESHOT, 0, 0, NULL);
    //                    kevent(kq, &evSet, 1, NULL, 0, NULL);
                    } else {
                        // Got a message from one of our sockets!
                        int to_read = (int)evList[i].data;
                        kafka_connection *kc = (kafka_connection *)evList[i].udata;
                        
                        while (1) {
                            data_buffer buf = kc_current_buf(kc);
                            ssize_t bytes_read = recv(fd, buf.bytes, buf.size, 0);
                            CHK(bytes_read);
                            
                            if (bytes_read == 0) break;
                            to_read -= bytes_read;
                            printf("read %zu bytes, %d bytes left\n", bytes_read, to_read);
                            
                            if (!kc_did_read(kc, bytes_read)) break;
                            if (to_read == 0) break;
                        }
                        
//                        printf("xxx %d\n", kc->x);
                    }
                    break;
                }
                case EVFILT_WRITE: {
                    printf("Ok to write more!\n");
                
//                off_t offset = (off_t)evList[i].udata;
//                off_t len = 0;//evList[i].data;
//                if (sendfile(junk, fd, offset, &len, NULL, 0) != 0) {
////                    perror("sendfile");
////                    printf("err %d\n", errno);
//                    
//                    if (errno == EAGAIN) {
//                        // schedule to send the rest of the file
//                        EV_SET(&evSet, fd, EVFILT_WRITE, EV_ADD | EV_ONESHOT, 0, 0, (void *)(offset + len));
//                        kevent(kq, &evSet, 1, NULL, 0, NULL);
//                    }
//                }
//                bytes_written += len;
//                printf("wrote %lld bytes, %lld total\n", len, bytes_written);
                    break;
                }
            }
            
            if (evList[i].flags & EV_EOF) {
                printf("Disconnect\n");
                kc_free(evList[i].udata);
                close(fd);
                // Socket is automatically removed from the kq by the kernel.
            }
        }
    }
    
    return 0;
}
