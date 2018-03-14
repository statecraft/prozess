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
#include <stdbool.h>

#include <sys/types.h>
#include <sys/stat.h>

#include "connection.h"
#include "db.h"
#include "common.h"

#include "crc32.h"
#include "topic.h"

#define ARR_SIZE(arrexpr) (sizeof(arrexpr) / sizeof(arrexpr[0]))

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

#define MIN(x,y) ((x) < (y) ? (x) : (y))

// https://gist.github.com/josephg/6c078a241b0e9e538ac04ef28be6e787
int main(int argc, const char * argv[]) {
    topic_t *topic = db_new("sometopic");
    if (topic == NULL) {
        fprintf(stderr, "Failed to open data store\n");
        return 1;
    }
    atexit_b(^{
        printf("db close\n");
        db_close(topic);
    });
    
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

    printf("Listening on port 9999\n");
    
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
//        CHK(nev)
        
//        printf("kqueue got %d events\n", nev);
        
        for (int i = 0; i < nev; i++) {
//            print_kevent(&evList[i]);
            int fd = (int)evList[i].ident;
            
            switch (evList[i].filter) {
                case EVFILT_READ: {
                    if (fd == localFd) {
                        struct sockaddr_storage addr;
                        socklen_t socklen = sizeof(addr);
                        int connfd = accept(fd, (struct sockaddr *)&addr, &socklen);
                        assert(connfd != -1);
                        
                        // Listen on the new socket
                        connection_t *kc = kc_new(connfd);
                        EV_SET(&evSet, connfd, EVFILT_READ, EV_ADD, 0, 0, kc);
                        kevent(kq, &evSet, 1, NULL, 0, NULL);
                        printf("Got connection!\n");
                        
                        int flags = fcntl(connfd, F_GETFL, 0);
                        assert(flags >= 0);
                        fcntl(connfd, F_SETFL, flags | O_NONBLOCK);
                        
                        db_send_hello(topic, kc);
                        // schedule to send the file when we can write (first chunk should happen immediately)
    //                    EV_SET(&evSet, connfd, EVFILT_WRITE, EV_ADD | EV_ONESHOT, 0, 0, NULL);
    //                    kevent(kq, &evSet, 1, NULL, 0, NULL);
                    } else {
                        // Got a message from one of our sockets!
                        int to_read = (int)evList[i].data;
                        connection_t *kc = (connection_t *)evList[i].udata;
                        
                        while (1) {
                            data_buffer buf = kc_current_buf(kc);
                            ssize_t bytes_read = recv(fd, buf.bytes, MIN(buf.size, to_read), 0);
                            
                            // This shouldn't happen in practice, but if it does ignore it.
//                            if (bytes_read == -1 && errno == EAGAIN) break;
                            
                            // Disconnected during read. Pass to EOF check.
                            if (bytes_read == -1 && errno == ECONNRESET) break;
                            
//                            printf("bytes_read - %zd %d\n", bytes_read, errno);
                            CHK(bytes_read);
                            
                            if (bytes_read == 0) break;
                            to_read -= bytes_read;
//                            printf("read %zu bytes, %d bytes left\n", bytes_read, to_read);
                            
                            if (!kc_did_read(kc, bytes_read, topic)) break;
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
                kc_free(evList[i].udata, topic);
                // Socket is automatically removed from the kq by the kernel.
            }
        }
    }
    
    return 0;
}
