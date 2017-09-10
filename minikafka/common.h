//
//  common.h
//  minikafka
//
//  Created by Joseph Gentle on 27/8/17.
//  Copyright © 2017 Joseph Gentle. All rights reserved.
//

#ifndef common_h
#define common_h
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

typedef struct {
    uint32_t size;
    void *bytes;
} data_buffer;

typedef uint64_t version_t;

// The ID source is just going to be 8 random characters, but its convenient
// to pass it around as a 64 bit number instead of an array.
typedef uint64_t source_id_t;

struct connection_t;
struct topic_t;

#endif /* common_h */
