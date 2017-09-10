//
//  arena.h
//  minikafka
//
//  Created by Joseph Gentle on 28/8/17.
//  Copyright © 2017 Joseph Gentle. All rights reserved.
//

#ifndef arena_h
#define arena_h

#include "common.h"

typedef struct {
    void *base;
    size_t len;
} arena_t;

#endif /* arena_h */
