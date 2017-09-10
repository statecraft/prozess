//
//  crc32.h
//  minikafka
//
//  Created by Joseph Gentle on 4/9/17.
//  Copyright © 2017 Joseph Gentle. All rights reserved.
//

#ifndef crc32_h
#define crc32_h

#include <stdint.h>

uint32_t
calculate_crc32c(uint32_t crc32c,
                 const unsigned char *buffer,
                 unsigned int length);

#endif /* crc32_h */
