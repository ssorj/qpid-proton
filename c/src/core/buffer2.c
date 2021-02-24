/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include <proton/error.h>
#include <proton/object.h>

#ifndef __cplusplus
#include <stdbool.h>
#endif
#include <stddef.h>
#include <string.h>
#include <stdio.h>

#include "buffer2.h"
#include "memory.h"
#include "util.h"

PN_STRUCT_CLASSDEF(pn_buffer2)

pn_buffer2_t *pn_buffer2(size_t capacity)
{
  pn_buffer2_t *buf = (pn_buffer2_t *) pni_mem_allocate(PN_CLASSCLASS(pn_buffer2), sizeof(pn_buffer2_t));

  if (buf != NULL) {
    buf->capacity = capacity;
    buf->size = 0;

    if (capacity > 0) {
        buf->bytes = (char *) pni_mem_suballocate(PN_CLASSCLASS(pn_buffer2), buf, capacity);

        if (buf->bytes == NULL) {
            pni_mem_deallocate(PN_CLASSCLASS(pn_buffer2), buf);
            buf = NULL;
        }
    } else {
        buf->bytes = NULL;
    }
  }

  return buf;
}

void pn_buffer2_free(pn_buffer2_t *buf)
{
  if (buf) {
    pni_mem_subdeallocate(PN_CLASSCLASS(pn_buffer2), buf, buf->bytes);
    pni_mem_deallocate(PN_CLASSCLASS(pn_buffer2), buf);
  }
}

int pni_buffer2_ensure(pn_buffer2_t *buf, size_t size)
{
  // assert(buf->bytes); XXX

  size_t old_capacity = buf->capacity;
  size_t old_size = buf->size;
  size_t new_capacity = old_capacity;
  size_t new_size = old_size + size;

  if (new_size > old_capacity) { // Nix this XXX
    while (new_capacity < new_size) new_capacity = 2 * new_capacity;

    buf->bytes = (char *) pni_mem_subreallocate(PN_CLASSCLASS(pn_buffer2), buf, buf->bytes, new_capacity);
    if (!buf->bytes) return PN_OUT_OF_MEMORY;

    buf->capacity = new_capacity; // Store the new capacity
  }

  return 0;
}

int pni_buffer2_append_string(pn_buffer2_t *buf, const char *bytes, size_t size)
{
  size_t old_size = buf->size;
  size_t new_size = old_size + size + 1;

  if (buf->capacity < size) {
    int err = pni_buffer2_ensure(buf, new_size);
    if (err) return err;
  }

  buf->size = new_size;

  memmove(buf->bytes + old_size, bytes, size);
  buf->bytes[new_size - 1] = '\0';

  return 0;
}

// PN_FORCE_INLINE pn_bytes_t pn_buffer2_bytes(pn_buffer2_t *buf)
// {
//   if (buf) {
//     pn_buffer2_defrag(buf);
//     return pn_bytes(buf->size, buf->bytes);
//   } else {
//     return pn_bytes(0, NULL);
//   }
// }

// PN_FORCE_INLINE pn_rwbytes_t pn_buffer2_memory(pn_buffer2_t *buf)
// {
//   if (buf) {
//     pn_buffer2_defrag(buf);
//     pn_rwbytes_t r = {buf->size, buf->bytes};
//     return r;
//   } else {
//     pn_rwbytes_t r = {0, NULL};
//     return r;
//   }
// }
