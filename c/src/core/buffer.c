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

#include "buffer.h"
#include "config.h"
#include "memory.h"

PN_STRUCT_CLASSDEF(pn_buffer)

pn_buffer_t *pn_buffer(size_t capacity)
{
  pn_buffer_t *buf = (pn_buffer_t *) pni_mem_allocate(PN_CLASSCLASS(pn_buffer), sizeof(pn_buffer_t));

  if (buf != NULL) {
    buf->capacity = capacity;
    buf->size = 0;

    if (capacity > 0) {
        buf->bytes = (char *) pni_mem_suballocate(PN_CLASSCLASS(pn_buffer), buf, capacity);
        buf->start = buf->bytes;

        if (buf->bytes == NULL) {
            pni_mem_deallocate(PN_CLASSCLASS(pn_buffer), buf);
            buf = NULL;
        }
    } else {
        buf->bytes = NULL;
        buf->start = NULL;
    }
  }

  return buf;
}

void pn_buffer_free(pn_buffer_t *buf)
{
  if (!buf) {
      return;
  }

  pni_mem_subdeallocate(PN_CLASSCLASS(pn_buffer), buf, buf->bytes);
  pni_mem_deallocate(PN_CLASSCLASS(pn_buffer), buf);
}

int pn_buffer_ensure(pn_buffer_t *buf, size_t n)
{
  assert(buf);

  size_t start_offset = buf->start - buf->bytes;
  size_t old_capacity = pn_buffer_capacity(buf);
  size_t old_size = pn_buffer_size(buf);
  size_t new_capacity = old_capacity;
  size_t new_size = old_size + n;

  if (n <= old_capacity) {
    return 0;
  }

  while (new_capacity < new_size) new_capacity = 2 * new_capacity;

  buf->bytes = (char *) pni_mem_subreallocate(PN_CLASSCLASS(pn_buffer), buf, buf->bytes, new_capacity + start_offset);
  if (!buf->bytes) return PN_OUT_OF_MEMORY;

  buf->start = buf->bytes + start_offset;
  buf->capacity = new_capacity;

  // fprintf(stderr, "pn_buffer_ensure: old_capacity=%ld old_size=%ld new_capacity=%ld new_size=%ld n=%ld\n",
  //         old_capacity, old_size, new_capacity, new_size, n);

  return 0;
}

int pn_buffer_quote(pn_buffer_t *buf, pn_string_t *str, size_t n)
{
  assert(buf);
  return pn_quote(str, buf->start, pn_min(n, pn_buffer_size(buf)));
}
