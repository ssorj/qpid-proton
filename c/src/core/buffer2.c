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

PN_STRUCT_CLASSDEF(pni_buffer2)

pni_buffer2_t *pni_buffer2(size_t capacity)
{
  pni_buffer2_t *buf = (pni_buffer2_t *) pni_mem_allocate(PN_CLASSCLASS(pni_buffer2), sizeof(pni_buffer2_t));

  if (buf != NULL) {
    buf->capacity = capacity;
    buf->size = 0;

    if (capacity > 0) {
        buf->bytes = (char *) pni_mem_suballocate(PN_CLASSCLASS(pni_buffer2), buf, capacity);

        if (buf->bytes == NULL) {
            pni_mem_deallocate(PN_CLASSCLASS(pni_buffer2), buf);
            buf = NULL;
        }
    } else {
        buf->bytes = NULL;
    }
  }

  return buf;
}

void pni_buffer2_free(pni_buffer2_t *buf)
{
  if (buf) {
    pni_mem_subdeallocate(PN_CLASSCLASS(pni_buffer2), buf, buf->bytes);
    pni_mem_deallocate(PN_CLASSCLASS(pni_buffer2), buf);
  }
}

int pni_buffer2_ensure(pni_buffer2_t *buf, size_t size)
{
  size_t old_capacity = pni_buffer2_capacity(buf);
  size_t old_size = pni_buffer2_size(buf);
  size_t new_capacity = old_capacity;
  size_t new_size = old_size + size;

  while (new_capacity < new_size) new_capacity = 2 * new_capacity;

  buf->bytes = (char *) pni_mem_subreallocate(PN_CLASSCLASS(pni_buffer2), buf, buf->bytes, new_capacity);
  if (!buf->bytes) return PN_OUT_OF_MEMORY;

  buf->capacity = new_capacity;

  return 0;
}

PN_INLINE int pni_buffer2_append(pni_buffer2_t *buf, const char *bytes, size_t size)
{
  size_t capacity = pni_buffer2_capacity(buf);
  size_t old_size = pni_buffer2_size(buf);
  size_t new_size = old_size + size;

  if (new_size > capacity) {
    int err = pni_buffer2_ensure(buf, new_size);
    if (err) return err;
  }

  memcpy(buf->bytes + old_size, bytes, size);
  buf->size = new_size;

  return 0;
}

PN_INLINE int pni_buffer2_append_string(pni_buffer2_t *buf, const char *bytes, size_t size)
{
  size_t capacity = pni_buffer2_capacity(buf);
  size_t old_size = pni_buffer2_size(buf);
  size_t new_size = old_size + size + 1;

  if (new_size > capacity) {
    int err = pni_buffer2_ensure(buf, new_size);
    if (err) return err;
  }

  memcpy(buf->bytes + old_size, bytes, size);
  buf->bytes[new_size - 1] = '\0';
  buf->size = new_size;

  return 0;
}

PN_INLINE size_t pni_buffer2_pop_left(pni_buffer2_t *buf, size_t size, char *dst)
{
  size_t old_size = pni_buffer2_size(buf);
  size = pn_min(size, old_size);
  size_t new_size = old_size - size;

  if (dst) {
    memcpy(dst, buf->bytes, size);
  }

  if (new_size) {
    memmove(buf->bytes, buf->bytes + size, new_size);
  }

  buf->size = new_size;

  return size;
}

int pni_buffer2_quote(pni_buffer2_t *buf, pn_string_t *str, size_t n)
{
  return pn_quote(str, buf->bytes, pn_min(n, pni_buffer2_size(buf)));
}
