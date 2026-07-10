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

#include <assert.h>
#ifndef __cplusplus
#include <stdbool.h>
#endif
#include <stddef.h>
#include <string.h>
#include <stdio.h>

#include "buffer.h"
#include "memory.h"
#include "util.h"

PN_STRUCT_CLASSDEF(pn_buffer)

static int buffer_grow(pn_buffer_t *buf, size_t size)
{
  size_t required = buf->size + size;

  assert(required >= buf->size);

  if (buf->start > 0 && (buf->start + required > buf->capacity)) {
    memmove(buf->bytes, buf->bytes + buf->start, buf->size);
    buf->start = 0;
  }

  if (required <= buf->capacity) return 0;

  size_t new_capacity = buf->capacity ? buf->capacity : 16;

  while (new_capacity < required) {
    if (new_capacity > SIZE_MAX >> 1) {
      new_capacity = required;
      break;
    }

    new_capacity <<= 1;
  }

  char *new_bytes = (char *) pni_mem_subreallocate(PN_CLASSCLASS(pn_buffer), buf, buf->bytes, new_capacity);
  if (!new_bytes) return PN_OUT_OF_MEMORY;

  buf->bytes = new_bytes;
  buf->capacity = new_capacity;

  return 0;
}

pn_buffer_t *pn_buffer(size_t capacity)
{
  pn_buffer_t *buf = (pn_buffer_t *) pni_mem_zallocate(PN_CLASSCLASS(pn_buffer), sizeof(pn_buffer_t));
  if (!buf) return NULL;

  if (capacity > 0) {
    int err = buffer_grow(buf, capacity);

    if (err) {
      pni_mem_deallocate(PN_CLASSCLASS(pn_buffer), buf);
      return NULL;
    }
  }

  return buf;
}

void pn_buffer_free(pn_buffer_t *buf)
{
  if (!buf) return;

  pni_mem_subdeallocate(PN_CLASSCLASS(pn_buffer), buf, buf->bytes);
  pni_mem_deallocate(PN_CLASSCLASS(pn_buffer), buf);
}

char *pn_buffer_write_ptr(pn_buffer_t *buf, size_t size)
{
  assert(buf);

  if (!size) return buf->bytes + buf->start + buf->size;

  if (buf->start + buf->size + size > buf->capacity) {
    int err = buffer_grow(buf, size);
    if (err) return NULL;
  }

  return buf->bytes + buf->start + buf->size;
}

// ---

int pn_buffer_append(pn_buffer_t *buf, const char *bytes, size_t size)
{
  assert(buf);

  if (!size) return 0;

  char *dst = pn_buffer_write_ptr(buf, size);
  if (!dst) return PN_OUT_OF_MEMORY;

  memcpy(dst, bytes, size);
  pn_buffer_advance_write(buf, size);

  return 0;
}

pn_bytes_t pn_buffer_bytes(pn_buffer_t *buf)
{
  assert(buf);

  return (pn_bytes_t) {
    .start = buf->bytes + buf->start,
    .size = buf->size,
  };
}

void pn_buffer_trim_left(pn_buffer_t *buf, size_t size)
{
  assert(buf);
  assert(size <= buf->size);

  buf->start += size;
  buf->size -= size;

  if (buf->size == 0) buf->start = 0;
}

size_t pn_buffer_pop_left(pn_buffer_t *buf, size_t size, char *dst)
{
  assert(buf);

  if (buf->size < size) size = buf->size;
  if (!size) return 0;

  memcpy(dst, pn_buffer_read_ptr(buf, size), size);
  pn_buffer_advance_read(buf, size);

  return size;
}

// XXX Only messenger uses this.  Remove it when messenger is gone.
pn_rwbytes_t pn_buffer_memory(pn_buffer_t *buf)
{
  assert(buf);

  pn_bytes_t bytes = pn_buffer_bytes(buf);

  return (pn_rwbytes_t) {
    .start = (char *) bytes.start,
    .size = bytes.size,
  };
}

// XXX Only messenger uses this.  Remove it when messenger is gone.
int pn_buffer_ensure(pn_buffer_t *buf, size_t size)
{
  return buffer_grow(buf, size);
}
