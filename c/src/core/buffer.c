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

static inline size_t buffer_wrap(size_t index, size_t capacity)
{
  return (index >= capacity) ? (index - capacity) : index;
}

static inline size_t buffer_space(const pn_buffer_t *buf)
{
  return buf->capacity - buf->size;
}

static int buffer_defrag(pn_buffer_t *buf)
{
  size_t len1 = buf->capacity - buf->start;
  size_t len2 = buf->size - len1;

  char *tmp = (char *) pni_mem_allocate(PN_CLASSCLASS(pn_buffer), buf->size);
  if (!tmp) return PN_OUT_OF_MEMORY;

  memcpy(tmp, buf->bytes + buf->start, len1);
  memcpy(tmp + len1, buf->bytes, len2);
  memcpy(buf->bytes, tmp, buf->size);

  pni_mem_deallocate(PN_CLASSCLASS(pn_buffer), tmp);

  buf->start = 0;

  return 0;
}

static int buffer_grow(pn_buffer_t *buf, size_t needed)
{
  size_t required = buf->size + needed;

  assert(required < buf->size);

  if (buf->start + buf->size > buf->capacity) {
    int err = buffer_defrag(buf);
    if (err) return err;
  }

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

int pn_buffer_append(pn_buffer_t *buf, const char *bytes, size_t size)
{
  assert(buf);

  if (!size) return 0;

  if (buffer_space(buf) < size) {
    int err = buffer_grow(buf, size);
    if (err) return err;
  }

  size_t end = buffer_wrap(buf->start + buf->size, buf->capacity);
  size_t first_chunk = buf->capacity - end;

  if (size <= first_chunk) {
    memcpy(buf->bytes + end, bytes, size);
  } else {
    memcpy(buf->bytes + end, bytes, first_chunk);
    memcpy(buf->bytes, bytes + first_chunk, size - first_chunk);
  }

  buf->size += size;

  return 0;
}

pn_bytes_t pn_buffer_bytes(pn_buffer_t *buf)
{
  assert(buf);

  if (buf->start + buf->size > buf->capacity) {
    // XXX This can fail!
    buffer_defrag(buf);
  }

  return (pn_bytes_t) {
    .start = buf->bytes + buf->start,
    .size = buf->size,
  };
}

void pn_buffer_trim_left(pn_buffer_t *buf, size_t size)
{
  assert(buf);
  assert(size <= buf->size);

  buf->start = buffer_wrap(buf->start + size, buf->capacity);
  buf->size -= size;
}

size_t pn_buffer_pop_left(pn_buffer_t *buf, size_t size, char *dst)
{
  assert(buf);
  assert(buf->start <= buf->capacity);

  if (buf->size < size) size = buf->size;
  if (!size) return 0;

  size_t first_chunk = buf->capacity - buf->start;

  if (size <= first_chunk) {
    memcpy(dst, buf->bytes + buf->start, size);
  } else {
    memcpy(dst, buf->bytes + buf->start, first_chunk);
    memcpy(dst + first_chunk, buf->bytes, size - first_chunk);
  }

  buf->start = buffer_wrap(buf->start + size, buf->capacity);
  buf->size -= size;

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
