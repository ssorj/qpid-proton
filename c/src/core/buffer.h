#ifndef PROTON_BUFFER_H
#define PROTON_BUFFER_H 1

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
#include <proton/import_export.h>
#include <proton/types.h>

#include <assert.h>
#include <string.h>

#include "core/object_private.h"

#ifdef __cplusplus
extern "C" {
#endif

struct pn_buffer_t {
  char *bytes;
  size_t capacity;
  size_t start;
  size_t size;
};

typedef struct pn_buffer_t pn_buffer_t;

PN_EXTERN pn_buffer_t *pn_buffer(size_t capacity);
PN_EXTERN void pn_buffer_free(pn_buffer_t *buffer);
PN_EXTERN int pn_buffer_ensure(pn_buffer_t *buffer, size_t size);

static inline size_t pn_buffer_size(pn_buffer_t *buffer)
{
  assert(buffer);
  return buffer->size;
}

static inline size_t pn_buffer_capacity(pn_buffer_t *buffer)
{
  assert(buffer);
  return buffer->capacity;
}

static inline void pn_buffer_clear(pn_buffer_t *buffer)
{
  assert(buffer);

  buffer->start = 0;
  buffer->size = 0;
}

static inline char *pn_buffer_get_write_ptr(pn_buffer_t *buffer, size_t size)
{
  assert(buffer);

  size_t required = buffer->size + size;

  if (buffer->start + required > buffer->capacity) {
    int err = pn_buffer_ensure(buffer, size);
    if (err) return NULL;
  }

  return &buffer->bytes[buffer->start + buffer->size];
}

static inline void pn_buffer_advance_write_ptr(pn_buffer_t *buffer, size_t size)
{
  assert(buffer);
  assert(buffer->start + buffer->size + size <= buffer->capacity);

  buffer->size += size;
}

static inline int pn_buffer_write(pn_buffer_t *buffer, const char *bytes, size_t size)
{
  assert(buffer);

  char *dst = pn_buffer_get_write_ptr(buffer, size);
  if (!dst) return PN_OUT_OF_MEMORY;

  memcpy(dst, bytes, size);
  pn_buffer_advance_write_ptr(buffer, size);

  return 0;
}

static inline char *pn_buffer_get_read_ptr(pn_buffer_t *buffer)
{
  assert(buffer);
  return &buffer->bytes[buffer->start];
}

static inline void pn_buffer_advance_read_ptr(pn_buffer_t *buffer, size_t size)
{
  assert(buffer);
  assert(size <= buffer->size);

  buffer->start += size;
  buffer->size -= size;

  if (!buffer->size) {
    buffer->start = 0;
  }
}

static inline size_t pn_buffer_read(pn_buffer_t *buffer, size_t size, char *dst)
{
  assert(buffer);

  if (!size) return 0;

  if (buffer->size < size) {
    size = buffer->size;
  }

  memcpy(dst, pn_buffer_get_read_ptr(buffer), size);
  pn_buffer_advance_read_ptr(buffer, size);

  return size;
}

static inline pn_bytes_t pn_buffer_bytes(pn_buffer_t *buffer)
{
  assert(buffer);

  pn_bytes_t bytes = { buffer->size, &buffer->bytes[buffer->start] };

  return bytes;
}

#ifdef __cplusplus
}
#endif

#endif /* buffer.h */
