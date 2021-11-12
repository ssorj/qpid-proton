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

#include <proton/import_export.h>
#include <proton/object.h>
#include <proton/types.h>

#include <assert.h>
#include <string.h>

#include "util.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct pn_buffer_t {
  char *bytes;
  char *start;
  size_t size;
  size_t capacity;
} pn_buffer_t;

pn_buffer_t *pn_buffer(size_t capacity);
void pn_buffer_free(pn_buffer_t *buf);
int pn_buffer_ensure(pn_buffer_t *buf, size_t size);
int pn_buffer_quote(pn_buffer_t *buf, pn_string_t *str, size_t n);

static inline size_t pn_buffer_capacity(pn_buffer_t *buf)
{
  assert(buf);
  return buf->capacity;
}

static inline size_t pn_buffer_size(pn_buffer_t *buf)
{
  assert(buf);
  return buf->size;
}

static inline size_t pn_buffer_available(pn_buffer_t *buf)
{
  assert(buf);
  return buf->capacity - buf->size;
}

static inline void pn_buffer_clear(pn_buffer_t *buf)
{
  assert(buf);

  buf->start = buf->bytes;
  buf->size = 0;
}

static inline pn_bytes_t pn_buffer_bytes(pn_buffer_t *buf)
{
  assert(buf);
  return pn_bytes(pn_buffer_size(buf), buf->start);
}

static inline pn_rwbytes_t pn_buffer_memory(pn_buffer_t *buf)
{
  assert(buf);
  return pn_rwbytes(pn_buffer_size(buf), buf->start);
}

static inline pn_rwbytes_t pn_buffer_free_memory(pn_buffer_t *buf)
{
  assert(buf);
  return pn_rwbytes(pn_buffer_available(buf), buf->start);
}

static inline int pn_buffer_append(pn_buffer_t *buf, const char *bytes, size_t n)
{
  assert(buf);

  size_t capacity = pn_buffer_capacity(buf);
  size_t old_size = pn_buffer_size(buf);
  size_t new_size = old_size + n;

  if (new_size > capacity) {
    int err = pn_buffer_ensure(buf, new_size);
    if (err) return err;
  }

  memcpy(buf->start + old_size, bytes, n);
  buf->size = new_size;

  return 0;
}

static inline int pn_buffer_append_string(pn_buffer_t *buf, const char *bytes, size_t n)
{
  assert(buf);

  size_t capacity = pn_buffer_capacity(buf);
  size_t old_size = pn_buffer_size(buf);
  size_t new_size = old_size + n + 1;

  if (new_size > capacity) {
    int err = pn_buffer_ensure(buf, new_size);
    if (err) return err;
  }

  memcpy(buf->start + old_size, bytes, n);
  buf->start[new_size - 1] = '\0';
  buf->size = new_size;

  return 0;
}

static inline size_t pn_buffer_pop_left(pn_buffer_t *buf, size_t n, char *dst)
{
  assert(buf);

  size_t old_size = pn_buffer_size(buf);
  n = pn_min(n, old_size);
  size_t new_size = old_size - n;

  if (dst) {
    memcpy(dst, buf->start, n);
  }

  // if (new_size) {
  //   memmove(buf->bytes, buf->bytes + size, new_size);
  // }

  if (new_size) {
    buf->start += n;
    buf->size = new_size;

    if (buf->start - buf->bytes > 1000 * 1000 * 10) {
      // fprintf(stderr, "pn_buffer_pop_left (memmove): start_offset=%ld n=%ld new_size=%ld\n", buf->start - buf->bytes, n, new_size);

      memmove(buf->bytes, buf->start, new_size);
      buf->start = buf->bytes;
    }
  } else {
    // fprintf(stderr, "pn_buffer_pop_left (clear): start_offset=%ld n=%ld new_size=%ld\n", buf->start - buf->bytes, n, new_size);

    pn_buffer_clear(buf);
  }

  // pn_buffer_pop_left (memmove): start_offset=1_048_576 n=131_072 new_size=35_089_796

  return n;
}

#ifdef __cplusplus
}
#endif

#endif /* buffer.h */
