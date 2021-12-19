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

// The idea here is to (1) make trim and pop extra cheap and (2) make memory views extra cheap
// Trim and pop move the start offset
// The capacity is computed relative to the start offset
// The start offset is reset to 0 whenever we grow the capacity

typedef struct pn_buffer_t {
  char *bytes;
  size_t start;
  size_t end;
  size_t size;
} pn_buffer_t;

pn_buffer_t *pn_buffer(size_t capacity);
void pn_buffer_free(pn_buffer_t *buf);
int pn_buffer_ensure(pn_buffer_t *buf, size_t size);
int pn_buffer_quote(pn_buffer_t *buf, pn_string_t *str, size_t n);

static inline size_t pn_buffer_capacity(pn_buffer_t *buf)
{
  assert(buf);
  return buf->end - buf->start;
}

static inline size_t pn_buffer_size(pn_buffer_t *buf)
{
  assert(buf);
  return buf->size;
}

static inline size_t pn_buffer_available(pn_buffer_t *buf)
{
  assert(buf);
  return pn_buffer_capacity(buf) - pn_buffer_size(buf);
}

static inline void pn_buffer_clear(pn_buffer_t *buf)
{
  assert(buf);

  buf->start = 0;
  buf->size = 0;
}

static inline pn_bytes_t pn_buffer_bytes(pn_buffer_t *buf)
{
  assert(buf);
  return pn_bytes(pn_buffer_size(buf), &buf->bytes[buf->start]);
}

static inline pn_rwbytes_t pn_buffer_memory(pn_buffer_t *buf)
{
  assert(buf);
  return pn_rwbytes(pn_buffer_size(buf), &buf->bytes[buf->start]);
}

static inline pn_rwbytes_t pn_buffer_free_memory(pn_buffer_t *buf)
{
  assert(buf);
  return pn_rwbytes(pn_buffer_available(buf), &buf->bytes[buf->start]);
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

  memcpy(&buf->bytes[buf->start + old_size], bytes, n);
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

  memcpy(&buf->bytes[buf->start + old_size], bytes, n);
  buf->bytes[buf->start + new_size - 1] = '\0';
  buf->size = new_size;

  return 0;
}

static inline size_t pn_buffer_trim_left(pn_buffer_t *buf, size_t n)
{
  assert(buf);

  size_t old_size = pn_buffer_size(buf);
  n = pn_min(n, old_size);
  size_t new_size = old_size - n;

  if (new_size) {
    buf->start += n;
    buf->size = new_size;
  } else {
    pn_buffer_clear(buf);
  }

  return n;
}

static inline size_t pn_buffer_pop_left(pn_buffer_t *buf, size_t n, char *dst)
{
  assert(buf);
  assert(dst);

  n = pn_min(n, pn_buffer_size(buf));

  memcpy(dst, &buf->bytes[buf->start], n);

  return pn_buffer_trim_left(buf, n);
}

#ifdef __cplusplus
}
#endif

#endif /* buffer.h */
