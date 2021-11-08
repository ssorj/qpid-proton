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
  size_t capacity;
  size_t size;
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
  buf->size = 0;
}

static inline pn_bytes_t pn_buffer_bytes(pn_buffer_t *buf)
{
  assert(buf);
  return pn_bytes(pn_buffer_size(buf), buf->bytes);
}

static inline pn_rwbytes_t pn_buffer_memory(pn_buffer_t *buf)
{
  assert(buf);
  return pn_rwbytes(pn_buffer_size(buf), buf->bytes);
}

static inline pn_rwbytes_t pn_buffer_free_memory(pn_buffer_t *buf)
{
  assert(buf);
  return pn_rwbytes(pn_buffer_available(buf), buf->bytes);
}

static inline int pn_buffer_append(pn_buffer_t *buf, const char *bytes, size_t size)
{
  assert(buf);

  size_t capacity = pn_buffer_capacity(buf);
  size_t old_size = pn_buffer_size(buf);
  size_t new_size = old_size + size;

  if (new_size > capacity) {
    int err = pn_buffer_ensure(buf, new_size);
    if (err) return err;
  }

  memcpy(buf->bytes + old_size, bytes, size);
  buf->size = new_size;

  return 0;
}

static inline int pn_buffer_append_string(pn_buffer_t *buf, const char *bytes, size_t size)
{
  assert(buf);

  size_t capacity = pn_buffer_capacity(buf);
  size_t old_size = pn_buffer_size(buf);
  size_t new_size = old_size + size + 1;

  if (new_size > capacity) {
    int err = pn_buffer_ensure(buf, new_size);
    if (err) return err;
  }

  memcpy(buf->bytes + old_size, bytes, size);
  buf->bytes[new_size - 1] = '\0';
  buf->size = new_size;

  return 0;
}

static inline size_t pn_buffer_pop_left(pn_buffer_t *buf, size_t size, char *dst)
{
  assert(buf);

  size_t old_size = pn_buffer_size(buf);
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

#ifdef __cplusplus
}
#endif

#endif /* buffer.h */
