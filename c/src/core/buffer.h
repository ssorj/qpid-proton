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
#include <proton/types.h>

#include <assert.h>
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
PN_EXTERN void pn_buffer_free(pn_buffer_t *buf);
PN_EXTERN int pn_buffer_append(pn_buffer_t *buf, const char *bytes, size_t size);
PN_EXTERN pn_bytes_t pn_buffer_bytes(pn_buffer_t *buf);
PN_EXTERN void pn_buffer_trim_left(pn_buffer_t *buf, size_t size);
PN_EXTERN size_t pn_buffer_pop_left(pn_buffer_t *buf, size_t size, char *dst);

// XXX Only messenger uses this.  Remove it when messenger is gone.
PN_EXTERN pn_rwbytes_t pn_buffer_memory(pn_buffer_t *buf);
// XXX Only messenger uses this.  Remove it when messenger is gone.
PN_EXTERN int pn_buffer_ensure(pn_buffer_t *buf, size_t needed);

static inline size_t pn_buffer_size(pn_buffer_t *buf)
{
  return buf->size;
}

static inline size_t pn_buffer_capacity(pn_buffer_t *buf)
{
  return buf->capacity;
}

static inline size_t pn_buffer_available(pn_buffer_t *buf)
{
  return buf->capacity - buf->size;
}

static inline void pn_buffer_clear(pn_buffer_t *buf)
{
  buf->start = 0;
  buf->size = 0;
}

PN_EXTERN char *pn_buffer_write_ptr(pn_buffer_t *buf, size_t size);

static inline void pn_buffer_advance_write(pn_buffer_t *buf, size_t size)
{
  assert(buf);
  assert(buf->start + buf->size + size <= buf->capacity);

  buf->size += size;
}

static inline char *pn_buffer_read_ptr(pn_buffer_t *buf, size_t size)
{
  assert(buf);

  if (buf->size < size) return NULL;

  return buf->bytes + buf->start;
}

static inline void pn_buffer_advance_read(pn_buffer_t *buf, size_t size)
{
  assert(buf);
  assert(size <= buf->size);

  buf->start += size;
  buf->size -= size;

  if (buf->size == 0) buf->start = 0;
}

#ifdef __cplusplus
}
#endif

#endif /* buffer.h */
