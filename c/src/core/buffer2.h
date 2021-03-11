#ifndef PROTON_BUFFER2_H
#define PROTON_BUFFER2_H 1

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

#include <string.h>

#include "util.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct pni_buffer2_t {
  char *bytes;
  size_t capacity;
  size_t size;
} pni_buffer2_t;

pni_buffer2_t *pni_buffer2(size_t capacity);
void pni_buffer2_free(pni_buffer2_t *buf);
int pni_buffer2_ensure(pni_buffer2_t *buf, size_t size);
int pni_buffer2_quote(pni_buffer2_t *buf, pn_string_t *str, size_t n);

static inline size_t pni_buffer2_capacity(pni_buffer2_t *buf)
{
  return buf->capacity;
}

static inline size_t pni_buffer2_size(pni_buffer2_t *buf)
{
  return buf->size;
}

static inline size_t pni_buffer2_available(pni_buffer2_t *buf)
{
  return buf->capacity - buf->size;
}

static inline void pni_buffer2_clear(pni_buffer2_t *buf)
{
  buf->size = 0;
}

static inline pn_bytes_t pni_buffer2_bytes(pni_buffer2_t *buf)
{
  return pn_bytes(pni_buffer2_size(buf), buf->bytes);
}

static inline pn_rwbytes_t pni_buffer2_memory(pni_buffer2_t *buf)
{
  return pn_rwbytes(pni_buffer2_size(buf), buf->bytes);
}

PNI_INLINE static int pni_buffer2_append(pni_buffer2_t *buf, const char *bytes, size_t size)
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

PNI_INLINE static int pni_buffer2_append_string(pni_buffer2_t *buf, const char *bytes, size_t size)
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

PNI_INLINE static size_t pni_buffer2_pop_left(pni_buffer2_t *buf, size_t size, char *dst)
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

#ifdef __cplusplus
}
#endif

#endif /* buffer2.h */
