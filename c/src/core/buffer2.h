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
int pni_buffer2_append(pni_buffer2_t *buf, const char *bytes, size_t size);
int pni_buffer2_append_string(pni_buffer2_t *buf, const char *bytes, size_t size);
size_t pni_buffer2_pop_left(pni_buffer2_t *buf, size_t size, char *dst);
int pni_buffer2_quote(pni_buffer2_t *buf, pn_string_t *str, size_t n);

static inline size_t pni_buffer2_capacity(pni_buffer2_t *buf)
{
  return buf->capacity;
}

static inline size_t pni_buffer2_size(pni_buffer2_t *buf)
{
  return buf->size;
}

static inline void pni_buffer2_clear(pni_buffer2_t *buf)
{
  buf->size = 0;
}

static inline pn_bytes_t pni_buffer2_bytes(pni_buffer2_t *buf)
{
  return pn_bytes(pni_buffer2_size(buf), buf->bytes);
}

#ifdef __cplusplus
}
#endif

#endif /* buffer2.h */
