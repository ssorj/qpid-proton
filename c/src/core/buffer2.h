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

typedef struct pn_buffer2_t {
  char *bytes;
  size_t capacity;
  size_t size;
} pn_buffer2_t;

// typedef struct pn_buffer2_t pn_buffer2_t;

pn_buffer2_t *pn_buffer2(size_t capacity);
void pn_buffer2_free(pn_buffer2_t *buf);

static inline size_t pn_buffer2_size(pn_buffer2_t *buf)
{
  return buf->size;
}

static inline void pn_buffer2_clear(pn_buffer2_t *buf)
{
  buf->size = 0;
}

int pn_buffer2_append_string(pn_buffer2_t *buf, const char *bytes, size_t size);

// size_t pn_buffer2_size(pn_buffer2_t *buf);
// size_t pn_buffer2_capacity(pn_buffer2_t *buf);
// size_t pn_buffer2_available(pn_buffer2_t *buf);
// int pn_buffer2_ensure(pn_buffer2_t *buf, size_t size);
// int pn_buffer2_append(pn_buffer2_t *buf, const char *bytes, size_t size);
// int pn_buffer2_prepend(pn_buffer2_t *buf, const char *bytes, size_t size);
// size_t pn_buffer2_get(pn_buffer2_t *buf, size_t offset, size_t size, char *dst);
// int pn_buffer2_trim(pn_buffer2_t *buf, size_t left, size_t right);
// void pn_buffer2_clear(pn_buffer2_t *buf);
// int pn_buffer2_defrag(pn_buffer2_t *buf);
// pn_bytes_t pn_buffer2_bytes(pn_buffer2_t *buf);
// pn_rwbytes_t pn_buffer2_memory(pn_buffer2_t *buf);
// int pn_buffer2_quote(pn_buffer2_t *buf, pn_string_t *string, size_t n);

#ifdef __cplusplus
}
#endif

#endif /* buffer2.h */
