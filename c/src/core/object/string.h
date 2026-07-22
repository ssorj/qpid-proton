#ifndef STRING_H
#define STRING_H

/*
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
 */

#include "platform/platform.h"

#include <assert.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <stdio.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct pn_string_t {
  char *bytes;
  size_t size;
  size_t capacity;
  bool is_set;
} pn_string_t;

PN_EXTERN pn_string_t *pn_stringn(const char *bytes, size_t n);
PN_EXTERN int pn_string_setn(pn_string_t *string, const char *bytes, size_t size);
PN_EXTERN int pn_string_resize(pn_string_t *string, size_t size);
PN_EXTERN char *pn_string_buffer(pn_string_t *string);

PN_EXTERN int pn_string_format(pn_string_t *string, PN_PRINTF_FORMAT const char *format, ...)
        PN_PRINTF_FORMAT_ATTR(2, 3);
int pn_string_vformat(pn_string_t *string, const char *format, va_list ap);
PN_EXTERN int pn_string_addf(pn_string_t *string, PN_PRINTF_FORMAT const char *format, ...)
        PN_PRINTF_FORMAT_ATTR(2, 3);
int pn_string_vaddf(pn_string_t *string, const char *format, va_list ap);

static inline pn_string_t *pn_string(const char *bytes)
{
  return pn_stringn(bytes, bytes ? strlen(bytes) : 0);
}

static inline const char *pn_string_get(pn_string_t *string) {
    assert(string);

    if (!string->is_set) return NULL;

    return string->bytes;
}

static inline pn_bytes_t pn_string_bytes(pn_string_t *string)
{
  assert(string);

  if (!string->is_set) return pn_bytes_null;

  pn_bytes_t bytes = { string->size, string->bytes };

  return bytes;
}

static inline size_t pn_string_size(pn_string_t *string)
{
  assert(string);
  return string->size;
}

static inline size_t pn_string_capacity(pn_string_t *string) {
  assert(string);
  return string->capacity;
}

static inline int pn_string_set(pn_string_t *string, const char *bytes) {
  assert(string);
  return pn_string_setn(string, bytes, bytes ? strlen(bytes) : 0);
}

static inline void pn_string_clear(pn_string_t *string) {
    assert(string);

    if (string->capacity > 0) {
        string->bytes[0] = '\0';
    }

    string->size = 0;
    string->is_set = false;
}

static inline int pn_string_copy(pn_string_t *string, pn_string_t *src) {
  assert(string);
  assert(src);

  return pn_string_setn(string, src->bytes, src->size);
}

#ifdef __cplusplus
}
#endif

#endif /* STRING_H */
