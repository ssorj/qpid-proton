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
#include "platform/platform.h"

#include <proton/error.h>
#include <proton/object.h>

#include "core/config.h"
#include "core/memory.h"
#include "core/util.h"

#include <stdio.h>
#include <stddef.h>
#include <string.h>
#include <assert.h>
#include <ctype.h>

struct pn_string_t {
  char *bytes;
  size_t capacity;
  size_t size;
  bool is_set;
};

static void pn_string_finalize(void *object)
{
  pn_string_t *string = (pn_string_t *) object;
  pni_mem_subdeallocate(pn_class(string), string, string->bytes);
}

static uintptr_t pn_string_hashcode(void *object)
{
  pn_string_t *string = (pn_string_t *) object;
  if (!string->is_set) {
    return 0;
  }

  uintptr_t hashcode = 1;
  for (size_t i = 0; i < string->size; i++) {
    hashcode = hashcode * 31 + string->bytes[i];
  }
  return hashcode;
}

static intptr_t pn_string_compare(void *oa, void *ob)
{
  pn_string_t *a = (pn_string_t *) oa;
  pn_string_t *b = (pn_string_t *) ob;
  if (a->size != b->size) {
    return b->size - a->size;
  }

  if (a->is_set) {
    return memcmp(a->bytes, b->bytes, a->size);
  } else {
    return 0;
  }
}

static int pn_string_inspect(void *obj, pn_string_t *dst)
{
  pn_string_t *str = (pn_string_t *) obj;
  if (!str->is_set) {
    return pn_string_addf(dst, "null");
  }

  int err = pn_string_addf(dst, "\"");
  if (err) return err;

  for (size_t i = 0; i < str->size; i++) {
    uint8_t c = str->bytes[i];
    if (isprint(c)) {
      err = pn_string_addf(dst, "%c", c);
      if (err) return err;
    } else {
      err = pn_string_addf(dst, "\\x%.2x", c);
      if (err) return err;
    }
  }

  return pn_string_addf(dst, "\"");
}

pn_string_t *pn_string(const char *bytes)
{
  return pn_stringn(bytes, bytes ? strlen(bytes) : 0);
}

#define pn_string_initialize NULL

PN_INLINE pn_string_t *pn_stringn(const char *bytes, size_t n)
{
  static const pn_class_t clazz = PN_CLASS(pn_string);
  pn_string_t *string = (pn_string_t *) pn_class_new(&clazz, sizeof(pn_string_t));

  pn_string_setn(string, bytes, n);

  return string;
}

PN_INLINE const char *pn_string_get(pn_string_t *string)
{
  assert(string);
  if (string->is_set) {
    return string->bytes;
  } else {
    return NULL;
  }
}

PN_INLINE size_t pn_string_size(pn_string_t *string)
{
  assert(string);
  return string->size;
}

PN_INLINE int pn_string_set(pn_string_t *string, const char *bytes)
{
  return pn_string_setn(string, bytes, bytes ? strlen(bytes) : 0);
}

PN_NO_INLINE static int pni_string_grow(pn_string_t *string, size_t capacity)
{
  bool grow = false;

  while (string->capacity < (capacity * sizeof(char) + 1)) {
    string->capacity = pn_max(2, string->capacity * 2);
    grow = true;
  }

  if (grow) {
    char *growed = (char *) pni_mem_subreallocate(pn_class(string), string, string->bytes, string->capacity);
    if (growed) {
      string->bytes = growed;
    } else {
      return PN_ERR;
    }
  }

  return 0;
}

int pn_string_grow(pn_string_t *string, size_t capacity) {
  return pni_string_grow(string, capacity);
}

PN_INLINE int pn_string_setn(pn_string_t *string, const char *bytes, size_t n)
{
  if (!bytes) {
    pn_string_clear(string);
    return 0;
  }

  if (string->capacity < n * sizeof(char) + 1) {
    int err = pni_string_grow(string, n);
    if (err) return err;
  }

  memcpy(string->bytes, bytes, n * sizeof(char));

  string->bytes[n] = '\0';
  string->size = n;
  string->is_set = true;

  return 0;
}

PN_INLINE ssize_t pn_string_put(pn_string_t *string, char *dst)
{
  assert(string);
  assert(dst);

  if (string->is_set) {
    memcpy(dst, string->bytes, string->size + 1);
  }

  return string->size;
}

PN_INLINE void pn_string_clear(pn_string_t *string)
{
  string->size = 0;
  string->is_set = false;
}

int pn_string_format(pn_string_t *string, const char *format, ...)
{
  va_list ap;

  va_start(ap, format);
  int err = pn_string_vformat(string, format, ap);
  va_end(ap);
  return err;
}

int pn_string_vformat(pn_string_t *string, const char *format, va_list ap)
{
  pn_string_set(string, "");
  return pn_string_vaddf(string, format, ap);
}

int pn_string_addf(pn_string_t *string, const char *format, ...)
{
  va_list ap;

  va_start(ap, format);
  int err = pn_string_vaddf(string, format, ap);
  va_end(ap);
  return err;
}

int pn_string_vaddf(pn_string_t *string, const char *format, va_list ap)
{
  va_list copy;

  if (!string->is_set) {
    return PN_ERR;
  }

  while (true) {
    va_copy(copy, ap);
    int err = vsnprintf(string->bytes + string->size, string->capacity - string->size, format, copy);
    va_end(copy);
    if (err < 0) {
      return err;
    } else if ((size_t) err >= string->capacity - string->size) {
      pni_string_grow(string, string->size + err);
    } else {
      string->size += err;
      return 0;
    }
  }
}

PN_INLINE char *pn_string_buffer(pn_string_t *string)
{
  assert(string);
  return string->bytes;
}

PN_INLINE size_t pn_string_capacity(pn_string_t *string)
{
  assert(string);
  return string->capacity - 1;
}

PN_INLINE int pn_string_resize(pn_string_t *string, size_t size)
{
  assert(string);
  int err = pni_string_grow(string, size);
  if (err) return err;
  string->size = size;
  string->bytes[size] = '\0';
  return 0;
}

PN_INLINE int pn_string_copy(pn_string_t *string, pn_string_t *src)
{
  assert(string);
  return pn_string_setn(string, pn_string_get(src), pn_string_size(src));
}
