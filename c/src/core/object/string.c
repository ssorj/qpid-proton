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
#include <proton/object.h>

#include "core/fixed_string.h"
#include "core/memory.h"
#include "platform/platform.h"

#include <stdio.h>
#include <stddef.h>
#include <string.h>
#include <assert.h>
#include <ctype.h>

#define pn_string_initialize NULL

static void pn_string_finalize(void *object)
{
  pn_string_t *string = (pn_string_t *) object;
  pni_mem_subdeallocate(pn_class(string), string, string->bytes);
}

static uintptr_t pn_string_hashcode(void *object)
{
  pn_string_t *string = (pn_string_t *) object;

  if (!string->size) return 0;
  if (!string->is_set) return 0;

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

  if (!a->size) return 0;
  if (!a->is_set) return 0;

  return memcmp(a->bytes, b->bytes, a->size);
}

static void pn_string_inspect(void *obj, pn_fixed_string_t *dst)
{
  pn_string_t *str = (pn_string_t *) obj;

  if (!str->is_set) {
    pn_fixed_string_addf(dst, "null");
    return;
  }

  pn_fixed_string_addf(dst, "\"");

  for (size_t i = 0; i < str->size; i++) {
    uint8_t c = str->bytes[i];

    if (isprint(c)) {
      pn_fixed_string_addf(dst, "%c", c);
    } else {
      pn_fixed_string_addf(dst, "\\x%.2x", c);
    }
  }

  pn_fixed_string_addf(dst, "\"");
}

static int string_grow(pn_string_t *string, size_t required)
{
  assert(string);
  assert(required > string->capacity);

  // Compute next power of two greater than or equal to required

  size_t new_capacity = required - 1;

  new_capacity |= (new_capacity >> 1);
  new_capacity |= (new_capacity >> 2);
  new_capacity |= (new_capacity >> 4);
  new_capacity |= (new_capacity >> 8);
  new_capacity |= (new_capacity >> 16);

  #if UINTPTR_MAX == 0xffffffffffffffffULL
  new_capacity |= (new_capacity >> 32);
  #endif

  new_capacity++;

  // Handle overflow
  if (new_capacity == 0) {
    new_capacity = required;
  }

  char *new_bytes = (char *) pni_mem_subreallocate(pn_class(string), string, string->bytes, new_capacity);
  if (!new_bytes) return PN_OUT_OF_MEMORY;

  string->bytes = new_bytes;
  string->capacity = new_capacity;

  return 0;
}

static inline int string_set(pn_string_t *string, const char *bytes, size_t size)
{
  assert(string);
  assert(bytes);

  if (size + 1 > string->capacity) {
    int err = string_grow(string, size + 1);
    if (err) return err;
  }

  if (size > 0) {
    memcpy(string->bytes, bytes, size);
  }

  string->bytes[size] = '\0';
  string->size = size;
  string->is_set = true;

  return 0;
}

pn_string_t *pn_stringn(const char *bytes, size_t size)
{
  assert(bytes || (!bytes && !size));

  static const pn_class_t clazz = PN_CLASS(pn_string);

  pn_string_t *string = (pn_string_t *) pn_class_new(&clazz, sizeof(pn_string_t));
  if (!string) return NULL;

  if (!bytes) return string;

  int err = string_set(string, bytes, size);

  if (err) {
    pni_mem_deallocate(&clazz, string);
    return NULL;
  }

  string->size = size;
  string->is_set = true;

  return string;
}

int pn_string_setn(pn_string_t *string, const char *bytes, size_t size)
{
  assert(string);
  assert(bytes || (!bytes && !size));

  if (!bytes) {
    pn_string_clear(string);
    return 0;
  }

  int err = string_set(string, bytes, size);
  if (err) return err;

  return 0;
}

// XXX Get rid of this.  Only messenger and sasl.c use it.  The sasl.c
// instance can be changed.
char *pn_string_buffer(pn_string_t *string) {
  assert(string);
  return string->bytes;
}

// XXX Only messenger uses this
size_t pn_string_capacity(pn_string_t *string) {
  assert(string);
  return string->capacity;
}

// XXX Only messenger uses this
int pn_string_resize(pn_string_t *string, size_t size)
{
  assert(string);

  if (size + 1 > string->capacity) {
    int err = string_grow(string, size + 1);
    if (err) return err;
  }

  if (size > string->size) {
    memset(string->bytes + string->size, 0, size - string->size);
  }

  string->bytes[size] = '\0';
  string->size = size;
  string->is_set = (size);

  return 0;
}

int pn_string_vformat(pn_string_t *string, const char *format, va_list ap)
{
  assert(string);

  va_list ap_copy;
  va_copy(ap_copy, ap);

  int len = vsnprintf(NULL, 0, format, ap_copy);

  va_end(ap_copy);

  if (len < 0) return PN_ERR;

  if ((size_t) len + 1 > string->capacity) {
    int err = string_grow(string, len + 1);
    if (err) return err;
  }

  vsnprintf(string->bytes, string->capacity, format, ap);

  string->size = len;


  return 0;
}

// XXX Only messenger uses this
int pn_string_format(pn_string_t *string, PN_PRINTF_FORMAT const char *format, ...)
{
  assert(string);

  va_list ap;
  va_start(ap, format);

  int err = pn_string_vformat(string, format, ap);

  va_end(ap);

  return err;
}

int pn_string_vaddf(pn_string_t *string, const char *format, va_list ap)
{
  assert(string);

  va_list ap_copy;
  va_copy(ap_copy, ap);

  int len = vsnprintf(NULL, 0, format, ap_copy);

  va_end(ap_copy);

  if (len < 0) return PN_ERR;

  if (string->size + len + 1 > string->capacity) {
    int err = string_grow(string, string->size + len + 1);
    if (err) return err;
  }

  vsnprintf(string->bytes + string->size, string->capacity - string->size, format, ap);

  string->size += len;
  string->is_set = true;

  return 0;
}

// XXX Only messenger, url, and sasl use this
int pn_string_addf(pn_string_t *string, PN_PRINTF_FORMAT const char *format, ...)
{
  assert(string);

  va_list ap;
  va_start(ap, format);

  int err = pn_string_vaddf(string, format, ap);

  va_end(ap);

  return err;
}
