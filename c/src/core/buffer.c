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

#include <assert.h>
#ifndef __cplusplus
#include <stdbool.h>
#endif
#include <stddef.h>
#include <string.h>
#include <stdio.h>

#include "buffer.h"
#include "memory.h"
#include "util.h"

PN_STRUCT_CLASSDEF(pn_buffer)

int pn_buffer_ensure(pn_buffer_t *buffer, size_t size)
{
  size_t required = buffer->size + size;

  if (buffer->start > 0 && (buffer->start + required > buffer->capacity)) {
    memmove(buffer->bytes, buffer->bytes + buffer->start, buffer->size);
    buffer->start = 0;
  }

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

  char *new_bytes = (char *) pni_mem_subreallocate(PN_CLASSCLASS(pn_buffer), buffer, buffer->bytes, new_capacity);
  if (!new_bytes) return PN_OUT_OF_MEMORY;

  buffer->bytes = new_bytes;
  buffer->capacity = new_capacity;

  return 0;
}

pn_buffer_t *pn_buffer(size_t capacity)
{
  pn_buffer_t *buffer = (pn_buffer_t *) pni_mem_zallocate(PN_CLASSCLASS(pn_buffer), sizeof(pn_buffer_t));
  if (!buffer) return NULL;

  if (capacity > 0) {
    int err = pn_buffer_ensure(buffer, capacity);

    if (err) {
      pni_mem_deallocate(PN_CLASSCLASS(pn_buffer), buffer);
      return NULL;
    }
  }

  return buffer;
}

void pn_buffer_free(pn_buffer_t *buffer)
{
  if (!buffer) return;

  pni_mem_subdeallocate(PN_CLASSCLASS(pn_buffer), buffer, buffer->bytes);
  pni_mem_deallocate(PN_CLASSCLASS(pn_buffer), buffer);
}
