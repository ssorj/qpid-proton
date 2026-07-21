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

#include <proton/object.h>

#include "core/memory.h"
#include "core/fixed_string.h"
#include "core/object_private.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#define CID_pn_default CID_pn_object
#define pn_default_initialize NULL
#define pn_default_finalize NULL
#define pn_default_inspect NULL
#define pn_default_hashcode NULL
#define pn_default_compare NULL

const pn_class_t PN_DEFAULT[] = { PN_CLASS(pn_default) };

void *pn_void_new(const pn_class_t *clazz, size_t size) {
  return pni_mem_allocate(clazz, size);
}

#define pn_void_initialize NULL
#define pn_void_finalize NULL

void pn_void_free(void *object) {
  pni_mem_deallocate(PN_VOID, object);
}

void pn_void_incref(void* p) {}
void pn_void_decref(void* p) {}
int pn_void_refcount(void *object) { return -1; }

#define pn_void_hashcode NULL
#define pn_void_compare NULL
#define pn_void_inspect NULL

static const pn_class_t PN_VOID_S = PN_METACLASS(pn_void);
const pn_class_t *PN_VOID = &PN_VOID_S;

#define pn_weakref_new NULL
#define pn_weakref_initialize NULL
#define pn_weakref_finalize NULL
#define pn_weakref_free NULL

#define pn_weakref_incref pn_void_incref
#define pn_weakref_decref pn_void_decref
#define pn_weakref_refcount pn_void_refcount

#define pn_weakref_hashcode pn_hashcode
#define pn_weakref_compare pn_compare
#define pn_weakref_inspect pn_finspect

const pn_class_t PN_WEAKREF[] = { PN_METACLASS(pn_weakref) };

#define CID_pn_strongref CID_pn_object
#define pn_strongref_new NULL
#define pn_strongref_initialize NULL

void pn_strongref_finalize(void *object) {
  const pn_class_t *clazz = object_header(object)->clazz;

  if (clazz->finalize) {
    clazz->finalize(object);
  }
}

#define pn_strongref_free NULL

#define pn_strongref_incref NULL
#define pn_strongref_decref NULL
#define pn_strongref_refcount NULL

#define pn_strongref_hashcode pn_hashcode
#define pn_strongref_compare pn_compare
#define pn_strongref_inspect pn_finspect

static const pn_class_t PN_OBJECT_S = PN_METACLASS(pn_strongref);
const pn_class_t *PN_OBJECT = &PN_OBJECT_S;

//
// Common implementation functions
//

void pn_object_deallocate(object_header_t *header)
{
  pni_mem_deallocate(header->clazz, header);
}

static inline void class_free(const pn_class_t *clazz, void *object)
{
  assert(clazz);
  assert(object);

  if (clazz->free) {
    clazz->free(object);
    return;
  }

  object_header_t *header = object_header(object);

  assert(header->clazz);
  assert(header->clazz == clazz || clazz == PN_WEAKREF || clazz == PN_OBJECT);
  assert(header->refcount == 1);

  if (header->refcount == 1) {
    class_decref(clazz, object);
  }
}

//
// The class API
//

pn_class_t *pn_class_create(const char *name,
                            void (*initialize)(void*),
                            void (*finalize)(void*),
                            void (*incref)(void*),
                            void (*decref)(void*),
                            int (*refcount)(void*))
{
  pn_class_t *clazz = malloc(sizeof(pn_class_t));
  if (!clazz) return NULL;

  *clazz = (pn_class_t) {
    .name = name,
    .cid = CID_pn_object,
    .initialize = initialize,
    .finalize = finalize,
    .incref = incref,
    .decref = decref,
    .refcount = refcount
  };

  return clazz;
}

const char *pn_class_name(const pn_class_t *clazz)
{
  assert(clazz);
  return clazz->name;
}

pn_cid_t pn_class_id(const pn_class_t *clazz)
{
  assert(clazz);
  return clazz->cid;
}

// XXX Rename to new_instance or _object - or _alloc?
void *pn_class_new(const pn_class_t *clazz, size_t size)
{
  assert(clazz);
  assert(!clazz->refcount || clazz->refcount == pn_void_refcount);
  assert(!(clazz->finalize && clazz->free));

  void *object = NULL;

  if (clazz->newinst) {
    object = clazz->newinst(clazz, size);
    if (!object) return NULL;
  } else {
    object_header_t *header = (object_header_t *) pni_mem_zallocate(clazz, sizeof(object_header_t) + size);
    if (!header) return NULL;

    *header = (object_header_t) {
      .clazz = clazz,
      .refcount = 1,
    };

    object = header + 1;
  }

  if (clazz->initialize) {
    clazz->initialize(object);
  }

  return object;
}

void pn_class_free(const pn_class_t *clazz, void *object)
{
  class_free(clazz, object);
}

intptr_t pn_class_compare(const pn_class_t *clazz, void *a, void *b)
{
  assert(clazz);

  if (a == b) return 0;

  if (a && b) {
    if (clazz->compare) {
      return clazz->compare(a, b);
    }
  }

  return (intptr_t) a - (intptr_t) b;
}

bool pn_class_equals(const pn_class_t *clazz, void *a, void *b)
{
  assert(clazz);
  return pn_class_compare(clazz, a, b) == 0;
}

void pn_class_inspect(const pn_class_t *clazz, void *object, pn_fixed_string_t *dst)
{
  assert(clazz);

  if (object && clazz->inspect) {
    clazz->inspect(object, dst);
    return;
  }

  const char *name = clazz->name ? clazz->name : "<anon>";

  pn_fixed_string_addf(dst, "%s<%p>", name, object);

  return;
}

//
// The existing object API
//

void *pn_incref(void *object)
{
  if (!object) return NULL;

  class_incref(object_header(object)->clazz, object);

  return object;
}

int pn_decref(void *object)
{
  if (!object) return 0;

  class_decref(object_header(object)->clazz, object);

  return 0;
}

int pn_refcount(void *object)
{
  assert(object);
  return object_header(object)->refcount;
}

void pn_free(void *object)
{
  if (object) class_free(object_header(object)->clazz, object);
}

const pn_class_t *pn_class(void *object)
{
  if (object) {
    return object_header(object)->clazz;
  } else {
    return PN_DEFAULT;
  }
}

uintptr_t pn_hashcode(void *object)
{
  if (!object) return 0;

  const pn_class_t *clazz = object_header(object)->clazz;

  if (clazz->hashcode) {
    return clazz->hashcode(object);
  } else {
    return (uintptr_t) object;
  }
}

intptr_t pn_compare(void *a, void *b)
{
  if (a == b) return 0;

  if (a && b) {
    const pn_class_t *clazz = object_header(a)->clazz;

    if (clazz->compare) {
      return clazz->compare(a, b);
    }
  }
  return (intptr_t) a - (intptr_t) b;
}

bool pn_equals(void *a, void *b)
{
  return !pn_compare(a, b);
}

int pn_inspect(void *object, pn_string_t *dst)
{
  if (!pn_string_get(dst)) {
    pn_string_set(dst, "");
  }

  if (!object) {
    return pn_string_addf(dst, "pn_object<%p>", object);
  }

  const pn_class_t *clazz = object_header(object)->clazz;

  if (clazz->inspect) {
    char buf[1024];
    pn_fixed_string_t str = pn_fixed_string(buf, sizeof(buf));
    clazz->inspect(object, &str);
    return pn_string_setn(dst, buf, str.position);
  }

  const char *name = clazz->name ? clazz->name : "<anon>";

  return pn_string_addf(dst, "%s<%p>", name, object);
}

void pn_finspect(void *object, pn_fixed_string_t *dst)
{
  if (!object) {
    pn_fixed_string_addf(dst, "pn_object<%p>", object);
    return;
  }

  const pn_class_t *clazz = object_header(object)->clazz;

  if (clazz->inspect) {
    clazz->inspect(object, dst);
    return;
  }

  const char *name = clazz->name ? clazz->name : "<anon>";
  pn_fixed_string_addf(dst, "%s<%p>", name, object);

  return;
}

char *pn_tostring(void *object)
{
  char buf[1024];
  pn_fixed_string_t s = pn_fixed_string(buf, sizeof(buf));
  pn_finspect(object, &s);
  pn_fixed_string_terminate(&s);

  int l = s.position+1; // include final null
  char *r = malloc(l);
  strncpy(r, buf, l);
  return r;
}
