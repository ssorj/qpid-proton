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

#include <stdlib.h>
#include <assert.h>

#define pn_object_initialize NULL
#define pn_object_finalize NULL
#define pn_object_inspect NULL
#define pn_object_hashcode NULL
#define pn_object_compare NULL

const pn_class_t PN_OBJECT[] = {PN_CLASS(pn_object)};

void *pn_void_new(const pn_class_t *clazz, size_t size) { return pni_mem_allocate(clazz, size); }
#define pn_void_initialize NULL
#define pn_void_finalize NULL
static void pn_void_free(void *object) { pni_mem_deallocate(PN_VOID, object); }

void pn_void_incref(void* p) {}
void pn_void_decref(void* p) {}
int pn_void_refcount(void *object) { return -1; }

#define pn_void_hashcode NULL
#define pn_void_compare NULL
#define pn_void_inspect NULL

const pn_class_t PN_VOID[] = {PN_METACLASS(pn_void)};

typedef struct {
  const pn_class_t *clazz;
  int refcount;
} pni_head_t;

#define pni_head(PTR) \
(((pni_head_t *) (PTR)) - 1)

const char *pn_class_name(const pn_class_t *clazz)
{
  return clazz->name;
}

inline pn_cid_t pn_class_id(const pn_class_t *clazz)
{
  return clazz->cid;
}

static inline void *pni_object_new(const pn_class_t *clazz, size_t size)
{
  void *object = NULL;
  pni_head_t *head = (pni_head_t *) pni_mem_zallocate(clazz, sizeof(pni_head_t) + size);
  if (head != NULL) {
    object = head + 1;
    head->clazz = clazz;
    head->refcount = 1;
  }
  return object;
}

static inline void pni_object_incref(void *object) {
  assert(object);
  pni_head(object)->refcount++;
}

static inline int pni_object_refcount(void *object)
{
  assert(object);
  return pni_head(object)->refcount;
}

static inline void pni_object_decref(void *object)
{
  pni_head_t *head = pni_head(object);
  assert(head->refcount > 0);
  head->refcount--;
}

static inline void pni_object_free(void *object)
{
  pni_head_t *head = pni_head(object);
  pni_mem_deallocate(head->clazz, head);
}

inline void pn_object_incref(void *object) {
  pni_object_incref(object);
}

static inline void *pni_class_new(const pn_class_t *clazz, size_t size) {
  return clazz->newinst
  ? clazz->newinst(clazz, size)
  : pni_object_new(clazz, size);
}

static inline void pni_class_incref(const pn_class_t *clazz, void *object) {
  if (clazz->incref) {
    clazz->incref(object);
  } else {
    pni_object_incref(object);
  }
}

static inline void pni_class_decref(const pn_class_t *clazz, void *object) {
  if (clazz->decref) {
    clazz->decref(object);
  } else {
    pni_object_decref(object);
  }
}

static inline int pni_class_refcount(const pn_class_t *clazz, void *object) {
  return clazz->refcount
  ? clazz->refcount(object)
  : pni_object_refcount(object);
}

static inline void pni_class_free(const pn_class_t *clazz, void *object) {
  if (clazz->free) {
    clazz->free(object);
  } else {
    pni_object_free(object);
  }
}

void *pn_class_new(const pn_class_t *clazz, size_t size)
{
  assert(clazz);
  void *object = pni_class_new(clazz, size);
  if (object && clazz->initialize) {
    clazz->initialize(object);
  }
  return object;
}

inline void *pn_class_incref(const pn_class_t *clazz, void *object)
{
  if (object) {
    if (clazz==PN_OBJECT) {
      clazz = pni_head(object)->clazz;
    }
    pni_class_incref(clazz, object);
  }
  return object;
}

inline int pn_class_refcount(const pn_class_t *clazz, void *object)
{
  if (clazz==PN_OBJECT) {
    clazz = pn_class(object);
  }

  return pni_class_refcount(clazz, object);
}

inline int pn_class_decref(const pn_class_t *clazz, void *object)
{
  if (object) {
    if (clazz==PN_OBJECT) {
      clazz = pni_head(object)->clazz;
    }
    pni_class_decref(clazz, object);
    int rc = pni_class_refcount(clazz, object);
    if (rc == 0) {
      if (clazz->finalize) {
        clazz->finalize(object);
        // check the refcount again in case the finalizer created a
        // new reference
        rc = pni_class_refcount(clazz, object);
      }
      if (rc == 0) {
        pni_class_free(clazz, object);
        return 0;
      }
    } else {
      return rc;
    }
  }

  return 0;
}

void pn_class_free(const pn_class_t *clazz, void *object)
{
  if (object) {
    if (clazz==PN_OBJECT) {
      clazz = pni_head(object)->clazz;
    }

    int rc = pni_class_refcount(clazz, object);
    assert(rc == 1 || rc == -1);
    if (rc == 1) {
      rc = pn_class_decref(clazz, object);
      assert(rc == 0);
    } else {
      if (clazz->finalize) {
        clazz->finalize(object);
      }
      pni_class_free(clazz, object);
    }
  }
}

intptr_t pn_class_compare(const pn_class_t *clazz, void *a, void *b)
{
  if (a == b) return 0;

  if (a && b) {
    if (clazz==PN_OBJECT) {
      clazz = pni_head(a)->clazz;
    }
    if (clazz->compare) {
      return clazz->compare(a, b);
    }
  }
  return (intptr_t) a - (intptr_t) b;
}

bool pn_class_equals(const pn_class_t *clazz, void *a, void *b)
{
  return pn_class_compare(clazz, a, b) == 0;
}

int pn_class_inspect(const pn_class_t *clazz, void *object, pn_string_t *dst)
{
  if (clazz==PN_OBJECT) {
      clazz = pni_head(object)->clazz;
  }

  if (!pn_string_get(dst)) {
    pn_string_set(dst, "");
  }

  if (object && clazz->inspect) {
    return clazz->inspect(object, dst);
  }

  const char *name = clazz->name ? clazz->name : "<anon>";

  return pn_string_addf(dst, "%s<%p>", name, object);
}

inline void *pn_incref(void *object)
{
  if (object) {
    const pn_class_t *clazz = pni_head(object)->clazz;
    pni_class_incref(clazz, object);
  }
  return object;
}

inline int pn_decref(void *object)
{
  if (object) {
    const pn_class_t *clazz = pni_head(object)->clazz;
    pni_class_decref(clazz, object);
    int rc = pni_class_refcount(clazz, object);
    if (rc == 0) {
      if (clazz->finalize) {
        clazz->finalize(object);
        // check the refcount again in case the finalizer created a
        // new reference
        rc = pni_class_refcount(clazz, object);
      }
      if (rc == 0) {
        pni_class_free(clazz, object);
        return 0;
      }
    } else {
      return rc;
    }
  }
  return 0;
}

inline int pn_refcount(void *object)
{
  assert(object);
  const pn_class_t *clazz = pni_head(object)->clazz;
  return pni_class_refcount(clazz, object);
}

void pn_free(void *object)
{
  if (object) {
    const pn_class_t *clazz = pni_head(object)->clazz;
    int rc = pni_class_refcount(clazz, object);
    assert(rc == 1 || rc == -1);
    if (rc == 1) {
      pni_class_decref(clazz, object);
      assert(pni_class_refcount(clazz, object) == 0);
      if (clazz->finalize) {
        clazz->finalize(object);
      }
      if (pni_class_refcount(clazz, object) == 0) {
        pni_class_free(clazz, object);
      }
    } else {
      if (clazz->finalize) {
        clazz->finalize(object);
      }
      pni_class_free(clazz, object);
    }
  }
}

inline const pn_class_t *pn_class(void *object)
{
  if (object) {
    return pni_head(object)->clazz;
  } else {
    return PN_OBJECT;
  }
}

uintptr_t pn_hashcode(void *object)
{
  if (!object) return 0;

  const pn_class_t *clazz = pni_head(object)->clazz;

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
    const pn_class_t *clazz = pni_head(a)->clazz;
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

  const pn_class_t *clazz = pni_head(object)->clazz;

  if (clazz->inspect) {
    return clazz->inspect(object, dst);
  }

  const char *name = clazz->name ? clazz->name : "<anon>";
  return pn_string_addf(dst, "%s<%p>", name, object);
}

#define pn_weakref_new NULL
#define pn_weakref_initialize NULL
#define pn_weakref_finalize NULL
#define pn_weakref_free NULL

#define pn_weakref_incref pn_void_incref
#define pn_weakref_decref pn_void_decref
#define pn_weakref_refcount pn_void_refcount

static uintptr_t pn_weakref_hashcode(void *object) {
  return pn_hashcode(object);
}
static intptr_t pn_weakref_compare(void *a, void *b) {
  return pn_compare(a, b);
}
static int pn_weakref_inspect(void *object, pn_string_t *dst) {
  return pn_inspect(object, dst);
}

const pn_class_t PN_WEAKREF[] = {PN_METACLASS(pn_weakref)};
