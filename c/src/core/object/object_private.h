#ifndef OBJECT_PRIVATE_H
#define OBJECT_PRIVATE_H

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

#include <proton/object.h>

#include <assert.h>

#if __cplusplus
extern "C" {
#endif

static inline void pni_class_incref(const pn_class_t *clazz, void *object)
{
  assert(clazz);
  assert(object);

  clazz->incref(object);
}

static inline int pni_class_refcount(const pn_class_t *clazz, void *object)
{
  assert(clazz);
  assert(object);

  return clazz->refcount(object);
}

static inline int pni_class_decref(const pn_class_t *clazz, void *object)
{
  assert(clazz);
  assert(object);

  clazz->decref(object);

  int rc = clazz->refcount(object);

  if (rc == 0) {
    if (clazz->finalize) {
      clazz->finalize(object);
      // check the refcount again in case the finalizer created a
      // new reference
      rc = clazz->refcount(object);
    }

    if (rc == 0) {
      clazz->free(object);
    }
  }

  return rc;
}

#if __cplusplus
}
#endif

#endif
