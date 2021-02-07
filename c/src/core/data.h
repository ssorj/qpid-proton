#ifndef _PROTON_DATA_H
#define _PROTON_DATA_H 1

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

#include <proton/codec.h>
#include "buffer.h"
#include "decoder.h"
#include "encoder.h"

typedef uint16_t pni_nid_t;
#define PNI_NID_MAX ((pni_nid_t)-1)
#define PNI_INTERN_MINSIZE 64

typedef struct {
  char *start;
  size_t data_offset;
  size_t data_size;
  pn_atom_t atom;
  pn_type_t type;
  pni_nid_t next;
  pni_nid_t prev;
  pni_nid_t down;
  pni_nid_t parent;
  pni_nid_t children;
  // for arrays
  bool described;
  bool data;
  bool small;
} pni_node_t;

struct pn_data_t {
  pni_node_t *nodes;
  pn_buffer_t *buf;
  pn_error_t *error;
  pni_nid_t capacity;
  pni_nid_t size;
  pni_nid_t parent;
  pni_nid_t current;
  pni_nid_t base_parent;
  pni_nid_t base_current;
  bool intern;
};

__attribute__((always_inline))
static inline pni_node_t * pn_data_node(pn_data_t *data, pni_nid_t nd)
{
  return nd ? (data->nodes + nd - 1) : NULL;
}

size_t pn_data_siblings(pn_data_t *data);

pn_data_t* pni_data(size_t capacity, bool intern);

int pni_data_traverse(pn_data_t *data,
                      int (*enter)(void *ctx, pn_data_t *data, pni_node_t *node),
                      int (*exit)(void *ctx, pn_data_t *data, pni_node_t *node),
                      void *ctx);

__attribute__((always_inline))
static inline bool pni_data_next_field(pn_data_t* data, int* err, pn_type_t type, const char* name)
{
  if (!pn_data_next(data) || pn_data_type(data) == PN_NULL) {
    return false;
  } else if (pn_data_type(data) == type) {
    return true;
  } else {
    *err = pn_error_format(pn_data_error(data), PN_ERR, "data error: %s: expected %s and got %s",
                           name, pn_type_name(type), pn_type_name(pn_data_type(data)));
    return false;
  }
}

__attribute__((always_inline))
static inline void pni_data_require_field(pn_data_t* data, int* err, pn_type_t type, const char* name)
{
  bool found = pni_data_next_field(data, err, type, name);

  if (*err) return;

  if (!found) {
    *err = pn_error_format(pn_data_error(data), PN_ERR, "data error: %s: required node not found", name);
  }
}

__attribute__((always_inline))
static inline pn_type_t pni_data_parent_type(pn_data_t *data)
{
  pni_node_t *node = pn_data_node(data, data->parent);
  if (node) {
    return node->atom.type;
  } else {
    return PN_INVALID;
  }
}

#endif /* data.h */
