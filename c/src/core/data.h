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

#include "buffer2.h"
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
  bool described; // For arrays
  bool data;
  bool small;
} pni_node_t;

struct pn_data_t {
  pni_node_t *nodes;
  pn_error_t *error;
  pni_buffer2_t *intern_buf;
  pni_nid_t capacity;
  pni_nid_t size;
  pni_nid_t parent;
  pni_nid_t current;
  pni_nid_t base_parent;
  pni_nid_t base_current;
  bool intern;
};

size_t pn_data_siblings(pn_data_t *data);
pn_data_t* pni_data(size_t capacity, bool intern);
void pni_data_set_array_type(pn_data_t *data, pn_type_t type);

int pni_data_traverse(pn_data_t *data,
                      int (*enter)(void *ctx, pn_data_t *data, pni_node_t *node),
                      int (*exit)(void *ctx, pn_data_t *data, pni_node_t *node),
                      void *ctx);

int pni_data_put_fixed0(pn_data_t *data, pn_type_t type);
int pni_data_put_fixed8(pn_data_t *data, pn_type_t type, uint8_t value);
int pni_data_put_fixed16(pn_data_t *data, pn_type_t type, uint16_t value);
int pni_data_put_fixed32(pn_data_t *data, pn_type_t type, uint32_t value);
int pni_data_put_fixed64(pn_data_t *data, pn_type_t type, uint64_t value);

static inline pni_node_t *pn_data_node(pn_data_t *data, pni_nid_t node_id)
{
  if (node_id) {
    return data->nodes + node_id - 1;
  } else {
    return NULL;
  }
}

static inline pn_type_t pni_data_parent_type(pn_data_t *data)
{
  pni_node_t *node = pn_data_node(data, data->parent);

  if (node) {
    return node->atom.type;
  } else {
    return PN_INVALID;
  }
}

PN_FORCE_INLINE static bool pni_data_next_field(pn_data_t* data, int* err, pn_type_t type, const char* name)
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

PN_FORCE_INLINE static void pni_data_require_field(pn_data_t* data, int* err, pn_type_t type, const char* name)
{
  bool found = pni_data_next_field(data, err, type, name);

  if (*err) return;

  if (!found) {
    *err = pn_error_format(pn_data_error(data), PN_ERR, "data error: %s: required node not found", name);
  }
}

#endif /* data.h */
