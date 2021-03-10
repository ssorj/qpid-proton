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

#include <assert.h>

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

// XXX Try to get rid of this
pni_node_t *pn_data_node(pn_data_t *data, pni_nid_t node_id);

pn_data_t* pni_data(size_t capacity, bool intern);

size_t pni_data_size(pn_data_t *data);
void pni_data_clear(pn_data_t *data);

pn_type_t pni_data_type(pn_data_t *data);
pni_node_t *pni_data_node(pn_data_t *data, pni_nid_t node_id);

bool pni_data_enter(pn_data_t *data);
bool pni_data_exit(pn_data_t *data);
bool pni_data_next(pn_data_t *data);

void pni_data_rewind(pn_data_t *data);
void pni_data_narrow(pn_data_t *data);
void pni_data_widen(pn_data_t *data);

int pni_data_put_described(pn_data_t *data);
int pni_data_put_compound(pn_data_t *data, pn_type_t type);
int pni_data_put_variable(pn_data_t *data, pn_bytes_t bytes, pn_type_t type);

int pni_data_put_null(pn_data_t *data);
int pni_data_put_bool(pn_data_t *data, bool b);
int pni_data_put_ubyte(pn_data_t *data, uint8_t ub);
int pni_data_put_uint(pn_data_t *data, uint32_t ui);
int pni_data_put_ulong(pn_data_t *data, uint64_t ul);
int pni_data_put_timestamp(pn_data_t *data, pn_timestamp_t t);
int pni_data_put_atom(pn_data_t *data, pn_atom_t atom);

int pni_data_appendn(pn_data_t *data, pn_data_t *src, int limit);
int pni_data_copy(pn_data_t *data, pn_data_t *src);

void pni_data_set_array_type(pn_data_t *data, pn_type_t type);

int pni_data_traverse(pn_data_t *data,
                      int (*enter)(void *ctx, pn_data_t *data, pni_node_t *node),
                      int (*exit)(void *ctx, pn_data_t *data, pni_node_t *node),
                      void *ctx);

PN_FORCE_INLINE static bool pni_data_first_field(pn_data_t *data, int *err, pn_type_t type, const char *name)
{
  assert(!data->current);

  if (pn_data_next(data)) {
    pni_node_t *next = pni_data_node(data, data->current);
    assert(next);

    pn_type_t next_type = next->atom.type;

    if (next_type == type) {
      return true;
    } else if (next_type != PN_NULL) {
      *err = pn_error_format(pn_data_error(data), PN_ERR, "data error: %s: expected %s and got %s",
                             name, pn_type_name(type), pn_type_name(next_type));
    }
  }

  return false;
}

PN_FORCE_INLINE static bool pni_data_next_field(pn_data_t *data, int *err, pn_type_t type, const char *name)
{
  assert(data->current);

  pni_node_t *current = pni_data_node(data, data->current);
  assert(current);

  if (current->next) {
    data->current = current->next;
    pni_node_t *next = pni_data_node(data, data->current);
    assert(next);

    pn_type_t next_type = next->atom.type;

    if (next_type == type) {
      return true;
    } else if (next_type != PN_NULL) {
      *err = pn_error_format(pn_data_error(data), PN_ERR, "data error: %s: expected %s and got %s",
                             name, pn_type_name(type), pn_type_name(next_type));
    }
  }

  return false;
}

PN_FORCE_INLINE static void pni_data_require_first_field(pn_data_t* data, int* err, pn_type_t type, const char* name)
{
  bool found = pni_data_first_field(data, err, type, name);

  if (*err) return;

  if (!found) {
    *err = pn_error_format(pn_data_error(data), PN_ERR, "data error: %s: required node not found", name);
  }
}

PN_FORCE_INLINE static void pni_data_require_next_field(pn_data_t* data, int* err, pn_type_t type, const char* name)
{
  bool found = pni_data_next_field(data, err, type, name);

  if (*err) return;

  if (!found) {
    *err = pn_error_format(pn_data_error(data), PN_ERR, "data error: %s: required node not found", name);
  }
}

static inline uint8_t pni_data_get_ubyte(pn_data_t *data)
{
  pni_node_t *node = pni_data_node(data, data->current);

  if (node->atom.type == PN_NULL) return 0;

  assert(node->atom.type == PN_UBYTE);

  return node->atom.u.as_ubyte;
}

static inline uint32_t pni_data_get_uint(pn_data_t *data)
{
  pni_node_t *node = pni_data_node(data, data->current);

  if (node->atom.type == PN_NULL) return 0;

  assert(node->atom.type == PN_UINT);

  return node->atom.u.as_uint;
}

static inline uint64_t pni_data_get_ulong(pn_data_t *data)
{
  pni_node_t *node = pni_data_node(data, data->current);

  if (node->atom.type == PN_NULL) return 0;

  assert(node->atom.type == PN_ULONG);

  return node->atom.u.as_ulong;
}

static inline bool pni_data_get_bool(pn_data_t *data)
{
  pni_node_t *node = pni_data_node(data, data->current);

  if (node->atom.type == PN_NULL) return 0;

  assert(node->atom.type == PN_BOOL);

  return node->atom.u.as_bool;
}

PN_FORCE_INLINE static pn_bytes_t pni_data_get_bytes(pn_data_t *data)
{
  pni_node_t *node = pni_data_node(data, data->current);

  if (node->atom.type == PN_NULL) return pn_bytes_null;

  return node->atom.u.as_bytes;
}

#endif /* data.h */
