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
int pni_data_put_long(pn_data_t *data, int64_t l);
int pni_data_put_ulong(pn_data_t *data, uint64_t ul);
int pni_data_put_timestamp(pn_data_t *data, pn_timestamp_t t);
int pni_data_put_atom(pn_data_t *data, pn_atom_t atom);

int pni_data_appendn(pn_data_t *data, pn_data_t *src, int limit);
int pni_data_copy(pn_data_t *data, pn_data_t *src);

void pni_data_set_array_type(pn_data_t *data, pn_type_t type);

ssize_t pni_data_decode(pn_data_t *data, const char *bytes, size_t size);

int pni_data_traverse(pn_data_t *data,
                      int (*enter)(void *ctx, pn_data_t *data, pni_node_t *node),
                      int (*exit)(void *ctx, pn_data_t *data, pni_node_t *node),
                      void *ctx);

static inline pni_node_t *pni_data_first_node(pn_data_t *data) {
  assert(!data->current);

  if (data->parent) {
    pni_node_t *parent = pni_data_node(data, data->parent);

    assert(parent->down);

    data->current = parent->down;
    return pni_data_node(data, data->current);
  } else if (data->size) {
    data->current = 1;
    return pni_data_node(data, data->current);
  }

  return NULL;
}

static inline pni_node_t *pni_data_next_node(pn_data_t *data) {
  pni_node_t *current = pni_data_node(data, data->current);

  if (current->next) {
    data->current = current->next;
    return pni_data_node(data, data->current);
  }

  return NULL;
}

static inline pni_node_t *pni_data_first_field(pn_data_t *data, pn_type_t type, int *err)
{
  pni_node_t *node = pni_data_first_node(data);

  if (node) {
    pn_type_t node_type = node->atom.type;

    if (node_type == type) {
      return node;
    } else if (node_type != PN_NULL) {
      *err = pn_error_format(pn_data_error(data), PN_ERR, "mismatched types! AAA");
    }
  }

  return NULL;
}

static inline pni_node_t *pni_data_next_field(pn_data_t *data, pn_type_t type, int *err)
{
  pni_node_t *node = pni_data_next_node(data);

  if (node) {
    pn_type_t node_type = node->atom.type;

    if (node_type == type) {
      return node;
    } else if (node_type != PN_NULL) {
      *err = pn_error_format(pn_data_error(data), PN_ERR, "mismatched types! AAA");
    }
  }

  return NULL;
}

static inline pni_node_t *pni_data_require_first_field(pn_data_t* data, pn_type_t type, int *err)
{
  pni_node_t *node = pni_data_first_field(data, type, err);

  if (node) {
    return node;
  } else if (!(*err)) {
    *err = pn_error_format(pn_data_error(data), PN_ERR, "mismatched types! BBB");
  }

  return NULL;
}

static inline pni_node_t *pni_data_require_next_field(pn_data_t* data, pn_type_t type, int *err)
{
  pni_node_t *node = pni_data_next_field(data, type, err);

  if (node) {
    return node;
  } else if (!(*err)) {
    *err = pn_error_format(pn_data_error(data), PN_ERR, "mismatched types! CCC");
  }

  return NULL;
}

static inline uint8_t pni_node_get_ubyte(pni_node_t *node)
{
  if (node->atom.type == PN_NULL) return 0;

  assert(node->atom.type == PN_UBYTE);

  return node->atom.u.as_ubyte;
}

static inline uint8_t pni_data_get_ubyte(pn_data_t *data)
{
  pni_node_t *node = pni_data_node(data, data->current);
  return pni_node_get_ubyte(node);
}

static inline uint32_t pni_node_get_uint(pni_node_t *node)
{
  if (node->atom.type == PN_NULL) return 0;

  assert(node->atom.type == PN_UINT);

  return node->atom.u.as_uint;
}

static inline uint32_t pni_data_get_uint(pn_data_t *data)
{
  pni_node_t *node = pni_data_node(data, data->current);
  return pni_node_get_uint(node);
}

static inline uint64_t pni_node_get_ulong(pni_node_t *node)
{
  if (node->atom.type == PN_NULL) return 0;

  assert(node->atom.type == PN_ULONG);

  return node->atom.u.as_ulong;
}

static inline uint64_t pni_data_get_ulong(pn_data_t *data)
{
  pni_node_t *node = pni_data_node(data, data->current);
  return pni_node_get_ulong(node);
}

static inline bool pni_node_get_bool(pni_node_t *node)
{
  if (node->atom.type == PN_NULL) return false;

  assert(node->atom.type == PN_BOOL);

  return node->atom.u.as_bool;
}

static inline bool pni_data_get_bool(pn_data_t *data)
{
  pni_node_t *node = pni_data_node(data, data->current);
  return pni_node_get_bool(node);
}

static inline pn_bytes_t pni_node_get_bytes(pni_node_t *node)
{
  if (node->atom.type == PN_NULL) return pn_bytes_null;

  // XXX assert(node->atom.type == <bytes types>);

  return node->atom.u.as_bytes;
}

static inline pn_bytes_t pni_data_get_bytes(pn_data_t *data)
{
  pni_node_t *node = pni_data_node(data, data->current);
  return pni_node_get_bytes(node);
}

#endif /* data.h */
