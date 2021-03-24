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
#include "config.h"
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
  // for arrays
  bool described;
  bool data;
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
};

void pni_data_enter(pn_data_t *data);
void pni_data_exit(pn_data_t *data);
bool pni_data_next(pn_data_t *data);
void pni_data_rewind(pn_data_t *data);

pni_node_t *pni_data_add_node(pn_data_t *data);
int pni_data_intern_node(pn_data_t *data, pni_node_t *node);

void pni_node_set_type(pni_node_t *node, pn_type_t type);
void pni_node_set_bytes(pni_node_t *node, pn_type_t type, pn_bytes_t bytes);

void pni_node_set_bool(pni_node_t *node, bool value);
void pni_node_set_byte(pni_node_t *node, int8_t value);
void pni_node_set_char(pni_node_t *node, pn_char_t value);
void pni_node_set_decimal32(pni_node_t *node, pn_decimal32_t value);
void pni_node_set_decimal64(pni_node_t *node, pn_decimal64_t value);
void pni_node_set_decimal128(pni_node_t *node, pn_decimal128_t value);
void pni_node_set_double(pni_node_t *node, double value);
void pni_node_set_float(pni_node_t *node, float value);
void pni_node_set_int(pni_node_t *node, int32_t value);
void pni_node_set_long(pni_node_t *node, int64_t value);
void pni_node_set_short(pni_node_t *node, int16_t value);
void pni_node_set_timestamp(pni_node_t *node, pn_timestamp_t value);
void pni_node_set_ubyte(pni_node_t *node, uint8_t value);
void pni_node_set_uint(pni_node_t *node, uint32_t value);
void pni_node_set_ulong(pni_node_t *node, uint64_t value);
void pni_node_set_ushort(pni_node_t *node, uint16_t value);
void pni_node_set_uuid(pni_node_t *node, pn_uuid_t value);

void pni_data_set_array_type(pn_data_t *data, pn_type_t type);

int pni_data_traverse(pn_data_t *data,
                      int (*enter)(void *ctx, pn_data_t *data, pni_node_t *node),
                      int (*exit)(void *ctx, pn_data_t *data, pni_node_t *node),
                      void *ctx);

static inline pni_node_t *pni_data_node(pn_data_t *data, pni_nid_t node_id)
{
  assert(data);
  assert(node_id);

  pni_node_t *node = data->nodes + node_id - 1;

  assert(node);
  assert(node >= data->nodes && node <= data->nodes + data->size);

  return node;
}

#endif /* data.h */
