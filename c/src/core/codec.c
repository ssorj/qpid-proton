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
#include <proton/codec.h>
#include <proton/error.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <stddef.h>
#include <ctype.h>
#include "encodings.h"
#define DEFINE_FIELDS
#include "protocol.h"
#include "platform/platform_fmt.h"
#include "util.h"
#include "decoder.h"
#include "encoder.h"
#include "data.h"
#include "logger_private.h"
#include "memory.h"

const char *pn_type_name(pn_type_t type)
{
  switch (type) {
  case PN_NULL: return "PN_NULL";
  case PN_BOOL: return "PN_BOOL";
  case PN_UBYTE: return "PN_UBYTE";
  case PN_BYTE: return "PN_BYTE";
  case PN_USHORT: return "PN_USHORT";
  case PN_SHORT: return "PN_SHORT";
  case PN_UINT: return "PN_UINT";
  case PN_INT: return "PN_INT";
  case PN_CHAR: return "PN_CHAR";
  case PN_ULONG: return "PN_ULONG";
  case PN_LONG: return "PN_LONG";
  case PN_TIMESTAMP: return "PN_TIMESTAMP";
  case PN_FLOAT: return "PN_FLOAT";
  case PN_DOUBLE: return "PN_DOUBLE";
  case PN_DECIMAL32: return "PN_DECIMAL32";
  case PN_DECIMAL64: return "PN_DECIMAL64";
  case PN_DECIMAL128: return "PN_DECIMAL128";
  case PN_UUID: return "PN_UUID";
  case PN_BINARY: return "PN_BINARY";
  case PN_STRING: return "PN_STRING";
  case PN_SYMBOL: return "PN_SYMBOL";
  case PN_DESCRIBED: return "PN_DESCRIBED";
  case PN_ARRAY: return "PN_ARRAY";
  case PN_LIST: return "PN_LIST";
  case PN_MAP: return "PN_MAP";
  default: return "<UNKNOWN>";
  }
}

static inline pn_type_t pni_node_get_type(pni_node_t *node)
{
  return node->atom.type;
}

static inline pn_bytes_t pni_node_get_bytes(pni_node_t *node)
{
  return node->atom.u.as_bytes;
}

static inline bool pni_node_get_bool(pni_node_t *node)
{
  return node->atom.u.as_bool;
}

static inline uint8_t pni_node_get_ubyte(pni_node_t *node)
{
  return node->atom.u.as_ubyte;
}

static inline int8_t pni_node_get_byte(pni_node_t *node)
{
  return node->atom.u.as_byte;
}

static inline uint16_t pni_node_get_ushort(pni_node_t *node)
{
  return node->atom.u.as_ushort;
}

static inline int16_t pni_node_get_short(pni_node_t *node)
{
  return node->atom.u.as_short;
}

static inline uint32_t pni_node_get_uint(pni_node_t *node)
{
  return node->atom.u.as_uint;
}

static inline int32_t pni_node_get_int(pni_node_t *node)
{
  return node->atom.u.as_int;
}

static inline float pni_node_get_float(pni_node_t *node)
{
  return node->atom.u.as_float;
}

static inline double pni_node_get_double(pni_node_t *node)
{
  return node->atom.u.as_double;
}

static inline pn_char_t pni_node_get_char(pni_node_t *node)
{
  return node->atom.u.as_char;
}

static inline pn_decimal32_t pni_node_get_decimal32(pni_node_t *node)
{
  return node->atom.u.as_decimal32;
}

static inline pn_decimal64_t pni_node_get_decimal64(pni_node_t *node)
{
  return node->atom.u.as_decimal64;
}

static inline pn_decimal128_t pni_node_get_decimal128(pni_node_t *node)
{
  return node->atom.u.as_decimal128;
}

static inline uint64_t pni_node_get_ulong(pni_node_t *node)
{
  return node->atom.u.as_ulong;
}

static inline int64_t pni_node_get_long(pni_node_t *node)
{
  return node->atom.u.as_long;
}

static inline pn_uuid_t pni_node_get_uuid(pni_node_t *node)
{
  return node->atom.u.as_uuid;
}

static inline pn_timestamp_t pni_node_get_timestamp(pni_node_t *node)
{
  return node->atom.u.as_timestamp;
}

PNI_INLINE void pni_node_set_type(pni_node_t *node, pn_type_t type)
{
  node->atom.type = type;
}

PNI_INLINE void pni_node_set_bytes(pni_node_t *node, pn_type_t type, pn_bytes_t bytes)
{
  node->atom.type = type;
  node->atom.u.as_bytes = bytes;
}

PNI_INLINE void pni_node_set_bool(pni_node_t *node, bool value)
{
  node->atom.type = PN_BOOL;
  node->atom.u.as_bool = value;
}

PNI_INLINE void pni_node_set_ubyte(pni_node_t *node, uint8_t value)
{
  node->atom.type = PN_UBYTE;
  node->atom.u.as_ubyte = value;
}

PNI_INLINE void pni_node_set_byte(pni_node_t *node, int8_t value)
{
  node->atom.type = PN_BYTE;
  node->atom.u.as_byte = value;
}

PNI_INLINE void pni_node_set_ushort(pni_node_t *node, uint16_t value)
{
  node->atom.type = PN_USHORT;
  node->atom.u.as_ushort = value;
}

PNI_INLINE void pni_node_set_short(pni_node_t *node, int16_t value)
{
  node->atom.type = PN_SHORT;
  node->atom.u.as_short = value;
}

PNI_INLINE void pni_node_set_uint(pni_node_t *node, uint32_t value)
{
  node->atom.type = PN_UINT;
  node->atom.u.as_uint = value;
}

PNI_INLINE void pni_node_set_int(pni_node_t *node, int32_t value)
{
  node->atom.type = PN_INT;
  node->atom.u.as_int = value;
}

PNI_INLINE void pni_node_set_float(pni_node_t *node, float value)
{
  node->atom.type = PN_FLOAT;
  node->atom.u.as_float = value;
}

PNI_INLINE void pni_node_set_double(pni_node_t *node, double value)
{
  node->atom.type = PN_DOUBLE;
  node->atom.u.as_double = value;
}

PNI_INLINE void pni_node_set_char(pni_node_t *node, pn_char_t value)
{
  node->atom.type = PN_CHAR;
  node->atom.u.as_char = value;
}

PNI_INLINE void pni_node_set_decimal32(pni_node_t *node, pn_decimal32_t value)
{
  node->atom.type = PN_DECIMAL32;
  node->atom.u.as_decimal32 = value;
}

PNI_INLINE void pni_node_set_decimal64(pni_node_t *node, pn_decimal64_t value)
{
  node->atom.type = PN_DECIMAL64;
  node->atom.u.as_decimal64 = value;
}

PNI_INLINE void pni_node_set_decimal128(pni_node_t *node, pn_decimal128_t value)
{
  node->atom.type = PN_DECIMAL128;
  memcpy(node->atom.u.as_decimal128.bytes, value.bytes, 16);
}

PNI_INLINE void pni_node_set_ulong(pni_node_t *node, uint64_t value)
{
  node->atom.type = PN_ULONG;
  node->atom.u.as_ulong = value;
}

PNI_INLINE void pni_node_set_long(pni_node_t *node, int64_t value)
{
  node->atom.type = PN_LONG;
  node->atom.u.as_long = value;
}

PNI_INLINE void pni_node_set_uuid(pni_node_t *node, pn_uuid_t value)
{
  node->atom.type = PN_UUID;
  memcpy(node->atom.u.as_uuid.bytes, value.bytes, 16);
}

PNI_INLINE void pni_node_set_timestamp(pni_node_t *node, pn_timestamp_t value)
{
  node->atom.type = PN_TIMESTAMP;
  node->atom.u.as_timestamp = value;
}

// data

static inline pni_node_t *pn_data_node(pn_data_t *data, pni_nid_t node_id)
{
  if (node_id) {
    return pni_data_node(data, node_id);
  } else {
    return NULL;
  }
}

static void pn_data_finalize(void *object)
{
  pn_data_t *data = (pn_data_t *) object;
  pni_mem_subdeallocate(pn_class(data), data, data->nodes);
  pn_buffer_free(data->buf);
  pn_error_free(data->error);
}

static const pn_fields_t *pni_node_fields(pn_data_t *data, pni_node_t *node)
{
  if (!node) return NULL;
  if (node->atom.type != PN_DESCRIBED) return NULL;

  pni_node_t *descriptor = pn_data_node(data, node->down);

  if (!descriptor || descriptor->atom.type != PN_ULONG) {
    return NULL;
  }

  if (descriptor->atom.u.as_ulong >= FIELD_MIN && descriptor->atom.u.as_ulong <= FIELD_MAX) {
    const pn_fields_t *f = &FIELDS[descriptor->atom.u.as_ulong-FIELD_MIN];
    return (f->name_index!=0) ? f : NULL;
  } else {
    return NULL;
  }
}

int pni_inspect_atom(pn_atom_t *atom, pn_string_t *str)
{
  switch (atom->type) {
  case PN_NULL:
    return pn_string_addf(str, "null");
  case PN_BOOL:
    return pn_string_addf(str, atom->u.as_bool ? "true" : "false");
  case PN_UBYTE:
    return pn_string_addf(str, "%" PRIu8, atom->u.as_ubyte);
  case PN_BYTE:
    return pn_string_addf(str, "%" PRIi8, atom->u.as_byte);
  case PN_USHORT:
    return pn_string_addf(str, "%" PRIu16, atom->u.as_ushort);
  case PN_SHORT:
    return pn_string_addf(str, "%" PRIi16, atom->u.as_short);
  case PN_UINT:
    return pn_string_addf(str, "%" PRIu32, atom->u.as_uint);
  case PN_INT:
    return pn_string_addf(str, "%" PRIi32, atom->u.as_int);
  case PN_CHAR:
    return pn_string_addf(str, "%c",  atom->u.as_char);
  case PN_ULONG:
    return pn_string_addf(str, "%" PRIu64, atom->u.as_ulong);
  case PN_LONG:
    return pn_string_addf(str, "%" PRIi64, atom->u.as_long);
  case PN_TIMESTAMP:
    return pn_string_addf(str, "%" PRIi64, atom->u.as_timestamp);
  case PN_FLOAT:
    return pn_string_addf(str, "%g", atom->u.as_float);
  case PN_DOUBLE:
    return pn_string_addf(str, "%g", atom->u.as_double);
  case PN_DECIMAL32:
    return pn_string_addf(str, "D32(%" PRIu32 ")", atom->u.as_decimal32);
  case PN_DECIMAL64:
    return pn_string_addf(str, "D64(%" PRIu64 ")", atom->u.as_decimal64);
  case PN_DECIMAL128:
    return pn_string_addf(str, "D128(%02hhx%02hhx%02hhx%02hhx%02hhx%02hhx"
                          "%02hhx%02hhx%02hhx%02hhx%02hhx%02hhx%02hhx%02hhx"
                          "%02hhx%02hhx)",
                          atom->u.as_decimal128.bytes[0],
                          atom->u.as_decimal128.bytes[1],
                          atom->u.as_decimal128.bytes[2],
                          atom->u.as_decimal128.bytes[3],
                          atom->u.as_decimal128.bytes[4],
                          atom->u.as_decimal128.bytes[5],
                          atom->u.as_decimal128.bytes[6],
                          atom->u.as_decimal128.bytes[7],
                          atom->u.as_decimal128.bytes[8],
                          atom->u.as_decimal128.bytes[9],
                          atom->u.as_decimal128.bytes[10],
                          atom->u.as_decimal128.bytes[11],
                          atom->u.as_decimal128.bytes[12],
                          atom->u.as_decimal128.bytes[13],
                          atom->u.as_decimal128.bytes[14],
                          atom->u.as_decimal128.bytes[15]);
  case PN_UUID:
    return pn_string_addf(str, "UUID(%02hhx%02hhx%02hhx%02hhx-"
                          "%02hhx%02hhx-%02hhx%02hhx-%02hhx%02hhx-"
                          "%02hhx%02hhx%02hhx%02hhx%02hhx%02hhx)",
                          atom->u.as_uuid.bytes[0],
                          atom->u.as_uuid.bytes[1],
                          atom->u.as_uuid.bytes[2],
                          atom->u.as_uuid.bytes[3],
                          atom->u.as_uuid.bytes[4],
                          atom->u.as_uuid.bytes[5],
                          atom->u.as_uuid.bytes[6],
                          atom->u.as_uuid.bytes[7],
                          atom->u.as_uuid.bytes[8],
                          atom->u.as_uuid.bytes[9],
                          atom->u.as_uuid.bytes[10],
                          atom->u.as_uuid.bytes[11],
                          atom->u.as_uuid.bytes[12],
                          atom->u.as_uuid.bytes[13],
                          atom->u.as_uuid.bytes[14],
                          atom->u.as_uuid.bytes[15]);
  case PN_BINARY:
  case PN_STRING:
  case PN_SYMBOL:
    {
      int err;
      const char *pfx;
      pn_bytes_t bin = atom->u.as_bytes;
      bool quote;
      switch (atom->type) {
      case PN_BINARY:
        pfx = "b";
        quote = true;
        break;
      case PN_STRING:
        pfx = "";
        quote = true;
        break;
      case PN_SYMBOL:
        pfx = ":";
        quote = false;
        for (unsigned i = 0; i < bin.size; i++) {
          if (!isalpha(bin.start[i])) {
            quote = true;
            break;
          }
        }
        break;
      default:
        assert(false);
        return PN_ERR;
      }

      if ((err = pn_string_addf(str, "%s", pfx))) return err;
      if (quote) if ((err = pn_string_addf(str, "\""))) return err;
      if ((err = pn_quote(str, bin.start, bin.size))) return err;
      if (quote) if ((err = pn_string_addf(str, "\""))) return err;
      return 0;
    }
  case PN_LIST:
    return pn_string_addf(str, "<list>");
  case PN_MAP:
    return pn_string_addf(str, "<map>");
  case PN_ARRAY:
    return pn_string_addf(str, "<array>");
  case PN_DESCRIBED:
    return pn_string_addf(str, "<described>");
  default:
    return pn_string_addf(str, "<undefined: %i>", atom->type);
  }
}

/* Return index in current list, array etc.*/
static int pni_node_lindex(pn_data_t *data, pni_node_t *node)
{
  int count = 0;
  while (node) {
    node = pn_data_node(data, node->prev);
    count++;
  }
  return count - 1;
}

int pni_inspect_enter(void *ctx, pn_data_t *data, pni_node_t *node)
{
  pn_string_t *str = (pn_string_t *) ctx;
  pn_atom_t *atom = (pn_atom_t *) &node->atom;

  pni_node_t *parent = pn_data_node(data, node->parent);
  const pn_fields_t *fields = pni_node_fields(data, parent);
  pni_node_t *grandparent = parent ? pn_data_node(data, parent->parent) : NULL;
  const pn_fields_t *grandfields = pni_node_fields(data, grandparent);

  int err;

  if (grandfields) {
    if (atom->type == PN_NULL) {
      return 0;
    }
    pni_nid_t lindex = pni_node_lindex(data, node);
    const char *name = (lindex < grandfields->field_count)
        ? (const char*)FIELD_STRINGPOOL.STRING0+FIELD_FIELDS[grandfields->first_field_index+lindex]
        : NULL;
    if (name) {
      err = pn_string_addf(str, "%s=", name);
      if (err) return err;
    }
  }

  switch (atom->type) {
  case PN_DESCRIBED:
    return pn_string_addf(str, "@");
  case PN_ARRAY:
    // XXX: need to fix for described arrays
    return pn_string_addf(str, "@%s[", pn_type_name(node->array_type));
  case PN_LIST:
    return pn_string_addf(str, "[");
  case PN_MAP:
    return pn_string_addf(str, "{");
  default:
    if (fields && node->prev == 0) {
      err = pn_string_addf(str, "%s", (const char *)FIELD_STRINGPOOL.STRING0+FIELD_NAME[fields->name_index]);
      if (err) return err;
      err = pn_string_addf(str, "(");
      if (err) return err;
      err = pni_inspect_atom(atom, str);
      if (err) return err;
      return pn_string_addf(str, ")");
    } else {
      return pni_inspect_atom(atom, str);
    }
  }
}

static inline pni_node_t *pni_data_current(pn_data_t *data)
{
  assert(data);

  if (data->current) return &data->nodes[data->current - 1];

  return NULL;
}

PNI_INLINE void pn_data_rewind(pn_data_t *data)
{
  data->parent = data->base_parent;
  data->current = data->base_current;
}

PNI_INLINE void pn_data_narrow(pn_data_t *data)
{
  data->base_parent = data->parent;
  data->base_current = data->current;
}

PNI_INLINE void pn_data_widen(pn_data_t *data)
{
  data->base_parent = 0;
  data->base_current = 0;
}

int pni_data_traverse(pn_data_t *data,
                      int (*enter)(void *ctx, pn_data_t *data, pni_node_t *node),
                      int (*exit)(void *ctx, pn_data_t *data, pni_node_t *node),
                      void *ctx)
{
  pni_node_t *node = data->size ? pn_data_node(data, 1) : NULL;

  while (node) {
    pni_node_t *parent = pn_data_node(data, node->parent);
    int err = enter(ctx, data, node);
    if (err) return err;

    size_t next = 0;
    if (node->down) {
      next = node->down;
    } else if (node->next) {
      err = exit(ctx, data, node);
      if (err) return err;
      next = node->next;
    } else {
      err = exit(ctx, data, node);
      if (err) return err;
      while (parent) {
        err = exit(ctx, data, parent);
        if (err) return err;
        if (parent->next) {
          next = parent->next;
          break;
        } else {
          parent = pn_data_node(data, parent->parent);
        }
      }
    }

    node = pn_data_node(data, next);
  }

  return 0;
}

static pni_node_t *pni_data_next_nonnull(pn_data_t *data, pni_node_t *node)
{
  while (node) {
    node = pn_data_node(data, node->next);
    if (node && node->atom.type != PN_NULL) {
      return node;
    }
  }

  return NULL;
}

int pni_inspect_exit(void *ctx, pn_data_t *data, pni_node_t *node)
{
  pn_string_t *str = (pn_string_t *) ctx;
  int err;

  switch (node->atom.type) {
  case PN_ARRAY:
  case PN_LIST:
    err = pn_string_addf(str, "]");
    if (err) return err;
    break;
  case PN_MAP:
    err = pn_string_addf(str, "}");
    if (err) return err;
    break;
  default:
    break;
  }

  pni_node_t *parent = pn_data_node(data, node->parent);
  pni_node_t *grandparent = parent ? pn_data_node(data, parent->parent) : NULL;
  const pn_fields_t *grandfields = pni_node_fields(data, grandparent);
  if (!grandfields || node->atom.type != PN_NULL) {
    if (node->next) {
      if (parent && parent->atom.type == PN_MAP && (pni_node_lindex(data, node) % 2) == 0) {
        err = pn_string_addf(str, "=");
        if (err) return err;
      } else if (parent && parent->atom.type == PN_DESCRIBED && node->prev == 0) {
        err = pn_string_addf(str, " ");
        if (err) return err;
      } else {
        if (!grandfields || pni_data_next_nonnull(data, node)) {
          err = pn_string_addf(str, ", ");
          if (err) return err;
        }
      }
    }
  }

  return 0;
}

static int pn_data_inspect(void *obj, pn_string_t *dst)
{
  pn_data_t *data = (pn_data_t *) obj;

  return pni_data_traverse(data, pni_inspect_enter, pni_inspect_exit, dst);
}

#define pn_data_initialize NULL
#define pn_data_hashcode NULL
#define pn_data_compare NULL

static pn_error_t *pni_data_error(pn_data_t *data)
{
  if (data->error == NULL) {
    data->error = pn_error();
  }
  return data->error;
}

pn_data_t *pn_data(size_t capacity)
{
  static const pn_class_t clazz = PN_CLASS(pn_data);
  pn_data_t *data = (pn_data_t *) pn_class_new(&clazz, sizeof(pn_data_t));
  pni_node_t *nodes = NULL;

  if (capacity) {
    nodes = (pni_node_t *) pni_mem_suballocate(&clazz, data, capacity * sizeof(pni_node_t));
  }

  *data = (pn_data_t) { 0, .capacity = capacity, .nodes = nodes };

  return data;
}

void pn_data_free(pn_data_t *data)
{
  pn_free(data);
}

int pn_data_errno(pn_data_t *data)
{
  return pn_error_code(pni_data_error(data));
}

pn_error_t *pn_data_error(pn_data_t *data)
{
  return pni_data_error(data);
}

static inline size_t pni_data_size(pn_data_t *data)
{
  return data->size;
}

size_t pn_data_size(pn_data_t *data)
{
  return data ? pni_data_size(data) : 0;
}

static inline void pni_data_clear(pn_data_t *data)
{
  data->size = 0;
  data->parent = 0;
  data->current = 0;
  data->base_parent = 0;
  data->base_current = 0;

  if (data->buf) pn_buffer_clear(data->buf);
}

PNI_INLINE void pn_data_clear(pn_data_t *data)
{
  if (data) pni_data_clear(data);
}

PNI_COLD static int pni_data_grow(pn_data_t *data)
{
  size_t capacity = data->capacity ? data->capacity : 2;

  capacity *= 2;

  if (capacity == PNI_NID_MAX + 1) capacity = PNI_NID_MAX;
  else if (capacity > PNI_NID_MAX) return PN_OUT_OF_MEMORY;

  pni_node_t *new_nodes = (pni_node_t *) pni_mem_subreallocate(pn_class(data), data, data->nodes,
                                                               capacity * sizeof(pni_node_t));

  if (new_nodes == NULL) return PN_OUT_OF_MEMORY;

  data->capacity = capacity;
  data->nodes = new_nodes;

  return 0;
}

static void pni_data_rebase(pn_data_t *data, const char *base)
{
  size_t size = data->size;

  for (size_t i = 0; i < size; i++) {
    pni_node_t *node = &data->nodes[i];

    if (node->data) {
      pn_bytes_t *bytes = &node->atom.u.as_bytes;
      bytes->start = base + node->data_offset;
    }
  }
}

PNI_INLINE int pni_data_intern_node(pn_data_t *data, pni_node_t *node)
{
  assert(node->atom.type == PN_BINARY || node->atom.type == PN_STRING || node->atom.type == PN_SYMBOL);

  pn_bytes_t *bytes = &node->atom.u.as_bytes;

  assert(bytes);

  if (!data->buf) {
    // A heuristic to avoid growing small buffers too much.  Set to
    // size + 1 to allow for zero termination.
    data->buf = pn_buffer(pn_max(bytes->size + 1, PNI_INTERN_MINSIZE));
    if (!data->buf) return PN_OUT_OF_MEMORY;
  }

  size_t old_capacity = pn_buffer_capacity(data->buf);
  size_t old_size = pn_buffer_size(data->buf);

  pn_buffer_append(data->buf, bytes->start, bytes->size);
  pn_buffer_append(data->buf, "\0", 1);

  node->data = true;
  node->data_offset = old_size;
  node->data_size = bytes->size;

  pn_bytes_t interned_bytes = pn_buffer_bytes(data->buf);

  // Set the atom pointer to the interned string
  bytes->start = interned_bytes.start + old_size;

  if (pn_buffer_capacity(data->buf) != old_capacity) {
    pni_data_rebase(data, interned_bytes.start);
  }

  return 0;
}

static int pni_data_copy_nodes(pn_data_t *dst_data, pni_node_t *dst_node,
                               pn_data_t *src_data, pni_node_t *src_node,
                               int limit);

// Copy src_data to dst after normalizing for "multiple" field
// encoding.
//
// AMQP composite field definitions can be declared "multiple".  See:
//
// - http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-types-v1.0-os.html#doc-idp115568
// - http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-types-v1.0-os.html#section-composite-type-representation
//
// Multiple fields allow redundant encoding of two cases:
//
// 1. empty: null or an empty array
// 2. single-value: direct encoding of value, or array with one element
//
// For encoding compactness and inter-operability, normalize multiple
// field values to always use null for empty, and direct encoding for
// single value.
static int pni_data_normalize_multiple(pn_data_t *dst_data, pni_node_t *dst_node, pn_data_t *src_data)
{
  int err = 0;
  pn_handle_t point = pn_data_point(src_data);

  pn_data_rewind(src_data);

  pni_node_t *src_node = pni_data_next_node(src_data);

  if (src_node->atom.type == PN_ARRAY) {
    switch (pn_data_get_array(src_data)) {
    case 0: {
      // Empty array => null
      pni_node_set_type(dst_node, PN_NULL);
      break;
    }
    case 1: {
      // Single-element array => encode the element

      pni_data_enter(src_data);

      src_node = pni_data_next_node(src_data);
      err = pni_data_copy_nodes(dst_data, dst_node, src_data, src_node, 1);

      break;
    }
    default:
      // Multi-element array => encode unchanged
      err = pni_data_copy_nodes(dst_data, dst_node, src_data, src_node, 1);
    }
  } else {
    // Non-array => append the value
    err = pni_data_copy_nodes(dst_data, dst_node, src_data, src_node, 1);
  }

  pn_data_restore(src_data, point);

  return err;
}

static inline pni_node_t *pni_data_first_node(pn_data_t *data)
{
  pni_nid_t next;

  if (data->base_current) {
    next = data->nodes[data->base_current - 1].next;
  } else if (data->base_parent) {
    next = data->nodes[data->base_parent - 1].down;
  } else if (data->size) {
    next = 1;
  } else {
    return 0;
  }

  if (next) {
    return pni_data_node(data, next);
  }

  return NULL;
}

static inline void pni_data_fill_skip_arg(va_list ap, char code)
{
  switch (code) {
  case '?':
  case 'o':
  case 'B':
  case 'b':
  case 'H':
  case 'h':
  case 'I':
  case 'i':
  case 'c':
  case 'L':
  case 'l':
  case 't':
  case 'f':
  case 'd':
  case 'S':
  case 's':
  case 'C':
  case 'M': va_arg(ap, void *); break;
  case '*':
  case 'Z':
  case 'z': va_arg(ap, void *); va_arg(ap, void *); break;
  }
}

static void pni_data_fill_skip_compound(const char **fmt, va_list ap, char open_code, char close_code)
{
  char code;
  size_t level = 0;

  while ((code = **fmt)) {
    (*fmt)++;

    if (code == open_code) {
      level++;
    } else if (code == close_code) {
      if (level == 0) break;
      else level--;
    } else {
      pni_data_fill_skip_arg(ap, code);
    }
  }
}

static void pni_data_fill_skip(const char **fmt, va_list ap, char code);

static void pni_data_fill_skip_described(const char **fmt, va_list ap)
{
  char code;
  size_t count = 0;

  while ((code = **fmt) && count++ < 2) {
    (*fmt)++;
    pni_data_fill_skip(fmt, ap, code);
  }
}

static inline void pni_data_fill_skip(const char **fmt, va_list ap, char code)
{
  switch (code) {
  case '@':
    assert(**fmt == 'T');
    (*fmt)++;
    assert(**fmt == '[');
    (*fmt)++;
    // Falls through
  case '[': pni_data_fill_skip_compound(fmt, ap, '[', ']'); break;
  case '{': pni_data_fill_skip_compound(fmt, ap, '{', '}'); break;
  case 'D': pni_data_fill_skip_described(fmt, ap); break;
  default: pni_data_fill_skip_arg(ap, code);
  }
}

PNI_HOT static int pni_data_vfill(pn_data_t *data, const char *fmt, va_list ap)
{
  // fprintf(stderr, "FILL fmt=%s\n", fmt);

  while (*fmt) {
    char code = *(fmt++);
    bool skip = false;

    // fprintf(stderr, "FILL   code=%c current=%d parent=%d\n", code, data->current, data->parent);

    if (code == '?') {
      assert(*fmt);
      assert(*fmt != '?');

      if (!va_arg(ap, int)) skip = true;

      code = *(fmt++);
    }

    if (code == ']' || code == '}') {
      assert(data->parent);

      pni_data_exit(data);
      goto end;
    }

    pni_node_t *node = pni_data_add_node(data);
    if (!node) return PN_OUT_OF_MEMORY;

    if (skip) {
      pni_data_fill_skip(&fmt, ap, code);
      pni_node_set_type(node, PN_NULL);
      goto end;
    }

    switch (code) {
    case 'n': pni_node_set_type(node, PN_NULL); break;
    case 'o': pni_node_set_bool(node, va_arg(ap, int)); break;
    case 'B': pni_node_set_ubyte(node, va_arg(ap, unsigned int)); break;
    case 'b': pni_node_set_byte(node, va_arg(ap, int)); break;
    case 'H': pni_node_set_ushort(node, va_arg(ap, unsigned int)); break;
    case 'h': pni_node_set_short(node, va_arg(ap, int)); break;
    case 'I': pni_node_set_uint(node, va_arg(ap, uint32_t)); break;
    case 'i': pni_node_set_int(node, va_arg(ap, int32_t)); break;
    case 'L': pni_node_set_ulong(node, va_arg(ap, uint64_t)); break;
    case 'l': pni_node_set_long(node, va_arg(ap, int64_t)); break;
    case 't': pni_node_set_timestamp(node, va_arg(ap, pn_timestamp_t)); break;
    case 'f': pni_node_set_float(node, va_arg(ap, double)); break;
    case 'd': pni_node_set_double(node, va_arg(ap, double)); break;
    case 'Z': // XXX error check this case?
    case 'z': {
      // For maximum portability, the caller must pass these as two
      // separate args, not a single struct
      size_t size = va_arg(ap, size_t);
      char *start = va_arg(ap, char *);
      if (start) {
        pni_node_set_bytes(node, PN_BINARY, pn_bytes(size, start));
      } else {
        pni_node_set_type(node, PN_NULL);
      }
      break;
    }
    case 'S': {
      char *start = va_arg(ap, char *);
      if (start) {
        pni_node_set_bytes(node, PN_STRING, pn_bytes(strlen(start), start));
      } else {
        pni_node_set_type(node, PN_NULL);
      }
      break;
    }
    case 's': {
      char *start = va_arg(ap, char *);
      if (start) {
        pni_node_set_bytes(node, PN_SYMBOL, pn_bytes(strlen(start), start));
      } else {
        pni_node_set_type(node, PN_NULL);
      }
      break;
    }
    case 'D': {
      pni_node_set_type(node, PN_DESCRIBED);
      pni_data_enter(data);
      break;
    }
    case '@': {
      assert(*fmt == 'T');
      fmt++;
      assert(*fmt == '[');
      fmt++;

      pni_node_set_type(node, PN_ARRAY);
      node->array_type = (pn_type_t) va_arg(ap, int);
      pni_data_enter(data);

      break;
    }
    case '[': {
      pni_node_set_type(node, PN_LIST);
      pni_data_enter(data);
      break;
    }
    case '{': {
      pni_node_set_type(node, PN_MAP);
      pni_data_enter(data);
      break;
    }
    case 'C': {
      pn_data_t *src_data = va_arg(ap, pn_data_t *);
      if (src_data && pni_data_size(src_data) > 0) {
        pni_node_t *src_node = pni_data_first_node(src_data);
        int err = pni_data_copy_nodes(data, node, src_data, src_node, 1);
        if (err) return err;
      } else {
        pni_node_set_type(node, PN_NULL);
      }
      break;
    }
    case 'M': {
      pn_data_t *src = va_arg(ap, pn_data_t *);
      if (src && pni_data_size(src) > 0) {
        int err = pni_data_normalize_multiple(data, node, src);
        if (err) return err;
      } else {
        pni_node_set_type(node, PN_NULL);
      }
      break;
    }
    case '*': {
      int count = va_arg(ap, int);
      void *ptr = va_arg(ap, void *);

      assert(*fmt == 's' && "unrecognized fill code");
      fmt++;

      char **sptr = (char **) ptr;

      for (int i = 0; i < count; i++) {
        char *sym = *(sptr++);

        if (sym) {
          pni_node_set_bytes(node, PN_SYMBOL, pn_bytes(strlen(sym), sym));
        } else {
          pni_node_set_type(node, PN_NULL);
        }

        node = pni_data_add_node(data);
        if (!node) return PN_OUT_OF_MEMORY;
      }

      break;
    }
    default:
      assert(false && "unrecognized fill code");
    }

  end:

    while (data->parent) {
      pni_node_t *parent = pni_data_node(data, data->parent);

      if (parent->atom.type == PN_DESCRIBED && parent->children == 2) {
        pni_data_exit(data);
      } else {
        break;
      }
    }
  }

  return 0;
}

int pn_data_vfill(pn_data_t *data, const char *fmt, va_list ap)
{
  return pni_data_vfill(data, fmt, ap);
}

/* Format codes:
   code: AMQP-type (arguments)
   n: null ()
   o: bool (int)
   B: ubyte (unsigned int)
   b: byte (int)
   H: ushort  (unsigned int)
   h: short (int)
   I: uint (uint32_t)
   i: int (int32_t)
   L: ulong (ulong32_t)
   l: long (long32_t)
   t: timestamp (pn_timestamp_t)
   f: float (float)
   d: double (double)
   Z: binary (size_t, char*) - must not be NULL
   z: binary (size_t, char*) - encode as AMQP null if NULL
   S: string (char*)
   s: symbol (char*)
   D: described - next two codes are [descriptor, body]
   @: enter array. If followed by D, a described array. Following codes to matching ']' are elements.
   T: type (pn_type_t) - set array type while in array
   [: enter list. Following codes up to matching ']' are elements
   {: enter map. Following codes up to matching '}' are key, value  pairs
   ]: exit list or array
   }: exit map
   ?: TODO document
   *: TODO document
   C: single value (pn_data_t*) - append the pn_data_t unmodified
   M: multiple value (pn_data_t*) - normalize and append multiple field value,
      see pni_normalize_multiple()
 */
int pn_data_fill(pn_data_t *data, const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  int err = pni_data_vfill(data, fmt, ap);
  va_end(ap);
  return err;
}

static inline void pni_data_scan_skip_arg(va_list ap, char code)
{
  switch (code) {
  case '?':
  case 'o':
  case 'B':
  case 'b': *va_arg(ap, uint8_t *) = 0; break;
  case 'H':
  case 'h': *va_arg(ap, uint16_t *) = 0; break;
  case 'I':
  case 'i':
  case 'c': *va_arg(ap, uint32_t *) = 0; break;
  case 'L':
  case 'l':
  case 't': *va_arg(ap, uint64_t *) = 0; break;
  case 'f': *va_arg(ap, float *) = 0; break;
  case 'd': *va_arg(ap, double *) = 0; break;
  case 'z':
  case 'S':
  case 's': *va_arg(ap, pn_bytes_t *) = pn_bytes_null; break;
  case 'C': va_arg(ap, pn_data_t *); break;
  }
}

static void pni_data_scan_skip_compound(const char **fmt, va_list ap, char open_code, char close_code)
{
  char code;
  size_t level = 0;

  while ((code = **fmt)) {
    (*fmt)++;

    if (code == open_code) {
      level++;
    } else if (code == close_code) {
      if (level == 0) break;
      else level--;
    } else {
      pni_data_scan_skip_arg(ap, code);
    }
  }
}

static void pni_data_scan_skip(const char **fmt, va_list ap, char code);

static void pni_data_scan_skip_described(const char **fmt, va_list ap)
{
  char code;
  size_t count = 0;

  while ((code = **fmt) && count++ < 2) {
    (*fmt)++;

    if (code == '?') {
      // Don't count presence args in the count
      *va_arg(ap, bool *) = false;
      (*fmt)++;
    }

    pni_data_scan_skip(fmt, ap, code);
  }
}

static inline void pni_data_scan_skip(const char **fmt, va_list ap, char code)
{
  switch (code) {
  case '@':
    assert(**fmt == '[');
    (*fmt)++;
    // Falls through
  case '[': pni_data_scan_skip_compound(fmt, ap, '[', ']'); break;
  case '{': pni_data_scan_skip_compound(fmt, ap, '{', '}'); break;
  case 'D': pni_data_scan_skip_described(fmt, ap); break;
  default: pni_data_scan_skip_arg(ap, code);
  }
}

PNI_HOT static int pni_data_vscan(pn_data_t *data, const char *fmt, va_list ap)
{
  // fprintf(stderr, "SCAN fmt=%s\n", fmt);

  pn_data_rewind(data);

  bool void_presence_arg = false;

  while (*fmt) {
    char code = *(fmt++);
    bool *presence_arg = &void_presence_arg;
    pni_node_t *node = NULL;

    // fprintf(stderr, "SCAN code=%c current=%d parent=%d\n", code, data->current, data->parent);

    if (code == '?') {
      assert(*fmt);
      assert(*fmt != '?');

      presence_arg = va_arg(ap, bool *);
      code = *(fmt++);
    }

    if (code == ']' || code == '}') {
      assert(data->parent);

      pni_data_exit(data);
      continue;
    }

    node = pni_data_next_node(data);

    if (!node) {
      if (data->current == data->size) {
        // There is no more data to process.  Zero the remaining args
        // and leave.

        do pni_data_scan_skip_arg(ap, code);
        while ((code = *(fmt++)));

        break;
      }

      while (!node && data->parent) {
        pni_node_t *parent = pni_data_node(data, data->parent);

        if (parent->atom.type != PN_DESCRIBED) break;

        pni_data_exit(data);
        node = pni_data_next_node(data);
      }
    }

    if (!node || node->atom.type == PN_NULL) {
      *presence_arg = false;
      pni_data_scan_skip(&fmt, ap, code);
      continue;
    }

    *presence_arg = true;

    switch (code) {
    case '.':
    case 'n': {
      break;
    }
    case 'o': {
      if (node->atom.type != PN_BOOL) return PN_ARG_ERR;
      *va_arg(ap, bool *) = pni_node_get_bool(node);
      break;
    }
    case 'B': {
      if (node->atom.type != PN_UBYTE) return PN_ARG_ERR;
      *va_arg(ap, uint8_t *) = pni_node_get_ubyte(node);
      break;
    }
    case 'b': {
      if (node->atom.type != PN_BYTE) return PN_ARG_ERR;
      *va_arg(ap, int8_t *) = pni_node_get_byte(node);
      break;
    }
    case 'H': {
      if (node->atom.type != PN_USHORT) return PN_ARG_ERR;
      *va_arg(ap, uint16_t *) = pni_node_get_ushort(node);
      break;
    }
    case 'h': {
      if (node->atom.type != PN_SHORT) return PN_ARG_ERR;
      *va_arg(ap, int16_t *) = pni_node_get_short(node);
      break;
    }
    case 'I': {
      if (node->atom.type != PN_UINT) return PN_ARG_ERR;
      *va_arg(ap, uint32_t *) = pni_node_get_uint(node);
      break;
    }
    case 'i': {
      if (node->atom.type != PN_INT) return PN_ARG_ERR;
      *va_arg(ap, int32_t *) = pni_node_get_int(node);
      break;
    }
    case 'c': {
      if (node->atom.type != PN_CHAR) return PN_ARG_ERR;
      *va_arg(ap, pn_char_t *) = pni_node_get_char(node);
      break;
    }
    case 'L': {
      if (node->atom.type != PN_ULONG) return PN_ARG_ERR;
      *va_arg(ap, uint64_t *) = pni_node_get_ulong(node);
      break;
    }
    case 'l': {
      if (node->atom.type != PN_LONG) return PN_ARG_ERR;
      *va_arg(ap, int64_t *) = pni_node_get_long(node);
      break;
    }
    case 't': {
      if (node->atom.type != PN_TIMESTAMP) return PN_ARG_ERR;
      *va_arg(ap, pn_timestamp_t *) = pni_node_get_timestamp(node);
      break;
    }
    case 'f': {
      if (node->atom.type != PN_FLOAT) return PN_ARG_ERR;
      *va_arg(ap, float *) = pni_node_get_float(node);
      break;
    }
    case 'd': {
      if (node->atom.type != PN_DOUBLE) return PN_ARG_ERR;
      *va_arg(ap, double *) = pni_node_get_double(node);
      break;
    }
    case 'z': {
      if (node->atom.type != PN_BINARY) return PN_ARG_ERR;
      *va_arg(ap, pn_bytes_t *) = pni_node_get_bytes(node);
      break;
    }
    case 'S': {
      // This is to accommodate the way targets of type coordinator
      // are handled in transport.c.
      if (node->atom.type != PN_STRING) {
        pni_data_scan_skip_arg(ap, code);
        *presence_arg = false;
        break;
      }
      // if (node->atom.type != PN_STRING) return PN_ARG_ERR;
      *va_arg(ap, pn_bytes_t *) = pni_node_get_bytes(node);
      break;
    }
    case 's': {
      if (node->atom.type != PN_SYMBOL) return PN_ARG_ERR;
      *va_arg(ap, pn_bytes_t *) = pni_node_get_bytes(node);
      break;
    }
    case 'C': {
      pn_data_t *dst_data = va_arg(ap, pn_data_t *);
      pni_node_t *dst_node = pni_data_add_node(dst_data);
      if (!dst_node) return PN_OUT_OF_MEMORY;
      int err = pni_data_copy_nodes(dst_data, dst_node, data, node, 1);
      if (err) return err;
      break;
    }
    case 'D': {
      if (node->atom.type != PN_DESCRIBED) {
        pni_data_scan_skip_described(&fmt, ap);
        *presence_arg = false;
        break;
      }
      pni_data_enter(data);
      break;
    }
    case '@': {
      if (node->atom.type != PN_ARRAY) return PN_ARG_ERR;
      assert(*fmt == '[');
      fmt++;
      pni_data_enter(data);
      break;
    }
    case '[': {
      if (node->atom.type != PN_LIST) return PN_ARG_ERR;
      pni_data_enter(data);
      break;
    }
    case '{': {
      if (node->atom.type != PN_MAP) return PN_ARG_ERR;
      pni_data_enter(data);
      break;
    }
    default:
      assert(false && "unrecognized scan code");
    }
  }

  return 0;
}

int pn_data_vscan(pn_data_t *data, const char *fmt, va_list ap)
{
  return pni_data_vscan(data, fmt, ap);
}

int pn_data_scan(pn_data_t *data, const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  int err = pni_data_vscan(data, fmt, ap);
  va_end(ap);
  return err;
}

int pn_data_print(pn_data_t *data)
{
  pn_string_t *str = pn_string("");
  int err = pn_data_inspect(data, str);
  if (err) {
    pn_free(str);
    return err;
  }
  printf("%s", pn_string_get(str));
  pn_free(str);
  return 0;
}

int pn_data_format(pn_data_t *data, char *bytes, size_t *size)
{
  pn_string_t *str = pn_string("");
  int err = pn_data_inspect(data, str);
  if (err) return err;
  if (pn_string_size(str) >= *size) {
    pn_free(str);
    return PN_OVERFLOW;
  } else {
    pn_string_put(str, bytes);
    *size = pn_string_size(str);
    pn_free(str);
    return 0;
  }
}

PNI_INLINE pn_handle_t pn_data_point(pn_data_t *data)
{
  if (data->current) {
    return (pn_handle_t)(uintptr_t)data->current;
  } else {
    return (pn_handle_t)(uintptr_t)-data->parent;
  }
}

bool pn_data_restore(pn_data_t *data, pn_handle_t point)
{
  pn_shandle_t spoint = (pn_shandle_t) point;
  if (spoint <= 0 && ((size_t) (-spoint)) <= data->size) {
    data->parent = -((pn_shandle_t) point);
    data->current = 0;
    return true;
  } else if (spoint && spoint <= data->size) {
    data->current = spoint;
    pni_node_t *current = pni_data_current(data);
    data->parent = current->parent;
    return true;
  } else {
    return false;
  }
}

PNI_INLINE pni_node_t *pni_data_next_node(pn_data_t *data)
{
  pni_nid_t next = 0;

  if (data->current) {
    next = data->nodes[data->current - 1].next;
  } else if (data->parent) {
    next = data->nodes[data->parent - 1].down;
  } else if (data->size) {
    next = 1;
  }

  if (next) {
    data->current = next;
    return &data->nodes[next - 1];
  }

  return NULL;
}

PNI_INLINE bool pn_data_next(pn_data_t *data)
{
  return pni_data_next_node(data) != NULL;
}

bool pn_data_prev(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->prev) {
    data->current = node->prev;
    return true;
  } else {
    return false;
  }
}

PNI_INLINE pn_type_t pn_data_type(pn_data_t *data)
{
  if (data->current) {
    pni_node_t *node = pni_data_node(data, data->current);
    return node->atom.type;
  } else {
    return PN_INVALID;
  }
}

PNI_INLINE void pni_data_enter(pn_data_t *data)
{
  assert(data->current);

  data->parent = data->current;
  data->current = 0;
}

PNI_INLINE bool pn_data_enter(pn_data_t *data)
{
  if (data->current) {
    pni_data_enter(data);
    return true;
  } else {
    return false;
  }
}

PNI_INLINE void pni_data_exit(pn_data_t *data)
{
  assert(data->parent);

  pni_node_t *parent = pni_data_node(data, data->parent);

  data->current = data->parent;
  data->parent = parent->parent;
}

PNI_INLINE bool pn_data_exit(pn_data_t *data)
{
  if (data->parent) {
    pni_data_exit(data);
    return true;
  } else {
    return false;
  }
}

bool pn_data_lookup(pn_data_t *data, const char *name)
{
  while (pn_data_next(data)) {
    pn_type_t type = pn_data_type(data);

    switch (type) {
    case PN_STRING:
    case PN_SYMBOL:
      {
        pn_bytes_t bytes = pn_data_get_bytes(data);
        if (pn_bytes_equal(bytes, pn_bytes(strlen(name), name))) {
          return pn_data_next(data);
        }
      }
      break;
    default:
      break;
    }

    // skip the value
    pn_data_next(data);
  }

  return false;
}

void pn_data_dump(pn_data_t *data)
{
  pn_string_t *str = pn_string(0);
  printf("{current=%" PN_ZI ", parent=%" PN_ZI "}\n", (size_t) data->current, (size_t) data->parent);
  for (unsigned i = 0; i < data->size; i++)
  {
    pni_node_t *node = &data->nodes[i];
    pn_string_setn(str, "", 0);
    pni_inspect_atom((pn_atom_t *) &node->atom, str);
    printf("Node %i: prev=%" PN_ZI ", next=%" PN_ZI ", parent=%" PN_ZI ", down=%" PN_ZI
           ", children=%" PN_ZI ", type=%s (%s)\n",
           i + 1, (size_t) node->prev,
           (size_t) node->next,
           (size_t) node->parent,
           (size_t) node->down,
           (size_t) node->children,
           pn_type_name(node->atom.type), pn_string_get(str));
  }
  pn_free(str);
}

PNI_HOT PNI_INLINE pni_node_t *pni_data_add_node(pn_data_t *data)
{
  if (data->capacity <= data->size) {
    int err = pni_data_grow(data);
    if (err) return NULL;
  }

  pni_node_t *node = &data->nodes[data->size++];
  pni_nid_t node_id = data->size;

  *node = (pni_node_t) {0};

  if (data->current) {
    pni_node_t *current = &data->nodes[data->current - 1];

    current->next = node_id;
    node->prev = data->current;
  }

  if (data->parent) {
    pni_node_t *parent = &data->nodes[data->parent - 1];

    node->parent = data->parent;
    parent->children++;

    if (!parent->down) {
      parent->down = node_id;
    }
  }

  data->current = node_id;

  return node;
}

ssize_t pn_data_encode(pn_data_t *data, char *bytes, size_t size)
{
  pni_encoder_t encoder;

  pni_encoder_initialize(&encoder);

  ssize_t result = pni_encoder_encode(&encoder, data, bytes, size);

  if (result < 0) {
    pn_error_copy(pn_data_error(data), encoder.error);
  }

  pni_encoder_finalize(&encoder);

  return result;
}

ssize_t pn_data_encoded_size(pn_data_t *data)
{
  pni_encoder_t encoder;

  pni_encoder_initialize(&encoder);

  ssize_t result = pni_encoder_size(&encoder, data);

  pni_encoder_finalize(&encoder);

  return result;
}

ssize_t pn_data_decode(pn_data_t *data, const char *bytes, size_t size)
{
  pni_decoder_t decoder;

  pni_decoder_initialize(&decoder);

  ssize_t result = pni_decoder_decode(&decoder, bytes, size, data);

  if (result < 0) {
    pn_error_copy(pn_data_error(data), decoder.error);
  }

  pni_decoder_finalize(&decoder);

  return result;
}

static inline int pni_data_put_compound(pn_data_t *data, pn_type_t type)
{
  pni_node_t *node = pni_data_add_node(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  pni_node_set_type(node, type);
  return 0;
}

int pn_data_put_list(pn_data_t *data)
{
  return pni_data_put_compound(data, PN_LIST);
}

int pn_data_put_map(pn_data_t *data)
{
  return pni_data_put_compound(data, PN_MAP);
}

int pn_data_put_array(pn_data_t *data, bool described, pn_type_t type)
{
  pni_node_t *node = pni_data_add_node(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  pni_node_set_type(node, PN_ARRAY);
  node->array_described = described;
  node->array_type = type;
  return 0;
}

int pn_data_put_described(pn_data_t *data)
{
  pni_node_t *node = pni_data_add_node(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  pni_node_set_type(node, PN_DESCRIBED);
  return 0;
}

int pn_data_put_null(pn_data_t *data)
{
  pni_node_t *node = pni_data_add_node(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  pni_node_set_type(node, PN_NULL);
  return 0;
}

int pn_data_put_bool(pn_data_t *data, bool b)
{
  pni_node_t *node = pni_data_add_node(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  pni_node_set_bool(node, b);
  return 0;
}

int pn_data_put_ubyte(pn_data_t *data, uint8_t ub)
{
  pni_node_t *node = pni_data_add_node(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  pni_node_set_ubyte(node, ub);
  return 0;
}

int pn_data_put_byte(pn_data_t *data, int8_t b)
{
  pni_node_t *node = pni_data_add_node(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  pni_node_set_byte(node, b);
  return 0;
}

int pn_data_put_ushort(pn_data_t *data, uint16_t us)
{
  pni_node_t *node = pni_data_add_node(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  pni_node_set_ushort(node, us);
  return 0;
}

int pn_data_put_short(pn_data_t *data, int16_t s)
{
  pni_node_t *node = pni_data_add_node(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  pni_node_set_short(node, s);
  return 0;
}

int pn_data_put_uint(pn_data_t *data, uint32_t ui)
{
  pni_node_t *node = pni_data_add_node(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  pni_node_set_uint(node, ui);
  return 0;
}

int pn_data_put_int(pn_data_t *data, int32_t i)
{
  pni_node_t *node = pni_data_add_node(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  pni_node_set_int(node, i);
  return 0;
}

int pn_data_put_char(pn_data_t *data, pn_char_t c)
{
  pni_node_t *node = pni_data_add_node(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  pni_node_set_char(node, c);
  return 0;
}

int pn_data_put_ulong(pn_data_t *data, uint64_t ul)
{
  pni_node_t *node = pni_data_add_node(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  pni_node_set_ulong(node, ul);
  return 0;
}

int pn_data_put_long(pn_data_t *data, int64_t l)
{
  pni_node_t *node = pni_data_add_node(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  pni_node_set_long(node, l);
  return 0;
}

int pn_data_put_timestamp(pn_data_t *data, pn_timestamp_t t)
{
  pni_node_t *node = pni_data_add_node(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  pni_node_set_timestamp(node, t);
  return 0;
}

int pn_data_put_float(pn_data_t *data, float f)
{
  pni_node_t *node = pni_data_add_node(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  pni_node_set_float(node, f);
  return 0;
}

int pn_data_put_double(pn_data_t *data, double d)
{
  pni_node_t *node = pni_data_add_node(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  pni_node_set_double(node, d);
  return 0;
}

int pn_data_put_decimal32(pn_data_t *data, pn_decimal32_t d)
{
  pni_node_t *node = pni_data_add_node(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  pni_node_set_decimal32(node, d);
  return 0;
}

int pn_data_put_decimal64(pn_data_t *data, pn_decimal64_t d)
{
  pni_node_t *node = pni_data_add_node(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  pni_node_set_decimal64(node, d);
  return 0;
}

int pn_data_put_decimal128(pn_data_t *data, pn_decimal128_t d)
{
  pni_node_t *node = pni_data_add_node(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  pni_node_set_decimal128(node, d);
  return 0;
}

int pn_data_put_uuid(pn_data_t *data, pn_uuid_t u)
{
  pni_node_t *node = pni_data_add_node(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  pni_node_set_uuid(node, u);
  return 0;
}

static inline int pni_data_put_variable(pn_data_t *data, pn_type_t type, pn_bytes_t bytes)
{
  pni_node_t *node = pni_data_add_node(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  pni_node_set_bytes(node, type, bytes);
  return pni_data_intern_node(data, node);
}

int pn_data_put_binary(pn_data_t *data, pn_bytes_t bytes)
{
  return pni_data_put_variable(data, PN_BINARY, bytes);
}

int pn_data_put_string(pn_data_t *data, pn_bytes_t bytes)
{
  return pni_data_put_variable(data, PN_STRING, bytes);
}

int pn_data_put_symbol(pn_data_t *data, pn_bytes_t bytes)
{
  return pni_data_put_variable(data, PN_SYMBOL, bytes);
}

int pn_data_put_atom(pn_data_t *data, pn_atom_t atom)
{
  pni_node_t *node = pni_data_add_node(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  node->atom = atom;
  if (node->atom.type == PN_STRING || node->atom.type == PN_SYMBOL || node->atom.type == PN_BINARY) {
    return pni_data_intern_node(data, node);
  } else {
    return 0;
  }
}

size_t pn_data_get_list(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_LIST) {
    return node->children;
  } else {
    return 0;
  }
}

size_t pn_data_get_map(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_MAP) {
    return node->children;
  } else {
    return 0;
  }
}

size_t pn_data_get_array(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_ARRAY) {
    if (node->array_described) {
      return node->children - 1;
    } else {
      return node->children;
    }
  } else {
    return 0;
  }
}

bool pn_data_is_array_described(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_ARRAY) {
    return node->array_described;
  } else {
    return false;
  }
}

pn_type_t pn_data_get_array_type(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_ARRAY) {
    return node->array_type;
  } else {
    return PN_INVALID;
  }
}

bool pn_data_is_described(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  return node && node->atom.type == PN_DESCRIBED;
}

bool pn_data_is_null(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  return node && node->atom.type == PN_NULL;
}

bool pn_data_get_bool(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_BOOL) {
    return pni_node_get_bool(node);
  } else {
    return false;
  }
}

uint8_t pn_data_get_ubyte(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_UBYTE) {
    return pni_node_get_ubyte(node);
  } else {
    return 0;
  }
}

int8_t pn_data_get_byte(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_BYTE) {
    return pni_node_get_byte(node);
  } else {
    return 0;
  }
}

uint16_t pn_data_get_ushort(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_USHORT) {
    return pni_node_get_ushort(node);
  } else {
    return 0;
  }
}

int16_t pn_data_get_short(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_SHORT) {
    return pni_node_get_short(node);
  } else {
    return 0;
  }
}

uint32_t pn_data_get_uint(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_UINT) {
    return pni_node_get_uint(node);
  } else {
    return 0;
  }
}

int32_t pn_data_get_int(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_INT) {
    return pni_node_get_int(node);
  } else {
    return 0;
  }
}

pn_char_t pn_data_get_char(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_CHAR) {
    return pni_node_get_char(node);
  } else {
    return 0;
  }
}

uint64_t pn_data_get_ulong(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_ULONG) {
    return pni_node_get_ulong(node);
  } else {
    return 0;
  }
}

int64_t pn_data_get_long(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_LONG) {
    return pni_node_get_long(node);
  } else {
    return 0;
  }
}

pn_timestamp_t pn_data_get_timestamp(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_TIMESTAMP) {
    return pni_node_get_timestamp(node);
  } else {
    return 0;
  }
}

float pn_data_get_float(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_FLOAT) {
    return pni_node_get_float(node);
  } else {
    return 0;
  }
}

double pn_data_get_double(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_DOUBLE) {
    return pni_node_get_double(node);
  } else {
    return 0;
  }
}

pn_decimal32_t pn_data_get_decimal32(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_DECIMAL32) {
    return pni_node_get_decimal32(node);
  } else {
    return 0;
  }
}

pn_decimal64_t pn_data_get_decimal64(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_DECIMAL64) {
    return pni_node_get_decimal64(node);
  } else {
    return 0;
  }
}

pn_decimal128_t pn_data_get_decimal128(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_DECIMAL128) {
    return pni_node_get_decimal128(node);
  } else {
    return (pn_decimal128_t) {{0}};
  }
}

pn_uuid_t pn_data_get_uuid(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_UUID) {
    return pni_node_get_uuid(node);
  } else {
    return (pn_uuid_t) {{0}};
  }
}

static inline pn_bytes_t pni_data_get_variable(pn_data_t *data, pn_type_t type)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == type) {
    return pni_node_get_bytes(node);
  } else {
    return pn_bytes_null;
  }
}

pn_bytes_t pn_data_get_binary(pn_data_t *data)
{
  return pni_data_get_variable(data, PN_BINARY);
}

pn_bytes_t pn_data_get_string(pn_data_t *data)
{
  return pni_data_get_variable(data, PN_STRING);
}

pn_bytes_t pn_data_get_symbol(pn_data_t *data)
{
  return pni_data_get_variable(data, PN_SYMBOL);
}

pn_bytes_t pn_data_get_bytes(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && (node->atom.type == PN_STRING ||
               node->atom.type == PN_SYMBOL ||
               node->atom.type == PN_BINARY)) {
    return node->atom.u.as_bytes;
  } else {
    return pn_bytes_null;
  }
}

pn_atom_t pn_data_get_atom(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node) {
    return *((pn_atom_t *) &node->atom);
  } else {
    pn_atom_t t = {PN_NULL, {0,}};
    return t;
  }
}

static int pni_data_copy_nodes(pn_data_t *dst_data, pni_node_t *dst_node,
                               pn_data_t *src_data, pni_node_t *src_node,
                               int limit)
{
  int level = 0;
  int count = 0;
  pni_nid_t next;

  while (true) {
    assert(src_node);
    assert(dst_node);

    dst_node->atom = src_node->atom;
    dst_node->array_described = src_node->array_described;
    dst_node->array_type = src_node->array_type;

    pn_type_t type = src_node->atom.type;

    if (type == PN_STRING || type == PN_SYMBOL || type == PN_BINARY) {
      int err = pni_data_intern_node(dst_data, dst_node);
      if (err) return err;
    }

    if (src_node->down) {
      pni_data_enter(dst_data);
      level++;

      next = src_node->down;
    } else if (src_node->next) {
      if (level == 0) count++;

      next = src_node->next;
    } else if (src_node->parent) {
      src_node = pni_data_node(src_data, src_node->parent);

      while (level > 0) {
        pni_data_exit(dst_data);
        level--;

        if (src_node->next) {
          if (level == 0) count++;
          next = src_node->next;
          goto outer;
        }

        if (src_node->parent) {
          src_node = pni_data_node(src_data, src_node->parent);
        }
      }

      break;
    } else {
      break;
    }

  outer:

    assert(next);

    if (count == limit) break;

    src_node = pni_data_node(src_data, next);
    dst_node = pni_data_add_node(dst_data);

    if (!dst_node) return PN_OUT_OF_MEMORY;
  }

  return 0;
}

PNI_INLINE int pn_data_appendn(pn_data_t *dst_data, pn_data_t *src_data, int limit)
{
  pni_node_t *src_node = pni_data_first_node(src_data);

  if (src_node) {
    pni_node_t *dst_node = pni_data_add_node(dst_data);

    if (!dst_node) return PN_OUT_OF_MEMORY;

    return pni_data_copy_nodes(dst_data, dst_node, src_data, src_node, limit);
  }

  return 0;
}

PNI_INLINE int pn_data_append(pn_data_t *dst_data, pn_data_t *src_data)
{
  return pn_data_appendn(dst_data, src_data, -1);
}

PNI_INLINE int pn_data_copy(pn_data_t *dst_data, pn_data_t *src_data)
{
  pni_data_clear(dst_data);
  int err = pn_data_appendn(dst_data, src_data, -1);
  pn_data_rewind(dst_data);
  return err;
}
