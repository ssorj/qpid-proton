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

#include "buffer2.h"
#include "encodings.h"
#define DEFINE_FIELDS
#include "protocol.h"
#include "platform/platform.h"
#include "platform/platform_fmt.h"
#include "util.h"
#include "decoder.h"
#include "encoder.h"
#include "data.h"
#include "logger_private.h"
#include "memory.h"

const char *pn_type_name(pn_type_t type)
{
  switch (type)
  {
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
  default: break;
  }

  return "<UNKNOWN>";
}

// data

static void pn_data_finalize(void *object)
{
  pn_data_t *data = (pn_data_t *) object;
  pni_mem_subdeallocate(pn_class(data), data, data->nodes);
  pni_buffer2_free(data->intern_buf);
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
    return pn_string_addf(str, "@%s[", pn_type_name(node->type));
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

pni_node_t *pni_next_nonnull(pn_data_t *data, pni_node_t *node)
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
        if (!grandfields || pni_next_nonnull(data, node)) {
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

static inline pn_error_t *pni_data_error(pn_data_t *data)
{
  if (data->error == NULL) {
    data->error = pn_error();
  }
  return data->error;
}

pn_data_t *pn_data(size_t capacity)
{
  return pni_data(capacity, true);
}

pn_data_t *pni_data(size_t capacity, bool intern)
{
  static const pn_class_t clazz = PN_CLASS(pn_data);
  pn_data_t *data = (pn_data_t *) pn_class_new(&clazz, sizeof(pn_data_t));
  data->capacity = capacity;
  data->size = 0;
  data->nodes = capacity ? (pni_node_t *) pni_mem_suballocate(&clazz, data, capacity * sizeof(pni_node_t)) : NULL;
  data->intern = intern; // For disabling interning when it's not required
  data->intern_buf = NULL;
  data->parent = 0;
  data->current = 0;
  data->base_parent = 0;
  data->base_current = 0;
  data->error = NULL;
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

PN_FORCE_INLINE size_t pni_data_size(pn_data_t *data)
{
  assert(data);
  return data->size;
}

size_t pn_data_size(pn_data_t *data)
{
  return pni_data_size(data);
}

PN_FORCE_INLINE void pni_data_clear(pn_data_t *data)
{
  assert(data);

  data->size = 0;
  data->parent = 0;
  data->current = 0;
  data->base_parent = 0;
  data->base_current = 0;

  if (data->intern_buf) pni_buffer2_clear(data->intern_buf);
}

void pn_data_clear(pn_data_t *data)
{
  pni_data_clear(data);
}

PN_NO_INLINE static int pni_data_grow(pn_data_t *data)
{
  size_t capacity = data->capacity ? data->capacity : 2;
  if (capacity >= PNI_NID_MAX) return PN_OUT_OF_MEMORY;
  else if (capacity < PNI_NID_MAX/2) capacity *= 2;
  else capacity = PNI_NID_MAX;

  pni_node_t *new_nodes = (pni_node_t *) pni_mem_subreallocate(pn_class(data), data, data->nodes, capacity * sizeof(pni_node_t));
  if (new_nodes == NULL) return PN_OUT_OF_MEMORY;
  data->capacity = capacity;
  data->nodes = new_nodes;
  return 0;
}

static void pni_data_rebase(pn_data_t *data, const char *base)
{
  for (unsigned i = 0; i < data->size; i++) {
    pni_node_t *node = &data->nodes[i];
    if (node->data) {
      pn_bytes_t *bytes = &node->atom.u.as_bytes;
      bytes->start = base + node->data_offset;
    }
  }
}

static int pni_data_intern_node(pn_data_t *data, pni_node_t *node)
{
  assert(node->atom.type == PN_BINARY || node->atom.type == PN_STRING || node->atom.type == PN_SYMBOL);

  pn_bytes_t *bytes = &node->atom.u.as_bytes;

  assert(bytes);

  if (!data->intern_buf) {
    // A heuristic to avoid growing small buffers too much.  Set to
    // size + 1 to allow for zero termination.
    data->intern_buf = pni_buffer2(pn_max(bytes->size + 1, PNI_INTERN_MINSIZE));
    if (!data->intern_buf) return PN_OUT_OF_MEMORY;
  }

  size_t old_capacity = pni_buffer2_capacity(data->intern_buf);
  size_t old_size = pni_buffer2_size(data->intern_buf);

  // XXX
  //
  // This uses append_string for the null termination, but I don't yet
  // know why that null termination is necessary.
  pni_buffer2_append_string(data->intern_buf, bytes->start, bytes->size);

  node->data = true;
  node->data_offset = old_size;
  node->data_size = bytes->size;

  pn_bytes_t interned_bytes = pni_buffer2_bytes(data->intern_buf);

  // Set the atom pointer to the interned string
  bytes->start = interned_bytes.start + old_size;

  if (pni_buffer2_capacity(data->intern_buf) != old_capacity) {
    pni_data_rebase(data, interned_bytes.start);
  }

  return 0;
}

// XXX
//
// The only thing that uses 'M' and this function are some tests.
// Remove this?
/*
   Append src to data after normalizing for "multiple" field encoding.

   AMQP composite field definitions can be declared "multiple", see:

   - http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-types-v1.0-os.html#doc-idp115568
   - http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-types-v1.0-os.html#section-composite-type-representation

   Multiple fields allow redundant encoding of two cases:

   1. empty: null or an empty array.
   2. single-value: direct encoding of value, or array with one element

   For encoding compactness and inter-operability, normalize multiple
   field values to always use null for empty, and direct encoding for
   single value.
*/
static int pni_normalize_multiple(pn_data_t *data, pn_data_t *src) {
  int err = 0;
  pn_handle_t point = pn_data_point(src);
  pn_data_rewind(src);
  pn_data_next(src);
  if (pni_data_type(src) == PN_ARRAY) {
    switch (pn_data_get_array(src)) {
     case 0:                    /* Empty array => null */
      err = pn_data_put_null(data);
      break;
     case 1:          /* Single-element array => encode the element */
      pn_data_enter(src);
      pn_data_narrow(src);
      err = pn_data_appendn(data, src, 1);
      pn_data_widen(src);
      break;
     default:              /* Multi-element array, encode unchanged */
      err = pn_data_appendn(data, src, 1);
      break;
    }
  } else {
    err = pn_data_appendn(data, src, 1); /* Non-array, append the value */
  }
  pn_data_restore(src, point);
  return err;
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
   S: symbol (char*)
   s: string (char*)
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
int pn_data_vfill(pn_data_t *data, const char *fmt, va_list ap)
{
  int err = 0;
  const char *begin = fmt;
  while (*fmt) {
    char code = *(fmt++);
    if (!code) return 0;

    switch (code) {
    case 'n':
      err = pn_data_put_null(data);
      break;
    case 'o':
      err = pn_data_put_bool(data, va_arg(ap, int));
      break;
    case 'B':
      err = pn_data_put_ubyte(data, va_arg(ap, unsigned int));
      break;
    case 'b':
      err = pn_data_put_byte(data, va_arg(ap, int));
      break;
    case 'H':
      err = pn_data_put_ushort(data, va_arg(ap, unsigned int));
      break;
    case 'h':
      err = pn_data_put_short(data, va_arg(ap, int));
      break;
    case 'I':
      err = pn_data_put_uint(data, va_arg(ap, uint32_t));
      break;
    case 'i':
      err = pn_data_put_int(data, va_arg(ap, uint32_t));
      break;
    case 'L':
      err = pn_data_put_ulong(data, va_arg(ap, uint64_t));
      break;
    case 'l':
      err = pn_data_put_long(data, va_arg(ap, int64_t));
      break;
    case 't':
      err = pn_data_put_timestamp(data, va_arg(ap, pn_timestamp_t));
      break;
    case 'f':
      err = pn_data_put_float(data, va_arg(ap, double));
      break;
    case 'd':
      err = pn_data_put_double(data, va_arg(ap, double));
      break;
    case 'Z':                   /* encode binary, must not be NULL */
      {
	// For maximum portability, caller must pass these as two separate args, not a single struct
        size_t size = va_arg(ap, size_t);
        char *start = va_arg(ap, char *);
        err = pn_data_put_binary(data, pn_bytes(size, start));
      }
      break;
    case 'z':                   /* encode binary or null if pointer is NULL */
      {
	// For maximum portability, caller must pass these as two separate args, not a single struct
        size_t size = va_arg(ap, size_t);
        char *start = va_arg(ap, char *);
        if (start) {
          err = pn_data_put_binary(data, pn_bytes(size, start));
        } else {
          err = pn_data_put_null(data);
        }
      }
      break;
    case 'S':                   /* encode symbol or null if NULL */
    case 's':                   /* encode string or null if NULL */
      {
        char *start = va_arg(ap, char *);
        size_t size;
        if (start) {
          size = strlen(start);
          if (code == 'S') {
            err = pn_data_put_string(data, pn_bytes(size, start));
          } else {
            err = pn_data_put_symbol(data, pn_bytes(size, start));
          }
        } else {
          err = pn_data_put_null(data);
        }
      }
      break;
    case 'D':
      /* The next 2 args are the descriptor, value for a described value. */
      err = pn_data_put_described(data);
      pn_data_enter(data);
      break;
    case 'T':                   /* Set type of open array */
      {
        pni_node_t *parent = pn_data_node(data, data->parent);
        if (parent->atom.type == PN_ARRAY) {
          parent->type = (pn_type_t) va_arg(ap, int);
        } else {
          return pn_error_format(pni_data_error(data), PN_ERR, "naked type");
        }
      }
      break;
    case '@':                   /* begin array */
      {
        bool described;
        if (*(fmt + 1) == 'D') {
          fmt++;
          described = true;
        } else {
          described = false;
        }
        err = pn_data_put_array(data, described, (pn_type_t) 0);
        pn_data_enter(data);
      }
      break;
    case '[':                   /* begin list */
      if (fmt < (begin + 2) || *(fmt - 2) != 'T') {
        err = pn_data_put_list(data);
        if (err) return err;
        pn_data_enter(data);
      }
      break;
    case '{':                   /* begin map */
      err = pn_data_put_map(data);
      if (err) return err;
      pn_data_enter(data);
      break;
    case '}':
    case ']':
      if (!pn_data_exit(data))
        return pn_error_format(pni_data_error(data), PN_ERR, "exit failed");
      break;
    case '?':
      if (!va_arg(ap, int)) {
        err = pn_data_put_null(data);
        if (err) return err;
        pn_data_enter(data);
      }
      break;
    case '*':
      {
        int count = va_arg(ap, int);
        void *ptr = va_arg(ap, void *);

        char c = *(fmt++);

        switch (c)
        {
        case 's':
          {
            char **sptr = (char **) ptr;
            for (int i = 0; i < count; i++)
            {
              char *sym = *(sptr++);
              err = pn_data_fill(data, "s", sym);
              if (err) return err;
            }
          }
          break;
        default:
          PN_LOG_DEFAULT(PN_SUBSYSTEM_AMQP, PN_LEVEL_CRITICAL, "unrecognized * code: 0x%.2X '%c'", code, code);
          return PN_ARG_ERR;
        }
      }
      break;
    case 'C':                   /* Append an existing pn_data_t *  */
      {
        pn_data_t *src = va_arg(ap, pn_data_t *);
        if (src && pn_data_size(src) > 0) {
          err = pn_data_appendn(data, src, 1);
          if (err) return err;
        } else {
          err = pn_data_put_null(data);
          if (err) return err;
        }
      }
      break;
     case 'M':
      {
        pn_data_t *src = va_arg(ap, pn_data_t *);
        err = (src && pn_data_size(src) > 0) ?
          pni_normalize_multiple(data, src) : pn_data_put_null(data);
        break;
      }
     default:
      PN_LOG_DEFAULT(PN_SUBSYSTEM_AMQP, PN_LEVEL_CRITICAL, "unrecognized fill code: 0x%.2X '%c'", code, code);
      return PN_ARG_ERR;
    }

    if (err) return err;

    pni_node_t *parent = pn_data_node(data, data->parent);
    pni_node_t *current = pn_data_node(data, data->current);
    while (parent) {
      if (parent->atom.type == PN_DESCRIBED && parent->children == 2) {
        current->described = true;
        pn_data_exit(data);
        current = pn_data_node(data, data->current);
        parent = pn_data_node(data, data->parent);
      } else if (parent->atom.type == PN_NULL && parent->children == 1) {
        pn_data_exit(data);
        current = pn_data_node(data, data->current);
        current->down = 0;
        current->children = 0;
        parent = pn_data_node(data, data->parent);
      } else {
        break;
      }
    }
  }

  return 0;
}


int pn_data_fill(pn_data_t *data, const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  int err = pn_data_vfill(data, fmt, ap);
  va_end(ap);
  return err;
}

static bool pn_scan_next(pn_data_t *data, pn_type_t *type, bool suspend)
{
  if (suspend) return false;
  bool found = pn_data_next(data);
  if (found) {
    *type = pni_data_type(data);
    return true;
  } else {
    pni_node_t *parent = pn_data_node(data, data->parent);
    if (parent && parent->atom.type == PN_DESCRIBED) {
      pn_data_exit(data);
      return pn_scan_next(data, type, suspend);
    } else {
      *type = PN_INVALID;
      return false;
    }
  }
}

static pni_node_t *pni_data_peek(pn_data_t *data);

int pn_data_vscan(pn_data_t *data, const char *fmt, va_list ap)
{
  pn_data_rewind(data);
  bool *scanarg = NULL;
  bool at = false;
  int level = 0;
  int count_level = -1;
  int resume_count = 0;

  while (*fmt) {
    char code = *(fmt++);

    bool found = false;
    pn_type_t type;

    bool scanned = false;
    bool suspend = resume_count > 0;

    switch (code) {
    case 'n':
      found = pn_scan_next(data, &type, suspend);
      if (found && type == PN_NULL) {
        scanned = true;
      } else {
        scanned = false;
      }
      if (resume_count && level == count_level) resume_count--;
      break;
    case 'o':
      {
        bool *value = va_arg(ap, bool *);
        found = pn_scan_next(data, &type, suspend);
        if (found && type == PN_BOOL) {
          *value = pn_data_get_bool(data);
          scanned = true;
        } else {
          *value = 0;
          scanned = false;
        }
      }
      if (resume_count && level == count_level) resume_count--;
      break;
    case 'B':
      {
        uint8_t *value = va_arg(ap, uint8_t *);
        found = pn_scan_next(data, &type, suspend);
        if (found && type == PN_UBYTE) {
          *value = pn_data_get_ubyte(data);
          scanned = true;
        } else {
          *value = 0;
          scanned = false;
        }
      }
      if (resume_count && level == count_level) resume_count--;
      break;
    case 'b':
      {
        int8_t *value = va_arg(ap, int8_t *);
        found = pn_scan_next(data, &type, suspend);
        if (found && type == PN_BYTE) {
          *value = pn_data_get_byte(data);
          scanned = true;
        } else {
          *value = 0;
          scanned = false;
        }
      }
      if (resume_count && level == count_level) resume_count--;
      break;
    case 'H':
      {
        uint16_t *value = va_arg(ap, uint16_t *);
        found = pn_scan_next(data, &type, suspend);
        if (found && type == PN_USHORT) {
          *value = pn_data_get_ushort(data);
          scanned = true;
        } else {
          *value = 0;
          scanned = false;
        }
      }
      if (resume_count && level == count_level) resume_count--;
      break;
    case 'h':
      {
        int16_t *value = va_arg(ap, int16_t *);
        found = pn_scan_next(data, &type, suspend);
        if (found && type == PN_SHORT) {
          *value = pn_data_get_short(data);
          scanned = true;
        } else {
          *value = 0;
          scanned = false;
        }
      }
      if (resume_count && level == count_level) resume_count--;
      break;
    case 'I':
      {
        uint32_t *value = va_arg(ap, uint32_t *);
        found = pn_scan_next(data, &type, suspend);
        if (found && type == PN_UINT) {
          *value = pn_data_get_uint(data);
          scanned = true;
        } else {
          *value = 0;
          scanned = false;
        }
      }
      if (resume_count && level == count_level) resume_count--;
      break;
    case 'i':
      {
        int32_t *value = va_arg(ap, int32_t *);
        found = pn_scan_next(data, &type, suspend);
        if (found && type == PN_INT) {
          *value = pn_data_get_int(data);
          scanned = true;
        } else {
          *value = 0;
          scanned = false;
        }
      }
      if (resume_count && level == count_level) resume_count--;
      break;
    case 'c':
      {
        pn_char_t *value = va_arg(ap, pn_char_t *);
        found = pn_scan_next(data, &type, suspend);
        if (found && type == PN_CHAR) {
          *value = pn_data_get_char(data);
          scanned = true;
        } else {
          *value = 0;
          scanned = false;
        }
      }
      if (resume_count && level == count_level) resume_count--;
      break;
    case 'L':
      {
        uint64_t *value = va_arg(ap, uint64_t *);
        found = pn_scan_next(data, &type, suspend);
        if (found && type == PN_ULONG) {
          *value = pn_data_get_ulong(data);
          scanned = true;
        } else {
          *value = 0;
          scanned = false;
        }
      }
      if (resume_count && level == count_level) resume_count--;
      break;
    case 'l':
      {
        int64_t *value = va_arg(ap, int64_t *);
        found = pn_scan_next(data, &type, suspend);
        if (found && type == PN_LONG) {
          *value = pn_data_get_long(data);
          scanned = true;
        } else {
          *value = 0;
          scanned = false;
        }
      }
      if (resume_count && level == count_level) resume_count--;
      break;
    case 't':
      {
        pn_timestamp_t *value = va_arg(ap, pn_timestamp_t *);
        found = pn_scan_next(data, &type, suspend);
        if (found && type == PN_TIMESTAMP) {
          *value = pn_data_get_timestamp(data);
          scanned = true;
        } else {
          *value = 0;
          scanned = false;
        }
      }
      if (resume_count && level == count_level) resume_count--;
      break;
    case 'f':
      {
        float *value = va_arg(ap, float *);
        found = pn_scan_next(data, &type, suspend);
        if (found && type == PN_FLOAT) {
          *value = pn_data_get_float(data);
          scanned = true;
        } else {
          *value = 0;
          scanned = false;
        }
      }
      if (resume_count && level == count_level) resume_count--;
      break;
    case 'd':
      {
        double *value = va_arg(ap, double *);
        found = pn_scan_next(data, &type, suspend);
        if (found && type == PN_DOUBLE) {
          *value = pn_data_get_double(data);
          scanned = true;
        } else {
          *value = 0;
          scanned = false;
        }
      }
      if (resume_count && level == count_level) resume_count--;
      break;
    case 'z':
      {
        pn_bytes_t *bytes = va_arg(ap, pn_bytes_t *);
        found = pn_scan_next(data, &type, suspend);
        if (found && type == PN_BINARY) {
          *bytes = pn_data_get_binary(data);
          scanned = true;
        } else {
          bytes->start = 0;
          bytes->size = 0;
          scanned = false;
        }
      }
      if (resume_count && level == count_level) resume_count--;
      break;
    case 'S':
      {
        pn_bytes_t *bytes = va_arg(ap, pn_bytes_t *);
        found = pn_scan_next(data, &type, suspend);
        if (found && type == PN_STRING) {
          *bytes = pn_data_get_string(data);
          scanned = true;
        } else {
          bytes->start = 0;
          bytes->size = 0;
          scanned = false;
        }
      }
      if (resume_count && level == count_level) resume_count--;
      break;
    case 's':
      {
        pn_bytes_t *bytes = va_arg(ap, pn_bytes_t *);
        found = pn_scan_next(data, &type, suspend);
        if (found && type == PN_SYMBOL) {
          *bytes = pn_data_get_symbol(data);
          scanned = true;
        } else {
          bytes->start = 0;
          bytes->size = 0;
          scanned = false;
        }
      }
      if (resume_count && level == count_level) resume_count--;
      break;
    case 'D':
      found = pn_scan_next(data, &type, suspend);
      if (found && type == PN_DESCRIBED) {
        pn_data_enter(data);
        scanned = true;
      } else {
        if (!suspend) {
          resume_count = 3;
          count_level = level;
        }
        scanned = false;
      }
      if (resume_count && level == count_level) resume_count--;
      break;
    case '@':
      found = pn_scan_next(data, &type, suspend);
      if (found && type == PN_ARRAY) {
        pn_data_enter(data);
        scanned = true;
        at = true;
      } else {
        if (!suspend) {
          resume_count = 3;
          count_level = level;
        }
        scanned = false;
      }
      if (resume_count && level == count_level) resume_count--;
      break;
    case '[':
      if (at) {
        scanned = true;
        at = false;
      } else {
        found = pn_scan_next(data, &type, suspend);
        if (found && type == PN_LIST) {
          pn_data_enter(data);
          scanned = true;
        } else {
          if (!suspend) {
            resume_count = 1;
            count_level = level;
          }
          scanned = false;
        }
      }
      level++;
      break;
    case '{':
      found = pn_scan_next(data, &type, suspend);
      if (found && type == PN_MAP) {
        pn_data_enter(data);
        scanned = true;
      } else {
        if (resume_count) {
          resume_count = 1;
          count_level = level;
        }
        scanned = false;
      }
      level++;
      break;
    case ']':
    case '}':
      level--;
      if (!suspend && !pn_data_exit(data))
        return pn_error_format(pni_data_error(data), PN_ERR, "exit failed");
      if (resume_count && level == count_level) resume_count--;
      break;
    case '.':
      found = pn_scan_next(data, &type, suspend);
      scanned = found;
      if (resume_count && level == count_level) resume_count--;
      break;
    case '?':
      if (!*fmt || *fmt == '?')
        return pn_error_format(pni_data_error(data), PN_ARG_ERR, "codes must follow a ?");
      scanarg = va_arg(ap, bool *);
      break;
    case 'C':
      {
        pn_data_t *dst = va_arg(ap, pn_data_t *);
        if (!suspend) {
          size_t old = pn_data_size(dst);
          pni_node_t *next = pni_data_peek(data);
          if (next && next->atom.type != PN_NULL) {
            pn_data_narrow(data);
            int err = pn_data_appendn(dst, data, 1);
            pn_data_widen(data);
            if (err) return err;
            scanned = pn_data_size(dst) > old;
          } else {
            scanned = false;
          }
          pn_data_next(data);
        } else {
          scanned = false;
        }
      }
      if (resume_count && level == count_level) resume_count--;
      break;
    default:
      return pn_error_format(pni_data_error(data), PN_ARG_ERR, "unrecognized scan code: 0x%.2X '%c'", code, code);
    }

    if (scanarg && code != '?') {
      *scanarg = scanned;
      scanarg = NULL;
    }
  }

  return 0;
}

int pn_data_scan(pn_data_t *data, const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  int err = pn_data_vscan(data, fmt, ap);
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

PN_FORCE_INLINE void pni_data_rewind(pn_data_t *data)
{
  data->parent = data->base_parent;
  data->current = data->base_current;
}

void pn_data_rewind(pn_data_t *data)
{
  pni_data_rewind(data);
}

static inline pni_node_t *pni_data_current(pn_data_t *data)
{
  assert(data);

  if (!data->current) return NULL;

  return data->nodes + data->current - 1;
}

PN_FORCE_INLINE void pni_data_narrow(pn_data_t *data)
{
  data->base_parent = data->parent;
  data->base_current = data->current;
}

void pn_data_narrow(pn_data_t *data)
{
  pni_data_narrow(data);
}

PN_FORCE_INLINE void pni_data_widen(pn_data_t *data)
{
  data->base_parent = 0;
  data->base_current = 0;
}

void pn_data_widen(pn_data_t *data)
{
  pni_data_widen(data);
}

PN_INLINE pn_handle_t pn_data_point(pn_data_t *data)
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

static pni_node_t *pni_data_peek(pn_data_t *data)
{
  if (data->current) {
    pni_node_t *current = pni_data_node(data, data->current);
    return pn_data_node(data, current->next);
  } else if (data->parent) {
    pni_node_t *parent = pni_data_node(data, data->parent);
    return pn_data_node(data, parent->down);
  // This doesn't handle the no-parent initial state
  } else {
    return NULL;
  }
}

PN_FORCE_INLINE bool pni_data_next(pn_data_t *data)
{
  if (data->current) {
    pni_node_t *current = data->nodes + data->current - 1;

    if (current->next) {
      data->current = current->next;
      return true;
    }
  } else if (data->parent) {
    pni_node_t *parent = data->nodes + data->parent - 1;

    if (parent->down) {
      data->current = parent->down;
      return true;
    }
  } else if (data->size) {
    data->current = 1;
    return true;
  }

  return false;
}

bool pn_data_next(pn_data_t *data)
{
  return pni_data_next(data);
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

int pni_data_traverse(pn_data_t *data,
                      int (*enter)(void *ctx, pn_data_t *data, pni_node_t *node),
                      int (*exit)(void *ctx, pn_data_t *data, pni_node_t *node),
                      void *ctx)
{
  pni_node_t *node = data->size ? pn_data_node(data, 1) : NULL;
  int err;

  while (node) {
    err = enter(ctx, data, node);
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
      pni_node_t *parent = pn_data_node(data, node->parent);
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

PN_FORCE_INLINE pn_type_t pni_data_type(pn_data_t *data)
{
  assert(data);
  assert(data->current);

  pni_node_t *node = pni_data_node(data, data->current);

  return node->atom.type;
}

pn_type_t pn_data_type(pn_data_t *data)
{
  if (data->current) {
    return pni_data_type(data);
  } else {
    return PN_INVALID;
  }
}

PN_FORCE_INLINE pni_node_t *pni_data_node(pn_data_t *data, pni_nid_t node_id)
{
  assert(data);
  assert(node_id);

  return data->nodes + node_id - 1;
}

pni_node_t *pn_data_node(pn_data_t *data, pni_nid_t node_id)
{
  if (node_id) {
    return pni_data_node(data, node_id);
  } else {
    return NULL;
  }
}

PN_FORCE_INLINE bool pni_data_enter(pn_data_t *data)
{
  if (data->current) {
    data->parent = data->current;
    data->current = 0;
    return true;
  } else {
    return false;
  }
}

bool pn_data_enter(pn_data_t *data)
{
  return pni_data_enter(data);
}

PN_FORCE_INLINE bool pni_data_exit(pn_data_t *data)
{
  if (data->parent) {
    pni_node_t *parent = pn_data_node(data, data->parent);
    data->current = data->parent;
    data->parent = parent->parent;
    return true;
  } else {
    return false;
  }
}

bool pn_data_exit(pn_data_t *data)
{
  return pni_data_exit(data);
}

bool pn_data_lookup(pn_data_t *data, const char *name)
{
  while (pn_data_next(data)) {
    pn_type_t type = pni_data_type(data);

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

// XXX
//
// Inlining and simplifying this is a substantial win.
//
// XXX There's a bug for this (untested) scenario:
// pn_data_prev(msg->data);
// pn_data_next(msg->data);
PN_FORCE_INLINE static pni_node_t *pni_data_add(pn_data_t *data)
{
  if (data->capacity <= data->size) {
    int err = pni_data_grow(data);
    if (err) return NULL;
  }

  pni_node_t *node = data->nodes + data->size++;
  pni_nid_t node_id = node - data->nodes + 1;

  *node = (pni_node_t) {0};

  if (data->current) {
    pni_node_t *current = data->nodes + data->current - 1;

    current->next = node_id;
    node->prev = data->current;
  }

  if (data->parent) {
    pni_node_t *parent = data->nodes + data->parent - 1;

    if (!parent->down) {
      parent->down = node_id;
    }

    node->parent = data->parent;
    parent->children++;
  }

  data->current = node_id;

  return node;
}

ssize_t pn_data_encode(pn_data_t *data, char *bytes, size_t size)
{
  pn_encoder_t encoder;
  pn_encoder_initialize(&encoder);
  ssize_t r = pn_encoder_encode(&encoder, data, bytes, size);
  pn_encoder_finalize(&encoder);
  return r;
}

ssize_t pn_data_encoded_size(pn_data_t *data)
{
  pn_encoder_t encoder;
  pn_encoder_initialize(&encoder);
  ssize_t r = pn_encoder_size(&encoder, data);
  pn_encoder_finalize(&encoder);
  return r;
}

ssize_t pn_data_decode(pn_data_t *data, const char *bytes, size_t size)
{
  pn_decoder_t decoder;
  pn_decoder_initialize(&decoder);
  ssize_t r = pn_decoder_decode(&decoder, bytes, size, data);
  pn_decoder_finalize(&decoder);
  return r;
}

PN_FORCE_INLINE int pni_data_put_compound(pn_data_t *data, pn_type_t type)
{
  pni_node_t *node = pni_data_add(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  node->atom.type = type;
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
  pni_node_t *node = pni_data_add(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  node->atom.type = PN_ARRAY;
  node->described = described;
  node->type = type;
  return 0;
}

void pni_data_set_array_type(pn_data_t *data, pn_type_t type)
{
  pni_node_t *array = pni_data_current(data);
  if (array) array->type = type;
}

PN_FORCE_INLINE int pni_data_put_described(pn_data_t *data)
{
  pni_node_t *node = pni_data_add(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  node->atom.type = PN_DESCRIBED;
  return 0;
}

int pn_data_put_described(pn_data_t *data)
{
  return pni_data_put_described(data);
}

PN_FORCE_INLINE int pni_data_put_null(pn_data_t *data)
{
  pni_node_t *node = pni_data_add(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  node->atom.type = PN_NULL;
  return 0;
}

int pn_data_put_null(pn_data_t *data)
{
  return pni_data_put_null(data);
}

PN_FORCE_INLINE int pni_data_put_bool(pn_data_t *data, bool b)
{
  pni_node_t *node = pni_data_add(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  node->atom.type = PN_BOOL;
  node->atom.u.as_bool = b;
  return 0;
}

int pn_data_put_bool(pn_data_t *data, bool b)
{
  return pni_data_put_bool(data, b);
}

int pn_data_put_ubyte(pn_data_t *data, uint8_t ub)
{
  return pni_data_put_ubyte(data, ub);
}

PN_FORCE_INLINE int pni_data_put_ubyte(pn_data_t *data, uint8_t ub)
{
  pni_node_t *node = pni_data_add(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  node->atom.type = PN_UBYTE;
  node->atom.u.as_ubyte = ub;
  return 0;
}

int pn_data_put_byte(pn_data_t *data, int8_t b)
{
  pni_node_t *node = pni_data_add(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  node->atom.type = PN_BYTE;
  node->atom.u.as_byte = b;
  return 0;
}

int pn_data_put_ushort(pn_data_t *data, uint16_t us)
{
  pni_node_t *node = pni_data_add(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  node->atom.type = PN_USHORT;
  node->atom.u.as_ushort = us;
  return 0;
}

int pn_data_put_short(pn_data_t *data, int16_t s)
{
  pni_node_t *node = pni_data_add(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  node->atom.type = PN_SHORT;
  node->atom.u.as_short = s;
  return 0;
}

PN_FORCE_INLINE int pni_data_put_uint(pn_data_t *data, uint32_t ui)
{
  pni_node_t *node = pni_data_add(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  node->atom.type = PN_UINT;
  node->atom.u.as_uint = ui;
  return 0;
}

int pn_data_put_uint(pn_data_t *data, uint32_t ui)
{
  return pni_data_put_uint(data, ui);
}

int pn_data_put_int(pn_data_t *data, int32_t i)
{
  pni_node_t *node = pni_data_add(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  node->atom.type = PN_INT;
  node->atom.u.as_int = i;
  return 0;
}

int pn_data_put_char(pn_data_t *data, pn_char_t c)
{
  pni_node_t *node = pni_data_add(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  node->atom.type = PN_CHAR;
  node->atom.u.as_char = c;
  return 0;
}

PN_FORCE_INLINE int pni_data_put_ulong(pn_data_t *data, uint64_t ul)
{
  pni_node_t *node = pni_data_add(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  node->atom.type = PN_ULONG;
  node->atom.u.as_ulong = ul;
  return 0;
}

int pn_data_put_ulong(pn_data_t *data, uint64_t ul)
{
  return pni_data_put_ulong(data, ul);
}

int pn_data_put_long(pn_data_t *data, int64_t l)
{
  pni_node_t *node = pni_data_add(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  node->atom.type = PN_LONG;
  node->atom.u.as_long = l;
  return 0;
}

PN_FORCE_INLINE int pni_data_put_timestamp(pn_data_t *data, pn_timestamp_t t)
{
  pni_node_t *node = pni_data_add(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  node->atom.type = PN_TIMESTAMP;
  node->atom.u.as_timestamp = t;
  return 0;
}

int pn_data_put_timestamp(pn_data_t *data, pn_timestamp_t t)
{
  return pni_data_put_timestamp(data, t);
}

int pn_data_put_float(pn_data_t *data, float f)
{
  pni_node_t *node = pni_data_add(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  node->atom.type = PN_FLOAT;
  node->atom.u.as_float = f;
  return 0;
}

int pn_data_put_double(pn_data_t *data, double d)
{
  pni_node_t *node = pni_data_add(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  node->atom.type = PN_DOUBLE;
  node->atom.u.as_double = d;
  return 0;
}

int pn_data_put_decimal32(pn_data_t *data, pn_decimal32_t d)
{
  pni_node_t *node = pni_data_add(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  node->atom.type = PN_DECIMAL32;
  node->atom.u.as_decimal32 = d;
  return 0;
}

int pn_data_put_decimal64(pn_data_t *data, pn_decimal64_t d)
{
  pni_node_t *node = pni_data_add(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  node->atom.type = PN_DECIMAL64;
  node->atom.u.as_decimal64 = d;
  return 0;
}

int pn_data_put_decimal128(pn_data_t *data, pn_decimal128_t d)
{
  pni_node_t *node = pni_data_add(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  node->atom.type = PN_DECIMAL128;
  memmove(node->atom.u.as_decimal128.bytes, d.bytes, 16);
  return 0;
}

int pn_data_put_uuid(pn_data_t *data, pn_uuid_t u)
{
  pni_node_t *node = pni_data_add(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  node->atom.type = PN_UUID;
  memmove(node->atom.u.as_uuid.bytes, u.bytes, 16);
  return 0;
}

PN_FORCE_INLINE int pni_data_put_variable(pn_data_t *data, pn_bytes_t bytes, pn_type_t type)
{
  pni_node_t *node = pni_data_add(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  node->atom.type = type;
  node->atom.u.as_bytes = bytes;
  if (data->intern) {
    return pni_data_intern_node(data, node);
  } else {
    return 0;
  }
}

int pn_data_put_binary(pn_data_t *data, pn_bytes_t bytes)
{
  return pni_data_put_variable(data, bytes, PN_BINARY);
}

int pn_data_put_string(pn_data_t *data, pn_bytes_t bytes)
{
  return pni_data_put_variable(data, bytes, PN_STRING);
}

int pn_data_put_symbol(pn_data_t *data, pn_bytes_t bytes)
{
  return pni_data_put_variable(data, bytes, PN_SYMBOL);
}

int pn_data_put_atom(pn_data_t *data, pn_atom_t atom)
{
  return pni_data_put_atom(data, atom);
}

PN_FORCE_INLINE int pni_data_put_atom(pn_data_t *data, pn_atom_t atom)
{
  pni_node_t *node = pni_data_add(data);
  if (node == NULL) return PN_OUT_OF_MEMORY;
  node->atom = atom;
  if ((node->atom.type == PN_BINARY || node->atom.type == PN_STRING || node->atom.type == PN_SYMBOL) && data->intern) {
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
    if (node->described) {
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
    return node->described;
  } else {
    return false;
  }
}

pn_type_t pn_data_get_array_type(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_ARRAY) {
    return node->type;
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
    return node->atom.u.as_bool;
  } else {
    return false;
  }
}

uint8_t pn_data_get_ubyte(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_UBYTE) {
    return node->atom.u.as_ubyte;
  } else {
    return 0;
  }
}

int8_t pn_data_get_byte(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_BYTE) {
    return node->atom.u.as_byte;
  } else {
    return 0;
  }
}

uint16_t pn_data_get_ushort(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_USHORT) {
    return node->atom.u.as_ushort;
  } else {
    return 0;
  }
}

int16_t pn_data_get_short(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_SHORT) {
    return node->atom.u.as_short;
  } else {
    return 0;
  }
}

uint32_t pn_data_get_uint(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_UINT) {
    return node->atom.u.as_uint;
  } else {
    return 0;
  }
}

int32_t pn_data_get_int(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_INT) {
    return node->atom.u.as_int;
  } else {
    return 0;
  }
}

pn_char_t pn_data_get_char(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_CHAR) {
    return node->atom.u.as_char;
  } else {
    return 0;
  }
}

uint64_t pn_data_get_ulong(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_ULONG) {
    return node->atom.u.as_ulong;
  } else {
    return 0;
  }
}

int64_t pn_data_get_long(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_LONG) {
    return node->atom.u.as_long;
  } else {
    return 0;
  }
}

pn_timestamp_t pn_data_get_timestamp(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_TIMESTAMP) {
    return node->atom.u.as_timestamp;
  } else {
    return 0;
  }
}

float pn_data_get_float(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_FLOAT) {
    return node->atom.u.as_float;
  } else {
    return 0;
  }
}

double pn_data_get_double(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_DOUBLE) {
    return node->atom.u.as_double;
  } else {
    return 0;
  }
}

pn_decimal32_t pn_data_get_decimal32(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_DECIMAL32) {
    return node->atom.u.as_decimal32;
  } else {
    return 0;
  }
}

pn_decimal64_t pn_data_get_decimal64(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_DECIMAL64) {
    return node->atom.u.as_decimal64;
  } else {
    return 0;
  }
}

pn_decimal128_t pn_data_get_decimal128(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_DECIMAL128) {
    return node->atom.u.as_decimal128;
  } else {
    pn_decimal128_t t = {{0}};
    return t;
  }
}

pn_uuid_t pn_data_get_uuid(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_UUID) {
    return node->atom.u.as_uuid;
  } else {
    pn_uuid_t t = {{0}};
    return t;
  }
}

pn_bytes_t pn_data_get_binary(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_BINARY) {
    return node->atom.u.as_bytes;
  } else {
    pn_bytes_t t = {0};
    return t;
  }
}

pn_bytes_t pn_data_get_string(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_STRING) {
    return node->atom.u.as_bytes;
  } else {
    pn_bytes_t t = {0};
    return t;
  }
}

pn_bytes_t pn_data_get_symbol(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && node->atom.type == PN_SYMBOL) {
    return node->atom.u.as_bytes;
  } else {
    pn_bytes_t t = {0};
    return t;
  }
}

pn_bytes_t pn_data_get_bytes(pn_data_t *data)
{
  pni_node_t *node = pni_data_current(data);
  if (node && (node->atom.type == PN_BINARY ||
               node->atom.type == PN_STRING ||
               node->atom.type == PN_SYMBOL)) {
    return node->atom.u.as_bytes;
  } else {
    pn_bytes_t t = {0};
    return t;
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

PN_FORCE_INLINE int pni_data_copy(pn_data_t *data, pn_data_t *src)
{
  pni_data_clear(data);
  int err = pni_data_appendn(data, src, -1);
  pni_data_rewind(data);
  return err;
}

int pn_data_copy(pn_data_t *data, pn_data_t *src)
{
  return pni_data_copy(data, src);
}

PN_FORCE_INLINE static int pni_data_copy_node(pn_data_t *data, pni_node_t *src) {
  int err = 0;
  pn_type_t type = src->atom.type;
  pni_node_t *dst = pni_data_add(data);

  if (dst == NULL) return PN_OUT_OF_MEMORY;

  dst->atom = src->atom;
  dst->described = src->described;
  dst->type = src->type;

  if ((type == PN_STRING || type == PN_SYMBOL || type == PN_BINARY) && data->intern) {
    err = pni_data_intern_node(data, dst);
  }

  return err;
}

PN_FORCE_INLINE int pni_data_appendn(pn_data_t *data, pn_data_t *src, int limit)
{
  int err = 0;
  int level = 0;
  int count = 0;
  pn_handle_t point = pn_data_point(src);

  pni_data_rewind(src);

  while (true) {
    if (!pni_data_next(src)) {
      if (level > 0) {
        pni_data_exit(data);
        pni_data_exit(src);
        level--;
        continue;
      } else {
        break;
      }
    }

    if (level == 0) {
      if (count == limit) break;
      count++;
    }

    err = pni_data_copy_node(data, pni_data_current(src));
    if (err) break;

    pn_type_t type = pni_data_current(src)->atom.type;

    if (type == PN_DESCRIBED || type == PN_LIST || type == PN_MAP || type == PN_ARRAY) {
      pni_data_enter(data);
      pni_data_enter(src);
      level++;
    }
  }

  pn_data_restore(src, point);

  return err;
}

int pn_data_append(pn_data_t *data, pn_data_t *src)
{
  return pni_data_appendn(data, src, -1);
}

int pn_data_appendn(pn_data_t *data, pn_data_t *src, int limit)
{
  return pni_data_appendn(data, src, limit);
}

int pni_data_copy_current_node(pn_data_t *data, pn_data_t *src) {
  return pni_data_copy_node(data, pni_data_current(src));
}
