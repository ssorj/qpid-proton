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

#include <proton/error.h>
#include <proton/object.h>
#include <proton/codec.h>
#include "encodings.h"
#include "encoder.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "data.h"

static int pni_encoder_encode_current_node(pn_encoder_t *encoder, pn_data_t *data);
static int pni_encoder_encode_current_node_in_array(pn_encoder_t *encoder, pn_data_t *data, uint8_t code);

static pn_error_t *pni_encoder_error(pn_encoder_t *encoder)
{
  if (!encoder->error) encoder->error = pn_error();
  return encoder->error;
}

void pn_encoder_initialize(pn_encoder_t *encoder)
{
  encoder->output = NULL;
  encoder->position = NULL;
  encoder->error = NULL;
  encoder->size = 0;
  encoder->null_count = 0;
}

void pn_encoder_finalize(pn_encoder_t *encoder)
{
  pn_error_free(encoder->error);
}

static uint8_t pn_type2code(pn_encoder_t *encoder, pn_type_t type)
{
  switch (type)
  {
  case PN_NULL: return PNE_NULL;
  case PN_BOOL: return PNE_BOOLEAN;
  case PN_UBYTE: return PNE_UBYTE;
  case PN_BYTE: return PNE_BYTE;
  case PN_USHORT: return PNE_USHORT;
  case PN_SHORT: return PNE_SHORT;
  case PN_UINT: return PNE_UINT;
  case PN_INT: return PNE_INT;
  case PN_CHAR: return PNE_UTF32;
  case PN_FLOAT: return PNE_FLOAT;
  case PN_LONG: return PNE_LONG;
  case PN_TIMESTAMP: return PNE_MS64;
  case PN_DOUBLE: return PNE_DOUBLE;
  case PN_DECIMAL32: return PNE_DECIMAL32;
  case PN_DECIMAL64: return PNE_DECIMAL64;
  case PN_DECIMAL128: return PNE_DECIMAL128;
  case PN_UUID: return PNE_UUID;
  case PN_ULONG: return PNE_ULONG;
  case PN_BINARY: return PNE_VBIN32;
  case PN_STRING: return PNE_STR32_UTF8;
  case PN_SYMBOL: return PNE_SYM32;
  case PN_LIST: return PNE_LIST32;
  case PN_ARRAY: return PNE_ARRAY32;
  case PN_MAP: return PNE_MAP32;
  case PN_DESCRIBED: return PNE_DESCRIPTOR;
  default:
    return pn_error_format(pni_encoder_error(encoder), PN_ERR, "not a value type: %u\n", type);
  }
}

PN_FORCE_INLINE static inline uint8_t pn_node2code(pn_encoder_t *encoder, pni_node_t *node)
{
  switch (node->atom.type) {
  case PN_LONG:
    if (-128 <= node->atom.u.as_long && node->atom.u.as_long <= 127) {
      return PNE_SMALLLONG;
    } else {
      return PNE_LONG;
    }
  case PN_INT:
    if (-128 <= node->atom.u.as_int && node->atom.u.as_int <= 127) {
      return PNE_SMALLINT;
    } else {
      return PNE_INT;
    }
  case PN_ULONG:
    if (node->atom.u.as_ulong == 0) {
      return PNE_ULONG0;
    } else if (node->atom.u.as_ulong < 256) {
      return PNE_SMALLULONG;
    } else {
      return PNE_ULONG;
    }
  case PN_UINT:
    if (node->atom.u.as_uint == 0) {
      return PNE_UINT0;
    } else if (node->atom.u.as_uint < 256) {
      return PNE_SMALLUINT;
    } else {
      return PNE_UINT;
    }
  case PN_BOOL:
    if (node->atom.u.as_bool) {
      return PNE_TRUE;
    } else {
      return PNE_FALSE;
    }
  case PN_STRING:
    if (node->atom.u.as_bytes.size < 256) {
      return PNE_STR8_UTF8;
    } else {
      return PNE_STR32_UTF8;
    }
  case PN_SYMBOL:
    if (node->atom.u.as_bytes.size < 256) {
      return PNE_SYM8;
    } else {
      return PNE_SYM32;
    }
  case PN_BINARY:
    if (node->atom.u.as_bytes.size < 256) {
      return PNE_VBIN8;
    } else {
      return PNE_VBIN32;
    }
  case PN_LIST:
    if (node->children == 0) {
      return PNE_LIST0;
    } else if (node->children < 256) {
      return PNE_LIST8;
    } else {
      return PNE_LIST32;
    }
  case PN_MAP:
    if (node->children < 256) {
      return PNE_MAP8;
    } else {
      return PNE_MAP32;
    }
  case PN_NULL: return PNE_NULL;
  case PN_UBYTE: return PNE_UBYTE;
  case PN_BYTE: return PNE_BYTE;
  case PN_USHORT: return PNE_USHORT;
  case PN_SHORT: return PNE_SHORT;
  case PN_CHAR: return PNE_UTF32;
  case PN_FLOAT: return PNE_FLOAT;
  case PN_TIMESTAMP: return PNE_MS64;
  case PN_DOUBLE: return PNE_DOUBLE;
  case PN_DECIMAL32: return PNE_DECIMAL32;
  case PN_DECIMAL64: return PNE_DECIMAL64;
  case PN_DECIMAL128: return PNE_DECIMAL128;
  case PN_UUID: return PNE_UUID;
  case PN_ARRAY: return PNE_ARRAY32;
  case PN_DESCRIBED: return PNE_DESCRIPTOR;
  default:
    return pn_error_format(pni_encoder_error(encoder), PN_ERR, "not a value type: %u\n", node->atom.type);
  }
}

// static inline size_t pn_encoder_remaining(pn_encoder_t *encoder) {
//   char *end = encoder->output + encoder->size;

//   if (end > encoder->position) {
//     return end - encoder->position;
//   } else {
//     return 0;
//   }
// }

// XXX Consider indexes instead of pointer math
static inline size_t pn_encoder_remaining(pn_encoder_t *encoder) {
  return (encoder->output + encoder->size) - encoder->position;
}

static inline void pn_encoder_writef8(pn_encoder_t *encoder, uint8_t value)
{
  assert(pn_encoder_remaining(encoder) >= 1);

  *encoder->position++ = value;
}

static inline void pn_encoder_writef16(pn_encoder_t *encoder, uint16_t value)
{
  assert(pn_encoder_remaining(encoder) >= 2);

  encoder->position[0] = 0xFF & (value >> 8);
  encoder->position[1] = 0xFF & (value     );
  encoder->position += 2;
}

static inline void pn_encoder_writef32(pn_encoder_t *encoder, uint32_t value)
{
  assert(pn_encoder_remaining(encoder) >= 4);

  encoder->position[0] = 0xFF & (value >> 24);
  encoder->position[1] = 0xFF & (value >> 16);
  encoder->position[2] = 0xFF & (value >>  8);
  encoder->position[3] = 0xFF & (value      );
  encoder->position += 4;
}

static inline void pn_encoder_writef64(pn_encoder_t *encoder, uint64_t value) {
  assert(pn_encoder_remaining(encoder) >= 8);

  encoder->position[0] = 0xFF & (value >> 56);
  encoder->position[1] = 0xFF & (value >> 48);
  encoder->position[2] = 0xFF & (value >> 40);
  encoder->position[3] = 0xFF & (value >> 32);
  encoder->position[4] = 0xFF & (value >> 24);
  encoder->position[5] = 0xFF & (value >> 16);
  encoder->position[6] = 0xFF & (value >>  8);
  encoder->position[7] = 0xFF & (value      );
  encoder->position += 8;
}

static inline void pn_encoder_writef128(pn_encoder_t *encoder, char *value) {
  assert(pn_encoder_remaining(encoder) >= 16);

  memcpy(encoder->position, value, 16);
  encoder->position += 16;
}

static inline void pn_encoder_writev8(pn_encoder_t *encoder, const pn_bytes_t value)
{
  assert(pn_encoder_remaining(encoder) >= value.size + 1);

  pn_encoder_writef8(encoder, value.size);
  memcpy(encoder->position, value.start, value.size);
  encoder->position += value.size;
}

static inline void pn_encoder_writev32(pn_encoder_t *encoder, const pn_bytes_t value)
{
  assert(pn_encoder_remaining(encoder) >= value.size + 4);

  pn_encoder_writef32(encoder, value.size);
  memcpy(encoder->position, value.start, value.size);
  encoder->position += value.size;
}

typedef union {
  uint32_t i;
  uint32_t a[2];
  uint64_t l;
  float f;
  double d;
} conv_t;

static inline int pni_encoder_encode_fixed8(pn_encoder_t *encoder, pn_atom_t *atom)
{
  if (pn_encoder_remaining(encoder) < 1) return PN_OVERFLOW;

  pn_encoder_writef8(encoder, atom->u.as_ubyte);

  return 0;
}

static int pni_encoder_encode_fixed16(pn_encoder_t *encoder, pn_atom_t *atom)
{
  if (pn_encoder_remaining(encoder) < 2) return PN_OVERFLOW;

  pn_encoder_writef16(encoder, atom->u.as_ushort);

  return 0;
}

static int pni_encoder_encode_fixed32(pn_encoder_t *encoder, pn_atom_t *atom, uint8_t code)
{
  if (pn_encoder_remaining(encoder) < 4) return PN_OVERFLOW;

  if (code == PNE_FLOAT) {
    conv_t conv = { .f = atom->u.as_float };
    pn_encoder_writef32(encoder, conv.i);
  } else {
    pn_encoder_writef32(encoder, atom->u.as_uint);
  }

  return 0;
}

static int pni_encoder_encode_fixed64(pn_encoder_t *encoder, pn_atom_t *atom, uint8_t code)
{
  if (pn_encoder_remaining(encoder) < 8) return PN_OVERFLOW;

  if (code == PNE_DOUBLE) {
    conv_t conv = { .d = atom->u.as_double };
    pn_encoder_writef64(encoder, conv.l);
  } else {
    pn_encoder_writef64(encoder, atom->u.as_ulong);
  }

  return 0;
}

static int pni_encoder_encode_fixed128(pn_encoder_t *encoder, pn_atom_t *atom, uint8_t code)
{
  if (pn_encoder_remaining(encoder) < 16) return PN_OVERFLOW;

  if (code == PNE_UUID) {
    pn_encoder_writef128(encoder, atom->u.as_uuid.bytes);
  } else if (code == PNE_DECIMAL128) {
    pn_encoder_writef128(encoder, atom->u.as_decimal128.bytes);
  }

  return 0;
}

static inline int pni_encoder_encode_variable8(pn_encoder_t *encoder, pn_atom_t *atom)
{
  // XXX Assert string length

  pn_bytes_t value = atom->u.as_bytes;

  if (pn_encoder_remaining(encoder) < value.size + 1) return PN_OVERFLOW;

  pn_encoder_writev8(encoder, value);

  return 0;
}

static int pni_encoder_encode_variable32(pn_encoder_t *encoder, pn_atom_t *atom)
{
  // XXX Assert string length

  pn_bytes_t value = atom->u.as_bytes; // XXX Pointer?

  if (pn_encoder_remaining(encoder) < value.size + 4) return PN_OVERFLOW;

  pn_encoder_writev32(encoder, value);

  return 0;
}

static inline pni_node_t *pni_data_current_node(pn_data_t *data)
{
  assert(data->current);
  return data->nodes + data->current - 1;
}

static inline int pni_encoder_encode_compound8(pn_encoder_t *encoder, pn_data_t *data)
{
  pni_node_t *node = pni_data_current_node(data);
  size_t count = node->children;
  char *start = encoder->position;
  int err;

  if (pn_encoder_remaining(encoder) < 2) return PN_OVERFLOW;

  encoder->position += 1; // The size is backfilled after writing the elements
  pn_encoder_writef8(encoder, node->children);

  data->parent = data->current;
  data->current = node->down;

  for (size_t i = 0; i < count; i++) {
    err = pni_encoder_encode_current_node(encoder, data);
    if (err) return err;

    data->current = pni_data_current_node(data)->next;
  }

  data->current = data->parent;
  data->parent = node->parent;

  char *pos = encoder->position;
  encoder->position = start;

  pn_encoder_writef8(encoder, pos - start - 1);

  encoder->position = pos;

  return 0;
}

static int pni_encoder_encode_compound32(pn_encoder_t *encoder, pn_data_t *data)
{
  pni_node_t *node = pni_data_current_node(data);
  size_t count = node->children;
  char *start = encoder->position;
  int err;

  if (pn_encoder_remaining(encoder) < 8) return PN_OVERFLOW;

  encoder->position += 4; // The size is backfilled after writing the elements
  pn_encoder_writef32(encoder, node->children);

  data->parent = data->current;
  data->current = node->down;

  for (size_t i = 0; i < count; i++) {
    err = pni_encoder_encode_current_node(encoder, data);
    if (err) return err;

    data->current = pni_data_current_node(data)->next;
  }

  data->current = data->parent;
  data->parent = node->parent;

  char *pos = encoder->position;
  encoder->position = start;

  pn_encoder_writef32(encoder, pos - start - 4);

  encoder->position = pos;

  return 0;
}

static int pni_encoder_encode_array32(pn_encoder_t *encoder, pn_data_t *data)
{
  pni_node_t *node = pni_data_current_node(data);
  uint8_t array_code = pn_type2code(encoder, node->type);
  size_t count = node->children;
  int err;

  char *start = encoder->position;

  if (node->described) {
    if (pn_encoder_remaining(encoder) < 9) return PN_OVERFLOW;

    encoder->position += 4; // The size is backfilled after writing the elements
    pn_encoder_writef32(encoder, node->children - 1);
    pn_encoder_writef8(encoder, 0);
  } else {
    if (pn_encoder_remaining(encoder) < 5) return PN_OVERFLOW;

    encoder->position += 4; // The size is backfilled after writing the elements
    pn_encoder_writef32(encoder, node->children);
  }

  // XXX Need to handle described values here? Yes.
  if (pn_encoder_remaining(encoder) < 1) return PN_OVERFLOW;
  pn_encoder_writef8(encoder, array_code);

  data->parent = data->current;
  data->current = node->down;

  for (size_t i = 0; i < count; i++) {
    err = pni_encoder_encode_current_node_in_array(encoder, data, array_code);
    if (err) return err;

    data->current = pni_data_current_node(data)->next;
  }

  data->current = data->parent;
  data->parent = node->parent;

  char *pos = encoder->position;
  encoder->position = start;

  pn_encoder_writef32(encoder, (size_t) (pos - start - 4));

  encoder->position = pos;

  return 0;
}

static inline int pni_encoder_encode_described(pn_encoder_t *encoder, pn_data_t *data)
{
  pni_node_t *node = pni_data_current_node(data);
  int err;

  data->parent = data->current;
  data->current = node->down;

  err = pni_encoder_encode_current_node(encoder, data);
  if (err) return err;

  data->current = pni_data_current_node(data)->next;

  err = pni_encoder_encode_current_node(encoder, data);
  if (err) return err;

  data->current = data->parent;
  data->parent = node->parent;

  return 0;
}

static int pni_encoder_encode_current_node(pn_encoder_t *encoder, pn_data_t *data)
{
  pni_node_t *node = pni_data_current_node(data);
  uint8_t code = pn_node2code(encoder, node);
  pn_atom_t *atom = &node->atom;

  if (pn_encoder_remaining(encoder) < 1) return PN_OVERFLOW;
  pn_encoder_writef8(encoder, code);

  switch (code & 0xF0) {
  case 0x00: return pni_encoder_encode_described(encoder, data);
  case 0x40: return 0;
  case 0x50: return pni_encoder_encode_fixed8(encoder, atom);
  case 0x60: return pni_encoder_encode_fixed16(encoder, atom);
  case 0x70: return pni_encoder_encode_fixed32(encoder, atom, code);
  case 0x80: return pni_encoder_encode_fixed64(encoder, atom, code);
  case 0x90: return pni_encoder_encode_fixed128(encoder, atom, code);
  case 0xA0: return pni_encoder_encode_variable8(encoder, atom);
  case 0xB0: return pni_encoder_encode_variable32(encoder, atom);
  case 0xC0: return pni_encoder_encode_compound8(encoder, data);
  case 0xD0: return pni_encoder_encode_compound32(encoder, data);
  case 0xE0:
  case 0xF0: return pni_encoder_encode_array32(encoder, data);
  default:   return pn_error_format(pn_data_error(data), PN_ERR, "unrecognized encoding: %u", code);
  }
}

static int pni_encoder_encode_current_node_in_array(pn_encoder_t *encoder, pn_data_t *data, uint8_t code)
{
  pni_node_t *node = pni_data_current_node(data);
  pn_atom_t *atom = &node->atom;

  switch (code & 0xF0) {
  case 0x00:
  case 0x40:
  case 0x50: return pni_encoder_encode_fixed8(encoder, atom);
  case 0x60: return pni_encoder_encode_fixed16(encoder, atom);
  case 0x70: return pni_encoder_encode_fixed32(encoder, atom, code);
  case 0x80: return pni_encoder_encode_fixed64(encoder, atom, code);
  case 0x90: return pni_encoder_encode_fixed128(encoder, atom, code);
  case 0xA0: return pni_encoder_encode_variable8(encoder, atom);
  case 0xB0: return pni_encoder_encode_variable32(encoder, atom);
  case 0xC0: return pni_encoder_encode_compound8(encoder, data);
  case 0xD0: return pni_encoder_encode_compound32(encoder, data);
  case 0xE0:
  case 0xF0: return pni_encoder_encode_array32(encoder, data);
  default:   return pn_error_format(pn_data_error(data), PN_ERR, "unrecognized encoding: %u", code);
  }
}

ssize_t pn_encoder_encode(pn_encoder_t *encoder, pn_data_t *src, char *dst, size_t size)
{
  if (!src->size) return 0;

  encoder->output = dst;
  encoder->position = dst;
  encoder->size = size;

  pn_handle_t save = pn_data_point(src);
  int err;

  pn_data_rewind(src);

  while (pn_data_next(src)) {
    err = pni_encoder_encode_current_node(encoder, src);
    if (err) return err;
  }

  pn_data_restore(src, save);

  size_t encoded = encoder->position - encoder->output;

  if (encoded > size) {
      pn_error_format(pn_data_error(src), PN_OVERFLOW, "not enough space to encode");
      return PN_OVERFLOW;
  }

  return (ssize_t) encoded;
}

#include <stdlib.h>

// XXX Use memory alloc functions in here
ssize_t pn_encoder_size(pn_encoder_t *encoder, pn_data_t *src)
{
  size_t buf_size = 64;
  char *buf = malloc(buf_size);
  int err;

  while (true) {
    err = pn_encoder_encode(encoder, src, buf, buf_size);

    if (err == PN_OVERFLOW) {
      buf_size = buf_size * 2;
      buf = realloc(buf, buf_size);
    } else {
      break;
    }
  }

  if (err) return err;

  size_t size = encoder->position - encoder->output;

  free(buf);

  return size;
}
