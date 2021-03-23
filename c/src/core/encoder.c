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

#include "data.h"
#include "encodings.h"
#include "encoder.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

static int pni_encoder_encode_node(pni_encoder_t *encoder, pn_data_t *data, pni_node_t *node);
static int pni_encoder_encode_type(pni_encoder_t *encoder, uint8_t code);
static int pni_encoder_encode_value(pni_encoder_t *encoder, pn_data_t *data, pni_node_t *node, uint8_t code);

static pn_error_t *pni_encoder_error(pni_encoder_t *encoder)
{
  if (!encoder->error) encoder->error = pn_error();
  return encoder->error;
}

static uint8_t pni_encoder_type2code(pni_encoder_t *encoder, const pn_type_t type, int *error)
{
  switch (type) {
  case PN_NULL:       return PNE_NULL;
  case PN_BOOL:       return PNE_BOOLEAN;
  case PN_UBYTE:      return PNE_UBYTE;
  case PN_BYTE:       return PNE_BYTE;
  case PN_USHORT:     return PNE_USHORT;
  case PN_SHORT:      return PNE_SHORT;
  case PN_UINT:       return PNE_UINT;
  case PN_INT:        return PNE_INT;
  case PN_CHAR:       return PNE_UTF32;
  case PN_FLOAT:      return PNE_FLOAT;
  case PN_LONG:       return PNE_LONG;
  case PN_TIMESTAMP:  return PNE_MS64;
  case PN_DOUBLE:     return PNE_DOUBLE;
  case PN_DECIMAL32:  return PNE_DECIMAL32;
  case PN_DECIMAL64:  return PNE_DECIMAL64;
  case PN_DECIMAL128: return PNE_DECIMAL128;
  case PN_UUID:       return PNE_UUID;
  case PN_ULONG:      return PNE_ULONG;
  case PN_BINARY:     return PNE_VBIN32;
  case PN_STRING:     return PNE_STR32_UTF8;
  case PN_SYMBOL:     return PNE_SYM32;
  case PN_LIST:       return PNE_LIST32;
  case PN_ARRAY:      return PNE_ARRAY32;
  case PN_MAP:        return PNE_MAP32;
  case PN_DESCRIBED:  return PNE_DESCRIPTOR;
  default:
    *error = pn_error_format(pni_encoder_error(encoder), PN_ERR, "not a value type: %u", type);
    return 0;
  }
}

static inline uint8_t pni_encoder_node2code(pni_encoder_t *encoder, pni_node_t *node, int *error)
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
    } else if (node->children < 16) {
      return PNE_LIST8;
    } else {
      return PNE_LIST32;
    }
  case PN_MAP:
    if (node->children < 16) {
      return PNE_MAP8;
    } else {
      return PNE_MAP32;
    }
  case PN_ARRAY:
    if (node->children < 16) {
      return PNE_ARRAY8;
    } else {
      return PNE_ARRAY32;
    }
  case PN_NULL:       return PNE_NULL;
  case PN_UBYTE:      return PNE_UBYTE;
  case PN_BYTE:       return PNE_BYTE;
  case PN_USHORT:     return PNE_USHORT;
  case PN_SHORT:      return PNE_SHORT;
  case PN_CHAR:       return PNE_UTF32;
  case PN_FLOAT:      return PNE_FLOAT;
  case PN_TIMESTAMP:  return PNE_MS64;
  case PN_DOUBLE:     return PNE_DOUBLE;
  case PN_DECIMAL32:  return PNE_DECIMAL32;
  case PN_DECIMAL64:  return PNE_DECIMAL64;
  case PN_DECIMAL128: return PNE_DECIMAL128;
  case PN_UUID:       return PNE_UUID;
  case PN_DESCRIBED:  return PNE_DESCRIPTOR;
  default:
    *error = pn_error_format(pni_encoder_error(encoder), PN_ERR, "not a value type: %u", node->atom.type);
    return 0;
  }
}

static inline size_t pni_encoder_remaining(pni_encoder_t *encoder)
{
  return encoder->output + encoder->size - encoder->position;
}

static inline void pni_encoder_writef8(pni_encoder_t *encoder, const uint8_t value)
{
  assert(pni_encoder_remaining(encoder) >= 1);

  *encoder->position++ = value;
}

static inline void pni_encoder_writef16(pni_encoder_t *encoder, const uint16_t value)
{
  assert(pni_encoder_remaining(encoder) >= 2);

  encoder->position[0] = 0xFF & (value >> 8);
  encoder->position[1] = 0xFF & (value     );
  encoder->position += 2;
}

static inline void pni_encoder_writef32(pni_encoder_t *encoder, const uint32_t value)
{
  assert(pni_encoder_remaining(encoder) >= 4);

  encoder->position[0] = 0xFF & (value >> 24);
  encoder->position[1] = 0xFF & (value >> 16);
  encoder->position[2] = 0xFF & (value >>  8);
  encoder->position[3] = 0xFF & (value      );
  encoder->position += 4;
}

static inline void pni_encoder_writef64(pni_encoder_t *encoder, const uint64_t value) {
  assert(pni_encoder_remaining(encoder) >= 8);

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

static inline void pni_encoder_writef128(pni_encoder_t *encoder, const char *value) {
  assert(pni_encoder_remaining(encoder) >= 16);

  memcpy(encoder->position, value, 16);
  encoder->position += 16;
}

static inline void pni_encoder_writev8(pni_encoder_t *encoder, const pn_bytes_t value)
{
  assert(pni_encoder_remaining(encoder) >= value.size + 1);

  pni_encoder_writef8(encoder, value.size);
  memcpy(encoder->position, value.start, value.size);
  encoder->position += value.size;
}

static inline void pni_encoder_writev32(pni_encoder_t *encoder, const pn_bytes_t value)
{
  assert(pni_encoder_remaining(encoder) >= value.size + 4);

  pni_encoder_writef32(encoder, value.size);
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

static inline int pni_encoder_encode_fixed8(pni_encoder_t *encoder, pni_node_t *node)
{
  if (pni_encoder_remaining(encoder) < 1) return PN_OVERFLOW;

  pni_encoder_writef8(encoder, node->atom.u.as_ubyte);

  return 0;
}

static int pni_encoder_encode_fixed16(pni_encoder_t *encoder, pni_node_t *node)
{
  if (pni_encoder_remaining(encoder) < 2) return PN_OVERFLOW;

  pni_encoder_writef16(encoder, node->atom.u.as_ushort);

  return 0;
}

static int pni_encoder_encode_fixed32(pni_encoder_t *encoder, pni_node_t *node, const uint8_t code)
{
  if (pni_encoder_remaining(encoder) < 4) return PN_OVERFLOW;

  if (code == PNE_FLOAT) {
    conv_t conv = { .f = node->atom.u.as_float };
    pni_encoder_writef32(encoder, conv.i);
  } else {
    pni_encoder_writef32(encoder, node->atom.u.as_uint);
  }

  return 0;
}

static int pni_encoder_encode_fixed64(pni_encoder_t *encoder, pni_node_t *node, const uint8_t code)
{
  if (pni_encoder_remaining(encoder) < 8) return PN_OVERFLOW;

  if (code == PNE_DOUBLE) {
    conv_t conv = { .d = node->atom.u.as_double };
    pni_encoder_writef64(encoder, conv.l);
  } else {
    pni_encoder_writef64(encoder, node->atom.u.as_ulong);
  }

  return 0;
}

static int pni_encoder_encode_fixed128(pni_encoder_t *encoder, pni_node_t *node, const uint8_t code)
{
  if (pni_encoder_remaining(encoder) < 16) return PN_OVERFLOW;

  if (code == PNE_UUID) {
    pni_encoder_writef128(encoder, node->atom.u.as_uuid.bytes);
  } else if (code == PNE_DECIMAL128) {
    pni_encoder_writef128(encoder, node->atom.u.as_decimal128.bytes);
  } else {
    return pn_error_format(pni_encoder_error(encoder), PN_ERR, "unrecognized encoding: %u", code);
  }

  return 0;
}

static inline int pni_encoder_encode_variable8(pni_encoder_t *encoder, pni_node_t *node)
{
  pn_bytes_t value = node->atom.u.as_bytes;

  if (pni_encoder_remaining(encoder) < value.size + 1) return PN_OVERFLOW;

  pni_encoder_writev8(encoder, value);

  return 0;
}

static int pni_encoder_encode_variable32(pni_encoder_t *encoder, pni_node_t *node)
{
  pn_bytes_t value = node->atom.u.as_bytes;

  if (pni_encoder_remaining(encoder) < value.size + 4) return PN_OVERFLOW;

  pni_encoder_writev32(encoder, value);

  return 0;
}

static inline int pni_encoder_encode_compound_values(pni_encoder_t *encoder, pn_data_t *data, pni_node_t *node,
                                                     unsigned *null_count)
{
  data->parent = data->current;
  data->current = node->down;

  for (size_t i = 0; i < node->children; i++) {
    pni_node_t *child = pni_data_node(data, data->current);

    if (child->atom.type == PN_NULL) {
      *null_count += 1;
      data->current = child->next;
      continue;
    } else if (*null_count) {
      if (pni_encoder_remaining(encoder) < *null_count) return PN_OVERFLOW;

      for (size_t i = 0; i < *null_count; i++) {
        pni_encoder_writef8(encoder, PNE_NULL);
      }

      *null_count = 0;
    }

    int err = pni_encoder_encode_node(encoder, data, child);
    if (err) return err;

    data->current = child->next;
  }

  data->current = data->parent;
  data->parent = node->parent;

  return 0;
}

static int pni_encoder_encode_compound32(pni_encoder_t *encoder, pn_data_t *data, pni_node_t *node)
{
  char *start = encoder->position;
  unsigned null_count = 0;

  // The size and count are backfilled after writing the elements
  if (pni_encoder_remaining(encoder) < 8) return PN_OVERFLOW;
  encoder->position += 8;

  int err = pni_encoder_encode_compound_values(encoder, data, node, &null_count);
  if (err) return err;

  char *pos = encoder->position;
  encoder->position = start;

  // Backfill the size and count
  pni_encoder_writef32(encoder, (size_t) (pos - start - 4));
  pni_encoder_writef32(encoder, node->children - null_count);

  encoder->position = pos;

  return 0;
}

static inline int pni_encoder_encode_compound8(pni_encoder_t *encoder, pn_data_t *data, pni_node_t *node)
{
  char *starting_position = encoder->position;
  pni_nid_t starting_current = data->current;
  pni_nid_t starting_parent = data->parent;
  unsigned null_count = 0;

  // The size and count are backfilled after writing the elements
  if (pni_encoder_remaining(encoder) < 2) return PN_OVERFLOW;
  encoder->position += 2;

  int err = pni_encoder_encode_compound_values(encoder, data, node, &null_count);
  if (err) return err;

  char *pos = encoder->position;
  encoder->position = starting_position;

  size_t size = pos - starting_position - 1;

  if (size >= 256) {
    // The encoded size is too large for compound8.  Fall back to
    // compound32 encoding.

    data->current = starting_current;
    data->parent = starting_parent;

    // Rewrite the format code
    encoder->position = encoder->position - 1;
    uint8_t code = (((uint8_t) *encoder->position) & 0x0F) | (0xD0 & 0xF0);
    pni_encoder_writef8(encoder, code);

    return pni_encoder_encode_compound32(encoder, data, node);
  }

  // Backfill the size and count
  pni_encoder_writef8(encoder, size);
  pni_encoder_writef8(encoder, node->children - null_count);

  encoder->position = pos;

  return 0;
}

static inline int pni_encoder_encode_described(pni_encoder_t *encoder, pn_data_t *data, pni_node_t *node)
{
  pni_node_t *child;
  int err;

  data->parent = data->current;
  data->current = node->down;

  child = pni_data_node(data, data->current);
  err = pni_encoder_encode_node(encoder, data, child);
  if (err) return err;

  data->current = child->next;

  child = pni_data_node(data, data->current);
  err = pni_encoder_encode_node(encoder, data, child);
  if (err) return err;

  data->current = data->parent;
  data->parent = node->parent;

  return 0;
}

static inline int pni_encoder_encode_array_values(pni_encoder_t *encoder, pn_data_t *data, pni_node_t *node)
{
  int err = 0;
  pni_node_t *child;

  const uint8_t array_code = pni_encoder_type2code(encoder, node->type, &err);
  if (err) return err;

  data->parent = data->current;
  data->current = node->down;

  if (node->described) {
    child = pni_data_node(data, data->current);

    err = pni_encoder_encode_type(encoder, PNE_DESCRIPTOR);
    if (err) return err;

    err = pni_encoder_encode_node(encoder, data, child);
    if (err) return err;

    data->current = child->next;
  }

  pni_encoder_encode_type(encoder, array_code);

  while (data->current) {
    child = pni_data_node(data, data->current);

    err = pni_encoder_encode_value(encoder, data, child, array_code);
    if (err) return err;

    data->current = child->next;
  }

  data->current = data->parent;
  data->parent = node->parent;

  return 0;
}

static int pni_encoder_encode_array32(pni_encoder_t *encoder, pn_data_t *data, pni_node_t *node)
{
  char *start = encoder->position;

  // The size and count are backfilled after writing the elements
  if (pni_encoder_remaining(encoder) < 8) return PN_OVERFLOW;
  encoder->position += 8;

  int err = pni_encoder_encode_array_values(encoder, data, node);
  if (err) return err;

  char *pos = encoder->position;
  encoder->position = start;

  // Backfill the size and count

  pni_encoder_writef32(encoder, (size_t) (pos - start - 4));

  if (node->described) {
    pni_encoder_writef32(encoder, node->children - 1);
  } else {
    pni_encoder_writef32(encoder, node->children);
  }

  encoder->position = pos;

  return 0;
}

static int pni_encoder_encode_array8(pni_encoder_t *encoder, pn_data_t *data, pni_node_t *node)
{
  char *starting_position = encoder->position;
  pni_nid_t starting_current = data->current;
  pni_nid_t starting_parent = data->parent;

  // The size and count are backfilled after writing the elements
  if (pni_encoder_remaining(encoder) < 2) return PN_OVERFLOW;
  encoder->position += 2;

  int err = pni_encoder_encode_array_values(encoder, data, node);
  if (err) return err;

  char *pos = encoder->position;
  encoder->position = starting_position;

  size_t size = pos - starting_position - 1;

  if (size >= 256) {
    // The encoded size is too large for array8.  Fall back to array32
    // encoding.

    data->current = starting_current;
    data->parent = starting_parent;

    // Rewrite the format code
    encoder->position = encoder->position - 1;
    pni_encoder_writef8(encoder, PNE_ARRAY32);

    return pni_encoder_encode_array32(encoder, data, node);
  }

  // Backfill the size and count

  pni_encoder_writef8(encoder, size);

  if (node->described) {
    pni_encoder_writef8(encoder, node->children - 1);
  } else {
    pni_encoder_writef8(encoder, node->children);
  }

  encoder->position = pos;

  return 0;
}

static inline int pni_encoder_encode_type(pni_encoder_t *encoder, const uint8_t code)
{
  if (pni_encoder_remaining(encoder) < 1) return PN_OVERFLOW;

  pni_encoder_writef8(encoder, code);

  return 0;
}

static int pni_encoder_encode_value(pni_encoder_t *encoder, pn_data_t *data, pni_node_t *node, const uint8_t code)
{
  switch (code & 0xF0) {
  case 0x00: return pni_encoder_encode_described(encoder, data, node);
  case 0x40:
  case 0x50: return pni_encoder_encode_fixed8(encoder, node);
  case 0x60: return pni_encoder_encode_fixed16(encoder, node);
  case 0x70: return pni_encoder_encode_fixed32(encoder, node, code);
  case 0x80: return pni_encoder_encode_fixed64(encoder, node, code);
  case 0x90: return pni_encoder_encode_fixed128(encoder, node, code);
  case 0xA0: return pni_encoder_encode_variable8(encoder, node);
  case 0xB0: return pni_encoder_encode_variable32(encoder, node);
  case 0xC0: return pni_encoder_encode_compound8(encoder, data, node);
  case 0xD0: return pni_encoder_encode_compound32(encoder, data, node);
  case 0xE0: return pni_encoder_encode_array8(encoder, data, node);
  case 0xF0: return pni_encoder_encode_array32(encoder, data, node);
  default:
    return pn_error_format(pni_encoder_error(encoder), PN_ERR, "unrecognized encoding: %u", code);
  }
}

static int pni_encoder_encode_node(pni_encoder_t *encoder, pn_data_t *data, pni_node_t *node)
{
  int err = 0;

  const uint8_t code = pni_encoder_node2code(encoder, node, &err);
  if (err) return err;

  err = pni_encoder_encode_type(encoder, code);
  if (err) return err;

  switch (code & 0xF0) {
  case 0x00: return pni_encoder_encode_described(encoder, data, node);
  case 0x40: return 0;
  case 0x50: return pni_encoder_encode_fixed8(encoder, node);
  case 0x60: return pni_encoder_encode_fixed16(encoder, node);
  case 0x70: return pni_encoder_encode_fixed32(encoder, node, code);
  case 0x80: return pni_encoder_encode_fixed64(encoder, node, code);
  case 0x90: return pni_encoder_encode_fixed128(encoder, node, code);
  case 0xA0: return pni_encoder_encode_variable8(encoder, node);
  case 0xB0: return pni_encoder_encode_variable32(encoder, node);
  case 0xC0: return pni_encoder_encode_compound8(encoder, data, node);
  case 0xD0: return pni_encoder_encode_compound32(encoder, data, node);
  case 0xE0: return pni_encoder_encode_array8(encoder, data, node);
  case 0xF0: return pni_encoder_encode_array32(encoder, data, node);
  default:
    return pn_error_format(pni_encoder_error(encoder), PN_ERR, "unrecognized encoding: %u", code);
  }
}

PNI_INLINE void pni_encoder_initialize(pni_encoder_t *encoder)
{
  *encoder = (pni_encoder_t) {0};
}

PNI_INLINE void pni_encoder_finalize(pni_encoder_t *encoder)
{
  pn_error_free(encoder->error);
}

ssize_t pni_encoder_encode(pni_encoder_t *encoder, pn_data_t *src, char *dst, size_t size)
{
  if (!src->size) return 0;

  encoder->output = dst;
  encoder->position = dst;
  encoder->size = size;

  pn_handle_t save = pn_data_point(src);

  pni_data_rewind(src);

  while (pni_data_next(src)) {
    pni_node_t *node = pni_data_node(src, src->current);
    int err = pni_encoder_encode_node(encoder, src, node);

    if (err) {
      pn_data_restore(src, save);
      return err;
    }
  }

  pn_data_restore(src, save);

  size_t encoded = encoder->position - encoder->output;

  if (encoded > size) {
    return pn_error_format(pni_encoder_error(encoder), PN_OVERFLOW, "not enough space to encode");
  }

  return (ssize_t) encoded;
}

ssize_t pni_encoder_size(pni_encoder_t *encoder, pn_data_t *src)
{
  size_t buf_size = 64;
  char *buf = malloc(buf_size);
  int err;

  while (true) {
    err = pni_encoder_encode(encoder, src, buf, buf_size);

    if (err == PN_OVERFLOW) {
      buf_size = buf_size * 2;
      buf = realloc(buf, buf_size);
    } else {
      break;
    }
  }

  free(buf);

  if (err) return err;

  size_t size = encoder->position - encoder->output;

  return size;
}
