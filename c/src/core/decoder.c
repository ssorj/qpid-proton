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
#include "decoder.h"

#include <string.h>

pn_type_t pni_data_parent_type(pn_data_t *data);
size_t pn_data_siblings(pn_data_t *data);
void pni_data_set_array_type(pn_data_t *data, pn_type_t type);

static int pni_decoder_decode_item(pn_decoder_t *decoder, pn_data_t *data);
static int pni_decoder_decode_type(pn_decoder_t *decoder, pn_data_t *data, uint8_t *code);
static int pni_decoder_decode_value(pn_decoder_t *decoder, pn_data_t *data, uint8_t code);
static int pni_decoder_decode_described(pn_decoder_t *decoder, pn_data_t *data);
static int pni_decoder_decode_variable(pn_decoder_t *decoder, pn_data_t *data, uint8_t code);
static int pni_decoder_decode_compound(pn_decoder_t *decoder, pn_data_t *data, uint8_t code);
static int pni_decoder_decode_array(pn_decoder_t *decoder, pn_data_t *data, uint8_t code);

static inline pn_error_t *pni_decoder_error(pn_decoder_t *decoder)
{
  if (!decoder->error) decoder->error = pn_error();
  return decoder->error;
}

void pn_decoder_initialize(pn_decoder_t *decoder)
{
  decoder->input = NULL;
  decoder->size = 0;
  decoder->position = NULL;
  decoder->error = NULL;
}

void pn_decoder_finalize(pn_decoder_t *decoder)
{
  pn_error_free(decoder->error);
}

static inline uint8_t pn_decoder_readf8(pn_decoder_t *decoder)
{
  uint8_t r = decoder->position[0];
  decoder->position++;
  return r;
}

static inline uint16_t pn_decoder_readf16(pn_decoder_t *decoder)
{
  uint16_t a = (uint8_t) decoder->position[0];
  uint16_t b = (uint8_t) decoder->position[1];
  uint16_t r = a << 8
    | b;
  decoder->position += 2;
  return r;
}

static inline uint32_t pn_decoder_readf32(pn_decoder_t *decoder)
{
  uint32_t a = (uint8_t) decoder->position[0];
  uint32_t b = (uint8_t) decoder->position[1];
  uint32_t c = (uint8_t) decoder->position[2];
  uint32_t d = (uint8_t) decoder->position[3];
  uint32_t r = a << 24
    | b << 16
    | c <<  8
    | d;
  decoder->position += 4;
  return r;
}

static inline uint64_t pn_decoder_readf64(pn_decoder_t *decoder)
{
  uint64_t a = pn_decoder_readf32(decoder);
  uint64_t b = pn_decoder_readf32(decoder);
  return a << 32 | b;
}

static inline void pn_decoder_readf128(pn_decoder_t *decoder, void *dst)
{
  memmove(dst, decoder->position, 16);
  decoder->position += 16;
}

static inline size_t pn_decoder_remaining(pn_decoder_t *decoder)
{
  return decoder->input + decoder->size - decoder->position;
}

typedef union {
  uint32_t i;
  uint32_t a[2];
  uint64_t l;
  float f;
  double d;
} conv_t;

static inline pn_type_t pn_code2type(uint8_t code)
{
  switch (code)
  {
  case PNE_DESCRIPTOR:
    return (pn_type_t) PN_ARG_ERR;
  case PNE_NULL:
    return PN_NULL;
  case PNE_TRUE:
  case PNE_FALSE:
  case PNE_BOOLEAN:
    return PN_BOOL;
  case PNE_UBYTE:
    return PN_UBYTE;
  case PNE_BYTE:
    return PN_BYTE;
  case PNE_USHORT:
    return PN_USHORT;
  case PNE_SHORT:
    return PN_SHORT;
  case PNE_UINT0:
  case PNE_SMALLUINT:
  case PNE_UINT:
    return PN_UINT;
  case PNE_SMALLINT:
  case PNE_INT:
    return PN_INT;
  case PNE_UTF32:
    return PN_CHAR;
  case PNE_FLOAT:
    return PN_FLOAT;
  case PNE_LONG:
  case PNE_SMALLLONG:
    return PN_LONG;
  case PNE_MS64:
    return PN_TIMESTAMP;
  case PNE_DOUBLE:
    return PN_DOUBLE;
  case PNE_DECIMAL32:
    return PN_DECIMAL32;
  case PNE_DECIMAL64:
    return PN_DECIMAL64;
  case PNE_DECIMAL128:
    return PN_DECIMAL128;
  case PNE_UUID:
    return PN_UUID;
  case PNE_ULONG0:
  case PNE_SMALLULONG:
  case PNE_ULONG:
    return PN_ULONG;
  case PNE_VBIN8:
  case PNE_VBIN32:
    return PN_BINARY;
  case PNE_STR8_UTF8:
  case PNE_STR32_UTF8:
    return PN_STRING;
  case PNE_SYM8:
  case PNE_SYM32:
    return PN_SYMBOL;
  case PNE_LIST0:
  case PNE_LIST8:
  case PNE_LIST32:
    return PN_LIST;
  case PNE_ARRAY8:
  case PNE_ARRAY32:
    return PN_ARRAY;
  case PNE_MAP8:
  case PNE_MAP32:
    return PN_MAP;
  default:
    return (pn_type_t) PN_ARG_ERR;
  }
}

static inline int pni_decoder_decode_value(pn_decoder_t *decoder, pn_data_t *data, uint8_t code)
{
  int err;
  conv_t conv;

  switch (code) {
  case PNE_NULL:
    err = pn_data_put_null(data);
    break;
  case PNE_TRUE:
    err = pn_data_put_bool(data, true);
    break;
  case PNE_FALSE:
    err = pn_data_put_bool(data, false);
    break;
  case PNE_BOOLEAN:
    if (!pn_decoder_remaining(decoder)) return PN_UNDERFLOW;
    err = pn_data_put_bool(data, pn_decoder_readf8(decoder) != 0);
    break;
  case PNE_UBYTE:
    if (!pn_decoder_remaining(decoder)) return PN_UNDERFLOW;
    err = pn_data_put_ubyte(data, pn_decoder_readf8(decoder));
    break;
  case PNE_BYTE:
    if (!pn_decoder_remaining(decoder)) return PN_UNDERFLOW;
    err = pn_data_put_byte(data, pn_decoder_readf8(decoder));
    break;
  case PNE_USHORT:
    if (pn_decoder_remaining(decoder) < 2) return PN_UNDERFLOW;
    err = pn_data_put_ushort(data, pn_decoder_readf16(decoder));
    break;
  case PNE_SHORT:
    if (pn_decoder_remaining(decoder) < 2) return PN_UNDERFLOW;
    err = pn_data_put_short(data, pn_decoder_readf16(decoder));
    break;
  case PNE_UINT:
    if (pn_decoder_remaining(decoder) < 4) return PN_UNDERFLOW;
    err = pn_data_put_uint(data, pn_decoder_readf32(decoder));
    break;
  case PNE_UINT0:
    err = pn_data_put_uint(data, 0);
    break;
  case PNE_SMALLUINT:
    if (!pn_decoder_remaining(decoder)) return PN_UNDERFLOW;
    err = pn_data_put_uint(data, pn_decoder_readf8(decoder));
    break;
  case PNE_SMALLINT:
    if (!pn_decoder_remaining(decoder)) return PN_UNDERFLOW;
    err = pn_data_put_int(data, (int8_t)pn_decoder_readf8(decoder));
    break;
  case PNE_INT:
    if (pn_decoder_remaining(decoder) < 4) return PN_UNDERFLOW;
    err = pn_data_put_int(data, pn_decoder_readf32(decoder));
    break;
  case PNE_UTF32:
    if (pn_decoder_remaining(decoder) < 4) return PN_UNDERFLOW;
    err = pn_data_put_char(data, pn_decoder_readf32(decoder));
    break;
  case PNE_FLOAT:
    if (pn_decoder_remaining(decoder) < 4) return PN_UNDERFLOW;
    // XXX: this assumes the platform uses IEEE floats
    conv.i = pn_decoder_readf32(decoder);
    err = pn_data_put_float(data, conv.f);
    break;
  case PNE_DECIMAL32:
    if (pn_decoder_remaining(decoder) < 4) return PN_UNDERFLOW;
    err = pn_data_put_decimal32(data, pn_decoder_readf32(decoder));
    break;
  case PNE_ULONG:
    if (pn_decoder_remaining(decoder) < 8) return PN_UNDERFLOW;
    err = pn_data_put_ulong(data, pn_decoder_readf64(decoder));
    break;
  case PNE_LONG:
    if (pn_decoder_remaining(decoder) < 8) return PN_UNDERFLOW;
    err = pn_data_put_long(data, pn_decoder_readf64(decoder));
    break;
  case PNE_MS64:
    if (pn_decoder_remaining(decoder) < 8) return PN_UNDERFLOW;
    err = pn_data_put_timestamp(data, pn_decoder_readf64(decoder));
    break;
  case PNE_DOUBLE:
    // XXX: this assumes the platform uses IEEE floats
    if (pn_decoder_remaining(decoder) < 8) return PN_UNDERFLOW;
    conv.l = pn_decoder_readf64(decoder);
    err = pn_data_put_double(data, conv.d);
    break;
  case PNE_DECIMAL64:
    if (pn_decoder_remaining(decoder) < 8) return PN_UNDERFLOW;
    err = pn_data_put_decimal64(data, pn_decoder_readf64(decoder));
    break;
  case PNE_ULONG0:
    err = pn_data_put_ulong(data, 0);
    break;
  case PNE_SMALLULONG:
    if (!pn_decoder_remaining(decoder)) return PN_UNDERFLOW;
    err = pn_data_put_ulong(data, pn_decoder_readf8(decoder));
    break;
  case PNE_SMALLLONG:
    if (!pn_decoder_remaining(decoder)) return PN_UNDERFLOW;
    err = pn_data_put_long(data, (int8_t) pn_decoder_readf8(decoder));
    break;
  case PNE_DECIMAL128: {
    pn_decimal128_t dec128;

    if (pn_decoder_remaining(decoder) < 16) return PN_UNDERFLOW;
    pn_decoder_readf128(decoder, &dec128);

    err = pn_data_put_decimal128(data, dec128);
    break;
  }
  case PNE_UUID: {
    pn_uuid_t uuid;

    if (pn_decoder_remaining(decoder) < 16) return PN_UNDERFLOW;
    pn_decoder_readf128(decoder, &uuid);

    err = pn_data_put_uuid(data, uuid);

    break;
  }
  case PNE_VBIN8:
  case PNE_STR8_UTF8:
  case PNE_SYM8:
  case PNE_VBIN32:
  case PNE_STR32_UTF8:
  case PNE_SYM32:
    return pni_decoder_decode_variable(decoder, data, code);
  case PNE_ARRAY8:
  case PNE_ARRAY32:
    return pni_decoder_decode_array(decoder, data, code);
  case PNE_LIST0:
    err = pn_data_put_list(data);
    break;
  case PNE_LIST8:
  case PNE_LIST32:
  case PNE_MAP8:
  case PNE_MAP32:
    return pni_decoder_decode_compound(decoder, data, code);
  default:
    return pn_error_format(pni_decoder_error(decoder), PN_ARG_ERR, "unrecognized typecode: %u", code);
  }

  return err;
}

__attribute__((always_inline))
static inline int pni_decoder_decode_type(pn_decoder_t *decoder, pn_data_t *data, uint8_t *code)
{
  if (!pn_decoder_remaining(decoder)) {
    return PN_UNDERFLOW;
  }

  int err;
  uint8_t next = *decoder->position++;

  if (next == PNE_DESCRIPTOR) {
    if (pni_data_parent_type(data) != PN_ARRAY) {
      err = pn_data_put_described(data);
      if (err) return err;

      // pni_decoder_decode_item has the corresponding exit
      pn_data_enter(data);
    }

    err = pni_decoder_decode_described(decoder, data);
    if (err) return err;

    if (!pn_decoder_remaining(decoder)) {
        return PN_UNDERFLOW;
    }

    next = *decoder->position++;

    if (next == PNE_DESCRIPTOR) {
      // Nested descriptor
      return PN_ARG_ERR;
    }
  }

  *code = next;

  return 0;
}

static int pni_decoder_decode_variable(pn_decoder_t *decoder, pn_data_t *data, uint8_t code)
{
  size_t size;

  if ((code & 0xF0) == 0xA0) {
    if (!pn_decoder_remaining(decoder)) return PN_UNDERFLOW;
    size = pn_decoder_readf8(decoder);
  } else {
    if (pn_decoder_remaining(decoder) < 4) return PN_UNDERFLOW;
    size = pn_decoder_readf32(decoder);
  }

  if (pn_decoder_remaining(decoder) < size) return PN_UNDERFLOW;

  int err;
  char *start = (char *) decoder->position;
  pn_bytes_t bytes = {size, start};

  switch (code & 0x0F) {
  case 0x0:
    err = pn_data_put_binary(data, bytes);
    if (err) return err;
    break;
  case 0x1:
    err = pn_data_put_string(data, bytes);
    if (err) return err;
    break;
  case 0x3:
    err = pn_data_put_symbol(data, bytes);
    if (err) return err;
    break;
  default:
    return PN_ARG_ERR;
  }

  decoder->position += size;

  return 0;
}

static int pni_decoder_decode_compound(pn_decoder_t *decoder, pn_data_t *data, uint8_t code)
{
  int err;
  size_t size;
  size_t count;

  if (code == PNE_LIST8 || code == PNE_MAP8) {
    if (pn_decoder_remaining(decoder) < 2) return PN_UNDERFLOW;
    size = pn_decoder_readf8(decoder);
    count = pn_decoder_readf8(decoder);

    // Check that the size is big enough for the count
    if (size < 1) return PN_ARG_ERR;
  } else {
    if (pn_decoder_remaining(decoder) < 8) return PN_UNDERFLOW;
    size = pn_decoder_readf32(decoder);
    count = pn_decoder_readf32(decoder);

    // Check that the size is big enough for the count
    if (size < 4) return PN_ARG_ERR;
  }

  if (code == PNE_LIST8 || code == PNE_LIST32) {
    err = pn_data_put_list(data);
    if (err) return err;
  } else {
    err = pn_data_put_map(data);
    if (err) return err;
  }

  pn_data_enter(data);

  for (size_t i = 0; i < count; i++) {
    err = pni_decoder_decode_item(decoder, data);
    if (err) return err;
  }

  pn_data_exit(data);

  return 0;
}

static int pni_decoder_decode_array(pn_decoder_t *decoder, pn_data_t *data, uint8_t code)
{
  size_t size;
  size_t count;

  if (code == PNE_ARRAY8) {
    if (pn_decoder_remaining(decoder) < 2) return PN_UNDERFLOW;
    size = pn_decoder_readf8(decoder);
    count = pn_decoder_readf8(decoder);

    // Check that the size is big enough for the count and the array
    // constructor
    if (size < 1 + 1) return PN_ARG_ERR;
  } else {
    if (pn_decoder_remaining(decoder) < 8) return PN_UNDERFLOW;
    size = pn_decoder_readf32(decoder);
    count = pn_decoder_readf32(decoder);

    // Check that the size is big enough for the count and the array
    // constructor
    if (size < 4 + 1) return PN_ARG_ERR;
  }

  int err;
  bool described = (*decoder->position == PNE_DESCRIPTOR);
  uint8_t array_code;

  err = pn_data_put_array(data, described, (pn_type_t) 0);
  if (err) return err;

  pn_data_enter(data);

  err = pni_decoder_decode_type(decoder, data, &array_code);
  if (err) return err;

  pn_type_t type = pn_code2type(array_code);
  if ((int) type < 0) return (int) type;

  for (size_t i = 0; i < count; i++) {
    err = pni_decoder_decode_value(decoder, data, array_code);
    if (err) return err;
  }

  pn_data_exit(data);
  pni_data_set_array_type(data, type);

  return 0;
}

// We disallow using any compound type as a described descriptor to avoid recursion
// in decoding. Although these seem syntactically valid they don't seem to be of any
// conceivable use!
static inline bool pni_allowed_descriptor_code(uint8_t code)
{
  return
    code != PNE_DESCRIPTOR &&
    code != PNE_ARRAY8 && code != PNE_ARRAY32 &&
    code != PNE_LIST8 && code != PNE_LIST32 &&
    code != PNE_MAP8 && code != PNE_MAP32;
}

static inline int pni_decoder_decode_described(pn_decoder_t *decoder, pn_data_t *data)
{
  if (!pn_decoder_remaining(decoder)) {
    return PN_UNDERFLOW;
  }

  uint8_t code = *decoder->position++;;

  // XXX
  //
  // This is in a hot path
  //
  // if (!pni_allowed_descriptor_code(code)) {
  //   return PN_ARG_ERR;
  // }

  int err = pni_decoder_decode_value(decoder, data, code);
  if (err) return err;

  if (pni_data_parent_type(data) == PN_DESCRIBED && pn_data_siblings(data) > 1) {
    pn_data_exit(data);
  }

  return 0;
}

static int pni_decoder_decode_item(pn_decoder_t *decoder, pn_data_t *data)
{
  int err;
  uint8_t code;

  err = pni_decoder_decode_type(decoder, data, &code);
  if (err) return err;

  err = pni_decoder_decode_value(decoder, data, code);
  if (err) return err;

  if (pni_data_parent_type(data) == PN_DESCRIBED && pn_data_siblings(data) > 1) {
    pn_data_exit(data);
  }

  return 0;
}

ssize_t pn_decoder_decode(pn_decoder_t *decoder, const char *src, size_t size, pn_data_t *dst)
{
  decoder->input = src;
  decoder->size = size;
  decoder->position = src;

  int err = pni_decoder_decode_item(decoder, dst);

  if (err == PN_UNDERFLOW) {
    return pn_error_format(pn_data_error(dst), PN_UNDERFLOW, "not enough data to decode");
  }

  if (err) return err;

  return decoder->position - decoder->input;
}
