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

static int pni_decoder_decode_item(pn_decoder_t *decoder, pn_data_t *data);
static int pni_decoder_decode_type(pn_decoder_t *decoder, pn_data_t *data, uint8_t* code);
static int pni_decoder_decode_value(pn_decoder_t *decoder, pn_data_t *data, uint8_t code);
static int pni_decoder_decode_described_type(pn_decoder_t *decoder, pn_data_t *data, uint8_t* code);
static int pni_decoder_decode_described_value(pn_decoder_t *decoder, pn_data_t *data);
static int pni_decoder_decode_fixed0(pn_decoder_t *decoder, pn_data_t *data, uint8_t code);
static int pni_decoder_decode_fixed1(pn_decoder_t *decoder, pn_data_t *data, uint8_t code);
static int pni_decoder_decode_fixed2(pn_decoder_t *decoder, pn_data_t *data, uint8_t code);
static int pni_decoder_decode_fixed4(pn_decoder_t *decoder, pn_data_t *data, uint8_t code);
static int pni_decoder_decode_fixed8(pn_decoder_t *decoder, pn_data_t *data, uint8_t code);
static int pni_decoder_decode_fixed16(pn_decoder_t *decoder, pn_data_t *data, uint8_t code);
static int pni_decoder_decode_variable8(pn_decoder_t *decoder, pn_data_t *data, uint8_t code);
static int pni_decoder_decode_variable32(pn_decoder_t *decoder, pn_data_t *data, uint8_t code);
static int pni_decoder_decode_compound8(pn_decoder_t *decoder, pn_data_t *data, uint8_t code);
static int pni_decoder_decode_compound32(pn_decoder_t *decoder, pn_data_t *data, uint8_t code);
static int pni_decoder_decode_array8(pn_decoder_t *decoder, pn_data_t *data, uint8_t code);
static int pni_decoder_decode_array32(pn_decoder_t *decoder, pn_data_t *data, uint8_t code);

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
  return *decoder->position++;
}

static inline uint16_t pn_decoder_readf16(pn_decoder_t *decoder)
{
  uint16_t a = (uint8_t) decoder->position[0];
  uint16_t b = (uint8_t) decoder->position[1];

  decoder->position += 2;

  return a << 8 | b;
}

static inline uint32_t pn_decoder_readf32(pn_decoder_t *decoder)
{
  uint32_t a = (uint8_t) decoder->position[0];
  uint32_t b = (uint8_t) decoder->position[1];
  uint32_t c = (uint8_t) decoder->position[2];
  uint32_t d = (uint8_t) decoder->position[3];

  decoder->position += 4;

  return a << 24 | b << 16 | c <<  8 | d;
}

static inline uint64_t pn_decoder_readf64(pn_decoder_t *decoder)
{
  uint64_t a = (uint8_t) decoder->position[0];
  uint64_t b = (uint8_t) decoder->position[1];
  uint64_t c = (uint8_t) decoder->position[2];
  uint64_t d = (uint8_t) decoder->position[3];
  uint64_t e = (uint8_t) decoder->position[4];
  uint64_t f = (uint8_t) decoder->position[5];
  uint64_t g = (uint8_t) decoder->position[6];
  uint64_t h = (uint8_t) decoder->position[7];

  decoder->position += 8;

  return a << 56 | b << 48 | c << 40 | d << 32 | e << 24 | f << 16 | g <<  8 | h;
}

static inline void pn_decoder_readf128(pn_decoder_t *decoder, void *dst)
{
  memcpy(dst, decoder->position, 16);
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
  switch (code) {
  case PNE_DESCRIPTOR: return (pn_type_t) PN_ARG_ERR;
  case PNE_NULL: return PN_NULL;
  case PNE_TRUE:
  case PNE_FALSE:
  case PNE_BOOLEAN: return PN_BOOL;
  case PNE_UBYTE: return PN_UBYTE;
  case PNE_BYTE: return PN_BYTE;
  case PNE_USHORT: return PN_USHORT;
  case PNE_SHORT: return PN_SHORT;
  case PNE_UINT0:
  case PNE_SMALLUINT:
  case PNE_UINT: return PN_UINT;
  case PNE_SMALLINT:
  case PNE_INT: return PN_INT;
  case PNE_UTF32: return PN_CHAR;
  case PNE_FLOAT: return PN_FLOAT;
  case PNE_LONG:
  case PNE_SMALLLONG: return PN_LONG;
  case PNE_MS64: return PN_TIMESTAMP;
  case PNE_DOUBLE: return PN_DOUBLE;
  case PNE_DECIMAL32: return PN_DECIMAL32;
  case PNE_DECIMAL64: return PN_DECIMAL64;
  case PNE_DECIMAL128: return PN_DECIMAL128;
  case PNE_UUID: return PN_UUID;
  case PNE_ULONG0:
  case PNE_SMALLULONG:
  case PNE_ULONG: return PN_ULONG;
  case PNE_VBIN8:
  case PNE_VBIN32: return PN_BINARY;
  case PNE_STR8_UTF8:
  case PNE_STR32_UTF8: return PN_STRING;
  case PNE_SYM8:
  case PNE_SYM32: return PN_SYMBOL;
  case PNE_LIST0:
  case PNE_LIST8:
  case PNE_LIST32: return PN_LIST;
  case PNE_ARRAY8:
  case PNE_ARRAY32: return PN_ARRAY;
  case PNE_MAP8:
  case PNE_MAP32: return PN_MAP;
  default: return (pn_type_t) PN_ARG_ERR;
  }
}

static inline int pni_decoder_decode_item(pn_decoder_t *decoder, pn_data_t *data)
{
  int err;
  uint8_t code;

  err = pni_decoder_decode_type(decoder, data, &code);
  if (err) return err;

  if (code == PNE_DESCRIPTOR) {
    return pni_decoder_decode_described_value(decoder, data);
  } else {
    return pni_decoder_decode_value(decoder, data, code);
  }
}

static inline int pni_decoder_decode_type(pn_decoder_t *decoder, pn_data_t *data, uint8_t *code)
{
  if (!pn_decoder_remaining(decoder)) return PN_UNDERFLOW;
  *code = pn_decoder_readf8(decoder);
  return 0;
}

static int pni_decoder_decode_value(pn_decoder_t *decoder, pn_data_t *data, uint8_t code)
{
  switch (code & 0xF0) {
  case 0x40: return pni_decoder_decode_fixed0(decoder, data, code);
  case 0x50: return pni_decoder_decode_fixed1(decoder, data, code);
  case 0x60: return pni_decoder_decode_fixed2(decoder, data, code);
  case 0x70: return pni_decoder_decode_fixed4(decoder, data, code);
  case 0x80: return pni_decoder_decode_fixed8(decoder, data, code);
  case 0x90: return pni_decoder_decode_fixed16(decoder, data, code);
  case 0xA0: return pni_decoder_decode_variable8(decoder, data, code);
  case 0xB0: return pni_decoder_decode_variable32(decoder, data, code);
  case 0xC0: return pni_decoder_decode_compound8(decoder, data, code);
  case 0xD0: return pni_decoder_decode_compound32(decoder, data, code);
  case 0xE0: return pni_decoder_decode_array8(decoder, data, code);
  case 0xF0: return pni_decoder_decode_array32(decoder, data, code);
  default:
    return pn_error_format(pni_decoder_error(decoder), PN_ARG_ERR, "unrecognized typecode: %u", code);
  }
}

static inline int pni_decoder_decode_described_type(pn_decoder_t *decoder, pn_data_t *data, uint8_t *code)
{
  int err;
  uint8_t next;

  // The descriptor type code
  err = pni_decoder_decode_type(decoder, data, &next);
  if (err) return err;

  // XXX Check nesting here?

  // The descriptor value
  err = pni_decoder_decode_value(decoder, data, next);
  if (err) return err;

  // The descriptor primitive type code
  err = pni_decoder_decode_type(decoder, data, &next);
  if (err) return err;

  // No nested descriptors
  if (next == PNE_DESCRIPTOR) return PN_ARG_ERR;

  *code = next;

  return 0;
}

static inline int pni_decoder_decode_described_value(pn_decoder_t *decoder, pn_data_t *data)
{
  int err;
  uint8_t code;

  err = pni_data_put_fixed0(data, PN_DESCRIBED);
  if (err) return err;

  pn_data_enter(data);

  err = pni_decoder_decode_described_type(decoder, data, &code);
  if (err) return err;

  err = pni_decoder_decode_value(decoder, data, code);
  if (err) return err;

  pn_data_exit(data);

  return 0;
}

static int pni_decoder_decode_fixed0(pn_decoder_t *decoder, pn_data_t *data, uint8_t code)
{
  switch (code) {
  case PNE_NULL: return pni_data_put_fixed0(data, PN_NULL);
  case PNE_TRUE: return pni_data_put_fixed1(data, PN_BOOL, (uint8_t) 1);
  case PNE_FALSE: return pni_data_put_fixed1(data, PN_BOOL, (uint8_t) 0);
  case PNE_UINT0: return pni_data_put_fixed4(data, PN_UINT, (uint32_t) 0);
  case PNE_ULONG0: return pni_data_put_fixed8(data, PN_ULONG, (uint64_t) 0);
  case PNE_LIST0: return pni_data_put_fixed0(data, PN_LIST);
  default:
    return pn_error_format(pni_decoder_error(decoder), PN_ARG_ERR, "unrecognized typecode: %u", code);
  }
}

static int pni_decoder_decode_fixed1(pn_decoder_t *decoder, pn_data_t *data, uint8_t code)
{
  if (!pn_decoder_remaining(decoder)) return PN_UNDERFLOW;

  pn_type_t type = pn_code2type(code);
  uint8_t value = pn_decoder_readf8(decoder);

  if (type == PN_ARG_ERR) {
    return pn_error_format(pni_decoder_error(decoder), PN_ARG_ERR, "unrecognized typecode: %u", code);
  }

  return pni_data_put_fixed1(data, type, value);
}

static int pni_decoder_decode_fixed2(pn_decoder_t *decoder, pn_data_t *data, uint8_t code)
{
  if (pn_decoder_remaining(decoder) < 2) return PN_UNDERFLOW;

  pn_type_t type = pn_code2type(code);
  uint16_t value = pn_decoder_readf16(decoder);

  if (type == PN_ARG_ERR) {
    return pn_error_format(pni_decoder_error(decoder), PN_ARG_ERR, "unrecognized typecode: %u", code);
  }

  return pni_data_put_fixed2(data, type, value);
}

static int pni_decoder_decode_fixed4(pn_decoder_t *decoder, pn_data_t *data, uint8_t code)
{
  if (pn_decoder_remaining(decoder) < 4) return PN_UNDERFLOW;

  pn_type_t type = pn_code2type(code);
  uint32_t value = pn_decoder_readf32(decoder);

  if (type == PN_ARG_ERR) {
    return pn_error_format(pni_decoder_error(decoder), PN_ARG_ERR, "unrecognized typecode: %u", code);
  }

  // switch (code) {
  // case PNE_UINT: return pn_data_put_uint(data, value);
  // case PNE_INT: return pn_data_put_int(data, value);
  // case PNE_UTF32: return pn_data_put_char(data, value);
  // case PNE_FLOAT: {
  //   // XXX: this assumes the platform uses IEEE floats
  //   conv_t conv = { .i = value };
  //   return pn_data_put_float(data, conv.f);
  // }
  // case PNE_DECIMAL32:
  //   return pn_data_put_decimal32(data, value);
  // default:
  //   return pn_error_format(pni_decoder_error(decoder), PN_ARG_ERR, "unrecognized typecode: %u", code);
  // }

  return pni_data_put_fixed4(data, type, value);
}

static int pni_decoder_decode_fixed8(pn_decoder_t *decoder, pn_data_t *data, uint8_t code)
{
  if (pn_decoder_remaining(decoder) < 8) return PN_UNDERFLOW;

  pn_type_t type = pn_code2type(code);
  uint64_t value = pn_decoder_readf64(decoder);

  if (type == PN_ARG_ERR) {
    return pn_error_format(pni_decoder_error(decoder), PN_ARG_ERR, "unrecognized typecode: %u", code);
  }

  // switch (code) {
  // case PNE_ULONG: return pn_data_put_ulong(data, value);
  // case PNE_LONG: return pn_data_put_long(data, value);
  // case PNE_MS64: return pn_data_put_timestamp(data, value);
  // case PNE_DOUBLE: {
  //   // XXX: this assumes the platform uses IEEE floats
  //   conv_t conv = { .l = value };
  //   return pn_data_put_double(data, conv.d);
  // }
  // case PNE_DECIMAL64: return pn_data_put_decimal64(data, value);
  // default:
  //   return pn_error_format(pni_decoder_error(decoder), PN_ARG_ERR, "unrecognized typecode: %u", code);
  // }

  return pni_data_put_fixed8(data, type, value);
}

static int pni_decoder_decode_fixed16(pn_decoder_t *decoder, pn_data_t *data, uint8_t code)
{
  if (pn_decoder_remaining(decoder) < 16) return PN_UNDERFLOW;

  if (code == PNE_UUID) {
    pn_uuid_t uuid;
    pn_decoder_readf128(decoder, &uuid);
    return pn_data_put_uuid(data, uuid);
  } else if (code == PNE_DECIMAL128) {
    pn_decimal128_t dec128;
    pn_decoder_readf128(decoder, &dec128);
    return pn_data_put_decimal128(data, dec128);
  } else {
    return pn_error_format(pni_decoder_error(decoder), PN_ARG_ERR, "unrecognized typecode: %u", code);
  }
}

static inline int pni_decoder_decode_variable_values(pn_decoder_t *decoder, pn_data_t *data, uint8_t code, size_t size)
{
  int err;
  char *start = (char *) decoder->position;
  pn_bytes_t bytes = {size, start};

  if ((code & 0x0F) == 0x1) {
    err = pn_data_put_string(data, bytes);
  } else if ((code & 0x0F) == 0x0) {
    err = pn_data_put_binary(data, bytes);
  } else if ((code & 0x0F) == 0x3) {
    err = pn_data_put_symbol(data, bytes);
  } else {
    err = PN_ARG_ERR;
  }

  if (err) return err;

  decoder->position += size;

  return 0;
}

static int pni_decoder_decode_variable8(pn_decoder_t *decoder, pn_data_t *data, uint8_t code)
{
  if (!pn_decoder_remaining(decoder)) return PN_UNDERFLOW;

  size_t size = pn_decoder_readf8(decoder);

  if (pn_decoder_remaining(decoder) < size) return PN_UNDERFLOW;

  return pni_decoder_decode_variable_values(decoder, data, code, size);
}

static int pni_decoder_decode_variable32(pn_decoder_t *decoder, pn_data_t *data, uint8_t code)
{
  if (pn_decoder_remaining(decoder) < 4) return PN_UNDERFLOW;

  size_t size = pn_decoder_readf32(decoder);

  if (pn_decoder_remaining(decoder) < size) return PN_UNDERFLOW;

  return pni_decoder_decode_variable_values(decoder, data, code, size);
}

PN_FORCE_INLINE static int pni_decoder_decode_compound_values(pn_decoder_t *decoder, pn_data_t *data, uint8_t code, size_t count)
{
  pn_type_t type = pn_code2type(code);

  if (type == PN_ARG_ERR) {
    return pn_error_format(pni_decoder_error(decoder), PN_ARG_ERR, "unrecognized typecode: %u", code);
  }

  int err = pni_data_put_fixed0(data, type);
  if (err) return err;

  pn_data_enter(data);

  for (size_t i = 0; i < count; i++) {
    err = pni_decoder_decode_item(decoder, data);
    if (err) return err;
  }

  pn_data_exit(data);

  return 0;
}

static int pni_decoder_decode_compound8(pn_decoder_t *decoder, pn_data_t *data, uint8_t code)
{
  if (pn_decoder_remaining(decoder) < 2) return PN_UNDERFLOW;

  size_t size = pn_decoder_readf8(decoder);
  size_t count = pn_decoder_readf8(decoder);

  // Check that the size is big enough for the count
  if (size < 1) return PN_ARG_ERR;

  return pni_decoder_decode_compound_values(decoder, data, code, count);
}

static int pni_decoder_decode_compound32(pn_decoder_t *decoder, pn_data_t *data, uint8_t code)
{
  if (pn_decoder_remaining(decoder) < 8) return PN_UNDERFLOW;

  size_t size = pn_decoder_readf32(decoder);
  size_t count = pn_decoder_readf32(decoder);

  // Check that the size is big enough for the count
  if (size < 4) return PN_ARG_ERR;

  return pni_decoder_decode_compound_values(decoder, data, code, count);
}

static inline int pni_decoder_decode_array_values(pn_decoder_t *decoder, pn_data_t *data, uint8_t code, size_t count)
{
  int err;
  bool described = (*decoder->position == PNE_DESCRIPTOR);
  uint8_t array_code;

  err = pn_data_put_array(data, described, (pn_type_t) 0);
  if (err) return err;

  pn_data_enter(data);

  // Get the array type code
  err = pni_decoder_decode_type(decoder, data, &array_code);
  if (err) return err;

  if (array_code == PNE_DESCRIPTOR) {
    err = pni_decoder_decode_described_type(decoder, data, &array_code);
    if (err) return err;
  }

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

static int pni_decoder_decode_array8(pn_decoder_t *decoder, pn_data_t *data, uint8_t code)
{
  if (pn_decoder_remaining(decoder) < 2) return PN_UNDERFLOW;

  size_t size = pn_decoder_readf8(decoder);
  size_t count = pn_decoder_readf8(decoder);

  // Check that the size is big enough for the count and the array
  // constructor
  if (size < 1 + 1) return PN_ARG_ERR;

  return pni_decoder_decode_array_values(decoder, data, code, count);
}

static int pni_decoder_decode_array32(pn_decoder_t *decoder, pn_data_t *data, uint8_t code)
{
  if (pn_decoder_remaining(decoder) < 8) return PN_UNDERFLOW;

  size_t size = pn_decoder_readf32(decoder);
  size_t count = pn_decoder_readf32(decoder);

  // Check that the size is big enough for the count and the array
  // constructor
  if (size < 4 + 1) return PN_ARG_ERR;

  return pni_decoder_decode_array_values(decoder, data, code, count);
}

// // We disallow using any compound type as a described descriptor to avoid recursion
// // in decoding. Although these seem syntactically valid they don't seem to be of any
// // conceivable use!
// static inline bool pni_allowed_descriptor_code(uint8_t code)
// {
//   return
//     code != PNE_DESCRIPTOR &&
//     code != PNE_ARRAY8 && code != PNE_ARRAY32 &&
//     code != PNE_LIST8 && code != PNE_LIST32 &&
//     code != PNE_MAP8 && code != PNE_MAP32;
// }

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
