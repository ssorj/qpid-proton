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

#include "./pn_test.hpp"

#include "core/data.h"
#include "core/util.h"

#include <proton/codec.h>
#include <proton/error.h>

#include <cstdarg>
#include <limits.h>
#include <string.h>

using namespace pn_test;

#define BUFSIZE 4096 * 2

const pn_type_t types[] = {
  PN_NULL,
  PN_BOOL,
  PN_UBYTE,
  PN_BYTE,
  PN_USHORT,
  PN_SHORT,
  PN_UINT,
  PN_INT,
  PN_CHAR,
  PN_FLOAT,
  PN_LONG,
  PN_TIMESTAMP,
  PN_DOUBLE,
  PN_DECIMAL32,
  PN_DECIMAL64,
  PN_DECIMAL128,
  PN_UUID,
  PN_ULONG,
  PN_BINARY,
  PN_STRING,
  PN_SYMBOL,
  PN_LIST,
  PN_ARRAY,
  PN_MAP,
  PN_DESCRIBED
};

const size_t type_count = sizeof(types) / sizeof(pn_type_t);

static void print_hex(const char *buf, const size_t size) {
  unsigned char *ptr = (unsigned char *) buf;

  for (size_t i = 0; i < size; i++) {
    if (i > 0 && i % 16 == 0) printf("\n");

    printf("0x%02x ", ptr[i]);
  }

  printf("\n");
}

static void check_encode_decode(auto_free<pn_data_t, pn_data_free>& src, auto_free<pn_data_t, pn_data_free>& dst) {
  char buf[BUFSIZE] = {0};

  // Encode src to buf
  int enc_size = pn_data_encode(src, buf, BUFSIZE);
  if (enc_size < 0) {
    FAIL("pn_data_encode() error: " << pn_code(enc_size) << ": " << pn_error_text(pn_data_error(src)));
  }

  // Decode buf to dst
  int dec_size = pn_data_decode(dst, buf, BUFSIZE);
  if (dec_size < 0) {
    FAIL("pn_data_decode() error: "  << pn_code(dec_size) << ": " << pn_error_text(pn_data_error(dst)));
  }

  if (enc_size != dec_size || inspect(src) != inspect(dst)) {
    printf("Source data:\n");
    pn_data_dump(src);
    printf("\nDecoded data:\n");
    pn_data_dump(dst);
    printf("\nWire bytes:\n");
    print_hex(buf, 64);
    printf("[...]\n");
  }

  CHECK(enc_size == dec_size);
  CHECK(inspect(src) == inspect(dst));
}

static void check_data_fill(const char *fmt, ...) {
  auto_free<pn_data_t, pn_data_free> src(pn_data(0));
  auto_free<pn_data_t, pn_data_free> dst(pn_data(0));

  // Create src array
  va_list ap;
  va_start(ap, fmt);
  pn_data_vfill(src, fmt, ap);
  va_end(ap);

  check_encode_decode(src, dst);
}

// Make sure we can grow the capacity of a pn_data_t all the way to the max and
// we stop there.
TEST_CASE("data_grow") {
  auto_free<pn_data_t, pn_data_free> data(pn_data(0));
  int code = 0;
  while (pn_data_size(data) < PNI_NID_MAX && !code) {
    code = pn_data_put_int(data, 1);
  }
  CHECK_THAT(*pn_data_error(data), error_empty());
  CHECK(pn_data_size(data) == PNI_NID_MAX);
  code = pn_data_put_int(data, 1);
  INFO(pn_code(code));
  CHECK(code == PN_OUT_OF_MEMORY);
  CHECK(pn_data_size(data) == PNI_NID_MAX);
}

TEST_CASE("data_multiple") {
  auto_free<pn_data_t, pn_data_free> data(pn_data(1));
  auto_free<pn_data_t, pn_data_free> src(pn_data(1));

  /* NULL data pointer */
  pn_data_fill(data, "M", NULL);
  CHECK("null" == inspect(data));

  /* Empty data object */
  pn_data_clear(data);
  pn_data_fill(data, "M", src.get());
  CHECK("null" == inspect(data));

  /* Empty array */
  pn_data_clear(data);
  pn_data_clear(src);
  pn_data_put_array(src, false, PN_SYMBOL);
  pn_data_fill(data, "M", src.get());
  CHECK("null" == inspect(data));

  /* Single-element array */
  pn_data_clear(data);
  pn_data_clear(src);
  pn_data_put_array(src, false, PN_SYMBOL);
  pn_data_enter(src);
  pn_data_put_symbol(src, pn_bytes("foo"));
  pn_data_fill(data, "M", src.get());
  CHECK(":foo" == inspect(data));

  /* Multi-element array */
  pn_data_clear(data);
  pn_data_clear(src);
  pn_data_put_array(src, false, PN_SYMBOL);
  pn_data_enter(src);
  pn_data_put_symbol(src, pn_bytes("foo"));
  pn_data_put_symbol(src, pn_bytes("bar"));
  pn_data_fill(data, "M", src.get());
  CHECK("@PN_SYMBOL[:foo, :bar]" == inspect(data));

  /* Non-array */
  pn_data_clear(data);
  pn_data_clear(src);
  pn_data_put_symbol(src, pn_bytes("baz"));
  pn_data_fill(data, "M", src.get());
  CHECK(":baz" == inspect(data));

  /* Described list with open frame descriptor */
  pn_data_clear(data);
  pn_data_fill(data, "DL[]", (uint64_t)16);
  CHECK("@open(16) []" == inspect(data));

  /* open frame with some fields */
  pn_data_clear(data);
  pn_data_fill(data, "DL[SSnI]", (uint64_t)16, "container-1", 0, 965);
  CHECK("@open(16) [container-id=\"container-1\", channel-max=965]" == inspect(data));

  /* Map */
  pn_data_clear(data);
  pn_data_fill(data, "{S[iii]SI}", "foo", 1, 987, 3, "bar", 965);
  CHECK("{\"foo\"=[1, 987, 3], \"bar\"=965}" == inspect(data));
}

TEST_CASE("data_type_null") {
  auto_free<pn_data_t, pn_data_free> src(pn_data(0));
  auto_free<pn_data_t, pn_data_free> dst(pn_data(0));

  pn_data_put_null(src);

  check_encode_decode(src, dst);

  pn_data_next(dst);

  CHECK(pn_data_is_null(dst) == true);
}

TEST_CASE("data_type_boolean") {
  auto_free<pn_data_t, pn_data_free> src(pn_data(0));
  auto_free<pn_data_t, pn_data_free> dst(pn_data(0));

  CHECK(pn_data_get_bool(src) == false);

  pn_data_put_list(src);
  pn_data_enter(src);

  pn_data_put_bool(src, true);
  pn_data_put_bool(src, false);

  check_encode_decode(src, dst);

  pn_data_next(dst);
  pn_data_enter(dst);

  CHECK(pn_data_next(dst) == true);
  CHECK(pn_data_get_bool(dst) == true);
  CHECK(pn_data_next(dst) == true);
  CHECK(pn_data_get_bool(dst) == false);
}

TEST_CASE("data_type_ubyte") {
  auto_free<pn_data_t, pn_data_free> src(pn_data(0));
  auto_free<pn_data_t, pn_data_free> dst(pn_data(0));

  CHECK(pn_data_get_ubyte(src) == 0);

  const uint8_t values[] = {
    0x00, 0x01, UCHAR_MAX
  };

  const size_t value_count = sizeof(values) / sizeof(uint8_t);

  pn_data_put_list(src);
  pn_data_enter(src);

  for (size_t i = 0; i < value_count; i++) {
    pn_data_put_ubyte(src, values[i]);
  }

  check_encode_decode(src, dst);

  pn_data_next(dst);
  pn_data_enter(dst);

  for (size_t i = 0; i < value_count; i++) {
    CHECK(pn_data_next(dst) == true);
    CHECK(pn_data_get_ubyte(dst) == values[i]);
  }
}

TEST_CASE("data_type_byte") {
  auto_free<pn_data_t, pn_data_free> src(pn_data(0));
  auto_free<pn_data_t, pn_data_free> dst(pn_data(0));

  CHECK(pn_data_get_byte(src) == 0);

  const int8_t values[] = {
    CHAR_MIN, -1, 0, 1, CHAR_MAX
  };

  const size_t value_count = sizeof(values) / sizeof(int8_t);

  pn_data_put_list(src);
  pn_data_enter(src);

  for (size_t i = 0; i < value_count; i++) {
    pn_data_put_byte(src, values[i]);
  }

  check_encode_decode(src, dst);

  pn_data_next(dst);
  pn_data_enter(dst);

  for (size_t i = 0; i < value_count; i++) {
    CHECK(pn_data_next(dst) == true);
    CHECK(pn_data_get_byte(dst) == values[i]);
  }
}

TEST_CASE("data_type_ushort") {
  auto_free<pn_data_t, pn_data_free> src(pn_data(0));
  auto_free<pn_data_t, pn_data_free> dst(pn_data(0));

  CHECK(pn_data_get_ushort(src) == 0);

  const uint16_t values[] = {
    0x00, 0x01, UCHAR_MAX, USHRT_MAX
  };

  const size_t value_count = sizeof(values) / sizeof(uint16_t);

  pn_data_put_list(src);
  pn_data_enter(src);

  for (size_t i = 0; i < value_count; i++) {
    pn_data_put_ushort(src, values[i]);
  }

  check_encode_decode(src, dst);

  pn_data_next(dst);
  pn_data_enter(dst);

  for (size_t i = 0; i < value_count; i++) {
    CHECK(pn_data_next(dst) == true);
    CHECK(pn_data_get_ushort(dst) == values[i]);
  }
}

TEST_CASE("data_type_short") {
  auto_free<pn_data_t, pn_data_free> src(pn_data(0));
  auto_free<pn_data_t, pn_data_free> dst(pn_data(0));

  CHECK(pn_data_get_short(src) == 0);

  const int16_t values[] = {
    SHRT_MIN, CHAR_MIN, -1, 0, 1, CHAR_MAX, SHRT_MAX
  };

  const size_t value_count = sizeof(values) / sizeof(int16_t);

  pn_data_put_list(src);
  pn_data_enter(src);

  for (size_t i = 0; i < value_count; i++) {
    pn_data_put_short(src, values[i]);
  }

  check_encode_decode(src, dst);

  pn_data_next(dst);
  pn_data_enter(dst);

  for (size_t i = 0; i < value_count; i++) {
    CHECK(pn_data_next(dst) == true);
    CHECK(pn_data_get_short(dst) == values[i]);
  }
}

TEST_CASE("data_type_uint") {
  auto_free<pn_data_t, pn_data_free> src(pn_data(0));
  auto_free<pn_data_t, pn_data_free> dst(pn_data(0));

  CHECK(pn_data_get_uint(src) == 0);

  const uint32_t values[] = {
    0x00, 0x01, UCHAR_MAX, USHRT_MAX, UINT_MAX
  };

  const size_t value_count = sizeof(values) / sizeof(uint32_t);

  pn_data_put_list(src);
  pn_data_enter(src);

  for (size_t i = 0; i < value_count; i++) {
    pn_data_put_uint(src, values[i]);
  }

  check_encode_decode(src, dst);

  pn_data_next(dst);
  pn_data_enter(dst);

  for (size_t i = 0; i < value_count; i++) {
    CHECK(pn_data_next(dst) == true);
    CHECK(pn_data_get_uint(dst) == values[i]);
  }
}

TEST_CASE("data_type_int") {
  auto_free<pn_data_t, pn_data_free> src(pn_data(0));
  auto_free<pn_data_t, pn_data_free> dst(pn_data(0));

  CHECK(pn_data_get_int(src) == 0);

  const int32_t values[] = {
    INT_MIN, SHRT_MIN, CHAR_MIN, -1, 0, 1, CHAR_MAX, SHRT_MAX, INT_MAX
  };

  const size_t value_count = sizeof(values) / sizeof(int32_t);

  pn_data_put_list(src);
  pn_data_enter(src);

  for (size_t i = 0; i < value_count; i++) {
    pn_data_put_int(src, values[i]);
  }

  check_encode_decode(src, dst);

  pn_data_next(dst);
  pn_data_enter(dst);

  for (size_t i = 0; i < value_count; i++) {
    CHECK(pn_data_next(dst) == true);
    CHECK(pn_data_get_int(dst) == values[i]);
  }
}

TEST_CASE("data_type_ulong") {
  auto_free<pn_data_t, pn_data_free> src(pn_data(0));
  auto_free<pn_data_t, pn_data_free> dst(pn_data(0));

  CHECK(pn_data_get_ulong(src) == 0);

  const uint64_t values[] = {
    0x00, 0x01, UCHAR_MAX, USHRT_MAX, UINT_MAX, ULLONG_MAX
  };

  const size_t value_count = sizeof(values) / sizeof(uint64_t);

  pn_data_put_list(src);
  pn_data_enter(src);

  for (size_t i = 0; i < value_count; i++) {
    pn_data_put_ulong(src, values[i]);
  }

  check_encode_decode(src, dst);

  pn_data_next(dst);
  pn_data_enter(dst);

  for (size_t i = 0; i < value_count; i++) {
    CHECK(pn_data_next(dst) == true);
    CHECK(pn_data_get_ulong(dst) == values[i]);
  }
}

TEST_CASE("data_type_long") {
  auto_free<pn_data_t, pn_data_free> src(pn_data(0));
  auto_free<pn_data_t, pn_data_free> dst(pn_data(0));

  CHECK(pn_data_get_long(src) == 0);

  const int64_t values[] = {
    LLONG_MIN, INT_MIN, SHRT_MIN, CHAR_MIN, -1, 0, 1, CHAR_MAX, SHRT_MAX, INT_MAX, LLONG_MAX
  };

  const size_t value_count = sizeof(values) / sizeof(int64_t);

  pn_data_put_list(src);
  pn_data_enter(src);

  for (size_t i = 0; i < value_count; i++) {
    pn_data_put_long(src, values[i]);
  }

  check_encode_decode(src, dst);

  pn_data_next(dst);
  pn_data_enter(dst);

  for (size_t i = 0; i < value_count; i++) {
    CHECK(pn_data_next(dst) == true);
    CHECK(pn_data_get_long(dst) == values[i]);
  }
}

TEST_CASE("data_type_described") {
  auto_free<pn_data_t, pn_data_free> src(pn_data(0));
  auto_free<pn_data_t, pn_data_free> dst(pn_data(0));

  pn_bytes_t key = pn_bytes("sprinkles");
  pn_bytes_t value = pn_bytes(".:!:.");

  pn_data_put_described(src);
  pn_data_enter(src);
  pn_data_put_symbol(src, key);
  pn_data_put_string(src, value);
  pn_data_exit(src);

  check_encode_decode(src, dst);

  pn_data_next(dst);

  CHECK(pn_data_is_described(dst) == true);

  pn_data_enter(dst);

  CHECK(pn_data_next(dst) == true);
  CHECK(pn_bytes_equal(pn_data_get_symbol(dst), key));
  CHECK(pn_data_next(dst) == true);
  CHECK(pn_bytes_equal(pn_data_get_string(dst), value));
}

TEST_CASE("array_list") {
  check_data_fill("@T[]", PN_LIST);
  // TODO: PROTON-2248: using S and s reversed
  // empty list as first array element
  check_data_fill("@T[[][oo][][iii][Sosid]]", PN_LIST, true, false, 1, 2, 3, "hello", false, "world", 43210, 2.565e-56);
  // empty list not as first array element
  check_data_fill("@T[[Sid][oooo][]]", PN_LIST, "aaa", 123, double(3.2415), true, true, false, true);
  // only empty lists
  check_data_fill("@T[[][][][][]]", PN_LIST);
}

TEST_CASE("data_debug") {
  auto_free<pn_data_t, pn_data_free> data(pn_data(0));

  pn_data_put_list(data);
  pn_data_enter(data);
  pn_data_put_string(data, pn_bytes("a"));
  pn_data_put_string(data, pn_bytes("b"));
  pn_data_put_string(data, pn_bytes("c"));
  pn_data_exit(data);

  pn_data_put_map(data);
  pn_data_enter(data);
  pn_data_put_string(data, pn_bytes("a"));
  pn_data_put_int(data, 1);
  pn_data_put_string(data, pn_bytes("b"));
  pn_data_put_int(data, 2);
  pn_data_put_string(data, pn_bytes("c"));
  pn_data_put_int(data, 3);
  pn_data_exit(data);

  pn_data_put_described(data);
  pn_data_enter(data);
  pn_data_put_symbol(data, pn_bytes("x"));
  pn_data_put_int(data, 1);
  pn_data_exit(data);

  CHECK(strcmp(pn_type_name(PN_INVALID), "<UNKNOWN>") == 0);

  pn_data_dump(data);
  pn_data_print(data);
}

TEST_CASE("data_fill_scan") {
  auto_free<pn_data_t, pn_data_free> src(pn_data(0));
  auto_free<pn_data_t, pn_data_free> dst(pn_data(0));

  bool bool_in = true;
  uint8_t ubyte_in = UCHAR_MAX;
  int8_t byte_in = CHAR_MAX;
  uint16_t ushort_in = USHRT_MAX;
  int16_t short_in = SHRT_MAX;
  uint32_t uint_in = UINT_MAX;
  int32_t int_in = INT_MAX;
  uint64_t ulong_in = ULLONG_MAX;
  int64_t long_in = LLONG_MAX;

  pn_data_fill(src, "[oBbHhIiLl]",
               bool_in, ubyte_in, byte_in, ushort_in, short_in, uint_in, int_in,
               ulong_in, long_in);

  check_encode_decode(src, dst);

  bool bool_out;
  uint8_t ubyte_out;
  int8_t byte_out;
  uint16_t ushort_out;
  int16_t short_out;
  uint32_t uint_out;
  int32_t int_out;
  uint64_t ulong_out;
  int64_t long_out;

  pn_data_scan(dst, "[oBbHhIiLl]",
               &bool_out, &ubyte_out, &byte_out, &ushort_out, &short_out, &uint_out, &int_out,
               &ulong_out, &long_out);

  CHECK(bool_in == bool_out);
  CHECK(ubyte_in == ubyte_out);
  CHECK(byte_in == byte_out);
  CHECK(ushort_in == ushort_out);
  CHECK(short_in == short_out);
  CHECK(uint_in == uint_out);
  CHECK(int_in == int_out);
  CHECK(ulong_in == ulong_out);
  CHECK(long_in == long_out);
}

TEST_CASE("data_navigation") {
  auto_free<pn_data_t, pn_data_free> data(pn_data(0));

  CHECK(pn_data_enter(data) == false);
  CHECK(pn_data_prev(data) == false);

  pn_data_put_int(data, 1);
  pn_data_put_int(data, 2);

  pn_data_put_list(data);
  pn_data_enter(data);

  pn_data_put_int(data, 3);
  pn_data_put_int(data, 4);

  pn_data_exit(data);

  pn_data_rewind(data);

  pn_data_next(data);
  CHECK(pn_data_get_int(data) == 1);

  pn_data_next(data);
  CHECK(pn_data_get_int(data) == 2);

  pn_data_prev(data);
  CHECK(pn_data_get_int(data) == 1);

  pn_data_next(data);
  CHECK(pn_data_get_int(data) == 2);

  pn_data_next(data);
  pn_data_enter(data);

  pn_data_next(data);
  CHECK(pn_data_get_int(data) == 3);

  pn_data_exit(data);

  pn_data_prev(data);
  CHECK(pn_data_get_int(data) == 2);

  pn_data_next(data);
  pn_data_enter(data);

  pn_data_next(data);
  CHECK(pn_data_get_int(data) == 3);

  pn_data_next(data);
  CHECK(pn_data_get_int(data) == 4);
}

TEST_CASE("invalid_data") {
  auto_free<pn_data_t, pn_data_free> data(pn_data(0));

  pn_data_put_list(data);
  pn_data_enter(data);

  pn_data_put_int(data, 1);
  pn_data_put_int(data, 2);
  pn_data_put_int(data, 3);

  char encoded[BUFSIZE];
  ssize_t size;

  size = pn_data_encode(data, encoded, BUFSIZE);
  CHECK(size > 0);

  encoded[0] = (char) 0xC2; // An invalid format code

  pn_data_clear(data);

  size = pn_data_decode(data, encoded, BUFSIZE);
  CHECK(size == PN_ARG_ERR);
}

TEST_CASE("array_sizes") {
  auto_free<pn_data_t, pn_data_free> src(pn_data(0));
  auto_free<pn_data_t, pn_data_free> dst(pn_data(0));

  pn_data_put_array(src, false, PN_STRING);
  pn_data_enter(src);

  // 15 is below the array8 count threshold
  for (int i = 0; i < 15; i++) {
    // Enough data to put us over the array8 size limit
    pn_data_put_string(src, pn_bytes("01234567890123456789"));
  }

  check_encode_decode(src, dst);
}

static void check_array_type(pn_type_t type, size_t count) {
  if (type == PN_DESCRIBED) return;

  auto_free<pn_data_t, pn_data_free> src(pn_data(0));
  auto_free<pn_data_t, pn_data_free> dst(pn_data(0));

  pn_data_put_array(src, false, type);
  pn_data_enter(src);

  for (size_t i = 0; i < count; i++) {
    switch (type) {
    case PN_NULL:       pn_data_put_null(src); break;
    case PN_BOOL:       pn_data_put_bool(src, true); break;
    case PN_UBYTE:      pn_data_put_ubyte(src, 1); break;
    case PN_BYTE:       pn_data_put_byte(src, 1); break;
    case PN_USHORT:     pn_data_put_ushort(src, 1); break;
    case PN_SHORT:      pn_data_put_short(src, 1); break;
    case PN_UINT:       pn_data_put_uint(src, 1); break;
    case PN_INT:        pn_data_put_int(src, 1); break;
    case PN_CHAR:       pn_data_put_char(src, 'a'); break;
    case PN_FLOAT:      pn_data_put_float(src, 1.1); break;
    case PN_LONG:       pn_data_put_long(src, 1); break;
    case PN_TIMESTAMP:  pn_data_put_timestamp(src, 1); break;
    case PN_DOUBLE:     pn_data_put_double(src, 1.1); break;
    case PN_DECIMAL32:  pn_data_put_decimal32(src, 1); break;
    case PN_DECIMAL64:  pn_data_put_decimal64(src, 1); break;
    case PN_DECIMAL128: {
      pn_decimal128_t value = {{
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01
        }};
      pn_data_put_decimal128(src, value);
      break;
    }
    case PN_UUID: {
      pn_uuid_t value = {{
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01
        }};
      pn_data_put_uuid(src, value);
      break;
    }
    case PN_ULONG:      pn_data_put_ulong(src, 1); break;
    case PN_BINARY:     pn_data_put_binary(src, pn_bytes("a")); break;
    case PN_STRING:     pn_data_put_string(src, pn_bytes("a")); break;
    case PN_SYMBOL:     pn_data_put_symbol(src, pn_bytes("a")); break;
    case PN_LIST:       pn_data_put_list(src); break;
    case PN_ARRAY:      pn_data_put_array(src, false, type); break;
    case PN_MAP:        pn_data_put_map(src); break;
    default:
      FAIL("Unhandled type");
    }
  }

  pn_data_exit(src);

  check_encode_decode(src, dst);
}

TEST_CASE("array_types_length_0") {
  for (size_t i = 0; i < type_count; i++) {
    check_array_type(types[i], 0);
  }
}

TEST_CASE("array_types_length_1") {
  for (size_t i = 0; i < type_count; i++) {
    check_array_type(types[i], 1);
  }
}

TEST_CASE("array_types_length_16") {
  for (size_t i = 0; i < type_count; i++) {
    check_array_type(types[i], 16);
  }
}

TEST_CASE("array_types_length_256") {
  for (size_t i = 0; i < type_count; i++) {
    check_array_type(types[i], 256);
  }
}

TEST_CASE("array_described") {
  auto_free<pn_data_t, pn_data_free> src(pn_data(0));
  auto_free<pn_data_t, pn_data_free> dst(pn_data(0));

  pn_data_put_array(src, true, PN_NULL);
  pn_data_enter(src);
  pn_data_put_symbol(src, pn_bytes("emptiness"));
  pn_data_exit(src);

  check_encode_decode(src, dst);

  pn_data_clear(src);
  pn_data_clear(dst);

  pn_data_put_array(src, true, PN_NULL);
  pn_data_enter(src);
  pn_data_put_symbol(src, pn_bytes("emptiness"));
  for (size_t i = 0; i < 256; i++) pn_data_put_null(src);
  pn_data_exit(src);

  check_encode_decode(src, dst);
}

TEST_CASE("list_sizes") {
  auto_free<pn_data_t, pn_data_free> src(pn_data(0));
  auto_free<pn_data_t, pn_data_free> dst(pn_data(0));

  pn_data_put_list(src);
  pn_data_enter(src);

  // 15 is below the compound8 count threshold
  for (int i = 0; i < 15; i++) {
    // Enough data to put us over the compound8 size limit
    pn_data_put_string(src, pn_bytes("01234567890123456789"));
  }

  check_encode_decode(src, dst);
}

TEST_CASE("map_sizes") {
  auto_free<pn_data_t, pn_data_free> src(pn_data(0));
  auto_free<pn_data_t, pn_data_free> dst(pn_data(0));

  pn_data_put_map(src);
  pn_data_enter(src);

  for (int i = 0; i < 7; i++) {
    char key[32];
    snprintf(key, 32, "key-%d", i);

    pn_data_put_string(src, pn_bytes(key));
    pn_data_put_string(src, pn_bytes("01234567890123456789"));
  }

  check_encode_decode(src, dst);
}
