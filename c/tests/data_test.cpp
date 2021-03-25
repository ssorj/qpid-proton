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

#include <proton/codec.h>
#include <proton/error.h>

#include <cstdarg>

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
  PN_MAP
};

const size_t type_count = sizeof(types) / sizeof(pn_type_t);

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

static void check_encode_decode(auto_free<pn_data_t, pn_data_free>& src) {
  char buf[BUFSIZE];
  auto_free<pn_data_t, pn_data_free> dst(pn_data(0));

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

  // Checks

  CHECK(enc_size == dec_size);
  CHECK(inspect(src) == inspect(dst));
}

static void check_data_fill(const char *fmt, ...) {
  auto_free<pn_data_t, pn_data_free> src(pn_data(1));

  // Create src array
  va_list ap;
  va_start(ap, fmt);
  pn_data_vfill(src, fmt, ap);
  va_end(ap);

  check_encode_decode(src);
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

TEST_CASE("data_navigation") {
  auto_free<pn_data_t, pn_data_free> data(pn_data(0));

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
  auto_free<pn_data_t, pn_data_free> data(pn_data(0));

  pn_data_put_array(data, false, PN_STRING);
  pn_data_enter(data);

  // 15 is below the array8 count threshold
  for (int i = 0; i < 15; i++) {
    // Enough data to put us over the array8 size limit
    pn_data_put_string(data, pn_bytes("01234567890123456789"));
  }

  check_encode_decode(data);
}

static void check_array_type(pn_type_t type, size_t count) {
  auto_free<pn_data_t, pn_data_free> data(pn_data(0));

  pn_data_put_array(data, false, type);
  pn_data_enter(data);

  for (size_t i = 0; i < count; i++) {
    switch (type) {
    case PN_NULL:       break; // pn_data_put_null(data); break; // XXX Bug!
    case PN_BOOL:       pn_data_put_bool(data, true); break;
    case PN_UBYTE:      pn_data_put_ubyte(data, 1); break;
    case PN_BYTE:       pn_data_put_byte(data, 1); break;
    case PN_USHORT:     pn_data_put_ushort(data, 1); break;
    case PN_SHORT:      pn_data_put_short(data, 1); break;
    case PN_UINT:       pn_data_put_uint(data, 1); break;
    case PN_INT:        pn_data_put_int(data, 1); break;
    case PN_CHAR:       pn_data_put_char(data, 'a'); break;
    case PN_FLOAT:      pn_data_put_float(data, 1.1); break;
    case PN_LONG:       pn_data_put_long(data, 1); break;
    case PN_TIMESTAMP:  pn_data_put_timestamp(data, 1); break;
    case PN_DOUBLE:     pn_data_put_double(data, 1.1); break;
    case PN_DECIMAL32:  pn_data_put_decimal32(data, 1); break;
    case PN_DECIMAL64:  pn_data_put_decimal64(data, 1); break;
    case PN_DECIMAL128: {
      pn_decimal128_t value = {{
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01
        }};
      pn_data_put_decimal128(data, value);
      break;
    }
    case PN_UUID: {
      pn_uuid_t value = {{
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01
        }};
      pn_data_put_uuid(data, value);
      break;
    }
    case PN_ULONG:      pn_data_put_ulong(data, 1); break;
    case PN_BINARY:     pn_data_put_binary(data, pn_bytes("a")); break;
    case PN_STRING:     pn_data_put_string(data, pn_bytes("a")); break;
    case PN_SYMBOL:     pn_data_put_symbol(data, pn_bytes("a")); break;
    case PN_LIST:       pn_data_put_list(data); break;
    case PN_ARRAY:      pn_data_put_array(data, false, type); break;
    case PN_MAP:        pn_data_put_map(data); break;
    default:
      FAIL("Unhandled type");
    }
  }

  pn_data_exit(data);

  check_encode_decode(data);
}

TEST_CASE("array_types") {
  for (size_t i = 0; i < type_count; i++) {
    check_array_type(types[i], 1);
  }

  for (size_t i = 0; i < type_count; i++) {
    check_array_type(types[i], 256);
  }
}

TEST_CASE("list_sizes") {
  auto_free<pn_data_t, pn_data_free> data(pn_data(0));

  pn_data_put_list(data);
  pn_data_enter(data);

  // 15 is below the compound8 count threshold
  for (int i = 0; i < 15; i++) {
    // Enough data to put us over the compound8 size limit
    pn_data_put_string(data, pn_bytes("01234567890123456789"));
  }

  check_encode_decode(data);
}

TEST_CASE("map_sizes") {
  auto_free<pn_data_t, pn_data_free> data(pn_data(0));

  pn_data_put_map(data);
  pn_data_enter(data);

  for (int i = 0; i < 7; i++) {
    char key[32];
    snprintf(key, 32, "key-%d", i);

    pn_data_put_string(data, pn_bytes(key));
    pn_data_put_string(data, pn_bytes("01234567890123456789"));
  }

  check_encode_decode(data);
}
