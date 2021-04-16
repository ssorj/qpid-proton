/*
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

#ifndef  _PROTON_SRC_CONFIG_H
#define  _PROTON_SRC_CONFIG_H

#ifndef PN_TRANSPORT_INITIAL_FRAME_SIZE
# define PN_TRANSPORT_INITIAL_FRAME_SIZE (512) /* bytes */
#endif

#if defined(__GNUC__)
#  define PNI_INLINE inline
#  define PNI_HOT __attribute__((hot))
#  define PNI_COLD __attribute__((cold))
#else
#  define PNI_INLINE
#  define PNI_HOT
#  define PNI_COLD
#endif

static inline uint16_t pni_read16(const char *bytes)
{
#if defined(__GNUC__)
  return __builtin_bswap16(*(uint16_t*) bytes);
#else
  uint16_t a = (uint8_t) decoder->position[0];
  uint16_t b = (uint8_t) decoder->position[1];

  return a << 8 | b;
#endif
}

static inline uint32_t pni_read32(const char *bytes)
{
#if defined(__GNUC__)
  return __builtin_bswap32(*(uint32_t*) bytes);
#else
  uint32_t a = (uint8_t) bytes[0];
  uint32_t b = (uint8_t) bytes[1];
  uint32_t c = (uint8_t) bytes[2];
  uint32_t d = (uint8_t) bytes[3];

  return a << 24 | b << 16 | c <<  8 | d;
#endif
}

static inline uint64_t pni_read64(const char *bytes)
{
#if defined(__GNUC__)
  return __builtin_bswap64(*(uint64_t*) bytes);
#else
  uint64_t a = (uint8_t) bytes[0];
  uint64_t b = (uint8_t) bytes[1];
  uint64_t c = (uint8_t) bytes[2];
  uint64_t d = (uint8_t) bytes[3];
  uint64_t e = (uint8_t) bytes[4];
  uint64_t f = (uint8_t) bytes[5];
  uint64_t g = (uint8_t) bytes[6];
  uint64_t h = (uint8_t) bytes[7];

  return a << 56 | b << 48 | c << 40 | d << 32 | e << 24 | f << 16 | g <<  8 | h;
#endif
}

static inline void pni_write16(char *bytes, const uint16_t value)
{
#if defined(__GNUC__)
  *(uint16_t*) bytes = __builtin_bswap16(value);
#else
  bytes[0] = 0xFF & (value >> 8);
  bytes[1] = 0xFF & (value     );
#endif
}

static inline void pni_write32(char *bytes, const uint32_t value)
{
#if defined(__GNUC__)
  *(uint32_t*) bytes = __builtin_bswap32(value);
#else
  bytes[0] = 0xFF & (value >> 24);
  bytes[1] = 0xFF & (value >> 16);
  bytes[2] = 0xFF & (value >>  8);
  bytes[3] = 0xFF & (value      );
#endif
}

static inline void pni_write64(char *bytes, const uint64_t value)
{
#if defined(__GNUC__)
  *(uint64_t*) bytes = __builtin_bswap64(value);
#else
  bytes[0] = 0xFF & (value >> 56);
  bytes[1] = 0xFF & (value >> 48);
  bytes[2] = 0xFF & (value >> 40);
  bytes[3] = 0xFF & (value >> 32);
  bytes[4] = 0xFF & (value >> 24);
  bytes[5] = 0xFF & (value >> 16);
  bytes[6] = 0xFF & (value >>  8);
  bytes[7] = 0xFF & (value      );
#endif
}

#endif /*  _PROTON_SRC_CONFIG_H */
