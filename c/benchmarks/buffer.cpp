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

#include "core/buffer.h"

#include <benchmark/benchmark.h>

#include <algorithm>
#include <cstdlib>

static void BM_ReadWriteBuffer(benchmark::State &state)
{
  const size_t total_size = state.range(0);
  const size_t chunk_size = state.range(1);

  char *src = static_cast<char *>(malloc(chunk_size));
  char *dst = static_cast<char *>(malloc(chunk_size));

  std::fill_n(src, chunk_size, 0x5A);

  for (auto _ : state) {
    pn_buffer_t *buffer = pn_buffer(0);

    size_t written = 0;
    size_t read = 0;

    while (read < total_size) {
      size_t to_write;

      // Fill a chunk, almost
      if (written < total_size) {
        to_write = std::min(chunk_size - 1, total_size - written);
        pn_buffer_write(buffer, src, to_write);
        written += to_write;
      }

      // Fill a bit over, triggering an allocation
      if (written < total_size) {
        to_write = std::min((size_t) 2, total_size - written);
        pn_buffer_write(buffer, src, to_write);
        written += to_write;
      }

      // Inform the compiler that writes occurred
      benchmark::ClobberMemory();

      // Partial read below chunk size
      read += pn_buffer_read(buffer, chunk_size - 1, dst);

      // Read beyond chunk size
      read += pn_buffer_read(buffer, (size_t) 2, dst);

      // Empty read
      read += pn_buffer_read(buffer, chunk_size, dst);

      // Prevent read optimization
      benchmark::DoNotOptimize(dst);
    }

    pn_buffer_free(buffer);
  }

  // Set throughput metrics (1 write pass + 1 read pass per iteration)
  state.SetBytesProcessed(int64_t(state.iterations()) * int64_t(total_size) * 2);

  free(src);
  free(dst);
}

// Generate power-of-two ranges for total size and chunk size
BENCHMARK(BM_ReadWriteBuffer)
  ->ArgsProduct({
      benchmark::CreateRange(64 * 1024, 4 * 1024 * 1024, 4),
      benchmark::CreateRange(64, 16 * 1024, 4)
  })
  ->ArgNames({"total_bytes", "chunk_size"})
  ->Unit(benchmark::kMillisecond);
