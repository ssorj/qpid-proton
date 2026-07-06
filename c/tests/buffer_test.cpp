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

//define CATCH_CONFIG_ENABLE_BENCHMARKING
#include "./pn_test.hpp"

#include "core/buffer.h"

// #include <vector>

TEST_CASE("buffer") {
  pn_buffer_t *buf = pn_buffer(0);
  pn_buffer_free(buf);
}

// TEST_CASE("pn_buffer performance benchmarks", "[pn_buffer][benchmark]") {
//     BENCHMARK_ADVANCED("append and drain (stream throughput)")(Catch::Benchmark::Chronometer meter) {
//         pn_buffer_t* buf = pn_buffer(1024);
//         std::vector<char> chunk(512, 'x');
//         std::vector<char> scratch(512);

//         meter.measure([&] {
//             for (int i = 0; i < 1000; ++i) {
//                 pn_buffer_append(buf, chunk.data(), chunk.size());
//                 pn_buffer_pop_left(buf, scratch.size(), scratch.data());
//             }
//         });

//         pn_buffer_free(buf);
//     };

//     BENCHMARK_ADVANCED("linear growth reallocation triggering")(Catch::Benchmark::Chronometer meter) {
//         std::vector<char> chunk(64, 'a');

//         meter.measure([&] {
//             pn_buffer_t* buf = pn_buffer(16);
//             for (int i = 0; i < 500; ++i) {
//                 pn_buffer_append(buf, chunk.data(), chunk.size());
//             }
//             pn_buffer_free(buf);
//         });
//     };

//     BENCHMARK_ADVANCED("buffer_bytes defragmentation cost")(Catch::Benchmark::Chronometer meter) {
//         pn_buffer_t* buf = pn_buffer(1024);
//         std::vector<char> chunk(400, 'b');
//         std::vector<char> scratch(400);

//         pn_buffer_append(buf, chunk.data(), chunk.size());
//         pn_buffer_append(buf, chunk.data(), chunk.size());
//         pn_buffer_pop_left(buf, 500, scratch.data());
//         pn_buffer_append(buf, chunk.data(), chunk.size());

//         meter.measure([&] {
//             return pn_buffer_bytes(buf);
//         });

//         pn_buffer_free(buf);
//     };
// }
