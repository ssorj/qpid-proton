#ifndef PROTON_CORE_TERMINUS_H
#define PROTON_CORE_TERMINUS_H 1

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

#include "proton/types.h"

struct pn_terminus_t {
  pn_string_t *address;
  pn_bytes_t properties_raw;
  pn_bytes_t capabilities_raw;
  pn_bytes_t outcomes_raw;
  pn_bytes_t filter_raw;
  pn_data_t *properties;
  pn_data_t *capabilities;
  pn_data_t *outcomes;
  pn_data_t *filter;
  pn_seconds_t timeout;
  uint8_t durability;
  uint8_t expiry_policy;
  uint8_t type;
  uint8_t distribution_mode;
  bool has_expiry_policy;
  bool dynamic;
};

#endif /* terminus.h */
