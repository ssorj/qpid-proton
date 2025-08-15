#ifndef PROTON_CORE_CONDITION_H
#define PROTON_CORE_CONDITION_H 1

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

#include "proton/condition.h"

#include "proton/types.h"

#include "core/object_private.h"

struct pn_condition_t {
  pn_bytes_t info_raw;
  pn_string_t *name;
  pn_string_t *description;
  pn_data_t *info;
};

void pn_condition_init(pn_condition_t *condition);
void pn_condition_tini(pn_condition_t *condition);

#endif /* condition.h */
