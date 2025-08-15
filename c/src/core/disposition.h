#ifndef PROTON_CORE_DISPOSITION_H
#define PROTON_CORE_DISPOSITION_H 1

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

typedef enum pn_disposition_type_t {
  PN_DISP_EMPTY = 0,
  PN_DISP_CUSTOM = 1,
  PN_DISP_RECEIVED = PN_RECEIVED,
  PN_DISP_ACCEPTED = PN_ACCEPTED,
  PN_DISP_REJECTED = PN_REJECTED,
  PN_DISP_RELEASED = PN_RELEASED,
  PN_DISP_MODIFIED = PN_MODIFIED,
  PN_DISP_DECLARED = PN_DECLARED,
  PN_DISP_TRANSACTIONAL = PN_TRANSACTIONAL_STATE,
} pn_disposition_type_t;

struct pn_received_disposition_t {
  uint64_t section_offset;
  uint32_t section_number;
};

struct pn_rejected_disposition_t {
  pn_condition_t condition;
};

struct pn_modified_disposition_t {
  pn_data_t *annotations;
  pn_bytes_t annotations_raw;
  bool failed;
  bool undeliverable;
};

struct pn_declared_disposition_t {
  pn_bytes_t id;
};

struct pn_transactional_disposition_t {
  pn_bytes_t id;
  pn_bytes_t outcome_raw;
};

struct pn_custom_disposition_t {
  pn_data_t *data;
  pn_bytes_t data_raw;
  uint64_t   type;
};

struct pn_disposition_t {
  union {
    struct pn_received_disposition_t s_received;
    struct pn_rejected_disposition_t s_rejected;
    struct pn_modified_disposition_t s_modified;
    struct pn_declared_disposition_t s_declared;
    struct pn_transactional_disposition_t s_transactional;
    struct pn_custom_disposition_t s_custom;
  } u;
  uint16_t type;
  bool settled;
};

void pn_disposition_clear(pn_disposition_t *ds);

#endif /* disposition.h */
