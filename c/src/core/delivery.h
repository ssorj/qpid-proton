#ifndef PROTON_CORE_DELIVERY_H
#define PROTON_CORE_DELIVERY_H 1

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

#include "core/disposition.h"

typedef struct {
  pn_sequence_t id;
  bool sending;
  bool sent;
  bool init;
} pn_delivery_state_t;

struct pn_delivery_t {
  pn_disposition_t local;
  pn_disposition_t remote;
  pn_delivery_tag_t tag;
  pn_link_t *link;  // reference counted
  pn_delivery_t *unsettled_next;
  pn_delivery_t *unsettled_prev;
  pn_delivery_t *work_next;
  pn_delivery_t *work_prev;
  pn_delivery_t *tpwork_next;
  pn_delivery_t *tpwork_prev;
  pn_delivery_state_t state;
  pn_buffer_t *bytes;
  pn_record_t *context;
  bool updated;
  bool settled; // tracks whether we're in the unsettled list or not
  bool work;
  bool tpwork;
  bool done;
  bool referenced;
  bool aborted;
};

void pn_real_settle(pn_delivery_t *delivery);  // will free delivery if link is freed
void pn_clear_tpwork(pn_delivery_t *delivery);

#endif /* delivery.h */
