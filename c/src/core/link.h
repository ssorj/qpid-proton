#ifndef PROTON_CORE_LINK_H
#define PROTON_CORE_LINK_H 1

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

#include "core/terminus.h"

typedef struct {
  // XXX: stop using negative numbers
  uint32_t local_handle;
  uint32_t remote_handle;
  pn_sequence_t delivery_count;
  pn_sequence_t link_credit;
} pn_link_state_t;

struct pn_link_t {
  pn_endpoint_t endpoint;
  pn_terminus_t source;
  pn_terminus_t target;
  pn_terminus_t remote_source;
  pn_terminus_t remote_target;
  pn_link_state_t state;
  pn_string_t *name;
  pn_session_t *session;  // reference counted
  pn_delivery_t *unsettled_head;
  pn_delivery_t *unsettled_tail;
  pn_delivery_t *current;
  pn_record_t *context;
  pn_data_t *properties;
  pn_bytes_t properties_raw;
  pn_data_t *remote_properties;
  pn_bytes_t remote_properties_raw;
  size_t unsettled_count;
  uint64_t max_message_size;
  uint64_t remote_max_message_size;
  pn_sequence_t available;
  pn_sequence_t credit;
  pn_sequence_t queued;
  pn_sequence_t more_id;
  int drained; // number of drained credits
  uint8_t snd_settle_mode;
  uint8_t rcv_settle_mode;
  uint8_t remote_snd_settle_mode;
  uint8_t remote_rcv_settle_mode;
  bool drain_flag_mode; // receiver only
  bool drain;
  bool detached;
  bool more_pending;
};

pn_link_t *pn_link_new(int type, pn_session_t *session, pn_string_t *name);
void pn_link_dump(pn_link_t *link);
void pn_link_unbound(pn_link_t* link);

#endif /* link.h */
