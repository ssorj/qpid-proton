#ifndef PROTON_CORE_SESSION_H
#define PROTON_CORE_SESSION_H 1

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

#include "proton/session.h"

#include <assert.h>

#include "proton/connection.h"
#include "core/endpoint.h"

typedef struct {
  pn_sequence_t next;
  pn_hash_t *deliveries;
} pn_delivery_map_t;

typedef struct {
  pn_delivery_map_t incoming;
  pn_delivery_map_t outgoing;
  pn_hash_t *local_handles;
  pn_hash_t *remote_handles;
  uint64_t disp_code;
  pn_sequence_t incoming_transfer_count;
  pn_sequence_t incoming_window;
  pn_sequence_t remote_incoming_window;
  pn_sequence_t outgoing_transfer_count;
  pn_sequence_t outgoing_window;
  pn_sequence_t disp_first;
  pn_sequence_t disp_last;
  // XXX: stop using negative numbers
#define PN_IMPL_HANDLE_MAX 0x7fffffff
  uint32_t remote_handle_max;
  uint16_t local_channel;
  uint16_t remote_channel;
  bool incoming_init;
  bool disp;
  bool disp_settled;
  bool disp_type;
} pn_session_state_t;

struct pn_session_t {
  pni_endpoint_t endpoint;
  pn_session_state_t state;
  pn_connection_t *connection;  // reference counted
  pn_list_t *links;
  pn_list_t *freed;
  pn_record_t *context;
  size_t incoming_capacity;
  uint32_t local_handle_max;
  pn_sequence_t incoming_bytes;
  pn_sequence_t outgoing_bytes;
  pn_sequence_t incoming_deliveries;
  pn_sequence_t outgoing_deliveries;
  pn_sequence_t outgoing_window;
  pn_frame_count_t incoming_window_lwm;
  pn_frame_count_t max_incoming_window;
  bool check_flow;
  bool need_flow;
  bool lwm_default;
};

void pn_delivery_map_init(pn_delivery_map_t *db, pn_sequence_t next);
void pn_delivery_map_del(pn_delivery_map_t *db, pn_delivery_t *delivery);
void pn_delivery_map_free(pn_delivery_map_t *db);

void pni_session_bound(pn_session_t *session);
void pni_session_unbound(pn_session_t *session);
void pni_session_update_incoming_lwm(pn_session_t *session);

static inline bool pni_connection_live(pn_connection_t *connection);

static inline bool pni_session_live(pn_session_t *session) {
  assert(session);
  return pni_connection_live(session->connection) || pn_refcount(session) > 1;
}

void pni_session_add_link(pn_session_t *session, pn_link_t *link);
void pni_session_remove_link(pn_session_t *session, pn_link_t *link);

#endif /* session.h */
