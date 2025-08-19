#ifndef PROTON_CORE_ENDPOINT_H
#define PROTON_CORE_ENDPOINT_H 1

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


#include "proton/event.h"

#include "proton/connection.h"

#include "core/condition.h"

typedef enum pn_endpoint_type_t {
  CONNECTION,
  SESSION,
  SENDER,
  RECEIVER
} pn_endpoint_type_t;

typedef struct pn_endpoint_t pn_endpoint_t;

struct pn_endpoint_t {
  pn_condition_t condition;
  pn_condition_t remote_condition;
  pn_endpoint_t *endpoint_next;
  pn_endpoint_t *endpoint_prev;
  pn_endpoint_t *transport_next;
  pn_endpoint_t *transport_prev;
  int refcount; // when this hits zero we generate a final event
  uint8_t state;
  uint8_t type;
  bool modified;
  bool freed;
  bool referenced;
};

void pn_endpoint_incref(pn_endpoint_t *endpoint);
void pn_endpoint_decref(pn_endpoint_t *endpoint);

void pn_endpoint_open(pn_endpoint_t *endpoint);
void pn_endpoint_close(pn_endpoint_t *endpoint);
void pn_endpoint_init(pn_endpoint_t *endpoint, int type, pn_connection_t *conn);
void pni_endpoint_tini(pn_endpoint_t *endpoint);

bool pni_matches(pn_endpoint_t *endpoint, pn_endpoint_type_t type, pn_state_t state);
pn_endpoint_t *pn_find(pn_endpoint_t *endpoint, pn_endpoint_type_t type, pn_state_t state);

bool pni_preserve_child(pn_endpoint_t *endpoint);
void pni_free_children(pn_list_t *children, pn_list_t *freed);

static inline void pni_set_local_state(uint8_t *state, uint8_t local_state) {
  *state = (*state & PN_REMOTE_MASK) | local_state;
}

static inline void pni_set_remote_state(uint8_t *state, uint8_t remote_state) {
  *state = (*state & PN_LOCAL_MASK) | remote_state;
}

static const pn_event_type_t endpoint_init_event_map[] = {
  PN_CONNECTION_INIT,  /* CONNECTION */
  PN_SESSION_INIT,     /* SESSION */
  PN_LINK_INIT,        /* SENDER */
  PN_LINK_INIT,        /* RECEIVER */
};

#endif /* endpoint.h */
