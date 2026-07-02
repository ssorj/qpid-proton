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

typedef enum pni_endpoint_type_t {
  CONNECTION,
  SESSION,
  SENDER,
  RECEIVER
} pni_endpoint_type_t;

typedef struct pni_endpoint_t pni_endpoint_t;

struct pni_endpoint_t {
  pn_condition_t condition;
  pn_condition_t remote_condition;
  pni_endpoint_t *endpoint_next;
  pni_endpoint_t *endpoint_prev;
  pni_endpoint_t *transport_next;
  pni_endpoint_t *transport_prev;
  int refcount; // when this hits zero we generate a final event
  uint8_t state;
  uint8_t type;
  bool modified;
  bool freed;
  bool referenced;
};

void pni_endpoint_incref(pni_endpoint_t *endpoint);
void pni_endpoint_decref(pni_endpoint_t *endpoint);

void pni_endpoint_open(pni_endpoint_t *endpoint);
void pni_endpoint_close(pni_endpoint_t *endpoint);
void pni_endpoint_init(pni_endpoint_t *endpoint, int type, pn_connection_t *conn);
void pni_endpoint_tini(pni_endpoint_t *endpoint);

pni_endpoint_t *pni_endpoint_find(pni_endpoint_t *endpoint, pni_endpoint_type_t type, pn_state_t state);

bool pni_endpoint_preserve_child(pni_endpoint_t *endpoint);
void pni_endpoint_free_children(pn_list_t *children, pn_list_t *freed);

static const pn_event_type_t endpoint_init_event_map[] = {
  PN_CONNECTION_INIT,  /* CONNECTION */
  PN_SESSION_INIT,     /* SESSION */
  PN_LINK_INIT,        /* SENDER */
  PN_LINK_INIT,        /* RECEIVER */
};

#endif /* endpoint.h */
