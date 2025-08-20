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

#include "core/endpoint.h"

#include <assert.h>

#include "proton/event.h"

#include "core/connection.h"
#include "core/link.h"
#include "core/session.h"

static pn_connection_t *pni_ep_get_connection(pn_endpoint_t *endpoint)
{
  switch (endpoint->type) {
  case CONNECTION:
    return (pn_connection_t *) endpoint;
  case SESSION:
    return ((pn_session_t *) endpoint)->connection;
  case SENDER:
  case RECEIVER:
    return ((pn_link_t *) endpoint)->session->connection;
  }

  return NULL;
}

static pn_event_type_t endpoint_event(pn_endpoint_type_t type, bool open) {
  switch (type) {
  case CONNECTION:
    return open ? PN_CONNECTION_LOCAL_OPEN : PN_CONNECTION_LOCAL_CLOSE;
  case SESSION:
    return open ? PN_SESSION_LOCAL_OPEN : PN_SESSION_LOCAL_CLOSE;
  case SENDER:
  case RECEIVER:
    return open ? PN_LINK_LOCAL_OPEN : PN_LINK_LOCAL_CLOSE;
  default:
    assert(false);
    return PN_EVENT_NONE;
  }
}

void pn_endpoint_open(pn_endpoint_t *endpoint)
{
  if (!(endpoint->state & PN_LOCAL_ACTIVE)) {
    pni_set_local_state(&endpoint->state, PN_LOCAL_ACTIVE);
    pn_connection_t *conn = pni_ep_get_connection(endpoint);
    pn_collector_put_object(conn->collector, endpoint,
                            endpoint_event((pn_endpoint_type_t) endpoint->type, true));
    pni_connection_add_endpoint_work(conn, endpoint, true);
  }
}

void pn_endpoint_close(pn_endpoint_t *endpoint)
{
  if (!(endpoint->state & PN_LOCAL_CLOSED)) {
    pni_set_local_state(&endpoint->state, PN_LOCAL_CLOSED);
    pn_connection_t *conn = pni_ep_get_connection(endpoint);
    pn_collector_put_object(conn->collector, endpoint,
                            endpoint_event((pn_endpoint_type_t) endpoint->type, false));
    pni_connection_add_endpoint_work(conn, endpoint, true);
  }
}

void pn_endpoint_init(pn_endpoint_t *endpoint, int type, pn_connection_t *conn)
{
  endpoint->type = (pn_endpoint_type_t) type;
  endpoint->referenced = true;
  endpoint->state = PN_LOCAL_UNINIT | PN_REMOTE_UNINIT;
  pn_condition_init(&endpoint->condition);
  pn_condition_init(&endpoint->remote_condition);
  endpoint->endpoint_next = NULL;
  endpoint->endpoint_prev = NULL;
  endpoint->transport_next = NULL;
  endpoint->transport_prev = NULL;
  endpoint->modified = false;
  endpoint->freed = false;
  endpoint->refcount = 1;
  //fprintf(stderr, "initting 0x%lx\n", (uintptr_t) endpoint);

  LL_ADD(conn, endpoint, endpoint);
}

static pn_event_type_t pn_final_type(pn_endpoint_type_t type) {
  switch (type) {
  case CONNECTION:
    return PN_CONNECTION_FINAL;
  case SESSION:
    return PN_SESSION_FINAL;
  case SENDER:
  case RECEIVER:
    return PN_LINK_FINAL;
  default:
    assert(false);
    return PN_EVENT_NONE;
  }
}

void pn_endpoint_incref(pn_endpoint_t *endpoint)
{
  endpoint->refcount++;
}

void pn_endpoint_decref(pn_endpoint_t *endpoint)
{
  assert(endpoint->refcount > 0);
  endpoint->refcount--;
  if (endpoint->refcount == 0) {
    pn_connection_t *conn = pni_ep_get_connection(endpoint);
    pn_collector_put_object(conn->collector, endpoint, pn_final_type((pn_endpoint_type_t) endpoint->type));
  }
}

static pn_endpoint_t *pn_ep_parent(pn_endpoint_t *endpoint) {
  switch (endpoint->type) {
  case CONNECTION:
    return NULL;
  case SESSION:
    return &((pn_session_t *) endpoint)->connection->endpoint;
  case SENDER:
  case RECEIVER:
    return &((pn_link_t *) endpoint)->session->endpoint;
  default:
    assert(false);
    return NULL;
  }
}

void pni_endpoint_tini(pn_endpoint_t *endpoint)
{
  pn_condition_tini(&endpoint->remote_condition);
  pn_condition_tini(&endpoint->condition);
}

void pni_free_children(pn_list_t *children, pn_list_t *freed)
{
  while (pn_list_size(children) > 0) {
    pn_endpoint_t *endpoint = (pn_endpoint_t *) pn_list_get(children, 0);
    assert(!endpoint->referenced);
    pn_free(endpoint);
  }

  while (pn_list_size(freed) > 0) {
    pn_endpoint_t *endpoint = (pn_endpoint_t *) pn_list_get(freed, 0);
    assert(!endpoint->referenced);
    pn_free(endpoint);
  }

  pn_free(children);
  pn_free(freed);
}

// static const pn_event_type_t endpoint_init_event_map[] = {
//   PN_CONNECTION_INIT,  /* CONNECTION */
//   PN_SESSION_INIT,     /* SESSION */
//   PN_LINK_INIT,        /* SENDER */
//   PN_LINK_INIT,        /* RECEIVER */
// };

bool pni_matches(pn_endpoint_t *endpoint, pn_endpoint_type_t type, pn_state_t state)
{
  if (endpoint->type != type) return false;

  if (!state) return true;

  int st = endpoint->state;
  if ((state & PN_REMOTE_MASK) == 0 || (state & PN_LOCAL_MASK) == 0)
    return st & state;
  else
    return st == state;
}

pn_endpoint_t *pn_find(pn_endpoint_t *endpoint, pn_endpoint_type_t type, pn_state_t state)
{
  while (endpoint)
  {
    if (pni_matches(endpoint, type, state))
      return endpoint;
    endpoint = endpoint->endpoint_next;
  }
  return NULL;
}

static bool pn_ep_bound(pn_endpoint_t *endpoint)
{
  pn_connection_t *conn = pni_ep_get_connection(endpoint);
  pn_session_t *ssn;
  pn_link_t *lnk;

  if (!conn->transport) return false;
  if (endpoint->modified) return true;

  switch (endpoint->type) {
  case CONNECTION:
    return ((pn_connection_t *)endpoint)->transport;
  case SESSION:
    ssn = (pn_session_t *) endpoint;
    return (((int16_t) ssn->state.local_channel) >= 0 || ((int16_t) ssn->state.remote_channel) >= 0);
  case SENDER:
  case RECEIVER:
    lnk = (pn_link_t *) endpoint;
    return ((int32_t) lnk->state.local_handle) >= 0 || ((int32_t) lnk->state.remote_handle) >= 0;
  default:
    assert(false);
    return false;
  }
}

static bool pni_endpoint_live(pn_endpoint_t *endpoint) {
  switch (endpoint->type) {
  case CONNECTION:
    return pni_connection_live((pn_connection_t *)endpoint);
  case SESSION:
    return pni_session_live((pn_session_t *) endpoint);
  case SENDER:
  case RECEIVER:
    return pni_link_live((pn_link_t *) endpoint);
  default:
    assert(false);
    return false;
  }
}

bool pni_preserve_child(pn_endpoint_t *endpoint)
{
  pn_connection_t *conn = pni_ep_get_connection(endpoint);
  pn_endpoint_t *parent = pn_ep_parent(endpoint);
  if (pni_endpoint_live(parent) && (!endpoint->freed || (pn_ep_bound(endpoint)))
      && endpoint->referenced) {
    pn_object_incref(endpoint);
    endpoint->referenced = false;
    pn_decref(parent);
    return true;
  } else {
    LL_REMOVE(conn, transport, endpoint);
    return false;
  }
}
