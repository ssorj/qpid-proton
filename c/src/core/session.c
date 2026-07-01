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

#include "core/session.h"

#include <assert.h>

#include "core/connection.h"
#include "core/framing.h"
#include "core/link.h"

pn_connection_t *pn_session_connection(pn_session_t *session)
{
  if (!session) return NULL;
  return session->connection;
}

void pn_session_open(pn_session_t *session)
{
  assert(session);
  pn_endpoint_open(&session->endpoint);
}

void pn_session_close(pn_session_t *session)
{
  assert(session);
  pn_endpoint_close(&session->endpoint);
}

void pn_session_free(pn_session_t *session)
{
  assert(!session->endpoint.freed);
  while(pn_list_size(session->links)) {
    pn_link_t *link = (pn_link_t *)pn_list_get(session->links, 0);
    pn_link_free(link);
  }
  pni_connection_remove_session(session->connection, session);
  pn_list_add(session->connection->freed, session);
  session->endpoint.freed = true;
  pn_endpoint_decref(&session->endpoint);

  // the finalize logic depends on endpoint.freed, so we incref/decref
  // to give it a chance to rerun
  pn_incref(session);
  pn_decref(session);
}

pn_record_t *pn_session_attachments(pn_session_t *session)
{
  assert(session);
  return session->context;
}

void *pn_session_get_context(pn_session_t *session)
{
  return session ? pn_record_get(session->context, PN_LEGCTX) : 0;
}

void pn_session_set_context(pn_session_t *session, void *context)
{
  assert(session);
  pn_record_set(session->context, PN_LEGCTX, context);
}


void pni_add_link(pn_session_t *ssn, pn_link_t *link)
{
  pn_list_add(ssn->links, link);
  link->session = ssn;
  pn_endpoint_incref(&ssn->endpoint);
}

void pni_remove_link(pn_session_t *ssn, pn_link_t *link)
{
  if (pn_list_remove(ssn->links, link)) {
    pn_endpoint_decref(&ssn->endpoint);
    LL_REMOVE(ssn->connection, endpoint, &link->endpoint);
  }
}

pn_session_t *pn_session_head(pn_connection_t *conn, pn_state_t state)
{
  if (conn)
    return (pn_session_t *) pn_find(conn->endpoint_head, SESSION, state);
  else
    return NULL;
}

pn_session_t *pn_session_next(pn_session_t *ssn, pn_state_t state)
{
  if (ssn)
    return (pn_session_t *) pn_find(ssn->endpoint.endpoint_next, SESSION, state);
  else
    return NULL;
}


static void pn_session_incref(void *object)
{
  pn_session_t *session = (pn_session_t *) object;
  if (!session->endpoint.referenced) {
    session->endpoint.referenced = true;
    pn_incref(session->connection);
  } else {
    pn_object_incref(object);
  }
}

static void pn_session_finalize(void *object)
{
  pn_session_t *session = (pn_session_t *) object;
  pn_endpoint_t *endpoint = &session->endpoint;

  if (pni_preserve_child(endpoint)) {
    return;
  }

  pn_free(session->context);
  pni_free_children(session->links, session->freed);
  pni_endpoint_tini(endpoint);
  pn_delivery_map_free(&session->state.incoming);
  pn_delivery_map_free(&session->state.outgoing);
  pn_free(session->state.local_handles);
  pn_free(session->state.remote_handles);
  pni_connection_remove_session(session->connection, session);
  pn_list_remove(session->connection->freed, session);

  if (session->connection->transport) {
    pn_transport_t *transport = session->connection->transport;
    pn_hash_del(transport->local_channels, session->state.local_channel);
    pn_hash_del(transport->remote_channels, session->state.remote_channel);
  }

  if (endpoint->referenced) {
    pn_decref(session->connection);
  }
}

#define pn_session_new NULL
#define pn_session_refcount NULL
#define pn_session_decref NULL
#define pn_session_initialize NULL
#define pn_session_hashcode NULL
#define pn_session_compare NULL
#define pn_session_inspect NULL

pn_session_t *pn_session(pn_connection_t *conn)
{
  assert(conn);

#define pn_session_free NULL
  static const pn_class_t clazz = PN_METACLASS(pn_session);
#undef pn_session_free

  pn_session_t *ssn = (pn_session_t *) pn_class_new(&clazz, sizeof(pn_session_t));
  if (!ssn) return NULL;

  *ssn = (pn_session_t) {
    .links = pn_list(PN_WEAKREF, 0),
    .freed = pn_list(PN_WEAKREF, 0),
    .context = pn_record(),
    .outgoing_window = AMQP_MAX_WINDOW_SIZE,
    .local_handle_max = PN_IMPL_HANDLE_MAX,
    .incoming_window_lwm = 1,
    .lwm_default = true,
    .state = {
      .remote_handle_max = UINT32_MAX,
      .local_channel = (uint16_t) - 1,
      .remote_channel = (uint16_t) - 1,
      .local_handles = pn_hash(PN_WEAKREF, 0, 0.75),
      .remote_handles = pn_hash(PN_WEAKREF, 0, 0.75),
    },
  };

  pn_delivery_map_init(&ssn->state.incoming, 0);
  pn_delivery_map_init(&ssn->state.outgoing, 0);

  pn_endpoint_init(&ssn->endpoint, SESSION, conn);
  pni_connection_add_session(conn, ssn);

  pn_collector_put_object(conn->collector, ssn, PN_SESSION_INIT);

  if (conn->transport) {
    pni_session_bound(ssn);
  }

  pn_decref(ssn);

  return ssn;
}

void pni_session_bound(pn_session_t *ssn)
{
  assert(ssn);
  size_t nlinks = pn_list_size(ssn->links);
  for (size_t i = 0; i < nlinks; i++) {
    pni_link_bound((pn_link_t *) pn_list_get(ssn->links, i));
  }
}

void pn_session_unbound(pn_session_t* ssn)
{
  assert(ssn);
  ssn->state.local_channel = (uint16_t)-1;
  ssn->state.remote_channel = (uint16_t)-1;
  ssn->incoming_bytes = 0;
  ssn->outgoing_bytes = 0;
  ssn->incoming_deliveries = 0;
  ssn->outgoing_deliveries = 0;
}

size_t pn_session_get_incoming_capacity(pn_session_t *ssn)
{
  assert(ssn);
  return ssn->incoming_capacity;
}

// Update required when (re)set by user or when session started (proxy: BEGIN frame).  No
// session flow control actually means flow control with huge window, so set lwm to 1.  There is
// low probability of a stall.  Any link credit flow frame will update session credit too.
void pni_session_update_incoming_lwm(pn_session_t *ssn) {
  if (ssn->incoming_capacity) {
    // Old API.
    if (!ssn->connection->transport)
      return; // Defer until called again from BEGIN frame setup with max frame known.
    if (ssn->connection->transport->local_max_frame) {
      ssn->incoming_window_lwm = (ssn->incoming_capacity / ssn->connection->transport->local_max_frame) / 2;
      if (!ssn->incoming_window_lwm)
        ssn->incoming_window_lwm = 1; // Zero may hang.
    } else {
      ssn->incoming_window_lwm = 1;
    }
  } else if (ssn->max_incoming_window) {
    // New API.
    // Only need to deal with default.  Called whensending BEGIN frame.
    if (ssn->connection->transport && ssn->connection->transport->local_max_frame && ssn->lwm_default) {
      ssn->incoming_window_lwm = (ssn->max_incoming_window + 1) / 2;
    }
  } else {
    ssn->incoming_window_lwm = 1;
  }
  assert(ssn->incoming_window_lwm != 0);  // 0 allows session flow to hang
}

void pn_session_set_incoming_capacity(pn_session_t *ssn, size_t capacity)
{
  assert(ssn);
  ssn->incoming_capacity = capacity;
  ssn->max_incoming_window = 0;
  ssn->incoming_window_lwm = 1;
  ssn->lwm_default = true;
  if (ssn->connection->transport) {
    ssn->check_flow = true;
    ssn->need_flow = true;
    pni_connection_add_endpoint_work(ssn->connection, &ssn->endpoint, false);
  }
  pni_session_update_incoming_lwm(ssn);
  // If capacity invalid, failure occurs when transport calculates value of incoming window.
}

int pn_session_set_incoming_window_and_lwm(pn_session_t *ssn, pn_frame_count_t window, pn_frame_count_t lwm) {
  assert(ssn);
  if (!window || (lwm && lwm > window))
    return PN_ARG_ERR;
  // Settings fixed after session open for simplicity.  AMPQ actually allows dynamic change with risk
  // of overflow if window reduced while transfers in flight.
  if (ssn->endpoint.state & PN_LOCAL_ACTIVE)
    return PN_STATE_ERR;
  ssn->incoming_capacity = 0;
  ssn->max_incoming_window = window;
  ssn->lwm_default = (lwm == 0);
  ssn->incoming_window_lwm = lwm;
  return 0;
}

pn_frame_count_t pn_session_incoming_window(pn_session_t *ssn) {
  return ssn->max_incoming_window;
}

pn_frame_count_t pn_session_incoming_window_lwm(pn_session_t *ssn) {
  return (!ssn->max_incoming_window || ssn->lwm_default) ? 0 : ssn->incoming_window_lwm;
}

pn_frame_count_t pn_session_remote_incoming_window(pn_session_t *ssn) {
  return ssn->state.remote_incoming_window;
}

size_t pn_session_get_outgoing_window(pn_session_t *ssn)
{
  assert(ssn);
  return ssn->outgoing_window;
}

void pn_session_set_outgoing_window(pn_session_t *ssn, size_t window)
{
  assert(ssn);
  ssn->outgoing_window = window;
}

size_t pn_session_outgoing_bytes(pn_session_t *ssn)
{
  assert(ssn);
  return ssn->outgoing_bytes;
}

size_t pn_session_incoming_bytes(pn_session_t *ssn)
{
  assert(ssn);
  return ssn->incoming_bytes;
}

pn_state_t pn_session_state(pn_session_t *session)
{
  return session->endpoint.state;
}

pn_condition_t *pn_session_condition(pn_session_t *session)
{
  assert(session);
  return &session->endpoint.condition;
}

pn_condition_t *pn_session_remote_condition(pn_session_t *session)
{
  assert(session);
  return &session->endpoint.remote_condition;
}
