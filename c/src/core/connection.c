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

#include "core/connection.h"

#include <assert.h>

#include "proton/event.h"

#include "core/delivery.h"
#include "core/endpoint.h"
#include "core/session.h"
#include "core/util.h"

#define pn_connection_initialize NULL
static void pn_connection_finalize(void *object);
#define pn_connection_hashcode NULL
#define pn_connection_compare NULL
#define pn_connection_inspect NULL

pn_connection_t *pn_connection(void)
{
  static const pn_class_t clazz = PN_CLASS(pn_connection);
  pn_connection_t *conn = (pn_connection_t *) pn_class_new(&clazz, sizeof(pn_connection_t));
  if (!conn) return NULL;

  conn->endpoint_head = NULL;
  conn->endpoint_tail = NULL;
  pn_endpoint_init(&conn->endpoint, CONNECTION, conn);
  conn->transport_head = NULL;
  conn->transport_tail = NULL;
  conn->sessions = pn_list(PN_WEAKREF, 0);
  conn->freed = pn_list(PN_WEAKREF, 0);
  conn->transport = NULL;
  conn->work_head = NULL;
  conn->work_tail = NULL;
  conn->tpwork_head = NULL;
  conn->tpwork_tail = NULL;
  conn->container = pn_string(NULL);
  conn->hostname = pn_string(NULL);
  conn->auth_user = pn_string(NULL);
  conn->authzid = pn_string(NULL);
  conn->auth_password = pn_string(NULL);
  conn->offered_capabilities_raw = (pn_bytes_t){0, NULL};
  conn->desired_capabilities_raw = (pn_bytes_t){0, NULL};
  conn->properties_raw = (pn_bytes_t){0, NULL};
  conn->offered_capabilities = NULL;
  conn->desired_capabilities = NULL;
  conn->properties = NULL;
  conn->remote_offered_capabilities = NULL;
  conn->remote_desired_capabilities = NULL;
  conn->remote_properties = NULL;
  conn->collector = NULL;
  conn->context = pn_record();
  conn->delivery_pool = pn_list(&PN_CLASSCLASS(pn_delivery), 0);
  conn->driver = NULL;

  return conn;
}

static void pn_connection_finalize(void *object)
{
  pn_connection_t *conn = (pn_connection_t *) object;
  pn_endpoint_t *endpoint = &conn->endpoint;

  if (conn->transport) {
    assert(!conn->transport->referenced);
    pn_free(conn->transport);
  }

  // freeing the transport could post events
  if (pn_refcount(conn) > 0) {
    return;
  }

  pni_free_children(conn->sessions, conn->freed);
  pn_free(conn->context);
  pn_decref(conn->collector);

  pn_free(conn->container);
  pn_free(conn->hostname);
  pn_free(conn->auth_user);
  pn_free(conn->authzid);
  pn_free(conn->auth_password);
  pn_bytes_free(conn->offered_capabilities_raw);
  pn_bytes_free(conn->desired_capabilities_raw);
  pn_bytes_free(conn->properties_raw);
  pn_free(conn->offered_capabilities);
  pn_free(conn->desired_capabilities);
  pn_free(conn->properties);
  pn_free(conn->remote_offered_capabilities);
  pn_free(conn->remote_desired_capabilities);
  pn_free(conn->remote_properties);
  pni_endpoint_tini(endpoint);
  pn_free(conn->delivery_pool);
}

void pn_connection_collect(pn_connection_t *connection, pn_collector_t *collector)
{
  pn_decref(connection->collector);
  connection->collector = collector;
  pn_incref(connection->collector);
  pn_endpoint_t *endpoint = connection->endpoint_head;
  while (endpoint) {
    pn_collector_put_object(connection->collector, endpoint, endpoint_init_event_map[endpoint->type]);
    endpoint = endpoint->endpoint_next;
  }
}

pn_collector_t* pn_connection_collector(pn_connection_t *connection) {
  return connection->collector;
}

pn_state_t pn_connection_state(pn_connection_t *connection)
{
  return connection ? connection->endpoint.state : 0;
}

const char *pn_connection_get_container(pn_connection_t *connection)
{
  assert(connection);
  return pn_string_get(connection->container);
}

void pn_connection_set_container(pn_connection_t *connection, const char *container)
{
  assert(connection);
  pn_string_set(connection->container, container);
}

const char *pn_connection_get_hostname(pn_connection_t *connection)
{
  assert(connection);
  return pn_string_get(connection->hostname);
}

void pn_connection_set_hostname(pn_connection_t *connection, const char *hostname)
{
  assert(connection);
  pn_string_set(connection->hostname, hostname);
}

const char *pn_connection_get_user(pn_connection_t *connection)
{
    assert(connection);
    return pn_string_get(connection->auth_user);
}

void pn_connection_set_user(pn_connection_t *connection, const char *user)
{
    assert(connection);
    pn_string_set(connection->auth_user, user);
}

const char *pn_connection_get_authorization(pn_connection_t *connection)
{
  assert(connection);
  return pn_string_get(connection->authzid);
}

void pn_connection_set_authorization(pn_connection_t *connection, const char *authzid)
{
  assert(connection);
  pn_string_set(connection->authzid, authzid);
}

void pn_connection_set_password(pn_connection_t *connection, const char *password)
{
    assert(connection);
    // Make sure the previous password is erased, if there was one.
    size_t n = pn_string_size(connection->auth_password);
    const char* s = pn_string_get(connection->auth_password);
    if (n > 0 && s) memset((void*)s, 0, n);
    pn_string_set(connection->auth_password, password);
}

pn_data_t *pn_connection_offered_capabilities(pn_connection_t *connection)
{
  assert(connection);
  pni_switch_to_data(&connection->offered_capabilities_raw, &connection->offered_capabilities);
  return connection->offered_capabilities;
}

pn_data_t *pn_connection_desired_capabilities(pn_connection_t *connection)
{
  assert(connection);
  pni_switch_to_data(&connection->desired_capabilities_raw, &connection->desired_capabilities);
  return connection->desired_capabilities;
}

pn_data_t *pn_connection_properties(pn_connection_t *connection)
{
  assert(connection);
  pni_switch_to_data(&connection->properties_raw, &connection->properties);
  return connection->properties;
}

pn_data_t *pn_connection_remote_offered_capabilities(pn_connection_t *connection)
{
  assert(connection);
  if (!connection->transport)
    return NULL;
  pni_switch_to_data(&connection->transport->remote_offered_capabilities_raw, &connection->remote_offered_capabilities);
  return connection->remote_offered_capabilities;
}

pn_data_t *pn_connection_remote_desired_capabilities(pn_connection_t *connection)
{
  assert(connection);
  if (!connection->transport)
    return NULL;
  pni_switch_to_data(&connection->transport->remote_desired_capabilities_raw, &connection->remote_desired_capabilities);
  return connection->remote_desired_capabilities;
}

pn_data_t *pn_connection_remote_properties(pn_connection_t *connection)
{
  assert(connection);
  if (!connection->transport)
    return NULL;
  pni_switch_to_data(&connection->transport->remote_properties_raw, &connection->remote_properties);
  return connection->remote_properties;
}

const char *pn_connection_remote_container(pn_connection_t *connection)
{
  assert(connection);
  return connection->transport ? connection->transport->remote_container : NULL;
}

const char *pn_connection_remote_hostname(pn_connection_t *connection)
{
  assert(connection);
  return connection->transport ? connection->transport->remote_hostname : NULL;
}

void pn_connection_reset(pn_connection_t *connection)
{
  assert(connection);
  pn_endpoint_t *endpoint = &connection->endpoint;
  endpoint->state = PN_LOCAL_UNINIT | PN_REMOTE_UNINIT;
}

void pn_connection_open(pn_connection_t *connection)
{
  assert(connection);
  pn_endpoint_open(&connection->endpoint);
}

void pn_connection_close(pn_connection_t *connection)
{
  assert(connection);
  pn_endpoint_close(&connection->endpoint);
}

void pn_connection_release(pn_connection_t *connection)
{
  assert(!connection->endpoint.freed);
  // free those endpoints that haven't been freed by the application
  LL_REMOVE(connection, endpoint, &connection->endpoint);
  while (connection->endpoint_head) {
    pn_endpoint_t *ep = connection->endpoint_head;
    switch (ep->type) {
    case SESSION:
      // note: this will free all child links:
      pn_session_free((pn_session_t *)ep);
      break;
    case SENDER:
    case RECEIVER:
      pn_link_free((pn_link_t *)ep);
      break;
    default:
      assert(false);
    }
  }
  connection->endpoint.freed = true;
  if (!connection->transport) {
    // no transport available to consume transport work items,
    // so manually clear them:
    pn_endpoint_incref(&connection->endpoint);
    pni_connection_unbound(connection);
  }
  pn_endpoint_decref(&connection->endpoint);
}

void pn_connection_free(pn_connection_t *connection) {
  pn_connection_release(connection);
  pn_decref(connection);
}

void pni_connection_bound(pn_connection_t *connection)
{
  pn_collector_put_object(connection->collector, connection, PN_CONNECTION_BOUND);
  pn_endpoint_incref(&connection->endpoint);

  size_t nsessions = pn_list_size(connection->sessions);
  for (size_t i = 0; i < nsessions; i++) {
    pni_session_bound((pn_session_t *) pn_list_get(connection->sessions, i));
  }
}

// invoked when transport has been removed:
void pni_connection_unbound(pn_connection_t *connection)
{
  connection->transport = NULL;
  if (connection->endpoint.freed) {
    // connection has been freed prior to unbinding, thus it
    // cannot be re-assigned to a new transport.  Clear the
    // transport work lists to allow the connection to be freed.
    while (connection->transport_head) {
      pni_connection_remove_endpoint_work(connection, connection->transport_head);
    }
    while (connection->tpwork_head) {
      pni_connection_remove_delivery_work(connection, connection->tpwork_head);
    }
  }
  pn_endpoint_decref(&connection->endpoint);
}

pn_record_t *pn_connection_attachments(pn_connection_t *connection)
{
  assert(connection);
  return connection->context;
}

void *pn_connection_get_context(pn_connection_t *conn)
{
  // XXX: we should really assert on conn here, but this causes
  // messenger tests to fail
  return conn ? pn_record_get(conn->context, PN_LEGCTX) : NULL;
}

void pn_connection_set_context(pn_connection_t *conn, void *context)
{
  assert(conn);
  pn_record_set(conn->context, PN_LEGCTX, context);
}

pn_transport_t *pn_connection_transport(pn_connection_t *connection)
{
  assert(connection);
  return connection->transport;
}

pn_condition_t *pn_connection_condition(pn_connection_t *connection)
{
  assert(connection);
  return &connection->endpoint.condition;
}

pn_condition_t *pn_connection_remote_condition(pn_connection_t *connection)
{
  assert(connection);
  pn_transport_t *transport = connection->transport;
  return transport ? &transport->remote_condition : NULL;
}

void pni_connection_add_session(pn_connection_t *conn, pn_session_t *ssn)
{
  pn_list_add(conn->sessions, ssn);
  ssn->connection = conn;
  pn_incref(conn);  // keep around until finalized
  pn_endpoint_incref(&conn->endpoint);
}

void pni_connection_remove_session(pn_connection_t *conn, pn_session_t *ssn)
{
  if (pn_list_remove(conn->sessions, ssn)) {
    pn_endpoint_decref(&conn->endpoint);
    LL_REMOVE(conn, endpoint, &ssn->endpoint);
  }
}

void pni_connection_dump(pn_connection_t *conn)
{
  pn_endpoint_t *endpoint = conn->transport_head;
  while (endpoint)
  {
    printf("%p", (void *) endpoint);
    endpoint = endpoint->transport_next;
    if (endpoint)
      printf(" -> ");
  }
  printf("\n");
}
