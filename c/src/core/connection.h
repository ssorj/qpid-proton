#ifndef PROTON_CORE_CONNECTION_H
#define PROTON_CORE_CONNECTION_H 1

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

#include "proton/connection.h"

#include <assert.h>

#include "proton/types.h"

#include "core/delivery.h"
#include "core/endpoint.h"
#include "core/link.h"
#include "core/object_private.h"
#include "core/session.h"
#include "core/transport.h"
#include "core/util.h"

struct pn_connection_t {
  pni_endpoint_t endpoint;
  pni_endpoint_t *endpoint_head;
  pni_endpoint_t *endpoint_tail;
  pni_endpoint_t *transport_head;  // reference counted
  pni_endpoint_t *transport_tail;
  pn_list_t *sessions;
  pn_list_t *freed;
  pn_transport_t *transport;
  pn_delivery_t *work_head;
  pn_delivery_t *work_tail;
  pn_delivery_t *tpwork_head;  // reference counted
  pn_delivery_t *tpwork_tail;
  pn_string_t *container;
  pn_string_t *hostname;
  pn_string_t *auth_user;
  pn_string_t *authzid;
  pn_string_t *auth_password;
  pn_bytes_t offered_capabilities_raw;
  pn_bytes_t desired_capabilities_raw;
  pn_bytes_t properties_raw;
  pn_data_t *offered_capabilities;
  pn_data_t *desired_capabilities;
  pn_data_t *properties;
  pn_data_t *remote_offered_capabilities;
  pn_data_t *remote_desired_capabilities;
  pn_data_t *remote_properties;
  pn_collector_t *collector;
  pn_record_t *context;
  pn_list_t *delivery_pool;
  struct pn_connection_driver_t *driver;
};

void pni_connection_add_session(pn_connection_t *connection, pn_session_t *ssn);
void pni_connection_remove_session(pn_connection_t *connection, pn_session_t *ssn);
void pni_connection_bound(pn_connection_t *connection);
void pni_connection_unbound(pn_connection_t *connection);
void pni_connection_dump(pn_connection_t *connection);

static inline bool pni_connection_live(pn_connection_t *connection) {
  assert(connection);
  return pn_refcount(connection) > 1;
}

static inline void pni_connection_add_endpoint_work(pn_connection_t *connection, pni_endpoint_t *endpoint, bool emit)
{
  assert(connection);
  assert(endpoint);

  if (!endpoint->modified) {
    LL_ADD(connection, transport, endpoint);
    endpoint->modified = true;
  }

  if (emit && connection->transport) {
    pn_collector_put_object(connection->collector, connection->transport, PN_TRANSPORT);
  }
}

static inline void pni_connection_remove_endpoint_work(pn_connection_t *connection, pni_endpoint_t *endpoint)
{
  assert(connection);
  assert(endpoint);

  if (endpoint->modified) {
    LL_REMOVE(connection, transport, endpoint);
    endpoint->transport_next = NULL;
    endpoint->transport_prev = NULL;
    endpoint->modified = false;
  }
}

static inline void pni_connection_add_delivery_work(pn_connection_t *connection, pn_delivery_t *delivery)
{
  assert(connection);
  assert(delivery);

  if (!delivery->tpwork) {
    LL_ADD(connection, tpwork, delivery);
    delivery->tpwork = true;
  }

  pni_connection_add_endpoint_work(connection, &connection->endpoint, true);
}

static inline void pni_connection_remove_delivery_work(pn_connection_t *connection, pn_delivery_t *delivery)
{
  assert(connection);
  assert(delivery);

  if (delivery->tpwork) {
    LL_REMOVE(connection, tpwork, delivery);
    delivery->tpwork = false;

    if (pn_refcount(delivery) > 0) {
      pn_incref(delivery);
      pn_decref(delivery);
    }
  }
}

static inline void pni_connection_add_legacy_work(pn_connection_t *connection, pn_delivery_t *delivery)
{
  assert(connection);
  assert(delivery);

  if (!delivery->work) {
    LL_ADD(connection, work, delivery);
    delivery->work = true;
  }
}

static inline void pni_connection_remove_legacy_work(pn_connection_t *connection, pn_delivery_t *delivery)
{
  assert(connection);
  assert(delivery);

  if (delivery->work) {
    LL_REMOVE(connection, work, delivery);
    delivery->work = false;
  }
}

static inline void pni_connection_update_legacy_work(pn_connection_t *connection, pn_delivery_t *delivery)
{
  assert(connection);
  assert(delivery);

  pn_link_t *link = pn_delivery_link(delivery);
  pn_delivery_t *current = pn_link_current(link);

  if (delivery->updated && !delivery->local.settled) {
    pni_connection_add_legacy_work(connection, delivery);
  } else if (delivery == current) {
    if (link->endpoint.type == SENDER) {
      if (pn_link_credit(link) > 0) {
        pni_connection_add_legacy_work(connection, delivery);
      } else {
        pni_connection_remove_legacy_work(connection, delivery);
      }
    } else {
      pni_connection_add_legacy_work(connection, delivery);
    }
  } else {
    pni_connection_remove_legacy_work(connection, delivery);
  }
}

#endif /* connection.h */
