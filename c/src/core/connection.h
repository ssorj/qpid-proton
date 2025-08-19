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

#include "proton/types.h"

#include "core/delivery.h"
#include "core/endpoint.h"
#include "core/link.h"
#include "core/object_private.h"
#include "core/session.h"
#include "core/transport.h"
#include "core/util.h"

struct pn_connection_t {
  pn_endpoint_t endpoint;
  pn_endpoint_t *endpoint_head;
  pn_endpoint_t *endpoint_tail;
  pn_endpoint_t *transport_head;  // reference counted
  pn_endpoint_t *transport_tail;
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

void pn_dump(pn_connection_t *conn);
void pn_work_update(pn_connection_t *connection, pn_delivery_t *delivery);
void pn_clear_modified(pn_connection_t *connection, pn_endpoint_t *endpoint);
void pn_connection_bound(pn_connection_t *conn);
void pn_connection_unbound(pn_connection_t *conn);
void pn_modified(pn_connection_t *connection, pn_endpoint_t *endpoint, bool emit);

void pni_add_session(pn_connection_t *conn, pn_session_t *ssn);
void pni_remove_session(pn_connection_t *conn, pn_session_t *ssn);

// XXX Add conn
static inline void pni_add_tpwork(pn_delivery_t *delivery)
{
  pn_connection_t *connection = delivery->link->session->connection;
  if (!delivery->tpwork)
  {
    LL_ADD(connection, tpwork, delivery);
    delivery->tpwork = true;
  }
  pn_modified(connection, &connection->endpoint, true);
}

// XXX Add conn
static inline void pn_clear_tpwork(pn_delivery_t *delivery)
{
  pn_connection_t *connection = delivery->link->session->connection;
  if (delivery->tpwork)
  {
    LL_REMOVE(connection, tpwork, delivery);
    delivery->tpwork = false;
    if (pn_refcount(delivery) > 0) {
      pn_incref(delivery);
      pn_decref(delivery);
    }
  }
}

static inline bool pni_connection_live(pn_connection_t *conn) {
  return pn_refcount(conn) > 1;
}

#endif /* connection.h */
