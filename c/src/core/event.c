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

#include "core/event-internal.h"

#include "core/fixed_string.h"
#include "core/object_private.h"
#include "core/transport.h"

#include <proton/connection.h>
#include <proton/delivery.h>
#include <proton/link.h>
#include <proton/session.h>

#include <assert.h>
#include <stdio.h>

#define pn_event_initialize NULL
static void pn_event_finalize(void *object);
static void pn_event_inspect(void *object, pn_fixed_string_t *string);
#define pn_event_hashcode NULL
#define pn_event_compare NULL

static const pn_class_t PN_CLASSCLASS(pn_event) = PN_CLASS(pn_event);

static void pn_collector_initialize(void* object)
{
  pn_collector_t *collector = (pn_collector_t *)object;
  collector->pool = pn_list(&PN_CLASSCLASS(pn_event), 0);
  collector->head = NULL;
  collector->tail = NULL;
  collector->prev = NULL;
  collector->freed = false;
}

static void pn_collector_finalize(void *object)
{
  pn_collector_t *collector = (pn_collector_t *)object;
  pn_collector_drain(collector);
  pn_object_decref(collector->pool);
}

static void pn_collector_inspect(void *object, pn_fixed_string_t *dst)
{
  assert(object);

  pn_collector_t *collector = (pn_collector_t *) object;

  pn_fixed_string_addf(dst, "EVENTS[");

  pn_event_t *event = collector->head;
  bool first = true;

  while (event) {
    if (first) {
      first = false;
    } else {
      pn_fixed_string_addf(dst, ", ");
    }

    pn_finspect(event, dst);
    event = event->next;
  }

  pn_fixed_string_addf(dst, "]");

  return;
}

#define pn_collector_hashcode NULL
#define pn_collector_compare NULL

pn_collector_t *pn_collector(void)
{
  static const pn_class_t clazz = PN_CLASS(pn_collector);
  return (pn_collector_t *) pn_class_new(&clazz, sizeof(pn_collector_t));
}

void pn_collector_free(pn_collector_t *collector)
{
  assert(collector);
  pn_collector_release(collector);
  pn_object_decref(collector);
}

void pn_collector_release(pn_collector_t *collector)
{
  assert(collector);

  if (!collector->freed) {
    collector->freed = true;

    pn_collector_drain(collector);
    pn_list_clear(collector->pool);
  }
}

void pn_collector_drain(pn_collector_t *collector)
{
  assert(collector);

  while (pn_collector_next(collector))
    ;

  assert(!collector->head);
  assert(!collector->tail);
}

pn_event_t *pn_collector_put(pn_collector_t *collector, const pn_class_t *clazz, void *context, pn_event_type_t type)
{
  return collector_put(collector, clazz, context, type);
}

pn_event_t *pn_collector_put_object(pn_collector_t *collector, void *object, pn_event_type_t type)
{
  return collector_put(collector, pn_class(object), object, type);
}

pn_event_t *pn_collector_peek(pn_collector_t *collector)
{
  return collector->head;
}

bool pn_collector_pop(pn_collector_t *collector) {
  assert(collector);

  pn_event_t *event = collector_get(collector);

  if (event) {
    pn_object_decref(event);
  }

  return event;
}

pn_event_t *pn_collector_next(pn_collector_t *collector) {
  assert(collector);

  if (collector->prev) {
    pn_object_decref(collector->prev);
  }

  collector->prev = collector_get(collector);

  return collector->prev;
}

pn_event_t *pn_collector_prev(pn_collector_t *collector) {
  assert(collector);
  return collector->prev;
}

// XXX Only reactor uses this
bool pn_collector_more(pn_collector_t *collector)
{
  assert(collector);
  return collector->head && collector->head->next;
}

static void pn_event_finalize(void *object) {
  pn_event_t *event = (pn_event_t *) object;

  // Decref before adding to the free list
  if (event->clazz && event->context) {
    pn_class_decref(event->clazz, event->context);
  }

  pn_list_t *pool = event->pool;

  if (pn_object_refcount(pool) > 1) {
    *event = (pn_event_t) {
      .pool = pool,
      .attachments = event->attachments,
    };

    if (event->attachments) pn_record_clear(event->attachments);

    pn_list_add(pool, event);
  } else {
    if (event->attachments) pn_object_decref(event->attachments);
    pn_object_decref(pool);
  }

}

static void pn_event_inspect(void *object, pn_fixed_string_t *dst)
{
  assert(object);
  assert(dst);

  pn_event_t *event = (pn_event_t *) object;

  const char *name = pn_event_type_name(event->type);

  if (name) {
    pn_fixed_string_addf(dst, "(%s", pn_event_type_name(event->type));
  } else {
    pn_fixed_string_addf(dst, "(<%u>", (unsigned int) event->type);
  }

  if (event->context) {
    pn_fixed_string_addf(dst, ", ");
    pn_class_inspect(event->clazz, event->context, dst);
  }

  pn_fixed_string_addf(dst, ")");
}

pn_event_t *pn_event(pn_list_t *pool)
{
  assert(pool);

  pn_event_t *event = (pn_event_t *) pn_class_new(&PN_CLASSCLASS(pn_event), sizeof(pn_event_t));

  event->pool = pool;

  return event;
}

pn_event_type_t pn_event_type(pn_event_t *event)
{
  return event ? event->type : PN_EVENT_NONE;
}

const pn_class_t *pn_event_class(pn_event_t *event)
{
  assert(event);
  return event->clazz;
}

void *pn_event_context(pn_event_t *event)
{
  assert(event);
  return event->context;
}

pn_record_t *pn_event_attachments(pn_event_t *event)
{
  assert(event);
  if (!event->attachments) event->attachments = pn_record();
  return event->attachments;
}

const char *pn_event_type_name(pn_event_type_t type)
{
  #define CASE(X) case X: return #X
  switch (type) {
  CASE(PN_EVENT_NONE);
  CASE(PN_REACTOR_INIT);
  CASE(PN_REACTOR_QUIESCED);
  CASE(PN_REACTOR_FINAL);
  CASE(PN_TIMER_TASK);
  CASE(PN_CONNECTION_INIT);
  CASE(PN_CONNECTION_BOUND);
  CASE(PN_CONNECTION_UNBOUND);
  CASE(PN_CONNECTION_REMOTE_OPEN);
  CASE(PN_CONNECTION_LOCAL_OPEN);
  CASE(PN_CONNECTION_REMOTE_CLOSE);
  CASE(PN_CONNECTION_LOCAL_CLOSE);
  CASE(PN_CONNECTION_FINAL);
  CASE(PN_SESSION_INIT);
  CASE(PN_SESSION_REMOTE_OPEN);
  CASE(PN_SESSION_LOCAL_OPEN);
  CASE(PN_SESSION_REMOTE_CLOSE);
  CASE(PN_SESSION_LOCAL_CLOSE);
  CASE(PN_SESSION_FLOW);
  CASE(PN_SESSION_FINAL);
  CASE(PN_LINK_INIT);
  CASE(PN_LINK_REMOTE_OPEN);
  CASE(PN_LINK_LOCAL_OPEN);
  CASE(PN_LINK_REMOTE_CLOSE);
  CASE(PN_LINK_LOCAL_DETACH);
  CASE(PN_LINK_REMOTE_DETACH);
  CASE(PN_LINK_LOCAL_CLOSE);
  CASE(PN_LINK_FLOW);
  CASE(PN_LINK_WORK);
  CASE(PN_LINK_FINAL);
  CASE(PN_DELIVERY);
  CASE(PN_TRANSPORT);
  CASE(PN_TRANSPORT_AUTHENTICATED);
  CASE(PN_TRANSPORT_ERROR);
  CASE(PN_TRANSPORT_HEAD_CLOSED);
  CASE(PN_TRANSPORT_TAIL_CLOSED);
  CASE(PN_TRANSPORT_CLOSED);
  CASE(PN_SELECTABLE_INIT);
  CASE(PN_SELECTABLE_UPDATED);
  CASE(PN_SELECTABLE_READABLE);
  CASE(PN_SELECTABLE_WRITABLE);
  CASE(PN_SELECTABLE_ERROR);
  CASE(PN_SELECTABLE_EXPIRED);
  CASE(PN_SELECTABLE_FINAL);
  CASE(PN_CONNECTION_WAKE);
  CASE(PN_LISTENER_ACCEPT);
  CASE(PN_LISTENER_CLOSE);
  CASE(PN_PROACTOR_INTERRUPT);
  CASE(PN_PROACTOR_TIMEOUT);
  CASE(PN_PROACTOR_INACTIVE);
  CASE(PN_LISTENER_OPEN);
  CASE(PN_RAW_CONNECTION_CONNECTED);
  CASE(PN_RAW_CONNECTION_DISCONNECTED);
  CASE(PN_RAW_CONNECTION_CLOSED_READ);
  CASE(PN_RAW_CONNECTION_CLOSED_WRITE);
  CASE(PN_RAW_CONNECTION_NEED_READ_BUFFERS);
  CASE(PN_RAW_CONNECTION_NEED_WRITE_BUFFERS);
  CASE(PN_RAW_CONNECTION_READ);
  CASE(PN_RAW_CONNECTION_WRITTEN);
  CASE(PN_RAW_CONNECTION_WAKE);
  CASE(PN_RAW_CONNECTION_DRAIN_BUFFERS);
  default:
    return "PN_UNKNOWN";
  }
  return NULL;
#undef CASE
}

pn_connection_t *pn_event_connection(pn_event_t *event)
{
  if (event->clazz->cid == CID_pn_connection) return (pn_connection_t *) event->context;

  if (event->clazz->cid == CID_pn_session) {
    pn_session_t *session = pn_event_session(event);
    if (session) return pn_session_connection(session);
  }

  pn_transport_t *transport = pn_event_transport(event);
  if (transport) return transport->connection;

  return NULL;
}

pn_session_t *pn_event_session(pn_event_t *event)
{
  if (event->clazz->cid == CID_pn_session) return (pn_session_t *) event->context;

  pn_link_t *link = pn_event_link(event);
  if (link) return pn_link_session(link);

  return NULL;
}

pn_link_t *pn_event_link(pn_event_t *event)
{
  if (event->clazz->cid == CID_pn_link) return (pn_link_t *) event->context;

  pn_delivery_t *delivery = pn_event_delivery(event);
  if (delivery) return pn_delivery_link(delivery);

  return NULL;
}

pn_delivery_t *pn_event_delivery(pn_event_t *event)
{
  if (event->clazz->cid == CID_pn_delivery) return (pn_delivery_t *) event->context;
  return NULL;
}

pn_transport_t *pn_event_transport(pn_event_t *event)
{
  if (event->clazz->cid == CID_pn_transport) return (pn_transport_t *) event->context;

  pn_connection_t *conn = pn_event_connection(event);
  if (conn) return pn_connection_transport(conn);

  return NULL;
}

static inline pn_condition_t *cond_set(pn_condition_t *cond) {
  return cond && pn_condition_is_set(cond) ? cond : NULL;
}

static inline pn_condition_t *cond2_set(pn_condition_t *cond1, pn_condition_t *cond2) {
  pn_condition_t *cond = cond_set(cond1);
  if (!cond) cond = cond_set(cond2);
  return cond;
}

pn_condition_t *pn_event_condition(pn_event_t *event) {
  switch (event->clazz->cid) {
   case CID_pn_connection: {
     pn_connection_t *c = (pn_connection_t*) event->context;
     return cond2_set(pn_connection_remote_condition(c), pn_connection_condition(c));
   }
   case CID_pn_session: {
     pn_session_t *s = (pn_session_t*) event->context;
     return cond2_set(pn_session_remote_condition(s), pn_session_condition(s));
   }
   case CID_pn_link: {
     pn_link_t *l = (pn_link_t*) event->context;
     return cond2_set(pn_link_remote_condition(l), pn_link_condition(l));
   }
   case CID_pn_transport:
    return cond_set(pn_transport_condition((pn_transport_t*) event->context));
   default:
    return NULL;
  }
}
