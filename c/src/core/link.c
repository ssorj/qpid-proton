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

#include "core/link.h"

#include <assert.h>

#include "core/connection.h"
#include "core/session.h"
#include "core/util.h"

static void pn_link_incref(void *object)
{
  pn_link_t *link = (pn_link_t *) object;
  if (!link->endpoint.referenced) {
    link->endpoint.referenced = true;
    pn_incref(link->session);
  } else {
    pn_object_incref(object);
  }
}

static void pn_link_finalize(void *object)
{
  pn_link_t *link = (pn_link_t *) object;
  pn_endpoint_t *endpoint = &link->endpoint;

  if (pni_preserve_child(endpoint)) {
    return;
  }

  while (link->unsettled_head) {
    assert(!link->unsettled_head->referenced);
    pn_free(link->unsettled_head);
  }

  pn_free(link->context);
  pni_terminus_free(&link->source);
  pni_terminus_free(&link->target);
  pni_terminus_free(&link->remote_source);
  pni_terminus_free(&link->remote_target);
  pn_free(link->name);
  pni_endpoint_tini(endpoint);
  pni_remove_link(link->session, link);
  pn_hash_del(link->session->state.local_handles, link->state.local_handle);
  pn_hash_del(link->session->state.remote_handles, link->state.remote_handle);
  pn_list_remove(link->session->freed, link);
  if (endpoint->referenced) {
    pn_decref(link->session);
  }
  pn_free(link->properties);
  pn_bytes_free(link->properties_raw);
  pn_free(link->remote_properties);
  pn_bytes_free(link->remote_properties_raw);
}

void pni_link_bound(pn_link_t *link)
{
}

void pn_link_unbound(pn_link_t* link)
{
  assert(link);
  link->state.local_handle = -1;
  link->state.remote_handle = -1;
  link->state.delivery_count = 0;
  link->state.link_credit = 0;
}

#define pn_link_refcount NULL
#define pn_link_decref NULL
#define pn_link_initialize NULL
#define pn_link_hashcode NULL
#define pn_link_compare NULL
#define pn_link_inspect NULL

pn_link_t *pn_link_new(int type, pn_session_t *session, pn_string_t *name)
{
#define pn_link_new NULL
#define pn_link_free NULL
  static const pn_class_t clazz = PN_METACLASS(pn_link);
#undef pn_link_new
#undef pn_link_free
  pn_link_t *link = (pn_link_t *) pn_class_new(&clazz, sizeof(pn_link_t));

  pn_endpoint_init(&link->endpoint, type, session->connection);
  pni_add_link(session, link);
  pn_incref(session);  // keep session until link finalized
  link->name = name;
  pni_terminus_init(&link->source, PN_SOURCE);
  pni_terminus_init(&link->target, PN_TARGET);
  pni_terminus_init(&link->remote_source, PN_UNSPECIFIED);
  pni_terminus_init(&link->remote_target, PN_UNSPECIFIED);
  link->unsettled_head = link->unsettled_tail = link->current = NULL;
  link->unsettled_count = 0;
  link->max_message_size = 0;
  link->remote_max_message_size = 0;
  link->available = 0;
  link->credit = 0;
  link->queued = 0;
  link->more_id = 0;
  link->drain = false;
  link->drain_flag_mode = true;
  link->drained = 0;
  link->context = pn_record();
  link->snd_settle_mode = PN_SND_MIXED;
  link->rcv_settle_mode = PN_RCV_FIRST;
  link->remote_snd_settle_mode = PN_SND_MIXED;
  link->remote_rcv_settle_mode = PN_RCV_FIRST;
  link->detached = false;
  link->more_pending = false;
  link->properties = 0;
  link->properties_raw = (pn_bytes_t){0, NULL};
  link->remote_properties = 0;
  link->remote_properties_raw = (pn_bytes_t){0, NULL};

  // begin transport state
  link->state.local_handle = -1;
  link->state.remote_handle = -1;
  link->state.delivery_count = 0;
  link->state.link_credit = 0;
  // end transport state

  pn_collector_put_object(session->connection->collector, link, PN_LINK_INIT);
  if (session->connection->transport) {
    pni_link_bound(link);
  }
  pn_decref(link);
  return link;
}

pn_terminus_t *pn_link_source(pn_link_t *link)
{
  return link ? &link->source : NULL;
}

pn_terminus_t *pn_link_target(pn_link_t *link)
{
  return link ? &link->target : NULL;
}

pn_terminus_t *pn_link_remote_source(pn_link_t *link)
{
  return link ? &link->remote_source : NULL;
}

pn_terminus_t *pn_link_remote_target(pn_link_t *link)
{
  return link ? &link->remote_target : NULL;
}

void pn_link_free(pn_link_t *link)
{
  assert(!link->endpoint.freed);
  pni_remove_link(link->session, link);
  pn_list_add(link->session->freed, link);
  pn_delivery_t *delivery = link->unsettled_head;
  while (delivery) {
    pn_delivery_t *next = delivery->unsettled_next;
    pn_delivery_settle(delivery);
    delivery = next;
  }
  link->endpoint.freed = true;
  pn_endpoint_decref(&link->endpoint);

  // the finalize logic depends on endpoint.freed (modified above), so
  // we incref/decref to give it a chance to rerun
  pn_incref(link);
  pn_decref(link);
}

void *pn_link_get_context(pn_link_t *link)
{
  assert(link);
  return pn_record_get(link->context, PN_LEGCTX);
}

void pn_link_set_context(pn_link_t *link, void *context)
{
  assert(link);
  pn_record_set(link->context, PN_LEGCTX, context);
}

pn_record_t *pn_link_attachments(pn_link_t *link)
{
  assert(link);
  return link->context;
}

pn_link_t *pn_link_head(pn_connection_t *conn, pn_state_t state)
{
  if (!conn) return NULL;

  pn_endpoint_t *endpoint = conn->endpoint_head;

  while (endpoint)
  {
    if (pni_matches(endpoint, SENDER, state) || pni_matches(endpoint, RECEIVER, state))
      return (pn_link_t *) endpoint;
    endpoint = endpoint->endpoint_next;
  }

  return NULL;
}

pn_link_t *pn_link_next(pn_link_t *link, pn_state_t state)
{
  if (!link) return NULL;

  pn_endpoint_t *endpoint = link->endpoint.endpoint_next;

  while (endpoint)
  {
    if (pni_matches(endpoint, SENDER, state) || pni_matches(endpoint, RECEIVER, state))
      return (pn_link_t *) endpoint;
    endpoint = endpoint->endpoint_next;
  }

  return NULL;
}

void pn_link_open(pn_link_t *link)
{
  assert(link);
  pn_endpoint_open(&link->endpoint);
}

void pn_link_close(pn_link_t *link)
{
  assert(link);
  pn_endpoint_close(&link->endpoint);
}

void pn_link_detach(pn_link_t *link)
{
  assert(link);
  if (link->detached) return;

  link->detached = true;
  pn_collector_put_object(link->session->connection->collector, link, PN_LINK_LOCAL_DETACH);
  pni_connection_add_endpoint_work(link->session->connection, &link->endpoint, true);
}

pn_link_t *pn_sender(pn_session_t *session, const char *name)
{
  return pn_link_new(SENDER, session, pn_string(name));
}

pn_link_t *pn_receiver(pn_session_t *session, const char *name)
{
  return pn_link_new(RECEIVER, session, pn_string(name));
}

pn_state_t pn_link_state(pn_link_t *link)
{
  return link->endpoint.state;
}

const char *pn_link_name(pn_link_t *link)
{
  assert(link);
  return pn_string_get(link->name);
}

bool pn_link_is_sender(pn_link_t *link)
{
  return link->endpoint.type == SENDER;
}

bool pn_link_is_receiver(pn_link_t *link)
{
  return link->endpoint.type == RECEIVER;
}

pn_session_t *pn_link_session(pn_link_t *link)
{
  assert(link);
  return link->session;
}

int pn_link_unsettled(pn_link_t *link)
{
  return link->unsettled_count;
}

pn_delivery_t *pn_unsettled_head(pn_link_t *link)
{
  pn_delivery_t *d = link->unsettled_head;
  while (d && d->local.settled) {
    d = d->unsettled_next;
  }
  return d;
}

pn_delivery_t *pn_link_current(pn_link_t *link)
{
  if (!link) return NULL;
  return link->current;
}

static void pni_advance_sender(pn_link_t *link)
{
  link->current->done = true;
  /* Skip accounting if the link is aborted and has not sent any frames.
     A delivery that was aborted before sending the first frame was not accounted
     for in pni_process_tpwork_sender() so we don't need to account for it being sent here.
  */
  bool skip = link->current->aborted && !link->current->state.sending;
  if (!skip) {
    link->queued++;
    link->credit--;
    link->session->outgoing_deliveries++;
  }
  pni_connection_add_delivery_work(link->session->connection, link->current);
  link->current = link->current->unsettled_next;
}

static void pni_advance_receiver(pn_link_t *link)
{
  link->credit--;
  link->queued--;
  link->session->incoming_deliveries--;

  pn_delivery_t *current = link->current;
  size_t drop_count = pn_buffer_size(current->bytes);
  pn_buffer_clear(current->bytes);

  if (drop_count) {
    pn_session_t *ssn = link->session;
    ssn->incoming_bytes -= drop_count;
    if (!ssn->check_flow && ssn->state.incoming_window < ssn->incoming_window_lwm) {
      ssn->check_flow = true;
      pni_connection_add_delivery_work(link->session->connection, current);
    }
  }

  link->current = link->current->unsettled_next;
}

bool pn_link_advance(pn_link_t *link)
{
  if (link && link->current) {
    pn_delivery_t *prev = link->current;
    if (link->endpoint.type == SENDER) {
      pni_advance_sender(link);
    } else {
      pni_advance_receiver(link);
    }
    pn_delivery_t *next = link->current;
    pni_connection_update_legacy_work(link->session->connection, prev);
    if (next) pni_connection_update_legacy_work(link->session->connection, next);
    return prev != next;
  } else {
    return false;
  }
}

int pn_link_credit(pn_link_t *link)
{
  return link ? link->credit : 0;
}

int pn_link_available(pn_link_t *link)
{
  return link ? link->available : 0;
}

int pn_link_queued(pn_link_t *link)
{
  return link ? link->queued : 0;
}

int pn_link_remote_credit(pn_link_t *link)
{
  assert(link);
  return link->credit - link->queued;
}

bool pn_link_get_drain(pn_link_t *link)
{
  assert(link);
  return link->drain;
}

pn_snd_settle_mode_t pn_link_snd_settle_mode(pn_link_t *link)
{
  return link ? (pn_snd_settle_mode_t)link->snd_settle_mode
      : PN_SND_MIXED;
}

pn_rcv_settle_mode_t pn_link_rcv_settle_mode(pn_link_t *link)
{
  return link ? (pn_rcv_settle_mode_t)link->rcv_settle_mode
      : PN_RCV_FIRST;
}

pn_snd_settle_mode_t pn_link_remote_snd_settle_mode(pn_link_t *link)
{
  return link ? (pn_snd_settle_mode_t)link->remote_snd_settle_mode
      : PN_SND_MIXED;
}

pn_rcv_settle_mode_t pn_link_remote_rcv_settle_mode(pn_link_t *link)
{
  return link ? (pn_rcv_settle_mode_t)link->remote_rcv_settle_mode
      : PN_RCV_FIRST;
}

void pn_link_set_snd_settle_mode(pn_link_t *link, pn_snd_settle_mode_t mode)
{
  if (link)
    link->snd_settle_mode = (uint8_t)mode;
}

void pn_link_set_rcv_settle_mode(pn_link_t *link, pn_rcv_settle_mode_t mode)
{
  if (link)
    link->rcv_settle_mode = (uint8_t)mode;
}

void pn_link_offered(pn_link_t *sender, int credit)
{
  sender->available = credit;
}

ssize_t pn_link_send(pn_link_t *sender, const char *bytes, size_t n)
{
  pn_delivery_t *current = pn_link_current(sender);
  if (!current) return PN_EOS;
  if (!bytes || !n) return 0;
  pn_buffer_append(current->bytes, bytes, n);
  sender->session->outgoing_bytes += n;
  pni_connection_add_delivery_work(sender->session->connection, current);
  return n;
}

int pn_link_drained(pn_link_t *link)
{
  assert(link);
  int drained = 0;

  if (pn_link_is_sender(link)) {
    if (link->drain && link->credit > 0) {
      link->drained = link->credit;
      link->credit = 0;
      pni_connection_add_endpoint_work(link->session->connection, &link->endpoint, true);
      drained = link->drained;
    }
  } else {
    drained = link->drained;
    link->drained = 0;
  }

  return drained;
}

ssize_t pn_link_recv(pn_link_t *receiver, char *bytes, size_t n)
{
  if (!receiver) return PN_ARG_ERR;
  pn_delivery_t *delivery = receiver->current;
  if (!delivery) return PN_STATE_ERR;
  if (delivery->aborted) return PN_ABORTED;
  size_t size = pn_buffer_get(delivery->bytes, 0, n, bytes);
  pn_buffer_trim(delivery->bytes, size, 0);
  if (size) {
    pn_session_t *ssn = receiver->session;
    ssn->incoming_bytes -= size;
    if (!ssn->check_flow && ssn->state.incoming_window < ssn->incoming_window_lwm) {
      ssn->check_flow = true;
      pni_connection_add_delivery_work(ssn->connection, delivery);
    }
    return size;
  } else {
    return delivery->done ? PN_EOS : 0;
  }
}

void pn_link_flow(pn_link_t *receiver, int credit)
{
  assert(receiver);
  assert(pn_link_is_receiver(receiver));
  receiver->credit += credit;
  pni_connection_add_endpoint_work(receiver->session->connection, &receiver->endpoint, true);
  if (!receiver->drain_flag_mode) {
    pn_link_set_drain(receiver, false);
    receiver->drain_flag_mode = false;
  }
}

void pn_link_drain(pn_link_t *receiver, int credit)
{
  assert(receiver);
  assert(pn_link_is_receiver(receiver));
  pn_link_set_drain(receiver, true);
  pn_link_flow(receiver, credit);
  receiver->drain_flag_mode = false;
}

void pn_link_set_drain(pn_link_t *receiver, bool drain)
{
  assert(receiver);
  assert(pn_link_is_receiver(receiver));
  receiver->drain = drain;
  pni_connection_add_endpoint_work(receiver->session->connection, &receiver->endpoint, true);
  receiver->drain_flag_mode = true;
}

bool pn_link_draining(pn_link_t *receiver)
{
  assert(receiver);
  assert(pn_link_is_receiver(receiver));
  return receiver->drain && (pn_link_credit(receiver) > pn_link_queued(receiver));
}

uint64_t pn_link_max_message_size(pn_link_t *link)
{
  return link->max_message_size;
}

void pn_link_set_max_message_size(pn_link_t *link, uint64_t size)
{
  link->max_message_size = size;
}

uint64_t pn_link_remote_max_message_size(pn_link_t *link)
{
  return link->remote_max_message_size;
}

pn_data_t *pn_link_properties(pn_link_t *link)
{
  assert(link);
  if (!link->properties)
      link->properties = pn_data(0);
  return link->properties;
}

pn_data_t *pn_link_remote_properties(pn_link_t *link)
{
  assert(link);
  // Annoying inconsistency: nearly everywhere else you *HAVE* to return an empty pn_data_t not NULL
  if (link->remote_properties_raw.size) {
    pni_switch_to_data(&link->remote_properties_raw, &link->remote_properties);
  }
  return link->remote_properties;
}

pn_condition_t *pn_link_condition(pn_link_t *link)
{
  assert(link);
  return &link->endpoint.condition;
}

pn_condition_t *pn_link_remote_condition(pn_link_t *link)
{
  assert(link);
  return &link->endpoint.remote_condition;
}
