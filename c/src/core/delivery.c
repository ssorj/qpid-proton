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

#include "core/delivery.h"

#include <assert.h>
#include <inttypes.h>

#include "core/connection.h"
#include "core/disposition.h"
#include "core/fixed_string.h"
#include "core/link.h"
#include "core/session.h"

static void pn_delivery_incref(void *object);
static void pn_delivery_finalize(void *object);
#define pn_delivery_new NULL
#define pn_delivery_refcount NULL
#define pn_delivery_decref NULL
#define pn_delivery_free NULL
#define pn_delivery_initialize NULL
#define pn_delivery_hashcode NULL
#define pn_delivery_compare NULL
static void pn_delivery_inspect(void *obj, pn_fixed_string_t *dst);

const pn_class_t PN_CLASSCLASS(pn_delivery) = PN_METACLASS(pn_delivery);

static inline bool delivery_preserved(pn_delivery_t *delivery)
{
  pn_connection_t *conn = delivery->link->session->connection;
  return !delivery->local.settled || (conn->transport && (delivery->state.init || delivery->tpwork));
}

pn_delivery_t *pn_delivery(pn_link_t *link, pn_delivery_tag_t tag)
{
  assert(link);
  pn_list_t *pool = link->session->connection->delivery_pool;
  pn_delivery_t *delivery = (pn_delivery_t *) pn_list_pop(pool);
  if (!delivery) {
    delivery = (pn_delivery_t *) pn_class_new(&PN_CLASSCLASS(pn_delivery), sizeof(pn_delivery_t));
    if (!delivery) return NULL;
    delivery->bytes = pn_buffer(64);
    pn_disposition_init(&delivery->local);
    pn_disposition_init(&delivery->remote);
    delivery->context = pn_record();
  } else {
    assert(!delivery->state.init);
  }
  delivery->link = link;
  pn_incref(delivery->link);  // keep link until finalized
  delivery->tag = pn_bytes_dup(tag);
  pn_disposition_clear(&delivery->local);
  pn_disposition_clear(&delivery->remote);
  delivery->updated = false;
  delivery->settled = false;
  LL_ADD(link, unsettled, delivery);
  delivery->referenced = true;
  delivery->tpwork_next = NULL;
  delivery->tpwork_prev = NULL;
  delivery->tpwork = false;
  pn_buffer_clear(delivery->bytes);
  delivery->done = false;
  delivery->aborted = false;
  pn_record_clear(delivery->context);

  // begin delivery state
  delivery->state.init = false;
  delivery->state.sending = false; /* True if we have sent at least 1 frame */
  delivery->state.sent = false;    /* True if we have sent the entire delivery */
  // end delivery state

  if (!link->current)
    link->current = delivery;

  link->unsettled_count++;

  // XXX: could just remove incref above
  pn_decref(delivery);

  return delivery;
}

static void pn_delivery_incref(void *object)
{
  pn_delivery_t *delivery = (pn_delivery_t *) object;
  if (delivery->link && !delivery->referenced) {
    delivery->referenced = true;
    pn_incref(delivery->link);
  } else {
    pn_object_incref(object);
  }
}

static void pn_delivery_finalize(void *object)
{
  pn_delivery_t *delivery = (pn_delivery_t *) object;
  pn_link_t *link = delivery->link;
  //  assert(!delivery->state.init);

  bool pooled = false;
  bool referenced = true;
  if (link) {
    if (pni_link_live(link) && delivery_preserved(delivery) && delivery->referenced) {
      delivery->referenced = false;
      pn_object_incref(delivery);
      pn_decref(link);
      return;
    }
    referenced = delivery->referenced;

    pn_connection_t *conn = link->session->connection;

    pni_connection_remove_delivery_work(conn, delivery);
    LL_REMOVE(link, unsettled, delivery);
    pn_delivery_map_del(pn_link_is_sender(link)
                        ? &link->session->state.outgoing
                        : &link->session->state.incoming,
                        delivery);
    pn_bytes_free(delivery->tag);
    delivery->tag = (pn_delivery_tag_t){0, NULL};
    pn_buffer_clear(delivery->bytes);
    pn_record_clear(delivery->context);
    delivery->settled = true;
    assert(pn_refcount(delivery) == 0);
    if (pni_connection_live(conn)) {
      pn_list_t *pool = link->session->connection->delivery_pool;
      delivery->link = NULL;
      pn_list_add(pool, delivery);
      pooled = true;
      assert(pn_refcount(delivery) == 1);
    }
  }

  if (!pooled) {
    pn_free(delivery->context);
    pn_bytes_free(delivery->tag);
    delivery->tag = (pn_delivery_tag_t){0, NULL};
    pn_buffer_free(delivery->bytes);
    pn_disposition_finalize(&delivery->local);
    pn_disposition_finalize(&delivery->remote);
  }

  if (referenced) {
    pn_decref(link);
  }
}

void pn_delivery_inspect(void *obj, pn_fixed_string_t *dst) {
  pn_delivery_t *d = (pn_delivery_t*)obj;
  const char* dir = pn_link_is_sender(d->link) ? "sending" : "receiving";
  pn_bytes_t bytes = d->tag;
  pn_fixed_string_addf(dst, "pn_delivery<%p>{%s, tag=b\"", obj, dir);
  pn_fixed_string_quote(dst, bytes.start, bytes.size);
  pn_fixed_string_addf(dst, "\", local=%s, remote=%s}",
                       pn_disposition_type_name(d->local.type),
                       pn_disposition_type_name(d->remote.type));
  return;
}

pn_delivery_tag_t pn_dtag(const char *bytes, size_t size) {
  pn_delivery_tag_t dtag = {size, bytes};
  return dtag;
}

bool pn_delivery_buffered(pn_delivery_t *delivery)
{
  assert(delivery);
  if (delivery->settled) return false;
  if (pn_link_is_sender(delivery->link)) {
    pn_delivery_state_t *state = &delivery->state;
    if (state->sent) {
      return false;
    } else {
      return delivery->done || (pn_buffer_size(delivery->bytes) > 0);
    }
  } else {
    return false;
  }
}

pn_delivery_t *pn_unsettled_next(pn_delivery_t *delivery)
{
  pn_delivery_t *d = delivery->unsettled_next;
  while (d && d->local.settled) {
    d = d->unsettled_next;
  }
  return d;
}

bool pn_delivery_current(pn_delivery_t *delivery)
{
  pn_link_t *link = delivery->link;
  return pn_link_current(link) == delivery;
}

void pn_delivery_dump(pn_delivery_t *d)
{
  char tag[1024];
  pn_bytes_t bytes = d->tag;
  pn_quote_data(tag, 1024, bytes.start, bytes.size);
  printf("{tag=%s, local.type=%" PRIu64 ", remote.type=%" PRIu64 ", local.settled=%d, "
         "remote.settled=%d, updated=%d, current=%d, writable=%d, readable=%d}",
         tag, pn_disposition_type(&d->local), pn_disposition_type(&d->remote), d->local.settled,
         d->remote.settled, d->updated, pn_delivery_current(d),
//         pn_delivery_writable(d), pn_delivery_readable(d), d->work);
         pn_delivery_writable(d), pn_delivery_readable(d));
}

void *pn_delivery_get_context(pn_delivery_t *delivery)
{
  assert(delivery);
  return pn_record_get(delivery->context, PN_LEGCTX);
}

void pn_delivery_set_context(pn_delivery_t *delivery, void *context)
{
  assert(delivery);
  pn_record_set(delivery->context, PN_LEGCTX, context);
}

pn_record_t *pn_delivery_attachments(pn_delivery_t *delivery)
{
  assert(delivery);
  return delivery->context;
}

pn_delivery_tag_t pn_delivery_tag(pn_delivery_t *delivery)
{
  if (delivery) {
    return delivery->tag;
  } else {
    return (pn_delivery_tag_t){0, NULL};
  }
}

void pn_delivery_settle(pn_delivery_t *delivery)
{
  assert(delivery);
  if (!delivery->local.settled) {
    pn_link_t *link = delivery->link;
    if (pn_delivery_current(delivery)) {
      pn_link_advance(link);
    }

    link->unsettled_count--;
    delivery->local.settled = true;
    pn_connection_t *conn = delivery->link->session->connection;
    pni_connection_add_delivery_work(conn, delivery);

    pn_incref(delivery);
    pn_decref(delivery);
  }
}

pn_link_t *pn_delivery_link(pn_delivery_t *delivery)
{
  assert(delivery);
  return delivery->link;
}

pn_disposition_t *pn_delivery_local(pn_delivery_t *delivery)
{
  assert(delivery);
  return &delivery->local;
}

uint64_t pn_delivery_local_state(pn_delivery_t *delivery)
{
  assert(delivery);
  return pn_disposition_type(&delivery->local);
}

pn_disposition_t *pn_delivery_remote(pn_delivery_t *delivery)
{
  assert(delivery);
  return &delivery->remote;
}

uint64_t pn_delivery_remote_state(pn_delivery_t *delivery)
{
  assert(delivery);
  return pn_disposition_type(&delivery->remote);
}

bool pn_delivery_settled(pn_delivery_t *delivery)
{
  return delivery ? delivery->remote.settled : false;
}

bool pn_delivery_updated(pn_delivery_t *delivery)
{
  return delivery ? delivery->updated : false;
}

void pn_delivery_clear(pn_delivery_t *delivery)
{
  delivery->updated = false;
}

void pn_delivery_update(pn_delivery_t *delivery, uint64_t state)
{
  if (!delivery) return;
  if (delivery->local.type == PN_DISP_CUSTOM) {
    switch (state) {
      case PN_ACCEPTED:
      case PN_REJECTED:
      case PN_RECEIVED:
      case PN_MODIFIED:
      case PN_RELEASED:
      case PN_DECLARED:
      case PN_TRANSACTIONAL_STATE:
        break;
      default:
        delivery->local.u.s_custom.type = state;
        pni_connection_add_delivery_work(delivery->link->session->connection, delivery);
        return;
    }
  }
  if (delivery->local.type != state) pn_disposition_clear(&delivery->local);
  switch (state) {
    case PN_ACCEPTED:
    case PN_REJECTED:
    case PN_RECEIVED:
    case PN_MODIFIED:
    case PN_RELEASED:
    case PN_DECLARED:
    case PN_TRANSACTIONAL_STATE:
      delivery->local.type = state;
      break;
    default:
      delivery->local.type = PN_DISP_CUSTOM;
      delivery->local.u.s_custom.type = state;
      break;
  }
  pni_connection_add_delivery_work(delivery->link->session->connection, delivery);
}

bool pn_delivery_writable(pn_delivery_t *delivery)
{
  if (!delivery) return false;

  pn_link_t *link = delivery->link;
  return pn_link_is_sender(link) && pn_delivery_current(delivery) && pn_link_credit(link) > 0;
}

bool pn_delivery_readable(pn_delivery_t *delivery)
{
  if (delivery) {
    pn_link_t *link = delivery->link;
    return pn_link_is_receiver(link) && pn_delivery_current(delivery);
  } else {
    return false;
  }
}

size_t pn_delivery_pending(pn_delivery_t *delivery)
{
  /* Aborted deliveries: for clients that don't check pn_delivery_aborted(),
     return 1 rather than 0. This will force them to call pn_link_recv() and get
     the PN_ABORTED error return code.
  */
  if (delivery->aborted) return 1;
  return pn_buffer_size(delivery->bytes);
}

bool pn_delivery_partial(pn_delivery_t *delivery)
{
  return !delivery->done;
}

void pn_delivery_abort(pn_delivery_t *delivery) {
  if (!delivery->local.settled) { /* Can't abort a settled delivery */
    delivery->aborted = true;
    pn_delivery_settle(delivery);
    delivery->link->session->outgoing_bytes -= pn_buffer_size(delivery->bytes);
    pn_buffer_clear(delivery->bytes);
  }
}

bool pn_delivery_aborted(pn_delivery_t *delivery) {
  return delivery->aborted;
}
