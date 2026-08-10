#ifndef PROTON_EVENT_INTERNAL_H
#define PROTON_EVENT_INTERNAL_H 1

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

#include <proton/event.h>

#include "core/object_private.h"

#include <assert.h>

#ifdef __cplusplus
extern "C" {
#endif

struct pn_collector_t {
  pn_list_t *pool;
  pn_event_t *head;
  pn_event_t *tail;
  pn_event_t *prev; // The event returned by the previous call to pn_collector_next()
  bool freed;
};

struct pn_event_t {
  pn_list_t *pool;
  const pn_class_t *clazz;
  void *context; // Depends on clazz
  pn_record_t *attachments;
  pn_event_t *next;
  pn_event_type_t type;
};

pn_event_t *pn_event(void);

static inline pn_event_t *collector_put(pn_collector_t *collector, const pn_class_t *clazz, void *context,
					pn_event_type_t type)
{
  assert(collector);
  assert(clazz);
  assert(context);

  if (collector->freed) return NULL;

  pn_event_t *tail = collector->tail;

  // Eliminate duplicates
  if (tail && tail->type == type && tail->context == context) return NULL;

  pn_event_t *event = (pn_event_t *) pn_list_pop(collector->pool);

  if (!event) {
    event = pn_event();
  }

  event->pool = collector->pool;

  pn_object_incref(event->pool);

  if (collector->tail) {
    collector->tail->next = event;
    collector->tail = event;
  } else {
    collector->tail = event;
    collector->head = event;
  }

  event->clazz = clazz;
  event->context = context;
  event->type = type;

  pn_class_incref(clazz, event->context);

  return event;
}

// Advance the head pointer for pop or next and return the old head
static inline pn_event_t *collector_pop(pn_collector_t *collector) {
  assert(collector);

  pn_event_t *event = collector->head;

  if (event) {
    collector->head = event->next;

    if (!collector->head) {
      collector->tail = NULL;
    }
  }

  return event;
}

static inline pn_event_t *pni_collector_put_object(pn_collector_t *collector, void *object, pn_event_type_t type)
{
  assert(collector);
  return collector_put(collector, pn_class(object), object, type);
}

static inline pn_event_t *pni_collector_next(pn_collector_t *collector)
{
  assert(collector);

  if (collector->prev) {
    pn_object_decref(collector->prev);
  }

  collector->prev = collector_pop(collector);

  return collector->prev;
}

static inline pn_event_t *pni_collector_prev(pn_collector_t *collector)
{
  assert(collector);
  return collector->prev;
}

static inline pn_event_t *pni_collector_peek(pn_collector_t *collector)
{
  assert(collector);
  return collector->head;
}

static inline pn_event_type_t pni_event_type(pn_event_t *event)
{
  assert(event);
  return event->type;
}

static inline const pn_class_t *pni_event_class(pn_event_t *event)
{
  assert(event);
  return event->clazz;
}

#ifdef __cplusplus
}
#endif

#endif /* event-internal.h */
