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

#include "core/condition.h"

#include <assert.h>
#include <stdio.h>

#include "core/memory.h"
#include "core/util.h"

void pn_condition_init(pn_condition_t *condition)
{
  condition->info_raw = (pn_bytes_t){0, NULL};
  condition->name = NULL;
  condition->description = NULL;
  condition->info = NULL;
}

pn_condition_t *pn_condition(void) {
  pn_condition_t *c = (pn_condition_t*)pni_mem_allocate(PN_VOID, sizeof(pn_condition_t));
  pn_condition_init(c);
  return c;
}

void pn_condition_tini(pn_condition_t *condition)
{
  pn_bytes_free(condition->info_raw);
  pn_data_free(condition->info);
  pn_free(condition->description);
  pn_free(condition->name);
}

void pn_condition_free(pn_condition_t *c) {
  if (c) {
    pn_condition_clear(c);
    pn_condition_tini(c);
    pni_mem_deallocate(PN_VOID, c);
  }
}

bool pn_condition_is_set(pn_condition_t *condition)
{
  return condition && condition->name && pn_string_get(condition->name);
}

void pn_condition_clear(pn_condition_t *condition)
{
  assert(condition);
  if (condition->name) pn_string_clear(condition->name);
  if (condition->description) pn_string_clear(condition->description);
  if (condition->info) pn_data_clear(condition->info);
  pn_bytes_free (condition->info_raw);
  condition->info_raw = (pn_bytes_t){0, NULL};
}

const char *pn_condition_get_name(pn_condition_t *condition)
{
  assert(condition);
  if (condition->name == NULL) {
    return NULL;
  } else {
    return pn_string_get(condition->name);
  }
}

int pn_condition_set_name(pn_condition_t *condition, const char *name)
{
  assert(condition);
  if (condition->name == NULL) {
    condition->name = pn_string(name);
    return 0;
  } else {
    return pn_string_set(condition->name, name);
  }
}

const char *pn_condition_get_description(pn_condition_t *condition)
{
  assert(condition);
  if (condition->description == NULL) {
    return NULL;
  } else {
    return pn_string_get(condition->description);
  }
}

int pn_condition_set_description(pn_condition_t *condition, const char *description)
{
  assert(condition);
  if (condition->description == NULL) {
    condition->description = pn_string(description);
    return 0;
  } else {
    return pn_string_set(condition->description, description);
  }
}

int pn_condition_vformat(pn_condition_t *condition, const char *name, const char *fmt, va_list ap)
{
  assert(condition);
  int err = pn_condition_set_name(condition, name);
  if (err)
      return err;

  char text[1024];
  size_t n = vsnprintf(text, 1024, fmt, ap);
  if (n >= sizeof(text))
      text[sizeof(text)-1] = '\0';
  err = pn_condition_set_description(condition, text);
  return err;
}

int pn_condition_format(pn_condition_t *condition, const char *name, PN_PRINTF_FORMAT const char *fmt, ...)
{
  assert(condition);
  va_list ap;
  va_start(ap, fmt);
  int err = pn_condition_vformat(condition, name, fmt, ap);
  va_end(ap);
  return err;
}

pn_data_t *pn_condition_info(pn_condition_t *condition)
{
  assert(condition);
  pni_switch_to_data(&condition->info_raw, &condition->info);
  return condition->info;
}

bool pn_condition_is_redirect(pn_condition_t *condition)
{
  const char *name = pn_condition_get_name(condition);
  return name && (!strcmp(name, "amqp:connection:redirect") ||
                  !strcmp(name, "amqp:link:redirect"));
}

const char *pn_condition_redirect_host(pn_condition_t *condition)
{
  pn_data_t *data = pn_condition_info(condition);
  pn_data_rewind(data);
  pn_data_next(data);
  pn_data_enter(data);
  pn_data_lookup(data, "network-host");
  pn_bytes_t host = pn_data_get_bytes(data);
  pn_data_rewind(data);
  return host.start;
}

int pn_condition_redirect_port(pn_condition_t *condition)
{
  pn_data_t *data = pn_condition_info(condition);
  pn_data_rewind(data);
  pn_data_next(data);
  pn_data_enter(data);
  pn_data_lookup(data, "port");
  int port = pn_data_get_int(data);
  pn_data_rewind(data);
  return port;
}

int pn_condition_copy(pn_condition_t *dest, pn_condition_t *src) {
  assert(dest);
  assert(src);
  int err = 0;
  if (src != dest) {
    if (!(src->name == NULL && dest->name == NULL)) {
      if (src->name == NULL) {
        pn_free(dest->name);
        dest->name = NULL;
      } else {
        if (dest->name == NULL) {
          dest->name = pn_string(NULL);
        }
        err = pn_string_copy(dest->name, src->name);
      }
    }
    if (!err && !(src->description == NULL && dest->description == NULL)) {
      if (src->description == NULL) {
        pn_free(dest->description);
        dest->description = NULL;
      } else {
        if (dest->description == NULL) {
          dest->description = pn_string(NULL);
        }
        err = pn_string_copy(dest->description, src->description);
      }
    }
    if (!err && !(src->info == NULL && dest->info == NULL)) {
      if (src->info == NULL) {
        pn_data_free(dest->info);
        dest->info = NULL;
      } else {
        if (dest->info == NULL) {
          dest->info = pn_data(0);
        }
        err = pn_data_copy(dest->info, src->info);
      }
    }
  }
  return err;
}
