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

#include "core/terminus.h"

#include <assert.h>

#include "core/object_private.h"
#include "core/util.h"

void pni_terminus_init(pn_terminus_t *terminus, pn_terminus_type_t type)
{
  terminus->type = type;
  terminus->address = pn_string(NULL);
  terminus->durability = PN_NONDURABLE;
  terminus->has_expiry_policy = false;
  terminus->expiry_policy = PN_EXPIRE_WITH_SESSION;
  terminus->timeout = 0;
  terminus->dynamic = false;
  terminus->distribution_mode = PN_DIST_MODE_UNSPECIFIED;
  terminus->properties_raw = (pn_bytes_t){0, NULL};
  terminus->capabilities_raw = (pn_bytes_t){0, NULL};
  terminus->outcomes_raw = (pn_bytes_t){0, NULL};
  terminus->filter_raw = (pn_bytes_t){0, NULL};
  terminus->properties = NULL;
  terminus->capabilities = NULL;
  terminus->outcomes = NULL;
  terminus->filter = NULL;
}

void pni_terminus_free(pn_terminus_t *terminus)
{
  pn_free(terminus->address);
  pn_bytes_free(terminus->properties_raw);
  pn_bytes_free(terminus->capabilities_raw);
  pn_bytes_free(terminus->outcomes_raw);
  pn_bytes_free(terminus->filter_raw);
  pn_free(terminus->properties);
  pn_free(terminus->capabilities);
  pn_free(terminus->outcomes);
  pn_free(terminus->filter);
}

int pn_terminus_set_type(pn_terminus_t *terminus, pn_terminus_type_t type)
{
  if (!terminus) return PN_ARG_ERR;
  terminus->type = type;
  return 0;
}

pn_terminus_type_t pn_terminus_get_type(pn_terminus_t *terminus)
{
  return (pn_terminus_type_t) (terminus ? terminus->type : 0);
}

const char *pn_terminus_get_address(pn_terminus_t *terminus)
{
  assert(terminus);
  return pn_string_get(terminus->address);
}

int pn_terminus_set_address(pn_terminus_t *terminus, const char *address)
{
  assert(terminus);
  return pn_string_set(terminus->address, address);
}

pn_durability_t pn_terminus_get_durability(pn_terminus_t *terminus)
{
  return (pn_durability_t) (terminus ? terminus->durability : 0);
}

int pn_terminus_set_durability(pn_terminus_t *terminus, pn_durability_t durability)
{
  if (!terminus) return PN_ARG_ERR;
  terminus->durability = durability;
  return 0;
}

pn_expiry_policy_t pn_terminus_get_expiry_policy(pn_terminus_t *terminus)
{
  return (pn_expiry_policy_t) (terminus ? terminus->expiry_policy : 0);
}

bool pn_terminus_has_expiry_policy(const pn_terminus_t *terminus)
{
    return terminus && terminus->has_expiry_policy;
}

int pn_terminus_set_expiry_policy(pn_terminus_t *terminus, pn_expiry_policy_t expiry_policy)
{
  if (!terminus) return PN_ARG_ERR;
  terminus->expiry_policy = expiry_policy;
  terminus->has_expiry_policy = true;
  return 0;
}

pn_seconds_t pn_terminus_get_timeout(pn_terminus_t *terminus)
{
  return terminus ? terminus->timeout : 0;
}

int pn_terminus_set_timeout(pn_terminus_t *terminus, pn_seconds_t timeout)
{
  if (!terminus) return PN_ARG_ERR;
  terminus->timeout = timeout;
  return 0;
}

bool pn_terminus_is_dynamic(pn_terminus_t *terminus)
{
  return terminus ? terminus->dynamic : false;
}

int pn_terminus_set_dynamic(pn_terminus_t *terminus, bool dynamic)
{
  if (!terminus) return PN_ARG_ERR;
  terminus->dynamic = dynamic;
  return 0;
}

pn_data_t *pn_terminus_properties(pn_terminus_t *terminus)
{
  if (!terminus)
    return NULL;
  pni_switch_to_data(&terminus->properties_raw, &terminus->properties);
  return terminus->properties;
}

pn_data_t *pn_terminus_capabilities(pn_terminus_t *terminus)
{
  if (!terminus)
    return NULL;
  pni_switch_to_data(&terminus->capabilities_raw, &terminus->capabilities);
  return terminus->capabilities;
}

pn_data_t *pn_terminus_outcomes(pn_terminus_t *terminus)
{
  if (!terminus)
    return NULL;
  pni_switch_to_data(&terminus->outcomes_raw, &terminus->outcomes);
  return terminus->outcomes;
}

pn_data_t *pn_terminus_filter(pn_terminus_t *terminus)
{
  if (!terminus)
    return NULL;
  pni_switch_to_data(&terminus->filter_raw, &terminus->filter);
  return terminus->filter;
}

pn_distribution_mode_t pn_terminus_get_distribution_mode(const pn_terminus_t *terminus)
{
  return terminus ? (pn_distribution_mode_t) terminus->distribution_mode : PN_DIST_MODE_UNSPECIFIED;
}

int pn_terminus_set_distribution_mode(pn_terminus_t *terminus, pn_distribution_mode_t m)
{
  if (!terminus) return PN_ARG_ERR;
  terminus->distribution_mode = m;
  return 0;
}

int pn_terminus_copy(pn_terminus_t *terminus, pn_terminus_t *src)
{
  if (!terminus || !src) {
    return PN_ARG_ERR;
  }

  terminus->type = src->type;
  int err = pn_terminus_set_address(terminus, pn_terminus_get_address(src));
  if (err) return err;
  terminus->durability = src->durability;
  terminus->has_expiry_policy = src->has_expiry_policy;
  terminus->expiry_policy = src->expiry_policy;
  terminus->timeout = src->timeout;
  terminus->dynamic = src->dynamic;
  terminus->distribution_mode = src->distribution_mode;
  pn_bytes_free(terminus->properties_raw);
  terminus->properties_raw = pn_bytes_dup(src->properties_raw);
  pn_bytes_free(terminus->capabilities_raw);
  terminus->capabilities_raw = pn_bytes_dup(src->capabilities_raw);
  pn_bytes_free(terminus->outcomes_raw);
  terminus->outcomes_raw = pn_bytes_dup(src->outcomes_raw);
  pn_bytes_free(terminus->filter_raw);
  terminus->filter_raw = pn_bytes_dup(src->filter_raw);
  if (!src->properties) {
    pn_free(terminus->properties);
    terminus->properties = NULL;
  } else {
    if (!terminus->properties) terminus->properties = pn_data(0);
    err = pn_data_copy(terminus->properties, src->properties);
    if (err) return err;
  }
  if (!src->capabilities) {
    pn_free(terminus->capabilities);
    terminus->capabilities = NULL;
  } else {
    if (!terminus->capabilities) terminus->capabilities = pn_data(0);
    err = pn_data_copy(terminus->capabilities, src->capabilities);
    if (err) return err;
  }
  if (!src->outcomes) {
    pn_free(terminus->outcomes);
    terminus->outcomes = NULL;
  } else {
    if (!terminus->outcomes) terminus->outcomes = pn_data(0);
    err = pn_data_copy(terminus->outcomes, src->outcomes);
    if (err) return err;
  }
  if (!src->filter) {
    pn_free(terminus->filter);
    terminus->filter = NULL;
  } else {
    if (!terminus->filter) terminus->filter = pn_data(0);
    err = pn_data_copy(terminus->filter, src->filter);
    if (err) return err;
  }
  return 0;
}
