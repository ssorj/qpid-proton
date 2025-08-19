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

#include "core/disposition.h"

#include <assert.h>
#include <string.h>

#include "core/emitters.h"
#include "core/frame_consumers.h"
#include "core/frame_generators.h"
#include "core/util.h"

void pn_disposition_init(pn_disposition_t *ds)
{
  memset(ds, 0, sizeof(*ds));
}

void pn_disposition_finalize(pn_disposition_t *ds)
{
  switch (ds->type) {
    case PN_DISP_EMPTY:
      break;
    case PN_DISP_RECEIVED:
      break;
    case PN_DISP_MODIFIED:
      pn_data_free(ds->u.s_modified.annotations);
      pn_bytes_free(ds->u.s_modified.annotations_raw);
      break;
    case PN_DISP_REJECTED:
      pn_condition_tini(&ds->u.s_rejected.condition);
      break;
    case PN_DISP_ACCEPTED:
    case PN_DISP_RELEASED:
      break;
    case PN_DISP_CUSTOM:
      pn_data_free(ds->u.s_custom.data);
      pn_bytes_free(ds->u.s_custom.data_raw);
      break;
    case PN_DISP_TRANSACTIONAL:
      pn_bytes_free(ds->u.s_transactional.id);
      pn_bytes_free(ds->u.s_transactional.outcome_raw);
      break;
    case PN_DISP_DECLARED:
      pn_bytes_free(ds->u.s_declared.id);
      break;
  }
}

void pn_disposition_clear(pn_disposition_t *ds)
{
  pn_disposition_finalize(ds);
  pn_disposition_init(ds);
}

uint64_t pn_disposition_type(pn_disposition_t *disposition)
{
  assert(disposition);
  switch (disposition->type) {
    case PN_DISP_CUSTOM:
      return disposition->u.s_custom.type;
    default:
      // This relies on the disposition types having the protocol values
      return (uint64_t)disposition->type;
  }
}

const char *pn_disposition_type_name(uint64_t d) {
  switch(d) {
   case PN_RECEIVED: return "received";
   case PN_ACCEPTED: return "accepted";
   case PN_REJECTED: return "rejected";
   case PN_RELEASED: return "released";
   case PN_MODIFIED: return "modified";
   case PN_DECLARED: return "transaction_declared";
   case PN_TRANSACTIONAL_STATE: return "transactional_state";
   default: return "unknown";
  }
}

void pni_disposition_to_raw(pn_disposition_t *disposition) {

  uint64_t type = disposition->type;
  if (type==PN_DISP_CUSTOM) return;

  char buffer[512];
  pn_rwbytes_t bytes = {.size=sizeof(buffer), .start=&buffer[0]};
  pni_emitter_t emitter = make_emitter_from_bytes(bytes);
  pni_compound_context compound = make_compound();

  switch (type) {
    case PN_DISP_EMPTY:
      break;
    case PN_DISP_RECEIVED:
      emit_received_disposition(&emitter, &compound, &disposition->u.s_received);
      break;
    case PN_DISP_ACCEPTED:
      emit_list0(&emitter, &compound);
      break;
    case PN_DISP_RELEASED:
      emit_list0(&emitter, &compound);
      break;
    case PN_DISP_REJECTED:
      emit_rejected_disposition(&emitter, &compound, &disposition->u.s_rejected);
      break;
    case PN_DISP_MODIFIED:
      emit_modified_disposition(&emitter, &compound, &disposition->u.s_modified);
      break;
    case PN_DISP_TRANSACTIONAL:
      emit_transactional_disposition(&emitter, &compound, &disposition->u.s_transactional);
      break;
  }

  if (type != PN_DISP_EMPTY) {
    pn_disposition_clear(disposition);
    disposition->u.s_custom.data_raw = pn_bytes_dup(make_bytes_from_emitter(emitter));
  }

  disposition->type = PN_DISP_CUSTOM;
  disposition->u.s_custom.type = type;
}

pn_data_t *pn_disposition_data(pn_disposition_t *disposition)
{
  assert(disposition);
  if (disposition->type != PN_DISP_CUSTOM) {
    pni_disposition_to_raw(disposition);
  }
  pni_switch_to_data(&disposition->u.s_custom.data_raw, &disposition->u.s_custom.data);
  return disposition->u.s_custom.data;
}

uint32_t pn_disposition_get_section_number(pn_disposition_t *disposition)
{
  assert(disposition);
  if (disposition->type == PN_DISP_RECEIVED) return disposition->u.s_received.section_number;
  else return 0;
}

void pn_disposition_set_section_number(pn_disposition_t *disposition, uint32_t section_number)
{
  assert(disposition);
  if (disposition->type != PN_DISP_RECEIVED) {
    pn_disposition_clear(disposition);
    disposition->type = PN_DISP_RECEIVED;
  }
  disposition->u.s_received.section_number = section_number;
}

uint64_t pn_disposition_get_section_offset(pn_disposition_t *disposition)
{
  assert(disposition);
  if (disposition->type == PN_DISP_RECEIVED) return disposition->u.s_received.section_offset;
  else return 0;
}

void pn_disposition_set_section_offset(pn_disposition_t *disposition, uint64_t section_offset)
{
  assert(disposition);
  if (disposition->type != PN_DISP_RECEIVED) {
    pn_disposition_clear(disposition);
    disposition->type = PN_DISP_RECEIVED;
  }
  disposition->u.s_received.section_offset = section_offset;
}

bool pn_disposition_is_failed(pn_disposition_t *disposition)
{
  assert(disposition);
  if (disposition->type == PN_DISP_MODIFIED) return disposition->u.s_modified.failed;
  else return false;
}

void pn_disposition_set_failed(pn_disposition_t *disposition, bool failed)
{
  assert(disposition);
  if (disposition->type != PN_DISP_MODIFIED) {
    pn_disposition_clear(disposition);
    disposition->type = PN_DISP_MODIFIED;
  }
  disposition->u.s_modified.failed = failed;
}

bool pn_disposition_is_undeliverable(pn_disposition_t *disposition)
{
  assert(disposition);
  if (disposition->type == PN_DISP_MODIFIED) return disposition->u.s_modified.undeliverable;
  else return false;
}

void pn_disposition_set_undeliverable(pn_disposition_t *disposition, bool undeliverable)
{
  assert(disposition);
  if (disposition->type != PN_DISP_MODIFIED) {
    pn_disposition_clear(disposition);
    disposition->type = PN_DISP_MODIFIED;
  }
  disposition->u.s_modified.undeliverable = undeliverable;
}

pn_data_t *pn_disposition_annotations(pn_disposition_t *disposition)
{
  assert(disposition);
  if (disposition->type != PN_DISP_MODIFIED) {
    pn_disposition_clear(disposition);
    disposition->type = PN_DISP_MODIFIED;
  }
  pni_switch_to_data(&disposition->u.s_modified.annotations_raw, &disposition->u.s_modified.annotations);
  return disposition->u.s_modified.annotations;
}

pn_condition_t *pn_disposition_condition(pn_disposition_t *disposition)
{
  assert(disposition);
  if (disposition->type != PN_DISP_REJECTED) {
    pn_disposition_clear(disposition);
    disposition->type = PN_DISP_REJECTED;
  }
  return &disposition->u.s_rejected.condition;
}

pn_custom_disposition_t *pn_custom_disposition(pn_disposition_t *disposition)
{
  pni_disposition_to_raw(disposition);
  return &disposition->u.s_custom;
}

pn_received_disposition_t *pn_received_disposition(pn_disposition_t *disposition)
{
  if (disposition->type==PN_DISP_EMPTY) disposition->type = PN_DISP_RECEIVED;
  else if (disposition->type!=PN_DISP_RECEIVED) return NULL;
  return &disposition->u.s_received;
}

pn_rejected_disposition_t *pn_rejected_disposition(pn_disposition_t *disposition)
{
  if (disposition->type==PN_DISP_EMPTY) disposition->type = PN_DISP_REJECTED;
  else if (disposition->type!=PN_DISP_REJECTED) return NULL;
  return &disposition->u.s_rejected;
}

pn_modified_disposition_t *pn_modified_disposition(pn_disposition_t *disposition)
{
  if (disposition->type==PN_DISP_EMPTY) disposition->type = PN_DISP_MODIFIED;
  else if (disposition->type!=PN_DISP_MODIFIED) return NULL;
  return &disposition->u.s_modified;
}

pn_declared_disposition_t *pn_declared_disposition(pn_disposition_t *disposition)
{
  if (disposition->type==PN_DISP_EMPTY) disposition->type = PN_DISP_DECLARED;
  else if (disposition->type!=PN_DISP_DECLARED) return NULL;
  return &disposition->u.s_declared;
}

pn_transactional_disposition_t *pn_transactional_disposition(pn_disposition_t *disposition)
{
  if (disposition->type==PN_DISP_EMPTY) disposition->type = PN_DISP_TRANSACTIONAL;
  else if (disposition->type!=PN_DISP_TRANSACTIONAL) return NULL;
  return &disposition->u.s_transactional;
}

pn_data_t *pn_custom_disposition_data(pn_custom_disposition_t *disposition)
{
  assert(disposition);
  pni_switch_to_data(&disposition->data_raw, &disposition->data);
  return disposition->data;
}

uint64_t pn_custom_disposition_get_type(pn_custom_disposition_t *disposition)
{
  assert(disposition);
  return disposition->type;
}

void pn_custom_disposition_set_type(pn_custom_disposition_t *disposition, uint64_t type)
{
  assert(disposition);
  disposition->type = type;
}

pn_condition_t *pn_rejected_disposition_condition(pn_rejected_disposition_t *disposition)
{
  assert(disposition);
  return &disposition->condition;
}

uint32_t pn_received_disposition_get_section_number(pn_received_disposition_t *disposition)
{
  assert(disposition);
  return disposition->section_number;
}

void pn_received_disposition_set_section_number(pn_received_disposition_t *disposition, uint32_t section_number)
{
  assert(disposition);
  disposition->section_number = section_number;
}

uint64_t pn_received_disposition_get_section_offset(pn_received_disposition_t *disposition)
{
  assert(disposition);
  return disposition->section_offset;
}

void pn_received_disposition_set_section_offset(pn_received_disposition_t *disposition, uint64_t section_offset)
{
  assert(disposition);
  disposition->section_offset = section_offset;
}

bool pn_modified_disposition_is_failed(pn_modified_disposition_t *disposition) {
  assert(disposition);
  return disposition->failed;
}

void pn_modified_disposition_set_failed(pn_modified_disposition_t *disposition, bool failed)
{
  assert(disposition);
  disposition->failed = failed;
}

bool pn_modified_disposition_is_undeliverable(pn_modified_disposition_t *disposition)
{
  assert(disposition);
  return disposition->undeliverable;
}

void pn_modified_disposition_set_undeliverable(pn_modified_disposition_t *disposition, bool undeliverable)
{
  assert(disposition);
  disposition->undeliverable = undeliverable;
}

pn_data_t *pn_modified_disposition_annotations(pn_modified_disposition_t *disposition)
{
  assert(disposition);
  pni_switch_to_data(&disposition->annotations_raw, &disposition->annotations);
  return disposition->annotations;
}

pn_bytes_t pn_declared_disposition_get_id(pn_declared_disposition_t *disposition)
{
  assert(disposition);
  return disposition->id;
}

void pn_declared_disposition_set_id(pn_declared_disposition_t *disposition, pn_bytes_t id)
{
  assert(disposition);
  pn_bytes_free(disposition->id);
  disposition->id = pn_bytes_dup(id);
}

pn_bytes_t pn_transactional_disposition_get_id(pn_transactional_disposition_t *disposition)
{
  assert(disposition);
  return disposition->id;
}

void pn_transactional_disposition_set_id(pn_transactional_disposition_t *disposition, pn_bytes_t id)
{
  assert(disposition);
  pn_bytes_free(disposition->id);
  disposition->id = pn_bytes_dup(id);
}

uint64_t pn_transactional_disposition_get_outcome_type(pn_transactional_disposition_t *disposition)
{
  assert(disposition);
  if (disposition->outcome_raw.size) {
    bool qtype = false;
    uint64_t type;
    pn_amqp_decode_DQLq(disposition->outcome_raw, &qtype, &type);
    if (qtype) {
      return type;
    }
  }
  return PN_DISP_EMPTY;
}

void pn_transactional_disposition_set_outcome_type(pn_transactional_disposition_t *disposition, uint64_t type)
{
  assert(disposition);
  // Generate a described LIST0 directly - this needs a max of 11 bytes
  char outcome_scratch[11];
  pn_rwbytes_t scratch = {.size=sizeof(outcome_scratch), .start=outcome_scratch};
  pn_bytes_t outcome_raw = pn_amqp_encode_DLEe(&scratch, type);
  pn_bytes_free(disposition->outcome_raw);
  disposition->outcome_raw = pn_bytes_dup(outcome_raw);
}
