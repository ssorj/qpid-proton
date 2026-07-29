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


#include "framing.h"

#include "core/transport.h"
#include "util.h"

#include <assert.h>

static inline void pn_do_tx_trace(pn_logger_t *logger, uint16_t ch, pn_bytes_t frame)
{
  if (PN_SHOULD_LOG(logger, PN_SUBSYSTEM_AMQP, PN_LEVEL_FRAME) ) {
    if (frame.size==0) {
      pn_logger_logf(logger, PN_SUBSYSTEM_AMQP, PN_LEVEL_FRAME, "%u -> (EMPTY FRAME)", ch);
    } else {
      pni_logger_log_msg_frame(logger, PN_SUBSYSTEM_AMQP, PN_LEVEL_FRAME, frame, "%u -> ", ch);
    }
  }
}

static inline void pn_do_rx_trace(pn_logger_t *logger, uint16_t ch, pn_bytes_t frame)
{
  if (PN_SHOULD_LOG(logger, PN_SUBSYSTEM_AMQP, PN_LEVEL_FRAME) ) {
    if (frame.size==0) {
      pn_logger_logf(logger, PN_SUBSYSTEM_AMQP, PN_LEVEL_FRAME, "%u <- (EMPTY FRAME)", ch);
    } else {
      pni_logger_log_msg_frame(logger, PN_SUBSYSTEM_AMQP, PN_LEVEL_FRAME, frame, "%u <- ", ch);
    }
  }
}

static inline void pn_do_raw_tx_trace(pn_logger_t *logger, pn_bytes_t frame, size_t size)
{
  if (PN_SHOULD_LOG(logger, PN_SUBSYSTEM_IO, PN_LEVEL_RAW)) {
    pni_logger_log_raw(logger, PN_SUBSYSTEM_IO, PN_LEVEL_RAW, frame, size, "->");
  }
}

static inline void pn_do_raw_rx_trace(pn_logger_t *logger, pn_bytes_t frame, size_t size)
{
  if (PN_SHOULD_LOG(logger, PN_SUBSYSTEM_IO, PN_LEVEL_RAW)) {
    pni_logger_log_raw(logger, PN_SUBSYSTEM_IO, PN_LEVEL_RAW, frame, size, "<-");
  }
}

ssize_t pn_read_frame(pn_frame_t *frame, const char *bytes, size_t available, uint32_t max, pn_logger_t *logger)
{
  if (available < AMQP_HEADER_SIZE) return 0;
  uint32_t size = pni_read32(&bytes[0]);
  if (max && size > max) return PN_ERR;
  if (available < size) return 0;
  unsigned int doff = 4 * (uint8_t)bytes[4];
  if (doff < AMQP_HEADER_SIZE || doff > size) return PN_ERR;

  frame->frame_payload0 = (pn_bytes_t){.size=size-doff, .start=bytes+doff};
  frame->frame_payload1 = (pn_bytes_t){.size=0,.start=NULL};
  frame->extended = (pn_bytes_t){.size=doff-AMQP_HEADER_SIZE, .start=bytes+AMQP_HEADER_SIZE};
  frame->type = bytes[5];
  frame->channel = pni_read16(&bytes[6]);

  pn_do_rx_trace(logger, frame->channel, frame->frame_payload0);
  pn_do_raw_rx_trace(logger, (pn_bytes_t){.size=size, .start=bytes}, AMQP_HEADER_SIZE+frame->extended.size+frame->frame_payload0.size+frame->frame_payload1.size);

  return size;
}

static size_t write_frame(pn_buffer_t* buffer, pn_frame_t frame, pn_logger_t *logger)
{
  size_t size = AMQP_HEADER_SIZE + frame.extended.size + frame.frame_payload0.size + frame.frame_payload1.size;

  char *bytes = pn_buffer_get_write_ptr(buffer, size);
  if (!bytes) return 0;

  // Frame size (4 bytes)
  pni_write32(&bytes[0], size);

  // Data offset (1 byte)
  bytes[4] = (AMQP_HEADER_SIZE + frame.extended.size - 1) / 4 + 1;

  // Frame type (1 byte)
  bytes[5] = frame.type;

  // Channel (2 bytes)
  pni_write16(&bytes[6], frame.channel);

  size_t offset = AMQP_HEADER_SIZE;

  // The optional extended header
  if (frame.extended.size) {
    memcpy(&bytes[offset], frame.extended.start, frame.extended.size);
    offset += frame.extended.size;
  }

  size_t payload_offset = offset;

  // The AMQP performative
  memcpy(&bytes[offset], frame.frame_payload0.start, frame.frame_payload0.size);
  offset += frame.frame_payload0.size;

  // The data payload
  memcpy(&bytes[offset], frame.frame_payload1.start, frame.frame_payload1.size);
  offset += frame.frame_payload1.size;

  pn_bytes_t payload_bytes = { .start = &bytes[payload_offset], .size = offset - payload_offset };
  pn_do_tx_trace(logger, frame.channel, payload_bytes);

  pn_bytes_t frame_bytes = { .start = bytes, .size = offset };
  pn_do_raw_tx_trace(logger, frame_bytes, size);

  assert(offset == size);

  pn_buffer_advance_write_ptr(buffer, size);

  return size;
}

static void post_frame(pn_buffer_t *output, pn_logger_t *logger, uint8_t type, uint16_t ch, pn_bytes_t performative, pn_bytes_t payload)
{
  pn_frame_t frame = {
    .type = type,
    .channel = ch,
    .frame_payload0 = performative,
    .frame_payload1 = payload
  };

  write_frame(output, frame, logger);
}

int pn_framing_send_amqp(pn_transport_t *transport, uint16_t ch, pn_bytes_t performative)
{
  if (!performative.start)
    return PN_ERR;

  post_frame(transport->output_buffer, &transport->logger, AMQP_FRAME_TYPE, ch, performative, (pn_bytes_t){0, NULL});
  transport->output_frames_ct += 1;
  return 0;
}

int pn_framing_send_amqp_with_payload(pn_transport_t *transport, uint16_t ch, pn_bytes_t performative, pn_bytes_t payload)
{
  if (!performative.start)
    return PN_ERR;

  post_frame(transport->output_buffer, &transport->logger, AMQP_FRAME_TYPE, ch, performative, payload);
  transport->output_frames_ct += 1;
  return 0;
}

int pn_framing_send_sasl(pn_transport_t *transport, pn_bytes_t performative)
{
  if (!performative.start)
    return PN_ERR;

  // All SASL frames go on channel 0
  post_frame(transport->output_buffer, &transport->logger, SASL_FRAME_TYPE, 0, performative, (pn_bytes_t){0, NULL});
  transport->output_frames_ct += 1;
  return 0;
}
