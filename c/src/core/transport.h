#ifndef PROTON_CORE_TRANSPORT_H
#define PROTON_CORE_TRANSPORT_H 1

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

#include "proton/transport.h"

#include "core/condition.h"
#include "core/logger_private.h"

typedef struct pn_io_layer_t {
  ssize_t (*process_input)(struct pn_transport_t *transport, unsigned int layer, const char *, size_t);
  ssize_t (*process_output)(struct pn_transport_t *transport, unsigned int layer, char *, size_t);
  void (*handle_error)(struct pn_transport_t* transport, unsigned int layer);
  int64_t (*process_tick)(struct pn_transport_t *transport, unsigned int layer, int64_t);
  size_t (*buffered_output)(struct pn_transport_t *transport);  // how much output is held
} pn_io_layer_t;

extern const pn_io_layer_t pni_passthru_layer;
extern const pn_io_layer_t ssl_layer;
extern const pn_io_layer_t sasl_header_layer;
extern const pn_io_layer_t sasl_write_header_layer;

// Bit flag defines for the protocol layers
typedef uint8_t pn_io_layer_flags_t;
#define LAYER_NONE     0
#define LAYER_AMQP1    1
#define LAYER_AMQPSASL 2
#define LAYER_AMQPSSL  4
#define LAYER_SSL      8

typedef struct pni_sasl_t pni_sasl_t;
typedef struct pni_ssl_t pni_ssl_t;

struct pn_transport_t {
  pn_logger_t logger;
  pn_tracer_t tracer;
  pni_sasl_t *sasl;
  pni_ssl_t *ssl;
  pn_connection_t *connection;  // reference counted
  char *remote_container;
  char *remote_hostname;
  pn_bytes_t remote_offered_capabilities_raw;
  pn_bytes_t remote_desired_capabilities_raw;
  pn_bytes_t remote_properties_raw;
  // DEFAULT_MAX_FRAME_SIZE see PROTON-2640
#define PN_DEFAULT_MAX_FRAME_SIZE (32*1024)
  uint32_t   local_max_frame;
  uint32_t   remote_max_frame;
  pn_condition_t remote_condition;
  pn_condition_t condition;
  pn_error_t *error;

#define PN_IO_LAYER_CT 3
  const pn_io_layer_t *io_layers[PN_IO_LAYER_CT];

  /* dead remote detection */
  pn_millis_t local_idle_timeout;
  pn_millis_t remote_idle_timeout;
  pn_timestamp_t dead_remote_deadline;
  uint64_t last_bytes_input;

  /* keepalive */
  pn_timestamp_t keepalive_deadline;
  uint64_t last_bytes_output;

  pn_hash_t *local_channels;
  pn_hash_t *remote_channels;


  /* scratch area */
  pn_rwbytes_t scratch_space;

  // Temporary - ??
  pn_buffer_t *output_buffer;

  /* statistics */
  uint64_t bytes_input;
  uint64_t bytes_output;
  uint64_t output_frames_ct;
  uint64_t input_frames_ct;

  /* output buffered for send */
#define PN_TRANSPORT_INITIAL_BUFFER_SIZE (8*1024)
  size_t output_size;
  size_t output_pending;
  char *output_buf;

  /* input from peer */
  size_t input_size;
  size_t input_pending;
  char *input_buf;

  pn_record_t *context;

  /*
   * The maximum channel number can be constrained in several ways:
   *   1. an unchangeable limit imposed by this library code
   *   2. a limit imposed by the remote peer when the connection is opened,
   *      which this app must honor
   *   3. a limit imposed by this app, which may be raised and lowered
   *      until the OPEN frame is sent.
   * These constraints are all summed up in channel_max, below.
   */
#define PN_IMPL_CHANNEL_MAX  32767
  uint16_t local_channel_max;
  uint16_t remote_channel_max;
  uint16_t channel_max;

  pn_io_layer_flags_t allowed_layers;
  pn_io_layer_flags_t present_layers;

  bool freed;
  bool open_sent;
  bool open_rcvd;
  bool close_sent;
  bool close_rcvd;
  bool tail_closed;      // input stream closed by driver
  bool head_closed;
  bool done_processing; // if true, don't call pn_process again
  bool posted_idle_timeout;
  bool server;
  bool halt;
  bool auth_required;
  bool authenticated;
  bool encryption_required;

  bool referenced;
};

// XXX transport_unmap
void pn_unmap_channel(pn_transport_t *transport, pn_session_t *ssn);
void pn_transport_sasl_init(pn_transport_t *transport);

int pn_do_error(pn_transport_t *transport, const char *condition, PN_PRINTF_FORMAT const char *fmt, ...)
        PN_PRINTF_FORMAT_ATTR(3, 4);
void pn_set_error_layer(pn_transport_t *transport);
ssize_t pni_transport_grow_capacity(pn_transport_t *transport, size_t n);

#endif /* transport.h */
