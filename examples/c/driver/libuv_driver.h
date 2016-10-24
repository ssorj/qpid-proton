#ifndef LIBUV_DRIVER_H
#define LIBUV_DRIVER_H
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

/**@cond INTERNAL  */

/*
  Definitions for libuv driver implementation.
  Included as part of proton/driver.h if libuv implementation is selected.

  Defines structs specific to libuv driver, see proton/driver.h for public API.
*/

#include <proton/connection_engine.h>
#include <uv.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct pn_driver_t pn_driver_t;

typedef struct pn_uv_addr_t {
    char host[NI_MAXHOST];
    char service[NI_MAXSERV];
} pn_uv_addr_t;

typedef struct pn_uv_socket_t {
    /* Protected by driver.lock */
    struct pn_uv_socket_t* next;
    bool user:1;                /* In use by user thread */
    bool wake:1;                /* Wake or close requested */

    /* Remaining members only used in leader thread */
    int state;
    uv_tcp_t socket;
    pn_driver_t *driver;
    bool is_conn:1;             /* True for connection, false for listener. */
} pn_uv_socket_t;

typedef struct pn_driver_connection_t {
    pn_uv_socket_t dsocket;

    /* Members only used by user or leader thread, never both at the same time. */
    pn_connection_engine_t engine;
    union {
        struct {
            pn_uv_addr_t addr;  /* for connect() */
            uv_connect_t connect;
        };
        pn_driver_listener_t *listener; /* for accept() */
    };
    uv_write_t write;
    size_t writing;
    bool reading:1;
    bool is_accept:1;
} pn_driver_connection_t;

#define PN_UV_MAX_ERR_LEN 128

typedef struct pn_driver_listener_t {
    pn_uv_socket_t dsocket;

    /* Members only used by leader thread */
    pn_uv_addr_t addr;
    size_t backlog;
    size_t pending;
    char error[PN_UV_MAX_ERR_LEN];
} pn_driver_listener_t;

typedef struct pn_uv_queue_t { pn_uv_socket_t *front, *back; } pn_uv_queue_t;

typedef struct pn_driver_t {
    uv_mutex_t lock;
    pn_uv_queue_t user_q;
    pn_uv_queue_t leader_q;
    size_t interrupt;

    uv_cond_t cond;
    uv_loop_t loop;
    uv_async_t async;

    bool has_leader:1;
} pn_driver_t;

#ifdef __cplusplus
}
#endif

/**@endcond INTERNAL  */

#endif // LIBUV_DRIVER_H
