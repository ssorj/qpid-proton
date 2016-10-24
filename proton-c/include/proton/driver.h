#ifndef PROTON_DRIVER_H
#define PROTON_DRIVER_H

/*
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
 */

/* FIXME aconway 2016-10-13: TOO:
   - handle transport ticks
   - support for scheduled wakeup (leave task queueing outside like conn wakeup)
   - check when driver is "empty" - not monitoring anything. For clean shutdown.
*/

/*@file
  @defgroup driver


*/
#include <proton/types.h>
#include <proton/import_export.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct pn_connection_engine_t pn_connection_engine_t;

/**
 * @defgroup driver The proton Driver API.
 *
 * **Experimental**: IO driver for a multi-threaded proton application
 *
 * The driver hides the details of the underlying IO platform, and handles IO
 * read/write events.  Multiple threads call pn_driver_wait(), which returns
 * a @ref pn_connection_engine_t when it has events to dispatch.
 *
 * Each connection can only be processed in a single thread at a time. The
 * pn_driver_wait() threads dispatch all the available events, then call
 * pn_driver_watch() to let the driver resume monitoring IO for the connection.
 * Different connections can be processed concurrently.
 *
 * The driver allows you to:
 *
 * - create outgoing connections
 * - listen for incoming connections
 * - wake connections for application processing
 *
 * Waking a connection causes it to be returned by a  pn_driver_wait() call even
 * if there are no IO events pending, so that you can safely do application
 * processing that is not triggered by IO.
 *
 * ## Thread Safety
 *
 * Functions that take a pn_driver_t* parameter are thread safe.
 *
 * Unless noted otherwise, functions that take a pn_driver_connection_t* or
 * pn_driver_listener_t* are not thread safe. They can be called sequentially
 * after the connection/listener has been returned by pn_driver_wait() and until
 * the connection/listener has been passed back to the driver by
 * pn_driver_watch() or pn_driver_accept()
 *
 * ## Error handling
 *
 * Driver functions do not return an error code. Errors are indicated by
 * PN_DRIVER_CONNECTION_FINISHED or PN_DRIVER_LISTENER_FINISHED events where the
 * connection or listener carries the error information.
 *
 * ## Context information
 *
 * You can get the pn_driver_connection_t* associated with a pn_connection_t* via
 * pn_driver_connection_get(). You can attach arbitrary additional data to the
 * driver connection without extra allocations with this technique:

 struct my_connection {
 pn_driver_connection_t driver_conn;
 // your additional data members here.
 }
 *
 * Pass a pointer to the driver_conn member in a my_connection struct to
 * pn_driver_connection_init() pn_driver_connect() and pn_driver_accept().  When
 * pn_driver_wait() returns a pn_driver_connection_t*, you can cast it to
 * my_connection* to access your data.
 *
 * You should not use pn_connection_context() with driver connections as
 * the driver may use it internally.

 *
 * ## Ease of use features
 *
 * This driver provides minimal features to hide the underlying IO platorm.
 * Additional features can be layered on top, but are not built-in.
 *
 * For example: a feature to "inject" function objects to a connection can be
 * implemented by using the pn_driver_wake() function and associating a
 * queue of functions with the connection, this is left for higher layers
 * because the best way to implement it will depend on the environment.
 *
 * @{
 */

/**
 * The driver struct, initialize with pn_driver_init
 */
typedef struct pn_driver_t pn_driver_t;

/**
 * The driver connection struct, initialize with pn_driver_connection_init.
 * Call pn_driver_engine() to get the contained pn_connection_engine_t.
 */
typedef struct pn_driver_connection_t pn_driver_connection_t;

/**
 * The driver listener struct, initialize with pn_driver_listener_init
 */
typedef struct pn_driver_listener_t pn_driver_listener_t;

/**
 * Type of event returned by pn_driver_wait()
 */
typedef enum {
    PN_DRIVER_CONNECTION_READY,    /**< Connection ready for dispatch */
    PN_DRIVER_CONNECTION_FINISHED, /**< Connection no longer active */
    PN_DRIVER_LISTENER_READY,      /**< Listener ready for accept */
    PN_DRIVER_LISTENER_FINISHED,   /**< Listener no longer active */
    PN_DRIVER_INTERRUPT            /**< pn_driver_interrupt() called */
} pn_driver_event_type_t;

/**
 * Event returned by pn_driver_wait()
 */
typedef struct pn_driver_event_t {
    pn_driver_event_type_t type;
    union {
        pn_driver_connection_t *connection;
        pn_driver_listener_t *listener;
    };
} pn_driver_event_t;

/**
 * Initialize a pn_driver_t struct
 */
void pn_driver_init(pn_driver_t *d);

/**
 * Finalize a driver struct, free all resources. Driver itself can be freed afterwards.
 */
void pn_driver_final(pn_driver_t *d);

/**
 * Wait for a driver event. Can be called in multiple threads concurrently.
 * It is safe to use the connection/listener in the returned event until it is
 * passed back to the driver via pn_driver_watch() or pn_driver_accept()
 */
pn_driver_event_t pn_driver_wait(pn_driver_t* d);

/**
 * Return PN_DRIVER_INTERRUPT in a single thread calling wait(). Thread safe.
 */
void pn_driver_interrupt(pn_driver_t* d);

/**
 * Initialize a driver connection struct
 */
void pn_driver_connection_init(pn_driver_t*, pn_driver_connection_t*);

/**
 * Finalize a driver connection after it has been returned by a PN_DRIVER_CONNECTION_FINISHED.
 * Can be freed after.
 */
void pn_driver_connection_final(pn_driver_connection_t *c);

/**
 * Connect to host:service with an initialized pn_driver_connection_t (thread
 * safe).  When there are events to dispatch, c it will be returned by
 * pn_driver_wait() in a PN_DRIVER_CONNECTION_READY event. When c is finished
 * and can be finalized and freed, it will be returned in a
 * PN_DRIVER_CONNECTION_FINISHED event.
 *
 * @param c initialized driver connection struct
 * @param host network host name
 * @param service network service (aka port) name
 * @param network can be NULL, placeholder for future multi-network drivers.
 */
void pn_driver_connect(pn_driver_connection_t* c, const char* host, const char* service, const char *network);

/**
 * Get the pn_connection_engine_t owned by a connection returned by pn_driver_wait().
 */
pn_connection_engine_t *pn_driver_engine(pn_driver_connection_t *c);

/**
 * Pass a connection that was previously returned by pn_driver_wait() back to the
 * driver so it can monitor IO. It is not safe to use the connection until it is
 * returned again by pn_driver_wait().
 */
void pn_driver_watch(pn_driver_connection_t *c);

/**
 * Cause a PN_DRIVER_CONNECTION_READY event for dc to be returned as soon as
 * possible, even if there are no IO events (thread safe).
 */
void pn_driver_wake(pn_driver_connection_t *dc);

/**
 * Initialize a driver listener
 */
void pn_driver_listener_init(pn_driver_t*, pn_driver_listener_t *);

/**
 * Finalize a driver listener
 */
void pn_driver_listener_final(pn_driver_listener_t *);

/**
 * Listen on host:service with an initialized pn_driver_listener_t (thread safe).
 * When there is an incoming connection pn_driver_wait() will return PN_DRIVER_LISTENER_READY,
 * you should accept it with pn_driver_accept(). When the listener is closed it will
 * be returned in a PN_DRIVER_LISTENER_FINISHED event.
 *
 * @param l initialized driver listener struct
 * @param host network host name
 * @param service network service (aka port) name
 * @param network can be NULL, placeholder for future multi-network drivers.
 * @param backlog number of incoming connections to buffer.
 */
void pn_driver_listen(pn_driver_listener_t* l, const char* host, const char* service, const char *network, int backlog);

/**
 * Accept a new connection on a listener returned in a PN_DRIVER_LISTENER_READY
 * event by pn_driver_wait() It is not safe to use l or c until they are
 * returned again by pn_driver_wait()
 *
 * @param l a ready listener, returned in a PN_DRIVER_LISTENER_READY event.
 * @param c an initialized connection, not previously used for connect or accept.
 */
void pn_driver_accept(pn_driver_listener_t* l, pn_driver_connection_t* c);

/**
 * Close the listener (thread safe).
 * Once closed it will be returned in a PN_DRIVER_LISTENER_FINISHED event.
 */
void pn_driver_listener_close(pn_driver_listener_t *l);

/**
 * Get the error (if any) on a listener returned in a PN_DRIVER_LISTENER_FINISHED event.
 */
const char* pn_driver_listener_error(pn_driver_listener_t*);

/** Get the pn_driver_connection_t associated with a pn_connection_t if there is one */
pn_driver_connection_t *pn_driver_connection_get(pn_connection_t*);

/**
 * @}
 */

#ifdef __cplusplus
}
#endif

/* FIXME aconway 2016-10-20: Build flags to get consistent link/include of driver. Exports. */
#ifdef PN_DRIVER_INCLUDE
#include PN_DRIVER_INCLUDE
#else
#error "Define PN_DRIVER_INCLUDE as the driver implementation header file"
#endif

#endif // PROTON_DRIVER_IMPL_H
