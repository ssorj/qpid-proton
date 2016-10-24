#ifndef PROTON_CONNECTION_ENGINE_H
#define PROTON_CONNECTION_ENGINE_H

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


/**
 * @file
 * @defgroup connection_engine Connection Engine
 * @ingroup engine
 *
 * **Experimental**: AMQP input bytes into proton @ref event "events", and
 * application calls to the @ref engine API into AMQP output bytes.
 *
 * Each individual engine is not thread safe but separate engines can be used
 * concurrently. A multi-threaded application must serialize activity for each
 * connection but can process separate connections concurrently.
 *
 * Note on blocking: the _read/write_buffer and _read/write_done functions can
 * generate events that may cause the engine to finish. Before you wait for IO,
 * always drain pn_connection_engine_dispatch() till it returns NULL and check
 * pn_connection_engine_finished() in case there is nothing more to do.
 *
 * Note on error handling: the pn_connection_engine_*() functions do not return
 * an error code. If an error occurs it will be reported as a
 * PN_TRANSPORT_ERROR event and pn_connection_engine_finished() will return
 * true once all final events have been processed.
 *
 * @{
 */

#include <proton/condition.h>
#include <proton/event.h>
#include <proton/import_export.h>
#include <proton/types.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * connection_engine is a plain struct, not a proton object.  Use
 * pn_connection_engine_init to initialize and pn_connection_engine_final before
 * freeing.
 */
typedef struct pn_connection_engine_t {
    pn_connection_t* connection;
    pn_transport_t* transport;
    pn_collector_t* collector;
    pn_event_t* event;
} pn_connection_engine_t;

/**
 * Initialize a pn_connection_engine_t struct, create a pn_connection_t and
 * pn_transport_t. You can configure security properties on the connection,
 * call connection_engine_start() to bind the transport before using the engine.
 *
 * @return 0 on success, a proton error code on failure (@see error.h)
 */
PN_EXTERN int pn_connection_engine_init(pn_connection_engine_t* engine);

/**
 * Free resources used by the engine, set the connection and transport pointers
 * to NULL.
 */
PN_EXTERN void pn_connection_engine_final(pn_connection_engine_t* engine);

/**
 * Get the engine's read buffer. Read data from your IO source to buf.start, up
 * to a max of buf.size. Then call pn_connection_engine_read_done().
 *
 * buf.size==0 means the engine cannot read presently, calling
 * pn_connection_engine_dispatch() may create more buffer space.
 */
PN_EXTERN pn_rwbytes_t pn_connection_engine_read_buffer(pn_connection_engine_t*);

/**
 * Consume the first n bytes of data in pn_connection_engine_read_buffer() and
 * update the buffer.
 */
PN_EXTERN void pn_connection_engine_read_done(pn_connection_engine_t*, size_t n);

/**
 * Close the read side of the transport when no more data is available.
 * Note there may still be events for pn_connection_engine_dispatch() or data
 * in pn_connection_engine_write_buffer()
 */
PN_EXTERN void pn_connection_engine_read_close(pn_connection_engine_t*);

/**
 * Get the engine's write buffer. Write data from buf.start to your IO destination,
 * up to a max of buf.size. Then call pn_connection_engine_write_done().

 * buf.size==0 means the engine has nothing to write presently.  Calling
 * pn_connection_engine_dispatch() may generate more data.
 */
PN_EXTERN pn_bytes_t pn_connection_engine_write_buffer(pn_connection_engine_t*);

/**
 * Call when the first n bytes of pn_connection_engine_write_buffer() have been
 * written to IO and can be re-used for new data.  Updates the buffer.
 */
PN_EXTERN void pn_connection_engine_write_done(pn_connection_engine_t*, size_t n);

/**
 * Call when the write side of IO has closed and no more data can be written.
 * Note that there may still be events for pn_connection_engine_dispatch() or
 * data to read into pn_connection_engine_read_buffer().
 */
PN_EXTERN void pn_connection_engine_write_close(pn_connection_engine_t*);

/**
 * Close both sides of the transport, equivalent to
 *
 *     pn_connection_engine_read_close(); pn_connection_engine_write_close()
 *
 * To pass a transport error to the handler, set it on
 * pn_connection_engine_condition() *before* calling
 * pn_connection_engine_disconnected().  Note this is different from an AMQP
 * close sent by the remote peer, which sets the connection condition.
 *
 * You must still call pn_connection_engine_dispatch() to process final
 * events.
 */
PN_EXTERN void pn_connection_engine_disconnected(pn_connection_engine_t*);

/**
 * Get the next available event.
 * Call in a loop until it returns NULL to dispatch all available events.
 * Note this call may modify the read and write buffers.
 *
 * @return Pointer to the next event, or NULL if there are none available.
 *
 */
PN_EXTERN pn_event_t* pn_connection_engine_dispatch(pn_connection_engine_t*);

/**
 * Return true if the engine is finished - all data has been written, all
 * events have been handled and the transport is closed.
 */
PN_EXTERN bool pn_connection_engine_finished(pn_connection_engine_t*);

/**
 * Return true if the engine transport is closed. There may still be
 * outstanding events to process.

 * Check if the transport has been closed internally (e.g. by an authentication
 * failure) before blocking for IO.
 */
PN_EXTERN bool pn_connection_engine_closed(pn_connection_engine_t*);

/**
 * Get the AMQP connection, owned by the pn_connection_engine_t.
 */
PN_EXTERN pn_connection_t* pn_connection_engine_connection(pn_connection_engine_t*);

/**
 * Get the proton transport, owned by the pn_connection_engine_t.
 */
PN_EXTERN pn_transport_t* pn_connection_engine_transport(pn_connection_engine_t*);

/**
 * Get the condition object for the engine's transport.
 *
 * Note that IO errors should be set on this, the transport condition, not on
 * the pn_connection_t condition. The connection's condition is for errors
 * received via the AMQP protocol, the transport condition is for errors in the
 * the IO layer such as a socket read or disconnect errors.
 */
PN_EXTERN pn_condition_t* pn_connection_engine_condition(pn_connection_engine_t*);

/** @}*/

#ifdef __cplusplus
}
#endif

#endif // PROTON_CONNECTION_ENGINE_H
