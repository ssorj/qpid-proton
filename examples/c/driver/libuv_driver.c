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

/*
 libuv loop functions are thread unsafe. The only exception is uv_async_send()
 which is a thread safe "wakeup" that can wake the uv_loop from another thread.

 To provide concurrency this driver uses a "leader-worker-follower" model.

 - Multiple threads can be "workers" and concurrently process distinct driver connections
   or listeners that have been woken by IO and now have non-IO work to do.

 - Only one thread, the "leader", is allowed to call unsafe libuv functions and
   run the uv_loop to wait for IO events.

 - Threads with no work to do are "followers", they wait on the leader.

 - The leader runs the uv_loop for one iteration, then gives up leadership and
   becomes a worker. One of the followers becomes the next leader.

 This model is symmetric: any thread can take on any role based on run-time
 requirements. It also allows the IO and non-IO work associated with an IO
 wake-up to be processed in a single thread with no context switches.

 Connections and listeners both contain a "dsocket". Requests to modify a
 dsocket are queued on the driver.leader_q to be handled by the leader. Sockets
 that are ready for user processing are queued on driver.user_q.

 Connections can be closed by IO (read EOF, read/write error) or by proton
 events (SASL failures, application closing the connection etc.) Once a
 connection is fully closed (uv_socket closed + pn_connection_engine_finished)

 Listeners can similarly be closed by IO or by pn_driver_listener_close.
*/

#include <proton/driver.h>

#include <proton/engine.h>
#include <proton/message.h>
#include <proton/object.h>
#include <proton/transport.h>
#include <proton/url.h>

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <uv.h>

const char *COND_NAME = "driver";
const char *AMQP_PORT = "5672";
const char *AMQP_SERVICE_NAME = "amqp";
const char *AMQPS_PORT = "5671";
const char *AMQPS_SERVICE_NAME = "amqps";

/* Short aliases */
typedef pn_uv_socket_t dsocket;
typedef pn_uv_queue_t queue;
typedef pn_driver_t driver;

/* State of a driver socket */
typedef enum {
    INACTIVE,                   /* Initialized but not yet called connect/listen/accept */
    RUNNING,                    /* Normal operation */
    CLOSING,                    /* uv_close request pending */
    CLOSED                      /* UV close completed */
}  dsocket_state;

/* Special value for dsocket.next pointer when socket is not on any any list. */
dsocket UNLISTED;

static void push(queue *q, dsocket *ds) {
    if (ds->next != &UNLISTED)  /* Don't move if already listed. */
        return;
    ds->next = NULL;
    if (!q->front) {
        q->front = q->back = ds;
    } else {
        q->back->next = ds;
        q->back =  ds;
    }
}

static dsocket* pop(queue *q) {
    dsocket *ds = q->front;
    if (ds) {
        q->front = ds->next;
        ds->next = &UNLISTED;
    }
    return ds;
}

static void dsocket_init(dsocket* ds, driver* d, bool is_conn) {
    ds->next = &UNLISTED;
    ds->state = INACTIVE;
    ds->driver = d;
    ds->user = true;
    ds->is_conn = is_conn;
    ds->socket.data = ds;
}

static void set_addr(pn_uv_addr_t* addr, const char *host, const char *service) {
     /* For platforms that don't know about "amqp" and "amqps" services. */
    if (strcmp(service, AMQP_SERVICE_NAME) == 0)
        service = AMQP_PORT;
    else if (strcmp(service, AMQPS_SERVICE_NAME) == 0)
        service = AMQPS_PORT;
    /* Set to "\001" to indicate a NULL as opposed to an empty string "" */
    strncpy(addr->host, host ? host : "\001", sizeof(addr->host));
    strncpy(addr->service, service ? service : "\001", sizeof(addr->service));
}

static void get_addr(pn_uv_addr_t* addr, const char **host, const char **service) {
    *host = addr->host[0] == '\001' ? NULL : addr->host;
    *service = addr->service[0] == '\001' ? NULL : addr->service;
}

static void to_leader_lh(dsocket* ds) {
    ds->user = false;
    push(&ds->driver->leader_q, ds);
    uv_async_send(&ds->driver->async); /* wake up the uv_loop */
}

static void to_leader(dsocket* ds) {
    uv_mutex_lock(&ds->driver->lock);
    to_leader_lh(ds);
    uv_mutex_unlock(&ds->driver->lock);
}

static void to_user(dsocket* ds) {
    uv_mutex_lock(&ds->driver->lock);
    ds->user = true;
    push(&ds->driver->user_q, ds);
    uv_mutex_unlock(&ds->driver->lock);
}

static void on_close(uv_handle_t *socket) {
    dsocket *ds = (dsocket*)socket->data;
    uv_mutex_lock(&ds->driver->lock);
    ds->state = CLOSED;
    push(&ds->driver->user_q, ds); /* Return in FINISHED event */
    uv_mutex_unlock(&ds->driver->lock);
}

static void do_close(dsocket *ds) {
    switch ((dsocket_state)ds->state) {
      case INACTIVE:
        ds->state = CLOSED;
        to_user(ds);
        break;
      case RUNNING:
        ds->state = CLOSING;
        uv_close((uv_handle_t*)&ds->socket, on_close);
        break;
      case CLOSING:
        break;
      case CLOSED:
        to_user(ds);
        break;
    }
}

static void set_error(dsocket* ds, const char* fmt, ...) {
    if (ds->is_conn) {
        pn_connection_engine_t *eng = &((pn_driver_connection_t*)ds)->engine;
        pn_condition_t* cond = pn_connection_engine_condition(eng);
        if (!pn_condition_is_set(cond)) { /* Don't overwrite an existing error */
            va_list ap;
            va_start(ap, fmt);
            pn_condition_vformat(cond, COND_NAME, fmt, ap);
            va_end(ap);
            pn_connection_engine_disconnected(eng);
        }
    } else {                    /* Listener */
        pn_driver_listener_t *dl = (pn_driver_listener_t*)ds;
        va_list ap;
        va_start(ap, fmt);
        vsnprintf(dl->error, sizeof(dl->error), fmt, ap);
        va_end(ap);
    }
    do_close(ds);
}

static int set_uv_error(dsocket *ds, int err, const char* prefix) {
    if (err < 0)
        set_error(ds, "%s: %s", prefix, uv_strerror(err));
    return err;
}

static void on_connect(uv_connect_t* connect, int status) {
    pn_driver_connection_t* dc = (pn_driver_connection_t*)connect->data;
    if (set_uv_error(&dc->dsocket, status, "cannot connect") == 0) {
        to_user(&dc->dsocket);    /* Process initial events before doing IO */
    }
}

static void on_connection(uv_stream_t* server, int status) {
    pn_driver_listener_t* dl = (pn_driver_listener_t*)server->data;
    if (status == 0) {
        ++dl->pending;
        to_user(&dl->dsocket);
    } else {
        set_uv_error(&dl->dsocket, status, "incoming connection error");
    }
}

static void do_connect(pn_driver_connection_t *dc) {
    const char *host, *service;
    get_addr(&dc->addr, &host, &service);
    uv_getaddrinfo_t info;
    int err = uv_getaddrinfo(&dc->dsocket.driver->loop, &info, NULL, host, service, NULL);
    if (!err) {
        err = uv_tcp_connect(&dc->connect, &dc->dsocket.socket, info.addrinfo->ai_addr, on_connect);
        uv_freeaddrinfo(info.addrinfo);
    }
    if (err)
        set_error(&dc->dsocket, "connect to %s:%s: %s", host, service, uv_strerror(err));
}

static void do_listen(pn_driver_listener_t *dl) {
    const char *host, *service;
    get_addr(&dl->addr, &host, &service);
    uv_getaddrinfo_t info;
    int err = uv_getaddrinfo(&dl->dsocket.driver->loop, &info, NULL, host, service, NULL);
    if (!err) {
        err = uv_tcp_bind(&dl->dsocket.socket, info.addrinfo->ai_addr, 0);
        if (!err)
            err = uv_listen((uv_stream_t*)&dl->dsocket.socket, dl->backlog, on_connection);
        uv_freeaddrinfo(info.addrinfo);
    }
    if (err)
        set_error(&dl->dsocket, "listen on %s:%s: %s", host, service, uv_strerror(err));
}

static void do_accept(pn_driver_connection_t *dc) {
    pn_driver_listener_t *dl = dc->listener;
    const char *host, *service;
    get_addr(&dl->addr, &host, &service);

    if (dl->pending == 0) {
        set_error(&dl->dsocket, "accept from %s:%s: %s", host, service,
                  "no connection available");
        return;
    }
    --dl->pending;
    int err = uv_accept((uv_stream_t*)&dl->dsocket.socket, (uv_stream_t*)&dc->dsocket.socket);
    if (err) {
        set_error(&dl->dsocket, "accept from %s:%s: %s", host, service, uv_strerror(err));
        set_error(&dc->dsocket, "accept from %s:%s: %s", host, service, uv_strerror(err));
    }
}

static void do_activate(dsocket *ds) {
    int err = uv_tcp_init(&ds->driver->loop, &ds->socket);
    if (err) {
        set_uv_error(ds, err, "tcp socket init");
        return;
    }
    if (ds->is_conn) {
        pn_driver_connection_t *dc = (pn_driver_connection_t*)ds;
        if (dc->is_accept)
            do_accept(dc);
        else
            do_connect(dc);
        to_user(ds);              /* Process initial events */
    } else {
        do_listen((pn_driver_listener_t*)ds);
    }
}

static void on_read(uv_stream_t* stream, ssize_t nread, const uv_buf_t* buf) {
    pn_driver_connection_t *dc = (pn_driver_connection_t*)stream->data;
    if (nread >= 0) {
        pn_connection_engine_read_done(&dc->engine, nread);
        if (!dc->writing) {     /* Don't go ready if write is pending */
            uv_read_stop(stream);
            dc->reading = false;
            to_user(&dc->dsocket);
        }
    } else if (nread == UV_EOF) { /* hangup */
        pn_connection_engine_read_close(&dc->engine);
        to_user(&dc->dsocket);
    } else {
        set_uv_error(&dc->dsocket, nread, "read");
    }
}

static void on_write(uv_write_t* request, int status) {
    if (status == UV_ECANCELED)
        return;                 /* Nothing to do */
    pn_driver_connection_t *dc = (pn_driver_connection_t*)request->data;
    if (set_uv_error(&dc->dsocket, status, "write") == 0) {
        pn_connection_engine_write_done(&dc->engine, dc->writing);
        dc->writing = 0;
        if (dc->reading) {         /* Cancel the read request before going ready. */
            uv_read_stop((uv_stream_t*)&dc->dsocket.socket);
            dc->reading = false;
        }
        to_user(&dc->dsocket);
    }
}

// Read buffer allocation function just returns the engine's read buffer.
static void alloc_read_buffer(uv_handle_t* stream, size_t size, uv_buf_t* buf) {
    pn_driver_connection_t *dc = (pn_driver_connection_t*)stream->data;
    pn_rwbytes_t rbuf = pn_connection_engine_read_buffer(&dc->engine);
    *buf = uv_buf_init(rbuf.start, rbuf.size);
}

static void do_connection(pn_driver_connection_t *dc) {
    uv_mutex_lock(&dc->dsocket.driver->lock);
    bool do_wake = dc->dsocket.wake;
    dc->dsocket.wake = false;
    uv_mutex_unlock(&dc->dsocket.driver->lock);

    if (do_wake) {
        /* Detach from the IO loop before sending to user. */
        if (dc->writing) {
            uv_cancel((uv_req_t*)&dc->write);
            dc->writing  = 0;
        }
        if (dc->reading) {
            uv_read_stop((uv_stream_t*)&dc->dsocket.socket);
            dc->reading = false;
        }
        to_user(&dc->dsocket);
    } else if (pn_connection_engine_finished(&dc->engine)) {
        do_close(&dc->dsocket); /* Request close */
    } else {                    /* Check for IO */
        pn_bytes_t wbuf = pn_connection_engine_write_buffer(&dc->engine);
        pn_rwbytes_t rbuf = pn_connection_engine_read_buffer(&dc->engine);
        /* Calling write_buffer can generate events.
           Make all events are processed before we resume IO, since events may
           close the transport.
        */
        if (pn_collector_peek(dc->engine.collector)) {
            to_user(&dc->dsocket);
        } else {                    /* Really resume IO */
            if (wbuf.size > 0 && !dc->writing) {
                dc->writing = wbuf.size;
                uv_buf_t buf = uv_buf_init((char*)wbuf.start, wbuf.size);
                uv_write(&dc->write, (uv_stream_t*)&dc->dsocket.socket, &buf, 1, on_write);
            }
            if (rbuf.size > 0 && !dc->reading) {
                dc->reading = true;
                uv_read_start((uv_stream_t*)&dc->dsocket.socket, alloc_read_buffer, on_read);
            }
        }
    }
}

static void do_listener(pn_driver_listener_t *dl) {
    uv_mutex_lock(&dl->dsocket.driver->lock);
    bool do_wake = dl->dsocket.wake;
    uv_mutex_unlock(&dl->dsocket.driver->lock);
    if (do_wake)
        do_close(&dl->dsocket);
    else if (dl->pending > 0)        /* Ready for another accept */
        to_user(&dl->dsocket);
    /* Don't need to resume IO, the uv_listen call remains in force all the time. */
}

/* Called by leader to handle queued requests */
static void do_socket(dsocket *ds) {
    switch ((dsocket_state)ds->state) {
      case INACTIVE:
        do_activate(ds);
        if (ds->state == INACTIVE)
            ds->state = RUNNING;
        break;
      case RUNNING:
        if (ds->is_conn) {
            do_connection((pn_driver_connection_t*)ds);
        } else {
            do_listener((pn_driver_listener_t*)ds);
        }
        break;
      case CLOSING:
        break;
      case CLOSED:
        to_user(ds);              /* Return FINISHED event to user */
        break;
    }
}

/* Fill in event and return true or return false if no events are available. */
bool get_event_lh(driver *d, pn_driver_event_t *event) {
    if (d->interrupt > 0) {
        --d->interrupt;
        event->type = PN_DRIVER_INTERRUPT;
        event->connection = NULL;
        return true;
    }
    dsocket *ds = pop(&d->user_q);
    if (!ds) return false;
    if (ds->is_conn) {
        pn_driver_connection_t *dc = event->connection = (pn_driver_connection_t*)ds;
        if (pn_connection_engine_finished(&dc->engine) && ds->state == CLOSED)
            event->type = PN_DRIVER_CONNECTION_FINISHED;
        else
            event->type = PN_DRIVER_CONNECTION_READY;
        return true;
    } else {                    /* Listener */
        pn_driver_listener_t *dl = event->listener = (pn_driver_listener_t*)ds;
        if (dl->dsocket.state == CLOSED) {
            event->type = PN_DRIVER_LISTENER_FINISHED;
            return true;
        }
        else if (dl->pending > 0) {
            event->type = PN_DRIVER_LISTENER_READY;
            return true;
        }
    }
    return false;
}


pn_driver_event_t pn_driver_wait(struct pn_driver_t* d) {
    uv_mutex_lock(&d->lock);
    pn_driver_event_t event;
    /* Try to grab work immediately. */
    if (!get_event_lh(d, &event)) {
        /* No work available, follow the leader */
        while (d->has_leader)
            uv_cond_wait(&d->cond, &d->lock);
        d->has_leader = true;       /* I am the leader */
        while (!get_event_lh(d, &event)) { /* Lead till there is work to do. */
            /* Run IO outside the lock */
            uv_mutex_unlock(&d->lock);
            uv_run(&d->loop, UV_RUN_ONCE);
            uv_mutex_lock(&d->lock);
            /* Process leader requests outside the lock */
            for (dsocket* ds = pop(&d->leader_q); ds; ds = pop(&d->leader_q)) {
                uv_mutex_unlock(&d->lock);
                do_socket(ds);
                uv_mutex_lock(&d->lock);
            }
        }
        d->has_leader = false;
        uv_cond_signal(&d->cond); /* Signal the next leader */
    }
    uv_mutex_unlock(&d->lock);
    return event;
}

void pn_driver_interrupt(pn_driver_t *d) {
    uv_mutex_lock(&d->lock);
    ++d->interrupt;
    uv_async_send(&d->async);   /* Interrupt the UV loop */
    uv_mutex_unlock(&d->lock);
}

void pn_driver_connect(pn_driver_connection_t* dc, const char *host, const char *service, const char *network) {
    dc->is_accept = false;
    set_addr(&dc->addr, host, service);
    to_leader(&dc->dsocket);
}

void pn_driver_watch(pn_driver_connection_t *dc) {
    to_leader(&dc->dsocket);
}

static void on_connect(uv_connect_t* connect, int status);

void pn_driver_connection_init(pn_driver_t *d, pn_driver_connection_t *dc) {
    memset(dc, 0, sizeof(*dc));
    dsocket_init(&dc->dsocket, d,  true);
    pn_connection_engine_init(&dc->engine);
    pn_connection_set_context(dc->engine.connection, dc);
    dc->connect.data = dc;
    dc->write.data = dc;
}

void pn_driver_connection_final(pn_driver_connection_t* dc) {
    pn_connection_engine_final(&dc->engine);
}

void pn_driver_wake(pn_driver_connection_t* dc) {
    uv_mutex_lock(&dc->dsocket.driver->lock);
    dc->dsocket.wake = true;
    if (!dc->dsocket.user)
        to_leader_lh(&dc->dsocket);
    uv_mutex_unlock(&dc->dsocket.driver->lock);
}

void pn_driver_listener_init(pn_driver_t *d, pn_driver_listener_t *dl) {
    memset(dl, 0, sizeof(*dl));
    dsocket_init(&dl->dsocket, d, false);
}

void pn_driver_listen(pn_driver_listener_t *dl, const char *host, const char *service, const char *network, int backlog) {
    set_addr(&dl->addr, host, service);
    dl->backlog = backlog;
    dl->pending = 0;
    to_leader(&dl->dsocket);
}

void pn_driver_accept(pn_driver_listener_t* dl, pn_driver_connection_t* dc) {
    dc->is_accept = true;
    dc->listener = dl;
    to_leader(&dc->dsocket);
    to_leader(&dl->dsocket);            /* Re-activate the listener */
}

void pn_driver_listener_close(pn_driver_listener_t* dl) {
    uv_mutex_lock(&dl->dsocket.driver->lock);
    dl->dsocket.wake = true;
    if (!dl->dsocket.user)
        to_leader_lh(&dl->dsocket);
    uv_mutex_unlock(&dl->dsocket.driver->lock);
}

const char* pn_driver_listener_error(pn_driver_listener_t* dl) {
    return dl->error[0] ? dl->error : NULL;
}

void pn_driver_listener_final(pn_driver_listener_t* l) {
    /* Nothing to do. */
}

void pn_driver_init(pn_driver_t *d) {
    memset(d, '\0', sizeof(*d));
    uv_mutex_init(&d->lock);
    uv_cond_init(&d->cond);
    uv_loop_init(&d->loop);
    uv_async_init(&d->loop, &d->async, NULL); /* Just wake the loop */
}

static void on_stopping(uv_handle_t* h, void* v) {
    uv_close(h, NULL);
    if (!uv_loop_alive(h->loop))
        uv_stop(h->loop);
}

void pn_driver_final(pn_driver_t *pd) {
    driver *d = (driver*)pd;
    uv_walk(&d->loop, on_stopping, NULL); /* Close all handles */
    uv_run(&d->loop, UV_RUN_DEFAULT);     /* Run till stop, all handles closed */
    uv_loop_close(&d->loop);
    uv_mutex_destroy(&d->lock);
    uv_cond_destroy(&d->cond);
}

pn_driver_connection_t *pn_driver_connection_get(pn_connection_t* c) {
    return c ? (pn_driver_connection_t*)pn_connection_get_context(c) : NULL;
}

pn_connection_engine_t *pn_driver_engine(pn_driver_connection_t *dc) {
    return dc ? &dc->engine : NULL;
}
