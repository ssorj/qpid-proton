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

#include <proton/connection_engine.h>
#include <proton/driver.h>
#include <proton/engine.h>
#include <proton/sasl.h>
#include <proton/transport.h>
#include <proton/url.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* TODO aconway 2016-10-14: this example does not require libuv IO,
   it uses uv.h only for portable mutex and thread functions.
*/
#include <uv.h>

bool verbose = false;

typedef struct broker_connection_t {
    pn_driver_connection_t dc;
    bool check_queues;
} broker_connection_t;


void debug(const char* fmt, ...) {
    if (verbose) {
        va_list(ap);
        va_start(ap, fmt);
        vfprintf(stderr, fmt, ap);
        fputc('\n', stderr);
        fflush(stderr);
    }
}

static void connection_debug(pn_driver_connection_t* dc, const char* fmt, ...) {
    if (verbose) {
        va_list(ap);
        va_start(ap, fmt);
        fprintf(stderr, "[%p] broker: ", (void*)dc->engine.transport);
        vfprintf(stderr, fmt, ap);
        fputc('\n', stderr);
        fflush(stderr);
    }
}

void check(int err, const char* s) {
    if (err != 0) {
        perror(s);
        exit(1);
    }
}

void pcheck(int err, const char* s) {
    if (err != 0) {
        fprintf(stderr, "%s: %s", s, pn_code(err));
        exit(1);
    }
}

/* Simple re-sizable vector that acts as a queue */
#define VEC(T) struct { T* data; size_t len, cap; }

#define VEC_INIT(V)                                     \
    do {                                                \
        V.len = 0;                                      \
        V.cap = 16;                                     \
        void **vp = (void**)&V.data;                    \
        *vp = malloc(V.cap * sizeof(*V.data));          \
    } while(0)

#define VEC_FINAL(V) free(V.data)

#define VEC_PUSH(V, X)                                          \
    do {                                                        \
        if (V.len == V.cap) {                                   \
            V.cap *= 2;                                         \
            void **vp = (void**)&V.data;                        \
            *vp = realloc(V.data, V.cap * sizeof(*V.data));     \
        }                                                       \
        V.data[V.len++] = X;                                    \
    } while(0)                                                  \

#define VEC_POP(V)                                                      \
    do {                                                                \
        if (V.len > 0)                                                  \
            memmove(V.data, V.data+1, (--V.len)*sizeof(*V.data));       \
    } while(0)

/* Simple thread-safe queue implementation */
typedef struct queue {
    uv_mutex_t lock;
    char* name;
    VEC(pn_rwbytes_t) messages;   /* Messages on the queue */
    VEC(pn_connection_t*) waiting; /* Connections waiting to send messages from this queue */
    struct queue *next;            /* Next queue in chain */
} queue;

static void queue_init(queue *q, const char* name, queue *next) {
    debug("created queue %s", name);
    uv_mutex_init(&q->lock);
    q->name = strdup(name);
    VEC_INIT(q->messages);
    VEC_INIT(q->waiting);
    q->next = next;
}

static void queue_final(queue *q) {
    uv_mutex_destroy(&q->lock);
    free(q->name);
    for (size_t i = 0; i < q->messages.len; ++i)
        free(q->messages.data[i].start);
    VEC_FINAL(q->messages);
    for (size_t i = 0; i < q->waiting.len; ++i)
        pn_decref(q->waiting.data[i]);
    VEC_FINAL(q->waiting);
}

/* Send a message on s, or record s as eating if no messages.
   Called in s dispatch loop, assumes s has credit.
*/
static void queue_send(queue *q, pn_link_t *s) {
    pn_rwbytes_t m = { 0 };
    uv_mutex_lock(&q->lock);
    if (q->messages.len == 0) { /* Empty, record connection as waiting */
        debug("queue is empty %s", q->name);
        /* Record connection for wake-up if not already on the list. */
        pn_connection_t *c = pn_session_connection(pn_link_session(s));
        size_t i = 0;
        for (; i < q->waiting.len && q->waiting.data[i] != c; ++i)
            ;
        if (i == q->waiting.len) {
            VEC_PUSH(q->waiting, c);
        }
    } else {
        debug("sending from queue %s", q->name);
        m = q->messages.data[0];
        VEC_POP(q->messages);
    }
    uv_mutex_unlock(&q->lock);
    if (m.start) {
        /* FIXME aconway 2016-10-13: unique tags, see send.c */
        pn_delivery_t *d = pn_delivery(s, pn_dtag("tag", 4));
        pn_link_send(s, m.start, m.size);
        pn_link_advance(s);
        pn_delivery_settle(d);  /* Pre-settled */
        free(m.start);
    }
}

/* Put a message on the queue, called in receiver dispatch loop.
   If the queue was previously empty, notify waiting senders.
*/
static void queue_receive(pn_driver_t *d, queue *q, pn_rwbytes_t m) {
    debug("received to queue %s", q->name);
    uv_mutex_lock(&q->lock);
    VEC_PUSH(q->messages, m);
    if (q->messages.len == 1) { /* Was empty, notify waiting connections */
        for (size_t i = 0; i < q->waiting.len; ++i) {
            pn_connection_t *c = q->waiting.data[i];
            broker_connection_t *bc = (broker_connection_t*)pn_driver_connection_get(c);
            if (bc) {
                bc->check_queues = true;
                pn_driver_wake(&bc->dc); /* Signal that the connection should check queues */
            }
        }
        q->waiting.len = 0;
    }
    uv_mutex_unlock(&q->lock);
}

/* Thread safe set of queues */
typedef struct queues_t {
    uv_mutex_t lock;
    queue *queues;
} queues_t;

void queues_init(queues_t *qs) {
    uv_mutex_init(&qs->lock);
    qs->queues = NULL;
}

void queues_final(queues_t *qs) {
    for (queue *q = qs->queues; q; q = q->next) {
        queue_final(q);
        free(q);
    }
    uv_mutex_destroy(&qs->lock);
}

/** Get or create the named queue. */
queue* queues_get(queues_t *qs, const char* name) {
    uv_mutex_lock(&qs->lock);
    queue *q;
    for (q = qs->queues; q && strcmp(q->name, name) != 0; q = q->next)
        ;
    if (!q) {
        q = (queue*)malloc(sizeof(queue));
        queue_init(q, name, qs->queues);
        qs->queues = q;
    }
    uv_mutex_unlock(&qs->lock);
    return q;
}

/* The broker implementation */
typedef struct broker {
    pn_driver_t driver;
    pn_driver_listener_t listener;
    queues_t queues;
    char container_id[256];     /* AMQP container-id */
    int threads;
} broker;

void broker_stop(broker *b) {
    for (int i = 0; i < b->threads; ++i)
        pn_driver_interrupt(&b->driver);
}

/* Try to send if link is sender and has credit */
static void link_send(broker *b, pn_link_t *s) {
    if (pn_link_is_sender(s) && pn_link_credit(s) > 0) {
        const char *qname = pn_terminus_get_address(pn_link_source(s));
        queue *q = queues_get(&b->queues, qname);
        queue_send(q, s);
    }
}

static void queue_unsub(queue *q, pn_connection_t *c) {
    uv_mutex_lock(&q->lock);
    for (size_t i = 0; i < q->waiting.len; ++i) {
        if (q->waiting.data[i] == c){
            q->waiting.data[i] = q->waiting.data[0]; /* save old [0] */
            VEC_POP(q->waiting);
            break;
        }
    }
    uv_mutex_unlock(&q->lock);
}

/* Unsubscribe from the queue of interest to this link. */
static void link_unsub(broker *b, pn_link_t *s) {
    if (pn_link_is_sender(s)) {
        const char *qname = pn_terminus_get_address(pn_link_source(s));
        if (qname) {
            queue *q = queues_get(&b->queues, qname);
            queue_unsub(q, pn_session_connection(pn_link_session(s)));
        }
    }
}

/* Called in connection's event loop when a connection is woken for messages.*/
static void connection_unsub(broker *b, pn_connection_t *c) {
    for (pn_link_t *l = pn_link_head(c, 0); l != NULL; l = pn_link_next(l, 0))
        link_unsub(b, l);
}

static void session_unsub(broker *b, pn_session_t *ssn) {
    pn_connection_t *c = pn_session_connection(ssn);
    for (pn_link_t *l = pn_link_head(c, 0); l != NULL; l = pn_link_next(l, 0)) {
        if (pn_link_session(l) == ssn)
            link_unsub(b, l);
    }
}

static void print_condition(pn_event_t *e, pn_condition_t *cond) {
    if (pn_condition_is_set(cond)) {
        fprintf(stderr, "%s: %s: %s\n", pn_event_type_name(pn_event_type(e)),
                pn_condition_get_name(cond), pn_condition_get_description(cond));
    }
}

const int WINDOW=10;            /* Incoming credit window */

broker_connection_t *broker_new_connection(broker* b) {
    broker_connection_t *bc = (broker_connection_t*)calloc(1, sizeof(broker_connection_t));
    pn_driver_connection_init(&b->driver, &bc->dc);
    bc->check_queues = false;
    pn_connection_engine_t *eng = pn_driver_engine(&bc->dc);
    pn_connection_set_container(eng->connection, b->container_id);
    pn_transport_set_server(eng->transport);
    /* No security */
    pn_transport_require_auth(eng->transport, false);
    pn_sasl_allowed_mechs(pn_sasl(eng->transport), "ANONYMOUS");
    return bc;
}

/* Dispatch events for a broker connection */
static void broker_dispatch(broker* b, broker_connection_t* bc) {
    pn_connection_engine_t *eng = pn_driver_engine(&bc->dc);
    pn_connection_t *c = pn_connection_engine_connection(eng);

    if (bc->check_queues) {
        int flags = PN_LOCAL_ACTIVE&PN_REMOTE_ACTIVE;
        for (pn_link_t *l = pn_link_head(c, flags); l != NULL; l = pn_link_next(l, flags))
            link_send(b, l);
        bc->check_queues = false;
    }

    pn_event_t *e;
    while ((e = pn_connection_engine_dispatch(eng)) != NULL) {
        connection_debug(&bc->dc, "dispatch event: %s\n",
                         pn_event_type_name(pn_event_type(e)));
        switch (pn_event_type(e)) {
          case PN_CONNECTION_REMOTE_OPEN: {
              pn_connection_open(pn_event_connection(e)); /* Complete the open */
              break;
          }
          case PN_SESSION_REMOTE_OPEN: {
              pn_session_open(pn_event_session(e));
              break;
          }
          case PN_LINK_REMOTE_OPEN: {
              pn_link_t *l = pn_event_link(e);
              if (pn_link_is_sender(l)) {
                  const char *source = pn_terminus_get_address(pn_link_remote_source(l));
                  pn_terminus_set_address(pn_link_source(l), source);
              } else {
                  const char* target = pn_terminus_get_address(pn_link_remote_target(l));
                  pn_terminus_set_address(pn_link_target(l), target);
                  pn_link_flow(l, WINDOW);
              }
              pn_link_open(l);
              break;
          }
          case PN_LINK_FLOW: {
              link_send(b, pn_event_link(e));
              break;
          }
          case PN_DELIVERY: {
              pn_delivery_t *d = pn_event_delivery(e);
              pn_link_t *r = pn_delivery_link(d);
              if (pn_link_is_receiver(r) &&
                  pn_delivery_readable(d) && !pn_delivery_partial(d))
              {
                  size_t size = pn_delivery_pending(d);
                  /* The broker does not decode the message, just forwards it. */
                  pn_rwbytes_t m = { size, (char*)malloc(size) };
                  pn_link_recv(r, m.start, m.size);
                  const char *qname = pn_terminus_get_address(pn_link_target(r));
                  queue_receive(&b->driver, queues_get(&b->queues, qname), m);
                  pn_delivery_update(d, PN_ACCEPTED);
                  pn_delivery_settle(d);
                  pn_link_flow(r, WINDOW - pn_link_credit(r));
              }
              /* FIXME aconway 2016-09-30: settlement... */
              break;
          }

          case PN_TRANSPORT_CLOSED:
            connection_unsub(b, pn_event_connection(e));
            print_condition(e, pn_transport_condition(pn_event_transport(e)));
            break;
          case PN_CONNECTION_REMOTE_CLOSE:
            print_condition(e, pn_connection_remote_condition(pn_event_connection(e)));
            connection_unsub(b, pn_event_connection(e));
            pn_connection_close(pn_event_connection(e));
            break;
          case PN_SESSION_REMOTE_CLOSE:
            print_condition(e, pn_session_remote_condition(pn_event_session(e)));
            session_unsub(b, pn_event_session(e));
            pn_session_close(pn_event_session(e));
            pn_session_free(pn_event_session(e));
            break;
          case PN_LINK_REMOTE_CLOSE:
            print_condition(e, pn_link_remote_condition(pn_event_link(e)));
            link_unsub(b, pn_event_link(e));
            pn_link_close(pn_event_link(e));
            pn_link_free(pn_event_link(e));
            break;
          default:
            break;
        }
    }
}

static void broker_thread(void *void_broker) {
    broker *b = (broker*)void_broker;
    pn_driver_t *d = (pn_driver_t*)&b->driver;
    while (true) {
        pn_driver_event_t e = pn_driver_wait(d);
        switch (e.type) {
          case PN_DRIVER_LISTENER_READY: {
              broker_connection_t *bc = broker_new_connection(b);
              connection_debug(e.connection, "accepting");
              pn_driver_accept(e.listener, &bc->dc);
              break;
          }
          case PN_DRIVER_CONNECTION_READY:
            connection_debug(e.connection, "dispatching");
            broker_dispatch(b, (broker_connection_t*)e.connection);
            connection_debug(e.connection, "watching");
            pn_driver_watch(e.connection);
            break;
          case PN_DRIVER_INTERRUPT:
            debug("interrupted");
            return;
          case PN_DRIVER_CONNECTION_FINISHED:
            connection_debug(e.connection, "connection finished");
            pn_driver_connection_final(e.connection);
            free(e.connection);
            break;
          case PN_DRIVER_LISTENER_FINISHED: {
              debug("listener finished");
              const char *errstr = pn_driver_listener_error(e.listener);
              if (errstr)
                  fprintf(stderr, "listener error: %s\n", errstr);
              pn_driver_listener_final(e.listener);
              broker_stop(b);
              break;
          }
        }
    }
}

static void usage(const char *arg0) {
    fprintf(stderr, "Usage: %s [-d] [-a url] [-t thread-count]\n", arg0);
    exit(1);
}

int main(int argc, char **argv) {
    /* Command line options */
    const char *urlstr = NULL;
    size_t nthreads = 4;        /* Thread count. */
    int opt;
    while ((opt = getopt(argc, argv, "a:t:d")) != -1) {
        switch (opt) {
          case 'a': urlstr = optarg; break;
          case 't': nthreads = atoi(optarg); break;
          case 'd': verbose = true; break;
          default: usage(argv[0]); break;
        }
    }
    if (optind < argc)
        usage(argv[0]);

    /* Run the broker */
    broker b;
    b.threads = nthreads;
    pn_driver_init(&b.driver);
    pn_driver_listener_init(&b.driver, &b.listener);
    snprintf(b.container_id, sizeof(b.container_id), "%s:%d", argv[0], getpid());
    queues_init(&b.queues);

    /* Parse the URL or use default values */
    pn_url_t *url = urlstr ? pn_url_parse(urlstr) : NULL;
    const char *host = url ? pn_url_get_host(url) : NULL;
    const char *port = url ? pn_url_get_port(url) : NULL;
    if (!port) port = "amqp";

    pn_driver_listen(&b.listener, host, port, NULL, 16);
    printf("listening on '%s:%s' %zd threads\n", host, port, nthreads);

    if (url) pn_url_free(url);
    uv_thread_t* threads = (uv_thread_t*)calloc(sizeof(uv_thread_t), nthreads);
    for (size_t i = 0; i < nthreads-1; ++i) {
        check(uv_thread_create(&threads[i], broker_thread, &b), "pthread_create");
    }
    broker_thread(&b);          /* Use the main thread too. */
    for (size_t i = 0; i < nthreads-1; ++i) {
        check(uv_thread_join(&threads[i]), "pthread_join");
    }
    pn_driver_final(&b.driver);
    free(threads);
    return 0;
}
