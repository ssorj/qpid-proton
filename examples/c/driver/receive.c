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

#include <proton/connection.h>
#include <proton/connection_engine.h>
#include <proton/delivery.h>
#include <proton/driver.h>
#include <proton/link.h>
#include <proton/message.h>
#include <proton/session.h>
#include <proton/transport.h>
#include <proton/url.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef char str[1024];

typedef struct app_data_t {
    str address;
    str container_id;
    pn_rwbytes_t message_buffer;
    int message_count;
    int received;
} app_data_t;

static const int BATCH = 100; /* Batch size for unlimited receive */

static void print_condition(pn_event_t *e, pn_condition_t *cond) {
    if (pn_condition_is_set(cond)) 
        fprintf(stderr, "%s: %s: %s\n", pn_event_type_name(pn_event_type(e)),
                pn_condition_get_name(cond), pn_condition_get_description(cond));
}

static void dispatch(app_data_t* app, pn_connection_engine_t* eng) {
    for (pn_event_t *event = pn_connection_engine_dispatch(eng);
         event != NULL;
         event = pn_connection_engine_dispatch(eng)
    ) {
        switch (pn_event_type(event)) {

          case PN_CONNECTION_INIT: {
              pn_connection_t* c = pn_event_connection(event);
              pn_connection_set_container(c, app->container_id);
              pn_connection_open(c);
              pn_session_t* s = pn_session(c);
              pn_session_open(s);
              pn_link_t* l = pn_receiver(s, "my_receiver");
              pn_terminus_set_address(pn_link_source(l), app->address);
              pn_link_open(l);
              /* cannot receive without granting credit: */
              pn_link_flow(l, app->message_count ? app->message_count : BATCH);
          } break;

          case PN_DELIVERY: {
              /* A message has been received */
              pn_link_t *link = NULL;
              pn_delivery_t *dlv = pn_event_delivery(event);
              if (pn_delivery_readable(dlv) && !pn_delivery_partial(dlv)) {
                  link = pn_delivery_link(dlv);
                  /* Accept the delivery */
                  pn_delivery_update(dlv, PN_ACCEPTED);
                  /* done with the delivery, move to the next and free it */
                  pn_link_advance(link);
                  pn_delivery_settle(dlv);  /* dlv is now freed */

                  if (app->message_count == 0) {
                      /* receive forever - see if more credit is needed */
                      if (pn_link_credit(link) < BATCH/2) {
                          /* Grant enough credit to bring it up to BATCH: */
                          pn_link_flow(link, BATCH - pn_link_credit(link));
                      }
                  } else if (++app->received >= app->message_count) {
                      /* done receiving, close the endpoints */
                      printf("%d messages received\n", app->received);
                      pn_session_t *ssn = pn_link_session(link);
                      pn_link_close(link);
                      pn_session_close(ssn);
                      pn_connection_close(pn_session_connection(ssn));
                  }
              }
          } break;

          case PN_TRANSPORT_ERROR:
            print_condition(event, pn_transport_condition(pn_event_transport(event)));
            break;

          case PN_CONNECTION_REMOTE_CLOSE:
            print_condition(event, pn_connection_remote_condition(pn_event_connection(event)));
            pn_connection_close(pn_event_connection(event));
            break;

          case PN_SESSION_REMOTE_CLOSE:
            print_condition(event, pn_session_remote_condition(pn_event_session(event)));
            pn_connection_close(pn_event_connection(event));
            break;

          case PN_LINK_REMOTE_CLOSE:
          case PN_LINK_REMOTE_DETACH:
            print_condition(event, pn_link_remote_condition(pn_event_link(event)));
            pn_connection_close(pn_event_connection(event));
            break;

          default: break;
        }
    }
}

static void usage(const char *arg0) {
    fprintf(stderr, "Usage: %s [-a url] [-m message-count]\n", arg0);
    exit(1);
}

int main(int argc, char **argv) {
    /* Default values for application and connection. */
    app_data_t app = {0};
    app.message_count = 100;
    const char* urlstr = NULL;

    int opt;
    while((opt = getopt(argc, argv, "a:m:")) != -1) {
        switch(opt) {
          case 'a': urlstr = optarg; break;
          case 'm': app.message_count = atoi(optarg); break;
          default: usage(argv[0]); break;
        }
    }
    if (optind < argc)
        usage(argv[0]);

    snprintf(app.container_id, sizeof(app.container_id), "%s:%d", argv[0], getpid());

    /* Parse the URL or use default values */
    pn_url_t *url = urlstr ? pn_url_parse(urlstr) : NULL;
    const char *host = url ? pn_url_get_host(url) : NULL;
    const char *port = url ? pn_url_get_port(url) : NULL;
    if (!port) port = "amqp";
    strncpy(app.address, (url && pn_url_get_path(url)) ? pn_url_get_path(url) : "example", sizeof(app.address));

    /* Create the driver and connect */
    pn_driver_t driver;
    pn_driver_init(&driver);
    pn_driver_connection_t dc;
    pn_driver_connection_init(&driver, &dc);
    pn_driver_connect(&dc, host, port, NULL);
    if (url) pn_url_free(url);

    for (pn_driver_event_t e = pn_driver_wait(&driver);
         e.type != PN_DRIVER_INTERRUPT;
         e = pn_driver_wait(&driver))
    {
        switch (e.type) {
          case PN_DRIVER_CONNECTION_READY:
            dispatch(&app, pn_driver_engine(e.connection));
            pn_driver_watch(e.connection);
            break;
          case PN_DRIVER_CONNECTION_FINISHED:
            pn_driver_connection_final(e.connection);
            pn_driver_interrupt(&driver);
            break;
          default:
            break;
        }
    }
    pn_driver_final(&driver);
    return 0;
}
