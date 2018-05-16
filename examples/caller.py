#!/usr/bin/env python

#
# wamplite is free software; you can redistribute it and/or modify
# it under the terms of the MIT license. See LICENSE for details.
#

import wamplite

import logging
import sys
import threading
import time
import websocket

# Connection & authentication details. Use ws:// for websocket, or wss:// for
# secure websocket.
url = "ws://localhost:55555"
realm = "default_realm"
authid = "peter"
password = "secret2"

event = threading.Event()
rpc_completed_ok=False

def wampclient_on_error(wampclient):
    # Unsophisticated error handling, just close the session
    logging.info("wampclient_on_error ... calling close")
    wampclient.close()


def wampclient_on_close(wampclient):
    logging.info("*** session closed ***")
    event.set()


def wampclient_on_open(wampclient):
    logging.info("*** session open ***")
    wampclient.call("hello", rpc_response,
                     "listarg1", "listarg2", dictarg1="v1", dictarg2="v2")


def rpc_response(wampclient, *args, **kwargs):
    logging.info("rpc_response, args: {}, kwargs: {}".format(args, kwargs))
    global rpc_completed_ok
    rpc_completed_ok = True
    event.set()


def init_logging(use_debug):
    websocket.enableTrace(use_debug)
    logger = logging.getLogger()
    if use_debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    ch = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)


init_logging(use_debug=False)

# attempts to make before giving up
attempts = 5

while True:

    event.clear()

    # create the wamp client
    client = wamplite.WampClient(url, realm, authid, password,
                                 on_error_cb = wampclient_on_error,
                                 on_close_cb = wampclient_on_close,
                                 on_open_cb = wampclient_on_open)
    client.start()

    # wait for IO thread to response
    event.wait(timeout=30)

    if rpc_completed_ok:
        break
    else:
        # if rpc failed, clean up and retry
        client.close()
        del client
        attempts -= 1
        if attempts == 0:
            logging.error("failed to call RPC")
            exit(1)
        else:
            logging.info("remaining attempts: %d" % attempts)
            time.sleep(3)
