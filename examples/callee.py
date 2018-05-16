#!/usr/bin/env python

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

def wampclient_on_error(wampclient):
    # unsophisticated error handling, just close the session
    logging.info("wampclient_on_error ... calling close")
    wampclient.close()


def wampclient_on_close(wampclient):
    logging.info("*** session closed ***")
    event.set()


def wampclient_on_open(wampclient):
    logging.info("*** session open ***")
    event.set()


def rpc_invoked(client, req_id, *args, **kwargs):
    logging.info("rpc_invoked, args: {}, kwargs: {}".format(args, kwargs))
    client.rpc_yield(req_id, ["hello", "world"])


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

run_forever = True

while run_forever:

    event.clear()

    # create the client
    client = wamplite.WampClient(url, realm, authid, password,
                                 on_error_cb = wampclient_on_error,
                                 on_close_cb = wampclient_on_close,
                                 on_open_cb = wampclient_on_open)

    # add a single procedure
    client.add_rpc("hello", rpc_invoked)

    # start the client
    client.start()

    # wait for a session event, or timeout if nothing has happened (e.g. if
    # connection attempt is not responding for whatever reason)
    event.wait(timeout=30)

    # if session established, wait for the next event, which can only be a
    # session close due to the action of the server
    while client.is_open():
        event.clear()
        event.wait()

    logging.info("attempting reconnect after brief wait")
    del client
    time.sleep(3)
