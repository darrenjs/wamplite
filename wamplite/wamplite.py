# Light-weight wamp client, for python 2 & python 3

#
# Copyright (c) 2017 Darren Smith
#
# pywamp is free software; you can redistribute it and/or modify
# it under the terms of the MIT license. See LICENSE for details.
#

import logging
import base64
import hashlib
import hmac
import json
import ssl
import threading
import websocket
import traceback
import sys


##
## Python 2/3 compatibility helpers
##

def is_py2():
    return sys.version_info < (3,)


# for python 2/3 compat., add unicode() for python3
if not is_py2():
    def unicode(s, encoding):
        return str(s, encoding)


# for python 2/3 compat., generic method to convert str-like object to bytes
def to_bytes(s):
    if isinstance(s, str):
        return str.encode(s)
    else:
        return s


class PendingCall(object):

    def __init__(self, request_id, procedure, user_cb):
        self.request_id = request_id
        self.procedure = procedure
        self.user_cb = user_cb


class MsgType(object):
    HELLO = 1
    WELCOME = 2
    ABORT = 3
    CHALLENGE = 4
    AUTHENTICATE = 5
    GOODBYE = 6
    ERROR = 8
    PUBLISH = 16
    CALL = 48
    RESULT = 50
    REGISTER = 64
    REGISTERED = 65
    INVOCATION = 68
    YIELD = 70


class UserRpc:

    def __init__(self, uri, callback):
        self.uri = uri
        self.callback = callback
        self.regid = None


    def on_registered(self, reg_id):
        self.regid = reg_id


    def is_registered(self):
        return self.regid is not None


class WampClient:

    def close(self):
        if self.is_connected():
            self.ws.close()


    def is_closed(self):
        return self._is_closed


    def is_open(self):
        return self._is_open


    def __init__(self, url, realm, authid, password,
                 on_error_cb,
                 on_close_cb,
                 on_open_cb):
        self.server_url = url
        self.realm = realm
        self.authid = authid
        self.password = password
        self._on_error_cb = on_error_cb
        self._on_close_cb = on_close_cb
        self._on_open_cb = on_open_cb
        self._is_closed = False
        self.ws = websocket.WebSocketApp(
            url,
            on_message=self._on_message,
            on_open=self._on_open,
            on_error=self._on_error,
            on_close=self._on_close)
        self._is_open = False

        self.main_thread = threading.Thread(target=self._thread_main)

        # Set of procedures pending registration
        self.rpc_pending = {}

        # Set of calls, pending a response; indexed by request id.
        self.pending_calls = {}

        # Set of procedures successfully registered. Key is the registration id.
        self.registrations = {}  # dict[id, UserRpc]
        self.next_reqid = 1

        # Collection of user rpcs that will be registered at session open
        self.rpclist = {}


    def add_rpc(self, uri, callback):
        if uri in self.rpclist:
            raise ValueError("rpc '%s' already added" % uri)
        self.rpclist[uri] = UserRpc(uri, callback)


    def _get_req_id(self):
        reqid = self.next_reqid
        self.next_reqid += 1
        return reqid


    def _send_json(self, msg):
        assert isinstance(msg, list)
        data = json.dumps(msg)
        logging.debug("send: %s" % str(msg))
        self.ws.send(data)


    def is_connected(self):
        return self.ws.sock and self.ws.sock.connected


    def start(self):
        self.main_thread.daemon = True
        self.main_thread.start()


    def topic_publish(self, uri, arglist):
        assert isinstance(uri, str)
        reqid = self._get_req_id()
        msg = [MsgType.PUBLISH, reqid, {}, uri, arglist]
        self._send_json(msg)


    def rpc_yield(self, reqid, arglist=None, argdict=None):
        """Reply to an rpc invocation with a yield reply"""
        options = {}
        msg = [MsgType.YIELD, reqid, options]
        if arglist or argdict:
            msg.append(arglist)
            if argdict:
                msg.append(argdict)
        self._send_json(msg)


    def rpc_yield_error(self, reqid, error="", arglist=None, argdict=None):
        """Reply to an rpc invocation with an error reply"""
        msg = [MsgType.ERROR, MsgType.INVOCATION, reqid, {}, error]
        if arglist or argdict:
            msg.append(arglist)
            if argdict:
                msg.append(argdict)
        self._send_json(msg)


    def _on_close(self, ws):
        self._is_closed = True
        self._is_open = False
        logging.warn("connection closed")
        if self._on_close_cb is not None:
            self._on_close_cb(self)


    def _on_error(self, ws, error):
        logging.error("websocket error: %s" % str(error))
        if self._on_error_cb is not None:
            self._on_error_cb(self)


    def _on_open(self, ws):
        """Callback when socket is connected"""
        logging.info("socket connected")
        logging.info("sending hello for realm '%s', authid '%s'"
                     % (self.realm, self.authid))
        msg = [MsgType.HELLO, self.realm,
                {"authid": self.authid,
                 "authmethods": ["wampcra"], "roles": {}}]
        self._send_json(msg)


    def _thread_main(self):
        logging.info("connecting to server %s" % self.server_url)
        try:
            self.ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
        except Exception as e:
            logging.warn("exception from websocket: %s " % e.message)
        finally:
            logging.info("exiting websocket thread")


    def _register_rpcs(self):
        """Register our rpc collection with peer"""
        if not self._is_open:
            logging.warn("cannot register rpcs; session not open")
            return

        # py2/3 compat. iteration
        for uri in self.rpclist.keys():
            userrpc = self.rpclist[uri]
            logging.info("attempting to register procedure '%s'" % uri)
            reqid = self._get_req_id()
            self.rpc_pending[reqid] = userrpc
            msg = [MsgType.REGISTER, reqid, {}, uri]
            self._send_json(msg)


    def _handle_wamp_registered(self, msg):
        reqid = msg[1]
        regid = msg[2]
        userrpc = self.rpc_pending[reqid]
        userrpc.on_registered(regid)
        self.registrations[regid] = userrpc
        logging.info("procedure registered, '{}' registration_id {}".format(userrpc.uri, regid))


    def _handle_wamp_welcome(self, msg):
        logging.info("wamp session open")
        self._is_open = True
        self._register_rpcs()
        if self._on_open_cb is not None:
            self._on_open_cb(self)


    def _handle_wamp_invocation(self, msg):
        reqid = msg[1]
        regid = msg[2]
        userrpc = self.registrations[regid]
        logging.info("procedure invocation, '%s' request_id %s" %
                     (userrpc.uri, reqid))
        if len(msg) > 5:
            args = msg[4]
            kwargs = msg[5]
            userrpc.callback(self, reqid, *args, **kwargs)
        elif len(msg) > 4:
            args = msg[4]
            userrpc.callback(self, reqid, *args)
        else:
            userrpc.callback(self, reqid)


    def _handle_wamp_challenge(self, msg):
        assert isinstance(msg[2], object)
        logging.info("attempting to authenticate")

        salt = msg[2].get('salt')
        keylen = msg[2].get('keylen')
        iterations = msg[2].get('iterations')
        challenge = msg[2][u'challenge'].encode('utf8')

        response = WampClient._handle_challenge(challenge,
                                                self.password,
                                                salt,
                                                iterations,
                                                keylen)

        msg = [MsgType.AUTHENTICATE, response, {}]
        self._send_json(msg)


    # Handle a WAMP CRA challenge.  Optional salting parameters are accepted.
    @staticmethod
    def _handle_challenge(challenge,
                          key,  # this is the user password
                          salt=None,
                          iterations=None,
                          keylen=None):

        # if salting, use pbkdf2 to generate the derived key
        if (salt is not None) and (iterations is not None) and (keylen is not None):
            dk = hashlib.pbkdf2_hmac('sha256',
                                     to_bytes(key),
                                     to_bytes(salt),
                                     int(iterations), int(keylen))
            key = base64.b64encode(dk)

        # compute the challenge
        digest = hmac.new(key,
                          challenge,
                          digestmod=hashlib.sha256).digest()

        # note, in p3k mode, need to use the .decode() method
        digestb64 = base64.b64encode(digest).decode()
        return digestb64


    def _handle_wamp_abort(self, msg):
        assert isinstance(msg, list)
        logging.error("received wamp abort from peer, message: %s" % msg[2])
        self.close()


    def _handle_wamp_goodbye(self, msg):
        assert isinstance(msg, list)
        logging.error("received wamp goodbye from peer, message: %s" % msg[2])
        self.close()


    def _handle_wamp_error(self, msg):
        assert isinstance(msg, list)
        logging.error("received wamp error from peer, message: %s" % msg[4])
        # TODO: if error is due to wamp call, could instead dispatch on the user
        # callback funcion associated with the rpc request.
        if self._on_error_cb is not None:
            self._on_error_cb(self)


    def call(self, procedure, user_cb, *args, **kwargs):
        request_id = self._get_req_id()
        self.pending_calls[request_id] = PendingCall(request_id,
                                                     procedure,
                                                     user_cb)
        msg = [MsgType.CALL, request_id, {}, procedure, args, kwargs]
        self._send_json(msg)


    def _handle_wamp_result(self, msg):
        request_id = msg[1]
        if request_id in self.pending_calls:
            pendingcall = self.pending_calls[request_id]
            logging.info("procedure invocation, '%s' request_id %s" %
                         (pendingcall.procedure, request_id))
            if len(msg) > 4:
                args = msg[3]
                kwargs = msg[4]
                pendingcall.user_cb(request_id, *args, **kwargs)
            elif len(msg) > 3:
                args = msg[3]
                pendingcall.user_cb(request_id, *args)
            else:
                pendingcall.user_cb(request_id)
        else:
            logging.warn("skipping wamp result, no matching request for request id %s",
                         request_id)


    def _on_message(self, ws, message):
        assert isinstance(message, str)
        logging.debug("recv: %s" % str(message))
        try:
            j = json.loads(message)

            if j[0] == MsgType.CHALLENGE:
                self._handle_wamp_challenge(j)
            elif j[0] == MsgType.ABORT:
                self._handle_wamp_abort(j)
            elif j[0] == MsgType.GOODBYE:
                self._handle_wamp_goodbye(j)
            elif j[0] == MsgType.WELCOME:
                self._handle_wamp_welcome(j)
            elif j[0] == MsgType.REGISTERED:
                self._handle_wamp_registered(j)
            elif j[0] == MsgType.INVOCATION:
                self._handle_wamp_invocation(j)
            elif j[0] == MsgType.ERROR:
                self._handle_wamp_error(j)
            elif j[0] == MsgType.RESULT:
                self._handle_wamp_result(j)
            elif j[0] == 17:
                pass
            else:
                logging.error("unhandled WAMP message type: {}".format(j[0]))
        except Exception as e:
            logging.error("exception: {}: {}".format(type(e), e))
            traceback.print_exc()
