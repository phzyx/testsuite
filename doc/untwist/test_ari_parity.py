#!/usr/bin/env python3
"""ARI WebSocket parity test for the Step 4 ``websockets`` cutover (design
02-design §6.2, parity contract §6.5).

``ari.py`` used ``autobahn.twisted`` for its ARI WebSocket client and server.
They now use the ``websockets`` library over the ``asterisk.aio`` reactor loop:
a high-level ``websockets.connect`` client (``AriClientFactory`` /
``AriClientProtocol``) and the shared sans-I/O ``ServerProtocol`` adapter
(``AriServerFactory`` / ``AriServerProtocol``) bound through
``reactor.listenTCP``.

The media WebSocket parity test (``test_ws_parity.py``) already covers the
low-level framing/fragmentation/sendFile mechanics of the shared adapter. This
test covers the ``ari.py`` classes specifically -- the JSON-envelope layer the
media test does not exercise:

  * Subprotocol negotiation: the client requests ``ari`` and the server's
    ``on_ws_connect`` selects it; the negotiated subprotocol is ``ari``.
  * The client's ``sendRequest`` produces a well-formed RESTRequest JSON
    envelope (type/method/uri/request_id + extra params) that the server
    decodes and delivers to ``on_ws_event``.
  * The server->client event direction: a JSON event sent by the server is
    parsed and delivered to the client's ``on_ws_event``.
  * Close callbacks (``on_ws_closed``) fire on both ends.

Run:  PYTHONPATH=lib/python .venv/bin/python doc/untwist/test_ari_parity.py
      (exit 0 = OK)
"""

import socket

from asterisk.aio import reactor
from asterisk.ari import AriClientFactory, AriServerFactory

results = {}


def _free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return port


# ---------------------------------------------------------------------------- #
# Server side
# ---------------------------------------------------------------------------- #
class _ServerReceiver(object):
    def __init__(self):
        self.protocol = None
        self.events = []
        self.closed = False

    def on_ws_connect(self, request):
        results['server_peer'] = request.peer
        return 'ari'                       # accept the ari subprotocol

    def on_ws_open(self, protocol):
        self.protocol = protocol

    def on_ws_event(self, event):
        # The client sends a RESTRequest envelope; record it, then push a JSON
        # event back down to the client and close.
        self.events.append(event)
        results['server_request'] = event
        self.protocol.sendMessage(
            b'{"type": "StasisStart", "application": "hello", '
            b'"channel": {"id": "1234"}}')
        self.protocol.sendClose(1000)

    def on_ws_closed(self, protocol):
        self.closed = True
        results['server_closed'] = True


# ---------------------------------------------------------------------------- #
# Client side
# ---------------------------------------------------------------------------- #
class _ClientReceiver(object):
    def __init__(self):
        self.protocol = None

    def on_ws_open(self, protocol):
        self.protocol = protocol
        results['client_subprotocol'] = protocol._connection.subprotocol
        # Fire a REST request across the socket; request_id is fixed so the
        # server-side round-trip can be asserted deterministically.
        results['client_request_id'] = protocol.sendRequest(
            'POST', 'channels/1234/answer',
            request_id='req-42', channelId='1234')

    def on_ws_event(self, event):
        results['client_event'] = event

    def on_ws_closed(self, protocol):
        results['client_closed'] = True
        reactor.stop()


def main():
    port = _free_port()

    server_rcv = _ServerReceiver()
    server_factory = AriServerFactory(
        server_rcv, "ws://127.0.0.1:%d/ari/events" % port,
        ['ari'], "localhost")
    # Bind BEFORE run() so the awaited startup guarantees the listener is up.
    reactor.listenTCP(port, server_factory, 10, "127.0.0.1")

    client_rcv = _ClientReceiver()
    client_factory = AriClientFactory(
        client_rcv, "127.0.0.1", "hello", ("user", "pass"),
        port=port, timeout_secs=10)

    reactor.callWhenRunning(client_factory.connect)
    reactor.callLater(15, reactor.stop)   # safety net
    reactor.run()

    # -- subprotocol -------------------------------------------------------- #
    assert results.get('client_subprotocol') == 'ari', \
        "subprotocol not negotiated: %r" % results.get('client_subprotocol')
    assert results.get('server_peer', '').startswith('tcp:'), \
        "server peer not reported: %r" % results.get('server_peer')

    # -- client sendRequest -> server on_ws_event --------------------------- #
    req = results.get('server_request')
    assert req is not None, "server never received the RESTRequest"
    assert req.get('type') == 'RESTRequest', \
        "envelope type wrong: %r" % req.get('type')
    assert req.get('method') == 'POST', "method wrong: %r" % req.get('method')
    assert req.get('uri') == 'channels/1234/answer', \
        "uri wrong: %r" % req.get('uri')
    assert req.get('request_id') == 'req-42', \
        "request_id not preserved: %r" % req.get('request_id')
    assert req.get('channelId') == '1234', \
        "extra param not preserved: %r" % req.get('channelId')
    assert results.get('client_request_id') == 'req-42', \
        "sendRequest did not return the request id: %r" \
        % results.get('client_request_id')

    # -- server -> client event direction ----------------------------------- #
    evt = results.get('client_event')
    assert evt is not None, "client never received the server event"
    assert evt.get('type') == 'StasisStart', \
        "client event type wrong: %r" % evt.get('type')
    assert evt.get('channel', {}).get('id') == '1234', \
        "client event payload garbled: %r" % evt

    # -- close callbacks ---------------------------------------------------- #
    assert results.get('server_closed') is True, "server on_ws_closed not fired"
    assert results.get('client_closed') is True, "client on_ws_closed not fired"

    print("  [ari subproto]  client<->server negotiated 'ari' OK")
    print("  [ari request]   RESTRequest envelope round-tripped to server OK")
    print("  [ari event]     server->client JSON event delivered OK")
    print("  [ari close]     on_ws_closed fired on both ends OK")
    print("ALL OK")


if __name__ == '__main__':
    print("asterisk ari (websockets) parity test")
    main()
