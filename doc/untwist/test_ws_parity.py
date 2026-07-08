#!/usr/bin/env python3
"""WebSocket parity test for the Step 4 ``websockets`` cutover (design
02-design §6.2, parity contract §6.5).

``media_websocket.py`` (and ``ari.py``) used ``autobahn.twisted`` for their
WebSocket client and server. They now use the ``websockets`` library over the
``asterisk.aio`` reactor loop: a high-level ``websockets.connect`` client and a
sans-I/O ``ServerProtocol`` adapter bound through ``current_runtime().listenTCP`` (so the
existing ``current_runtime().listenTCP(port, factory, ...)`` server call sites are
unchanged).

This test stands the real server factory up on the reactor via
``current_runtime().listenTCP`` and drives the real client factory against it, asserting
the §6.5 WebSocket contract points:

  * Subprotocol negotiation: the client requests ``media`` and the server's
    ``on_ws_connect`` selects it; the negotiated subprotocol is ``media``.
  * Binary vs text framing is preserved end to end.
  * Outgoing fragmentation (``setProtocolOptions(autoFragmentSize=...)``) splits
    a large message into multiple frames on the wire, and the receiver
    reassembles it into the original bytes.
  * ``sendFile`` streams a file (run in a worker thread via
    ``current_runtime().callInThread``) with the START/STOP media sentinels, and every
    byte arrives intact and in order.
  * Close callbacks (``on_ws_closed``) fire on both ends.

Run:  PYTHONPATH=lib/python .venv/bin/python doc/untwist/test_ws_parity.py
      (exit 0 = OK)
"""

import os
import socket
import tempfile

from asterisk.aio.runtime import current_runtime
from asterisk.media_websocket import (MediaWebSocketClientFactory,
                                      MediaWebSocketServerFactory)

results = {}

# A payload larger than the 500-byte fragment size so the client fragments it.
BIG = bytes((i % 256) for i in range(1300))
FRAGMENT = 500


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
        self.messages = []
        self.file_bytes = bytearray()
        self.buffering = False
        self.closed = False

    def on_ws_connect(self, request):
        results['server_peer'] = request.peer
        return 'media'          # accept the media subprotocol

    def on_ws_open(self, protocol):
        self.protocol = protocol

    def on_message(self, message, binary):
        self.messages.append((message, binary))
        if not binary:
            text = message.decode('utf-8')
            if text == 'START_MEDIA_BUFFERING':
                self.buffering = True
            elif text == 'STOP_MEDIA_BUFFERING':
                self.buffering = False
                # Record how many frames the fragmented BIG payload became.
                results['server_frames'] = self.protocol.frames_received
                results['server_file'] = bytes(self.file_bytes)
                # Echo a text + a binary back, then close (exercises the
                # server->client direction and the close handshake).
                self.protocol.sendMessage(b'ACK', isBinary=False)
                self.protocol.sendMessage(bytes(range(200)), isBinary=True)
                self.protocol.sendClose(1000)
        elif self.buffering:
            self.file_bytes.extend(message)

    def on_ws_closed(self, protocol):
        self.closed = True
        results['server_closed'] = True


# ---------------------------------------------------------------------------- #
# Client side
# ---------------------------------------------------------------------------- #
class _ClientReceiver(object):
    def __init__(self, ulaw_path):
        self.protocol = None
        self.ulaw_path = ulaw_path
        self.acked = False

    def on_ws_open(self, protocol):
        self.protocol = protocol
        results['client_subprotocol'] = protocol._connection.subprotocol
        # 1) a plain text frame, 2) a single big binary that the client's
        # autoFragmentSize splits into fragments, then 3) stream a file from a
        # worker thread.
        protocol.sendMessage(b'HELLO', isBinary=False)
        protocol.sendMessage(BIG, isBinary=True)
        current_runtime().callInThread(protocol.sendFile, self.ulaw_path)

    def on_message(self, message, binary):
        if not binary and message.decode('utf-8') == 'ACK':
            results['client_ack'] = True
        elif binary:
            results['client_bin'] = message

    def on_ws_closed(self, protocol):
        results['client_closed'] = True
        current_runtime().stop()


def main():
    tmp = tempfile.mkdtemp()
    ulaw = os.path.join(tmp, 'test.ulaw')
    file_payload = bytes((i * 7) % 256 for i in range(2500))
    with open(ulaw, 'wb') as handle:
        handle.write(file_payload)

    port = _free_port()

    server_rcv = _ServerReceiver()
    server_factory = MediaWebSocketServerFactory(
        server_rcv, "ws://127.0.0.1:%d/media" % port, "localhost",
        protocols=['media'])
    # Bind BEFORE run() so the awaited startup guarantees the listener is up.
    current_runtime().listenTCP(port, server_factory, 10, "127.0.0.1")

    client_rcv = _ClientReceiver(ulaw)
    client_factory = MediaWebSocketClientFactory(
        client_rcv, "ws://127.0.0.1:%d/media" % port,
        protocol="media", timeout_secs=10)
    client_factory.setProtocolOptions(tcpNoDelay=True,
                                      autoFragmentSize=FRAGMENT)

    current_runtime().callWhenRunning(client_factory.connect)
    current_runtime().callLater(15, current_runtime().stop)   # safety net
    current_runtime().run()

    # -- subprotocol -------------------------------------------------------- #
    assert results.get('client_subprotocol') == 'media', \
        "subprotocol not negotiated: %r" % results.get('client_subprotocol')
    assert results.get('server_peer', '').startswith('tcp:'), \
        "server peer not reported: %r" % results.get('server_peer')

    # -- server received text + fragmented binary --------------------------- #
    msgs = server_rcv.messages
    assert (b'HELLO', False) in msgs, "text frame not received as text: %r" % msgs
    assert (BIG, True) in msgs, "fragmented binary not reassembled intact"
    # 1300 bytes at a 500-byte fragment size => 3 frames on the wire.
    assert results.get('server_frames', 0) >= 3, \
        "big message was not fragmented (frames=%r)" \
        % results.get('server_frames')

    # -- sendFile round-trip ------------------------------------------------ #
    assert results.get('server_file') == file_payload, \
        "streamed file bytes did not arrive intact/in order"

    # -- server->client direction ------------------------------------------ #
    assert results.get('client_ack') is True, "client missed ACK text frame"
    assert results.get('client_bin') == bytes(range(200)), \
        "client missed/garbled server binary frame"

    # -- close callbacks ---------------------------------------------------- #
    assert results.get('server_closed') is True, "server on_ws_closed not fired"
    assert results.get('client_closed') is True, "client on_ws_closed not fired"

    print("  [ws subproto]  client<->server negotiated 'media' OK")
    print("  [ws framing]   text vs binary preserved both directions OK")
    print("  [ws fragment]  1300B split into %d frames, reassembled OK"
          % results['server_frames'])
    print("  [ws sendFile]  2500B streamed from worker thread intact OK")
    print("  [ws close]     on_ws_closed fired on both ends OK")
    print("ALL OK")


if __name__ == '__main__':
    print("asterisk media_websocket / ari (websockets) parity test")
    main()
