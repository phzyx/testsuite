"""
Copyright (C) 2025, Sangoma Technologies Corporation
George T Joseph <gjoseph@sangoma.com>

This program is free software, distributed under the terms of
the GNU General Public License Version 2.

Media WebSocket client/server support is implemented on the ``websockets``
library over the ``asterisk.aio`` reactor loop.

  * The *client* uses the high-level ``websockets.connect`` coroutine; outgoing
    fragmentation (``autoFragmentSize``) is reproduced by sending a message as an
    iterable of chunks.
  * The *server* is bound through ``reactor.listenTCP`` and driven as a raw byte
    stream. The WebSocket handshake and framing are performed with the
    ``websockets`` sans-I/O
    ``ServerProtocol`` (``receive_data`` -> ``events_received`` ->
    ``data_to_send``), which lets us keep the reactor's TCP listener while
    reproducing subprotocol negotiation, binary/text framing, fragment
    reassembly and the close handshake.

The receiver-facing surface (``on_ws_connect``/``on_ws_open``/``on_message``/
``on_ws_closed`` and protocol ``sendMessage``/``sendClose``/``sendFile``) is
preserved verbatim.
"""

import asyncio
import datetime
import io
import logging

import websockets
from websockets.frames import Opcode
from websockets.http11 import Request
from websockets.server import ServerProtocol

from asterisk.aio.runtime import current_runtime

LOGGER = logging.getLogger(__name__)

_FILE_BUFF_SIZE = 1000


def _on_loop(loop):
    """Return True if the caller is already running on ``loop``'s thread."""
    try:
        return asyncio.get_running_loop() is loop
    except RuntimeError:
        return False


class MediaWebSocketMixin:
    """Receiver-dispatch + ``sendFile`` behaviour shared by client and server.

    Subclasses provide ``sendMessage(payload, isBinary)`` and ``sendClose``.
    """

    def has_function(self, func):
        return hasattr(self.receiver, func) \
            and callable(getattr(self.receiver, func))

    def onConnect(self, request):
        LOGGER.debug("New WebSocket Connected")
        if self.has_function("on_ws_connect"):
            return self.receiver.on_ws_connect(request)
        return None

    def onOpen(self):
        LOGGER.debug("WebSocket Open")
        if self.has_function("on_ws_open"):
            self.receiver.on_ws_open(self)

    def onClose(self, wasClean, code, reason):
        LOGGER.debug(f"WebSocket closed({wasClean}, {code}, {reason})")
        if self.has_function("on_ws_closed"):
            self.receiver.on_ws_closed(self)

    def onMessage(self, msg, binary):
        if self.has_function("on_message"):
            self.receiver.on_message(msg, binary)

    def sendFile(self, filename, sent_buffer=None):
        """Stream a file over the socket as binary media (blocking; intended to
        run under ``reactor.callInThread``).

        Sends the ``START_MEDIA_BUFFERING``/``STOP_MEDIA_BUFFERING`` text
        sentinels around the binary payload. Each ``sendMessage`` marshals the
        actual send onto the reactor loop.
        """
        f = io.open(filename, "rb", buffering=0)
        LOGGER.info(f"Playing '{filename}'")
        self.sendMessage(b"START_MEDIA_BUFFERING", isBinary=False)
        while True:
            buff = f.read(_FILE_BUFF_SIZE)
            if buff is None or len(buff) <= 0:
                break
            self.sendMessage(buff, isBinary=True)
            if sent_buffer is not None:
                # Save so the test can compare against what was echoed back.
                sent_buffer.write(buff)
        f.close()
        self.sendMessage(b"STOP_MEDIA_BUFFERING", isBinary=False)
        LOGGER.info(f"Stopping '{filename}'")


def _chunk(data, size):
    """Yield ``size``-long slices of ``data`` (bytes or str)."""
    for i in range(0, len(data), size):
        yield data[i:i + size]


# ---------------------------------------------------------------------------- #
# Client (high-level websockets.connect)
# ---------------------------------------------------------------------------- #
class MediaWebSocketClientFactory:
    """Factory that opens a media WebSocket client on the reactor loop."""

    def __init__(self, receiver, uri, protocol="media", timeout_secs=60):
        """Constructor

        :param receiver The object that will receive events from the protocol
        :param uri The websocket server URI
        :param protocol The subprotocol to request
        :param timeout_secs Maximum time to keep retrying the connection
        """
        LOGGER.info(f"WebSocketClientFactory(uri={uri})")
        self.receiver = receiver
        self.uri = uri
        self.protocol = protocol
        self.timeout_secs = timeout_secs
        self.auto_fragment_size = 0
        self.attempts = 0
        self.start = None

    def setProtocolOptions(self, **kwargs):
        """Accept the protocol options the tests use.

        Only ``autoFragmentSize`` affects behaviour here (outgoing messages
        larger than it are sent as fragmented frames); the rest (``tcpNoDelay``,
        etc.) are transport tweaks that the websockets client handles itself and
        are accepted and ignored.
        """
        if 'autoFragmentSize' in kwargs and kwargs['autoFragmentSize']:
            self.auto_fragment_size = int(kwargs['autoFragmentSize'])

    def buildProtocol(self):
        return MediaWebSocketClientProtocol(self.receiver, self)

    def connect(self):
        self.reconnect()

    def reconnect(self):
        self.attempts += 1
        LOGGER.debug(f"WebSocket attempt #{self.attempts}")
        if not self.start:
            self.start = datetime.datetime.now()
        runtime = (datetime.datetime.now() - self.start).seconds
        if runtime >= self.timeout_secs:
            LOGGER.error(f"  Giving up after {self.timeout_secs} seconds")
            raise Exception(
                f"Failed to connect after {self.timeout_secs} seconds")
        current_runtime().create_task(self._connect_async())

    async def _connect_async(self):
        try:
            connection = await websockets.connect(
                self.uri, subprotocols=[self.protocol])
        except Exception as exc:
            LOGGER.debug("Connection failed (%s); retrying in 1s", exc)
            current_runtime().callLater(1, self.reconnect)
            return
        proto = self.buildProtocol()
        proto._attach(connection)


class MediaWebSocketClientProtocol(MediaWebSocketMixin):
    """Media WebSocket client protocol backed by a ``websockets`` connection."""

    def __init__(self, receiver, factory):
        self.receiver = receiver
        self.factory = factory
        self._loop = current_runtime()._ensure_loop()
        self._auto_fragment_size = factory.auto_fragment_size
        self._connection = None

    def _attach(self, connection):
        """Bind an open websockets connection and start reading."""
        self._connection = connection
        self.onOpen()
        current_runtime().create_task(self._reader())

    async def _reader(self):
        try:
            async for message in self._connection:
                binary = isinstance(message, (bytes, bytearray))
                payload = message if binary else message.encode('utf-8')
                self.onMessage(payload, binary)
        except websockets.ConnectionClosed:
            pass
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.debug("Media client reader error: %s", exc)
        finally:
            self.onClose(True, 1000, "connection closed")

    def sendMessage(self, payload, isBinary=False):
        """Send a text (``isBinary=False``) or binary frame.

        ``payload`` is bytes. Text frames are decoded to ``str`` so
        ``websockets`` emits a TEXT frame; when ``autoFragmentSize`` is set and
        the payload exceeds it, the message is sent as an iterable of chunks
        (fragmented frames).
        """
        data = bytes(payload) if isBinary else \
            (payload.decode('utf-8') if isinstance(payload, (bytes, bytearray))
             else payload)
        if self._auto_fragment_size and len(data) > self._auto_fragment_size:
            outgoing = _chunk(data, self._auto_fragment_size)
        else:
            outgoing = data
        self._dispatch(self._connection.send(outgoing))

    def sendClose(self, code=1000, reason=""):
        self._dispatch(self._connection.close(code, reason))

    dropConnection = sendClose

    def _dispatch(self, coro):
        """Run ``coro`` on the reactor loop.

        If called from the loop thread (e.g. a message handler), schedule it
        without blocking. If called from a worker thread (``sendFile`` under
        ``callInThread``), block on the result so file streaming honours
        websocket write backpressure and preserves ordering.
        """
        if _on_loop(self._loop):
            current_runtime().create_task(coro)
        else:
            try:
                asyncio.run_coroutine_threadsafe(coro, self._loop).result()
            except (asyncio.CancelledError, websockets.ConnectionClosed):
                # The connection is closing (e.g. reactor teardown raced an
                # in-flight sendFile chunk); stop streaming quietly.
                pass


# ---------------------------------------------------------------------------- #
# Server (sans-I/O ServerProtocol driven by the runtime TCP listener)
# ---------------------------------------------------------------------------- #
class _ConnectRequest(object):
    """Request object passed to ``on_ws_connect`` with peer, path, and headers."""

    def __init__(self, peer, request):
        self.peer = peer
        self.path = request.path
        self.headers = request.headers


class MediaWebSocketServerFactory:
    """Factory bound via ``reactor.listenTCP`` that builds media WS servers."""

    def __init__(self, receiver, uri, server_name, protocols=['media']):
        """Constructor

        :param receiver The object that will receive events from the protocol
        :param uri URI to be served (informational)
        :param server_name Server name for the HTTP response (informational)
        :param protocols List of acceptable subprotocols
        """
        self.receiver = receiver
        self.uri = uri
        self.server_name = server_name
        self.protocols = list(protocols) if protocols else []
        self.auto_fragment_size = 0
        self.attempts = 0
        self.start = None

    def setProtocolOptions(self, **kwargs):
        if 'autoFragmentSize' in kwargs and kwargs['autoFragmentSize']:
            self.auto_fragment_size = int(kwargs['autoFragmentSize'])

    def buildProtocol(self, addr):
        return MediaWebSocketServerProtocol(self.receiver, self)


class _SansIOServerProtocol(object):
    """Byte-stream adapter driving a websockets ``ServerProtocol``.

    ``reactor.listenTCP`` hands us a raw byte stream via ``makeConnection``/
    ``dataReceived``/``connectionLost``. We feed those bytes into the sans-I/O
    state machine, perform the handshake (honouring the subprotocol the
    receiver's ``on_ws_connect`` selects), reassemble fragmented data frames,
    and surface complete messages/opens/closes to the subclass.
    Subclasses implement ``_deliver_message(data, binary)``, ``_notify_open`` and
    ``_notify_close``.
    """

    def __init__(self, receiver, factory):
        self.receiver = receiver
        self.factory = factory
        self._loop = current_runtime()._ensure_loop()
        self._auto_fragment_size = factory.auto_fragment_size
        self.transport = None
        self.peer = None
        self._sansio = ServerProtocol(
            subprotocols=(factory.protocols or None))
        self._opened = False
        self._recv_opcode = None
        self._recv_buf = bytearray()
        # Count of inbound data frames (TEXT/BINARY/CONT). Lets tests confirm a
        # fragmented message arrived as multiple frames before reassembly.
        self.frames_received = 0

    # -- byte-stream protocol surface ------------------------------------ #
    def makeConnection(self, transport):
        self.transport = transport
        try:
            host, port = transport.getPeer()[:2]
            self.peer = "tcp:%s:%d" % (host, port)
        except Exception:
            self.peer = "tcp:unknown"

    def dataReceived(self, data):
        self._sansio.receive_data(data)
        self._flush()
        for event in self._sansio.events_received():
            self._handle_event(event)
        self._flush()

    def connectionLost(self, reason):
        if self._opened:
            self._opened = False
            self._notify_close(True, 1006, "connection lost")

    # -- event handling -------------------------------------------------- #
    def _handle_event(self, event):
        if isinstance(event, Request):
            # Let the receiver choose the subprotocol; honour it by making it the
            # server's preferred offer (websockets' default selection picks the
            # first server subprotocol the client also offered).
            chosen = self._on_connect(event)
            if chosen:
                self._sansio.available_subprotocols = [chosen]
            response = self._sansio.accept(event)
            self._sansio.send_response(response)
            self._flush()
            if self._sansio.state.name == 'OPEN':
                self._opened = True
                self._notify_open()
            return
        # Data / control frame.
        opcode = event.opcode
        if opcode in (Opcode.TEXT, Opcode.BINARY):
            self.frames_received += 1
            self._recv_opcode = opcode
            self._recv_buf = bytearray(event.data)
            if event.fin:
                self._deliver()
        elif opcode == Opcode.CONT:
            self.frames_received += 1
            self._recv_buf.extend(event.data)
            if event.fin:
                self._deliver()
        elif opcode == Opcode.CLOSE:
            if self._opened:
                self._opened = False
                self._notify_close(True,
                                   self._sansio.close_code or 1000,
                                   self._sansio.close_reason or "")
            if self.transport is not None:
                self.transport.loseConnection()

    def _deliver(self):
        data = bytes(self._recv_buf)
        binary = (self._recv_opcode == Opcode.BINARY)
        self._recv_buf = bytearray()
        self._deliver_message(data, binary)

    def _flush(self):
        if self.transport is None:
            return
        for chunk in self._sansio.data_to_send():
            if chunk == b'':
                # sans-I/O sentinel: half-close the TCP connection.
                self.transport.loseConnection()
            else:
                self.transport.write(chunk)

    # -- outgoing -------------------------------------------------------- #
    def sendMessage(self, payload, isBinary=False):
        self._run_on_loop(self._send_now, bytes(payload), isBinary)

    def _send_now(self, payload, is_binary):
        size = self._auto_fragment_size
        if size and len(payload) > size:
            chunks = list(_chunk(payload, size))
            for index, chunk in enumerate(chunks):
                fin = index == len(chunks) - 1
                if index == 0:
                    if is_binary:
                        self._sansio.send_binary(chunk, fin=fin)
                    else:
                        self._sansio.send_text(chunk, fin=fin)
                else:
                    self._sansio.send_continuation(chunk, fin=fin)
        elif is_binary:
            self._sansio.send_binary(payload)
        else:
            self._sansio.send_text(payload)
        self._flush()

    def sendClose(self, code=1000, reason=""):
        self._run_on_loop(self._close_now, code, reason)

    dropConnection = sendClose

    def _close_now(self, code, reason):
        self._sansio.send_close(code, reason)
        self._flush()

    def _run_on_loop(self, fn, *args):
        if _on_loop(self._loop):
            fn(*args)
        else:
            self._loop.call_soon_threadsafe(fn, *args)

    # -- subclass hooks -------------------------------------------------- #
    def _on_connect(self, request):
        raise NotImplementedError

    def _notify_open(self):
        raise NotImplementedError

    def _notify_close(self, was_clean, code, reason):
        raise NotImplementedError

    def _deliver_message(self, data, binary):
        raise NotImplementedError


class MediaWebSocketServerProtocol(MediaWebSocketMixin, _SansIOServerProtocol):
    """Media WebSocket server protocol."""

    def __init__(self, receiver, factory):
        _SansIOServerProtocol.__init__(self, receiver, factory)

    def _on_connect(self, request):
        return self.onConnect(_ConnectRequest(self.peer, request))

    def _notify_open(self):
        self.onOpen()

    def _notify_close(self, was_clean, code, reason):
        self.onClose(was_clean, code, reason)

    def _deliver_message(self, data, binary):
        self.onMessage(data, binary)
