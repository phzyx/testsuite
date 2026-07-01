"""asyncio replacement for the twisted.internet ``reactor``.

This module provides a single ``reactor`` object exposing the subset of the
Twisted reactor API the Asterisk test suite drives:

  * lifecycle: ``run()`` / ``stop()`` / ``running``
  * scheduling: ``callLater()`` (returning a cancellable ``_DelayedCall``),
    ``callWhenRunning()``, ``callFromThread()``, ``callInThread()``
  * networking: ``listenUDP()``, ``listenTCP()``, ``connectTCP()``
  * processes: ``spawnProcess()``

It is implemented on top of a single ``asyncio`` event loop. ``run()`` owns the
loop and blocks until ``stop()`` is called, mirroring Twisted's blocking reactor.

Two correctness properties added in response to review findings 6 and 7:

  * Awaited listener startup. ``listenUDP``/``listenTCP``/``connectTCP``/
    ``spawnProcess`` calls made *before* ``run()`` are bound during an awaited
    startup phase; a bind failure aborts startup and propagates out of ``run()``
    rather than being silently swallowed by a background task. Calls made while
    running are scheduled as tracked tasks, and a fatal bind error stops the
    reactor with the error recorded.
  * Resource-owned shutdown. The reactor tracks delayed calls, endpoint tasks,
    listening ports, connectors, and child-process transports, and performs an
    ordered asynchronous teardown in ``stop()``/``run()`` so nothing leaks
    between self-tests.

IMPORTANT (design doc Section 3.2 / Section 14): this reactor object is
*transitional* scaffolding. It exists only to make the Twisted-to-asyncio cutover
a mechanical import swap. The modernization end state replaces reactor.run()/stop()
with ``asyncio.run()`` and direct loop usage, and deletes this module. Do not build
new functionality on top of it.
"""

import asyncio

from .defer import Deferred
from .failure import Failure


class ReactorNotRunning(Exception):
    """Raised by stop() when the reactor is not running (twisted parity)."""


class ReactorAlreadyRunning(Exception):
    """Raised by run() when the reactor is already running (twisted parity)."""


class AlreadyCalled(Exception):
    """Raised by _DelayedCall.cancel() when the call already fired."""


class AlreadyCancelled(Exception):
    """Raised by _DelayedCall.cancel() when already cancelled."""


def _get_loop():
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.get_event_loop_policy().get_event_loop()


# ---------------------------------------------------------------------------- #
# Delayed calls
# ---------------------------------------------------------------------------- #
class _DelayedCall(object):
    """Cancellable scheduled call (twisted.internet.base.DelayedCall).

    Wraps an asyncio ``TimerHandle``. Supports ``cancel()`` and ``active()``;
    ``reset()``/``delay()`` reschedule relative to now. Deregisters itself from
    the reactor's timer registry when it fires or is cancelled.
    """

    def __init__(self, reactor, delay, fn, args, kw):
        self._reactor = reactor
        self._delay = delay
        self._fn = fn
        self._args = args
        self._kw = kw
        self._cancelled = False
        self._called = False
        self._handle = reactor._loop.call_later(delay, self._fire)

    def _fire(self):
        self._called = True
        self._reactor._delayed_calls.discard(self)
        self._fn(*self._args, **self._kw)

    def active(self):
        """Return True if the call has neither fired nor been cancelled."""
        return not (self._cancelled or self._called)

    def cancel(self):
        """Cancel the pending call."""
        if self._called:
            raise AlreadyCalled()
        if self._cancelled:
            raise AlreadyCancelled()
        self._cancelled = True
        self._handle.cancel()
        self._reactor._delayed_calls.discard(self)

    def reset(self, delay):
        """Reschedule to fire ``delay`` seconds from now."""
        if not self.active():
            raise AlreadyCalled()
        self._handle.cancel()
        self._delay = delay
        self._handle = self._reactor._loop.call_later(delay, self._fire)

    def delay(self, seconds_later):
        """Push the firing time out by ``seconds_later`` seconds."""
        self.reset(self._delay + seconds_later)


# ---------------------------------------------------------------------------- #
# Listening / connecting handles
# ---------------------------------------------------------------------------- #
class _Port(object):
    """Handle for a listening UDP or TCP endpoint (twisted IListeningPort)."""

    def __init__(self):
        self._transport = None
        self._server = None
        self._closed = False

    def _set_transport(self, transport):
        if self._closed and transport is not None:
            transport.close()
            return
        self._transport = transport

    def _set_server(self, server):
        if self._closed and server is not None:
            server.close()
            return
        self._server = server

    def stopListening(self):
        """Stop listening and close the endpoint."""
        self._closed = True
        if self._transport is not None:
            self._transport.close()
            self._transport = None
        if self._server is not None:
            self._server.close()
            self._server = None


class _Connector(object):
    """Handle for an outgoing TCP connection (twisted IConnector).

    Supports reconnection: when an established connection is lost,
    ``_TwistedProtocolAdapter.connection_lost`` notifies the factory via
    ``clientConnectionLost``; a ReconnectingClientFactory's ``retry`` then calls
    ``connector.connect()`` to start a fresh attempt. Honors the original
    ``timeout`` and ``bindAddress`` on every attempt.
    """

    def __init__(self, reactor, host, port, factory, timeout, bindAddress):
        self._reactor = reactor
        self.host = host
        self.port = port
        self._factory = factory
        self._timeout = timeout
        self._bindAddress = bindAddress
        self._transport = None
        self._stopped = False

    def connect(self):
        """Begin (or retry) the connection attempt."""
        if self._stopped:
            return
        self._transport = None
        factory = self._factory
        if hasattr(factory, 'startedConnecting'):
            factory.startedConnecting(self)
        loop = self._reactor._ensure_loop()
        local_addr = self._bindAddress if self._bindAddress else None
        coro = loop.create_connection(
            lambda: _TwistedProtocolAdapter(factory, self),
            self.host, self.port, local_addr=local_addr)
        if self._timeout:
            coro = asyncio.wait_for(coro, self._timeout)

        def apply(result):
            transport, _proto = result
            self._transport = transport

        def on_error(exc):
            # A connection failure is not fatal to the reactor: hand it to the
            # factory (starpy reconnect logic depends on this) instead of
            # aborting the run. A wait_for timeout arrives as TimeoutError.
            if self._stopped:
                return
            if hasattr(factory, 'clientConnectionFailed'):
                factory.clientConnectionFailed(self, Failure(exc))

        self._reactor._register_bind(
            coro, apply, on_error,
            'connectTCP:%s:%d' % (self.host, self.port))

    def stopConnecting(self):
        self.disconnect()

    def disconnect(self):
        # Mark stopped first so an in-flight connection_lost does not trigger a
        # reconnect (e.g. during reactor shutdown).
        self._stopped = True
        factory = self._factory
        if hasattr(factory, 'stopTrying'):
            factory.stopTrying()
        if self._transport is not None:
            self._transport.close()
            self._transport = None


# ---------------------------------------------------------------------------- #
# Twisted factory/protocol -> asyncio.Protocol adapter (for listenTCP/connectTCP)
# ---------------------------------------------------------------------------- #
class _TwistedProtocolAdapter(asyncio.Protocol):
    """Drive a Twisted-style protocol from asyncio.Protocol callbacks.

    The wrapped protocol is produced by ``factory.buildProtocol(addr)`` and is
    expected to expose ``makeConnection(transport)``/``dataReceived(data)``/
    ``connectionLost(reason)`` (the twisted.internet.protocol.Protocol surface).
    """

    def __init__(self, factory, connector=None):
        self._factory = factory
        self._connector = connector
        self._proto = None

    def connection_made(self, transport):
        peer = transport.get_extra_info('peername')
        self._proto = self._factory.buildProtocol(peer)
        adapter = _TCPTransportAdapter(transport)
        if hasattr(self._proto, 'makeConnection'):
            self._proto.makeConnection(adapter)
        else:
            self._proto.transport = adapter
            if hasattr(self._proto, 'connectionMade'):
                self._proto.connectionMade()

    def data_received(self, data):
        if self._proto is not None:
            self._proto.dataReceived(data)

    def connection_lost(self, exc):
        from .protocols import ConnectionDone
        reason = Failure(exc) if exc is not None else Failure(ConnectionDone())
        if self._proto is not None and hasattr(self._proto, 'connectionLost'):
            self._proto.connectionLost(reason)
        # For *client* connections (connectTCP), an established-then-lost
        # connection must notify the factory so a ReconnectingClientFactory can
        # retry. Server connections (listenTCP) have no connector and Twisted
        # server factories are not given clientConnectionLost. (review blocker 1)
        connector = self._connector
        if connector is not None:
            connector._transport = None
            if not connector._stopped and \
                    hasattr(self._factory, 'clientConnectionLost'):
                self._factory.clientConnectionLost(connector, reason)


class _TCPTransportAdapter(object):
    """Expose the twisted ITransport surface used by protocols over TCP."""

    def __init__(self, transport):
        self._transport = transport

    def write(self, data):
        self._transport.write(data)

    def writeSequence(self, seq):
        self._transport.writelines(seq)

    def loseConnection(self):
        self._transport.close()

    def getPeer(self):
        return self._transport.get_extra_info('peername')

    def getHost(self):
        return self._transport.get_extra_info('sockname')

    def __getattr__(self, name):
        return getattr(self._transport, name)


# ---------------------------------------------------------------------------- #
# The reactor
# ---------------------------------------------------------------------------- #
class _Reactor(object):
    """asyncio-backed stand-in for twisted.internet.reactor."""

    def __init__(self):
        self.running = False
        self._loop = None
        self._when_running = []      # callWhenRunning queue: (fn, args, kw)
        self._pending_binds = []     # pre-run binds: (coro, apply, on_error, label)
        self._stop_future = None
        self._failure = None         # first fatal mid-run error, re-raised by run()
        # Resource registries for ordered shutdown.
        self._delayed_calls = set()
        self._tasks = set()
        self._ports = []
        self._connectors = []
        self._process_transports = []

    # -- loop access ------------------------------------------------------ #
    def _ensure_loop(self):
        if self._loop is None:
            self._loop = _get_loop()
        return self._loop

    # -- lifecycle -------------------------------------------------------- #
    def run(self, installSignalHandlers=True):
        """Run the event loop until stop() is called (blocks).

        Binds any listeners registered before run() in an awaited startup phase;
        a bind failure aborts startup and is raised. After the loop ends, performs
        an ordered shutdown and re-raises any fatal mid-run error.
        """
        if self.running:
            raise ReactorAlreadyRunning()
        loop = self._ensure_loop()
        if loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop
        self.running = True
        self._failure = None
        self._stop_future = loop.create_future()

        # 1. Awaited startup: bind pre-run listeners, surfacing failures.
        pending = self._pending_binds
        self._pending_binds = []
        for coro, apply, on_error, label in pending:
            try:
                result = loop.run_until_complete(coro)
            except Exception as exc:
                if on_error is not None:
                    on_error(exc)
                    continue
                self.running = False
                self._failure = exc
                loop.run_until_complete(self._shutdown())
                self._stop_future = None
                raise
            apply(result)

        # 2. Flush callWhenRunning once the loop is turning.
        queued = self._when_running
        self._when_running = []
        for fn, args, kw in queued:
            loop.call_soon(fn, *args, **kw)

        # 3. Run until stop(), then tear down owned resources.
        try:
            loop.run_until_complete(self._stop_future)
        finally:
            self.running = False
            loop.run_until_complete(self._shutdown())
            self._stop_future = None

        if self._failure is not None:
            failure = self._failure
            self._failure = None
            raise failure

    def stop(self):
        """Stop the reactor, unblocking run().

        Idempotent: calling stop() when the reactor is not running is a no-op
        (Twisted raises ReactorNotRunning, but callers here treat shutdown as
        safe to request more than once).
        """
        if not self.running:
            return
        self.running = False
        fut = self._stop_future

        def _resolve():
            if fut is not None and not fut.done():
                fut.set_result(None)

        self._loop.call_soon_threadsafe(_resolve)

    async def _shutdown(self):
        """Ordered teardown of all reactor-owned resources."""
        # Timers first, so nothing new is scheduled during teardown.
        for dc in list(self._delayed_calls):
            if dc.active():
                dc.cancel()
        self._delayed_calls.clear()

        # Listening ports and outgoing connectors.
        for port in list(self._ports):
            port.stopListening()
        self._ports.clear()
        for connector in list(self._connectors):
            connector.disconnect()
        self._connectors.clear()

        # Child processes: terminate any still running, then close.
        for transport in list(self._process_transports):
            try:
                if transport.get_returncode() is None:
                    transport.terminate()
                transport.close()
            except Exception:
                pass
        self._process_transports.clear()

        # Outstanding endpoint/executor tasks.
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

        # Cancel any remaining loop tasks that were NOT created through a reactor
        # registry -- e.g. defer.maybeDeferred / aio.utils schedule work with
        # asyncio.ensure_future directly. Without this, stopping while such a
        # task is pending leaks a live task past run(). The current task (this
        # _shutdown coroutine) is excluded.
        try:
            current = asyncio.current_task(self._loop)
            stragglers = [t for t in asyncio.all_tasks(self._loop)
                          if t is not current and not t.done()]
        except RuntimeError:
            stragglers = []
        for task in stragglers:
            task.cancel()
        if stragglers:
            await asyncio.gather(*stragglers, return_exceptions=True)

        # Let close callbacks run.
        await asyncio.sleep(0)

    # -- scheduling ------------------------------------------------------- #
    def callWhenRunning(self, fn, *args, **kw):
        """Call ``fn`` as soon as the reactor is running."""
        if self.running:
            self._ensure_loop().call_soon(fn, *args, **kw)
        else:
            self._when_running.append((fn, args, kw))

    def callLater(self, delay, fn, *args, **kw):
        """Schedule ``fn`` after ``delay`` seconds; return a _DelayedCall."""
        self._ensure_loop()
        dc = _DelayedCall(self, delay, fn, args, kw)
        self._delayed_calls.add(dc)
        return dc

    def callFromThread(self, fn, *args, **kw):
        """Schedule ``fn`` to run in the reactor thread (thread-safe)."""
        self._ensure_loop().call_soon_threadsafe(lambda: fn(*args, **kw))

    def callInThread(self, fn, *args, **kw):
        """Run ``fn`` in a worker thread; return a Deferred with the result."""
        loop = self._ensure_loop()
        deferred = Deferred()
        fut = loop.run_in_executor(None, lambda: fn(*args, **kw))
        self._tasks.add(fut)

        def _done(f):
            self._tasks.discard(f)
            try:
                deferred.callback(f.result())
            except Exception:
                deferred.errback(Failure())

        fut.add_done_callback(_done)
        return deferred

    # -- networking ------------------------------------------------------- #
    def listenUDP(self, port, protocol, interface='', maxPacketSize=8192):
        """Listen for UDP datagrams, driving ``protocol`` (a DatagramProtocol)."""
        loop = self._ensure_loop()
        handle = _Port()
        self._ports.append(handle)
        local_addr = (interface or '0.0.0.0', port)
        coro = loop.create_datagram_endpoint(lambda: protocol,
                                              local_addr=local_addr)

        def apply(result):
            transport, _proto = result
            handle._set_transport(transport)

        self._register_bind(coro, apply, None, 'listenUDP:%d' % port)
        return handle

    def listenTCP(self, port, factory, backlog=50, interface=''):
        """Listen for TCP connections, building protocols from ``factory``."""
        loop = self._ensure_loop()
        handle = _Port()
        self._ports.append(handle)
        coro = loop.create_server(lambda: _TwistedProtocolAdapter(factory),
                                  interface or '0.0.0.0', port,
                                  backlog=backlog)

        def apply(server):
            handle._set_server(server)

        self._register_bind(coro, apply, None, 'listenTCP:%d' % port)
        return handle

    def connectTCP(self, host, port, factory, timeout=30, bindAddress=None):
        """Connect to ``host:port``, building a protocol from ``factory``.

        Honors ``timeout`` (seconds, via asyncio.wait_for) and ``bindAddress``
        (a local ``(host, port)`` passed to create_connection's ``local_addr``).
        The returned connector supports reconnection through a
        ReconnectingClientFactory.
        """
        self._ensure_loop()
        connector = _Connector(self, host, port, factory, timeout, bindAddress)
        self._connectors.append(connector)
        connector.connect()
        return connector

    # -- processes -------------------------------------------------------- #
    def spawnProcess(self, processProtocol, executable, args=(), env=None,
                     path=None, uid=None, gid=None, usePTY=0, childFDs=None):
        """Spawn a child process, driving ``processProtocol`` (a ProcessProtocol).

        Returns a connector proxying the eventual process transport. ``args``
        follows the Twisted convention where ``args[0]`` is the program name.
        """
        loop = self._ensure_loop()
        rest = tuple(args[1:]) if args else ()
        coro = loop.subprocess_exec(lambda: processProtocol,
                                    executable, *rest,
                                    env=env, cwd=path)

        def apply(result):
            transport, _proto = result
            self._process_transports.append(transport)

        self._register_bind(coro, apply, None, 'spawnProcess:%s' % executable)
        return _ProcessConnector(processProtocol)

    # -- internal bind scheduling ----------------------------------------- #
    def _register_bind(self, coro, apply, on_error, label):
        """Bind an endpoint now (if running) or during run()'s startup phase.

        ``apply(result)`` installs the bound transport/server/connection.
        ``on_error(exc)`` handles a bind failure; when None, the failure is fatal
        and stops the reactor (and aborts pre-run startup).
        """
        if self.running:
            task = asyncio.ensure_future(coro)
            self._tasks.add(task)

            def _done(t):
                self._tasks.discard(t)
                if t.cancelled():
                    return
                exc = t.exception()
                if exc is not None:
                    if on_error is not None:
                        on_error(exc)
                    else:
                        self._fatal(exc)
                    return
                apply(t.result())

            task.add_done_callback(_done)
        else:
            self._pending_binds.append((coro, apply, on_error, label))

    def _fatal(self, exc):
        """Record a fatal mid-run error and stop the reactor."""
        if self._failure is None:
            self._failure = exc
        if self.running:
            try:
                self.stop()
            except ReactorNotRunning:
                pass


class _ProcessConnector(object):
    """Proxy returned by spawnProcess; delegates to the process transport.

    The real transport is created asynchronously; attribute access resolves
    against ``processProtocol.transport`` once connectionMade has populated it.
    """

    def __init__(self, protocol):
        object.__setattr__(self, '_protocol', protocol)

    def __getattr__(self, name):
        transport = object.__getattribute__(self, '_protocol').transport
        if transport is None:
            raise AttributeError(
                "process transport not yet available (attr %r)" % name)
        return getattr(transport, name)


# Module-level singleton, mirroring ``from twisted.internet import reactor``.
reactor = _Reactor()
