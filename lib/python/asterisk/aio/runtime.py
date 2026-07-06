"""The single async test-runtime owner for the asterisk.aio layer.

Phase B, step B1.0. This module defines ``AsyncTestRuntime``: the one concrete
object that owns, as the authoritative set,

  * the event loop reference,
  * every resource registry (delayed calls, endpoint/executor tasks, listening
    ports, outgoing connectors, child-process transports, async cleanups),
  * the run-completion signal and the first fatal mid-run error,
  * the ordered asynchronous shutdown, and
  * an explicit lifecycle **state machine**.

There is no module-level runtime singleton. Ownership is *per run*: a fresh
``AsyncTestRuntime`` is installed as the current runtime for the duration of a
run and detached on shutdown (see the current-runtime holder at the bottom of
this module). ``reactor.py`` is a thin, stateless facade that resolves whatever
runtime is currently installed on every call; the higher-level ``asyncio.run()``
entrypoint added in later B1 steps installs and drives one such runtime
directly. The Twisted-shaped reactor surface (``run``/``stop``/``callLater``/
``listenTCP``/...) lives here so both the facade and the native entrypoint share
one implementation and one set of registries.

Lifecycle states (``_RuntimeState``)::

    COLLECTING  -- idle / pre-run; binds queue, callWhenRunning queues
    STARTING    -- run() is draining the pre-run bind queue to quiescence;
                   a bind failure with no error handler is fatal and aborts
    RUNNING     -- the loop is turning; mid-run binds become tracked tasks
    STOPPING    -- ordered shutdown in progress
    STOPPED     -- run() has returned/raised; loop no longer owned

The public ``running`` boolean is *derived* from the state (true only during
STARTING/RUNNING and only until stop() is requested), so the state machine is
the single source of truth rather than a separately-maintained flag.
"""

import asyncio
import enum
import logging
import sys
import time

from .defer import Deferred
from .failure import Failure

LOGGER = logging.getLogger(__name__)


class ReactorNotRunning(Exception):
    """Raised by stop() when the reactor is not running (twisted parity)."""


class ReactorAlreadyRunning(Exception):
    """Raised by run() when the reactor is already running (twisted parity)."""


class AlreadyCalled(Exception):
    """Raised by _DelayedCall.cancel() when the call already fired."""


class AlreadyCancelled(Exception):
    """Raised by _DelayedCall.cancel() when already cancelled."""


class _RuntimeState(enum.Enum):
    """Explicit lifecycle states for the runtime (see module docstring)."""

    COLLECTING = "collecting"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"


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
    the runtime's timer registry when it fires or is cancelled.
    """

    def __init__(self, runtime, delay, fn, args, kw):
        self._runtime = runtime
        self._delay = delay
        self._fn = fn
        self._args = args
        self._kw = kw
        self._cancelled = False
        self._called = False
        # Absolute wall-clock time (time.time() units) at which this call is
        # scheduled to fire. Twisted's DelayedCall.getTime() returns this in
        # reactor.seconds() units, and callers (e.g. TestCase.reset_timeout)
        # feed it to datetime.fromtimestamp(), so it must be a POSIX timestamp
        # rather than asyncio's monotonic loop clock.
        self._scheduled_time = time.time() + delay
        self._handle = runtime._loop.call_later(delay, self._fire)

    @classmethod
    def inert(cls, runtime, delay, fn, args, kw):
        """Build a DelayedCall that never scheduled anything on the loop.

        Used when ``callLater`` is refused during teardown: after the run the
        loop may be closed, so we must not call ``loop.call_later`` at all. The
        returned call is already inert (``active()`` is False) and holds no loop
        handle, so it neither fires nor needs cancelling.
        """
        self = cls.__new__(cls)
        self._runtime = runtime
        self._delay = delay
        self._fn = fn
        self._args = args
        self._kw = kw
        self._cancelled = True   # inert: active() is False
        self._called = False
        self._scheduled_time = time.time() + delay
        self._handle = None
        return self

    def _fire(self):
        self._called = True
        self._runtime._delayed_calls.discard(self)
        self._fn(*self._args, **self._kw)

    def active(self):
        """Return True if the call has neither fired nor been cancelled."""
        return not (self._cancelled or self._called)

    def getTime(self):
        """Return the wall-clock time (seconds since epoch) this call fires.

        Mirrors twisted.internet.base.DelayedCall.getTime(), whose result is a
        reactor.seconds()/time.time()-compatible absolute timestamp.
        """
        return self._scheduled_time

    def cancel(self):
        """Cancel the pending call."""
        if self._called:
            raise AlreadyCalled()
        if self._cancelled:
            raise AlreadyCancelled()
        self._cancelled = True
        self._handle.cancel()
        self._runtime._delayed_calls.discard(self)

    def reset(self, delay):
        """Reschedule to fire ``delay`` seconds from now."""
        if not self.active():
            raise AlreadyCalled()
        self._handle.cancel()
        self._delay = delay
        self._scheduled_time = time.time() + delay
        self._handle = self._runtime._loop.call_later(delay, self._fire)

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
        # For UDP: the socket bound synchronously in listenUDP. Owned by this
        # handle only until the asyncio datagram transport takes it over (at
        # which point _set_transport clears it); closed here if that never
        # happens (e.g. stopListening before the endpoint finished binding).
        self._presock = None

    def _set_transport(self, transport):
        if self._closed and transport is not None:
            transport.close()
            return
        self._transport = transport
        # The asyncio transport now owns the pre-bound socket's fd.
        self._presock = None

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
        if self._presock is not None:
            try:
                self._presock.close()
            except OSError:
                pass
            self._presock = None


class _SyncDatagramTransport(object):
    """A UDP transport available synchronously from ``listenUDP``.

    Twisted's ``reactor.listenUDP`` binds the socket and installs
    ``protocol.transport`` before returning, so pluggable modules routinely send
    a first datagram on the very next line. asyncio's
    ``create_datagram_endpoint`` is a coroutine, so between ``listenUDP``
    returning and ``DatagramProtocol.connection_made`` installing the
    asyncio-backed writer there is a window where ``protocol.transport`` would
    otherwise be ``None``. This shim closes that window by carrying sends
    straight to the freshly bound socket. Its ``write(data, addr)`` mirrors the
    Twisted UDP transport signature; ``connection_made`` later replaces it with
    the asyncio-backed writer over the same fd.
    """

    def __init__(self, sock):
        self._sock = sock

    def write(self, data, addr=None):
        try:
            if addr is None:
                self._sock.send(data)
            else:
                self._sock.sendto(data, addr)
        except (BlockingIOError, InterruptedError):
            # UDP send buffer momentarily full; Twisted drops silently too.
            pass

    def writeSequence(self, seq, addr=None):
        self.write(b''.join(seq), addr)

    def getHost(self):
        return self._sock.getsockname()

    def loseConnection(self):
        # No-op: once connection_made fires the asyncio transport owns the fd,
        # and the _Port handle closes the socket if it never does.
        pass

    def close(self):
        pass


class _Connector(object):
    """Handle for an outgoing TCP connection (twisted IConnector).

    Supports reconnection: when an established connection is lost,
    ``_TwistedProtocolAdapter.connection_lost`` notifies the factory via
    ``clientConnectionLost``; a ReconnectingClientFactory's ``retry`` then calls
    ``connector.connect()`` to start a fresh attempt. Honors the original
    ``timeout`` and ``bindAddress`` on every attempt.
    """

    def __init__(self, runtime, host, port, factory, timeout, bindAddress):
        self._runtime = runtime
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
        loop = self._runtime._ensure_loop()
        local_addr = self._bindAddress if self._bindAddress else None

        def _bind():
            coro = loop.create_connection(
                lambda: _TwistedProtocolAdapter(factory, self),
                self.host, self.port, local_addr=local_addr)
            if self._timeout:
                coro = asyncio.wait_for(coro, self._timeout)
            return coro

        def apply(result):
            transport, _proto = result
            self._transport = transport

        def on_error(exc):
            # A connection failure is not fatal to the runtime: hand it to the
            # factory (starpy reconnect logic depends on this) instead of
            # aborting the run. A wait_for timeout arrives as TimeoutError.
            if self._stopped:
                return
            if hasattr(factory, 'clientConnectionFailed'):
                factory.clientConnectionFailed(self, Failure(exc))

        self._runtime._register_bind(
            _bind, apply, on_error,
            'connectTCP:%s:%d' % (self.host, self.port))

    def stopConnecting(self):
        self.disconnect()

    def disconnect(self):
        # Mark stopped first so an in-flight connection_lost does not trigger a
        # reconnect (e.g. during runtime shutdown).
        self._stopped = True
        factory = self._factory
        # Close the transport in ``finally`` so a raising ``stopTrying()`` can
        # never leave the socket open: the shutdown path clears the connector
        # registry right after this call, so a leaked transport here would be
        # unreachable and outlive the run. The stopTrying() error still
        # propagates (the caller collects it into the teardown error set).
        try:
            if hasattr(factory, 'stopTrying'):
                factory.stopTrying()
        finally:
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
# The runtime
# ---------------------------------------------------------------------------- #
class AsyncTestRuntime(object):
    """The single concrete owner of the asyncio test runtime.

    Holds the one authoritative copy of the event loop, every resource
    registry, the completion signal, the first fatal error, and the ordered
    shutdown, driven by an explicit lifecycle state machine. The ``reactor``
    facade and the native ``asyncio.run()`` entrypoint both delegate here.
    """

    def __init__(self):
        self._state = _RuntimeState.COLLECTING
        self._stop_requested = False
        self._loop = None
        self._when_running = []       # callWhenRunning queue: (fn, args, kw)
        self._pending_binds = []      # pre-run binds: (coro, apply, on_error, label)
        self._completion = None       # resolved by stop(); awaited by run()
        self._failure = None          # first fatal mid-run error, re-raised by run()
        self._startup_task = None     # in-flight start_all() drain; awaited by
                                      # concurrent start_all() callers
        # Pluggable-module lifecycle (design point 3).
        self._modules = []            # retained module instances (strong refs),
                                      # in registration order
        self._started_modules = []    # modules whose start() was invoked (incl.
                                      # failed) or that have no start() -- each is
                                      # close()d once, in forward order
        # Resource registries for ordered shutdown.
        self._delayed_calls = set()
        self._tasks = set()
        self._ports = []
        self._connectors = []
        self._process_transports = []
        self._async_cleanups = []     # zero-arg awaitable factories, run at shutdown

    # -- state ------------------------------------------------------------ #
    @property
    def state(self):
        """The current lifecycle state (a ``_RuntimeState``)."""
        return self._state

    @property
    def running(self):
        """True while the loop is (about to be) turning and no stop is pending.

        Derived from the state machine rather than stored, so the state is the
        single source of truth. True during STARTING and RUNNING, and only
        until stop() marks a stop as requested.
        """
        return (self._state in (_RuntimeState.STARTING, _RuntimeState.RUNNING)
                and not self._stop_requested)

    def is_active(self):
        """True while this runtime owns a run in progress.

        Active means the state machine is mid-run (STARTING, RUNNING, or
        STOPPING) -- i.e. it owns the loop and a completion signal. COLLECTING
        (idle/pre-run) and STOPPED (run finished) are *not* active.
        """
        return self._state in (_RuntimeState.STARTING, _RuntimeState.RUNNING,
                               _RuntimeState.STOPPING)

    def _tearing_down(self):
        """True once ordered shutdown has begun (STOPPING) or finished (STOPPED).

        Both shutdown owners set STOPPING *before* running ``_shutdown`` and flip
        to STOPPED once it completes (native ``_finish``; legacy ``run``'s
        ``finally``), so a registration issued from a callback firing *during*
        teardown -- a process-exit, a connection-lost, an async cleanup -- as
        well as any registration attempted against an already-finished run
        observes this as True. Every registration entry point consults it and
        refuses cleanly, so a resource cannot slip past the snapshot-based
        cleanup or queue work that survives into another run (design doc
        point 4).

        STOPPED is included on purpose: a finished runtime is not a valid
        registration target. Sequential reuse must first return the runtime to a
        clean COLLECTING baseline via ``reset()`` (or install a fresh runtime);
        only then do registrations for the next run land. This keeps "no callback
        may queue work for the next run" enforceable -- the reset is the single
        gate between runs, not an implicit "register while STOPPED" path.
        """
        return self._state in (_RuntimeState.STOPPING, _RuntimeState.STOPPED)

    def live_resources(self):
        """Return sorted names of registries still holding a live resource.

        Empty list means the runtime owns nothing (safe to discard/replace).
        Used by ``reset()`` and by ``install_runtime()`` to refuse silently
        orphaning a runtime that still owns timers, tasks, ports, connectors,
        process transports, cleanups, queued binds/callbacks, or retained
        modules awaiting close().

        Retained modules count as live: a registered module holds a strong
        reference and an unclosed ``close()`` hook, so a runtime carrying one
        must run the ordered shutdown before it can be replaced or reset --
        otherwise the module is orphaned and never closed. ``_started_modules``
        is a subset of ``_modules`` but is reported independently so a partial
        state (all closed but registration not yet cleared, or vice versa) still
        surfaces.
        """
        live = {
            'delayed_calls': self._delayed_calls,
            'tasks': self._tasks,
            'ports': self._ports,
            'connectors': self._connectors,
            'process_transports': self._process_transports,
            'async_cleanups': self._async_cleanups,
            'pending_binds': self._pending_binds,
            'when_running': self._when_running,
            'modules': self._modules,
            'started_modules': self._started_modules,
        }
        return sorted(name for name, reg in live.items() if reg)

    def reset(self, loop=None):
        """Return the runtime to a fresh COLLECTING baseline.

        One authoritative reinitialization entry point (kept in lockstep with
        ``__init__``) so callers -- notably the unit-test fixtures -- do not
        have to poke the private registries individually. Optionally binds a
        fresh event loop.

        Test-facing only, and deliberately conservative: it refuses to run while
        the runtime is active (STARTING/RUNNING/STOPPING) or while any registry
        still holds a live resource, rather than silently discarding timers,
        ports, tasks, or process transports into a leak. Callers that want a
        clean slate mid-process should install a *fresh* runtime
        (``new_runtime``) instead of reusing one; reset() exists for fixtures
        that have already driven a run to completion.
        """
        if self.is_active():
            raise RuntimeError(
                "cannot reset an active runtime (state=%s); stop it first"
                % self._state.value)
        leaked = self.live_resources()
        if leaked:
            raise RuntimeError(
                "cannot reset a runtime with live resources (%s); run the "
                "ordered shutdown first" % ', '.join(leaked))
        self._state = _RuntimeState.COLLECTING
        self._stop_requested = False
        self._loop = loop
        self._when_running = []
        self._pending_binds = []
        self._completion = None
        self._failure = None
        self._delayed_calls = set()
        self._tasks = set()
        self._ports = []
        self._connectors = []
        self._process_transports = []
        self._async_cleanups = []
        self._startup_task = None
        self._modules = []
        self._started_modules = []

    # -- loop access ------------------------------------------------------ #
    def _ensure_loop(self):
        if self._loop is None:
            self._loop = _get_loop()
        return self._loop

    # -- lifecycle -------------------------------------------------------- #
    def run(self, installSignalHandlers=True):
        """Run the event loop until stop() is called (blocks).

        Legacy driver for the still-unmigrated ``run-test`` scripts. It owns its
        loop (``run_until_complete``) but drives the **same** ``start_all`` /
        ``run_async`` / ``_shutdown`` sequence the native ``_main`` path uses, so
        module ``start()`` and the startup state machine have a single
        implementation across both paths (design doc points 2b/5): COLLECTING ->
        STARTING (``start_all`` drains the pre-run queue, surfacing a fatal bind)
        -> RUNNING (kickoff flushed) -> await completion (``run_async``) ->
        STOPPING (ordered ``_shutdown`` in this method's ``finally``) -> STOPPED,
        then re-raise any fatal mid-run error.
        """
        # Guard on the *state*, not the ``running`` property: stop() flips
        # running to False (via _stop_requested) while this run() is still
        # unwinding through STOPPING, so a callback that calls stop() then run()
        # would otherwise slip past a running-based guard and re-enter. Reject
        # while a run is in any active phase or while an unresolved completion
        # future still exists.
        if (self._state in (_RuntimeState.STARTING, _RuntimeState.RUNNING,
                            _RuntimeState.STOPPING)
                or self._completion is not None):
            raise ReactorAlreadyRunning()
        loop = self._ensure_loop()
        if loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop

        # Drive the shared startup state machine, then await completion. A fatal
        # startup bind raises out of start_all; the finally still tears down.
        #
        # NOTE (per-run ownership): the ordered shutdown clears every registry,
        # but this blocking run() does *not* detach the runtime from the holder
        # -- the STOPPED (resource-free) runtime stays installed until the caller
        # detaches it. Full detach-on-shutdown belongs to the native asyncio.run()
        # entrypoint / legacy-run wiring (later B1 steps); callers that want a
        # fresh owner per run install one via new_runtime() and detach it after.
        try:
            loop.run_until_complete(self.start_all())
            loop.run_until_complete(self.run_async())
        finally:
            # Single ordered teardown via _finish() (guarantees STOPPED even if
            # _shutdown raises), mirroring the native _main path. A fatal
            # mid-run task stores _failure and stops *normally* -- no exception
            # is unwinding through this finally -- so sys.exc_info() alone would
            # miss it and let a teardown error mask the real failure. Fold
            # _failure into the snapshot so a teardown error is logged-and-
            # suppressed (never masks) whenever setup/run already failed; on a
            # genuinely clean run a teardown failure is itself real and
            # propagates.
            setup_failed = (sys.exc_info()[0] is not None
                            or self._failure is not None)
            try:
                loop.run_until_complete(self._finish())
            except Exception:
                if setup_failed:
                    LOGGER.exception("error during runtime shutdown")
                else:
                    raise

        # Re-raise the fatal mid-run/startup error after teardown above.
        if self._failure is not None:
            failure = self._failure
            self._failure = None
            raise failure

    # -- pluggable-module lifecycle (design point 3) ---------------------- #
    def register_module(self, module):
        """Retain a constructed pluggable module for the run's lifetime.

        The loader hands every constructed local/global module here so it is
        (a) kept alive by a strong reference rather than garbage-collected after
        construction, and (b) enrolled in the async ``start()``/``close()``
        lifecycle driven by ``start_all`` and ``_shutdown``. Registration order
        is preserved: ``start()`` runs in this order, ``close()`` in the same
        (forward) order.

        A module offered during teardown is refused: the close snapshot has
        already been taken, so appending it here would either discard it
        unclosed or leak it into another run (design doc point 4).
        """
        if self._tearing_down():
            return
        self._modules.append(module)

    async def _start_modules(self):
        """Invoke each retained module's optional ``start()`` (design point 3).

        Runs during STARTING, before the bind-queue drain, so binds a module
        issues from ``start()`` land in the awaited startup queue. Semantics:

          - a module *without* ``start()`` is treated as already-started;
          - a module is enrolled for ``close()`` *before* its ``start()`` is
            awaited, so a start that fails or partially initializes is still
            closed (``close()`` must tolerate partial init and be idempotent);
          - modules whose ``start()`` is never reached -- because an earlier
            module failed or a stop aborted startup -- are NOT enrolled and so
            are not closed;
          - each ``start()`` is raced against the completion signal so a
            cooperative stop aborts a long start without launching the test;
          - a ``start()`` failure is fatal: it records ``_failure`` and
            propagates, aborting startup (the driver's teardown then closes the
            enrolled modules, including this one).

        Returns False if a stop won a race (abort the rest of startup), else
        True.
        """
        for module in self._modules:
            if self._stop_requested:
                return False
            start = getattr(module, 'start', None)
            # Enroll BEFORE awaiting: a failed/partial start must still close.
            self._started_modules.append(module)
            if start is None:
                continue
            if not await self._drive_start(start()):
                return False
        return True

    async def _drive_start(self, coro):
        """Await one module ``start()``, racing it against a stop.

        Mirrors ``_drive_bind`` without apply/on_error: a module ``start()`` has
        no non-fatal handler. Returns True if it completed; False if a stop won
        the race (the in-flight start task is cancelled and drained so nothing is
        left unawaited). A failure records ``_failure`` and propagates.
        """
        start_task = asyncio.ensure_future(coro)
        done, _pending = await asyncio.wait(
            {start_task, self._completion},
            return_when=asyncio.FIRST_COMPLETED)
        if start_task not in done:
            start_task.cancel()
            try:
                await start_task
            except BaseException:
                pass
            return False
        try:
            start_task.result()
        except Exception as exc:
            self._failure = exc
            raise
        return True

    async def start_all(self):
        """Single guarded startup driver for the native AND legacy paths.

        The one place the startup state machine runs (design doc point 2b).
        ``_main`` (native) awaits it under ``asyncio.run``; blocking ``run()``
        (legacy) drives the *same* coroutine via ``run_until_complete`` -- module
        ``start()`` is therefore invoked from exactly one sequence, never twice.
        It creates the per-run completion signal on the running loop, advances
        COLLECTING -> STARTING (``running`` becomes true here, so a stop during
        awaited startup is meaningful), drains the startup queue to quiescence
        surfacing a fatal bind, then RUNNING and flushes the ``callWhenRunning``
        kickoff queue.

        Deliberately narrow (points 2/2b): it does NOT await completion (that is
        ``run_async``) and does NOT tear down (that is ``_shutdown``, run from the
        driver's ``finally``). A fatal bind stores ``_failure`` and re-raises so
        the driver's ``finally`` runs the ordered teardown and re-raises after.

        Cooperative stop: a stop requested during STARTING (cross-thread via
        ``call_soon_threadsafe``, or from a bind's ``apply``) must actually abort
        startup, not merely resolve completion -- otherwise this coroutine would
        keep invoking binds, reach RUNNING, and flush kickoff, launching the very
        test the stop was meant to prevent. So the flag is checked between binds,
        each bind is raced against the completion signal, and on stop we skip all
        remaining startup + kickoff and fall through to STOPPING (never RUNNING),
        leaving the ordered teardown to the driver's ``finally``.

        Once-per-run under overlap: the drain runs as one tracked task
        (``_startup_task``). A caller that arrives while startup is still in
        flight awaits that same task rather than returning early, so it never
        proceeds believing startup finished while binds are still running.
        """
        # Coordinate concurrent callers around a single in-flight startup so the
        # once-per-run guarantee holds even under overlap (design point 2b):
        #
        #   - A startup is already in flight (STARTING): a second caller must
        #     AWAIT that same task, not return early -- returning would let it
        #     proceed as if startup finished while binds are still running.
        #   - Startup already finished (RUNNING) or torn down (STOPPING) with no
        #     task in flight: stray post-startup call, a harmless no-op.
        #   - An unresolved completion with no startup task means a run is
        #     already in flight by some other path: reject.
        #
        # Secondary callers shield the shared task: awaiting it directly would
        # propagate *their* cancellation into the shared drain, cancelling the
        # primary driver's startup too. shield() lets a cancelled secondary
        # waiter unwind without owning cancellation of the in-flight startup.
        if self._startup_task is not None:
            await asyncio.shield(self._startup_task)
            return
        if self._state in (_RuntimeState.STARTING, _RuntimeState.RUNNING,
                           _RuntimeState.STOPPING):
            return
        if self._completion is not None:
            raise ReactorAlreadyRunning()

        loop = asyncio.get_running_loop()
        self._loop = loop
        self._failure = None
        self._stop_requested = False
        self._completion = loop.create_future()
        self._state = _RuntimeState.STARTING

        # Drive the drain as a tracked task so overlapping callers await the SAME
        # startup. No await separates the state flip above from scheduling the
        # task, so a concurrent caller never observes STARTING without a task.
        self._startup_task = asyncio.ensure_future(self._drain_startup(loop))
        try:
            await self._startup_task
        finally:
            self._startup_task = None

    async def _drain_startup(self, loop):
        """Drive the startup drain to quiescence: the body of ``start_all``.

        Split out so it can run as a single tracked task per run (see
        ``start_all``), letting concurrent ``start_all`` callers await the one
        in-flight startup rather than each re-driving the drain. Runs the point-2b
        startup sequence: invoke each retained module's ``start()`` (step 2),
        drain the bind queue to quiescence (step 3) -- both surfacing failures and
        honoring a stop -- then, absent a stop, enter RUNNING and flush the
        ``callWhenRunning`` kickoff queue. On stop it goes straight to STOPPING
        (never RUNNING/kickoff).
        """
        # Step 2: module start() hooks (may enqueue binds drained just below).
        if not await self._start_modules():
            # A stop won a start() race: abort startup without launching.
            self._state = _RuntimeState.STOPPING
            if not self._completion.done():
                self._completion.set_result(None)
            return

        # Step 3: drain the bind queue (constructor + start()-issued binds).
        while self._pending_binds and not self._stop_requested:
            batch = self._pending_binds
            self._pending_binds = []
            for index, (make_coro, apply, on_error, label) in enumerate(batch):
                if self._stop_requested:
                    # Return the still-unrun remainder (inert factories, discarded
                    # at shutdown) and abort the drain.
                    self._pending_binds = batch[index:] + self._pending_binds
                    break
                try:
                    resolved = await self._drive_bind(make_coro(), apply,
                                                      on_error)
                except Exception:
                    # Fatal bind (no handler). Hand back the unrun remainder so
                    # shutdown discards the inert factories, then re-raise; the
                    # driver's finally tears down and re-raises _failure.
                    self._pending_binds = batch[index + 1:] + self._pending_binds
                    raise
                if not resolved:
                    # Stop won the race against this bind; the in-flight task was
                    # cancelled and drained. Return the unrun remainder and abort.
                    self._pending_binds = batch[index + 1:] + self._pending_binds
                    break

        if self._stop_requested:
            # Abort startup: never flush kickoff, never enter RUNNING. Go
            # straight to STOPPING; the driver's finally runs the ordered
            # teardown. Ensure completion is resolved so the bridge returns.
            self._state = _RuntimeState.STOPPING
            if not self._completion.done():
                self._completion.set_result(None)
            return

        # RUNNING: flush callWhenRunning kickoffs (after all binds, never before).
        self._state = _RuntimeState.RUNNING
        queued = self._when_running
        self._when_running = []
        for fn, args, kw in queued:
            loop.call_soon(fn, *args, **kw)

    async def _drive_bind(self, coro, apply, on_error):
        """Await one startup bind, racing it against a stop.

        Returns True if the bind resolved (success -> ``apply`` invoked, or a
        non-fatal failure handled by ``on_error``); returns False if a stop won
        the race, in which case the in-flight bind task is cancelled and drained
        (so nothing is left unawaited). A fatal failure (``on_error is None``)
        records ``_failure`` and propagates, aborting the drain.
        """
        bind_task = asyncio.ensure_future(coro)
        done, _pending = await asyncio.wait(
            {bind_task, self._completion},
            return_when=asyncio.FIRST_COMPLETED)
        if bind_task not in done:
            # Stop resolved completion first: cancel and drain the in-flight bind
            # so it cannot emit "task/coroutine was never awaited".
            bind_task.cancel()
            try:
                await bind_task
            except BaseException:
                pass
            return False
        try:
            result = bind_task.result()
        except Exception as exc:
            if on_error is not None:
                on_error(exc)
                return True
            # Fatal: record and propagate to abort startup.
            self._failure = exc
            raise
        apply(result)
        return True

    async def run_async(self):
        """Transitional bridge: adopt the running loop and await completion.

        The linchpin of incrementality (design doc Section, point 2). By the time
        ``_main`` awaits this, ``start_all`` has already advanced the runtime
        to RUNNING with a live completion future, so this bridge does the one
        thing it owns: **await the runtime's completion signal**. ``stop()``
        resolves that future (via ``call_soon_threadsafe``), so the relay is
        implicit -- when stop is requested this returns.

        It deliberately does NOT mark ``running``, does NOT drive ``start_all`` /
        startup, and does NOT perform shutdown: startup has one driver (``_main``)
        and shutdown has one owner (``_main``'s ``finally`` -> ordered
        ``_shutdown``). Every shim registration it services already lands in this
        runtime's registries, and any fatal error sits on this runtime's
        ``_failure``, which ``_main`` re-raises. Deleted in B4 with reactor.py.
        """
        if self._completion is None:
            # Misuse: run_async() awaits a completion start_all() must have
            # created. Never reached on the _main path.
            raise ReactorNotRunning()
        await self._completion

    async def _finish(self):
        """Ordered teardown for the native ``_main`` entrypoint's ``finally``.

        The single shutdown owner on the native path: STOPPING -> run the ordered
        ``_shutdown`` over the one registry set -> STOPPED, then clear the
        completion future. Safe to call from ``_main``'s ``finally`` regardless of
        where the run failed -- construction (state still COLLECTING, but
        constructor-registered timers/ports/processes are torn down), awaited
        startup (a partially-bound run), or mid-run -- so nothing a constructor or
        a failing module registered escapes teardown.

        Final-state cleanup is protected by its own ``finally`` so the runtime
        still lands in STOPPED with a cleared completion future even if the
        ordered ``_shutdown`` itself raises -- the runtime must never be left
        pinned in STOPPING.
        """
        self._state = _RuntimeState.STOPPING
        try:
            await self._shutdown()
        finally:
            self._state = _RuntimeState.STOPPED
            self._completion = None

    def stop(self):
        """Stop the runtime, unblocking run().

        Idempotent: calling stop() when not running is a no-op (Twisted raises
        ReactorNotRunning, but callers here treat shutdown as safe to request
        more than once). Marking the stop requested immediately flips
        ``running`` to False, matching the old eager flag.
        """
        if not self.running:
            return
        self._stop_requested = True
        fut = self._completion

        def _resolve():
            if fut is not None and not fut.done():
                fut.set_result(None)

        self._loop.call_soon_threadsafe(_resolve)

    async def _shutdown(self):
        """Ordered teardown of all runtime-owned resources.

        Teardown is *best-effort*: a failure closing one resource must not skip
        the resources after it, or a leaked port/connector/task would outlive
        the run. Every phase therefore runs to completion, each phase's
        exception is collected rather than propagated on the spot, and the
        collected errors are raised only at the very end -- as a single
        exception if there was one, or an ``ExceptionGroup`` if several. So the
        caller still learns teardown failed (findings 1 & 4: "isolated" must not
        mean invisible) without any resource being abandoned unclosed.
        """
        errors = []

        # Un-driven startup queues first. If a run is torn down before (or
        # during) the awaited startup drain -- e.g. a constructor registered an
        # addStartupBind/listenTCP/connectTCP bind and then a later constructor
        # raised, or a stop aborted startup with binds still queued -- the
        # remaining entries are coroutine *factories* that were never invoked.
        # They hold no live coroutine, so simply discarding them cannot emit
        # "coroutine was never awaited". Also drop any queued callWhenRunning
        # kickoffs that will now never fire.
        self._pending_binds = []
        self._when_running = []

        # Timers, so nothing new is scheduled during teardown.
        for dc in list(self._delayed_calls):
            if dc.active():
                dc.cancel()
        self._delayed_calls.clear()

        # Async cleanups (e.g. aiohttp AppRunner.cleanup) run first, while the
        # loop is still healthy, so servers close their sites/connections
        # gracefully before we cancel any straggler tasks.
        for cleanup in list(self._async_cleanups):
            try:
                await cleanup()
            except Exception as exc:
                errors.append(exc)
        self._async_cleanups.clear()

        # Module close() hooks (design point 3): every module whose start() was
        # invoked -- or that had no start() and so is treated as started -- is
        # closed exactly once, in forward registration order (behavior-preserving:
        # the async-cleanup registry also runs forward). close() must tolerate
        # partial initialization and be idempotent; failures are isolated so one
        # module's broken close cannot skip the rest. Modules whose start() was
        # never reached were never enrolled and are not closed here.
        for module in list(self._started_modules):
            close = getattr(module, 'close', None)
            if close is None:
                continue
            try:
                await close()
            except Exception as exc:
                errors.append(exc)
        self._started_modules.clear()
        self._modules = []

        # Listening ports and outgoing connectors. Guarded so a broken
        # stopListening()/disconnect() cannot abort the phases that follow
        # (child-process teardown, task cancellation); the error is collected
        # and re-raised at the end.
        for port in list(self._ports):
            try:
                port.stopListening()
            except Exception as exc:
                errors.append(exc)
        self._ports.clear()
        for connector in list(self._connectors):
            try:
                connector.disconnect()
            except Exception as exc:
                errors.append(exc)
        self._connectors.clear()

        # Child processes: terminate and let asyncio's watcher reap them before
        # transports are closed. Calling BaseSubprocessTransport.close() while a
        # child is live invokes Popen.poll()/kill(), which can steal waitpid()
        # from PidfdChildWatcher and manufacture return code 255.
        process_transports = list(self._process_transports)

        def _live(transports):
            # A transport is "live" iff its child has not exited -- decided by
            # return code ALONE, never by whether terminate()/kill() succeeded.
            # A terminate() that raised must still leave its child in the live
            # set so the kill phase below reaches it; otherwise a broken
            # terminate() (signal never delivered) leaks a running child. A
            # get_returncode() that itself raises is treated as "still live" and
            # its error collected, so the child is still driven to exit.
            live = []
            for transport in transports:
                try:
                    still_running = transport.get_returncode() is None
                except Exception as exc:
                    errors.append(exc)
                    still_running = True
                if still_running:
                    live.append(transport)
            return live

        async def _await_exit(transports):
            # gather(return_exceptions=True) reports a raising waitForExit() as a
            # result rather than propagating it, so ONE failing child can neither
            # abort the reap of its siblings nor skip the phases that follow. The
            # wait_for bound turns a child that ignores the signal into a timeout
            # (NOT an error) -- the caller re-checks live state and escalates to
            # kill(). A cancellation from that timeout is not itself an error.
            if not transports:
                return
            try:
                results = await asyncio.wait_for(asyncio.gather(*(
                    transport.waitForExit() for transport in transports),
                    return_exceptions=True), 1.0)
            except asyncio.TimeoutError:
                return
            for res in results:
                if (isinstance(res, BaseException)
                        and not isinstance(res, asyncio.CancelledError)):
                    errors.append(res)

        # Every not-yet-exited child is driven to termination. terminate() errors
        # are collected but do NOT remove the transport from the live set.
        running_processes = _live(process_transports)
        for transport in running_processes:
            try:
                transport.terminate()
            except Exception as exc:
                errors.append(exc)

        if running_processes:
            # First reap after terminate(). Whether it times out OR a
            # waitForExit() raised OR it returned without the child actually
            # exiting, the kill decision below is driven by RECOMPUTED live state
            # (return code still None), never by the wait outcome or a timeout
            # flag -- so a raising waitForExit() can no longer leave a child
            # alive. Escalate any remaining live child to kill(), then reap again.
            await _await_exit(running_processes)
            still_live = _live(running_processes)
            for transport in still_live:
                try:
                    transport.kill()
                except Exception as exc:
                    errors.append(exc)
            await _await_exit(still_live)

        for transport in process_transports:
            try:
                transport.loseConnection()
            except Exception as exc:
                errors.append(exc)
        self._process_transports.clear()

        # Outstanding endpoint/executor tasks.
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

        # Cancel any remaining loop tasks that were NOT created through a runtime
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

        # Every resource was attempted; now surface any failures collected
        # along the way. One error re-raises as itself (preserving type/context
        # for callers that special-case it); several aggregate into an
        # ExceptionGroup so none is lost.
        if errors:
            if len(errors) == 1:
                raise errors[0]
            raise ExceptionGroup("errors during runtime shutdown", errors)

    # -- scheduling ------------------------------------------------------- #
    def callWhenRunning(self, fn, *args, **kw):
        """Call ``fn`` as soon as the runtime is running."""
        if self._tearing_down():
            # Never fire a kickoff during/after teardown -- the run is ending.
            return
        if self._state == _RuntimeState.RUNNING:
            self._ensure_loop().call_soon(fn, *args, **kw)
        else:
            self._when_running.append((fn, args, kw))

    def callLater(self, delay, fn, *args, **kw):
        """Schedule ``fn`` after ``delay`` seconds; return a _DelayedCall."""
        if self._tearing_down():
            # Refuse BEFORE touching the loop: once STOPPED the loop may be
            # closed, so calling loop.call_later would raise. Hand back an inert
            # DelayedCall that never scheduled anything (active() is False) and
            # is not tracked.
            return _DelayedCall.inert(self, delay, fn, args, kw)
        self._ensure_loop()
        dc = _DelayedCall(self, delay, fn, args, kw)
        self._delayed_calls.add(dc)
        return dc

    def callFromThread(self, fn, *args, **kw):
        """Schedule ``fn`` to run in the runtime thread (thread-safe)."""
        if self._tearing_down():
            # Refuse cross-thread scheduling once teardown has begun: the call
            # would run after the shutdown snapshot (or into another run).
            return
        self._ensure_loop().call_soon_threadsafe(lambda: fn(*args, **kw))

    def callInThread(self, fn, *args, **kw):
        """Run ``fn`` in a worker thread; return a Deferred with the result."""
        deferred = Deferred()
        if self._tearing_down():
            # Refuse new executor work during teardown, but FIRE the Deferred so
            # an awaiter resolves instead of deadlocking shutdown: errback with
            # ReactorNotRunning rather than handing back an inert (never-fired)
            # Deferred. No job is submitted and the loop is not touched, so a
            # closed loop is safe (design doc point 4).
            deferred.errback(ReactorNotRunning(
                "callInThread refused: runtime is shutting down"))
            return deferred
        loop = self._ensure_loop()
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

    # -- background tasks ------------------------------------------------- #
    def track_task(self, task, fatal=False):
        """Adopt an already-scheduled asyncio Task into the runtime registry.

        Gives a module (or internal code) a single place to hand off a
        background task so it is (a) held by a strong reference -- neither GC'd
        early nor leaked past shutdown -- and (b) governed by the owned-task
        exception policy (design doc point 4). On completion the task's exception
        is always *retrieved*, so Python never emits "task exception was never
        retrieved"; but retrieval does not erase a real error:

          - a task marked ``fatal`` stores the FIRST failure on ``_failure`` and
            stops the runtime (drives completion -> ordered shutdown -> re-raise
            through ``_main``);
          - a non-fatal task's exception is dispatched to
            ``loop.call_exception_handler`` (reported, not swallowed), preserving
            visibility while keeping task hygiene.

        A task adopted after teardown has begun is cancelled immediately (it must
        not survive the shutdown snapshot), but the exception policy is installed
        *first*: a task that has already failed by the time it is handed off still
        has its exception retrieved and reported (a late cancel does not overwrite
        an exception that is already set), and a still-running task is cancelled
        and drained through the same callback. The task is not added to ``_tasks``
        (the snapshot is closed) but is returned so the caller can await it.
        """
        if self._tearing_down():
            if task.done():
                # Already finished: retrieve the exception SYNCHRONOUSLY. A
                # done-callback would be scheduled through the task's loop
                # (loop.call_soon), which raises if that loop is already closed
                # (the STOPPED-after-close case). Doing it inline retrieves +
                # reports an already-failed task safely with no loop work.
                self._retrieve_task_exception(task, fatal)
                return task
            # Still pending: cancel it (it must not survive the snapshot) and
            # drain the cancellation through a done-callback.
            if not task.get_loop().is_closed():
                task.add_done_callback(
                    lambda t: self._retrieve_task_exception(t, fatal))
                task.cancel()
                return task
            # Pending on an ALREADY-CLOSED loop: the task can neither advance
            # nor be cancelled/drained (touching a closed loop raises), so it
            # would be silently left alive past the run. That is a runtime-
            # ownership violation -- the ordered _shutdown must cancel/drain all
            # owned tasks *before* the loop closes -- so reject it loudly rather
            # than leak it.
            raise RuntimeError(
                "track_task: cannot adopt a still-pending task on a closed "
                "loop; owned tasks must be drained before loop closure")
        self._tasks.add(task)

        def _done(t):
            self._tasks.discard(t)
            self._retrieve_task_exception(t, fatal)

        task.add_done_callback(_done)
        return task

    def _retrieve_task_exception(self, task, fatal):
        """Retrieve an owned task's exception and apply the exception policy.

        Always called from a task done-callback so Python never emits "task
        exception was never retrieved". A cancelled task carries no error. A
        real exception on a ``fatal`` task records the FIRST failure and stops
        the runtime; on a non-fatal task it is dispatched to
        ``loop.call_exception_handler`` (reported, not swallowed).
        """
        if task.cancelled():
            return
        exc = task.exception()
        if exc is None:
            return
        if fatal:
            self._fatal(exc)
        else:
            self._ensure_loop().call_exception_handler({
                'message': 'runtime-tracked background task raised',
                'exception': exc,
                'task': task,
            })

    def create_task(self, coro, fatal=False):
        """Schedule ``coro`` as a runtime-owned background task.

        Convenience wrapper over ``track_task``: schedules the coroutine on the
        runtime's loop and adopts the resulting task under the same retention +
        exception policy. Prefer this over a bare ``asyncio.create_task`` for
        module background work so the task is centrally torn down at shutdown.

        Refused during teardown: the gate is checked BEFORE scheduling (once
        STOPPED the loop may be closed, so ``ensure_future`` would raise), the
        coroutine is closed so it is not left un-awaited, and ``None`` is
        returned -- no task was created to hand back.
        """
        if self._tearing_down():
            coro.close()
            return None
        loop = self._ensure_loop()
        return self.track_task(asyncio.ensure_future(coro, loop=loop),
                               fatal=fatal)

    # -- async server helpers --------------------------------------------- #
    def addStartupBind(self, make_coro, apply=None, label='startup'):
        """Bind during the awaited pre-run startup phase.

        ``make_coro`` is a **coroutine factory** (a zero-arg callable returning a
        fresh coroutine, e.g. an ``async def start`` method passed unbound-called
        as ``self._start``). If the runtime is already running the factory is
        invoked and scheduled immediately; otherwise it is enqueued and awaited
        by start_all's startup drain, so a failure (e.g. an aiohttp bind that
        cannot claim its port) is fatal and surfaces out of the run. Queuing a
        factory (not a live coroutine) keeps an un-run entry inert -- discarded,
        not leaked, if startup never reaches it. ``apply(result)`` receives the
        coroutine's result. This lets transport-agnostic servers (aiohttp, etc.)
        reuse the same awaited-startup + failure-surfacing path as listenTCP.
        """
        self._ensure_loop()
        if self._tearing_down():
            # Refuse a bind requested during teardown: the factory is never
            # invoked, so nothing is scheduled and no coroutine is created.
            return
        self._register_bind(make_coro, apply or (lambda result: None), None,
                            label)

    def addAsyncCleanup(self, cleanup):
        """Register a zero-arg callable returning an awaitable, run at shutdown.

        Used for resources whose teardown is asynchronous (e.g. aiohttp's
        ``AppRunner.cleanup``). Cleanups run before straggler-task cancellation.
        """
        if self._tearing_down():
            # The async-cleanup phase has already run (or is running) its
            # snapshot; a cleanup registered this late must never be deferred to
            # the next run, so reject it rather than append (design doc point 4).
            return
        self._async_cleanups.append(cleanup)

    # -- networking ------------------------------------------------------- #
    def listenUDP(self, port, protocol, interface='', maxPacketSize=8192):
        """Listen for UDP datagrams, driving ``protocol`` (a DatagramProtocol).

        The socket is bound *synchronously* (as Twisted's listenUDP does) and
        ``protocol.transport`` is installed before returning, so a pluggable
        module can send its first datagram on the next line without racing the
        asyncio endpoint-creation coroutine (review finding: strict-RTP
        fixtures). A bind failure therefore also surfaces synchronously here,
        matching Twisted's ``CannotListenError``. The asyncio receive path is
        then wired from the same bound socket; when it is ready,
        ``DatagramProtocol.connection_made`` upgrades ``protocol.transport`` to
        the asyncio-backed writer.
        """
        import socket as _socket

        loop = self._ensure_loop()
        handle = _Port()
        if self._tearing_down():
            # Opened during teardown: bind nothing, return a closed handle.
            handle._closed = True
            return handle
        self._ports.append(handle)

        sock = _socket.socket(_socket.AF_INET, _socket.SOCK_DGRAM)
        try:
            sock.setblocking(False)
            sock.bind((interface or '0.0.0.0', port))
        except OSError:
            sock.close()
            self._ports.remove(handle)
            raise
        handle._presock = sock
        # Give the protocol a working transport immediately (Twisted parity).
        if getattr(protocol, 'transport', None) is None:
            protocol.transport = _SyncDatagramTransport(sock)

        async def _bind():
            # stopListening() may have run before this coroutine gets its turn
            # (it closes _presock). Binding a closed fd raises EBADF, so honour
            # an early teardown by doing nothing -- the socket is already gone.
            if handle._closed:
                return None
            return await loop.create_datagram_endpoint(lambda: protocol,
                                                       sock=sock)

        def apply(result):
            if result is None:
                return
            transport, _proto = result
            handle._set_transport(transport)

        self._register_bind(_bind, apply, None, 'listenUDP:%d' % port)
        return handle

    def listenTCP(self, port, factory, backlog=50, interface=''):
        """Listen for TCP connections, building protocols from ``factory``."""
        loop = self._ensure_loop()
        handle = _Port()
        if self._tearing_down():
            # Opened during teardown: bind nothing, return a closed handle.
            handle._closed = True
            return handle
        self._ports.append(handle)

        def _bind():
            return loop.create_server(lambda: _TwistedProtocolAdapter(factory),
                                      interface or '0.0.0.0', port,
                                      backlog=backlog)

        def apply(server):
            handle._set_server(server)

        self._register_bind(_bind, apply, None, 'listenTCP:%d' % port)
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
        if self._tearing_down():
            # Requested during teardown: hand back a stopped connector that
            # never attempts a connection and is not tracked for cleanup.
            connector._stopped = True
            return connector
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
        from .protocols import _PendingProcessTransport

        loop = self._ensure_loop()
        rest = tuple(args[1:]) if args else ()

        # Twisted installs protocol.transport synchronously; asyncio's
        # subprocess_exec is a coroutine. Give the protocol a placeholder now so
        # a signal/kill issued before connection_made (e.g. a SIPp scenario
        # killed from an AMI event) is buffered and replayed, instead of hitting
        # protocol.transport is None.
        if getattr(processProtocol, 'transport', None) is None:
            processProtocol.transport = _PendingProcessTransport()

        if self._tearing_down():
            # Requested during teardown: spawn nothing and register nothing.
            # The connector proxies the (never-connected) placeholder transport.
            return _ProcessConnector(processProtocol)

        def _bind():
            return loop.subprocess_exec(lambda: processProtocol,
                                        executable, *rest,
                                        env=env, cwd=path)

        def apply(result):
            transport, _proto = result
            # connection_made runs before subprocess_exec resolves, so the
            # protocol now owns the non-reaping process transport adapter.
            self._process_transports.append(processProtocol.transport)

        self._register_bind(_bind, apply, None, 'spawnProcess:%s' % executable)
        return _ProcessConnector(processProtocol)

    # -- internal bind scheduling ----------------------------------------- #
    def _register_bind(self, make_coro, apply, on_error, label):
        """Bind an endpoint now (if RUNNING) or during startup (via start_all).

        ``make_coro`` is a **coroutine factory** -- a zero-arg callable that
        returns a fresh coroutine when the bind is actually driven. Queuing
        factories (never bare coroutine *objects*) keeps an un-run startup entry
        inert: if construction fails, or a bind aborts, the factories left on
        the queue are simply discarded at shutdown -- nothing was created, so
        nothing can emit "coroutine was never awaited" (design doc point 2b).

        Classification is state-based: only while RUNNING is a bind turned into
        a tracked mid-run task (the factory is invoked now); during COLLECTING or
        STARTING the factory is enqueued and driven by start_all's startup drain.
        ``apply(result)`` installs the bound transport/server/connection.
        ``on_error(exc)`` handles a bind failure; when None, the failure is fatal
        and stops the runtime (and aborts the pre-run startup drain).
        """
        if self._tearing_down():
            # Defensive chokepoint: a bind reaching here during teardown (e.g. a
            # reconnect attempt driven by a late callback) is refused outright.
            # Nothing is scheduled and nothing is queued, so no task can survive
            # the shutdown snapshot or repopulate the next run's registries. The
            # public entry points already guard, so this is belt-and-braces.
            return
        if self._state == _RuntimeState.RUNNING:
            task = asyncio.ensure_future(make_coro())
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
            self._pending_binds.append((make_coro, apply, on_error, label))

    def _fatal(self, exc):
        """Record a fatal mid-run error and stop the runtime."""
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


# ---------------------------------------------------------------------------- #
# Current-runtime holder
# ---------------------------------------------------------------------------- #
# There is no permanent runtime. Ownership is per-run: the native asyncio.run()
# entrypoint (later B1 steps) creates a fresh AsyncTestRuntime, installs it as
# the current runtime for the duration of the run, and detaches it on shutdown;
# the reactor facade resolves the current runtime dynamically on every call, and
# lazily installs one for legacy callers that never went through the entrypoint.
# This keeps each run's registries, timers, and completion signal isolated from
# the next, instead of mutating a single shared object across runs.
_current_runtime = None


def current_runtime():
    """Return the installed current runtime, lazily creating one if none.

    Used by the reactor facade so ``reactor.<call>`` always resolves to whatever
    runtime is currently installed. Legacy code that calls the facade without an
    entrypoint having installed a runtime gets one created on first use.
    """
    global _current_runtime
    if _current_runtime is None:
        _current_runtime = AsyncTestRuntime()
    return _current_runtime


def install_runtime(rt):
    """Install ``rt`` as the current runtime, returning it.

    The native entrypoint calls this with a freshly constructed runtime before
    driving it; test fixtures call it (via ``new_runtime``) to give each test an
    isolated owner.

    Refuses to silently orphan the runtime already in the holder: installation
    is allowed only when the holder is empty, the same object is being
    reinstalled, or the incumbent is idle *and* owns no resources. Superseding a
    runtime that is mid-run (active) raises ``ReactorAlreadyRunning``; superseding
    an idle-but-resource-owning runtime raises ``RuntimeError`` naming the live
    registries. Callers must ``detach_runtime()`` (after the ordered shutdown)
    before installing a replacement.
    """
    global _current_runtime
    incumbent = _current_runtime
    if incumbent is not None and incumbent is not rt:
        if incumbent.is_active():
            raise ReactorAlreadyRunning(
                "cannot install a runtime while the current one is active "
                "(state=%s); detach it after shutdown first"
                % incumbent._state.value)
        leaked = incumbent.live_resources()
        if leaked:
            raise RuntimeError(
                "cannot install a runtime while the current one still owns "
                "resources (%s); run its shutdown and detach it first"
                % ', '.join(leaked))
    _current_runtime = rt
    return rt


def new_runtime(loop=None):
    """Create a fresh runtime, optionally bind ``loop``, install and return it."""
    rt = AsyncTestRuntime()
    if loop is not None:
        rt._loop = loop
    return install_runtime(rt)


def detach_runtime(rt=None):
    """Detach the current runtime so the next access installs a fresh one.

    With no argument, unconditionally clears the holder. With ``rt`` given, only
    clears it if ``rt`` is still the installed runtime (so a late detach from a
    superseded run does not clobber a newer one).
    """
    global _current_runtime
    if rt is None or rt is _current_runtime:
        _current_runtime = None


def get_current_runtime():
    """Return the installed runtime without creating one (may be ``None``)."""
    return _current_runtime
