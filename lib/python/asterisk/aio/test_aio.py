"""Unit tests for the asterisk.aio compatibility layer.

Covers the Twisted semantics the test suite relies on:
  * Deferred callback/errback threading and branch-switching
  * late-added callbacks (added after the Deferred has fired)
  * AlreadyCalledError on double-fire
  * DeferredList result shaping, fireOnOne*, and consumeErrors
  * maybeDeferred wrapping of plain values, Failures, and raised exceptions
  * Failure.check/trap
  * reactor.callLater / _DelayedCall.cancel
  * a UDP echo round-trip through the DatagramProtocol adapter
  * a subprocess exit through the ProcessProtocol adapter

Run with:  python3 -m unittest asterisk.aio.test_aio
"""

import asyncio
import logging
import signal
import sys
import unittest
from unittest import mock

from asterisk.aio import defer, reactor
from asterisk.aio.defer import (
    Deferred, DeferredList, gatherResults, maybeDeferred, succeed, fail,
    AlreadyCalledError, TimeoutError as DeferTimeoutError,
)
from asterisk.aio.failure import Failure
from asterisk.aio.protocols import (
    DatagramProtocol, ProcessProtocol, ProcessDone, ProcessTerminated,
    Protocol, Factory, ClientFactory, _ProcessTransportAdapter,
)


class _LoopTestCase(unittest.TestCase):
    """Base class giving each test a fresh event loop bound to the reactor."""

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        reactor._loop = self.loop
        reactor.running = False
        reactor._when_running = []
        reactor._pending_binds = []
        reactor._stop_future = None
        reactor._failure = None
        reactor._delayed_calls = set()
        reactor._tasks = set()
        reactor._ports = []
        reactor._connectors = []
        reactor._process_transports = []

    def tearDown(self):
        if not self.loop.is_closed():
            self.loop.close()
        asyncio.set_event_loop(None)


# --------------------------------------------------------------------------- #
# Deferred semantics (no running loop required)
# --------------------------------------------------------------------------- #
class DeferredTests(_LoopTestCase):

    def test_callback_threading(self):
        d = Deferred()
        d.addCallback(lambda r: r + 1)
        d.addCallback(lambda r: r * 2)
        seen = []
        d.addCallback(lambda r: seen.append(r) or r)
        d.callback(3)
        self.assertEqual(seen, [(3 + 1) * 2])

    def test_late_callback_runs_immediately(self):
        d = Deferred()
        d.callback(10)
        seen = []
        d.addCallback(lambda r: seen.append(r) or r)
        self.assertEqual(seen, [10])

    def test_errback_branch_and_switch_back(self):
        d = Deferred()
        order = []

        def boom(_):
            raise ValueError("boom")

        def handle(failure):
            order.append(('errback', failure.check(ValueError)))
            return "recovered"   # switch back to callback branch

        def after(result):
            order.append(('callback', result))
            return result

        d.addCallback(boom)
        d.addErrback(handle)
        d.addCallback(after)
        d.callback("start")

        self.assertEqual(order, [('errback', ValueError), ('callback', 'recovered')])

    def test_errback_fired_directly(self):
        d = Deferred()
        captured = []
        d.addErrback(lambda f: captured.append(f.getErrorMessage()))
        d.errback(RuntimeError("nope"))
        self.assertEqual(captured, ["nope"])

    def test_already_called(self):
        d = Deferred()
        d.callback(1)
        with self.assertRaises(AlreadyCalledError):
            d.callback(2)
        with self.assertRaises(AlreadyCalledError):
            d.errback(RuntimeError("x"))

    def test_nested_deferred_pauses_chain(self):
        outer = Deferred()
        inner = Deferred()
        order = []

        def returns_inner(_):
            return inner

        def after(result):
            order.append(result)
            return result

        outer.addCallback(returns_inner)
        outer.addCallback(after)
        outer.callback("go")
        # Chain is paused waiting on inner; nothing after it yet.
        self.assertEqual(order, [])
        inner.callback("inner-done")
        self.assertEqual(order, ["inner-done"])

    def test_await_resolves_with_result(self):
        async def coro():
            d = Deferred()
            d.addCallback(lambda r: r + 5)
            d.callback(10)
            return await d
        self.assertEqual(self.loop.run_until_complete(coro()), 15)

    def test_await_raises_on_failure(self):
        async def coro():
            d = Deferred()
            d.errback(ValueError("bad"))
            return await d
        with self.assertRaises(ValueError):
            self.loop.run_until_complete(coro())


class DeferredListTests(_LoopTestCase):

    def test_shaping_all_succeed(self):
        d1, d2 = Deferred(), Deferred()
        dl = DeferredList([d1, d2])
        out = []
        dl.addCallback(out.append)
        d2.callback("b")
        d1.callback("a")
        self.assertEqual(out, [[(True, "a"), (True, "b")]])

    def test_empty_fires_immediately(self):
        out = []
        DeferredList([]).addCallback(out.append)
        self.assertEqual(out, [[]])

    def test_fire_on_one_callback(self):
        d1, d2 = Deferred(), Deferred()
        dl = DeferredList([d1, d2], fireOnOneCallback=True)
        out = []
        dl.addCallback(out.append)
        d2.callback("first")
        self.assertEqual(out, [("first", 1)])

    def test_consume_errors(self):
        d1, d2 = Deferred(), Deferred()
        dl = DeferredList([d1, d2], consumeErrors=True)
        out = []
        dl.addCallback(out.append)
        d1.errback(ValueError("x"))
        d2.callback("ok")
        # First element is the (False, Failure) tuple.
        self.assertFalse(out[0][0][0])
        self.assertIsInstance(out[0][0][1], Failure)
        self.assertEqual(out[0][1], (True, "ok"))
        # consumeErrors means the child Deferred settled cleanly: its chain
        # result was replaced with None, so no Failure dangles on it.
        self.assertTrue(d1.called)
        self.assertFalse(getattr(d1.result, '_is_failure', False))


class MaybeDeferredTests(_LoopTestCase):

    def test_plain_value(self):
        out = []
        maybeDeferred(lambda: 42).addCallback(out.append)
        self.assertEqual(out, [42])

    def test_passes_deferred_through(self):
        d = Deferred()
        self.assertIs(maybeDeferred(lambda: d), d)

    def test_wraps_raised_exception(self):
        out = []

        def boom():
            raise ValueError("kaboom")

        maybeDeferred(boom).addErrback(lambda f: out.append(f.check(ValueError)))
        self.assertEqual(out, [ValueError])


class FailureTests(unittest.TestCase):

    def test_check_and_trap(self):
        f = Failure(ValueError("v"))
        self.assertEqual(f.check(KeyError, ValueError), ValueError)
        self.assertIsNone(f.check(KeyError))
        self.assertEqual(f.trap(ValueError), ValueError)
        with self.assertRaises(ValueError):
            f.trap(KeyError)

    def test_capture_current_exception(self):
        try:
            raise RuntimeError("captured")
        except RuntimeError:
            f = Failure()
        self.assertIs(f.type, RuntimeError)
        self.assertEqual(f.getErrorMessage(), "captured")


# --------------------------------------------------------------------------- #
# reactor scheduling
# --------------------------------------------------------------------------- #
class DelayedCallTests(_LoopTestCase):

    def test_call_later_fires(self):
        out = []
        reactor.callWhenRunning(
            lambda: reactor.callLater(0.01, lambda: (out.append('fired'),
                                                     reactor.stop())))
        reactor.run()
        self.assertEqual(out, ['fired'])

    def test_cancel_prevents_firing(self):
        out = []

        def setup():
            dc = reactor.callLater(0.05, lambda: out.append('should-not'))
            self.assertTrue(dc.active())
            dc.cancel()
            self.assertFalse(dc.active())
            reactor.callLater(0.02, reactor.stop)

        reactor.callWhenRunning(setup)
        reactor.run()
        self.assertEqual(out, [])

    def test_get_time_and_reset(self):
        # TestCase.reset_timeout() calls timeout_id.getTime() and feeds the
        # result to datetime.fromtimestamp(), so getTime() must exist and return
        # a wall-clock POSIX timestamp. A missing getTime() raised AttributeError
        # that the Deferred chain swallowed, silently stalling multi-phase SIPp
        # tests (pjsip hold, stir_shaken, attended_transfer, ...).
        import time as _time
        import datetime as _datetime
        out = {}

        def setup():
            dc = reactor.callLater(30.0, lambda: None)
            t0 = dc.getTime()
            # Sane POSIX timestamp (fromtimestamp must not raise).
            _datetime.datetime.fromtimestamp(t0)
            self.assertAlmostEqual(t0, _time.time() + 30.0, delta=2.0)
            dc.reset(60.0)
            t1 = dc.getTime()
            _datetime.datetime.fromtimestamp(t1)
            self.assertGreater(t1, t0)
            dc.cancel()
            out['ok'] = True
            reactor.stop()

        reactor.callWhenRunning(setup)
        reactor.run()
        self.assertTrue(out.get('ok'))


# --------------------------------------------------------------------------- #
# UDP echo through the DatagramProtocol adapter
# --------------------------------------------------------------------------- #
class _EchoClient(DatagramProtocol):
    def __init__(self, server_port, result, timeout_dc_holder):
        self._server_port = server_port
        self._result = result
        self._holder = timeout_dc_holder

    def startProtocol(self):
        self.transport.write(b'ping', ('127.0.0.1', self._server_port))

    def datagramReceived(self, data, addr):
        self._result['data'] = data
        if self._holder['dc'].active():
            self._holder['dc'].cancel()
        for port in self._holder.get('ports', []):
            port.stopListening()
        reactor.stop()


class _EchoServer(DatagramProtocol):
    def __init__(self, result, holder):
        self._result = result
        self._holder = holder

    def startProtocol(self):
        port = self.transport.getHost()[1]
        client = _EchoClient(port, self._result, self._holder)
        handle = reactor.listenUDP(0, client, '127.0.0.1')
        self._holder.setdefault('ports', []).append(handle)

    def datagramReceived(self, data, addr):
        self.transport.write(b'echo:' + data, addr)


class UDPEchoTests(_LoopTestCase):

    def test_round_trip(self):
        result = {}
        holder = {}

        def setup():
            holder['dc'] = reactor.callLater(3.0, reactor.stop)  # safety net
            holder['ports'] = []
            server = _EchoServer(result, holder)
            handle = reactor.listenUDP(0, server, '127.0.0.1')
            holder['ports'].append(handle)

        reactor.callWhenRunning(setup)
        reactor.run()
        self.assertEqual(result.get('data'), b'echo:ping')


class UDPSyncTransportTests(_LoopTestCase):
    """listenUDP must install a usable transport *synchronously*.

    Twisted's ``reactor.listenUDP`` binds the socket and installs
    ``protocol.transport`` before returning, so fixtures may write a datagram on
    the very next line. The strict-RTP fixtures rely on this: they call
    ``listenUDP(port, proto)`` and immediately ``proto.transport.write(...)``.
    The asyncio ``create_datagram_endpoint`` is a coroutine that binds later, so
    listenUDP pre-binds the socket and installs a synchronous transport to close
    the race. This test asserts that guarantee directly.
    """

    def test_transport_usable_before_endpoint_awaited(self):
        observed = {}

        def setup():
            proto = DatagramProtocol()
            handle = reactor.listenUDP(0, proto, '127.0.0.1')
            # Synchronously — no await, no callLater — the transport must exist
            # and expose a bound host and a working write().
            observed['transport'] = proto.transport
            try:
                observed['host'] = proto.transport.getHost()
                proto.transport.write(b'x', ('127.0.0.1', observed['host'][1]))
                observed['wrote'] = True
            except Exception as exc:                       # pragma: no cover
                observed['error'] = repr(exc)
            handle.stopListening()
            reactor.stop()

        reactor.callWhenRunning(setup)
        reactor.run()
        self.assertIsNotNone(observed.get('transport'),
                             'transport was None synchronously')
        self.assertIsInstance(observed.get('host'), tuple)
        self.assertEqual(observed['host'][0], '127.0.0.1')
        self.assertNotEqual(observed['host'][1], 0)
        self.assertTrue(observed.get('wrote'),
                        'synchronous write failed: %s' % observed.get('error'))


# --------------------------------------------------------------------------- #
# Subprocess through the ProcessProtocol adapter
# --------------------------------------------------------------------------- #
class _CollectingProcess(ProcessProtocol):
    # Deliberately does NOT call super().__init__(), mirroring AsteriskProtocol
    # and SIPpProtocol, to prove the adapter works without it (review finding 1).
    def __init__(self, record):
        self._record = record

    def outReceived(self, data):
        self._record.setdefault('out', b'')
        self._record['out'] += data

    def processEnded(self, reason):
        self._record['reason'] = reason
        reactor.stop()


class SubprocessTests(_LoopTestCase):

    def test_clean_exit(self):
        record = {}
        script = "import sys; sys.stdout.write('hi'); sys.stdout.flush()"

        def setup():
            proto = _CollectingProcess(record)
            reactor.spawnProcess(proto, sys.executable,
                                 [sys.executable, '-c', script])
            reactor.callLater(5.0, reactor.stop)  # safety net

        reactor.callWhenRunning(setup)
        reactor.run()

        self.assertEqual(record.get('out'), b'hi')
        reason = record.get('reason')
        self.assertIsInstance(reason, Failure)
        self.assertEqual(reason.check(ProcessDone), ProcessDone)
        self.assertEqual(reason.value.exitCode, 0)

    def test_nonzero_exit(self):
        record = {}
        script = "import sys; sys.exit(3)"

        def setup():
            proto = _CollectingProcess(record)
            reactor.spawnProcess(proto, sys.executable,
                                 [sys.executable, '-c', script])
            reactor.callLater(5.0, reactor.stop)  # safety net

        reactor.callWhenRunning(setup)
        reactor.run()

        reason = record.get('reason')
        self.assertIsInstance(reason, Failure)
        self.assertEqual(reason.check(ProcessTerminated), ProcessTerminated)
        self.assertEqual(reason.value.exitCode, 3)


class SubprocessSyncKillTests(_LoopTestCase):
    """spawnProcess must accept a signal/kill issued *synchronously*.

    Twisted wires ``protocol.transport`` inside ``spawnProcess``; the suite kills
    scenarios on the next line (SIPp is killed straight from an AMI event
    handler). asyncio's ``subprocess_exec`` is a coroutine, so without a
    synchronous placeholder ``protocol.transport`` is ``None`` at that point and
    ``self.transport.signalProcess('KILL')`` raises ``AttributeError``. This
    reproduces that exact call ordering and asserts the child is actually
    killed, not that the call silently blew up.
    """

    def test_kill_before_connection_made_is_replayed(self):
        record = {}
        watcher_warnings = []

        class _WarningCapture(logging.Handler):
            def emit(self, log_record):
                if 'exit status already read' in log_record.getMessage():
                    watcher_warnings.append(log_record.getMessage())

        warning_capture = _WarningCapture()
        asyncio_logger = logging.getLogger('asyncio')
        asyncio_logger.addHandler(warning_capture)
        # A child that would run for a long time unless it is killed.
        script = "import time; time.sleep(30)"

        def setup():
            proto = _CollectingProcess(record)
            reactor.spawnProcess(proto, sys.executable,
                                 [sys.executable, '-c', script])
            # Synchronously, before connection_made: transport must exist and
            # accept a kill (mirrors SIPpProtocol.kill()).
            record['transport_sync'] = proto.transport
            proto.transport.signalProcess('KILL')
            reactor.callLater(10.0, reactor.stop)  # safety net

        try:
            reactor.callWhenRunning(setup)
            reactor.run()
        finally:
            asyncio_logger.removeHandler(warning_capture)

        self.assertIsNotNone(record.get('transport_sync'),
                             'transport was None synchronously after spawn')
        reason = record.get('reason')
        self.assertIsInstance(reason, Failure)
        # Killed by SIGKILL -> ProcessTerminated with a signal, not a timeout.
        self.assertEqual(reason.check(ProcessTerminated), ProcessTerminated)
        self.assertEqual(reason.value.signal, signal.SIGKILL)
        self.assertEqual(watcher_warnings, [])

    def test_pidfd_signal_does_not_call_popen_transport(self):
        """The Linux signalling path must not invoke Popen.send_signal/poll."""
        class _FakeTransport(object):
            def __init__(self):
                self.returncode = None
                self.raw_signals = []

            def get_pid(self):
                return 12345

            def get_returncode(self):
                return self.returncode

            def get_pipe_transport(self, fd):
                return None

            def send_signal(self, sig):
                self.raw_signals.append(sig)

            def close(self):
                pass

        async def exercise():
            raw = _FakeTransport()
            with mock.patch('asterisk.aio.protocols.os.pidfd_open',
                            return_value=99), \
                    mock.patch(
                        'asterisk.aio.protocols._signal.pidfd_send_signal') \
                    as pidfd_send, \
                    mock.patch('asterisk.aio.protocols.os.close') as close:
                adapter = _ProcessTransportAdapter(raw)
                adapter.signalProcess('KILL')
                pidfd_send.assert_called_once_with(
                    99, signal.SIGKILL, None, 0)
                self.assertEqual(raw.raw_signals, [])

                raw.returncode = -signal.SIGKILL
                adapter.processExited()
                self.assertEqual(await adapter.waitForExit(),
                                 -signal.SIGKILL)
                close.assert_called_once_with(99)

        self.loop.run_until_complete(exercise())

    def test_posix_fallback_does_not_call_popen_transport(self):
        """Without pidfds, POSIX signalling must still avoid Popen.poll."""
        class _FakeTransport(object):
            def __init__(self):
                self.raw_signals = []

            def get_pid(self):
                return 12345

            def get_returncode(self):
                return None

            def send_signal(self, sig):
                self.raw_signals.append(sig)

        async def exercise():
            raw = _FakeTransport()
            with mock.patch('asterisk.aio.protocols.os.pidfd_open',
                            side_effect=OSError), \
                    mock.patch('asterisk.aio.protocols.os.kill') as os_kill:
                adapter = _ProcessTransportAdapter(raw)
                adapter.signalProcess('TERM')
                os_kill.assert_called_once_with(12345, signal.SIGTERM)
                self.assertEqual(raw.raw_signals, [])
                adapter._exit_waiter.cancel()

        self.loop.run_until_complete(exercise())

    def test_reactor_shutdown_reaps_live_process_without_warning(self):
        """Shutdown waits for its child watcher instead of Popen.poll reaping."""
        record = {}
        watcher_warnings = []

        class _WarningCapture(logging.Handler):
            def emit(self, log_record):
                if 'exit status already read' in log_record.getMessage():
                    watcher_warnings.append(log_record.getMessage())

        warning_capture = _WarningCapture()
        asyncio_logger = logging.getLogger('asyncio')
        asyncio_logger.addHandler(warning_capture)

        def setup():
            proto = _CollectingProcess(record)
            reactor.spawnProcess(
                proto, sys.executable,
                [sys.executable, '-c', 'import time; time.sleep(30)'])
            reactor.callLater(0.1, reactor.stop)

        try:
            reactor.callWhenRunning(setup)
            reactor.run()
        finally:
            asyncio_logger.removeHandler(warning_capture)

        reason = record.get('reason')
        self.assertIsInstance(reason, Failure)
        self.assertEqual(reason.check(ProcessTerminated), ProcessTerminated)
        self.assertEqual(reason.value.signal, signal.SIGTERM)
        self.assertEqual(watcher_warnings, [])


# --------------------------------------------------------------------------- #
# Late callback + await (review finding 3)
# --------------------------------------------------------------------------- #
class LateCallbackAwaitTests(_LoopTestCase):

    def test_late_callback_visible_to_await(self):
        async def scenario():
            d = Deferred()
            d.callback(1)
            d.addCallback(lambda v: v + 1)   # added AFTER firing
            return await d
        # The Future-subclass prototype returned 1 here; the wrapper returns 2.
        self.assertEqual(self.loop.run_until_complete(scenario()), 2)

    def test_called_property(self):
        d = Deferred()
        self.assertFalse(d.called)
        d.callback(1)
        self.assertTrue(d.called)


# --------------------------------------------------------------------------- #
# Cross-shim chaining and pausing on Futures/coroutines (review finding 4)
# --------------------------------------------------------------------------- #
class _ForeignDeferred(object):
    """Minimal stand-in for a *different* shim's Deferred (e.g. starpy's).

    Exposes only addBoth/callback/errback, so it is recognised purely by duck
    typing, and routes failures using the shared ``_is_failure`` marker.
    """

    def __init__(self):
        self._cbs = []
        self._fired = False
        self._result = None

    def addBoth(self, fn):
        self._cbs.append(fn)
        if self._fired:
            self._drain()
        return self

    def callback(self, result):
        self._fire(result)

    def errback(self, failure):
        self._fire(failure)

    def _fire(self, result):
        self._fired = True
        self._result = result
        self._drain()

    def _drain(self):
        while self._cbs:
            fn = self._cbs.pop(0)
            self._result = fn(self._result)


class CrossShimTests(_LoopTestCase):

    def test_pause_on_foreign_deferred_success(self):
        foreign = _ForeignDeferred()
        d = Deferred()
        out = []
        d.addCallback(lambda r: foreign)            # return a foreign deferred
        d.addCallback(lambda r: out.append(r) or r)
        d.callback('go')
        self.assertEqual(out, [])                   # paused on foreign
        foreign.callback('foreign-result')
        self.assertEqual(out, ['foreign-result'])

    def test_pause_on_foreign_deferred_failure(self):
        foreign = _ForeignDeferred()
        d = Deferred()
        out = []
        d.addCallback(lambda r: foreign)
        d.addErrback(lambda f: out.append(f.check(ValueError)) or 'handled')
        d.callback('go')
        foreign.errback(Failure(ValueError('x')))   # foreign Failure, marker set
        self.assertEqual(out, [ValueError])

    def test_pause_on_future(self):
        async def producer():
            return 'fut-done'
        out = []
        d = Deferred()
        d.addCallback(lambda r: asyncio.ensure_future(producer()))
        d.addCallback(lambda r: out.append(r) or r)
        d.callback('x')
        self.loop.run_until_complete(asyncio.sleep(0.02))
        self.assertEqual(out, ['fut-done'])

    def test_pause_on_coroutine(self):
        async def producer():
            return 'coro-done'
        out = []
        d = Deferred()
        d.addCallback(lambda r: producer())         # raw coroutine
        d.addCallback(lambda r: out.append(r) or r)
        d.callback('x')
        self.loop.run_until_complete(asyncio.sleep(0.02))
        self.assertEqual(out, ['coro-done'])


# --------------------------------------------------------------------------- #
# succeed / fail / gatherResults / TimeoutError helpers
# --------------------------------------------------------------------------- #
class HelperTests(_LoopTestCase):

    def test_succeed_and_fail(self):
        out = []
        succeed(7).addCallback(out.append)
        self.assertEqual(out, [7])
        errs = []
        fail(Failure(ValueError('z'))).addErrback(lambda f: errs.append(f.check(ValueError)))
        self.assertEqual(errs, [ValueError])

    def test_gather_results(self):
        d1, d2 = Deferred(), Deferred()
        out = []
        gatherResults([d1, d2]).addCallback(out.append)
        d1.callback('a')
        d2.callback('b')
        self.assertEqual(out, [['a', 'b']])

    def test_timeout_error_is_exception(self):
        self.assertTrue(issubclass(DeferTimeoutError, Exception))


# --------------------------------------------------------------------------- #
# Listener bind-failure propagation (review finding 6)
# --------------------------------------------------------------------------- #
class BindFailureTests(_LoopTestCase):

    def test_prerun_spawn_failure_propagates(self):
        # Registered before run(): the awaited startup phase must surface the
        # bind error out of run() rather than swallowing it in a task.
        proto = ProcessProtocol()
        bad = '/nonexistent/binary/definitely-not-here'
        reactor.spawnProcess(proto, bad, [bad])
        with self.assertRaises(FileNotFoundError):
            reactor.run()
        self.assertFalse(reactor.running)

    def test_running_spawn_failure_propagates(self):
        # Registered while running (via callWhenRunning): a fatal bind error
        # stops the reactor and is re-raised by run().
        proto = ProcessProtocol()
        bad = '/nonexistent/binary/definitely-not-here'

        def setup():
            reactor.spawnProcess(proto, bad, [bad])
            reactor.callLater(5.0, reactor.stop)  # safety net

        reactor.callWhenRunning(setup)
        with self.assertRaises(FileNotFoundError):
            reactor.run()
        self.assertFalse(reactor.running)


# --------------------------------------------------------------------------- #
# Subprocess pipe-drain ordering (review finding 1)
# --------------------------------------------------------------------------- #
class _BulkCollectingProcess(ProcessProtocol):
    def __init__(self, record):
        self._record = record
        self._record['out'] = b''

    def outReceived(self, data):
        self._record['out'] += data

    def processEnded(self, reason):
        self._record['reason'] = reason
        reactor.stop()


class PipeDrainTests(_LoopTestCase):

    def test_all_stdout_captured_despite_early_exit(self):
        record = {}
        # Write a large payload then exit immediately; process_exited may be
        # delivered before the final stdout chunks. All bytes must still arrive.
        script = ("import sys; sys.stdout.buffer.write(b'x'*200000); "
                  "sys.stdout.flush()")

        def setup():
            proto = _BulkCollectingProcess(record)
            reactor.spawnProcess(proto, sys.executable,
                                 [sys.executable, '-c', script])
            reactor.callLater(5.0, reactor.stop)  # safety net

        reactor.callWhenRunning(setup)
        reactor.run()
        self.assertEqual(len(record.get('out', b'')), 200000)
        self.assertEqual(record['reason'].check(ProcessDone), ProcessDone)


# --------------------------------------------------------------------------- #
# callInThread returns a Deferred with the worker result
# --------------------------------------------------------------------------- #
class CallInThreadTests(_LoopTestCase):

    def test_result_delivered(self):
        record = {}

        def setup():
            d = reactor.callInThread(lambda: 21 * 2)
            d.addCallback(lambda r: (record.__setitem__('r', r), reactor.stop()))
            reactor.callLater(5.0, reactor.stop)  # safety net

        reactor.callWhenRunning(setup)
        reactor.run()
        self.assertEqual(record.get('r'), 42)


# --------------------------------------------------------------------------- #
# TCP client: clientConnectionLost notification + reconnect (review blocker 1)
# --------------------------------------------------------------------------- #
class _DroppingServerProto(object):
    """Server-side protocol that drops every connection as soon as it opens."""

    def makeConnection(self, transport):
        self.transport = transport
        transport.loseConnection()

    def dataReceived(self, data):
        pass

    def connectionLost(self, reason):
        pass


class _DroppingServerFactory(object):
    def buildProtocol(self, addr):
        return _DroppingServerProto()


class _ClientProto(object):
    def makeConnection(self, transport):
        self.transport = transport

    def dataReceived(self, data):
        pass

    def connectionLost(self, reason):
        pass


class _RecordingClientFactory(object):
    """Minimal ReconnectingClientFactory-style client factory."""

    def __init__(self, events, reconnect_once=False):
        self.events = events
        self.reconnect_once = reconnect_once
        self._reconnected = False

    def startedConnecting(self, connector):
        self.events.append('connecting')

    def buildProtocol(self, addr):
        return _ClientProto()

    def clientConnectionLost(self, connector, reason):
        self.events.append('lost')
        if self.reconnect_once and not self._reconnected:
            self._reconnected = True
            connector.connect()          # retry, like ReconnectingClientFactory
        else:
            reactor.stop()

    def clientConnectionFailed(self, connector, reason):
        self.events.append(('failed', reason))
        reactor.stop()

    def stopTrying(self):
        self.events.append('stopTrying')


def _server_port(handle):
    return handle._server.sockets[0].getsockname()[1]


class TCPClientNotificationTests(_LoopTestCase):

    def test_connection_lost_notifies_factory(self):
        events = []
        server_handle = reactor.listenTCP(0, _DroppingServerFactory(),
                                          interface='127.0.0.1')

        def setup():
            port = _server_port(server_handle)
            factory = _RecordingClientFactory(events)
            reactor.connectTCP('127.0.0.1', port, factory)
            reactor.callLater(5.0, reactor.stop)  # safety net

        reactor.callWhenRunning(setup)
        reactor.run()
        self.assertIn('lost', events)

    def test_reconnect_via_retry(self):
        events = []
        server_handle = reactor.listenTCP(0, _DroppingServerFactory(),
                                          interface='127.0.0.1')

        def setup():
            port = _server_port(server_handle)
            factory = _RecordingClientFactory(events, reconnect_once=True)
            reactor.connectTCP('127.0.0.1', port, factory)
            reactor.callLater(5.0, reactor.stop)  # safety net

        reactor.callWhenRunning(setup)
        reactor.run()
        # connecting -> lost -> (retry) connecting -> lost
        self.assertEqual(events.count('lost'), 2)
        self.assertEqual(events.count('connecting'), 2)


# --------------------------------------------------------------------------- #
# connectTCP honors timeout and bindAddress (review blocker 2)
# --------------------------------------------------------------------------- #
class _AcceptingServerProto(object):
    def makeConnection(self, transport):
        self.transport = transport
        # Record done; drop immediately so neither side leaks a socket.
        transport.loseConnection()

    def dataReceived(self, data):
        pass

    def connectionLost(self, reason):
        pass


class _AcceptingServerFactory(object):
    def __init__(self, peers):
        self._peers = peers

    def buildProtocol(self, addr):
        self._peers.append(addr)
        return _AcceptingServerProto()


class ConnectTCPOptionsTests(_LoopTestCase):

    def test_bind_address_is_used(self):
        peers = []
        server_handle = reactor.listenTCP(0, _AcceptingServerFactory(peers),
                                          interface='127.0.0.1')

        def setup():
            port = _server_port(server_handle)
            factory = _RecordingClientFactory([])
            # Bind the client socket to loopback explicitly; the server should
            # see a 127.0.0.1 peer, proving local_addr was applied.
            reactor.connectTCP('127.0.0.1', port, factory,
                               bindAddress=('127.0.0.1', 0))
            reactor.callLater(0.3, reactor.stop)

        reactor.callWhenRunning(setup)
        reactor.run()
        self.assertTrue(peers)
        self.assertEqual(peers[0][0], '127.0.0.1')

    def test_timeout_triggers_connection_failed(self):
        events = []

        def setup():
            factory = _RecordingClientFactory(events)
            # 192.0.2.0/24 is TEST-NET-1 (RFC 5737), guaranteed unreachable; a
            # short timeout must surface as clientConnectionFailed, not a hang.
            reactor.connectTCP('192.0.2.1', 9, factory, timeout=0.2)
            reactor.callLater(5.0, reactor.stop)  # safety net

        reactor.callWhenRunning(setup)
        reactor.run()
        self.assertTrue(any(isinstance(e, tuple) and e[0] == 'failed'
                            for e in events))


# --------------------------------------------------------------------------- #
# reactor.stop() is idempotent (review blocker 3)
# --------------------------------------------------------------------------- #
class ReactorStopIdempotentTests(_LoopTestCase):

    def test_stop_when_not_running_is_noop(self):
        self.assertFalse(reactor.running)
        reactor.stop()                      # must not raise
        self.assertFalse(reactor.running)

    def test_double_stop_while_running(self):
        def setup():
            reactor.stop()
            reactor.stop()                  # second call is a no-op
            reactor.callLater(5.0, reactor.stop)  # safety net

        reactor.callWhenRunning(setup)
        reactor.run()
        self.assertFalse(reactor.running)


# --------------------------------------------------------------------------- #
# maybeDeferred adapts awaitables without leaving them un-awaited (blocker 4)
# --------------------------------------------------------------------------- #
class MaybeDeferredAwaitableTests(_LoopTestCase):

    def test_coroutine_result(self):
        async def producer():
            return 'async-result'
        out = []

        def run():
            d = maybeDeferred(producer)
            d.addCallback(lambda r: out.append(r) or reactor.stop())
            reactor.callLater(5.0, reactor.stop)

        reactor.callWhenRunning(run)
        reactor.run()
        self.assertEqual(out, ['async-result'])

    def test_future_result(self):
        out = []

        def run():
            fut = self.loop.create_future()
            d = maybeDeferred(lambda: fut)
            d.addCallback(lambda r: out.append(r) or reactor.stop())
            self.loop.call_later(0.01, lambda: fut.set_result('fut-val'))
            reactor.callLater(5.0, reactor.stop)

        reactor.callWhenRunning(run)
        reactor.run()
        self.assertEqual(out, ['fut-val'])

    def test_coroutine_failure_errbacks(self):
        async def boom():
            raise ValueError('async-boom')
        out = []

        def run():
            d = maybeDeferred(boom)
            d.addErrback(lambda f: out.append(f.check(ValueError)) or reactor.stop())
            reactor.callLater(5.0, reactor.stop)

        reactor.callWhenRunning(run)
        reactor.run()
        self.assertEqual(out, [ValueError])

    def test_no_unawaited_coroutine_warning(self):
        import gc
        import warnings
        async def producer():
            return 1
        out = []

        with warnings.catch_warnings():
            warnings.simplefilter('error', RuntimeWarning)

            def run():
                d = maybeDeferred(producer)
                d.addCallback(lambda r: out.append(r) or reactor.stop())
                reactor.callLater(5.0, reactor.stop)

            reactor.callWhenRunning(run)
            reactor.run()
            gc.collect()                    # force any "never awaited" warning
        self.assertEqual(out, [1])


# --------------------------------------------------------------------------- #
# aio.utils.getProcessOutputAndValue (replaces twisted.internet.utils)
# --------------------------------------------------------------------------- #
class GetProcessOutputAndValueTests(_LoopTestCase):

    def test_clean_exit_callbacks_with_tuple(self):
        from asterisk.aio import utils
        record = {}
        script = ("import sys; sys.stdout.write('hi'); "
                  "sys.stderr.write('eh'); sys.exit(2)")

        def run():
            d = utils.getProcessOutputAndValue(sys.executable, ['-c', script])
            d.addCallback(lambda r: record.__setitem__('r', r) or reactor.stop())
            reactor.callLater(5.0, reactor.stop)

        reactor.callWhenRunning(run)
        reactor.run()
        out, err, code = record['r']
        self.assertEqual(out, b'hi')
        self.assertEqual(err, b'eh')
        self.assertEqual(code, 2)

    def test_signal_errbacks_with_value_tuple(self):
        from asterisk.aio import utils
        import signal
        record = {}
        script = "import os, signal; os.kill(os.getpid(), signal.SIGTERM)"

        def run():
            d = utils.getProcessOutputAndValue(sys.executable, ['-c', script])
            d.addErrback(lambda f: record.__setitem__('f', f) or reactor.stop())
            reactor.callLater(5.0, reactor.stop)

        reactor.callWhenRunning(run)
        reactor.run()
        f = record['f']
        self.assertIsInstance(f, Failure)
        out, err, sig = f.value
        self.assertEqual(sig, int(signal.SIGTERM))


# --------------------------------------------------------------------------- #
# Shutdown cancels tasks created outside the reactor registry
# --------------------------------------------------------------------------- #
class ShutdownStrayTaskTests(_LoopTestCase):

    def test_pending_maybe_deferred_task_is_cancelled(self):
        # maybeDeferred(coroutine) schedules work via asyncio.ensure_future, not
        # through a reactor registry. Stopping while it is pending must not leak
        # a live task past run().
        def setup():
            maybeDeferred(lambda: asyncio.sleep(60))   # stray task
            reactor.callLater(0.05, reactor.stop)

        reactor.callWhenRunning(setup)
        reactor.run()
        pending = [t for t in asyncio.all_tasks(self.loop) if not t.done()]
        self.assertEqual(pending, [])

    def test_pending_subprocess_is_terminated(self):
        import os
        import tempfile
        from asterisk.aio import utils

        watcher_warnings = []

        class _WarningCapture(logging.Handler):
            def emit(self, log_record):
                if 'exit status already read' in log_record.getMessage():
                    watcher_warnings.append(log_record.getMessage())

        warning_capture = _WarningCapture()
        asyncio_logger = logging.getLogger('asyncio')
        asyncio_logger.addHandler(warning_capture)
        fd, pidfile = tempfile.mkstemp()
        os.close(fd)
        self.addCleanup(lambda: os.path.exists(pidfile) and os.remove(pidfile))
        script = ("import os, time; "
                  "open(%r, 'w').write(str(os.getpid())); "
                  "time.sleep(60)" % pidfile)
        holder = {}

        def setup():
            d = utils.getProcessOutputAndValue(sys.executable, ['-c', script])
            d.addErrback(lambda f: holder.__setitem__(
                'err', f.check(asyncio.CancelledError)))

            def check_ready():
                if os.path.getsize(pidfile) > 0:
                    reactor.stop()
                else:
                    reactor.callLater(0.02, check_ready)

            reactor.callLater(0.02, check_ready)
            reactor.callLater(10.0, reactor.stop)   # safety net

        try:
            reactor.callWhenRunning(setup)
            reactor.run()
        finally:
            asyncio_logger.removeHandler(warning_capture)

        # The Deferred errbacked with CancelledError, no task leaked, and the
        # child was terminated (and reaped) rather than left running.
        self.assertEqual(holder.get('err'), asyncio.CancelledError)
        pending = [t for t in asyncio.all_tasks(self.loop) if not t.done()]
        self.assertEqual(pending, [])
        with open(pidfile) as handle:
            pid = int(handle.read())
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)
        self.assertEqual(watcher_warnings, [])


class StreamProtocolBaseTests(unittest.TestCase):
    """The generic Protocol/Factory/ClientFactory base classes used by
    stream-oriented fixtures (e.g. the PJSIP keep_alive TCP client)."""

    def test_make_connection_binds_transport_and_fires(self):
        events = []

        class _Proto(Protocol):
            def connectionMade(self):
                events.append(('made', self.transport))

            def dataReceived(self, data):
                events.append(('data', data))

            def connectionLost(self, reason=None):
                events.append(('lost', reason))

        proto = _Proto()
        sentinel = object()
        proto.makeConnection(sentinel)
        proto.dataReceived(b'hi')
        proto.connectionLost('done')

        self.assertIs(proto.transport, sentinel)
        self.assertEqual(events,
                         [('made', sentinel), ('data', b'hi'), ('lost', 'done')])

    def test_factory_build_protocol_backlinks_factory(self):
        class _Proto(Protocol):
            pass

        class _Factory(Factory):
            protocol = _Proto

        factory = _Factory()
        proto = factory.buildProtocol(('127.0.0.1', 5060))
        self.assertIsInstance(proto, _Proto)
        self.assertIs(proto.factory, factory)

    def test_client_factory_is_a_factory_with_callbacks(self):
        factory = ClientFactory()
        self.assertIsInstance(factory, Factory)
        # The connection-lifecycle callbacks exist as no-op hooks so subclasses
        # may override only the ones they need (keep_alive overrides the failed/
        # lost pair). Calling them must not raise.
        factory.startedConnecting(None)
        factory.clientConnectionFailed(None, 'reason')
        factory.clientConnectionLost(None, 'reason')
        factory.doStart()
        factory.doStop()


if __name__ == '__main__':
    unittest.main()
