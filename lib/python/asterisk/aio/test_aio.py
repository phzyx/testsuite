"""Unit tests for the asterisk.aio compatibility layer.

Covers the Twisted semantics the test suite relies on:
  * Deferred callback/errback threading and branch-switching
  * late-added callbacks (added after the Deferred has fired)
  * AlreadyCalledError on double-fire
  * DeferredList result shaping, fireOnOne*, and consumeErrors
  * maybeDeferred wrapping of plain values, Failures, and raised exceptions
  * Failure.check/trap
  * current_runtime().callLater / _DelayedCall.cancel
  * a UDP echo round-trip through the DatagramProtocol adapter
  * a subprocess exit through the ProcessProtocol adapter

Run with:  python3 -m unittest asterisk.aio.test_aio
"""

import asyncio
import gc
import logging
import signal
import sys
import unittest
import warnings
from unittest import mock

from asterisk.aio.defer import (
    Deferred, DeferredList, gatherResults, maybeDeferred, succeed, fail,
    AlreadyCalledError, TimeoutError as DeferTimeoutError,
)
from asterisk.aio.failure import Failure
from asterisk.aio.protocols import (
    DatagramProtocol, ProcessProtocol, ProcessDone, ProcessTerminated,
    Protocol, Factory, ClientFactory, _ProcessTransportAdapter,
)
from asterisk.aio.runtime import (
    AsyncTestRuntime, ReactorAlreadyRunning, _RuntimeState, _Connector,
    new_runtime, install_runtime, detach_runtime, get_current_runtime,
    current_runtime,
)
from asterisk.test_runner import run_test_object


class _LoopTestCase(unittest.TestCase):
    """Base class giving each test a fresh event loop bound to the current runtime."""

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        # Install a *fresh* runtime bound to this loop as the current runtime, so
        # each test gets an isolated per-run owner rather than a reset shared
        # singleton. current_runtime() resolves this dynamically on every call.
        self.runtime = new_runtime(self.loop)

    def tearDown(self):
        # Detach so the next test starts from an empty holder and no superseded
        # runtime lingers as "current".
        detach_runtime(self.runtime)
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
        current_runtime().callWhenRunning(
            lambda: current_runtime().callLater(0.01, lambda: (out.append('fired'),
                                                     current_runtime().stop())))
        current_runtime().run()
        self.assertEqual(out, ['fired'])

    def test_cancel_prevents_firing(self):
        out = []

        def setup():
            dc = current_runtime().callLater(0.05, lambda: out.append('should-not'))
            self.assertTrue(dc.active())
            dc.cancel()
            self.assertFalse(dc.active())
            current_runtime().callLater(0.02, current_runtime().stop)

        current_runtime().callWhenRunning(setup)
        current_runtime().run()
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
            dc = current_runtime().callLater(30.0, lambda: None)
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
            current_runtime().stop()

        current_runtime().callWhenRunning(setup)
        current_runtime().run()
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
        current_runtime().stop()


class _EchoServer(DatagramProtocol):
    def __init__(self, result, holder):
        self._result = result
        self._holder = holder

    def startProtocol(self):
        port = self.transport.getHost()[1]
        client = _EchoClient(port, self._result, self._holder)
        handle = current_runtime().listenUDP(0, client, '127.0.0.1')
        self._holder.setdefault('ports', []).append(handle)

    def datagramReceived(self, data, addr):
        self.transport.write(b'echo:' + data, addr)


class UDPEchoTests(_LoopTestCase):

    def test_round_trip(self):
        result = {}
        holder = {}

        def setup():
            holder['dc'] = current_runtime().callLater(3.0, current_runtime().stop)  # safety net
            holder['ports'] = []
            server = _EchoServer(result, holder)
            handle = current_runtime().listenUDP(0, server, '127.0.0.1')
            holder['ports'].append(handle)

        current_runtime().callWhenRunning(setup)
        current_runtime().run()
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
            handle = current_runtime().listenUDP(0, proto, '127.0.0.1')
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
            current_runtime().stop()

        current_runtime().callWhenRunning(setup)
        current_runtime().run()
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
        current_runtime().stop()


class SubprocessTests(_LoopTestCase):

    def test_clean_exit(self):
        record = {}
        script = "import sys; sys.stdout.write('hi'); sys.stdout.flush()"

        def setup():
            proto = _CollectingProcess(record)
            current_runtime().spawnProcess(proto, sys.executable,
                                 [sys.executable, '-c', script])
            current_runtime().callLater(5.0, current_runtime().stop)  # safety net

        current_runtime().callWhenRunning(setup)
        current_runtime().run()

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
            current_runtime().spawnProcess(proto, sys.executable,
                                 [sys.executable, '-c', script])
            current_runtime().callLater(5.0, current_runtime().stop)  # safety net

        current_runtime().callWhenRunning(setup)
        current_runtime().run()

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
            current_runtime().spawnProcess(proto, sys.executable,
                                 [sys.executable, '-c', script])
            # Synchronously, before connection_made: transport must exist and
            # accept a kill (mirrors SIPpProtocol.kill()).
            record['transport_sync'] = proto.transport
            proto.transport.signalProcess('KILL')
            current_runtime().callLater(10.0, current_runtime().stop)  # safety net

        try:
            current_runtime().callWhenRunning(setup)
            current_runtime().run()
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
            current_runtime().spawnProcess(
                proto, sys.executable,
                [sys.executable, '-c', 'import time; time.sleep(30)'])
            current_runtime().callLater(0.1, current_runtime().stop)

        try:
            current_runtime().callWhenRunning(setup)
            current_runtime().run()
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
        current_runtime().spawnProcess(proto, bad, [bad])
        with self.assertRaises(FileNotFoundError):
            current_runtime().run()
        self.assertFalse(current_runtime().running)

    def test_running_spawn_failure_propagates(self):
        # Registered while running (via callWhenRunning): a fatal bind error
        # stops the reactor and is re-raised by run().
        proto = ProcessProtocol()
        bad = '/nonexistent/binary/definitely-not-here'

        def setup():
            current_runtime().spawnProcess(proto, bad, [bad])
            current_runtime().callLater(5.0, current_runtime().stop)  # safety net

        current_runtime().callWhenRunning(setup)
        with self.assertRaises(FileNotFoundError):
            current_runtime().run()
        self.assertFalse(current_runtime().running)


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
        current_runtime().stop()


class PipeDrainTests(_LoopTestCase):

    def test_all_stdout_captured_despite_early_exit(self):
        record = {}
        # Write a large payload then exit immediately; process_exited may be
        # delivered before the final stdout chunks. All bytes must still arrive.
        script = ("import sys; sys.stdout.buffer.write(b'x'*200000); "
                  "sys.stdout.flush()")

        def setup():
            proto = _BulkCollectingProcess(record)
            current_runtime().spawnProcess(proto, sys.executable,
                                 [sys.executable, '-c', script])
            current_runtime().callLater(5.0, current_runtime().stop)  # safety net

        current_runtime().callWhenRunning(setup)
        current_runtime().run()
        self.assertEqual(len(record.get('out', b'')), 200000)
        self.assertEqual(record['reason'].check(ProcessDone), ProcessDone)


# --------------------------------------------------------------------------- #
# callInThread returns a Deferred with the worker result
# --------------------------------------------------------------------------- #
class CallInThreadTests(_LoopTestCase):

    def test_result_delivered(self):
        record = {}

        def setup():
            d = current_runtime().callInThread(lambda: 21 * 2)
            d.addCallback(lambda r: (record.__setitem__('r', r), current_runtime().stop()))
            current_runtime().callLater(5.0, current_runtime().stop)  # safety net

        current_runtime().callWhenRunning(setup)
        current_runtime().run()
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
            current_runtime().stop()

    def clientConnectionFailed(self, connector, reason):
        self.events.append(('failed', reason))
        current_runtime().stop()

    def stopTrying(self):
        self.events.append('stopTrying')


def _server_port(handle):
    return handle._server.sockets[0].getsockname()[1]


class TCPClientNotificationTests(_LoopTestCase):

    def test_connection_lost_notifies_factory(self):
        events = []
        server_handle = current_runtime().listenTCP(0, _DroppingServerFactory(),
                                          interface='127.0.0.1')

        def setup():
            port = _server_port(server_handle)
            factory = _RecordingClientFactory(events)
            current_runtime().connectTCP('127.0.0.1', port, factory)
            current_runtime().callLater(5.0, current_runtime().stop)  # safety net

        current_runtime().callWhenRunning(setup)
        current_runtime().run()
        self.assertIn('lost', events)

    def test_reconnect_via_retry(self):
        events = []
        server_handle = current_runtime().listenTCP(0, _DroppingServerFactory(),
                                          interface='127.0.0.1')

        def setup():
            port = _server_port(server_handle)
            factory = _RecordingClientFactory(events, reconnect_once=True)
            current_runtime().connectTCP('127.0.0.1', port, factory)
            current_runtime().callLater(5.0, current_runtime().stop)  # safety net

        current_runtime().callWhenRunning(setup)
        current_runtime().run()
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
        server_handle = current_runtime().listenTCP(0, _AcceptingServerFactory(peers),
                                          interface='127.0.0.1')

        def setup():
            port = _server_port(server_handle)
            factory = _RecordingClientFactory([])
            # Bind the client socket to loopback explicitly; the server should
            # see a 127.0.0.1 peer, proving local_addr was applied.
            current_runtime().connectTCP('127.0.0.1', port, factory,
                               bindAddress=('127.0.0.1', 0))
            current_runtime().callLater(0.3, current_runtime().stop)

        current_runtime().callWhenRunning(setup)
        current_runtime().run()
        self.assertTrue(peers)
        self.assertEqual(peers[0][0], '127.0.0.1')

    def test_timeout_triggers_connection_failed(self):
        events = []

        def setup():
            factory = _RecordingClientFactory(events)
            # 192.0.2.0/24 is TEST-NET-1 (RFC 5737), guaranteed unreachable; a
            # short timeout must surface as clientConnectionFailed, not a hang.
            current_runtime().connectTCP('192.0.2.1', 9, factory, timeout=0.2)
            current_runtime().callLater(5.0, current_runtime().stop)  # safety net

        current_runtime().callWhenRunning(setup)
        current_runtime().run()
        self.assertTrue(any(isinstance(e, tuple) and e[0] == 'failed'
                            for e in events))


# --------------------------------------------------------------------------- #
# current_runtime().stop() is idempotent (review blocker 3)
# --------------------------------------------------------------------------- #
class ReactorStopIdempotentTests(_LoopTestCase):

    def test_stop_when_not_running_is_noop(self):
        self.assertFalse(current_runtime().running)
        current_runtime().stop()                      # must not raise
        self.assertFalse(current_runtime().running)

    def test_double_stop_while_running(self):
        def setup():
            current_runtime().stop()
            current_runtime().stop()                  # second call is a no-op
            current_runtime().callLater(5.0, current_runtime().stop)  # safety net

        current_runtime().callWhenRunning(setup)
        current_runtime().run()
        self.assertFalse(current_runtime().running)


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
            d.addCallback(lambda r: out.append(r) or current_runtime().stop())
            current_runtime().callLater(5.0, current_runtime().stop)

        current_runtime().callWhenRunning(run)
        current_runtime().run()
        self.assertEqual(out, ['async-result'])

    def test_future_result(self):
        out = []

        def run():
            fut = self.loop.create_future()
            d = maybeDeferred(lambda: fut)
            d.addCallback(lambda r: out.append(r) or current_runtime().stop())
            self.loop.call_later(0.01, lambda: fut.set_result('fut-val'))
            current_runtime().callLater(5.0, current_runtime().stop)

        current_runtime().callWhenRunning(run)
        current_runtime().run()
        self.assertEqual(out, ['fut-val'])

    def test_coroutine_failure_errbacks(self):
        async def boom():
            raise ValueError('async-boom')
        out = []

        def run():
            d = maybeDeferred(boom)
            d.addErrback(lambda f: out.append(f.check(ValueError)) or current_runtime().stop())
            current_runtime().callLater(5.0, current_runtime().stop)

        current_runtime().callWhenRunning(run)
        current_runtime().run()
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
                d.addCallback(lambda r: out.append(r) or current_runtime().stop())
                current_runtime().callLater(5.0, current_runtime().stop)

            current_runtime().callWhenRunning(run)
            current_runtime().run()
            gc.collect()                    # force any "never awaited" warning
        self.assertEqual(out, [1])


# --------------------------------------------------------------------------- #
# aio.utils.getProcessOutputAndValue (replaces twisted.internet.utils)
# --------------------------------------------------------------------------- #
class GetProcessOutputAndValueTests(_LoopTestCase):

    def test_clean_exit_returns_tuple(self):
        from asterisk.aio import utils
        record = {}
        script = ("import sys; sys.stdout.write('hi'); "
                  "sys.stderr.write('eh'); sys.exit(2)")

        def run():
            async def go():
                record['r'] = await utils.getProcessOutputAndValue(
                    sys.executable, ['-c', script])
                current_runtime().stop()
            asyncio.ensure_future(go())
            current_runtime().callLater(5.0, current_runtime().stop)

        current_runtime().callWhenRunning(run)
        current_runtime().run()
        out, err, code = record['r']
        self.assertEqual(out, b'hi')
        self.assertEqual(err, b'eh')
        self.assertEqual(code, 2)

    def test_signal_raises_with_value_tuple(self):
        from asterisk.aio import utils
        import signal
        record = {}
        script = "import os, signal; os.kill(os.getpid(), signal.SIGTERM)"

        def run():
            async def go():
                try:
                    await utils.getProcessOutputAndValue(
                        sys.executable, ['-c', script])
                except utils.ProcessSignaled as exc:
                    record['exc'] = exc
                current_runtime().stop()
            asyncio.ensure_future(go())
            current_runtime().callLater(5.0, current_runtime().stop)

        current_runtime().callWhenRunning(run)
        current_runtime().run()
        exc = record['exc']
        self.assertIsInstance(exc, utils.ProcessSignaled)
        out, err, sig = exc.value
        self.assertEqual(sig, int(signal.SIGTERM))


# --------------------------------------------------------------------------- #
# AsteriskCliCommand.execute() bridges getProcessOutputAndValue onto a Deferred
# --------------------------------------------------------------------------- #
class AsteriskCliCommandBridgeTests(_LoopTestCase):
    """Regression: a subprocess *startup* failure must errback the returned
    Deferred (not leave it unresolved with an unretrieved task exception).

    getProcessOutputAndValue is now a native ``async def``; if execute()'s bridge
    only caught ``ProcessSignaled``, a ``FileNotFoundError``/``OSError`` from
    ``create_subprocess_exec`` would crash the task and never fire the Deferred.
    """

    def test_bad_executable_errbacks_returned_deferred(self):
        from asterisk.asterisk import AsteriskCliCommand

        # cmd[0] is a nonexistent binary; cmd[4] satisfies __init__'s cli_cmd.
        cmd = ['/nonexistent/definitely-not-a-real-asterisk',
               '-C', 'asterisk.conf', '-rx', 'core show version']
        cli = AsteriskCliCommand('127.0.0.1', cmd)
        record = {}

        # Capture asyncio's "Task exception was never retrieved" warning, which
        # is what the old (ProcessSignaled-only) bridge produced on this path.
        task_warnings = []

        class _WarningCapture(logging.Handler):
            def emit(self, log_record):
                if 'never retrieved' in log_record.getMessage():
                    task_warnings.append(log_record.getMessage())

        warning_capture = _WarningCapture()
        asyncio_logger = logging.getLogger('asyncio')
        asyncio_logger.addHandler(warning_capture)

        def run():
            d = cli.execute()
            d.addErrback(lambda f: record.__setitem__('err', f)
                         or current_runtime().stop())
            current_runtime().callLater(5.0, current_runtime().stop)

        try:
            current_runtime().callWhenRunning(run)
            current_runtime().run()
            gc.collect()      # force any unretrieved-task-exception warning
        finally:
            asyncio_logger.removeHandler(warning_capture)

        # The Deferred errbacked (did not hang unresolved) ...
        self.assertIn('err', record)
        self.assertTrue(_is_failure_like(record['err']))
        # ... execute() recorded the failure state ...
        self.assertEqual(cli.exitcode, -1)
        self.assertTrue(cli.err)
        # ... no task leaked and no unretrieved task exception was logged.
        pending = [t for t in asyncio.all_tasks(self.loop) if not t.done()]
        self.assertEqual(pending, [])
        self.assertEqual(task_warnings, [])


def _is_failure_like(obj):
    """True for the aio Failure the errback chain delivers."""
    return isinstance(obj, Failure) or hasattr(obj, 'value')


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
            current_runtime().callLater(0.05, current_runtime().stop)

        current_runtime().callWhenRunning(setup)
        current_runtime().run()
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
            async def go():
                try:
                    await utils.getProcessOutputAndValue(
                        sys.executable, ['-c', script])
                except asyncio.CancelledError:
                    holder['err'] = asyncio.CancelledError
                    raise
            asyncio.ensure_future(go())

            def check_ready():
                if os.path.getsize(pidfile) > 0:
                    current_runtime().stop()
                else:
                    current_runtime().callLater(0.02, check_ready)

            current_runtime().callLater(0.02, check_ready)
            current_runtime().callLater(10.0, current_runtime().stop)   # safety net

        try:
            current_runtime().callWhenRunning(setup)
            current_runtime().run()
        finally:
            asyncio_logger.removeHandler(warning_capture)

        # The stray task was cancelled with CancelledError, no task leaked, and
        # the child was terminated (and reaped) rather than left running.
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


# --------------------------------------------------------------------------- #
# B1.0: explicit lifecycle state machine
# --------------------------------------------------------------------------- #
class RuntimeStateMachineTests(_LoopTestCase):
    """The runtime advances through explicit _RuntimeState transitions, and the
    public ``running`` boolean is derived from that state rather than stored."""

    def test_state_progression_collecting_running_stopped(self):
        seen = {}
        self.assertIs(self.runtime.state, _RuntimeState.COLLECTING)

        def cb():
            seen['at_run'] = self.runtime.state
            current_runtime().stop()

        current_runtime().callWhenRunning(cb)
        current_runtime().run()
        self.assertIs(seen['at_run'], _RuntimeState.RUNNING)
        self.assertIs(self.runtime.state, _RuntimeState.STOPPED)

    def test_running_property_derives_from_state(self):
        obs = {}
        self.assertFalse(current_runtime().running)          # COLLECTING

        def cb():
            obs['before_stop'] = current_runtime().running   # RUNNING, not stopped -> True
            current_runtime().stop()
            obs['after_stop'] = current_runtime().running    # stop requested -> False

        current_runtime().callWhenRunning(cb)
        current_runtime().run()
        self.assertTrue(obs['before_stop'])
        self.assertFalse(obs['after_stop'])
        self.assertFalse(current_runtime().running)          # STOPPED

    def test_fatal_prerun_bind_ends_in_stopped(self):
        proto = ProcessProtocol()
        bad = '/nonexistent/binary/definitely-not-here'
        current_runtime().spawnProcess(proto, bad, [bad])
        with self.assertRaises(FileNotFoundError):
            current_runtime().run()
        self.assertIs(self.runtime.state, _RuntimeState.STOPPED)
        self.assertFalse(current_runtime().running)


# --------------------------------------------------------------------------- #
# B1.0: re-entrant run() is rejected even after stop() flips running False
# --------------------------------------------------------------------------- #
class ReentrantRunTests(_LoopTestCase):

    def test_run_rejected_during_run_after_stop(self):
        outcome = {}

        def cb():
            current_runtime().stop()
            # stop() has flipped ``running`` False (stop requested) while the
            # outer run() is still in RUNNING and about to unwind. A guard on
            # ``running`` would let this re-enter; a state guard must reject it.
            self.assertFalse(current_runtime().running)
            try:
                current_runtime().run()
                outcome['result'] = 'ran'
            except ReactorAlreadyRunning:
                outcome['result'] = 'rejected'

        current_runtime().callWhenRunning(cb)
        current_runtime().run()
        self.assertEqual(outcome['result'], 'rejected')


# --------------------------------------------------------------------------- #
# B1.0: reset() is guarded (test-facing), never a silent registry discard
# --------------------------------------------------------------------------- #
class ResetSafetyTests(_LoopTestCase):

    def test_reset_after_clean_run_is_allowed(self):
        current_runtime().callWhenRunning(lambda: current_runtime().callLater(0.01, current_runtime().stop))
        current_runtime().run()
        # Clean completion: registries drained, STOPPED -> reset permitted.
        self.runtime.reset(self.loop)
        self.assertIs(self.runtime.state, _RuntimeState.COLLECTING)

    def test_reset_rejected_with_live_resources(self):
        dc = current_runtime().callLater(30.0, lambda: None)   # a live timer
        with self.assertRaises(RuntimeError):
            self.runtime.reset(self.loop)
        dc.cancel()

    def test_reset_rejected_while_active(self):
        outcome = {}

        def cb():
            try:
                self.runtime.reset()
            except RuntimeError:
                outcome['reset'] = 'rejected'
            current_runtime().stop()

        current_runtime().callWhenRunning(cb)
        current_runtime().run()
        self.assertEqual(outcome.get('reset'), 'rejected')


# --------------------------------------------------------------------------- #
# B1.0: install_runtime() refuses to silently orphan the incumbent runtime
# --------------------------------------------------------------------------- #
class InstallGuardTests(_LoopTestCase):
    """Installing a replacement is rejected while the incumbent is active or
    still owns resources; only an empty holder, the same object, or an idle
    resource-free incumbent may be superseded."""

    def test_install_rejected_when_current_active(self):
        other = AsyncTestRuntime()
        outcome = {}

        def cb():
            # In-run: self.runtime is RUNNING (active). Superseding it would
            # orphan the live run, so install must reject.
            try:
                install_runtime(other)
            except ReactorAlreadyRunning:
                outcome['install'] = 'rejected'
            # Holder untouched: the facade still resolves to self.runtime.
            self.assertIs(get_current_runtime(), self.runtime)
            current_runtime().stop()

        current_runtime().callWhenRunning(cb)
        current_runtime().run()
        self.assertEqual(outcome.get('install'), 'rejected')

    def test_install_rejected_when_current_owns_resources(self):
        other = AsyncTestRuntime()
        dc = current_runtime().callLater(30.0, lambda: None)   # idle runtime, live timer
        with self.assertRaises(RuntimeError):
            install_runtime(other)
        # Holder unchanged; the incumbent still owns its timer.
        self.assertIs(get_current_runtime(), self.runtime)
        dc.cancel()

    def test_install_same_runtime_is_allowed(self):
        # Reinstalling the same object is a no-op, never a rejection.
        self.assertIs(install_runtime(self.runtime), self.runtime)

    def test_install_allowed_when_current_idle_and_empty(self):
        # The fixture runtime is COLLECTING with no resources -> replaceable.
        other = AsyncTestRuntime()
        self.assertIs(install_runtime(other), other)
        self.assertIs(get_current_runtime(), other)
        # Restore the fixture's runtime so tearDown's detach matches.
        install_runtime(self.runtime)


# --------------------------------------------------------------------------- #
# B1.0: sequential runs with fresh installed runtimes own distinct loop/runtime
# --------------------------------------------------------------------------- #
class SequentialRunOwnershipTests(unittest.TestCase):
    """Two sequential runs, each installing a freshly created runtime, own
    distinct runtimes and loops. This is the per-run isolation the native
    entrypoint will rely on: the blocking run() leaves its STOPPED runtime
    installed, so the caller detaches it before installing the next owner."""

    def tearDown(self):
        detach_runtime()
        asyncio.set_event_loop(None)

    def _one_run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        rt = new_runtime(loop)
        current_runtime().callWhenRunning(lambda: current_runtime().callLater(0.01, current_runtime().stop))
        current_runtime().run()
        self.assertIs(rt.state, _RuntimeState.STOPPED)
        self.assertEqual(rt.live_resources(), [])
        # Blocking run() does not detach; the caller does before the next run.
        detach_runtime(rt)
        loop.close()
        return rt, loop

    def test_two_sequential_runs_have_distinct_ownership(self):
        rt1, loop1 = self._one_run()
        rt2, loop2 = self._one_run()
        self.assertIsNot(rt1, rt2)
        self.assertIsNot(loop1, loop2)
        self.assertIs(rt1._loop, loop1)
        self.assertIs(rt2._loop, loop2)


# --------------------------------------------------------------------------- #
# B1.1: native entrypoint primitives -- startup driver, await-only bridge,
# ordered teardown -- with responsibilities decoupled (design doc points 2/2b/4)
# --------------------------------------------------------------------------- #
class NativeEntrypointTests(_LoopTestCase):
    """start_all() drives startup, run_async() only awaits completion, and
    _finish() owns the single ordered teardown -- driven under a live loop the
    caller is turning (asyncio.run / loop.run_until_complete)."""

    def _drive(self, coro):
        """Run a coroutine that mimics _main: startup -> await -> finish."""
        return self.loop.run_until_complete(coro)

    def test_startup_then_bridge_then_finish(self):
        async def go():
            current_runtime().callWhenRunning(lambda: current_runtime().callLater(0.01, current_runtime().stop))
            await self.runtime.start_all()
            # After startup: RUNNING with a live completion future.
            self.assertIs(self.runtime.state, _RuntimeState.RUNNING)
            self.assertIsNotNone(self.runtime._completion)
            await self.runtime.run_async()   # bridge: only awaits completion
            await self.runtime._finish()      # single ordered teardown
        self._drive(go())
        self.assertIs(self.runtime.state, _RuntimeState.STOPPED)
        self.assertEqual(self.runtime.live_resources(), [])

    def test_run_async_only_awaits_no_shutdown(self):
        # run_async() must NOT tear down: a timer registered before it is still
        # live after it returns (teardown is _finish's job, not the bridge's).
        async def go():
            await self.runtime.start_all()
            dc = current_runtime().callLater(30.0, lambda: None)
            current_runtime().stop()
            await self.runtime.run_async()
            # Bridge returned on stop; the timer is untouched (still RUNNING).
            self.assertIs(self.runtime.state, _RuntimeState.RUNNING)
            self.assertEqual(self.runtime.live_resources(), ['delayed_calls'])
            self.assertTrue(dc.active())
            await self.runtime._finish()      # now it is torn down
        self._drive(go())
        self.assertIs(self.runtime.state, _RuntimeState.STOPPED)
        self.assertEqual(self.runtime.live_resources(), [])

    def test_fatal_bind_reraises_and_finish_cleans(self):
        async def boom():
            raise RuntimeError('bind failed')

        async def go():
            self.runtime.addStartupBind(boom)
            raised = None
            try:
                await self.runtime.start_all()   # fatal bind re-raises
            except RuntimeError as exc:
                raised = exc
            finally:
                await self.runtime._finish()         # teardown runs regardless
            self.assertIsNotNone(raised)
            self.assertIs(self.runtime._failure, raised)
        self._drive(go())
        self.assertIs(self.runtime.state, _RuntimeState.STOPPED)
        self.assertEqual(self.runtime.live_resources(), [])

    def test_start_all_rejects_reentrant_completion(self):
        # An unresolved completion future means a run is already in flight.
        async def go():
            self.runtime._completion = self.loop.create_future()
            with self.assertRaises(ReactorAlreadyRunning):
                await self.runtime.start_all()
            self.runtime._completion.cancel()
            self.runtime._completion = None
        self._drive(go())

    def test_first_bind_fatal_leaves_later_queued_binds_inert(self):
        # First queued bind fails fatally; the later binds are coroutine
        # FACTORIES that were never invoked (no coroutine object was ever
        # created), so the drain hands the unrun remainder back for _shutdown
        # to discard. The later factory must never run and there must be no
        # "coroutine was never awaited" warning.
        ran = {'later': False}

        async def boom():
            raise RuntimeError('first bind failed')

        async def later():
            ran['later'] = True

        async def go():
            self.runtime.addStartupBind(boom)
            self.runtime.addStartupBind(later)
            raised = None
            try:
                await self.runtime.start_all()
            except RuntimeError as exc:
                raised = exc
            finally:
                await self.runtime._finish()
            self.assertIsNotNone(raised)

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            self._drive(go())

        # The unrun factory was discarded (never invoked) by the teardown.
        self.assertFalse(ran['later'])
        self.assertIs(self.runtime.state, _RuntimeState.STOPPED)
        self.assertEqual(self.runtime.live_resources(), [])
        never_awaited = [w for w in caught
                         if 'never awaited' in str(w.message)]
        self.assertEqual(never_awaited, [])


# --------------------------------------------------------------------------- #
# B1.2: single start_all() startup driver -- cooperative stop-during-STARTING,
# binds registered mid-startup drained before kickoff, cross-thread stop, and
# the legacy blocking run() driving the SAME start_all/run_async/shutdown path.
# --------------------------------------------------------------------------- #
class StartupDriverTests(_LoopTestCase):
    """One start_all() drives startup for both native and legacy paths; a stop
    during STARTING aborts before RUNNING/kickoff, so the test never launches."""

    def _drive(self, coro):
        return self.loop.run_until_complete(coro)

    def test_stop_during_starting_aborts_before_kickoff(self):
        # A stop requested from a bind's apply (still STARTING) must abort the
        # drain: no later bind runs, RUNNING is never entered, and the kickoff
        # queue is never flushed -- the test the stop meant to prevent never
        # launches.
        events = {'second': False, 'launched': False}

        async def first():
            return 'ok'

        def stop_now(_result):
            current_runtime().stop()

        async def second():
            events['second'] = True

        async def go():
            current_runtime().callWhenRunning(
                lambda: events.__setitem__('launched', True))
            self.runtime.addStartupBind(first, stop_now)
            self.runtime.addStartupBind(second)
            await self.runtime.start_all()
            # Stopped during STARTING -> STOPPING, never RUNNING.
            self.assertIs(self.runtime.state, _RuntimeState.STOPPING)
            await self.runtime.run_async()   # completion already resolved
            await self.runtime._finish()

        self._drive(go())
        self.assertFalse(events['second'])    # later bind never ran
        self.assertFalse(events['launched'])  # kickoff never fired
        self.assertIs(self.runtime.state, _RuntimeState.STOPPED)
        self.assertEqual(self.runtime.live_resources(), [])

    def test_bind_registered_during_startup_is_drained_before_kickoff(self):
        # A bind's apply registers ANOTHER startup bind while still STARTING.
        # Because state is not yet RUNNING, it enqueues and the drain picks it
        # up in the same startup phase -- it must run BEFORE the kickoff flush,
        # never fire-and-forget after RUNNING.
        order = []

        async def first():
            order.append('first')

        def apply_first(_result):
            async def second():
                order.append('second')
            self.runtime.addStartupBind(second)

        def on_running():
            order.append('kickoff')
            current_runtime().stop()

        async def go():
            current_runtime().callWhenRunning(on_running)
            self.runtime.addStartupBind(first, apply_first)
            await self.runtime.start_all()
            # Startup reached RUNNING; both binds ran during the drain, and the
            # follow-on bind ('second') ran before the kickoff flush.
            self.assertIs(self.runtime.state, _RuntimeState.RUNNING)
            self.assertEqual(order[:2], ['first', 'second'])
            await self.runtime.run_async()   # completes on the kickoff's stop()
            await self.runtime._finish()

        self._drive(go())
        # The mid-startup-registered bind drained before kickoff, never after.
        self.assertEqual(order, ['first', 'second', 'kickoff'])
        self.assertLess(order.index('second'), order.index('kickoff'))
        self.assertIs(self.runtime.state, _RuntimeState.STOPPED)
        self.assertEqual(self.runtime.live_resources(), [])

    def test_concurrent_start_all_awaits_in_flight_startup(self):
        # A second start_all() issued while startup is still draining must AWAIT
        # the in-flight startup, not return early -- otherwise the caller would
        # proceed as if startup finished while a bind is still running. The
        # second caller must observe RUNNING (startup complete) on return.
        gate = None            # created on the loop; released to finish bind 1
        order = []

        async def slow_bind():
            order.append('bind-start')
            await gate.wait()   # hold startup open until the test releases it
            order.append('bind-done')

        async def second_caller():
            # Runs concurrently while start_all #1 is parked in slow_bind.
            await self.runtime.start_all()
            order.append('second-returned')
            # On return, startup is genuinely complete.
            self.assertIs(self.runtime.state, _RuntimeState.RUNNING)

        async def go():
            nonlocal gate
            gate = asyncio.Event()
            current_runtime().callWhenRunning(current_runtime().stop)
            self.runtime.addStartupBind(slow_bind)

            first = asyncio.ensure_future(self.runtime.start_all())
            # Let start_all #1 advance into STARTING and park on the bind.
            while self.runtime.state is not _RuntimeState.STARTING:
                await asyncio.sleep(0)
            await asyncio.sleep(0)

            second = asyncio.ensure_future(second_caller())
            # Give the second caller a chance to run; it must still be pending,
            # blocked on the same in-flight startup (not returned early).
            await asyncio.sleep(0)
            self.assertFalse(second.done())
            self.assertNotIn('second-returned', order)

            gate.set()                 # release the bind -> startup completes
            await first
            await second
            await self.runtime.run_async()
            await self.runtime._finish()

        self._drive(go())
        # The bind fully finished before the second caller returned.
        self.assertLess(order.index('bind-done'),
                        order.index('second-returned'))
        self.assertIs(self.runtime.state, _RuntimeState.STOPPED)
        self.assertEqual(self.runtime.live_resources(), [])

    def test_cancelling_secondary_waiter_does_not_cancel_startup(self):
        # A secondary start_all() waiter shields the shared startup task, so
        # cancelling that waiter must NOT cancel the in-flight startup: the
        # primary driver continues to RUNNING and the bind finishes.
        gate = None
        events = {'bind_done': False}

        async def slow_bind():
            await gate.wait()
            events['bind_done'] = True

        async def go():
            nonlocal gate
            gate = asyncio.Event()
            current_runtime().callWhenRunning(current_runtime().stop)
            self.runtime.addStartupBind(slow_bind)

            first = asyncio.ensure_future(self.runtime.start_all())
            while self.runtime.state is not _RuntimeState.STARTING:
                await asyncio.sleep(0)
            await asyncio.sleep(0)

            # A secondary waiter, then cancel it while startup is still parked.
            second = asyncio.ensure_future(self.runtime.start_all())
            await asyncio.sleep(0)
            self.assertFalse(second.done())
            second.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await second

            # The primary startup is unaffected: still STARTING, not cancelled.
            self.assertIs(self.runtime.state, _RuntimeState.STARTING)
            self.assertFalse(first.done())

            gate.set()                 # release the bind -> startup completes
            await first                # primary driver finishes cleanly
            self.assertIs(self.runtime.state, _RuntimeState.RUNNING)
            self.assertTrue(events['bind_done'])
            await self.runtime.run_async()
            await self.runtime._finish()

        self._drive(go())
        self.assertIs(self.runtime.state, _RuntimeState.STOPPED)
        self.assertEqual(self.runtime.live_resources(), [])

    def test_sequential_reuse_same_runtime(self):
        # After a clean start_all/run_async/_finish the runtime is STOPPED. A
        # STOPPED runtime refuses registrations (it is a spent teardown target),
        # so reuse must first reset() it back to a clean COLLECTING baseline --
        # the single documented gate between runs. reset() succeeds because the
        # finished run left no live resources.
        counts = {'runs': 0}

        def kickoff():
            counts['runs'] += 1
            current_runtime().stop()

        async def go():
            for _ in range(2):
                self.assertEqual(self.runtime.live_resources(), [])
                self.runtime.reset(self.loop)
                self.assertIs(self.runtime.state, _RuntimeState.COLLECTING)
                current_runtime().callWhenRunning(kickoff)
                await self.runtime.start_all()
                await self.runtime.run_async()
                await self.runtime._finish()
                self.assertIs(self.runtime.state, _RuntimeState.STOPPED)
                self.assertIsNone(self.runtime._completion)

        self._drive(go())
        self.assertEqual(counts['runs'], 2)

    def test_legacy_run_drives_start_all(self):
        # The blocking legacy run() drives the SAME start_all path: the startup
        # bind runs, kickoff flushes, and teardown clears all resources.
        events = {'bind': False, 'kickoff': False}

        async def bind():
            events['bind'] = True

        def on_running():
            events['kickoff'] = True
            current_runtime().callLater(0.01, current_runtime().stop)

        current_runtime().addStartupBind(bind)
        current_runtime().callWhenRunning(on_running)
        self.runtime.run()
        self.assertTrue(events['bind'])
        self.assertTrue(events['kickoff'])
        self.assertIs(self.runtime.state, _RuntimeState.STOPPED)
        self.assertEqual(self.runtime.live_resources(), [])

    def test_legacy_run_stopped_cross_thread(self):
        # stop() from another thread (via call_soon_threadsafe) unblocks the
        # blocking run() -- the cross-thread stop path the migrated scripts rely
        # on.
        import threading
        launched = threading.Event()

        def stopper():
            launched.wait(2.0)
            self.runtime.stop()     # cross-thread

        t = threading.Thread(target=stopper)
        current_runtime().callWhenRunning(launched.set)
        current_runtime().callWhenRunning(t.start)
        self.runtime.run()          # blocks until the other thread stops it
        t.join(2.0)
        self.assertFalse(t.is_alive())
        self.assertTrue(launched.is_set())
        self.assertIs(self.runtime.state, _RuntimeState.STOPPED)
        self.assertEqual(self.runtime.live_resources(), [])


# --------------------------------------------------------------------------- #
# B1.3: async pluggable-module lifecycle -- retained instances, awaited start()
# in registration order during STARTING, close() on every started module in the
# same forward order at shutdown (design doc point 3).
# --------------------------------------------------------------------------- #
class _RecordingModule(object):
    """Test double for a pluggable module with async start()/close() hooks.

    Records the shared event log so tests can assert ordering across modules.
    ``has_start=False`` models a module that defines no start() (treated as
    already-started); ``fail_start`` makes start() raise (fatal); ``fail_close``
    makes close() raise (must be isolated).
    """

    def __init__(self, name, log, has_start=True, fail_start=False,
                 fail_close=False):
        self.name = name
        self.log = log
        self.fail_start = fail_start
        self.fail_close = fail_close
        self.started = False
        self.closed = 0
        if not has_start:
            # Remove the bound method so getattr(module, 'start', None) is None.
            self.start = None

    async def start(self):
        self.log.append(('start', self.name))
        if self.fail_start:
            raise RuntimeError("start failed: %s" % self.name)
        self.started = True

    async def close(self):
        self.closed += 1
        self.log.append(('close', self.name))
        if self.fail_close:
            raise RuntimeError("close failed: %s" % self.name)


class ModuleLifecycleTests(_LoopTestCase):
    """Retained modules get start() awaited in registration order during
    STARTING and close() in the same forward order at shutdown; a module without
    start() is treated as started; a never-reached module is never closed."""

    def _drive(self, coro):
        return self.loop.run_until_complete(coro)

    def test_start_runs_in_registration_order_then_close_forward(self):
        log = []
        a = _RecordingModule('a', log)
        b = _RecordingModule('b', log)
        c = _RecordingModule('c', log)
        for m in (a, b, c):
            self.runtime.register_module(m)

        async def go():
            current_runtime().callWhenRunning(current_runtime().stop)
            await self.runtime.start_all()
            self.assertIs(self.runtime.state, _RuntimeState.RUNNING)
            await self.runtime.run_async()
            await self.runtime._finish()

        self._drive(go())
        self.assertEqual(
            log,
            [('start', 'a'), ('start', 'b'), ('start', 'c'),
             ('close', 'a'), ('close', 'b'), ('close', 'c')])
        self.assertEqual((a.closed, b.closed, c.closed), (1, 1, 1))
        self.assertEqual(self.runtime.live_resources(), [])

    def test_module_without_start_is_treated_as_started_and_closed(self):
        # A module that defines no start() is still enrolled and close()d.
        log = []
        a = _RecordingModule('a', log, has_start=False)
        b = _RecordingModule('b', log)
        self.runtime.register_module(a)
        self.runtime.register_module(b)

        async def go():
            current_runtime().callWhenRunning(current_runtime().stop)
            await self.runtime.start_all()
            await self.runtime.run_async()
            await self.runtime._finish()

        self._drive(go())
        # 'a' has no start() (no start log line) but is closed.
        self.assertEqual(
            log, [('start', 'b'), ('close', 'a'), ('close', 'b')])
        self.assertEqual(a.closed, 1)

    def test_failed_start_is_fatal_and_still_closes_enrolled_modules(self):
        # b.start() fails: it is fatal (recorded as _failure, re-raised), and
        # every module enrolled *up to and including* b is closed. c.start()
        # is never reached, so c is never closed.
        log = []
        a = _RecordingModule('a', log)
        b = _RecordingModule('b', log, fail_start=True)
        c = _RecordingModule('c', log)
        for m in (a, b, c):
            self.runtime.register_module(m)

        async def go():
            with self.assertRaises(RuntimeError):
                await self.runtime.start_all()
            await self.runtime._finish()

        self._drive(go())
        # a + b started (b failed); c never started. Close a, b -- not c.
        self.assertEqual(
            log,
            [('start', 'a'), ('start', 'b'),
             ('close', 'a'), ('close', 'b')])
        self.assertEqual(c.closed, 0)
        self.assertEqual(self.runtime.live_resources(), [])

    def test_stop_during_module_start_leaves_later_modules_unstarted(self):
        # A stop requested from a module's start() aborts the module phase before
        # the next module: 'a' starts (and is closed), 'b' never starts (and is
        # never closed), and the runtime goes to STOPPING not RUNNING -- the test
        # the stop meant to prevent never launches.
        log = []
        b = _RecordingModule('b', log)

        class StoppingModule(object):
            async def start(self):
                log.append(('start', 'a'))
                current_runtime().stop()

            async def close(self):
                log.append(('close', 'a'))

        self.runtime.register_module(StoppingModule())
        self.runtime.register_module(b)

        async def go():
            current_runtime().callWhenRunning(
                lambda: log.append(('kickoff', None)))
            await self.runtime.start_all()
            self.assertIs(self.runtime.state, _RuntimeState.STOPPING)
            await self.runtime.run_async()   # completion already resolved
            await self.runtime._finish()

        self._drive(go())
        # 'a' started + closed; 'b' never started; kickoff never fired.
        self.assertEqual(log, [('start', 'a'), ('close', 'a')])
        self.assertEqual(b.started, False)
        self.assertEqual(b.closed, 0)
        self.assertEqual(self.runtime.live_resources(), [])

    def test_broken_close_is_isolated_but_reported(self):
        # One module's close() raising must not skip the others' close()
        # (isolation), but the error must still surface at the end of teardown
        # rather than be silently swallowed (finding 4: isolated != invisible).
        log = []
        a = _RecordingModule('a', log)
        b = _RecordingModule('b', log, fail_close=True)
        c = _RecordingModule('c', log)
        for m in (a, b, c):
            self.runtime.register_module(m)

        async def go():
            current_runtime().callWhenRunning(current_runtime().stop)
            await self.runtime.start_all()
            await self.runtime.run_async()
            await self.runtime._finish()

        # b's broken close is isolated (a and c still close) but the single
        # collected error is re-raised at the end.
        with self.assertRaises(RuntimeError) as ctx:
            self._drive(go())
        self.assertIn('close failed: b', str(ctx.exception))
        self.assertEqual(
            [e for e in log if e[0] == 'close'],
            [('close', 'a'), ('close', 'b'), ('close', 'c')])
        self.assertEqual((a.closed, c.closed), (1, 1))
        self.assertIs(self.runtime.state, _RuntimeState.STOPPED)
        self.assertEqual(self.runtime.live_resources(), [])

    def test_start_issued_bind_is_drained_before_running(self):
        # A module's start() may enqueue a startup bind; because binds drain
        # AFTER the module phase but still during STARTING, it runs before the
        # kickoff flush -- never fire-and-forget after RUNNING.
        order = []

        class BindingModule(object):
            def __init__(self, runtime):
                self._runtime = runtime

            async def start(self):
                order.append('start')

                async def late_bind():
                    order.append('bind')
                self._runtime.addStartupBind(late_bind)

            async def close(self):
                order.append('close')

        self.runtime.register_module(BindingModule(self.runtime))

        def on_running():
            order.append('kickoff')
            current_runtime().stop()

        async def go():
            current_runtime().callWhenRunning(on_running)
            await self.runtime.start_all()
            await self.runtime.run_async()
            await self.runtime._finish()

        self._drive(go())
        self.assertLess(order.index('bind'), order.index('kickoff'))
        self.assertEqual(order[0], 'start')
        self.assertIn('close', order)

    def test_retained_module_counts_as_live_resource(self):
        # A registered-but-unclosed module makes the runtime non-empty: it holds
        # a strong ref and an unrun close() hook, so live_resources() must name
        # it. Otherwise install_runtime()/reset() would orphan it silently.
        a = _RecordingModule('a', [])
        self.assertEqual(self.runtime.live_resources(), [])
        self.runtime.register_module(a)
        self.assertIn('modules', self.runtime.live_resources())

    def test_reset_rejects_runtime_with_retained_module(self):
        # reset() must refuse while a module is still retained -- discarding it
        # would drop the close() hook. The guard names the live registry.
        self.runtime.register_module(_RecordingModule('a', []))
        with self.assertRaises(RuntimeError) as ctx:
            self.runtime.reset(self.loop)
        self.assertIn('modules', str(ctx.exception))

    def test_install_rejects_replacing_runtime_with_retained_module(self):
        # install_runtime() must refuse to supersede an idle runtime that still
        # retains a module, rather than silently orphaning it unclosed.
        self.runtime.register_module(_RecordingModule('a', []))
        replacement = AsyncTestRuntime()
        with self.assertRaises(RuntimeError) as ctx:
            install_runtime(replacement)
        self.assertIn('modules', str(ctx.exception))
        # The incumbent is untouched; the replacement was not installed.
        self.assertIs(get_current_runtime(), self.runtime)

    def test_shutdown_then_reset_clears_module_registries(self):
        # After the ordered shutdown closes the modules, the registries are
        # empty and reset() (in lockstep with __init__) keeps them empty so a
        # reused runtime does not re-close the previous run's modules.
        log = []
        a = _RecordingModule('a', log)
        self.runtime.register_module(a)

        async def go():
            current_runtime().callWhenRunning(current_runtime().stop)
            await self.runtime.start_all()
            await self.runtime.run_async()
            await self.runtime._finish()

        self._drive(go())
        self.assertEqual(a.closed, 1)
        self.assertEqual(self.runtime.live_resources(), [])
        # Shutdown already emptied the registries; reset() must keep them empty.
        self.runtime.reset(self.loop)
        self.assertEqual(self.runtime._modules, [])
        self.assertEqual(self.runtime._started_modules, [])
        self.assertIsNone(self.runtime._startup_task)


# --------------------------------------------------------------------------- #
# B1.1: test_runner._main -- construction under a running loop, guaranteed
# teardown + detach on every exit path (including constructor/module failures)
# --------------------------------------------------------------------------- #
class MainEntrypointTests(unittest.TestCase):
    """test_runner._main() installs one runtime, builds the test object under a
    running loop, and always runs the ordered teardown + detaches -- even when a
    constructor or module load raises after registering resources."""

    def setUp(self):
        import asterisk.test_runner as test_runner
        self.test_runner = test_runner
        self._orig_create = test_runner.create_test_object
        self._orig_load = test_runner.load_test_modules
        # Ensure a clean holder before each case.
        detach_runtime()

    def tearDown(self):
        self.test_runner.create_test_object = self._orig_create
        self.test_runner.load_test_modules = self._orig_load
        detach_runtime()
        asyncio.set_event_loop(None)

    class _Global(object):
        config = None

    def _run_main(self):
        result = {}
        asyncio.run(self.test_runner._main('/fake/dir', {}, result))
        return result

    def test_main_runs_under_running_loop_and_detaches(self):
        captured = {}

        tc = self

        class Obj(object):
            def __init__(self):
                self.passed = True
                self.global_config = tc._Global()
                # Construction happens with a live loop AND a live runtime.
                captured['loop_running'] = asyncio.get_running_loop().is_running()
                captured['runtime'] = get_current_runtime()
                current_runtime().callWhenRunning(
                    lambda: current_runtime().callLater(0.01, current_runtime().stop))

        self.test_runner.create_test_object = lambda d, c: Obj()
        self.test_runner.load_test_modules = lambda c, o: None

        result = self._run_main()
        self.assertTrue(captured['loop_running'])
        self.assertIsNotNone(captured['runtime'])
        self.assertIs(captured['runtime'].state, _RuntimeState.STOPPED)
        self.assertEqual(captured['runtime'].live_resources(), [])
        self.assertIsNone(get_current_runtime())      # detached
        self.assertTrue(result['test_object'].passed)

    def test_main_tears_down_after_constructor_failure(self):
        captured = {}
        tc = self

        class Obj(object):
            def __init__(self):
                self.global_config = tc._Global()
                # Register a resource, THEN fail -- it must not leak.
                captured['runtime'] = get_current_runtime()
                captured['timer'] = current_runtime().callLater(30.0, lambda: None)
                raise RuntimeError('constructor blew up')

        self.test_runner.create_test_object = lambda d, c: Obj()
        self.test_runner.load_test_modules = lambda c, o: None

        with self.assertRaises(RuntimeError):
            self._run_main()
        rt = captured['runtime']
        self.assertIs(rt.state, _RuntimeState.STOPPED)
        self.assertEqual(rt.live_resources(), [])       # timer torn down
        self.assertFalse(captured['timer'].active())
        self.assertIsNone(get_current_runtime())         # detached

    def test_main_tears_down_after_module_load_failure(self):
        captured = {}
        tc = self

        class Obj(object):
            def __init__(self):
                self.passed = True
                self.global_config = tc._Global()

        def bad_load(config, obj):
            # A module registers a resource, then loading raises.
            captured['runtime'] = get_current_runtime()
            captured['timer'] = current_runtime().callLater(30.0, lambda: None)
            raise RuntimeError('module load failed')

        self.test_runner.create_test_object = lambda d, c: Obj()
        self.test_runner.load_test_modules = bad_load

        with self.assertRaises(RuntimeError):
            self._run_main()
        rt = captured['runtime']
        self.assertIs(rt.state, _RuntimeState.STOPPED)
        self.assertEqual(rt.live_resources(), [])
        self.assertIsNone(get_current_runtime())

    def test_main_constructor_failure_after_startup_bind_no_leak(self):
        # Constructor queues an addStartupBind (state COLLECTING -> lands in
        # _pending_binds as an un-run factory) THEN raises. _main's teardown
        # must clear the queues -- no leaked pending_binds/when_running and no
        # "coroutine was never awaited" warning (the factory never ran).
        captured = {}
        tc = self

        class Obj(object):
            def __init__(self):
                self.global_config = tc._Global()
                captured['runtime'] = get_current_runtime()

                async def never_awaited():
                    return 'unused'
                current_runtime().addStartupBind(never_awaited)
                current_runtime().callWhenRunning(lambda: None)   # queues _when_running
                raise RuntimeError('constructor blew up after bind')

        self.test_runner.create_test_object = lambda d, c: Obj()
        self.test_runner.load_test_modules = lambda c, o: None

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            with self.assertRaises(RuntimeError):
                self._run_main()

        rt = captured['runtime']
        self.assertIs(rt.state, _RuntimeState.STOPPED)
        self.assertEqual(rt.live_resources(), [])        # no queue leak
        self.assertIsNone(get_current_runtime())          # detached
        never_awaited = [w for w in caught
                         if 'never awaited' in str(w.message)]
        self.assertEqual(never_awaited, [])

    def test_main_detaches_even_when_finish_raises(self):
        # If the ordered teardown itself raises, _main must still land the
        # holder detached and the runtime in STOPPED (nested finally), and it
        # must NOT mask the original setup exception.
        captured = {}
        tc = self

        class Obj(object):
            def __init__(self):
                self.global_config = tc._Global()
                captured['runtime'] = get_current_runtime()
                raise RuntimeError('original setup failure')

        self.test_runner.create_test_object = lambda d, c: Obj()
        self.test_runner.load_test_modules = lambda c, o: None

        # Force the ordered _shutdown to blow up during teardown.
        rt_box = {}

        async def exploding_shutdown():
            raise ValueError('teardown exploded')

        orig_create = self.test_runner.create_test_object

        def create_and_sabotage(d, c):
            rt = get_current_runtime()
            rt_box['rt'] = rt
            rt._shutdown = exploding_shutdown
            return orig_create(d, c)

        self.test_runner.create_test_object = create_and_sabotage

        # The ORIGINAL setup exception must propagate, not the teardown error.
        with self.assertRaises(RuntimeError) as ctx:
            self._run_main()
        self.assertIn('original setup failure', str(ctx.exception))

        rt = rt_box['rt']
        self.assertIs(rt.state, _RuntimeState.STOPPED)    # final state cleaned
        self.assertIsNone(get_current_runtime())           # still detached

    def test_main_propagates_teardown_failure_on_clean_run(self):
        # Clean setup + run, but the ordered teardown fails. Since nothing was
        # in flight, the teardown failure is a REAL failure and must propagate
        # -- a test with broken teardown must not report success. The runtime
        # is still detached.
        rt_box = {}
        tc = self

        class Obj(object):
            def __init__(self):
                self.passed = True
                self.global_config = tc._Global()
                current_runtime().callWhenRunning(
                    lambda: current_runtime().callLater(0.01, current_runtime().stop))

        async def exploding_shutdown():
            raise ValueError('teardown exploded on a clean run')

        def create_and_sabotage(d, c):
            rt = get_current_runtime()
            rt_box['rt'] = rt
            rt._shutdown = exploding_shutdown
            return Obj()

        self.test_runner.create_test_object = create_and_sabotage
        self.test_runner.load_test_modules = lambda c, o: None

        # The teardown failure must surface (not be suppressed).
        with self.assertRaises(ValueError) as ctx:
            self._run_main()
        self.assertIn('teardown exploded', str(ctx.exception))

        rt = rt_box['rt']
        self.assertIs(rt.state, _RuntimeState.STOPPED)
        self.assertIsNone(get_current_runtime())           # still detached

    def test_main_teardown_failure_does_not_mask_fatal_task(self):
        # A fatal background task stores runtime._failure and stops the run
        # *normally* (no exception unwinds through the finally). If the ordered
        # teardown then ALSO fails, the teardown error must be suppressed and
        # the ORIGINAL fatal error is what propagates -- otherwise a broken
        # teardown would hide the real cause of failure (finding 2).
        rt_box = {}
        tc = self

        async def boom():
            raise RuntimeError('original fatal task failure')

        class Obj(object):
            def __init__(self):
                self.passed = True
                self.global_config = tc._Global()

                def spawn_fatal():
                    get_current_runtime().create_task(boom(), fatal=True)
                current_runtime().callWhenRunning(spawn_fatal)

        async def exploding_shutdown():
            raise ValueError('teardown exploded while a fatal was pending')

        def create_and_sabotage(d, c):
            rt = get_current_runtime()
            rt_box['rt'] = rt
            rt._shutdown = exploding_shutdown
            return Obj()

        self.test_runner.create_test_object = create_and_sabotage
        self.test_runner.load_test_modules = lambda c, o: None

        # The fatal task error propagates, NOT the teardown ValueError. The
        # suppressed teardown error is LOGGER.exception'd by _main; silence it
        # so the expected traceback does not clutter the test output.
        tr_logger = logging.getLogger('test_runner')
        prev_level = tr_logger.level
        tr_logger.setLevel(logging.CRITICAL)
        try:
            with self.assertRaises(RuntimeError) as ctx:
                self._run_main()
        finally:
            tr_logger.setLevel(prev_level)
        self.assertIn('original fatal task failure', str(ctx.exception))

        rt = rt_box['rt']
        self.assertIs(rt.state, _RuntimeState.STOPPED)
        self.assertIsNone(get_current_runtime())           # still detached


# --------------------------------------------------------------------------- #
# B1.4: teardown late-registration policy -- once ordered shutdown is running
# (state STOPPING) every registration entry point refuses cleanly, so a callback
# firing during teardown cannot slip a resource past the snapshot-based cleanup
# or queue work that survives the run (design doc point 4).
# --------------------------------------------------------------------------- #
class TeardownPolicyTests(_LoopTestCase):
    """A callback firing during ordered shutdown must not register live work."""

    def _drive(self, coro):
        return self.loop.run_until_complete(coro)

    def test_registrations_during_teardown_are_refused(self):
        # An async cleanup runs *inside* _shutdown (state STOPPING). From there we
        # attempt every kind of late registration and prove each is refused: no
        # timer fires, no kickoff fires, no port/task is tracked, and no deferred
        # cleanup is queued -- the runtime lands STOPPED owning nothing.
        seen = {}
        fired = {'timer': False, 'kickoff': False, 'late_cleanup': False,
                 'from_thread': False, 'in_thread': False}

        async def late_cleanup():
            fired['late_cleanup'] = True

        class _LateModule(object):
            pass

        async def probe():
            # Runs during teardown.
            seen['state'] = self.runtime.state

            dc = self.runtime.callLater(
                0, lambda: fired.__setitem__('timer', True))
            seen['timer_active'] = dc.active()
            seen['timer_tracked'] = dc in self.runtime._delayed_calls

            self.runtime.callWhenRunning(
                lambda: fired.__setitem__('kickoff', True))
            seen['kickoff_queued'] = len(self.runtime._when_running)

            port = self.runtime.listenTCP(0, _DroppingServerFactory())
            seen['port_closed'] = port._closed
            seen['port_tracked'] = port in self.runtime._ports

            udp = self.runtime.listenUDP(0, _EchoServer({}, {}))
            seen['udp_closed'] = udp._closed
            seen['udp_tracked'] = udp in self.runtime._ports

            conn = self.runtime.connectTCP(
                '127.0.0.1', 9, _RecordingClientFactory([]))
            seen['conn_stopped'] = conn._stopped
            seen['conn_tracked'] = conn in self.runtime._connectors

            proc = self.runtime.spawnProcess(
                _CollectingProcess({}), sys.executable,
                [sys.executable, '-c', 'pass'])
            seen['proc_returned'] = proc is not None
            seen['proc_tracked'] = len(self.runtime._process_transports)

            seen['task_result'] = self.runtime.create_task(asyncio.sleep(10))
            seen['tasks_len'] = len(self.runtime._tasks)

            # callInThread must FIRE its Deferred (not hang shutdown).
            in_thread_d = self.runtime.callInThread(
                lambda: fired.__setitem__('in_thread', True))
            seen['in_thread_fired'] = in_thread_d.called

            self.runtime.addStartupBind(lambda: asyncio.sleep(0))
            seen['binds_queued'] = len(self.runtime._pending_binds)

            self.runtime.addAsyncCleanup(late_cleanup)
            # The current cleanup list is mid-iteration; a late add must not be
            # appended for deferral.
            seen['cleanups_len'] = len(self.runtime._async_cleanups)

            self.runtime.callFromThread(
                lambda: fired.__setitem__('from_thread', True))

            mod = _LateModule()
            self.runtime.register_module(mod)
            seen['module_tracked'] = mod in self.runtime._modules

        async def go():
            self.runtime.addAsyncCleanup(probe)
            current_runtime().callWhenRunning(current_runtime().stop)
            await self.runtime.start_all()
            await self.runtime.run_async()
            await self.runtime._finish()

        self._drive(go())
        self.assertIs(seen['state'], _RuntimeState.STOPPING)
        self.assertFalse(seen['timer_active'])      # callLater handed back inert
        self.assertFalse(seen['timer_tracked'])     # not added to the registry
        self.assertEqual(seen['kickoff_queued'], 0)  # callWhenRunning refused
        self.assertTrue(seen['port_closed'])        # listenTCP returned closed
        self.assertFalse(seen['port_tracked'])      # not added to the registry
        self.assertTrue(seen['udp_closed'])         # listenUDP returned closed
        self.assertFalse(seen['udp_tracked'])       # not added to the registry
        self.assertTrue(seen['conn_stopped'])       # connectTCP returned stopped
        self.assertFalse(seen['conn_tracked'])      # not added to the registry
        self.assertTrue(seen['proc_returned'])      # spawnProcess handed a proxy
        self.assertEqual(seen['proc_tracked'], 0)   # no transport registered
        self.assertIsNone(seen['task_result'])      # create_task refused (None)
        self.assertEqual(seen['tasks_len'], 0)      # nothing scheduled/tracked
        self.assertTrue(seen['in_thread_fired'])    # callInThread Deferred fired
        self.assertEqual(seen['binds_queued'], 0)   # addStartupBind refused
        # Only 'probe' itself is in the cleanup list (the late add was refused).
        self.assertEqual(seen['cleanups_len'], 1)
        self.assertFalse(seen['module_tracked'])    # register_module refused

        self.assertFalse(fired['timer'])            # never fired
        self.assertFalse(fired['kickoff'])          # never fired
        self.assertFalse(fired['late_cleanup'])     # never deferred/run
        self.assertFalse(fired['from_thread'])      # callFromThread refused
        self.assertFalse(fired['in_thread'])        # callInThread never ran fn
        self.assertIs(self.runtime.state, _RuntimeState.STOPPED)
        self.assertEqual(self.runtime.live_resources(), [])

    def test_stopped_runtime_refuses_registrations(self):
        # A runtime left STOPPED after a completed run is a spent teardown
        # target, not a valid registration surface. Every entry point must refuse
        # until reset() returns it to COLLECTING (design doc point 4: STOPPED is
        # included in the teardown gate on purpose).
        fired = {'timer': False, 'kickoff': False}

        async def go():
            current_runtime().callWhenRunning(current_runtime().stop)
            await self.runtime.start_all()
            await self.runtime.run_async()
            await self.runtime._finish()

        self._drive(go())
        self.assertIs(self.runtime.state, _RuntimeState.STOPPED)

        dc = self.runtime.callLater(0, lambda: fired.__setitem__('timer', True))
        self.assertFalse(dc.active())
        self.assertNotIn(dc, self.runtime._delayed_calls)

        self.runtime.callWhenRunning(
            lambda: fired.__setitem__('kickoff', True))
        self.assertEqual(self.runtime._when_running, [])

        port = self.runtime.listenTCP(0, _DroppingServerFactory())
        self.assertTrue(port._closed)
        self.assertNotIn(port, self.runtime._ports)

        # create_task refuses without scheduling: returns None, coro not left
        # un-awaited, nothing tracked.
        self.assertIsNone(self.runtime.create_task(asyncio.sleep(10)))
        self.assertEqual(self.runtime._tasks, set())

        # callInThread fires its Deferred (no hang) without submitting work.
        in_thread_d = self.runtime.callInThread(lambda: None)
        self.assertTrue(in_thread_d.called)

        mod = object()
        self.runtime.register_module(mod)
        self.assertNotIn(mod, self.runtime._modules)

        # Nothing was queued for a hypothetical next run.
        self.assertEqual(self.runtime.live_resources(), [])

    def test_refusals_are_safe_after_loop_closed(self):
        # Production condition: after a run the loop is CLOSED. Every refusal
        # path must decline without touching the closed loop -- no
        # "Event loop is closed" RuntimeError -- and callInThread must still
        # fire its Deferred rather than deadlock an awaiter.
        reports = []
        self.loop.set_exception_handler(lambda loop, ctx: reports.append(ctx))
        holder = {}

        async def boom():
            raise ValueError('failed before loop closed')

        async def go():
            current_runtime().callWhenRunning(current_runtime().stop)
            await self.runtime.start_all()
            await self.runtime.run_async()
            # Create + finish a failing task while the loop is open; hand it to
            # track_task only AFTER the loop is closed.
            holder['failed'] = asyncio.ensure_future(boom())
            await asyncio.sleep(0)                 # let boom raise
            await self.runtime._finish()

        self._drive(go())
        self.assertIs(self.runtime.state, _RuntimeState.STOPPED)
        self.assertTrue(holder['failed'].done())

        # Close the run's loop, mimicking asyncio.run() teardown.
        self.loop.close()
        self.assertTrue(self.loop.is_closed())

        # callLater: inert, never scheduled on the closed loop.
        dc = self.runtime.callLater(0, lambda: None)
        self.assertFalse(dc.active())

        # callFromThread: no-op, no raise.
        self.runtime.callFromThread(lambda: None)

        # callInThread: immediately-fired (failed) Deferred, no executor work.
        d = self.runtime.callInThread(lambda: None)
        self.assertTrue(d.called)

        # create_task: refused, returns None, coroutine closed (not scheduled).
        self.assertIsNone(self.runtime.create_task(asyncio.sleep(10)))
        self.assertEqual(self.runtime._tasks, set())

        # track_task on an ALREADY-FAILED task: exception retrieved
        # SYNCHRONOUSLY (a done-callback would schedule through the closed loop
        # and raise) and reported.
        self.runtime.track_task(holder['failed'], fatal=False)
        self.assertNotIn(holder['failed'], self.runtime._tasks)
        self.assertTrue(any(isinstance(c.get('exception'), ValueError)
                            for c in reports))
        self.assertIsNone(self.runtime._failure)

    def test_midrun_shim_timer_is_torn_down_by_runtime_shutdown(self):
        # Single-registry ownership: a callLater scheduled mid-run through
        # current_runtime() lands in the runtime's own registry and is cancelled
        # by the ordered shutdown -- it neither fires after the run nor leaks.
        fired = {'late': False}

        def on_running():
            # A long timer that must NOT survive teardown.
            current_runtime().callLater(30, lambda: fired.__setitem__('late', True))
            current_runtime().stop()

        async def go():
            current_runtime().callWhenRunning(on_running)
            await self.runtime.start_all()
            await self.runtime.run_async()
            await self.runtime._finish()

        self._drive(go())
        self.assertFalse(fired['late'])
        self.assertEqual(self.runtime._delayed_calls, set())
        self.assertEqual(self.runtime.live_resources(), [])


# --------------------------------------------------------------------------- #
# B1.4: owned-task exception policy -- every runtime-owned task has its exception
# retrieved (no "task exception was never retrieved"); a fatal task stores the
# first failure and stops the runtime, a non-fatal one is dispatched to
# loop.call_exception_handler (design doc point 4).
# --------------------------------------------------------------------------- #
class OwnedTaskExceptionTests(_LoopTestCase):
    """create_task/track_task retain background tasks and govern their errors."""

    def _drive(self, coro):
        return self.loop.run_until_complete(coro)

    def test_nonfatal_task_exception_reported_not_fatal(self):
        reports = []
        self.loop.set_exception_handler(lambda loop, ctx: reports.append(ctx))

        async def boom():
            raise ValueError('non-fatal background boom')

        async def go():
            await self.runtime.start_all()          # -> RUNNING
            self.runtime.create_task(boom(), fatal=False)
            await asyncio.sleep(0)                    # let boom raise
            await asyncio.sleep(0)                    # let the done-callback run
            current_runtime().stop()
            await self.runtime.run_async()
            await self.runtime._finish()

        self._drive(go())
        # Retrieved + reported to the loop handler, run not aborted.
        self.assertTrue(any(isinstance(c.get('exception'), ValueError)
                            for c in reports))
        self.assertIsNone(self.runtime._failure)
        self.assertEqual(self.runtime._tasks, set())

    def test_fatal_task_stores_first_failure_and_stops(self):
        async def boom():
            raise RuntimeError('fatal background boom')

        async def go():
            await self.runtime.start_all()          # -> RUNNING
            self.runtime.create_task(boom(), fatal=True)
            # A fatal task drives stop() -> completion resolves -> run_async
            # returns without an explicit current_runtime().stop().
            await self.runtime.run_async()
            await self.runtime._finish()

        self._drive(go())
        self.assertIsInstance(self.runtime._failure, RuntimeError)
        self.assertIn('fatal background boom', str(self.runtime._failure))
        self.assertEqual(self.runtime._tasks, set())

    def test_tracked_task_is_retained_then_cancelled_at_shutdown(self):
        # The registry holds a strong ref (retention) and the ordered shutdown
        # cancels + drains it -- no leak, no early GC.
        box = {}

        async def worker():
            await asyncio.sleep(30)

        def on_running():
            box['task'] = self.runtime.create_task(worker())
            box['tracked'] = box['task'] in self.runtime._tasks
            current_runtime().stop()

        async def go():
            current_runtime().callWhenRunning(on_running)
            await self.runtime.start_all()
            await self.runtime.run_async()
            await self.runtime._finish()

        self._drive(go())
        self.assertTrue(box['tracked'])                 # retained while running
        self.assertTrue(box['task'].cancelled())        # torn down at shutdown
        self.assertEqual(self.runtime._tasks, set())
        self.assertEqual(self.runtime.live_resources(), [])

    def test_already_failed_task_adopted_during_teardown_is_reported(self):
        # A task handed to track_task after teardown has begun is NOT tracked and
        # is cancelled, but if it has ALREADY failed the cancel is a no-op and the
        # exception policy still runs: the error is retrieved (no "task exception
        # was never retrieved") and reported to the loop handler.
        reports = []
        self.loop.set_exception_handler(lambda loop, ctx: reports.append(ctx))
        seen = {}

        async def boom():
            raise ValueError('failed before adoption')

        async def probe():
            # Runs during teardown (STOPPING).
            failed = asyncio.ensure_future(boom())
            await asyncio.sleep(0)              # let boom raise; task now done
            self.assertTrue(failed.done())
            self.runtime.track_task(failed, fatal=False)
            seen['tracked'] = failed in self.runtime._tasks   # False: refused
            seen['cancelled'] = failed.cancelled()            # False: already done
            await asyncio.sleep(0)             # let the done-callback report

        async def go():
            self.runtime.addAsyncCleanup(probe)
            current_runtime().callWhenRunning(current_runtime().stop)
            await self.runtime.start_all()
            await self.runtime.run_async()
            await self.runtime._finish()

        self._drive(go())
        self.assertFalse(seen['tracked'])       # not adopted into the snapshot
        self.assertFalse(seen['cancelled'])     # cancel was a no-op on a done task
        # The already-set exception was retrieved and reported, not swallowed.
        self.assertTrue(any(isinstance(c.get('exception'), ValueError)
                            for c in reports))
        self.assertIsNone(self.runtime._failure)


# --------------------------------------------------------------------------- #
# B1.x review findings 1/2/3/4: teardown correctness -- ordered shutdown runs
# every phase best-effort and surfaces (never masks) the errors it collects,
# the legacy blocking run() preserves a fatal error across a failing teardown,
# and track_task refuses to silently leak a pending task on a closed loop.
# --------------------------------------------------------------------------- #
class TeardownCorrectnessTests(_LoopTestCase):

    def _drive(self, coro):
        return self.loop.run_until_complete(coro)

    def test_shutdown_continues_through_all_phases_on_port_failure(self):
        # Finding 1: a failing port stopListening() must NOT abort the phases
        # after it. The connector is still disconnected and the tracked task is
        # still cancelled/drained; the port error is re-raised only at the end.
        class _FailingPort(object):
            def stopListening(self):
                raise RuntimeError('port close failed')

        class _RecordingConnector(object):
            def __init__(self):
                self.disconnected = False

            def disconnect(self):
                self.disconnected = True

        port = _FailingPort()
        connector = _RecordingConnector()
        task = self.loop.create_task(asyncio.sleep(30))
        self.runtime._ports.append(port)
        self.runtime._connectors.append(connector)
        self.runtime._tasks.add(task)

        with self.assertRaises(RuntimeError) as ctx:
            self._drive(self.runtime._finish())
        self.assertIn('port close failed', str(ctx.exception))

        # Phases after the failing port still ran.
        self.assertTrue(connector.disconnected)
        self.assertTrue(task.cancelled())
        self.assertIs(self.runtime.state, _RuntimeState.STOPPED)
        self.assertEqual(self.runtime.live_resources(), [])

    def test_shutdown_aggregates_multiple_cleanup_errors(self):
        # Finding 4: an async-cleanup failure and a module close() failure are
        # both isolated AND both reported -- aggregated into an ExceptionGroup
        # rather than one silently discarded.
        async def bad_cleanup():
            raise ValueError('cleanup failed')

        class _FailingCloseModule(object):
            async def close(self):
                raise KeyError('close failed')

        self.runtime.addAsyncCleanup(bad_cleanup)
        self.runtime._started_modules.append(_FailingCloseModule())

        with self.assertRaises(ExceptionGroup) as ctx:
            self._drive(self.runtime._finish())
        kinds = {type(e) for e in ctx.exception.exceptions}
        self.assertEqual(kinds, {ValueError, KeyError})
        self.assertIs(self.runtime.state, _RuntimeState.STOPPED)
        self.assertEqual(self.runtime.live_resources(), [])

    def test_shutdown_kills_child_when_waitforexit_raises(self):
        # A waitForExit() that RAISES (not just times out) must NOT leave the
        # child alive: the fix drives kill() off recomputed live state (return
        # code still None), not off a timeout flag. Assert the child was actually
        # killed -- asserting only loseConnection() ran would mask the leak.
        class _FakeTransport(object):
            def __init__(self):
                self.rc = None
                self.killed = False
                self.closed = False

            def get_returncode(self):
                return self.rc

            def terminate(self):
                pass                       # stays live -> awaited for exit

            async def waitForExit(self):
                # Raises while alive; once kill() records an exit, reports it so
                # the second reap does not re-raise.
                if self.rc is None:
                    raise RuntimeError('waitForExit blew up')
                return self.rc

            def kill(self):
                self.rc = -9
                self.killed = True

            def loseConnection(self):
                self.closed = True

        transport = _FakeTransport()
        task = self.loop.create_task(asyncio.sleep(30))
        self.runtime._process_transports.append(transport)
        self.runtime._tasks.add(task)

        with self.assertRaises(RuntimeError) as ctx:
            self._drive(self.runtime._finish())
        self.assertIn('waitForExit blew up', str(ctx.exception))

        self.assertTrue(transport.killed)      # child was actually killed
        self.assertEqual(transport.rc, -9)     # return code became non-None
        self.assertTrue(transport.closed)      # loseConnection still ran
        self.assertTrue(task.cancelled())      # task phase still ran
        self.assertIs(self.runtime.state, _RuntimeState.STOPPED)
        self.assertEqual(self.runtime.live_resources(), [])

    def test_shutdown_kills_child_when_terminate_raises(self):
        # A terminate() that RAISES must not remove the child from the live set:
        # the child keeps running, so the recomputed-live kill phase must still
        # reach it. Assert the child was killed, not just closed.
        class _FakeTransport(object):
            def __init__(self):
                self.rc = None
                self.killed = False
                self.closed = False

            def get_returncode(self):
                return self.rc

            def terminate(self):
                raise RuntimeError('terminate blew up')

            async def waitForExit(self):
                # Models a child that ignored the (failed) terminate: the reap
                # completes without an exit until kill() records one.
                return self.rc

            def kill(self):
                self.rc = -9
                self.killed = True

            def loseConnection(self):
                self.closed = True

        transport = _FakeTransport()
        task = self.loop.create_task(asyncio.sleep(30))
        self.runtime._process_transports.append(transport)
        self.runtime._tasks.add(task)

        with self.assertRaises(RuntimeError) as ctx:
            self._drive(self.runtime._finish())
        self.assertIn('terminate blew up', str(ctx.exception))

        self.assertTrue(transport.killed)      # child killed despite terminate error
        self.assertEqual(transport.rc, -9)
        self.assertTrue(transport.closed)      # loseConnection still ran
        self.assertTrue(task.cancelled())      # task phase still ran
        self.assertIs(self.runtime.state, _RuntimeState.STOPPED)
        self.assertEqual(self.runtime.live_resources(), [])

    def test_connector_disconnect_closes_transport_even_if_stoptrying_raises(self):
        # _Connector.disconnect(): a raising factory.stopTrying() must not leave
        # the transport open -- it is closed in finally, and the stopTrying error
        # still propagates (so the shutdown path can collect it).
        class _RaisingFactory(object):
            def stopTrying(self):
                raise RuntimeError('stopTrying blew up')

        class _RecordingTransport(object):
            def __init__(self):
                self.closed = False

            def close(self):
                self.closed = True

        transport = _RecordingTransport()
        connector = _Connector(self.runtime, '127.0.0.1', 9,
                               _RaisingFactory(), None, None)
        connector._transport = transport

        with self.assertRaises(RuntimeError) as ctx:
            connector.disconnect()
        self.assertIn('stopTrying blew up', str(ctx.exception))
        self.assertTrue(transport.closed)         # closed despite the raise
        self.assertIsNone(connector._transport)   # cleared

    def test_track_task_rejects_pending_task_on_closed_loop(self):
        # Finding 3: adopting a still-pending task whose loop is already closed
        # cannot be drained (touching a closed loop raises), so it must be
        # rejected loudly rather than silently left alive past the run. Modelled
        # with a stand-in exposing exactly the two attributes the reject branch
        # reads -- a real pending Task on a closed loop would otherwise leak and
        # emit a spurious "Task was destroyed but it is pending" warning.
        closed = asyncio.new_event_loop()
        closed.close()

        class _PendingOnClosedLoop(object):
            def done(self):
                return False

            def get_loop(self):
                return closed

        # Force the teardown gate so track_task takes the STOPPING branch.
        self.runtime._state = _RuntimeState.STOPPING
        with self.assertRaises(RuntimeError) as ctx:
            self.runtime.track_task(_PendingOnClosedLoop(), fatal=False)
        self.assertIn('closed loop', str(ctx.exception))

    def test_legacy_run_teardown_failure_does_not_mask_fatal_task(self):
        # Finding 2 (legacy path): the blocking run() must snapshot the fatal
        # _failure and, if teardown ALSO fails, suppress the teardown error and
        # re-raise the original fatal -- and still land STOPPED (via _finish()).
        async def boom():
            raise RuntimeError('original fatal legacy failure')

        async def exploding_shutdown():
            raise ValueError('legacy teardown exploded')

        def on_running():
            self.runtime.create_task(boom(), fatal=True)

        self.runtime._shutdown = exploding_shutdown
        current_runtime().callWhenRunning(on_running)

        # The suppressed teardown error is LOGGER.exception'd; silence it so the
        # expected-and-swallowed traceback does not clutter the test output.
        rt_logger = logging.getLogger('asterisk.aio.runtime')
        prev_level = rt_logger.level
        rt_logger.setLevel(logging.CRITICAL)
        try:
            with self.assertRaises(RuntimeError) as ctx:
                self.runtime.run()
        finally:
            rt_logger.setLevel(prev_level)
        self.assertIn('original fatal legacy failure', str(ctx.exception))
        self.assertIs(self.runtime.state, _RuntimeState.STOPPED)


class _RunTestObjectFake(object):
    """Minimal ``run-test`` object for exercising ``run_test_object``.

    Mirrors the shape a real run-test relies on: construction registers a
    ``callWhenRunning`` kickoff that records it ran, flips ``passed``, and stops
    the runtime so ``run_async()`` returns. Construction, kickoff, and hooks all
    append to a shared ``log`` so ordering can be asserted. An optional
    ``on_running`` callback fires inside the kickoff (before ``stop()``) to let a
    test inject mid-run behavior (e.g. a fatal owned task).
    """

    def __init__(self, log, on_running=None):
        self.log = log
        self.passed = False
        self._on_running_extra = on_running
        log.append('construct')
        current_runtime().callWhenRunning(self._kick)

    def _kick(self):
        self.log.append('running')
        self.passed = True
        if self._on_running_extra is not None:
            self._on_running_extra(self)
        current_runtime().stop()


class RunTestObjectTests(unittest.TestCase):
    """B2.1: ``run_test_object`` builds the object inside the loop via a factory,
    drives the B1 lifecycle, runs optional in-loop ``before_start``/``after_run``
    hooks (before the loop closes), returns the object after shutdown, and
    detaches the runtime.

    Not a ``_LoopTestCase``: ``run_test_object`` owns the loop end-to-end through
    ``asyncio.run()``, so these drive it synchronously with no ambient loop.
    """

    def setUp(self):
        # Start from an empty holder so the helper's new_runtime install is
        # unobstructed regardless of a prior test's runtime.
        detach_runtime()

    def tearDown(self):
        detach_runtime()

    def test_constructor_only_runs_and_returns_object(self):
        # Bare factory form: build, run, tear down, return the object.
        log = []
        test = run_test_object(lambda: _RunTestObjectFake(log))
        self.assertIsNotNone(test)
        self.assertTrue(test.passed)
        self.assertEqual(log, ['construct', 'running'])
        # Runtime detached after the run (holder empty for the next run).
        self.assertIsNone(get_current_runtime())

    def test_hooks_run_inside_loop_in_order(self):
        # before_start runs before the kickoff; after_run runs after it. The
        # exact order proves before_start is pre-run and after_run is post-run.
        log = []
        test = run_test_object(
            lambda: _RunTestObjectFake(log),
            before_start=lambda t: log.append('before'),
            after_run=lambda t: log.append('after'))
        self.assertEqual(log, ['construct', 'before', 'running', 'after'])
        self.assertTrue(test.passed)

    def test_after_run_executes_before_loop_closes(self):
        # The B2 invariant: a post-run, loop-dependent step must run BEFORE
        # asyncio.run() closes the loop. Capture the loop inside after_run and
        # assert it was open there but closed once the helper returned.
        seen = {}

        def after_run(_test):
            loop = asyncio.get_running_loop()
            seen['loop'] = loop
            seen['closed_during'] = loop.is_closed()

        run_test_object(lambda: _RunTestObjectFake([]), after_run=after_run)
        self.assertIn('loop', seen)
        self.assertFalse(seen['closed_during'])    # loop live inside after_run
        self.assertTrue(seen['loop'].is_closed())  # closed after the helper

    def test_async_hooks_are_awaited(self):
        # Hooks may be coroutine functions; the helper awaits them in place.
        log = []

        async def before_start(_test):
            await asyncio.sleep(0)
            log.append('before-async')

        async def after_run(_test):
            await asyncio.sleep(0)
            log.append('after-async')

        run_test_object(lambda: _RunTestObjectFake(log),
                        before_start=before_start, after_run=after_run)
        self.assertEqual(
            log, ['construct', 'before-async', 'running', 'after-async'])

    def test_factory_returning_none_returns_none(self):
        # A factory that builds nothing: no object to run, helper returns None,
        # teardown still runs, and the runtime is detached.
        self.assertIsNone(run_test_object(lambda: None))
        self.assertIsNone(get_current_runtime())

    def test_before_start_failure_propagates_and_detaches(self):
        # A raising pre-run hook aborts before startup; the error propagates and
        # the runtime is still torn down and detached.
        def before_start(_test):
            raise ValueError('pre-run boom')

        log = []
        with self.assertRaises(ValueError) as ctx:
            run_test_object(lambda: _RunTestObjectFake(log),
                            before_start=before_start)
        self.assertIn('pre-run boom', str(ctx.exception))
        self.assertEqual(log, ['construct'])   # kickoff never flushed
        self.assertIsNone(get_current_runtime())

    def test_after_run_failure_propagates_and_detaches(self):
        # A raising post-run hook on an otherwise-clean run is a real failure:
        # it propagates, and the runtime is still torn down and detached.
        def after_run(_test):
            raise ValueError('post-run boom')

        log = []
        with self.assertRaises(ValueError) as ctx:
            run_test_object(lambda: _RunTestObjectFake(log),
                            after_run=after_run)
        self.assertIn('post-run boom', str(ctx.exception))
        self.assertEqual(log, ['construct', 'running'])
        self.assertIsNone(get_current_runtime())

    def test_fatal_mid_run_error_propagates_after_teardown(self):
        # A fatal owned-task failure stores runtime._failure and stops the run;
        # run_test_object re-raises it AFTER teardown, and after_run (a
        # loop-dependent post-run step) still runs before the loop closes.
        log = []

        async def failing():
            raise RuntimeError('fatal mid-run')

        def on_running(_test):
            get_current_runtime().create_task(failing(), fatal=True)

        rt_logger = logging.getLogger('asterisk.aio.runtime')
        prev_level = rt_logger.level
        rt_logger.setLevel(logging.CRITICAL)
        try:
            with self.assertRaises(RuntimeError) as ctx:
                run_test_object(
                    lambda: _RunTestObjectFake(log, on_running=on_running),
                    after_run=lambda t: log.append('after'))
        finally:
            rt_logger.setLevel(prev_level)
        self.assertIn('fatal mid-run', str(ctx.exception))
        self.assertIn('after', log)   # post-run hook still ran, loop still open
        self.assertIsNone(get_current_runtime())

    def test_fatal_error_takes_precedence_over_failing_after_run(self):
        # Combined-failure precedence: when a fatal owned task has stored
        # _failure AND after_run raises, the ORIGINAL fatal must propagate --
        # the later hook error must not mask it. The hook still runs, and its
        # error is superseded (logged), not raised.
        log = []

        async def failing():
            raise RuntimeError('original fatal')

        def on_running(_test):
            get_current_runtime().create_task(failing(), fatal=True)

        def after_run(_test):
            log.append('after')
            raise ValueError('after hook failed')

        # The superseded hook error is logged on 'test_runner'; the fatal task's
        # retrieval logs on 'asterisk.aio.runtime'. Silence both.
        loggers = [logging.getLogger('asterisk.aio.runtime'),
                   logging.getLogger('test_runner')]
        prev = [lg.level for lg in loggers]
        for lg in loggers:
            lg.setLevel(logging.CRITICAL)
        try:
            with self.assertRaises(RuntimeError) as ctx:
                run_test_object(
                    lambda: _RunTestObjectFake(log, on_running=on_running),
                    after_run=after_run)
        finally:
            for lg, level in zip(loggers, prev):
                lg.setLevel(level)
        # Fatal is primary; the after_run ValueError did NOT win.
        self.assertIn('original fatal', str(ctx.exception))
        self.assertNotIsInstance(ctx.exception, ValueError)
        self.assertIn('after', log)   # after_run still ran before teardown
        self.assertIsNone(get_current_runtime())


class HandleOriginateFailureTests(unittest.TestCase):
    """Regression for B5.2a-1.

    After the ari.py await conversion, TestCase.handle_originate_failure()
    is called with a raw exception (the object re-raised by ``await`` on a
    starpy Deferred) rather than a Twisted-style Failure. It must degrade
    gracefully -- log, stop the test, and NOT raise AttributeError on the
    Failure-only methods getErrorMessage()/getTraceback().
    """

    def setUp(self):
        from asterisk import test_case as _tc
        self._tc = _tc
        # test_case.LOGGER is None until a live run assigns it; give the
        # error handler a real logger so it can format its messages.
        self._saved_logger = _tc.LOGGER
        _tc.LOGGER = logging.getLogger('test_case_handle_originate_regression')

    def tearDown(self):
        self._tc.LOGGER = self._saved_logger

    @staticmethod
    def _stub():
        class _Stub(object):
            def __init__(self):
                self.stopped = False

            def stop_reactor(self):
                self.stopped = True
        return _Stub()

    def test_raw_exception_stops_test_without_raising(self):
        stub = self._stub()
        ret = self._tc.TestCase.handle_originate_failure(
            stub, RuntimeError('boom'))
        self.assertTrue(stub.stopped)
        self.assertIsInstance(ret, RuntimeError)

    def test_failure_like_object_still_supported(self):
        class _FakeFailure(object):
            def getErrorMessage(self):
                return 'fail-message'

            def getTraceback(self):
                return 'fake-traceback'
        stub = self._stub()
        failure = _FakeFailure()
        ret = self._tc.TestCase.handle_originate_failure(stub, failure)
        self.assertTrue(stub.stopped)
        self.assertIs(ret, failure)


if __name__ == '__main__':
    unittest.main()
