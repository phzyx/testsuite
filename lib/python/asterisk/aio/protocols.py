"""asyncio replacements for the twisted protocol base classes the suite uses.

Provides the small subset of Twisted's protocol surface that the Asterisk test
suite actually subclasses:

  * ``DatagramProtocol`` - mirrors ``twisted.internet.protocol.DatagramProtocol``
    (``datagramReceived``/``startProtocol``/``stopProtocol`` plus a transport
    exposing ``write(data, addr)`` and ``loseConnection()``).
  * ``ProcessProtocol`` - mirrors ``twisted.internet.protocol.ProcessProtocol``
    (``connectionMade``/``outReceived``/``errReceived``/``processEnded``) on top
    of ``asyncio.SubprocessProtocol``.
  * ``LoopingCall`` - mirrors ``twisted.internet.task.LoopingCall``.
  * ``ProcessDone`` / ``ProcessTerminated`` - the ``reason.value`` types handed to
    ``processEnded``.

Design reference: doc/untwist/02-design.md Sections 3.3-3.4. These adapters are
the bridge between asyncio's transport/protocol callbacks and the Twisted-shaped
methods existing code overrides. They are part of the transitional ``asterisk.aio``
layer (design Section 14); Phase B migrates callers to native asyncio protocols.
"""

import asyncio

from .defer import Deferred
from .failure import Failure


# ---------------------------------------------------------------------------- #
# Process termination reasons
# ---------------------------------------------------------------------------- #
class ConnectionDone(Exception):
    """Connection closed cleanly (twisted.internet.error.ConnectionDone)."""


class ProcessDone(Exception):
    """A process exited cleanly (twisted.internet.error.ProcessDone)."""

    def __init__(self, status=0):
        super().__init__("process finished with exit code %s" % status)
        self.exitCode = status
        self.signal = None
        self.status = status


class ProcessTerminated(Exception):
    """A process exited with a non-zero status or signal.

    Mirrors twisted.internet.error.ProcessTerminated: callers read
    ``reason.value.exitCode`` (None when killed by signal) and ``.signal``.
    """

    def __init__(self, exitCode=None, signal=None, status=None):
        msg = "process ended"
        if exitCode is not None:
            msg += " with exit code %s" % exitCode
        if signal is not None:
            msg += " by signal %s" % signal
        super().__init__(msg)
        self.exitCode = exitCode
        self.signal = signal
        self.status = status


# ---------------------------------------------------------------------------- #
# Datagram (UDP) protocol adapter
# ---------------------------------------------------------------------------- #
class _DatagramWriter(object):
    """Transport wrapper giving a Twisted-style write(data, addr)/loseConnection."""

    def __init__(self, transport):
        self._transport = transport

    def write(self, data, addr=None):
        """Send ``data``; ``addr`` selects the destination as in Twisted."""
        self._transport.sendto(data, addr)

    def loseConnection(self):
        """Close the underlying datagram transport."""
        self._transport.close()

    # Some callers reach through for the raw socket / extra info.
    def getHost(self):
        return self._transport.get_extra_info('sockname')

    def __getattr__(self, name):
        return getattr(self._transport, name)


class DatagramProtocol(asyncio.DatagramProtocol):
    """Twisted-style DatagramProtocol backed by asyncio.DatagramProtocol.

    Subclasses override ``datagramReceived(data, addr)`` and optionally
    ``startProtocol``/``stopProtocol``. ``self.transport`` exposes
    ``write(data, addr)`` and ``loseConnection()``.
    """

    transport = None

    # asyncio callbacks --------------------------------------------------- #
    def connection_made(self, transport):
        self.transport = _DatagramWriter(transport)
        self.startProtocol()

    def datagram_received(self, data, addr):
        self.datagramReceived(data, addr)

    def connection_lost(self, exc):
        self.stopProtocol()

    def error_received(self, exc):
        # Twisted delivers most UDP errors silently; keep parity by ignoring.
        pass

    # Twisted-style overrides --------------------------------------------- #
    def startProtocol(self):
        """Called when the transport is connected (override as needed)."""

    def stopProtocol(self):
        """Called when the transport is closed (override as needed)."""

    def datagramReceived(self, data, addr):
        """Called with each received datagram (override in subclass)."""


# ---------------------------------------------------------------------------- #
# Process protocol adapter
# ---------------------------------------------------------------------------- #
class _ProcessTransportAdapter(object):
    """Expose the Twisted process-transport surface used by the suite."""

    def __init__(self, transport):
        self._transport = transport
        self.pid = transport.get_pid()

    def write(self, data):
        stdin = self._transport.get_pipe_transport(0)
        if stdin is not None:
            stdin.write(data)

    def closeStdin(self):
        stdin = self._transport.get_pipe_transport(0)
        if stdin is not None:
            stdin.close()

    def loseConnection(self):
        self._transport.close()

    def signalProcess(self, signal):
        """Send a signal; accepts a name ('KILL'/'TERM') or number like Twisted."""
        import signal as _signal
        if isinstance(signal, str):
            signal = getattr(_signal, 'SIG' + signal, None) or \
                getattr(_signal, signal)
        self._transport.send_signal(signal)

    def __getattr__(self, name):
        return getattr(self._transport, name)


class ProcessProtocol(asyncio.SubprocessProtocol):
    """Twisted-style ProcessProtocol backed by asyncio.SubprocessProtocol.

    Subclasses override ``connectionMade``, ``outReceived(data)``,
    ``errReceived(data)`` and ``processEnded(reason)`` where ``reason`` is a
    ``Failure`` wrapping ``ProcessDone`` or ``ProcessTerminated``.
    """

    # ``transport`` and the lifecycle flags are class-level defaults so that
    # subclasses (AsteriskProtocol, SIPpProtocol) need NOT call super().__init__
    # -- which they historically do not. Instance assignment shadows the class
    # default on first write. (review finding 1)
    transport = None
    _proc_exited = False
    _stdout_open = True
    _stderr_open = True
    _ended = False
    _returncode = None

    # asyncio callbacks --------------------------------------------------- #
    def connection_made(self, transport):
        self.transport = _ProcessTransportAdapter(transport)
        # Pipes that the child was not given do not produce a
        # pipe_connection_lost, so treat absent stdout/stderr as already closed.
        if transport.get_pipe_transport(1) is None:
            self._stdout_open = False
        if transport.get_pipe_transport(2) is None:
            self._stderr_open = False
        self.connectionMade()

    def pipe_data_received(self, fd, data):
        if fd == 1:
            self.outReceived(data)
        elif fd == 2:
            self.errReceived(data)

    def pipe_connection_lost(self, fd, exc):
        if fd == 0:
            self.inConnectionLost()
        elif fd == 1:
            self._stdout_open = False
            self.outConnectionLost()
        elif fd == 2:
            self._stderr_open = False
            self.errConnectionLost()
        self._maybe_end()

    def process_exited(self):
        # asyncio may deliver process_exited BEFORE the final pipe_data_received
        # / pipe_connection_lost callbacks. Record the exit but defer
        # processEnded until stdout and stderr have both drained, so no trailing
        # Asterisk/SIPp output is lost. (review finding 1)
        self._proc_exited = True
        self._returncode = self.transport.get_returncode()
        self._maybe_end()

    def _maybe_end(self):
        if self._ended:
            return
        if not (self._proc_exited and not self._stdout_open
                and not self._stderr_open):
            return
        self._ended = True
        returncode = self._returncode
        if returncode == 0 or returncode is None:
            reason = Failure(ProcessDone(returncode or 0))
        elif returncode < 0:
            reason = Failure(ProcessTerminated(exitCode=None,
                                               signal=-returncode,
                                               status=returncode))
        else:
            reason = Failure(ProcessTerminated(exitCode=returncode,
                                               status=returncode))
        try:
            self.processEnded(reason)
        finally:
            # Mirror Twisted: the process transport is finished once the child
            # has exited. Closing it also avoids asyncio ResourceWarnings.
            self.transport.loseConnection()

    # Twisted-style overrides --------------------------------------------- #
    def connectionMade(self):
        """Called once the process has started (override as needed)."""

    def outReceived(self, data):
        """Called with bytes from the process's stdout (override as needed)."""

    def errReceived(self, data):
        """Called with bytes from the process's stderr (override as needed)."""

    def inConnectionLost(self):
        """Called when the process's stdin is closed (override as needed)."""

    def outConnectionLost(self):
        """Called when the process's stdout is closed (override as needed)."""

    def errConnectionLost(self):
        """Called when the process's stderr is closed (override as needed)."""

    def processEnded(self, reason):
        """Called when the process has exited (override in subclass).

        ``reason`` is a Failure wrapping ProcessDone or ProcessTerminated.
        """


# ---------------------------------------------------------------------------- #
# LoopingCall
# ---------------------------------------------------------------------------- #
class LoopingCall(object):
    """Periodically call a function (twisted.internet.task.LoopingCall).

    ``start(interval, now=True)`` returns a Deferred that fires when the loop is
    stopped via ``stop()``. Exceptions raised by the function stop the loop and
    errback the Deferred, matching Twisted.
    """

    def __init__(self, f, *args, **kw):
        self.f = f
        self.args = args
        self.kw = kw
        self.running = False
        self.interval = None
        self._deferred = None
        self._handle = None
        self._loop = None

    def start(self, interval, now=True):
        """Start calling the function every ``interval`` seconds."""
        if self.running:
            raise AssertionError("LoopingCall already running")
        self.interval = interval
        self.running = True
        self._deferred = Deferred()
        self._loop = asyncio.get_event_loop()
        if now:
            self._run()
        else:
            self._schedule()
        return self._deferred

    def stop(self):
        """Stop the loop and fire the start() Deferred with this LoopingCall."""
        self.running = False
        if self._handle is not None:
            self._handle.cancel()
            self._handle = None
        if self._deferred is not None and not self._deferred._called:
            self._deferred.callback(self)

    def _schedule(self):
        if not self.running:
            return
        self._handle = self._loop.call_later(self.interval, self._run)

    def _run(self):
        if not self.running:
            return
        try:
            self.f(*self.args, **self.kw)
        except Exception:
            self.running = False
            if self._deferred is not None and not self._deferred._called:
                self._deferred.errback(Failure())
            return
        self._schedule()
