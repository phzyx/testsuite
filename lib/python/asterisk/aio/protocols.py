"""asyncio replacements for the twisted protocol base classes the suite uses.

Provides the small subset of Twisted's protocol surface that the Asterisk test
suite actually subclasses:

  * ``DatagramProtocol`` - mirrors ``twisted.internet.protocol.DatagramProtocol``
    (``datagramReceived``/``startProtocol``/``stopProtocol`` plus a transport
    exposing ``write(data, addr)`` and ``loseConnection()``).
  * ``ProcessProtocol`` - mirrors ``twisted.internet.protocol.ProcessProtocol``
    (``connectionMade``/``outReceived``/``errReceived``/``processEnded``) on top
    of ``asyncio.SubprocessProtocol``.
  * ``ProcessDone`` / ``ProcessTerminated`` - the ``reason.value`` types handed to
    ``processEnded``.

Design reference: doc/untwist/02-design.md Sections 3.3-3.4. These adapters are
the bridge between asyncio's transport/protocol callbacks and the Twisted-shaped
methods existing code overrides. They are part of the transitional ``asterisk.aio``
layer (design Section 14); Phase B migrates callers to native asyncio protocols.
"""

import asyncio
import os
import signal as _signal

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


class ProcessExitedAlready(Exception):
    """Signalling a process that has already exited.

    Mirrors ``twisted.internet.error.ProcessExitedAlready``: the process
    transport adapter raises this from ``signalProcess()`` when the underlying
    child is already gone (asyncio raises ``ProcessLookupError``).
    """


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
# Generic (stream/TCP) protocol + factory base classes
# ---------------------------------------------------------------------------- #
class Protocol(object):
    """Twisted-style stream Protocol base (twisted.internet.protocol.Protocol).

    Subclasses override ``connectionMade``/``dataReceived``/``connectionLost``.
    ``self.transport`` exposes ``write``/``writeSequence``/``loseConnection``/
    ``getPeer``/``getHost`` -- it is supplied by the reactor's connect/listen
    adapter, which calls ``makeConnection`` on the built protocol.
    """

    transport = None
    factory = None

    def makeConnection(self, transport):
        """Bind the transport and fire ``connectionMade`` (Twisted contract)."""
        self.transport = transport
        self.connectionMade()

    # Twisted-style overrides --------------------------------------------- #
    def connectionMade(self):
        """Called when a connection is established (override as needed)."""

    def dataReceived(self, data):
        """Called with each chunk of received bytes (override as needed)."""

    def connectionLost(self, reason=None):
        """Called when the connection is closed (override as needed)."""


class Factory(object):
    """Twisted-style protocol Factory (twisted.internet.protocol.Factory).

    ``buildProtocol(addr)`` instantiates ``self.protocol`` and back-links the
    factory, matching Twisted so subclasses that only set ``protocol`` work
    unchanged.
    """

    protocol = None

    def buildProtocol(self, addr):
        """Create a protocol instance for a new connection."""
        proto = self.protocol()
        proto.factory = self
        return proto

    def startedConnecting(self, connector):
        """Called when a connection attempt begins (override as needed)."""

    def clientConnectionFailed(self, connector, reason):
        """Called when a connection attempt fails (override as needed)."""

    def clientConnectionLost(self, connector, reason):
        """Called when an established connection is lost (override as needed)."""

    def doStart(self):
        """Called when the factory starts being used (override as needed)."""

    def doStop(self):
        """Called when the factory stops being used (override as needed)."""


class ClientFactory(Factory):
    """Twisted-style ClientFactory (twisted.internet.protocol.ClientFactory)."""


# ---------------------------------------------------------------------------- #
# Process protocol adapter
# ---------------------------------------------------------------------------- #
class _ProcessTransportAdapter(object):
    """Expose the Twisted process-transport surface used by the suite."""

    def __init__(self, transport):
        self._transport = transport
        self.pid = transport.get_pid()
        self._exit_waiter = asyncio.get_running_loop().create_future()
        self._pidfd = None

        # asyncio's subprocess transport signals through subprocess.Popen.
        # Popen.send_signal() calls poll(), which can reap the child before
        # asyncio's PidfdChildWatcher does and produces a bogus return code 255.
        # Hold our own pidfd where Linux supports it so signalling is both
        # race-free (the PID cannot be recycled underneath us) and non-reaping.
        if (hasattr(os, 'pidfd_open')
                and hasattr(_signal, 'pidfd_send_signal')):
            try:
                self._pidfd = os.pidfd_open(self.pid, 0)
            except OSError:
                # Older kernels / restricted containers may expose the Python
                # API without allowing pidfds. The POSIX fallback below remains
                # non-reaping, although it cannot guard against PID reuse.
                self._pidfd = None

    def write(self, data):
        stdin = self._transport.get_pipe_transport(0)
        if stdin is not None:
            stdin.write(data)

    def closeStdin(self):
        stdin = self._transport.get_pipe_transport(0)
        if stdin is not None:
            stdin.close()

    def loseConnection(self):
        # BaseSubprocessTransport.close() polls and kills a live Popen, racing
        # asyncio's child watcher. For a live child, close only stdin and let an
        # explicit signal plus process_exited own process termination/reaping.
        if self._transport.get_returncode() is None:
            self.closeStdin()
        else:
            self._transport.close()

    def signalProcess(self, signal):
        """Send a signal; accepts a name ('KILL'/'TERM') or number like Twisted."""
        if isinstance(signal, str):
            signal = getattr(_signal, 'SIG' + signal, None) or \
                getattr(_signal, signal)
        # asyncio's transport (via Popen.send_signal) silently no-ops on a child
        # that has already exited; Twisted raised ProcessExitedAlready there, and
        # Asterisk.stop()'s kill path relies on catching it. Reproduce that.
        get_returncode = getattr(self._transport, 'get_returncode', None)
        if get_returncode is not None and get_returncode() is not None:
            raise ProcessExitedAlready()
        try:
            if self._pidfd is not None:
                _signal.pidfd_send_signal(self._pidfd, signal, None, 0)
            elif os.name == 'posix':
                # pidfds are unavailable on older Unix platforms. os.kill()
                # still avoids Popen.poll()/waitpid; the return-code check above
                # and ESRCH handling below preserve ProcessExitedAlready as far
                # as the non-pidfd API permits.
                os.kill(self.pid, signal)
            else:
                # Non-POSIX fallback preserves platform-specific behavior.
                self._transport.send_signal(signal)
        except ProcessLookupError:
            raise ProcessExitedAlready()

    def terminate(self):
        """Terminate without allowing Popen.poll() to reap the child."""
        self.signalProcess(_signal.SIGTERM)

    def kill(self):
        """Kill without allowing Popen.poll() to reap the child."""
        self.signalProcess(_signal.SIGKILL)

    async def waitForExit(self):
        """Wait until asyncio's child watcher has delivered process_exited."""
        if self._transport.get_returncode() is None:
            await asyncio.shield(self._exit_waiter)
        return self._transport.get_returncode()

    def processExited(self):
        """Release signalling resources and wake reactor shutdown waiters."""
        if not self._exit_waiter.done():
            self._exit_waiter.set_result(self._transport.get_returncode())
        if self._pidfd is not None:
            os.close(self._pidfd)
            self._pidfd = None

    def __getattr__(self, name):
        return getattr(self._transport, name)


class _PendingProcessTransport(object):
    """Synchronous stand-in installed by ``reactor.spawnProcess`` before the
    asyncio subprocess transport connects.

    Twisted's ``reactor.spawnProcess`` wires ``protocol.transport`` (and returns
    a transport) synchronously, so a fixture may signal or kill the child on the
    very next line. asyncio's ``loop.subprocess_exec`` is a coroutine, so
    ``connection_made`` -- which installs the real transport -- runs later; a
    kill issued in between (e.g. a SIPp scenario killed from an AMI event
    handler) would otherwise hit ``protocol.transport is None`` and raise
    ``AttributeError`` instead of doing anything. This placeholder absorbs
    ``signalProcess``/``loseConnection`` synchronously and replays them once the
    real transport arrives, so the kill still lands. It also makes the
    ``_ProcessConnector`` returned by ``spawnProcess`` usable immediately, since
    that connector delegates to ``protocol.transport``.
    """

    pid = None

    def __init__(self):
        self._signal = None
        self._close = False

    def signalProcess(self, signal):
        # Remember the last signal requested; replayed on connect. A pre-connect
        # child can't have "already exited", so never raise ProcessExitedAlready.
        self._signal = signal

    def loseConnection(self):
        self._close = True

    def closeStdin(self):
        # No stdin yet; the child hasn't started. Fold into a close-on-connect
        # so nothing is silently dropped.
        self._close = True

    def write(self, data):
        # Nothing in the suite writes to a process's stdin before it has
        # connected; dropping here matches Twisted, which would not have a
        # transport to write to either.
        pass

    def replay(self, transport):
        """Apply the buffered actions to the now-live process ``transport``."""
        if self._signal is not None:
            try:
                transport.signalProcess(self._signal)
            except ProcessExitedAlready:
                pass
        if self._close:
            transport.loseConnection()

    def __bool__(self):
        return True


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
        # A pre-connect signal/kill may have been buffered on the synchronous
        # placeholder that spawnProcess installed; replay it onto the real
        # transport now that the child exists (review finding: SIPp scenario
        # killed from an AMI event before subprocess_exec resolved).
        pending = self.transport
        self.transport = _ProcessTransportAdapter(transport)
        if isinstance(pending, _PendingProcessTransport):
            pending.replay(self.transport)
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
        self._returncode = self._reliable_returncode()
        self.transport.processExited()
        self._maybe_end()

    def _reliable_returncode(self):
        """Return the authoritative child exit status.

        asyncio's child watcher can lose a reap race if code bypasses this
        adapter and calls a ``subprocess.Popen`` method that polls/waits for the
        child. PidfdChildWatcher then gets ``ECHILD`` and substitutes its sentinel
        return code 255. The adapter's pidfd/os.kill signalling and ordered
        reactor shutdown prevent that race for owned children; this recovery is
        retained defensively for external or platform-specific process paths.

        The underlying ``Popen.returncode`` is set only from a real
        ``os.waitpid`` result, so it is authoritative whenever present (it agrees
        with the watcher on a clean exit and holds the true signal status on the
        raced path). Prefer it; fall back to the transport's value otherwise.
        This restores Twisted parity, where a signal-terminated process reports
        ``signal``/``exitCode is None`` rather than a fabricated exit code.
        """
        transport_rc = self.transport.get_returncode()
        inner = getattr(self.transport, '_transport', None)
        proc = getattr(inner, '_proc', None)
        proc_rc = getattr(proc, 'returncode', None)
        if proc_rc is not None:
            return proc_rc
        return transport_rc

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
