"""asyncio replacement for the small slice of ``twisted.internet.utils`` used
by the testsuite.

Only ``getProcessOutputAndValue`` is consumed (asterisk.py), so only that is
implemented. It is a native ``async def`` (Phase B step B5.1); awaiting it:

* returns ``(stdout_bytes, stderr_bytes, exit_code)`` on a normal exit
  (including a non-zero exit code); and
* raises ``ProcessSignaled`` -- whose ``.value`` is
  ``(stdout_bytes, stderr_bytes, signal_number)`` -- when the child is
  terminated by a signal, mirroring the ``(out, err, signal)`` tuple the old
  Twisted-shaped errback exposed as ``Failure.value`` so callers reading
  ``err.value`` keep working unchanged.
"""

import asyncio
import os
import signal

__all__ = ['getProcessOutputAndValue', 'ProcessSignaled']


class ProcessSignaled(Exception):
    """Raised when ``getProcessOutputAndValue``'s child dies from a signal.

    ``.value`` carries ``(stdout_bytes, stderr_bytes, signal_number)`` to match
    the tuple Twisted's signal-termination errback exposed as ``Failure.value``.
    """

    def __init__(self, out, err, signal_number):
        super().__init__('process terminated by signal %d' % signal_number)
        self.value = (out, err, signal_number)


def _terminate_without_reaping(proc):
    """Send SIGTERM without calling Popen.poll()/waitpid.

    ``asyncio.subprocess.Process.terminate()`` delegates to ``Popen.terminate``.
    Popen polls first and can reap the child before asyncio's child watcher,
    causing PidfdChildWatcher to manufacture return code 255. Use a pidfd when
    possible, or direct POSIX signalling as the non-reaping fallback.
    """
    if proc.returncode is not None:
        return

    pidfd = None
    if hasattr(os, 'pidfd_open') and hasattr(signal, 'pidfd_send_signal'):
        try:
            pidfd = os.pidfd_open(proc.pid, 0)
        except ProcessLookupError:
            # The process is already gone; its asyncio watcher still owns reap.
            return
        except OSError:
            pidfd = None

    try:
        if pidfd is not None:
            signal.pidfd_send_signal(pidfd, signal.SIGTERM, None, 0)
        elif os.name == 'posix':
            os.kill(proc.pid, signal.SIGTERM)
        else:
            proc.terminate()
    except ProcessLookupError:
        pass
    finally:
        if pidfd is not None:
            os.close(pidfd)


async def getProcessOutputAndValue(executable, args=(), env=None, path=None):
    """Run ``executable`` and collect stdout, stderr, and exit status.

    Returns ``(stdout_bytes, stderr_bytes, exit_code)`` on a normal exit and
    raises ``ProcessSignaled`` on signal termination (see module docstring).
    ``args`` follows the os-level convention (it does NOT include the program
    name, unlike Twisted's ``spawnProcess`` ``args[0]``).
    """
    proc = await asyncio.create_subprocess_exec(
        executable, *tuple(args),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env, cwd=path)
    try:
        out, err = await proc.communicate()
    except asyncio.CancelledError:
        # Cancellation (e.g. runtime shutdown) must not leave the child
        # running: terminate it and reap before propagating.
        if proc.returncode is None:
            _terminate_without_reaping(proc)
            try:
                await proc.wait()
            except asyncio.CancelledError:
                pass
        raise
    code = proc.returncode
    if code is not None and code < 0:
        # Terminated by signal: surface (out, err, signal) like Twisted did.
        raise ProcessSignaled(out, err, -code)
    return out, err, code
