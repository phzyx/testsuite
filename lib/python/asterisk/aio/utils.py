"""asyncio replacements for the small slice of ``twisted.internet.utils`` used
by the testsuite.

Only ``getProcessOutputAndValue`` is consumed (asterisk.py), so only that is
implemented. The contract mirrors Twisted's:

* On a normal exit (including a non-zero exit code) the returned Deferred fires
  its callback with ``(stdout_bytes, stderr_bytes, exit_code)``.
* On termination by a signal the Deferred fires its errback with a Failure whose
  ``.value`` is ``(stdout_bytes, stderr_bytes, signal_number)`` -- matching the
  ``_ProcessOutputValueAndError`` shape Twisted hands back, so callers that do
  ``out, err, code = result`` / ``result.value`` keep working unchanged.
"""

import asyncio
import os
import signal

from .defer import Deferred
from .failure import Failure

__all__ = ['getProcessOutputAndValue']


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


def getProcessOutputAndValue(executable, args=(), env=None, path=None):
    """Run ``executable`` and collect stdout, stderr, and exit status.

    Returns a Deferred; see module docstring for the firing contract. ``args``
    follows the os-level convention (it does NOT include the program name, unlike
    Twisted's ``spawnProcess`` ``args[0]``).
    """
    d = Deferred()

    async def _run():
        proc = await asyncio.create_subprocess_exec(
            executable, *tuple(args),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env, cwd=path)
        try:
            out, err = await proc.communicate()
        except asyncio.CancelledError:
            # Cancellation (e.g. reactor shutdown) must not leave the child
            # running: terminate it and reap before propagating.
            if proc.returncode is None:
                _terminate_without_reaping(proc)
                try:
                    await proc.wait()
                except asyncio.CancelledError:
                    pass
            raise
        return out, err, proc.returncode

    task = asyncio.ensure_future(_run())

    def _done(fut):
        if fut.cancelled():
            d.errback(Failure(asyncio.CancelledError()))
            return
        exc = fut.exception()
        if exc is not None:
            try:
                raise exc
            except Exception:
                d.errback(Failure())
            return
        out, err, code = fut.result()
        if code is not None and code < 0:
            # Terminated by signal: errback with the (out, err, signal) tuple as
            # the Failure value, mirroring Twisted's _UnexpectedErrorOutput path.
            failure = Failure(RuntimeError(
                'process %s terminated by signal %d' % (executable, -code)))
            failure.value = (out, err, -code)
            d.errback(failure)
        else:
            d.callback((out, err, code))

    task.add_done_callback(_done)
    return d
