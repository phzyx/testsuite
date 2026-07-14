#!/usr/bin/env python3
"""Runtime smoke test for the Step 3 core cutover (design 03-implementation Sec 5).

Proves the converted core modules (`test_runner`, `test_case`, `asterisk`) drive
the asyncio reactor shim end to end with NO Twisted in the code path:

  * The `asterisk.aio` reactor runs and stops cleanly via `callWhenRunning` /
    `current_runtime().stop()` (test_runner's `current_runtime().run()` / test_case's stop path).
  * `asterisk.AsteriskProtocol` (now an `aio.ProcessProtocol` subclass) receives
    a real child process's stdout through `outReceived` and fires its
    `stop_deferred` from `processEnded`, with `reason.value.exitCode` intact --
    the exact shape `Asterisk.stop()` relies on.
  * `current_runtime().spawnProcess` returns a transport whose `signalProcess`/
    `loseConnection` work, and `error.ProcessExitedAlready` is raised (not a raw
    ProcessLookupError) when signalling a dead child -- the `Asterisk.stop()`
    kill path.
  * The stop future's `set_result` + `asyncio.InvalidStateError` behave as the
    stop path expects (a second result raises).

Run:  PYTHONPATH=lib/python .venv/bin/python test_core_runtime.py   (exit 0 = OK)
"""

import asyncio
import os
import sys

from asterisk.aio import error
from asterisk.aio.runtime import current_runtime
from asterisk.asterisk import AsteriskProtocol

results = {}


def _drive_process_and_stop():
    """callWhenRunning entry: spawn a real process wired through AsteriskProtocol."""
    stop_deferred = current_runtime()._ensure_loop().create_future()
    proto = AsteriskProtocol('unit-host', stop_deferred)
    results['proto'] = proto

    # A short-lived child that writes to stdout and exits non-zero, so we can
    # assert outReceived captured the bytes and processEnded carried exitCode.
    transport = current_runtime().spawnProcess(
        proto, sys.executable,
        [sys.executable, '-c', "import sys; sys.stdout.write('hello-core'); "
                               "sys.stdout.flush(); sys.exit(7)"],
        env=os.environ)

    def _on_stopped(fut):
        message = fut.result()
        results['stop_message'] = message
        results['output'] = proto.output
        results['exitcode'] = proto.exitcode

        # The child is gone: signalling it must raise ProcessExitedAlready
        # (mapped from asyncio's ProcessLookupError), not leak a raw error.
        try:
            transport.signalProcess('KILL')
            results['signal_dead'] = 'no-raise'
        except error.ProcessExitedAlready:
            results['signal_dead'] = 'ProcessExitedAlready'
        except Exception as exc:  # pragma: no cover - would be a failure
            results['signal_dead'] = 'wrong:%r' % (exc,)

        # Future already resolved -> a second set_result must raise.
        try:
            stop_deferred.set_result('again')
            results['double_callback'] = 'no-raise'
        except asyncio.InvalidStateError:
            results['double_callback'] = 'AlreadyCalledError'

        current_runtime().stop()
        return message

    stop_deferred.add_done_callback(_on_stopped)


def main():
    assert 'twisted' not in [m.split('.')[0] for m in _core_module_files()], \
        "core modules must not import twisted"

    current_runtime().callWhenRunning(_drive_process_and_stop)
    # Safety net so a regression fails instead of hanging.
    current_runtime().callLater(20, current_runtime().stop)
    current_runtime().run()

    assert results.get('output') == 'hello-core', \
        "outReceived did not capture stdout: %r" % results.get('output')
    assert results.get('exitcode') == 7, \
        "processEnded lost exit code: %r" % results.get('exitcode')
    # processEnded sets `exited` only after firing stop_deferred (Twisted parity),
    # so assert it now that the reactor has fully unwound.
    assert results['proto'].exited is True, "proto.exited not set"
    assert results.get('signal_dead') == 'ProcessExitedAlready', \
        "dead-process signal not mapped: %r" % results.get('signal_dead')
    assert results.get('double_callback') == 'AlreadyCalledError', \
        "double callback not guarded: %r" % results.get('double_callback')

    print("  [reactor]     run + callWhenRunning + stop OK")
    print("  [process]     spawnProcess -> outReceived=%r exitCode=%s OK"
          % (results['output'], results['exitcode']))
    print("  [stop path]   ProcessExitedAlready + AlreadyCalled parity OK")
    print("ALL OK")


def _core_module_files():
    import asterisk.test_runner as a
    import asterisk.test_case as b
    import asterisk.asterisk as c
    mods = []
    for m in (a, b, c):
        for name in dir(m):
            obj = getattr(m, name)
            mods.append(getattr(obj, '__module__', '') or '')
    return mods


if __name__ == '__main__':
    print("starpy/asterisk core runtime smoke test")
    main()
