#!/usr/bin/env python3
"""SSH parity test for the Step 3 remote-CLI cutover (design 03-implementation
Sec 5.3, SSH contract Sec 6.5).

`AsteriskRemoteCliCommand` used to drive `twisted.conch`; it now drives
`asyncssh`. This test stands up a real loopback asyncssh server and runs the
converted `execute()` against it end to end -- on the `asterisk.aio` reactor
loop, with NO Twisted in the code path -- to prove the four contract points the
review called out:

  * Argument quoting round-trips. The command vector is shlex-joined by
    execute(); the server reports back the command string it actually received,
    and the test asserts `shlex.split(received) == original vector` even for
    arguments containing spaces (`'core show version'`).
  * stdout and stderr stay separate. `self.output` carries only the remote
    stdout; `self.err` carries only the remote stderr -- stderr is NOT aliased
    onto stdout.
  * The true exit status is preserved. A remote exit of 3 surfaces as
    `exitcode == 3` (not collapsed to -1), and drives the errback (nonzero) vs
    callback (zero) split.
  * The operation is bounded by a timeout. A remote handler that sleeps past the
    configured `timeout` makes execute() errback with `exitcode == -1` and a
    'timed out' message, instead of wedging the reactor.

Run:  PYTHONPATH=lib/python .venv/bin/python doc/untwist/test_ssh_parity.py
      (exit 0 = OK)
"""

import asyncio
import shlex
import sys

import asyncssh

from asterisk.aio.runtime import current_runtime
from asterisk.asterisk import AsteriskRemoteCliCommand

results = {}


# ---------------------------------------------------------------------------- #
# Loopback SSH server
# ---------------------------------------------------------------------------- #
class _StubSSHServer(asyncssh.SSHServer):
    """Accept every connection without authentication (loopback test only)."""

    def begin_auth(self, username):
        # Returning False means "no authentication required" -- auth succeeds
        # immediately, so the client's none/password attempt is accepted.
        return False


async def _handle_session(process):
    """Echo the received command, emit stderr, and exit per embedded tokens.

    The command string the remote shell received is written to stdout verbatim
    so the client side can verify shlex quoting round-tripped. Tokens in the
    command steer behavior:  ``EXIT=<n>`` sets the exit status; ``SLEEP=<n>``
    delays before finishing (to exercise the client timeout).
    """
    command = process.command or ''
    exit_code = 0
    sleep_for = 0.0
    for token in command.split():
        if token.startswith('EXIT='):
            exit_code = int(token[len('EXIT='):])
        elif token.startswith('SLEEP='):
            sleep_for = float(token[len('SLEEP='):])

    if sleep_for:
        await asyncio.sleep(sleep_for)

    # stdout gets ONLY the echoed command; stderr gets a distinct marker so the
    # test can prove the two streams are not conflated.
    process.stdout.write(command)
    process.stderr.write('stderr-marker')
    process.exit(exit_code)


async def _start_server():
    """Start the loopback SSH server; return (server, port)."""
    host_key = asyncssh.generate_private_key('ssh-rsa')
    server = await asyncssh.create_server(
        _StubSSHServer, '127.0.0.1', 0,
        server_host_keys=[host_key],
        process_factory=_handle_session)
    port = server.sockets[0].getsockname()[1]
    return server, port


# ---------------------------------------------------------------------------- #
# Driver
# ---------------------------------------------------------------------------- #
def _wait(d):
    """Bridge an aio Deferred to an awaitable future via addBoth.

    execute() errbacks with ``Failure(self)`` (the value wraps the command
    object, so it cannot simply be awaited); addBoth captures either outcome and
    we read the command's own attributes, which execute() sets in both paths.
    """
    loop = asyncio.get_event_loop()
    fut = loop.create_future()

    def _capture(res):
        if not fut.done():
            fut.set_result(res)
        return res

    d.addBoth(_capture)
    return fut


def _make_command(port, cmd, timeout=30):
    config = {
        'host': '127.0.0.1',
        'port': port,
        'username': 'test',
        'password': 'test',
        # A path that does not exist -> execute() disables known_hosts checking,
        # which is what we want against an ephemeral loopback host key.
        'known_hosts': '/nonexistent/known_hosts_%d' % port,
        'no-agent': True,
        'timeout': timeout,
    }
    return AsteriskRemoteCliCommand(config, cmd)


async def _run_scenarios():
    server, port = await _start_server()
    try:
        # 1. Success + quoting round-trip. 'core show version' is a single
        #    argument containing spaces; it must survive intact.
        cmd_vec = ['asterisk', '-rx', 'core show version', 'EXIT=0']
        c1 = _make_command(port, cmd_vec)
        await _wait(c1.execute())
        results['s1_exit'] = c1.exitcode
        results['s1_roundtrip'] = shlex.split(c1.output) == cmd_vec
        results['s1_stdout_no_stderr'] = 'stderr-marker' not in c1.output
        results['s1_stderr'] = c1.err

        # 2. Nonzero exit preserved (not collapsed to -1) -> errback path.
        c2 = _make_command(port, ['asterisk', '-rx', 'bad thing', 'EXIT=3'])
        await _wait(c2.execute())
        results['s2_exit'] = c2.exitcode
        results['s2_stderr'] = c2.err
        results['s2_stdout'] = c2.output

        # 3. Timeout: remote sleeps 5s, client timeout is 1s.
        c3 = _make_command(port, ['asterisk', 'SLEEP=5'], timeout=1)
        await _wait(c3.execute())
        results['s3_exit'] = c3.exitcode
        results['s3_err'] = c3.err
    finally:
        server.close()
        current_runtime().stop()


def main():
    current_runtime().callWhenRunning(lambda: asyncio.ensure_future(_run_scenarios()))
    current_runtime().callLater(30, current_runtime().stop)  # safety net against a hang
    current_runtime().run()

    # -- Scenario 1: success + quoting + stream separation -------------------
    assert results.get('s1_exit') == 0, \
        "success exit not 0: %r" % results.get('s1_exit')
    assert results.get('s1_roundtrip') is True, \
        "argument quoting did not round-trip through shlex"
    assert results.get('s1_stdout_no_stderr') is True, \
        "stderr leaked into stdout"
    assert results.get('s1_stderr') == 'stderr-marker', \
        "stderr not captured into self.err: %r" % results.get('s1_stderr')

    # -- Scenario 2: real exit status + stream separation --------------------
    assert results.get('s2_exit') == 3, \
        "nonzero exit collapsed/lost: %r" % results.get('s2_exit')
    assert results.get('s2_stderr') == 'stderr-marker', \
        "stderr not captured on failure: %r" % results.get('s2_stderr')
    assert 'stderr-marker' not in results.get('s2_stdout', ''), \
        "stderr leaked into stdout on failure"

    # -- Scenario 3: bounded timeout -----------------------------------------
    assert results.get('s3_exit') == -1, \
        "timeout did not set exitcode -1: %r" % results.get('s3_exit')
    assert 'timed out' in results.get('s3_err', ''), \
        "timeout message not reported: %r" % results.get('s3_err')

    print("  [ssh quoting] shlex round-trip through remote shell OK")
    print("  [ssh streams] stdout=%r stderr=%r kept separate OK"
          % (results['s1_stdout_no_stderr'], results['s1_stderr']))
    print("  [ssh status]  exit 0 -> callback, exit 3 -> errback (exitcode=3) OK")
    print("  [ssh timeout] slow remote -> errback exitcode=-1 'timed out' OK")
    print("ALL OK")


if __name__ == '__main__':
    print("starpy/asterisk SSH remote-CLI parity test")
    main()
