#!/usr/bin/env python3
"""Smoke test for pcap.py's independent asyncio entrypoint (design 05 point 6).

pcap.py is a standalone capture tool, not a test-runtime consumer: scapy's
AsyncSniffer delivers packets from its own thread, so the tool needs no reactor,
only something to keep the process alive until Ctrl+C. B1.5 replaces its
module-level ``reactor.run()``/``reactor.stop()`` with an independent
``asyncio.run(_pcap_main())`` wrapper whose stop is wired to SIGINT.

This proves:
  * ``_pcap_main`` is a coroutine driven under ``asyncio.run`` (no reactor);
  * a SIGINT delivered while it is awaiting unblocks it and it returns cleanly
    (the loop signal handler sets the stop event);
  * pcap.py no longer references the ``reactor`` facade at all (independence).

The real VOIPListener (which would start a scapy capture needing an interface /
privileges) is stubbed out; this exercises the lifecycle wrapper, not capture.

Run:  PYTHONPATH=lib/python .venv/bin/python test_pcap_main.py   (exit 0 = OK)
"""

import asyncio
import inspect
import signal
import sys

import asterisk.pcap as pcap

# pcap.py's own source: used for the independence check below. (The ``reactor``
# name may still leak into the module namespace via ``from pcap_proxy import *``,
# which is a separate, still-transitional module; what B1.5 point 6 requires is
# that pcap.py itself no longer drives a reactor run/stop.)
_PCAP_SRC = inspect.getsource(pcap)


class _Options(object):
    interface = 'lo'
    filter = 'udp port 5060'
    output = '/tmp/pcap_smoke_test.pcap'


def main():
    assert inspect.iscoroutinefunction(pcap._pcap_main), \
        "_pcap_main must be a coroutine function driven by asyncio.run"

    # Independence: pcap.py must no longer drive a reactor run/stop -- its
    # lifecycle is now asyncio.run(_pcap_main()).
    assert 'reactor.run(' not in _PCAP_SRC, \
        "pcap.py must not call reactor.run() (independent asyncio.run wrapper)"
    assert 'reactor.stop(' not in _PCAP_SRC, \
        "pcap.py must not call reactor.stop() (SIGINT drives the stop event)"
    assert 'asyncio.run(' in _PCAP_SRC, \
        "pcap.py must drive its entrypoint via asyncio.run(_pcap_main())"

    # Drive _pcap_main once with a *custom* SIGINT handler already installed,
    # then report both the config it built and whether that custom handler was
    # restored afterwards. force_fallback exercises the signal.signal() branch
    # by making the loop's add_signal_handler raise, as it would on a platform
    # where it is unavailable.
    def _run_once(force_fallback):
        constructed = {}

        class _StubListener(object):
            def __init__(self, module_config, test_object):
                constructed['config'] = module_config

        pcap.VOIPListener = _StubListener

        def _custom_handler(signum, frame):
            pass

        prior = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, _custom_handler)
        try:
            async def _drive():
                loop = asyncio.get_running_loop()
                if force_fallback:
                    def _no_loop_handler(*a, **k):
                        raise NotImplementedError("forced signal.signal fallback")
                    # Shadow the bound method on this loop instance so
                    # _pcap_main takes the except branch.
                    loop.add_signal_handler = _no_loop_handler
                task = asyncio.ensure_future(pcap._pcap_main(_Options()))
                # Let _pcap_main construct the (stub) listener and install its
                # SIGINT handler, then deliver the interrupt like Ctrl+C would.
                await asyncio.sleep(0.05)
                signal.raise_signal(signal.SIGINT)
                await asyncio.wait_for(task, timeout=5)

            asyncio.run(_drive())
            restored = signal.getsignal(signal.SIGINT) is _custom_handler
        finally:
            signal.signal(signal.SIGINT, prior)
        return constructed, restored

    # Loop-handler path.
    constructed, restored = _run_once(force_fallback=False)
    assert constructed.get('config', {}).get('device') == 'lo', \
        "listener was not constructed from options: %r" % constructed
    assert restored, \
        "loop-handler path did not restore the pre-existing custom SIGINT handler"

    # signal.signal() fallback path.
    _, restored_fb = _run_once(force_fallback=True)
    assert restored_fb, \
        "signal.signal fallback did not restore the pre-existing custom SIGINT handler"

    print("  [pcap]        asyncio.run(_pcap_main) + SIGINT stop OK")
    print("  [pcap]        loop-handler path restores prior SIGINT handler OK")
    print("  [pcap]        signal.signal fallback restores prior SIGINT handler OK")
    print("  [pcap]        no reactor.run()/stop() in pcap.py (independent) OK")
    print("ALL OK")


if __name__ == '__main__':
    print("pcap.py independent asyncio entrypoint smoke test")
    main()
