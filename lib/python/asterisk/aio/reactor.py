"""Thin facade over the current ``AsyncTestRuntime`` (transitional scaffolding).

This module used to *be* the asyncio reactor. As of Phase B step B1.0 the
concrete owner of the loop, every resource registry, the completion signal, the
fatal error, and the ordered shutdown lives in ``runtime.py`` as
``AsyncTestRuntime``, and ownership is *per run*: the native ``asyncio.run()``
entrypoint (later B1 steps) installs a fresh runtime for the duration of a run
and detaches it on shutdown.

What remains here is a stateless ``reactor`` facade that, on every attribute
access, resolves whatever runtime is *currently installed* (creating one lazily
for legacy callers) and forwards to it. It deliberately holds no runtime
reference of its own, so it always tracks the current owner rather than pinning a
single permanent instance. ``from asterisk.aio import reactor`` keeps working
unchanged while the suite is migrated; this facade module is deleted in step B4.

IMPORTANT (design doc Section 3.2 / Section 14): the reactor object is
*transitional*. Do not build new functionality on top of it; new code should use
``asterisk.aio.runtime`` (or the native entrypoint) directly.
"""

from .runtime import (  # noqa: F401  (re-exported for backward compatibility)
    AsyncTestRuntime,
    current_runtime,
    install_runtime,
    new_runtime,
    detach_runtime,
    get_current_runtime,
    ReactorNotRunning,
    ReactorAlreadyRunning,
    AlreadyCalled,
    AlreadyCancelled,
    _RuntimeState,
    _DelayedCall,
    _Port,
    _SyncDatagramTransport,
    _Connector,
    _TwistedProtocolAdapter,
    _TCPTransportAdapter,
    _ProcessConnector,
    _get_loop,
)


class _Reactor(object):
    """twisted.internet.reactor-shaped facade over the *current* runtime.

    Every reactor call the suite makes -- ``run``/``stop``/``running``/
    ``callLater``/``listenTCP``/``spawnProcess``/``_ensure_loop``/... -- is
    forwarded to whichever ``AsyncTestRuntime`` is installed at the moment of the
    call. The facade is intentionally stateless: it stores no runtime reference,
    so it never pins a stale owner and always follows install/detach. It exists
    only to keep the ``reactor`` name and its import site working during the
    migration, and is removed in step B4.
    """

    def __getattr__(self, name):
        # Reached for every access (the facade holds no instance attributes) ->
        # resolve the current runtime and delegate the whole reactor surface
        # (methods, the ``running`` property, private registries) to it.
        return getattr(current_runtime(), name)

    def __setattr__(self, name, value):
        # Route any write to the current runtime rather than shadowing it on the
        # facade (property-backed names like ``running`` raise, as on the
        # runtime, which is the intended behavior).
        setattr(current_runtime(), name, value)


# Module-level facade, mirroring ``from twisted.internet import reactor``. Note
# this is the *facade*, not a runtime; the runtime it points at can change.
reactor = _Reactor()
