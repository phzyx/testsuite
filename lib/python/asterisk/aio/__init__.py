"""asterisk.aio - asyncio helpers for the Asterisk test suite.

This package provides the runtime, protocol, process, datagram, Failure, and
error surfaces used by the suite, implemented on top of ``asyncio``. Runtime
primitives live on ``AsyncTestRuntime``; obtain the current runtime via
``from asterisk.aio.runtime import current_runtime`` and call
``current_runtime().<method>()`` directly.

The suite uses native ``async``/``await`` and ``asyncio`` primitives for
asynchronous control flow. The starpy fork keeps its own self-contained
Deferred implementation in ``starpy._aio`` for its public callback-chain API.
"""

from . import utils
from . import error
from .failure import Failure
from .protocols import (
    Protocol,
    Factory,
    ClientFactory,
    DatagramProtocol,
    ProcessProtocol,
    ProcessDone,
    ProcessTerminated,
    ProcessExitedAlready,
    ConnectionDone,
)

__all__ = [
    'utils',
    'error',
    'Failure',
    'Protocol',
    'Factory',
    'ClientFactory',
    'DatagramProtocol',
    'ProcessProtocol',
    'ProcessDone',
    'ProcessTerminated',
    'ProcessExitedAlready',
    'ConnectionDone',
]
