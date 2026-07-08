"""Subset of ``twisted.internet.error`` used by the Asterisk test suite.

The suite catches a handful of Twisted reactor/process error types by name
(``error.AlreadyCalled``, ``error.ProcessExitedAlready``, ...). This module
gathers the asyncio-shim equivalents under the same names so the only change at
the call sites is the import (``from twisted.internet import error`` ->
``from asterisk.aio import error``).

Part of the transitional ``asterisk.aio`` layer (design doc Section 14).
"""

from .runtime import (
    ReactorNotRunning,
    ReactorAlreadyRunning,
    AlreadyCalled,
    AlreadyCancelled,
)
from .protocols import (
    ProcessDone,
    ProcessTerminated,
    ProcessExitedAlready,
    ConnectionDone,
)

__all__ = [
    'ReactorNotRunning',
    'ReactorAlreadyRunning',
    'AlreadyCalled',
    'AlreadyCancelled',
    'ProcessDone',
    'ProcessTerminated',
    'ProcessExitedAlready',
    'ConnectionDone',
]
