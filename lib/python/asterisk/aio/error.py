"""Runtime and transport error types used by the Asterisk test suite.

The suite catches these runtime/process errors through ``asterisk.aio.error`` so
callers have a single import location for scheduling, cancellation, and process
exit failures.
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
