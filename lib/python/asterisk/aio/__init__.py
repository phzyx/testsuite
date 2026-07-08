"""asterisk.aio - asyncio compatibility layer replacing Twisted.

This package provides drop-in replacements for the Twisted surface the Asterisk
test suite (and the bundled starpy fork) depend on, implemented on top of
``asyncio``. The goal is to make removing Twisted a mostly mechanical import
swap::

    from twisted.internet import defer                ->  from asterisk.aio import defer
    from twisted.python.failure import Failure        ->  from asterisk.aio import Failure
    from twisted.internet.protocol import ProcessProtocol, DatagramProtocol
                                                      ->  from asterisk.aio import ProcessProtocol, DatagramProtocol

The transitional ``reactor`` facade that this package once re-exported has been
removed (Phase B step B4). Reactor-shaped primitives now live on
``AsyncTestRuntime``; obtain the current runtime via
``from asterisk.aio.runtime import current_runtime`` and call
``current_runtime().<method>()`` directly.

See doc/untwist/02-design.md for the full design.
"""

from . import defer
from . import utils
from . import error
from .failure import Failure
from .protocols import (
    Protocol,
    Factory,
    ClientFactory,
    DatagramProtocol,
    ProcessProtocol,
    LoopingCall,
    ProcessDone,
    ProcessTerminated,
    ProcessExitedAlready,
    ConnectionDone,
)

__all__ = [
    'defer',
    'utils',
    'error',
    'Failure',
    'Protocol',
    'Factory',
    'ClientFactory',
    'DatagramProtocol',
    'ProcessProtocol',
    'LoopingCall',
    'ProcessDone',
    'ProcessTerminated',
    'ProcessExitedAlready',
    'ConnectionDone',
]
