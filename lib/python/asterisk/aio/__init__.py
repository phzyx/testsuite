"""asterisk.aio - asyncio compatibility layer replacing Twisted.

This package provides drop-in replacements for the Twisted surface the Asterisk
test suite (and the bundled starpy fork) depend on, implemented on top of
``asyncio``. The goal is to make removing Twisted a mostly mechanical import
swap::

    from twisted.internet import reactor, defer      ->  from asterisk.aio import reactor, defer
    from twisted.python.failure import Failure        ->  from asterisk.aio import Failure
    from twisted.internet.protocol import ProcessProtocol, DatagramProtocol
                                                      ->  from asterisk.aio import ProcessProtocol, DatagramProtocol

See doc/untwist/02-design.md for the full design. Per Section 14, the
reactor-shaped pieces of this package are transitional; the modernization phase
migrates callers to idiomatic asyncio and removes them.
"""

from . import defer
from . import utils
from .reactor import reactor
from .failure import Failure
from .protocols import (
    DatagramProtocol,
    ProcessProtocol,
    LoopingCall,
    ProcessDone,
    ProcessTerminated,
    ConnectionDone,
)

__all__ = [
    'defer',
    'utils',
    'reactor',
    'Failure',
    'DatagramProtocol',
    'ProcessProtocol',
    'LoopingCall',
    'ProcessDone',
    'ProcessTerminated',
    'ConnectionDone',
]
