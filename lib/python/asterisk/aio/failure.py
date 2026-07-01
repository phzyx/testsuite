"""asyncio replacement for twisted.python.failure.Failure.

A Failure wraps an exception (and optionally its traceback) so it can be passed
through Deferred errback chains the same way Twisted's Failure was. Only the
surface actually used by the test suite is reproduced: ``value``, ``type``,
``getErrorMessage()``, ``check()``, plus ``trap()``/``raiseException()`` for the
process adapter and completeness.

This is part of the ``asterisk.aio`` compatibility layer (design doc Section 2,
Section 3.1/3.2). It is a transitional aid for the Twisted removal; see the
modernization end state (design Section 14).
"""

import sys
import traceback as _traceback


class Failure(object):
    """Wraps an exception for transport through Deferred errback chains."""

    # Cross-shim marker: any Failure-compatible type sets this True so callback
    # chains in either shim (testsuite / starpy) recognise a foreign Failure
    # without importing one concrete class. See aio/defer.py._is_failure.
    _is_failure = True

    def __init__(self, exc=None, exc_type=None, tb=None):
        """Create a Failure.

        Keyword Arguments:
        exc      The exception instance. If None, the exception currently being
                 handled (sys.exc_info) is captured.
        exc_type The exception class. Defaults to type(exc).
        tb       The traceback. Defaults to the current traceback when captured.
        """
        if exc is None:
            etype, evalue, etb = sys.exc_info()
            if evalue is None:
                evalue = Exception("Unknown failure (no active exception)")
                etype = type(evalue)
                etb = None
            exc = evalue
            exc_type = exc_type or etype
            tb = tb if tb is not None else etb
        self.value = exc
        self.type = exc_type or type(exc)
        self.tb = tb

    def getErrorMessage(self):
        """Return the string form of the wrapped exception."""
        return str(self.value)

    def check(self, *error_types):
        """Return the first error type that matches, or None.

        Mirrors twisted Failure.check: matches against the wrapped exception's
        class hierarchy.
        """
        for et in error_types:
            if isinstance(self.value, et):
                return et
            try:
                if issubclass(self.type, et):
                    return et
            except TypeError:
                pass
        return None

    def trap(self, *error_types):
        """Return the matching error type, or re-raise the wrapped exception."""
        et = self.check(*error_types)
        if et is None:
            self.raiseException()
        return et

    def raiseException(self):
        """Re-raise the wrapped exception with its original traceback."""
        if isinstance(self.value, BaseException):
            raise self.value.with_traceback(self.tb)
        raise RuntimeError(str(self.value))

    def getTraceback(self):
        """Return a formatted traceback string (best effort)."""
        if self.tb is not None:
            return ''.join(
                _traceback.format_exception(self.type, self.value, self.tb))
        return str(self.value)

    def __repr__(self):
        return "<Failure %s: %s>" % (
            getattr(self.type, '__name__', self.type), self.value)
