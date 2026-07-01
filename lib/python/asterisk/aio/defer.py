"""asyncio replacement for twisted.internet.defer.

Provides ``Deferred``, ``DeferredList``, ``gatherResults``, ``maybeDeferred``,
``succeed``, ``fail``, ``TimeoutError`` and ``AlreadyCalledError`` reproducing the
Twisted callback-chain semantics the test suite (and the bundled starpy fork)
rely on.

Design reference: doc/untwist/02-design.md Section 3.1. This is a transitional
compatibility layer; the modernization end state (design Section 14) migrates
callback chains to async/await and ultimately removes this module.

Implementation note (review finding 3): an earlier prototype subclassed
``asyncio.Future``. That cannot reproduce Twisted's late-callback semantics,
because a Future's result is immutable once settled while a Deferred keeps
threading its *current* result through callbacks added after it has fired. This
implementation therefore *wraps* its own mutable chain state and exposes
``__await__`` over an idle-event, so ``await d`` always observes the latest
chain result.

Cross-shim interoperability (review finding 4): a callback may return another
shim's Deferred (e.g. starpy's), a bare ``asyncio.Future``, a Task, or a
coroutine. The chain pauses on any of these, not just this exact class. Failure
detection across shims uses the duck-typed ``_is_failure`` class marker rather
than ``isinstance`` against one concrete ``Failure`` type, so a foreign Failure
flowing in from another shim is still routed to the errback branch.

Semantics reproduced from Twisted:
  * A Deferred holds an ordered chain of (callback, errback) stages.
  * Firing with callback()/errback() runs the chain, threading each stage's
    return value into the next. A raised exception becomes a Failure and routes
    to the errback branch; an errback returning a non-Failure switches back to
    the callback branch.
  * Adding a callback after the Deferred has fired runs it immediately and
    continues threading the current result.
  * Firing an already-fired Deferred raises AlreadyCalledError.
  * A stage may return another Deferred/Future/awaitable; the chain pauses until
    it resolves.
"""

import asyncio
import inspect

from .failure import Failure


class AlreadyCalledError(Exception):
    """Raised when callback()/errback() is invoked on an already-fired Deferred."""


class TimeoutError(Exception):
    """Raised when an operation times out (twisted.internet.defer.TimeoutError)."""


def _passthrough(result):
    return result


def _get_loop():
    """Return the running loop if any, else the current policy loop."""
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.get_event_loop_policy().get_event_loop()


def _is_failure(obj):
    """True if ``obj`` is a Failure from this or any compatible shim.

    Compatible Failure types set the class attribute ``_is_failure = True``;
    this avoids coupling cross-shim chaining to one concrete class.
    """
    return getattr(obj, '_is_failure', False) is True


def _is_deferred_like(obj):
    """True if ``obj`` quacks like a Deferred (has an ``addBoth`` method)."""
    return callable(getattr(obj, 'addBoth', None))


class Deferred(object):
    """A Twisted-style Deferred backed by an explicit callback chain.

    Composes with asyncio: ``await deferred`` resolves to the chain's final
    result (raising on failure), and because Deferred is awaitable it can be
    wrapped by ``asyncio.ensure_future`` / passed to ``asyncio.gather``.
    """

    def __init__(self, canceller=None):
        self._chain = []
        self._called = False
        self._chain_result = None
        self._running = False
        self._paused = 0
        self._canceller = canceller
        self._idle_event = None

    # ------------------------------------------------------------------ #
    # Public Twisted-compatible introspection
    # ------------------------------------------------------------------ #
    @property
    def called(self):
        """True once callback()/errback() has fired (twisted Deferred.called)."""
        return self._called

    @property
    def result(self):
        """The current chain result (twisted Deferred.result; best effort)."""
        return self._chain_result

    # ------------------------------------------------------------------ #
    # Callback registration
    # ------------------------------------------------------------------ #
    def addCallbacks(self, callback, errback=None,
                     callbackArgs=None, callbackKeywords=None,
                     errbackArgs=None, errbackKeywords=None):
        """Append paired success/failure stages (Twisted addCallbacks)."""
        cb = (callback, tuple(callbackArgs or ()), dict(callbackKeywords or {}))
        if errback is None:
            eb = None
        else:
            eb = (errback, tuple(errbackArgs or ()), dict(errbackKeywords or {}))
        self._chain.append((cb, eb))
        if self._called:
            self._run_callbacks()
        return self

    def addCallback(self, callback, *args, **kw):
        """Append a success stage."""
        return self.addCallbacks(callback,
                                 callbackArgs=args, callbackKeywords=kw)

    def addErrback(self, errback, *args, **kw):
        """Append a failure stage."""
        return self.addCallbacks(_passthrough, errback,
                                 errbackArgs=args, errbackKeywords=kw)

    def addBoth(self, fn, *args, **kw):
        """Append a stage invoked on both success and failure."""
        return self.addCallbacks(fn, fn,
                                 callbackArgs=args, callbackKeywords=kw,
                                 errbackArgs=args, errbackKeywords=kw)

    def chainDeferred(self, other):
        """Fire ``other`` from this Deferred's result (Twisted chainDeferred)."""
        return self.addCallbacks(other.callback, other.errback)

    # ------------------------------------------------------------------ #
    # Firing
    # ------------------------------------------------------------------ #
    def callback(self, result=None):
        """Fire the success chain with ``result``."""
        self._start(result)

    def errback(self, fail=None):
        """Fire the failure chain with ``fail`` (Exception or Failure)."""
        if fail is None:
            fail = Failure()
        elif isinstance(fail, BaseException):
            fail = Failure(fail)
        elif not _is_failure(fail):
            fail = Failure(RuntimeError(str(fail)))
        self._start(fail)

    def _start(self, result):
        if self._called:
            raise AlreadyCalledError()
        self._called = True
        self._chain_result = result
        self._run_callbacks()

    # ------------------------------------------------------------------ #
    # Chain execution
    # ------------------------------------------------------------------ #
    def _run_callbacks(self):
        if self._running or self._paused:
            return
        self._running = True
        self._clear_idle()
        try:
            while self._chain:
                cb, eb = self._chain.pop(0)
                stage = eb if _is_failure(self._chain_result) else cb
                if stage is None:
                    # No handler for this branch; pass the result through.
                    continue
                fn, args, kw = stage
                if fn is _passthrough:
                    continue
                try:
                    new_result = fn(self._chain_result, *args, **kw)
                except Exception:
                    self._chain_result = Failure()
                    continue
                if _is_deferred_like(new_result):
                    self._pause_on_deferred(new_result)
                    return
                if isinstance(new_result, asyncio.Future) or \
                        inspect.isawaitable(new_result):
                    self._pause_on_future(new_result)
                    return
                self._chain_result = new_result
        finally:
            self._running = False
        self._settle()

    def _pause_on_deferred(self, inner):
        """Pause this chain until ``inner`` (a Deferred-like) resolves."""
        self._paused += 1
        self._chain_result = None

        def _resume(res):
            self._chain_result = res
            self._paused -= 1
            self._run_callbacks()
            return res

        inner.addBoth(_resume)

    def _pause_on_future(self, awaitable):
        """Pause this chain until ``awaitable`` (Future/Task/coroutine) resolves."""
        self._paused += 1
        self._chain_result = None
        task = asyncio.ensure_future(awaitable)

        def _resume(fut):
            self._paused -= 1
            if fut.cancelled():
                self._chain_result = Failure(asyncio.CancelledError())
            elif fut.exception() is not None:
                self._chain_result = Failure(fut.exception())
            else:
                self._chain_result = fut.result()
            self._run_callbacks()

        task.add_done_callback(_resume)

    # ------------------------------------------------------------------ #
    # await support (idle-event based; observes latest chain result)
    # ------------------------------------------------------------------ #
    def _get_idle_event(self):
        if self._idle_event is None:
            self._idle_event = asyncio.Event()
        return self._idle_event

    def _clear_idle(self):
        if self._idle_event is not None:
            self._idle_event.clear()

    def _settle(self):
        """Mark the chain quiescent and wake any awaiters."""
        if self._called and not self._running and not self._paused:
            self._get_idle_event().set()

    def __await__(self):
        return self._await_result().__await__()

    async def _await_result(self):
        while not (self._called and not self._running and not self._paused):
            await self._get_idle_event().wait()
            self._get_idle_event().clear()
        if _is_failure(self._chain_result):
            self._chain_result.raiseException()
        return self._chain_result

    # ------------------------------------------------------------------ #
    # Cancellation
    # ------------------------------------------------------------------ #
    def cancel(self, msg=None):
        """Cancel a not-yet-fired Deferred (Twisted Deferred.cancel)."""
        if not self._called:
            if self._canceller is not None:
                try:
                    self._canceller(self)
                except Exception:
                    pass
            if not self._called:
                self.errback(Failure(asyncio.CancelledError()))
            return True
        return False


# ---------------------------------------------------------------------------- #
# Helpers
# ---------------------------------------------------------------------------- #
def succeed(result):
    """Return a Deferred already fired successfully with ``result``."""
    d = Deferred()
    d.callback(result)
    return d


def fail(failure=None):
    """Return a Deferred already fired with a failure."""
    d = Deferred()
    d.errback(failure)
    return d


def DeferredList(deferreds, fireOnOneCallback=False, fireOnOneErrback=False,
                 consumeErrors=False):
    """Aggregate a list of Deferreds into one (Twisted DeferredList).

    The returned Deferred fires with a list of ``(success, result)`` tuples once
    every input Deferred has fired. Implemented via direct callbacks (rather than
    asyncio.gather) to faithfully reproduce ordering, already-fired inputs, and
    consumeErrors handling.
    """
    deferreds = list(deferreds)
    result_list = [None] * len(deferreds)
    state = {'remaining': len(deferreds), 'fired': False}
    dlist = Deferred()

    if not deferreds:
        dlist.callback(result_list)
        return dlist

    def _record(result, index, succeeded):
        result_list[index] = (succeeded, result)
        state['remaining'] -= 1
        if not state['fired']:
            if succeeded and fireOnOneCallback:
                state['fired'] = True
                dlist.callback((result, index))
            elif (not succeeded) and fireOnOneErrback:
                state['fired'] = True
                dlist.errback(result)
            elif state['remaining'] == 0:
                state['fired'] = True
                dlist.callback(result_list)
        if (not succeeded) and consumeErrors:
            return None
        return result

    for index, d in enumerate(deferreds):
        d.addCallbacks(_record, _record,
                       callbackArgs=(index, True),
                       errbackArgs=(index, False))
    return dlist


def gatherResults(deferreds, consumeErrors=False):
    """Like DeferredList but fire with a plain list of results, errback on first
    failure (Twisted defer.gatherResults)."""
    dl = DeferredList(deferreds, fireOnOneErrback=True,
                      consumeErrors=consumeErrors)

    def _strip(results):
        return [r for (_success, r) in results]

    def _unwrap_failure(failure):
        # fireOnOneErrback wraps the child failure; re-raise the inner one.
        return failure

    dl.addCallbacks(_strip, _unwrap_failure)
    return dl


def _from_awaitable(awaitable):
    """Adapt a Future/Task/coroutine into a Deferred that fires on completion."""
    d = Deferred()
    task = asyncio.ensure_future(awaitable)

    def _done(fut):
        if fut.cancelled():
            d.errback(Failure(asyncio.CancelledError()))
        elif fut.exception() is not None:
            exc = fut.exception()
            try:
                raise exc
            except Exception:
                d.errback(Failure())
        else:
            d.callback(fut.result())

    task.add_done_callback(_done)
    return d


def maybeDeferred(f, *args, **kw):
    """Invoke f; wrap a plain return in a fired Deferred, pass a Deferred through.

    A coroutine or Future returned by ``f`` is scheduled and adapted into a
    Deferred (never left un-awaited)."""
    try:
        result = f(*args, **kw)
    except Exception:
        return fail(Failure())
    if isinstance(result, Deferred) or _is_deferred_like(result):
        return result
    if isinstance(result, asyncio.Future) or inspect.isawaitable(result):
        return _from_awaitable(result)
    if _is_failure(result):
        return fail(result)
    return succeed(result)
