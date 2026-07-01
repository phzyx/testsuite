# Potential Issues in the Twisted-to-asyncio Proposal

Status: Review findings  
Date: 2026-06-30

The overall migration direction is workable, but the current proposal should not
be implemented as written. The following issues were found by comparing the
documents with the existing testsuite and starpy code and by running the proposed
`asterisk.aio` unit tests.

## 1. Critical: subprocess completion can lose output

The proposed `ProcessProtocol` adapter calls `processEnded` from asyncio's
`process_exited` callback. asyncio permits `process_exited` to run before the
final `pipe_data_received` and `pipe_connection_lost` callbacks. Calling
`processEnded` and closing the transport at that point can discard final Asterisk
or SIPp output and signal completion too early.

The adapter should track process exit and closure of stdout and stderr, invoking
`processEnded` only after all three conditions are satisfied.

There is a second constructor problem in the prototype: its base
`ProcessProtocol.__init__` initializes `_exited`, but the existing
`AsteriskProtocol` and `SIPpProtocol` constructors do not call `super().__init__`.
The proposed claim that their methods can remain unchanged is therefore unsafe.
The adapter must not require subclass constructors to call the base initializer,
or all subclasses must be updated and tested.

Relevant locations:

- `03-implementation.md`, section 3.4
- `lib/python/asterisk/aio/protocols.py`, `ProcessProtocol.process_exited`
- `lib/python/asterisk/asterisk.py`, `AsteriskProtocol.__init__`
- `lib/python/asterisk/sipp.py`, `SIPpProtocol.__init__`

## 2. Critical: the Deferred compatibility surface is incomplete

The required Deferred API in the proposal omits `.called`, although production
code reads that property in at least four places in `asterisk.py` and `sipp.py`.
The current prototype has `_called` but no public `called` property, so those
paths will raise `AttributeError` after the import swap.

The Failure survey is also incorrect. The documents state that first-party code
uses only `.value`, `.type`, and `.getErrorMessage()`, but `.getTraceback()` is
used in `test_case.py`, `apptest.py`, `voicemail.py`, `pluggable_modules.py`,
`confbridge.py`, and starpy.

Before implementation, generate a complete compatibility inventory including
attributes, methods, exception types, cancellation behavior, and late-callback
behavior. At minimum it must cover `called` and `Failure.getTraceback()`.

## 3. High: an asyncio.Future subclass cannot fully reproduce late callbacks

An `asyncio.Future` result is immutable once settled. A Twisted Deferred,
however, continues threading its current result through callbacks added after it
has fired. The prototype runs a late callback but cannot update the already
settled Future result. For example:

```python
d = Deferred()
d.callback(1)
d.addCallback(lambda value: value + 1)
result = await d  # prototype returns 1, while its callback-chain result is 2
```

This contradicts the proposal's stated late-callback behavior. Options include:

1. Implementing Deferred as a wrapper with a replaceable internal Future rather
   than subclassing `asyncio.Future`.
2. Using native Twisted Deferred/Future adapters during an incremental migration.
3. Explicitly restricting late callback plus `await` mixing and proving no caller
   relies on it.

The behavior must be specified and tested before the shim becomes foundational.

## 4. High: the suite and starpy Deferred shims are not automatically compatible

The design gives the testsuite and starpy separate Deferred implementations and
asserts that they compose seamlessly because both subclass `asyncio.Future`.
Shared ancestry is insufficient for callback-chain semantics.

The prototype pauses a callback chain only when the callback returns its own
exact `Deferred` class. A starpy Deferred, ordinary Future, Task, or coroutine is
treated as a completed plain value rather than being awaited. The reverse issue
can occur inside starpy when a user callback returns a testsuite Deferred.

Both shims need a common implementation or generic handling for Futures and
awaitables, including success, failure, cancellation, and cross-loop checks.
Tests should explicitly chain in both directions between starpy and testsuite
Deferreds.

## 5. High: the WebSocket plan overlooks async and thread boundaries

Autobahn's current protocol methods expose synchronous `sendMessage` and
`sendClose` calls. The `websockets` library exposes coroutine-based send and
close operations. Existing media code also calls `sendFile` through
`reactor.callInThread`, and that worker then performs WebSocket sends.

A mechanical factory/protocol replacement will therefore produce un-awaited
coroutines or unsafe cross-thread loop access. The migration needs an explicit
API adapter or a redesign in which file reads may occur in an executor but every
WebSocket operation is scheduled and awaited on the event-loop thread.

The plan must also preserve intentional frame fragmentation in
`tests/channels/websocket/inbound/basic-call/media_client.py`, subprotocol
selection, binary-versus-text behavior, flow-control messages, reconnects, and
close callbacks.

## 6. High: asynchronous listener readiness and bind failures are unspecified

Twisted callers currently treat `listenTCP` and `listenUDP` as established
listeners. asyncio's `create_server` and `create_datagram_endpoint` are
coroutines. The proposed shim returns a placeholder immediately and schedules
the bind in the background.

This introduces races in which Asterisk or another client starts before DNS,
FastAGI, HTTP, UDP, or WebSocket listeners are ready. It also changes bind-error
behavior: without an error callback, a failed bind may be consumed by a task and
the test can continue until timing out for an unrelated reason.

Listener creation must be part of an awaited startup phase, with failures
propagated into the test result. Shutdown should similarly await server closure
and transport cleanup.

## 7. High: the promised reactor shutdown behavior is not implemented

The implementation document says `stop()` will cancel outstanding timers and
tasks and close transports. The prototype only resolves the sentinel Future. It
does not maintain registries of timers, endpoint tasks, servers, datagram
transports, subprocesses, or executor work.

This can leak resources between self-tests, leave background work running after
the test result has been decided, and generate pending-task or unclosed-transport
warnings. The design needs explicit resource ownership and an asynchronous,
ordered shutdown procedure.

## 8. High: the current Step 1 exit criterion is not green

The proposed unit suite was run with:

```text
PYTHONPATH=lib/python python3 -m unittest asterisk.aio.test_aio -v
```

Result on 2026-06-30: 21 tests passed and the UDP round-trip test failed after
its three-second safety timeout. `result.get('data')` was `None` rather than
`b'echo:ping'`.

The compatibility layer therefore does not currently meet the Step 1 exit
criterion described in `03-implementation.md`.

## 9. Medium: the proposal incorrectly rules out an incremental bridge

The scope analysis says the Twisted reactor does not cooperate with asyncio, and
later says the two loops cannot both own the process. Twisted provides
`AsyncioSelectorReactor`, which runs Twisted on an asyncio event loop, as well as
`Deferred.fromFuture`, `Deferred.asFuture`, and coroutine adapters.

This does not remove Twisted by itself, but it may allow a safer incremental
migration while preserving Twisted's mature Deferred, process, DNS, web, and
transport behavior until each subsystem is replaced. The design should compare
this bridge against the proposed big-bang compatibility reimplementation before
settling the cutover strategy.

## 10. Medium: DNS, HTTP, and SSH parity needs concrete contracts

The proposal names replacement libraries but does not define enough behavioral
parity to review the replacements safely:

- DNS needs zone parsing rules, record types, authoritative flags, NXDOMAIN and
  NODATA behavior, UDP truncation, TCP framing, and startup readiness.
- HTTP needs exact route matching, path traversal protection, query/form parsing,
  response bodies, status codes, headers, and shutdown behavior.
- SSH needs host-key policy, agent behavior, encrypted key handling, password
  behavior, command quoting, stdout/stderr separation, exit status, timeout, and
  connection closure.

Focused behavioral tests against the Twisted baseline should be specified before
these components are rewritten.

## 11. Minor: inventory, grep gate, and dependency reproducibility

- The current working tree contains 30 existing library modules and 32 test
  fixtures importing Twisted, rather than the documented 30 and 34. Counts should
  be regenerated after excluding the new `asterisk.aio` files.
- The literal grep gate also matches explanatory docstrings and comments inside
  the compatibility layer, so it currently fails even when no Twisted import is
  present. Use an AST-based import check or restrict the expression to imports.
- A source grep cannot prove that Twisted is absent from the virtual environment.
  Add environment checks such as package inventory, `pip check`, and an explicit
  Twisted import/package assertion.
- The proposed starpy dependency points at the moving `master-untwisted` branch.
  Pin a reviewed commit hash or release tag for reproducible environments.
- Newly introduced dependencies should be version-pinned consistently with the
  rest of `requirements.txt`.

## Recommended revision order

1. Correct the Twisted API and call-site inventory.
2. Evaluate `AsyncioSelectorReactor` as an incremental migration mechanism.
3. Define one interoperable Deferred/Future boundary rather than two independent
   partial implementations.
4. Fix subprocess exit and pipe-drain ordering.
5. Define awaited listener startup and resource-owned shutdown.
6. Redesign the WebSocket API boundary and its threading behavior.
7. Add focused parity tests for process output, late callbacks, cross-shim
   chaining, bind failures, WebSocket fragmentation, DNS, HTTP, and SSH.
8. Require the compatibility unit suite and representative integration tests to
   pass before beginning the import sweep.
