# Removing Twisted from the Asterisk Test Suite — Section 3: Implementation

Status: Draft for review
Branch: `master-untwisted` (suite) / `master-untwisted` (starpy fork)
Date: 2026-06-30
Companion to: `doc/untwist/01-scope-analysis.md`, `doc/untwist/02-design.md`

> **Historical note (Phase B, step B4):** This is the Phase A implementation plan.
> The `reactor.py` shim module and `asterisk.aio.reactor` facade described below
> were **deleted** in Phase B step B4; their primitives now live on
> `AsyncTestRuntime` (`from asterisk.aio.runtime import current_runtime`).
> Present-tense references to `reactor.py` here are historical.

## 1. Purpose and how to use this document

This is the executable plan for **Phase A** (the cutover, per design §14): land
the suite on the asyncio loop with no Twisted dependency anywhere, full suite
passing. It is organized as ordered steps, each with the concrete files to touch,
the specific edits, the new code to write, and an **exit criterion** that must be
green before moving on. Phase B (modernization — deleting the reactor shim,
migrating to `async`/`await`) is out of scope here and tracked separately.

Ground rules:

- All package changes go in the suite's virtualenv via `requirements.txt` /
  `setupVenv.sh` and the fork's `pyproject.toml` only.
- Nothing is pushed to GitHub; merging is manual.
- Keep a pristine Twisted baseline (the pre-branch `master`) available for parity
  comparison throughout.
- Each step is committed locally as a reviewable unit.

## 2. Implementation order (the critical path)

```
Step 0  Environment + baseline capture
Step 1  Build & unit-test  asterisk.aio          (no suite behavior change yet)
Step 2  Convert starpy fork  (manager, fastagi)  + isolated smoke test
Step 3  Convert core  (test_runner, test_case, asterisk.py + SSH)  + self_test
Step 4  Convert subsystems (ami, ari, sipp, servers, udp, pluggable_modules)
Step 5  Convert *_test_condition family + 32 tests/** fixtures
Step 6  Strip dependencies + AST import gate + full-suite parity run
```

Rationale: build and prove the foundation (`aio` package) before anything depends
on it; prove starpy in isolation before the core consumes it; bring up the core so
`self_test` can run; then fan out to subsystems and fixtures, which all rest on the
now-stable core.

## 3. The `asterisk.aio` package — API specification

New package at `lib/python/asterisk/aio/` with these modules. This is the contract
the rest of Step 1 implements and everything else imports.

```
lib/python/asterisk/aio/
  __init__.py        # re-exports: reactor, defer, utils, Failure,
                     #             DatagramProtocol, ProcessProtocol, LoopingCall
  defer.py           # Deferred, DeferredList, maybeDeferred, AlreadyCalledError
  failure.py         # Failure
  reactor.py         # reactor shim object + run()/stop() lifecycle
  protocols.py       # DatagramProtocol, ProcessProtocol adapters, LoopingCall
  utils.py           # getProcessOutputAndValue (replaces twisted.internet.utils)
```

### 3.1 `defer.py`

```python
class AlreadyCalledError(Exception): ...
class TimeoutError(Exception): ...

class Deferred(object):                       # a Future *wrapper*, NOT a subclass
    @property
    def called(self) -> bool                  # read in asterisk.py/sipp.py (4+ sites)
    @property
    def result(self)                          # current chain result
    def addCallback(self, fn, *args, **kw) -> "Deferred"
    def addErrback(self, fn, *args, **kw) -> "Deferred"
    def addCallbacks(self, callback, errback=None,
                     callbackArgs=(), callbackKeywords={},
                     errbackArgs=(), errbackKeywords={}) -> "Deferred"
    def addBoth(self, fn, *args, **kw) -> "Deferred"
    def callback(self, result) -> None        # raises AlreadyCalledError if done
    def errback(self, failure=None) -> None    # accepts Exception or Failure
    def chainDeferred(self, other) -> "Deferred"
    def cancel(self) -> None                   # implemented (not inherited)
    def __await__(self)                        # awaits the latest chain result

def succeed(result) -> Deferred                # already-fired success
def fail(failure=None) -> Deferred             # already-fired failure
def gatherResults(deferreds, consumeErrors=False) -> Deferred
def DeferredList(deferreds, consumeErrors=False, fireOnOneCallback=False,
                 fireOnOneErrback=False) -> Deferred      # fires [(ok, result), ...]

def maybeDeferred(fn, *args, **kw) -> Deferred
```

Implementation notes (the behavioral contract, as built and unit-tested):

- **Not an `asyncio.Future` subclass.** A settled Future result is immutable, but a
  Twisted Deferred keeps threading its *current* result through callbacks added
  after it fires (review issue 3). `Deferred` therefore wraps mutable chain state
  (`_chain_result`) and exposes `__await__` over an internal idle event that fires
  only when the chain is quiescent (`_called and not _running and not _paused`), so
  `await d` observes the latest result. `LateCallbackAwaitTests` pins the canonical
  `callback(1); addCallback(+1); await d == 2` case and the `called` property.
- **Result threading.** `_run_callbacks` walks the ordered `(callback, errback)`
  stages over `_chain_result`, branching on `_is_failure(...)`; a raising stage is
  wrapped in `Failure` and routed to the errback branch; an errback returning a
  non-`Failure` switches back to success.
- **Pausing on a returned awaitable (cross-shim interop, review issue 4).** If a
  stage returns a *Deferred-like* object (duck-typed: callable `addBoth`), pause on
  it via `_pause_on_deferred`; if it returns an `asyncio.Future`/coroutine/awaitable,
  schedule it with `asyncio.ensure_future` and resume from its result
  (cancellation/exception mapped to `Failure`). This is what lets the suite and
  starpy Deferreds chain in both directions without shared ancestry.
- **Late callbacks.** Adding a callback after firing runs it immediately against
  the current result and updates the chain result.
- **`maybeDeferred` adapts awaitables.** When `fn` returns a coroutine or
  `asyncio.Future` (not just a Deferred), it is scheduled with
  `asyncio.ensure_future` and adapted into a Deferred via `_from_awaitable`,
  rather than being wrapped as an opaque plain value. This prevents an
  un-awaited-coroutine warning and matches Twisted's behavior of treating a
  returned awaitable as deferred work. `MaybeDeferredAwaitableTests` covers the
  coroutine-result, Future-result, coroutine-failure (errback), and
  no-un-awaited-warning cases.
- **`callback` on an already-resolved Deferred** raises `AlreadyCalledError`.
- **`DeferredList`/`gatherResults`** build over
  `asyncio.gather(*deferreds, return_exceptions=True)`, reshaping each entry to
  `(not _is_failure(r), r_or_failure)`; honor `consumeErrors` and the `fireOnOne*`
  flags.

### 3.2 `failure.py`

```python
class Failure(object):                        # NOT an Exception subclass
    _is_failure = True                        # cross-shim marker (no shared import)
    def __init__(self, exc=None, exc_type=None, traceback=None): ...
    value: Exception          # the wrapped exception instance
    type: type                # exception class
    def getErrorMessage(self) -> str
    def getTraceback(self) -> str             # used in test_case/apptest/voicemail/...
    def check(self, *errorTypes) -> "type|None"
    def trap(self, *errorTypes) -> "type"     # re-raise if no match
    def raiseException(self) -> NoReturn
```

Captures the current exception when constructed inside an `except` block.
`.value`, `.type`, `.getErrorMessage`, and `.getTraceback()` are all used in
first-party code (review issue 2); `.check`/`.trap` serve the process adapter and
completeness. The class-level `_is_failure = True` marker lets either shim's
Deferred recognize a `Failure` produced by the other without importing it.

### 3.3 `reactor.py` (transitional — design §3.2/§14)

A singleton `reactor` object plus the loop lifecycle:

```python
class _Reactor:
    running: bool
    def callLater(self, delay, fn, *args, **kw) -> _DelayedCall
    def callWhenRunning(self, fn, *args, **kw) -> None
    def callInThread(self, fn, *args, **kw) -> Deferred   # via run_in_executor
    def callFromThread(self, fn, *args, **kw) -> None      # loop.call_soon_threadsafe
    def listenUDP(self, port, proto, interface='', maxPacketSize=8192) -> _UDPPort
    def listenTCP(self, port, factory, backlog=50, interface='') -> _TCPPort
    def connectTCP(self, host, port, factory, timeout=30, bindAddress=None) -> _Connector
    def spawnProcess(self, proto, executable, args=(), env={}, path=None, ...) -> transport
    def run(self) -> None      # owns loop until stop()
    def stop(self) -> None     # idempotent; resolves the main future

class _DelayedCall:            # wraps asyncio.TimerHandle
    def cancel(self) -> None
    def active(self) -> bool

reactor = _Reactor()
```

- `callLater` → `loop.call_later`; `_DelayedCall.active()` tracks fired/cancelled
  state (used implicitly; `cancel` used explicitly at the `timeout_id` and
  `_stop_cancel_tokens` sites).
- `listenUDP(port, proto)` → `loop.create_datagram_endpoint(lambda: proto,
  local_addr=(interface or '0.0.0.0', port))`; returns a port handle with
  `stopListening()`.
- `listenTCP(port, factory)` → `loop.create_server(factory.buildProtocol-adapter,
  ...)`; the factory adapter bridges Twisted-style `buildProtocol(addr)` used by
  starpy/FastAGI (Step 2/4).
- `connectTCP(host, port, factory, timeout, bindAddress)` → a reconnect-capable
  `_Connector` whose `connect()` calls `loop.create_connection(...,
  local_addr=bindAddress or None)` wrapped in `asyncio.wait_for(coro, timeout)`
  when a timeout is set. Both options are honored on every attempt (review
  blocker 2). The connector calls `factory.startedConnecting` before each
  attempt, routes a bind failure (including a `wait_for` timeout) to
  `factory.clientConnectionFailed`, and on an *established-then-lost* connection
  the protocol adapter calls `factory.clientConnectionLost(connector, reason)`
  (review blocker 1) — a `ReconnectingClientFactory.retry` then calls
  `connector.connect()` again, which is exactly how starpy reconnect works.
  `connector.disconnect()` sets a `_stopped` flag (suppressing further
  reconnects, e.g. during shutdown) and calls `factory.stopTrying()`. Server
  connections from `listenTCP` have no connector and are never given
  `clientConnectionLost`. Covered by `TCPClientNotificationTests` and
  `ConnectTCPOptionsTests`.
- `spawnProcess` → `loop.subprocess_exec` with the `ProcessProtocol` adapter
  (§3.4); returns the transport stored as `self.process` in `asterisk.py`/`sipp.py`.
- **Awaited startup + bind-failure propagation (review issue 6).** `listenUDP`/
  `listenTCP`/`spawnProcess` register their bind coroutine on `_pending_binds`
  rather than scheduling it loose. `run()` first drains `_pending_binds` with
  `run_until_complete`; a bind that raises with no `on_error` records `_failure`,
  runs `_shutdown()`, and re-raises out of `run()`. Binds issued *while already
  running* become tracked tasks whose failure calls the caller's `on_error`
  (`connectTCP` routes to `factory.clientConnectionFailed` for starpy reconnect) or
  `_fatal`. Covered by `BindFailureTests`.
- **Resource-owned ordered shutdown (review issue 7).** The reactor keeps
  registries — `_delayed_calls`, `_tasks`, `_ports`, `_connectors`,
  `_process_transports`. `_shutdown()` is async and ordered: cancel delayed calls,
  `stopListening` on ports, disconnect connectors, terminate+close process
  transports, then cancel and `gather(..., return_exceptions=True)` tasks. It then
  cancels any *remaining* loop tasks — `defer.maybeDeferred`/`_from_awaitable` and
  `aio.utils` schedule work with `asyncio.ensure_future` directly, outside the
  reactor registries, so stopping with such a task pending would otherwise leak a
  live task past `run()`. All not-done tasks except the running `_shutdown`
  coroutine are cancelled and awaited. `aio.utils.getProcessOutputAndValue`
  terminates and reaps its child on cancellation, so a pending subprocess is not
  left running. `run()` does `run_until_complete(main_future)`, then
  `_shutdown()`, then re-raises a recorded `_failure`. Clean under
  `-W error::ResourceWarning`; `ShutdownStrayTaskTests` regresses both the stray
  task and the subprocess-termination cases.
- **Idempotent `stop()` (review blocker 3).** Calling `stop()` when the reactor is
  not running is a no-op `return`, not a `ReactorNotRunning` raise. Consumers
  (and `asterisk.py`'s shutdown path) treat shutdown as safe to request more than
  once; the `ReactorNotRunning` class is retained for parity but no longer raised
  by `stop()`. Covered by `ReactorStopIdempotentTests`.

### 3.4 `protocols.py`

```python
class DatagramProtocol(asyncio.DatagramProtocol):
    transport: _DatagramWriter         # .write(data, addr) -> sendto; .loseConnection()->close
    def startProtocol(self): ...        # called from connection_made
    def stopProtocol(self): ...
    def datagramReceived(self, data, addr): ...    # from datagram_received
    def connectionMade(self): ...                  # optional, some fixtures define it
    # connection_made / datagram_received implemented to dispatch to the above

class ProcessProtocol(asyncio.SubprocessProtocol):
    transport
    def connectionMade(self): ...
    def outReceived(self, data): ...    # from pipe_data_received(1, data)
    def errReceived(self, data): ...    # from pipe_data_received(2, data)
    def processEnded(self, reason): ... # from process_exited; reason is a Failure
    #   whose .value.exitCode = returncode and .type in {ProcessDone, ProcessTerminated}

class ProcessDone(Exception): ...
class ProcessTerminated(Exception):
    exitCode: int|None
    signal: int|None

class LoopingCall:
    def __init__(self, fn, *args, **kw): ...
    def start(self, interval, now=True) -> Deferred
    def stop(self) -> None
```

`processEnded`'s `reason` is constructed so existing reads
(`reason.value and reason.value.exitCode`, `reason.type == ProcessTerminated`) are
unchanged. `LoopingCall.start` returns a `Deferred` (the `strict_rtp_seqno` fixture
assigns it) and reschedules via `reactor.callLater`.

Two correctness points from review issue 1, both implemented:

- **Pipe-drain ordering.** asyncio can deliver `process_exited` before the final
  `pipe_data_received`/`pipe_connection_lost`. The adapter tracks `_proc_exited`,
  `_stdout_open`, `_stderr_open` separately and fires `processEnded` only once all
  three are satisfied (`_maybe_end`), then closes the transport — so no trailing
  Asterisk/SIPp output is lost. Pipes the child never received are treated as
  already closed. `PipeDrainTests` verifies 200 000 bytes survive an early exit.
- **No `super().__init__` requirement.** `AsteriskProtocol`/`SIPpProtocol` do not
  call the base constructor, so the lifecycle flags (`transport`, `_proc_exited`,
  `_stdout_open`, `_stderr_open`, `_ended`, `_returncode`) are **class-level
  defaults** shadowed on first assignment; the base class defines no `__init__`.

### 3.5 `utils.py`

`asterisk.py` is the sole consumer of `twisted.internet.utils`, using only
`getProcessOutputAndValue` (line 203). The import swap therefore needs a matching
`asterisk.aio.utils`:

```python
def getProcessOutputAndValue(executable, args=(), env=None, path=None) -> Deferred
```

Built on `asyncio.create_subprocess_exec` + `communicate()`. The firing contract
mirrors Twisted exactly so `asterisk.py`'s `_set_properties` is unchanged: on a
normal exit (any exit code) the callback fires with `(stdout_bytes, stderr_bytes,
exit_code)`; on termination by a signal the errback fires with a `Failure` whose
`.value` is `(stdout_bytes, stderr_bytes, signal_number)` (so
`_set_properties(result.value)` still unpacks a 3-tuple). This matches Twisted's
documented `getProcessOutputAndValue` contract
(<https://docs.twisted.org/en/stable/api/twisted.internet.utils.html>). `args`
follows the os-level convention (no program name), matching `self._cmd[1:]` at
the call site. `aio/__init__.py` exports it as `from . import utils`. Covered by
`GetProcessOutputAndValueTests`.

### 3.6 Step 1 exit criterion

`asterisk.aio` imports cleanly and the unit-test module
(`lib/python/asterisk/aio/test_aio.py`) passes — currently 49 tests, green and
clean under `-W error::ResourceWarning`. Coverage: callback/errback threading and
branch-switching, late-added callbacks, `AlreadyCalledError`, `DeferredList`/
`gatherResults` shaping + `consumeErrors`, `maybeDeferred` (incl. awaitable
adaptation), `_DelayedCall.cancel`, a round-trip UDP echo and a subprocess exit
through the adapters, plus the review-driven classes: `LateCallbackAwaitTests`,
`CrossShimTests`, `HelperTests`, `BindFailureTests`, `PipeDrainTests`,
`CallInThreadTests`, and the blocker-driven classes `TCPClientNotificationTests`,
`ConnectTCPOptionsTests`, `ReactorStopIdempotentTests`,
`MaybeDeferredAwaitableTests`, `GetProcessOutputAndValueTests`,
`ShutdownStrayTaskTests`. No suite behavior has changed yet (nothing imports
`aio` outside its tests).

## 4. Step 2 — convert the starpy fork

**Scope of Twisted in the fork.** The AST gate finds Twisted imports in 19 files:
the 2 shipped package modules `starpy/{manager.py, fastagi.py}`, and 17
standalone scripts under `starpy/examples/` (including
`examples/autosurvey/frontend.py`). Only the package modules are imported by the
suite (`starpy.manager`, `starpy.fastagi`) and installed with the package; the
examples are demo programs run by hand and are not part of the import graph or
the wheel. See §4.5 for their disposition — they are declared out of scope, and
the default gate scans the package only.

Files to convert: `/usr/src/phzyx/starpy/starpy/{manager.py, fastagi.py}`, new
`starpy/_async.py`, `pyproject.toml`. `error.py`/`__init__.py` docstring-only.

### 4.1 `starpy/_async.py` (self-contained, mirrors `asterisk.aio`)

starpy must not import the test suite, so it carries its own minimal copy of the
pieces it needs: `Deferred`, `maybeDeferred`, a `call_later` helper, and a
`LineProtocol` base (asyncio line buffering reproducing
`basic.LineOnlyReceiver.lineReceived`). Interop with the suite's `Deferred` does
**not** rely on shared `asyncio.Future` ancestry (review issue 4): `starpy._async`
reuses the same interoperability contract — pause on any *Deferred-like* (callable
`addBoth`) or awaitable, and the `_is_failure = True` marker for cross-shim failure
routing — so a starpy `Deferred` and a suite `Deferred` chain in both directions.
`CrossShimTests` exercises this explicitly.

### 4.2 `manager.py` (AMI client)

| Twisted construct | Change |
|-------------------|--------|
| `from twisted.internet import protocol, reactor, defer` / `protocols.basic` / `error as tw_error` | replace with `from . import _async` (Deferred, call_later, LineProtocol) and stdlib |
| `AMIProtocol(basic.LineOnlyReceiver)` | `class AMIProtocol(_async.LineProtocol)`; keep `lineReceived`, `connectionMade`, `connectionLost`, `sendDeferred`, `sendMessage`, `collectDeferred`, all action methods unchanged in signature |
| `deferredErrorResp(defer.Deferred)` | subclass `_async.Deferred` |
| `AMIFactory(protocol.ReconnectingClientFactory)` | replace with an asyncio connector that implements `login(ip, port, timeout, bindAddress)` → `Deferred`, using `loop.create_connection`; reproduce `clientConnectionFailed` (errback `loginDefer`) and `clientConnectionLost` (reconnect with backoff + `on_reconnect(self.loginDefer)`) explicitly |
| `reactor.connectTCP(ip, port, self, timeout, bindAddress)` | `loop.create_connection(protocol_factory, ip, port, local_addr=bindAddress or None)` wrapped in `asyncio.wait_for(coro, timeout)`; on failure call the failed-path; on an established-then-lost connection call the lost-path and re-`connect()` (the same reconnect contract the suite's `_Connector` implements) |

Preserve the public surface exactly: `manager.AMIFactory(user, secret[, id,
plaintext_login, on_reconnect])`, `.login(host, port)` returning a `Deferred`, and
the `AMIProtocol` methods the suite calls (`registerEvent`, `deregisterEvent`,
`sendDeferred`, `collectDeferred`, etc.). The reconnect/backoff that
`ReconnectingClientFactory.retry` provided is reimplemented in the connector.

### 4.3 `fastagi.py` (FastAGI server)

| Twisted construct | Change |
|-------------------|--------|
| `FastAGIProtocol(basic.LineOnlyReceiver)` | `_async.LineProtocol`; keep `lineReceived`, `lostConnectionDeferred` (now `_async.Deferred`) |
| `FastAGIFactory(protocol.Factory)` | an asyncio-compatible factory: a `buildProtocol(addr)` returning a `FastAGIProtocol`, consumable by the suite's `reactor.listenTCP` adapter |
| `reactor.callLater` (in `wait`) | `_async.call_later` |
| `defer.maybeDeferred` (in `InSequence`) | `_async.maybeDeferred` |

### 4.4 Packaging + exit criterion

- `pyproject.toml`: remove `Twisted >= 24.10.0` from `dependencies`.
- Exit criterion: `doc/untwist/check_no_twisted.py` reports no banned imports in
  the shipped starpy package (scanned by default as `../starpy/starpy`); a
  standalone smoke test connects an AMI client to a stub/real Asterisk and runs
  one action, and a FastAGI server accepts one connection and completes a simple
  dialog.
  Validate before wiring into the suite.

### 4.5 `starpy/examples/` — out of scope

The 17 demo scripts under `starpy/examples/` import Twisted directly
(`twisted.internet.reactor`, `twisted.application`, etc.). They are **declared out
of scope** for the migration: they are not imported by the suite or the `starpy`
package, are not installed by `pyproject.toml`, and exercise no code path the
definition-of-done covers. Porting all 17 would be effort spent on material the
suite never runs.

Consequences and options, in the maintainer's hands:

- The **default gate does not scan `examples/`** — `_default_roots()` targets the
  shipped package `../starpy/starpy`, so the examples cannot block Step 2. They
  remain auditable on demand: `check_no_twisted.py ../starpy` scans the whole
  fork and will still report them.
- Preferred follow-up: **port opportunistically or delete**. Because `_async`
  mirrors the `Deferred`/`call_later`/factory surface these scripts use, a later
  pass can convert the ones worth keeping (`hellofastagi`, `amicommand`,
  `menu`, the `autosurvey` app) and drop the rest, then widen the gate back to
  `../starpy` to enforce it permanently.
- Until then this is a documented, bounded exception rather than a silent gap:
  the fork-wide count is 19 Twisted files = 2 in scope (converted) + 17 demos
  (excluded).

## 5. Step 3 — convert the core

### 5.1 `test_runner.py`

- `from twisted.internet import reactor` → `from asterisk.aio import reactor`.
- `reactor.run()` (line 310) unchanged in form (now drives the asyncio loop).
- Exit criterion: a trivial test object reaches `run()` and the process exits on
  `stop()`.

### 5.2 `test_case.py`

- Import swap: `from asterisk.aio import reactor, defer` and
  `from asterisk.aio.failure import Failure`; drop
  `from twisted.python import log` and remove the
  `log.PythonLoggingObserver().start()` install (stdlib logging already
  configured) — line ~187.
- `reactor.callWhenRunning(self._run)` (187/190), `reactor.callLater` (584, 952,
  982), `defer.DeferredList` (472, 515), `defer.Deferred` (519), and the
  `reactor.stop()` guard (533–536): all work unchanged via the shim. Replace
  `except twisted_error.ReactorNotRunning` with the shim's idempotent `stop()`
  (catch nothing, or catch `aio.reactor.NotRunning` if we define it).
- `reactor.listenTCP(4573, fastagi_factory, ...)` (384) uses the listenTCP adapter
  + the converted starpy `FastAGIFactory`.
- `create_fastagi_factory` / `fastagi_connect` unchanged (starpy API preserved).
- Exit criterion: a no-Asterisk test and a single-Asterisk test run `_run` →
  `run` → timeout/stop cleanly.

### 5.3 `asterisk.py`

- Import swap for `reactor, protocol, defer, utils, error` and `Failure`,
  `FilePath`.
- `AsteriskProtocol(protocol.ProcessProtocol)` → `aio.ProcessProtocol`; methods
  unchanged. `reason.value.exitCode` / `reason.type == ProcessTerminated` reads
  unchanged (adapter supplies the shape).
- `reactor.spawnProcess(self.protocol, ...)` (468) → shim `spawnProcess`; store
  returned transport as `self.process`.
- The `reactor.callLater` start/stop/wait-fully-booted chains (464–649) unchanged.
- **SSH (remote instances), lines 33–38, 99–120:** replace the lazy `twisted.conch`
  imports and `SSHCommandClientEndpoint.newConnection(...)` with an `asyncssh`
  helper: `Key.fromFile` → `asyncssh` `client_keys`; `KnownHostsFile.fromPath` →
  `known_hosts`; `UNIXClientEndpoint`(agent) → asyncssh agent support;
  `SSHCommandClientEndpoint` run → `await conn.run(cmd)`. Wrap behind the existing
  internal method boundary so callers are unchanged.
- `twisted.python.filepath.FilePath` → `pathlib.Path`.
- Exit criterion: local Asterisk start/stop works; `self_test` harness
  (`lib/python/asterisk/self_test`) passes. Remote/SSH path validated separately
  against the design §6.5 SSH contract (host-key policy, agent, encrypted keys,
  password, command quoting, stdout/stderr separation, exit status, timeout,
  connection closure) if a remote target is available.

## 6. Step 4 — convert the subsystems

Each is an import swap plus the subsystem-specific replacement from design §6.

| Module | Change | Exit check |
|--------|--------|-----------|
| `ami.py` | import swap; `manager.AMIFactory` now the asyncio fork; `reactor.callLater` reconnect (682) unchanged | an AMI-driven test passes |
| `ari.py` | drop autobahn; client coroutine on `websockets.connect` dispatching to `onOpen/onMessage/onClose` logic; reconnect via `reactor.callLater` preserved. Wrap sync `sendMessage`/`sendClose` over coroutine send, marshalled onto the loop thread (design §6.1) | an ARI/WebSocket test passes |
| `media_websocket.py` | autobahn → `websockets` (client/server as used). `sendFile` reads in an executor but every send is scheduled on the loop via `run_coroutine_threadsafe` (no cross-thread loop access); preserve frame fragmentation, subprotocol selection, binary/text framing, close callbacks (design §6.1) | media websocket test + fragmentation parity test pass |
| `sipp.py`, `sipp_iterator.py` | `SIPpProtocol(ProcessProtocol)` → `aio.ProcessProtocol`; `spawnProcess` (757) shim; `Deferred`/`DeferredList` via shim | a SIPp test passes |
| `dns_server.py` | `twisted.names` → `dnslib` responder over asyncio UDP + TCP servers (listens both, lines 54–55); bind via the awaited-startup listener so the server is answering before Asterisk queries | DNS parity test (zones, record types, AA, NXDOMAIN/NODATA, UDP truncation→TCP, framing) vs Twisted baseline, design §6.5 |
| `http_static_server.py` | `twisted.web` Site/static → `aiohttp` static route on the configured port | static-file + path-traversal parity test, design §6.5 |
| `realtime_test_module.py` | `twisted.web` `Resource`/`NoResource` (RootResource/TableResource) → `aiohttp` routes; `listenTCP(46821)` via aiohttp runner | realtime test: route match, query/form parse, bodies, status codes, headers (design §6.5) |
| `pcap.py`, `pcap_proxy.py`, `matcher_listener.py` | `DatagramProtocol` → `aio.DatagramProtocol`; `listenUDP` shim; `transport.write(data, addr)` preserved | pcap/matcher exercised |
| `pluggable_modules.py` | import swap; `fastagi.FastAGIFactory` (fork) + `listenTCP(self.port, ...)` (700) | FastAGI module test passes |
| `pcap_listener.py` | delete the vestigial `from twisted.internet import abstract, protocol` | imports clean |

## 7. Step 5 — test-condition family and fixtures

### 7.1 `*_test_condition.py` + `test_conditions.py`

Mechanical: `from twisted.internet import defer` →
`from asterisk.aio import defer`. These use only `Deferred`/`DeferredList`. Files:
`channel_test_condition.py`, `fd_test_condition.py`, `lock_test_condition.py`,
`thread_test_condition.py`, `sip_channel_test_condition.py`,
`sip_dialog_test_condition.py`, `pjsip_channel_test_condition.py`,
`taskprocessor_test_condition.py`, `test_conditions.py`, and
`self_test/harness_shared.py`. Plus `apptest.py`, `confbridge.py`, `originate.py`,
`extension_bank.py` (reactor/defer import swaps).

### 7.2 The 32 `tests/**` fixtures

(32 fixtures *import* Twisted; 34 files mention the string "twisted", the extra
two only in comments. Counts regenerated via `doc/untwist/check_no_twisted.py`,
excluding the new `asterisk/aio/` layer. The first-party library tier is 30
modules importing Twisted, again excluding `aio/`.)

> **Scope correction (found during Step 5):** the "32 fixtures" count came from
> a `.py`-only scan and therefore *missed the per-test `run-test` entry
> scripts*, which are executable Python modules with a `#!` line and **no `.py`
> suffix**. 146 of them do `from twisted.internet import reactor` and call
> `reactor.run()`; left unconverted they would break the moment Step 6 removes
> Twisted. All 146 take the same trivial `from asterisk.aio import reactor`
> swap (the reactor attrs they touch — `run`, `callLater`, `listenTCP`, `stop`,
> `running`, `callWhenRunning` — are all provided by `aio.reactor`). The gate
> had the same blind spot: `check_no_twisted.py` now also scans files named
> `run-test`, but only when their shebang names a Python interpreter (some
> directories ship a *bash* `run-test`, which must not be AST-parsed).

Convert by the templates established in Steps 1/4:

- **UDP listeners** (RTP/HEP/strict-rtp — 6+ files): import swap + `DatagramProtocol`
  adapter; `reactor.listenUDP` shim; `task.LoopingCall` → `aio.LoopingCall`
  (`strict_rtp_seqno`).
- **WebSocket fixtures** (`tests/channels/websocket/*`): autobahn → `websockets`
  server/client.
- **Timer/orchestration fixtures** (`audio_analyzer.py`, `recording/*`,
  `message_modules.py`, etc.): import swap; `reactor.callLater` preserved.
- **TCP client** (`keep_alive.py`): `reactor.connectTCP` shim.
- **AMI/AGI-driven fixtures** (`tests/manager/*`, `tests/pbx/*`,
  `rest_api/*`): rely on the converted framework AMI/AGI; usually import-swap only.

Exit criterion: each converted fixture's test passes individually.

> **Review findings fixed before sign-off (three High):**
>
> - **F1 — YAML dependency declarations (735 twisted + 184 autobahn files).**
>   Each `test-config.yaml` lists prerequisites as `- python: 'twisted'`; the
>   framework resolves them by `__import__` and, on failure, marks the test
>   *unmet* and **silently skips** it (`test_config.py`). Left untouched, Step 6's
>   package removal would turn hundreds of tests into no-ops with no error. All
>   736 twisted lines were repointed to `asterisk.aio` and all 185
>   `autobahn[.websocket]` lines to `websockets`, preserving each file's exact
>   quote/spacing style (four variants: `python :`/`python:` × quoted/bare). The
>   AST gate was data-blind here, so `check_no_twisted.py` now also textually
>   scans uncommented `python:` dep lines in `*.yaml` and fails on a banned
>   module — the category is now gate-enforced against regressions.
> - **F2 — `AriClientProtocol.sendClose()`.** The inbound-WebSocket media client
>   calls `self.protocol.sendClose(1000)` on the ARI client protocol (autobahn
>   `WebSocketProtocol` API). The websockets port had dropped that surface, so the
>   fixture failed then timed out. Re-added `sendClose(code=1000, reason="")`
>   delegating to `dropConnection`; covered by `test_ari_parity` (close fires on
>   both ends).
> - **F3 — synchronous `listenUDP` transport.** Twisted's `listenUDP` binds and
>   installs `protocol.transport` before returning; the strict-RTP fixtures send a
>   datagram on the very next line. asyncio's `create_datagram_endpoint` is a
>   coroutine, so `protocol.transport` was still `None` at that point and
>   `strict_rtp_seqno`/`strict_rtp_yes` raced. `listenUDP` now binds the socket
>   synchronously and installs a `_SyncDatagramTransport` over that fd immediately;
>   the asyncio receive path is wired from the same socket and upgrades the
>   transport on `connection_made`. Early `stopListening()` (before the endpoint
>   coroutine runs) is handled gracefully. Covered by
>   `UDPSyncTransportTests.test_transport_usable_before_endpoint_awaited`.
> - **F4 — synchronous `spawnProcess` transport (same class as F3, found in
>   manual review).** Twisted wires `protocol.transport` inside `spawnProcess`;
>   the suite kills scenarios on the next line — SIPp's `kill()` calls
>   `self.transport.signalProcess('KILL')` straight from an AMI `TestEvent`
>   handler (`sipp.py`), and `asterisk.py`'s stop path signals `self.process`
>   (the connector, which delegates to `protocol.transport`). asyncio's
>   `subprocess_exec` is a coroutine, so `connection_made` had not yet installed
>   the transport and the kill raised `'NoneType' object has no attribute
>   'signalProcess'`. `spawnProcess` now installs a `_PendingProcessTransport`
>   synchronously that buffers `signalProcess`/`loseConnection` and replays them
>   in `connection_made` once the child exists. Covered by
>   `SubprocessSyncKillTests.test_kill_before_connection_made_is_replayed` (a
>   long-lived child killed synchronously must end via signal, not the safety-net
>   timeout).

## 8. Step 6 — dependency strip and final gate

1. Rewrite `requirements.txt`:
   - **Remove:** `Twisted`, `txaio`, `autobahn`, `Automat`, `constantly`,
     `hyperlink`, `incremental`, `zope.interface`. Audit and likely remove
     `service_identity`, `pyOpenSSL`, `pycparser`, `cffi`, `six`, `constantly`
     (keep any still pulled by a kept package).
   - **Add (version-pinned, consistent with the rest of `requirements.txt`):**
     `websockets`, `aiohttp`, `dnslib`, `asyncssh`. Pin to exact reviewed
     versions (e.g. `websockets==X.Y`), not floating ranges, so the venv is
     reproducible.
   - **Repoint:** `starpy` → the converted fork pinned to a **reviewed commit
     hash or release tag**, not a moving branch
     (`git+https://github.com/phzyx/starpy@<commit-sha>`), so rebuilds are
     reproducible. Do not point at `master-untwisted`.
2. Rebuild the venv via `setupVenv.sh` (the `md5sum` checksum gate in
   `runInVenv.sh` triggers reinstall automatically).
3. **Definition-of-done gate** — run `python3 doc/untwist/check_no_twisted.py`.
   This is an **AST import check, not a literal grep**, so it does not
   false-positive on the explanatory prose inside `asterisk.aio` (review finding
   11). Because only imports are flagged, the `asterisk/aio/` layer is itself
   scanned (no directory exclusion): it may mention Twisted in comments but must
   not import it, and the gate proves that. It verifies two things and exits
   non-zero on either:
   - No `twisted` / `txaio` / `autobahn` *imports* remain in `lib`, `tests`, or
     the shipped starpy package (`../starpy/starpy`, scanned by default).
     `starpy/examples/` is out of scope (§4.5) and not in the default set; audit
     it explicitly with `check_no_twisted.py ../starpy` if desired.
   - None of those packages are importable in the active venv (`find_spec`).
   Additionally run `pip check` inside the venv and assert a clean dependency
   tree after the strip.
4. **Full-suite parity run** vs. the Twisted baseline; triage any diffs.

## 9. Testing matrix (parity targets)

Run these representative tests after the relevant step and again at the end,
comparing pass/behavior to the pre-branch Twisted baseline:

| Subsystem | Representative test area |
|-----------|--------------------------|
| Compat layer | `asterisk.aio.test_aio` (35 tests incl. late-callback/await, cross-shim, bind-failure, pipe-drain) — must be green before any consumer |
| AMI | `tests/manager/*` (e.g. device/exten/presence state list) |
| FastAGI | a FastAGI-using test via `pluggable_modules` |
| ARI / WebSocket | `tests/rest_api/applications/stasisstatus`, recording tests |
| WebSocket framing | `tests/channels/websocket/inbound/basic-call` fragmentation + subprotocol/binary/close parity (design §6.1) |
| SIPp | a `tests/channels/pjsip` SIPp scenario |
| Process control | any single- and multi-Asterisk test |
| UDP/RTP | `tests/rtp/strict_rtp/*`, `tests/channels/pjsip/rtp/*` |
| HEP | `tests/hep/*` |
| DNS | a test exercising `dns_server` (parity contract, design §6.5) |
| HTTP server | a realtime / static-server test (parity contract, design §6.5) |
| SSH (remote) | remote-instance control if a target is available (parity contract, design §6.5) |

## 10. Risks specific to execution

- **starpy reconnect parity.** `ReconnectingClientFactory.retry` had specific
  backoff behavior; the reimplemented connector must match closely enough for
  reconnect-sensitive tests. Validate in the Step 2 smoke test and an AMI
  reconnect test.
- **Deferred chaining bugs** surface as wrong values / swallowed errors across many
  tests at once — caught early by the Step 1 unit tests, which is why Step 1 ships
  before any consumer.
- **Shutdown hangs** (lingering tasks/transports) — covered by the reactor's
  resource registries + ordered async `_shutdown()` (§3.3), verified clean under
  `-W error::ResourceWarning`; watch for tests that previously relied on reactor
  stop timing.
- **Listener readiness / bind failures** — the awaited startup phase (§3.3) makes a
  failed bind fail the test rather than time out; watch for fixtures that assumed a
  listener was up synchronously.
- **DNS/SSH** are low-frequency; validate explicitly against the §6.5 contracts
  since few tests cover them.

## 11. Tracking checklist

- [x] Step 0 — env + baseline captured
- [x] Step 1 — `asterisk.aio` built + unit tests green
- [x] Step 2 — starpy fork converted + smoke test green + `pyproject.toml` updated
- [x] Step 3 — core converted + `self_test` green
- [x] Step 4 — subsystems converted (ami/ari/sipp/dns/http/ws/udp/pluggable)
- [x] Step 5 — test-condition family + fixtures converted (32 `.py` fixtures + 146 `run-test` entry scripts; gate extended to scan Python `run-test` files; review findings F1/F2/F3 fixed: 736 twisted + 185 autobahn YAML deps repointed and gate-enforced, `AriClientProtocol.sendClose` restored, synchronous `listenUDP` transport)
- [x] Step 6 — deps stripped (Twisted/txaio/autobahn/Automat/constantly/hyperlink/incremental/zope.interface + service_identity + six removed; pyOpenSSL kept — `opensslversion.py` uses it for OpenSSL-version test gating, and it reports the cryptography-bundled 3.2.0, preserving baseline gating; cffi/pycparser/attrs kept as transitive deps of cryptography←asyncssh / aiohttp), venv rebuilt clean, AST import gate + `pip check` clean, parity spot-run green (AMI, SIPp/process, HTTP static, ARI/websockets, WS framing, DNS)
