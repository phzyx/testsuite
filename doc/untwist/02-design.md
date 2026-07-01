# Removing Twisted from the Asterisk Test Suite — Section 2: Design

Status: Draft for review
Branch: `master-untwisted`
Date: 2026-06-30
Companion to: `doc/untwist/01-scope-analysis.md`

## 1. Purpose and approach

This document specifies the target architecture for running the test suite on
Python's native `asyncio` event loop with **no dependency on Twisted** (directly
or transitively, including the `starpy` fork). It turns the scope analysis into
concrete design decisions that the implementation document (Section 3) will
sequence into work items.

### 1.1 Decisions taken (inputs to this design)

These were settled before drafting and drive everything below:

1. **Async programming model: Future + callbacks (drop-in).** The framework keeps
   its `Deferred`/`addCallback` *style*. We provide an `asyncio.Future`-backed
   compatibility layer that reproduces the `Deferred` API rather than rewriting
   hundreds of callback chains into `async`/`await`. This minimizes churn across
   the 62 first-party files and the ~180 `Deferred`/callback call sites.
2. **Cutover: hard cutover of the core, then fixtures.** `test_runner.py`,
   `test_case.py`, the starpy fork, `asterisk.py`, and the rest of the core
   library are converted in one coordinated change so the process has a single
   event loop at all times; the 32 test fixtures are then swept using established
   templates. No Twisted/asyncio bridge is introduced. (Alternative considered:
   `AsyncioSelectorReactor` — see §1.3.)
3. **WebSockets: the `websockets` library.** `autobahn.twisted` (and `txaio`) are
   dropped from `ari.py` and `media_websocket.py`.
4. **Standalone servers:** `asyncssh` for SSH (remote Asterisk control),
   `aiohttp` for the HTTP servers (`http_static_server.py`,
   `realtime_test_module.py`), `dnslib` for the DNS server (`dns_server.py`), and
   stdlib/asyncio directly wherever a full library is unnecessary (UDP endpoints,
   line protocols, subprocess control).
5. **Compatibility package name: `asterisk.aio`.**
6. **The `reactor`-shaped shim is a transitional migration aid only.** It exists
   to get the suite onto the asyncio loop quickly with a reviewable diff; it is
   **not** part of the intended end state. After the cutover is green, a
   modernization pass (§14) inlines the reactor calls to native asyncio and
   deletes the shim. The end result should be idiomatic, modern asyncio — not a
   reactor emulation that happens to run on asyncio.

### 1.2 Design principle

**Confine the asyncio knowledge to a small compatibility layer; keep everything
else looking almost exactly like it does today.** The bulk of the migration
becomes mechanical import swaps plus a handful of adapter classes, which makes the
diff reviewable and the behavior easy to compare against the Twisted baseline.

### 1.3 Alternative considered: `AsyncioSelectorReactor` bridge (review issue 9)

The scope analysis originally implied the two loops simply cannot coexist. That is
too strong: Twisted ships `AsyncioSelectorReactor` (Twisted running *on* an asyncio
event loop) plus `Deferred.fromFuture`, `Deferred.asFuture`, and coroutine
adapters. This would let asyncio own the single process loop while Twisted's
*mature* Deferred, process, DNS, web, and transport implementations keep running,
and each subsystem is then replaced incrementally — a lower-risk path than
reimplementing those behaviors in the compat layer up front.

Why the hard cutover is still chosen here: the end-state goal is **zero Twisted in
the tree and the environment** (the §10 gate asserts Twisted is not even
importable). The bridge keeps Twisted installed and on the critical path for the
entire migration, so it defers — rather than removes — every parity risk, and the
final Twisted-removal step still has to happen. It also adds a second concurrency
model to reason about during the transition. The bridge remains a viable fallback
if the compat layer proves too costly for a specific subsystem (DNS is the most
likely candidate); the trade-off is recorded here so the choice is revisitable per
subsystem rather than assumed away.

## 2. Target architecture overview

Today the process is owned by the Twisted reactor: `test_runner.main()` builds the
test object, then calls `reactor.run()`, which blocks until something calls
`reactor.stop()`. Every timer, socket, subprocess, and Deferred is serviced by the
reactor.

The target replaces that single owner with the asyncio event loop, but preserves
the same shape via a compatibility module:

```
test_runner.main()
  └─ aio.run(test_object)        # was reactor.run()
       └─ asyncio loop.run_until_complete(_main_future)
            ├─ call_when_running hooks     # was reactor.callWhenRunning
            ├─ call_later timers           # was reactor.callLater
            ├─ datagram/stream endpoints   # was listenUDP/listenTCP/connectTCP
            ├─ subprocess transports       # was spawnProcess/ProcessProtocol
            └─ Deferred(Future) chains     # was defer.Deferred/DeferredList
       └─ stop() resolves _main_future     # was reactor.stop()
```

A new package, **`asterisk.aio`** (working name), houses the compatibility
layer. Most modules change only their import line:

```python
# before
from twisted.internet import reactor, defer
# after
from asterisk.aio import reactor, defer
```

`reactor` and `defer` here are our shims, not Twisted's. The starpy fork carries
its own equivalent internal module (§7) so it stays a self-contained package with
no dependency on the test suite.

## 3. The compatibility layer (`asterisk.aio`)

This is the keystone of the migration. It has three parts: a `Deferred` shim, a
`reactor` shim, and a set of protocol/transport adapters.

### 3.1 `defer` — Deferred as a Future *wrapper* (not a subclass)

`Deferred` is implemented as a standalone object that *wraps* asyncio Future
machinery rather than subclassing `asyncio.Future`. The review (issue 3) showed
why subclassing cannot work: an `asyncio.Future` result is immutable once settled,
but a Twisted `Deferred` keeps threading its *current* result through callbacks
added after it has fired. The canonical example must hold:

```python
d = Deferred()
d.callback(1)
d.addCallback(lambda value: value + 1)
result = await d            # must be 2, not 1
```

A Future subclass returns 1 here (the settled result), so `Deferred` instead keeps
mutable chain state (`_chain_result`) and exposes `__await__` over an internal
idle event that fires only when the chain is quiescent
(`_called and not _running and not _paused`). `await d` therefore observes the
latest chain result, and chaining a new callback after settling re-runs and
updates it. This is implemented and covered by `LateCallbackAwaitTests`.

Required surface, taken from the actual call sites in the suite (counts from the
scope survey):

| Method / symbol | Sites | Semantics to reproduce |
|-----------------|------:|------------------------|
| `addCallback(fn, *a, **kw)` | 68 | append a success stage; fn's return feeds the next stage |
| `callback(result)` | 50 | fire the success chain (raise `AlreadyCalledError` if already fired) |
| `addErrback(fn, *a, **kw)` | 27 | append a failure stage |
| `addCallbacks(cb, eb, ...)` | 15 | append paired success/failure stages |
| `errback(failure)` | 4 | fire the failure chain |
| `called` (property) | 4+ | read in `asterisk.py`/`sipp.py`; returns `_called` (review issue 2) |
| `AlreadyCalledError` | 4 | exception type used in guards |
| `addBoth` / `chainDeferred` | (lib) | provided for completeness |
| `cancel()` | 1 | cancel a pending Deferred |

Design notes:

- **Callback chaining semantics.** Twisted threads a result (or a `Failure`)
  through a linear chain, where each callback's return value becomes the next
  callback's input and a raised exception switches to the errback branch. We
  reproduce this with an internal result-passing chain (`_run_callbacks`) over the
  mutable `_chain_result`, branching on `_is_failure(...)`.
- **Awaiting a Future/Deferred returned by a callback.** When a callback returns
  another awaitable, Twisted pauses the chain until it settles. The shim pauses
  generically (review issue 4): if the returned value is *Deferred-like*
  (duck-typed via a callable `addBoth`) it pauses on it; if it is an
  `asyncio.Future`/coroutine/awaitable it is scheduled with
  `asyncio.ensure_future` and the chain resumes from its result (cancellation and
  exceptions mapped to `Failure`). This is what makes the suite and starpy shims
  interoperate without shared ancestry (§7).
- **Failure objects.** Twisted errbacks receive a `Failure` wrapping the
  exception. The `Failure` shim exposes the attributes actually used — `.value`,
  `.type`, `.getErrorMessage()`, `.getTraceback()`, `.check()` — backed by a
  stored exception and traceback. `getTraceback()` is required: it is used in
  `test_case.py`, `apptest.py`, `voicemail.py`, `pluggable_modules.py`,
  `confbridge.py`, and starpy (review issue 2). Cross-shim failure detection uses a
  class-level `_is_failure = True` marker so a `Failure` from either shim is
  recognized without a shared import.
- **`DeferredList(deferreds, consumeErrors=...)`** — returns a `Deferred` that
  fires with the Twisted-shaped result list `[(success_bool, result), ...]`.
  Implemented over `asyncio.gather(..., return_exceptions=True)` and reshaped.
  18 call sites, mostly start/stop aggregation in `test_case.py` and `sipp.py`;
  `consumeErrors` is honored. `gatherResults` and `succeed`/`fail` helpers are
  also provided.
- **`maybeDeferred(fn, *a, **kw)`** — runs `fn`; wraps a plain return value in an
  already-fired `Deferred`, passes a `Deferred`/awaitable (including a foreign
  Deferred-like) through. One site (the starpy fork's `InSequence`); provided here
  too for symmetry.

### 3.2 `reactor` — scheduling and transports on the asyncio loop

**Transitional scaffolding (per decision §1.1.6) — slated for removal in §14.**
This shim exists only to make the cutover a mechanical import swap; it is not the
target architecture. Each method is a thin wrapper over the running loop, so the
modernization pass can replace call sites with the native asyncio call inline and
then delete the shim. A module-level shim object exposing the reactor methods
actually used:

| Twisted reactor API | Sites | Implementation |
|---------------------|------:|----------------|
| `callLater(delay, fn, *a)` | 62 | `loop.call_later`; returns a handle object exposing `.cancel()` (and `.active()`) to match `DelayedCall` usage |
| `callWhenRunning(fn)` | 1 | run now if loop running, else `loop.call_soon`/startup hook |
| `callInThread(fn, *a)` | 3 | `loop.run_in_executor(None, ...)` |
| `listenUDP(port, proto)` | 11 | `loop.create_datagram_endpoint` (§3.3) |
| `listenTCP(port, factory)` | 7 | `loop.create_server` / `asyncio.start_server` (§3.3, §6) |
| `connectTCP(host, port, factory)` | 1 | `loop.create_connection` |
| `spawnProcess(proto, ...)` | 2 | `loop.subprocess_exec` (§4) |
| `run()` / `stop()` / `running` | 6 | own the loop lifecycle (§3.4) |

`callLater` returning a cancelable handle matters: `test_case.py` stores
`timeout_id` and `asterisk.py` keeps `_stop_cancel_tokens` and calls `.cancel()`.
The handle wraps `asyncio.TimerHandle`.

**Awaited listener readiness and bind-error propagation (review issue 6).**
Twisted callers treat `listenUDP`/`listenTCP` as established listeners by the time
the call returns, but asyncio's `create_server`/`create_datagram_endpoint` are
coroutines. A fire-and-forget background bind races Asterisk (or another client)
against an unready DNS/FastAGI/HTTP/UDP/WebSocket listener, and a failed bind gets
swallowed by an orphaned task so the test only fails later on an unrelated
timeout. The shim therefore separates *registration* from *readiness*:

- Before the loop runs, `listenUDP`/`listenTCP`/`spawnProcess` append their bind
  coroutine to a `_pending_binds` list rather than scheduling it loose.
- `reactor.run()` first drives an **awaited startup phase** that runs each pending
  bind to completion (`run_until_complete`). A bind that raises with no error
  callback records `_failure`, runs the ordered shutdown, and re-raises out of
  `run()` so the failure surfaces in the test result instead of timing out.
- Binds issued *after* the loop is already running are scheduled as tracked tasks;
  on failure they invoke the caller's `on_error` (e.g.
  `factory.clientConnectionFailed` for `connectTCP`, preserving starpy reconnect)
  or `_fatal` when none is supplied.

**Resource-owned, ordered shutdown (review issue 7).** The reactor keeps
registries — `_delayed_calls`, `_tasks`, `_ports`, `_connectors`,
`_process_transports` — populated as resources are created. `_shutdown()` is async
and ordered: cancel outstanding delayed calls, `stopListening` on servers/datagram
endpoints, disconnect connectors, terminate and close subprocess transports, then
cancel and `gather(..., return_exceptions=True)` remaining tasks. This prevents
inter-self-test resource leaks and the pending-task / unclosed-transport warnings
the prototype produced; the unit suite runs clean under
`-W error::ResourceWarning`.

### 3.3 Protocol/transport adapters

Twisted protocol base classes are reproduced as adapters so subclasses
(`AsteriskProtocol`, `SIPpProtocol`, the UDP handlers, etc.) keep their method
names:

- **`DatagramProtocol`** — a base class bridging asyncio's
  `datagram_received(data, addr)` → the suite's `datagramReceived(data, addr)`,
  and `connection_made(transport)` → `connectionMade`/`startProtocol`. The
  `self.transport.write(data, addr)` calls used by the suite map to asyncio's
  `transport.sendto(data, addr)`; the adapter's transport wrapper exposes `write`
  with that signature so call sites are unchanged. `transport.loseConnection()` →
  `transport.close()`. (20 `DatagramProtocol` subclasses; `datagramReceived`,
  `connectionMade`, `transport.write`, `transport.loseConnection` are the only
  surface used.)
- **`ProcessProtocol`** — see §4.
- **Line protocols** (`basic.LineOnlyReceiver`) live in the starpy fork and are
  handled there (§7).

### 3.4 Loop lifecycle (replacing reactor.run/stop)

`aio.run()` creates/gets the loop, runs the awaited startup phase (§3.2: drains
`_pending_binds`, re-raising any bind failure), flushes the `callWhenRunning`
hooks, then runs until a sentinel "main" Future is resolved. `reactor.stop()`
resolves that Future (idempotent, replacing the `ReactorNotRunning` guard with a
simple already-resolved check). `reactor.running` reflects loop state. On stop,
`run()` invokes the ordered async `_shutdown()` (§3.2) that drains the resource
registries, then re-raises a recorded `_failure` if one was set — so a fatal bind
or background-task error fails the test rather than exiting silently. This matches
and tightens the current teardown in `test_case.stop_reactor`.

## 4. Process control (`asterisk.py`, `sipp.py`)

Both modules subclass `protocol.ProcessProtocol` (`AsteriskProtocol`,
`SIPpProtocol`) and launch via `reactor.spawnProcess`. We provide a
`aio.ProcessProtocol` adapter over `asyncio.SubprocessProtocol`:

| Twisted ProcessProtocol | asyncio SubprocessProtocol | Notes |
|-------------------------|----------------------------|-------|
| `connectionMade` | `connection_made` | unchanged subclass method name |
| `outReceived(data)` | `pipe_data_received(1, data)` | adapter routes fd 1 |
| `errReceived(data)` | `pipe_data_received(2, data)` | adapter routes fd 2 |
| `processEnded(reason)` | `process_exited` *after* both pipes drain | adapter synthesizes a `reason` |

`processEnded` is the sensitive part, and the review (issue 1) flagged two traps:

- **Pipe-drain ordering.** asyncio may deliver `process_exited` *before* the final
  `pipe_data_received`/`pipe_connection_lost` callbacks. Firing `processEnded`
  there discards trailing Asterisk/SIPp output and signals completion too early.
  The adapter records process exit and stdout/stderr closure separately and calls
  `processEnded` only once **all three** conditions hold (`_maybe_end`). Pipes the
  child was never given are treated as already closed. `PipeDrainTests` proves
  200 000 bytes survive an early exit.
- **No reliance on `super().__init__`.** `AsteriskProtocol` and `SIPpProtocol` do
  not call `super().__init__`, so the adapter's lifecycle flags (`_proc_exited`,
  `_stdout_open`, `_stderr_open`, `_ended`, `transport`) are **class-level
  defaults** shadowed on first assignment — the base class needs no constructor.

The suite reads `reason.value.exitCode` and `reason.type == ProcessTerminated`.
The adapter constructs a `Failure`-shaped `reason` whose `.value.exitCode` is the
asyncio returncode and whose `.type` distinguishes clean exit (`ProcessDone`) from
signal/non-zero termination (`ProcessTerminated`), so `asterisk.py:66/254` and
`sipp.py:567` are unchanged.
`reactor.spawnProcess(proto, executable, args, env, path)` maps to
`loop.subprocess_exec(lambda: proto, *args, env=..., cwd=...)`; the returned
transport is stored where the suite keeps `self.process`. Process kill/stop
sequencing (currently `reactor.callLater` chains in `asterisk.py`) is unchanged
because `callLater` is preserved.

## 5. UDP networking (`pcap.py`, `pcap_proxy.py`, `matcher_listener.py`, RTP/HEP fixtures)

These are the most repetitive conversions and the §3.3 `DatagramProtocol` adapter
covers them directly. Per module:

- `reactor.listenUDP(port, proto)` → `aio.reactor.listenUDP` →
  `create_datagram_endpoint(lambda: proto, local_addr=('0.0.0.0', port))`.
- `proto.datagramReceived(data, addr)` — method name preserved by the adapter.
- `proto.transport.write(data, addr)` — preserved via the transport wrapper
  (`sendto`).
- `task.LoopingCall(fn, ...).start(interval)` (one site,
  `strict_rtp_seqno`) → a small `aio.LoopingCall` helper that schedules itself
  with `call_later`, exposing `.start()`/`.stop()`.

## 6. HTTP, DNS, WebSocket, SSH subsystems

### 6.1 WebSockets — `websockets` library (`ari.py`, `media_websocket.py`)

`ari.py` uses an autobahn `WebSocketClientFactory`/`WebSocketClientProtocol` with
`connectWS`, reconnect via `reactor.callLater`, and `onOpen`/`onMessage`/`onClose`
handlers. Redesign on `websockets`:

- Replace the factory/protocol pair with an async client task: a coroutine that
  `await websockets.connect(url, subprotocols=['ari'])`, then loops
  `async for message in ws:` dispatching to the existing event-handling code
  (`WebSocketEventModule`). The `onOpen` work (which schedules AMI connection via
  `reactor.callLater(0, ...)`) runs right after connect; `onClose`/reconnect
  becomes the loop's reconnect/backoff (preserving the current attempt counter and
  `reactor.callLater(1, reconnect)` timing).
- `media_websocket.py` and the `tests/channels/websocket/*` server fixtures use
  `websockets.serve` for the server side.
- This removes `autobahn` and `txaio` from `requirements.txt`.

**Async/thread boundary (review issue 5).** This is not a mechanical
factory/protocol swap. Autobahn exposes *synchronous* `sendMessage`/`sendClose`,
whereas `websockets` exposes *coroutine* send/close; a naive port produces
un-awaited coroutines. Worse, media code calls `sendFile` through
`reactor.callInThread`, and that worker thread then performs WebSocket sends —
unsafe cross-thread access to the event loop. The design rule: **file reads may
run in an executor, but every WebSocket operation is scheduled and awaited on the
event-loop thread.** Concretely, the existing synchronous `sendMessage` surface is
preserved by wrapping the coroutine send and marshalling it back onto the loop with
`loop.call_soon_threadsafe` / `run_coroutine_threadsafe` from any worker thread, so
callers keep their current (synchronous-looking) call shape. The port must also
preserve the behaviors the current tests depend on: intentional frame
fragmentation in
`tests/channels/websocket/inbound/basic-call/media_client.py`, subprotocol
selection, binary-vs-text framing, flow-control messages, reconnects, and close
callbacks. Each gets a focused parity test (§10).

### 6.2 HTTP servers — `aiohttp` (`http_static_server.py`, `realtime_test_module.py`)

- `http_static_server.py`: `twisted.web` `static.File` + `server.Site` on
  `listenTCP(8090)` → an `aiohttp.web.Application` serving a static route, started
  on the running loop.
- `realtime_test_module.py`: `twisted.web` `Resource`/`Site` with custom
  `render_*` handlers on `listenTCP(46821)` → `aiohttp.web` route handlers. The
  request/response bodies and status codes carry over directly.

### 6.3 DNS server — `dnslib` (`dns_server.py`)

`twisted.names` (`server`, `authority`, `dns`) provides an authoritative server
backed by zone data. Redesign: a `dnslib`-based responder served over an
`asyncio.DatagramProtocol` (UDP) and an asyncio TCP server (the current module
listens on both UDP and TCP). Zone/record configuration is reproduced from the
existing authority setup. This is the one subsystem with no near-drop-in
equivalent, so it gets a dedicated, test-backed implementation.

### 6.4 SSH — `asyncssh` (`asterisk.py` remote instances)

The `twisted.conch` path (`SSHCommandClientEndpoint.newConnection` over a
`UNIXClientEndpoint` to the SSH agent, `Key.fromFile`, `KnownHostsFile.fromPath`)
becomes `asyncssh.connect(...)` + `conn.run(command)`. Key loading and
known-hosts handling map onto asyncssh's `client_keys`/`known_hosts` options and
its built-in SSH-agent support. This path controls *remote* Asterisk instances and
is the least-exercised part of the core, so it is designed to preserve the small
public surface used by `asterisk.py` (connect, run a command, stream output,
disconnect) behind the same internal helper.

### 6.5 Parity contracts for DNS, HTTP, and SSH (review issue 10)

Naming a replacement library is not enough to review these rewrites safely. Each
replacement is held to an explicit behavioral contract, captured as focused tests
run against the **Twisted baseline first** (recorded before cutover) and then
against the new implementation:

- **DNS (`dnslib`)** — zone-file parsing rules, supported record types,
  authoritative-answer (`AA`) flag, `NXDOMAIN` vs `NODATA` distinction, UDP
  truncation (`TC` bit) with TCP fallback, TCP length-prefixed framing, and
  startup readiness (the server is bound and answering before Asterisk queries it,
  per §3.2's awaited startup).
- **HTTP (`aiohttp`)** — exact route matching, path-traversal protection for the
  static file root, query- and form-parameter parsing, response bodies, status
  codes, headers, and clean shutdown.
- **SSH (`asyncssh`)** — host-key policy, SSH-agent behavior, encrypted-key
  handling, password behavior, command quoting, stdout/stderr separation, exit
  status, timeout, and connection closure.

These contracts are the acceptance criteria for §6.2–§6.4 and feed directly into
the validation matrix (§10).

## 7. The starpy fork (`/usr/src/phzyx/starpy`)

starpy is converted in place to asyncio while **preserving its public API** so the
suite's call sites (`ami.py`, `test_case.py`, `pluggable_modules.py`) barely
change. It stays a self-contained package: rather than importing the suite's
`asterisk.aio`, starpy carries its own small internal async module
(`starpy._async`) providing the same `Deferred`/`maybeDeferred` shim and a line
protocol base.

**One interoperable boundary, not two ad-hoc ones (review issue 4).** Shared
`asyncio.Future` ancestry is *not* sufficient for callback-chain interop — the
prototype paused a chain only when a callback returned its own exact `Deferred`
class, so a starpy `Deferred`, a plain `Future`, a `Task`, or a coroutine returned
from a callback was mistaken for a finished plain value (and the reverse failed
inside starpy). The two shims are therefore built to one **interoperability
contract** rather than relying on inheritance:

- A callback returning any *Deferred-like* object (duck-typed: callable
  `addBoth`) pauses the chain on it, in either direction.
- A callback returning any `asyncio.Future`/coroutine/awaitable is scheduled and
  awaited.
- `Failure` is recognized cross-shim via the `_is_failure = True` class marker, so
  success/failure routing works regardless of which package created the failure.

`starpy._async` reuses the same `_is_failure`/`addBoth` conventions, and the test
suite explicitly chains Deferreds **in both directions** between starpy and the
suite (success, failure, and cancellation) — `CrossShimTests`.

### 7.1 `manager.py` (AMI client)

- `AMIProtocol(basic.LineOnlyReceiver)` → an `asyncio.Protocol` (or
  `StreamReader`-based reader) with a small line-buffering mixin reproducing
  `lineReceived(line)`. The AMI message parser above the line layer is unchanged.
- `AMIFactory(protocol.ReconnectingClientFactory)` → a connector class that calls
  `loop.create_connection(...)` and implements reconnect/backoff explicitly
  (Twisted's reconnecting factory behavior). `login(ip, port, timeout, ...)`
  returns a `Deferred` (Future) exactly as today, so `ami.py`'s
  `deferred.addCallbacks(self.on_login_success, self.on_login_error)` is unchanged.
- `deferredErrorResp(defer.Deferred)` and all the `sendDeferred(...)
  .addCallback(errorUnlessResponse)` action methods keep their signatures —
  internally they resolve via the new `Deferred` shim.
- `reactor.callLater` reconnect timing → the fork's own `call_later` helper.

### 7.2 `fastagi.py` (FastAGI server)

- `FastAGIProtocol(basic.LineOnlyReceiver)` → `asyncio.Protocol` line server with
  the same `lineReceived` parsing and `lostConnectionDeferred` (now a Future).
- `FastAGIFactory(protocol.Factory)` → an asyncio server factory used by
  `loop.create_server`; `test_case.create_fastagi_factory` /
  `pluggable_modules.FastAGIModule` keep calling it the same way, with the suite's
  `reactor.listenTCP(4573, factory)` shim doing the bind.
- `InSequence` uses `defer.maybeDeferred` and `reactor.callLater` — both provided
  by `starpy._async`.

### 7.3 Packaging

- `pyproject.toml`: drop `Twisted >= 24.10.0` from `dependencies` (likely leaving
  no runtime deps).
- The suite's `requirements.txt`: repoint the `starpy` entry from
  `git+https://github.com/asterisk/starpy@1.1.1` to the converted fork pinned at a
  **reviewed commit SHA** (not the moving `master-untwisted` branch) for
  reproducible environments (review issue 11).
- `error.py` and `__init__.py`: no functional change (drop "for Twisted" wording
  in docstrings only).

## 8. Cross-cutting swaps

| Twisted | Replacement | Sites |
|---------|-------------|-------|
| `twisted.python.log.PythonLoggingObserver` | nothing needed — log directly via stdlib `logging` (remove the observer install in `test_case.py`) | 1 |
| `twisted.python.failure.Failure` | `aio.Failure` shim (§3.1) | `asterisk.py` + adapters |
| `twisted.python.filepath.FilePath` | `pathlib.Path` | `asterisk.py` |
| `twisted.internet.error` (`ProcessTerminated`, `ReactorNotRunning`, etc.) | `aio` equivalents / asyncio + `OSError` family | `asterisk.py`, `sipp.py`, `test_case.py` |
| Vestigial `from twisted.internet import abstract, protocol` | delete | `pcap_listener.py` |

## 9. Dependency and environment changes

All package changes are confined to the suite's virtualenv via `setupVenv.sh` /
`requirements.txt` (and the fork's `pyproject.toml`), per the project constraint.

**Remove:** `Twisted`, `txaio`, `autobahn`, `Automat`, `constantly`, `hyperlink`,
`incremental`, `zope.interface` (and re-evaluate `service_identity`, `pyOpenSSL`,
`pycparser`, `cffi` — keep only if a remaining dependency needs them).

**Add:** `websockets`, `aiohttp`, `dnslib`, `asyncssh`.

**Keep:** `scapy`, `PyYAML`, `lxml`, `requests`, `numpy`, `netifaces`,
`construct`, `PyXB-X`, `rawsocket`, `six` (audit `six` usage — likely removable),
and the repointed `starpy` fork.

`setupVenv.sh`/`runInVenv.sh` logic is unchanged except for the regenerated
`requirements.txt` (the `md5sum` checksum gate picks up the new file
automatically).

## 10. Validation strategy

Because this is a hard cutover, parity testing is the safety net:

1. **Unit-test the compatibility layer first.** Beyond the basic `Deferred`
   chaining (result threading, errback switching, `DeferredList` shaping,
   `AlreadyCalledError` guards) and the `callLater` handle (`cancel`), the suite
   now pins the specific behaviors the review surfaced, each with a named test
   class: `LateCallbackAwaitTests` (await-after-settle returns the chain result,
   `called` property), `CrossShimTests` (bidirectional starpy↔suite chaining over
   foreign Deferred-like/Future/coroutine, success + failure), `HelperTests`
   (`succeed`/`fail`/`gatherResults`/`TimeoutError`), `BindFailureTests` (bind
   failure both pre-run and while running propagates, not swallowed),
   `PipeDrainTests` (no subprocess output lost on early exit), and
   `CallInThreadTests`. The suite runs clean under `-W error::ResourceWarning`.
2. **Capture Twisted baselines, then build parity tests for the rewritten
   subsystems** (review issues 5, 10). Before each of WebSocket, DNS, HTTP, and SSH
   is rewritten, record the Twisted behavior, then assert the new implementation
   against the §6.5 contracts — including WebSocket frame fragmentation,
   subprotocol selection, binary/text framing, reconnect, and close callbacks
   (§6.1), and the DNS/HTTP/SSH contract points (§6.5).
3. **Convert and exercise the starpy fork in isolation** (its own examples /
   a minimal AMI+FastAGI smoke test) before wiring it into the suite.
4. **Bring up the core** (`test_runner`, `test_case`, `asterisk.py`) and run the
   `self_test`/`lib/python/asterisk/self_test` harness.
5. **Run a representative cross-section** of the 1,097 tests covering each
   converted subsystem: an AMI test, a FastAGI test, an ARI/WebSocket test, a SIPp
   test, a UDP/RTP test, a DNS test, and an HTTP-server test — comparing
   pass/behavior against the Twisted baseline on `master` before the branch.
6. **Full-suite run** once the cross-section is green.
7. **Definition-of-done gate** (review issue 11): `doc/untwist/check_no_twisted.py`
   — an AST-based import check (not a literal grep, so the `asterisk.aio` layer's
   explanatory prose does not false-positive) — confirms no
   `twisted`/`txaio`/`autobahn` import remains in first-party code or the starpy
   fork, **and** asserts those packages are not importable in the venv. Paired with
   `pip check` for a consistent dependency set.

## 11. Risks and mitigations

- **Deferred semantics mismatch.** The callback-chain result threading is the
  highest-risk piece. Mitigation: implement and unit-test it first (§10.1) as a
  standalone, reviewed component.
- **Scheduling/ordering differences.** asyncio `call_soon`/`call_later` ordering
  and exception propagation differ subtly from the reactor. Mitigation: preserve
  exact delays via the `callLater` shim; rely on the cross-section parity runs to
  surface ordering-sensitive tests.
- **Shutdown cleanliness.** Twisted's reactor stop and asyncio loop teardown
  differ; lingering tasks/transports can hang a test process. Mitigation: the
  reactor owns resource registries and runs an ordered async `_shutdown()` on
  `stop()` (§3.2), verified clean under `-W error::ResourceWarning`.
- **Listener readiness / bind failures.** A background bind can race an unready
  listener or silently swallow a bind error. Mitigation: awaited startup phase with
  bind-failure propagation into the test result (§3.2), covered by
  `BindFailureTests`.
- **WebSocket async/thread boundary.** `websockets` is coroutine-based and media
  sends cross thread boundaries. Mitigation: the §6.1 rule (loop-thread sends via
  `run_coroutine_threadsafe`) plus fragmentation/subprotocol parity tests.
- **DNS, HTTP, and SSH parity.** These have no drop-in equivalent. Mitigation:
  hold each to the §6.5 behavioral contracts with baseline-comparison tests before
  rewrite; these are low-frequency code paths.
- **Big single step.** Hard cutover means the suite doesn't run until the core +
  starpy are done. Mitigation: the staged validation in §10 (layer → starpy →
  core → cross-section → full) keeps each step independently verifiable.

## 12. What the implementation document (Section 3) will sequence

1. Build and unit-test `asterisk.aio` (`defer`, `reactor`, adapters, `Failure`,
   `LoopingCall`).
2. Convert the starpy fork (`_async`, `manager.py`, `fastagi.py`); smoke-test;
   update `pyproject.toml`.
3. Convert the core (`test_runner.py`, `test_case.py`, `asterisk.py`) via import
   swaps + the process/SSH adapters; run `self_test`.
4. Convert AMI/ARI/SIPp/servers (`ami.py`, `ari.py`, `media_websocket.py`,
   `sipp.py`, `sipp_iterator.py`, `dns_server.py`, `http_static_server.py`,
   `realtime_test_module.py`, `pcap*.py`, `matcher_listener.py`,
   `pluggable_modules.py`).
5. Convert the `*_test_condition.py` family and sweep the 32 `tests/**` fixtures.
6. Update `requirements.txt`/`setupVenv.sh`; delete vestigial imports; run the
   AST import gate (`doc/untwist/check_no_twisted.py`) + `pip check` and the full
   suite.

## 13. Open items to confirm before implementation

- `service_identity`/`pyOpenSSL` retention — depends on whether `asyncssh`/TLS
  paths need them; confirm during the dependency rebuild.
- Confirm the DNS zone behavior currently relied on by `dns_server.py` tests so
  the `dnslib` responder reproduces it exactly.

(Two earlier open items are now settled: the compatibility package is named
**`asterisk.aio`**, and the `reactor`-shaped shim is a **transitional migration
aid only** — see §1.1 and §14.)

## 14. Modernization end state (post-cutover)

The drop-in shims are scaffolding chosen to make the *cutover* fast and
low-risk — not the destination. The goal is a codebase that reads as modern,
idiomatic asyncio, with the Twisted-shaped emulation gone. The work splits into
two clearly separated phases so each is independently reviewable and verifiable:

**Phase A — cutover (Sections 3–12).** Land the suite on the asyncio loop using
`asterisk.aio` (`reactor` shim + `defer` shim + adapters). Success criterion: the
full suite passes on asyncio with no Twisted import anywhere. At this point the
reactor shim still exists.

**Phase B — modernization (this section).** With a green suite as the safety net,
remove the scaffolding:

1. **Delete the `reactor` shim.** Inline each call to its native asyncio form at
   the call site:
   - `reactor.callLater(d, fn)` → `loop.call_later(d, fn)` (or an awaited
     `asyncio.sleep` where the surrounding code is already a coroutine).
   - `reactor.listenUDP/listenTCP/connectTCP` → direct
     `create_datagram_endpoint` / `create_server` / `create_connection`.
   - `reactor.spawnProcess` → `asyncio.create_subprocess_exec` /
     `loop.subprocess_exec`.
   - `reactor.callInThread` → `loop.run_in_executor`.
   - `reactor.run/stop` → the `asyncio.run(...)` / main-Future lifecycle, owned
     directly by `test_runner`/`test_case`.
   Once no module imports `asterisk.aio.reactor`, the shim module is removed.
2. **Migrate `Deferred` chains toward `async`/`await`.** The `defer` shim can
   persist longer than the reactor shim (it touches ~180 call sites), but the
   direction is to replace linear `addCallback`/`addErrback` chains with
   coroutines and `try/except`, and `DeferredList` with `asyncio.gather`, module
   by module. Because `aio.Deferred` is awaitable and pauses on any returned
   Future/awaitable (§3.1), a half-migrated module still composes correctly — a
   coroutine can await a Deferred and a Deferred chain can await a coroutine — so
   this proceeds incrementally without a second big-bang. The `defer` shim is
   deleted when the last chain is gone.
3. **Adapter retirement.** The `DatagramProtocol`/`ProcessProtocol` adapters can
   remain (they wrap native asyncio protocols and are already idiomatic), or the
   handful of subclasses can be rebased directly on `asyncio.DatagramProtocol`/
   `asyncio.SubprocessProtocol`. Low priority; cosmetic.

**End-state definition of done (beyond Phase A's AST import gate):** no
`asterisk.aio.reactor` references remain; `test_runner`/`test_case` own the loop
via `asyncio` directly; new code is written in `async`/`await` style; the `defer`
shim is either removed or reduced to a small, clearly-marked legacy helper with a
tracked path to removal.

Phase B is intentionally scheduled *after* a fully-passing Phase A so that
modernization is a series of safe, reviewable refactors against a working
baseline, rather than being entangled with the riskier protocol-level cutover.
