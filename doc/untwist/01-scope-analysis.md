# Removing Twisted from the Asterisk Test Suite — Section 1: Scope Analysis

Status: Draft for review
Branch: `master-untwisted`
Date: 2026-06-30

## 1. Purpose

The test suite was ported from Python 2 to Python 3 but retained its original
dependency on [Twisted](https://twisted.org/) for all asynchronous I/O and event
scheduling. Twisted runs its own reactor event loop, which does not cooperate
with Python 3's native `asyncio` loop. This blocks the suite (and any test
fixture) from using `asyncio`-based libraries and modern Python async features.

This document is the first of four planned deliverables:

1. **Scope analysis (this document)** — what uses Twisted, how deeply, and what
   it will take to remove.
2. Design document — the target architecture on `asyncio` and the replacement
   chosen for each Twisted facility.
3. Implementation document — concrete, ordered work items and migration
   sequencing.
4. Implementation and test.

The end state: **no remaining import of, or dependency on, `twisted` (or any
package that pulls Twisted in transitively).** This now explicitly includes the
project's `starpy` fork at `/usr/src/phzyx/starpy`, which is in scope as a source
change rather than a library swap (see §7.1).

## 2. Method

Findings are from a static survey of the `master-untwisted` working tree
(Python 3.12, 229 first-party `.py` files, 1,097 tests under `tests/`). Searches
covered direct `twisted` imports, the Twisted-coupled third-party libraries in
`requirements.txt`, and every distinct Twisted API call site. The installed
virtual environment was not present at survey time, so library internals were
reasoned about from import sites and the pinned versions in `requirements.txt` —
except `starpy`, whose internals were surveyed directly from the fork at
`/usr/src/phzyx/starpy` (branch `master-untwisted`).

## 3. Headline findings

Twisted is not a peripheral dependency — it is the **event-loop substrate of the
whole framework**. `test_runner.py` starts the test by calling `reactor.run()`,
and `test_case.py` (the base class nearly every one of the 1,097 tests inherits
from) drives startup, scheduling, timeouts, and shutdown entirely through the
reactor and `Deferred`s. Because the core is Twisted, the migration touches every
test indirectly even though most individual tests never import Twisted
themselves.

Direct first-party usage:

| Area | Files |
|------|-------|
| Core framework library (`lib/python`) | 30 |
| Individual test fixtures (`tests/**`) | 32 |
| **Total first-party files importing `twisted`** | **62** |

(32 `tests/**` fixtures *import* Twisted; 34 files mention the string "twisted",
the extra two only in comments. Counts regenerated via
`doc/untwist/check_no_twisted.py`, which parses imports via the AST rather than
grepping the literal string.)

Twisted-coupled third-party libraries that must also be changed: `starpy` (AMI +
FastAGI) — **now in scope as a source change**: the project owns a fork at
`/usr/src/phzyx/starpy` (branch `master-untwisted`), so starpy will be converted
to asyncio in place rather than swapped for another library; `autobahn.twisted`
(ARI/media WebSockets) and `txaio`, to be replaced or repointed; plus Twisted's
own transitive stack (`Automat`, `constantly`, `hyperlink`, `incremental`,
`zope.interface`, `service_identity`).

Two pieces of good news bound the risk:

- **No `inlineCallbacks`/`@defer.inlineCallbacks`/`returnValue` anywhere.** The
  code uses explicit `Deferred` + `addCallback/addErrback` chains, which map
  cleanly onto `asyncio.Future`/`async def`. There is no generator-coroutine
  rewrite to untangle.
- At least one Twisted import is already **vestigial** (`pcap_listener.py`
  imports `twisted.internet.abstract, protocol` but uses neither — it runs on
  scapy's `AsyncSniffer`). Some files will be deletions, not rewrites.

## 4. Twisted API surface in use

Distinct call sites across first-party code (excluding the venv):

| Twisted facility | Approx. sites | Where |
|------------------|--------------:|-------|
| `reactor.callLater` | 62 | pervasive — timeouts, deferred kickoffs |
| `defer.Deferred` | 35 | result plumbing across framework + tests |
| `defer.DeferredList` | 20 | start/stop aggregation (test_case, sipp) |
| `DatagramProtocol` + `reactor.listenUDP` | 20 / 11 | RTP, HEP, pcap_proxy, matcher, DNS |
| `protocol.ProcessProtocol` + `reactor.spawnProcess` | 11 / 2 | Asterisk + SIPp process control |
| `reactor.listenTCP` | 7 | FastAGI, web servers, websocket servers |
| `reactor.run` / `reactor.stop` / `reactor.running` | 6 | lifecycle in test_runner / test_case |
| `reactor.callInThread` / `threads.*` | 4 | background blocking work |
| `task.LoopingCall` | 1 | periodic timer |
| `reactor.connectTCP` | 1 | pjsip keep_alive test |
| `twisted.web` (`Site`, `Resource`, `static`) | 2 | http_static_server, realtime_test_module |
| `twisted.names` (DNS server) | 1 | dns_server.py |
| `twisted.conch` (SSH) + `UNIXClientEndpoint` | 1 | asterisk.py remote-instance control |
| `twisted.python.log` / `failure.Failure` / `filepath.FilePath` | 3 | logging bridge, error handling, paths |

Notably **absent** (reduces complexity): no `inlineCallbacks`, no
`gatherResults`, no `maybeDeferred`, no Twisted SSL/TLS context usage in
first-party code, no Perspective Broker, no Twisted application/plugin system.

## 5. The core framework (highest-risk tier)

These modules in `lib/python/asterisk` are load-bearing; every test depends on
them transitively.

**`test_runner.py`** — the entry point. Its async footprint is tiny but
absolute: `reactor.run()` is the single blocking call that powers a test run.
This becomes the `asyncio` loop bootstrap (`asyncio.run(...)` /
`loop.run_forever()`).

**`test_case.py` (1,031 lines)** — the base `TestCase`. Uses
`reactor.callWhenRunning` to schedule `_run`, `reactor.callLater` for the global
test timeout and end-of-test delays, `defer.DeferredList` to coordinate starting
and stopping multiple Asterisk instances, `defer.Deferred` for the stop sequence,
`reactor.listenTCP(4573, ...)` to stand up the FastAGI server (via the starpy
fork, converted in place — see §7.1), and
`reactor.stop()` (guarded by `twisted_error.ReactorNotRunning`). It also installs
`twisted.python.log.PythonLoggingObserver`. This file is the linchpin of the
migration; its public surface (the callbacks and `Deferred`-returning methods
that test fixtures override and chain onto) defines the compatibility contract
the rest of the suite is written against.

**`asterisk.py` (1,238 lines)** — controls Asterisk processes via
`protocol.ProcessProtocol` (`AsteriskProtocol`) and `reactor.spawnProcess`, with
`reactor.callLater` orchestrating start/wait-fully-booted/stop sequencing. For
*remote* Asterisk instances it uses `twisted.conch` SSH
(`SSHCommandClientEndpoint`, `Key`, `KnownHostsFile`) over a `UNIXClientEndpoint`
to the SSH agent. Two distinct replacements needed: local subprocess control and
an SSH client.

**`ami.py` (861 lines)** — Asterisk Manager Interface client built on
`starpy.manager.AMIFactory`, returning `Deferred`s (`login`, `addCallbacks`,
reconnect via `reactor.callLater`). Starpy is Twisted-native, but the project
owns the fork, so AMI is handled by **converting starpy in place** to asyncio
(see §7.1) rather than introducing a new client. If the fork preserves its public
API, `ami.py`'s own changes are limited to the residual `reactor.callLater`
reconnect timing and how it consumes the (now asyncio) results.

**`ari.py` (952 lines)** — ARI REST + event WebSocket using
`autobahn.twisted.websocket` (`WebSocketClientFactory/Protocol`, `connectWS`) and
`reactor.callLater` for reconnect/timeout. The `media_websocket.py` module uses
the same autobahn-on-Twisted stack.

**`sipp.py` (1,041 lines) / `sipp_iterator.py`** — run SIPp scenarios via
`ProcessProtocol` (`SIPpProtocol`), `reactor.spawnProcess`, and coordinate fleets
of them with `defer.Deferred` + `defer.DeferredList`.

**Networking/server modules** — `pcap.py`, `pcap_proxy.py`, `matcher_listener.py`
(UDP `DatagramProtocol` + `listenUDP`); `dns_server.py` (`twisted.names`
authoritative DNS server); `http_static_server.py` and `realtime_test_module.py`
(`twisted.web` `Site`/`Resource`); `pluggable_modules.py` (FastAGI server via the
converted starpy fork — see §7.1).

**`test_conditions.py` and the `*_test_condition.py` family** — use only
`defer.Deferred`/`DeferredList` to aggregate async checks. These are mechanical
conversions to `asyncio.Future`/`gather` with no protocol or transport concerns.

## 6. Test-fixture tier (32 files)

Per-test Python fixtures under `tests/**`. They fall into a few repeating
patterns, which is favorable — converting the patterns once gives reusable
recipes:

- **UDP listeners** (RTP/HEP/strict-rtp): `DatagramProtocol` + `listenUDP` — e.g.
  `tests/rtp/strict_rtp/...`, `tests/channels/pjsip/rtp/...`, `tests/hep/...`.
- **WebSocket media endpoints**: autobahn-on-Twisted servers/clients under
  `tests/channels/websocket/...`.
- **One-shot timers / orchestration**: `reactor.callLater` to stop media or end a
  test (e.g. `tests/codecs/audio_analyzer.py`, the `tests/rest_api/recording/*`
  fixtures).
- **TCP client**: `reactor.connectTCP` in `tests/channels/pjsip/keep_alive`.
- **AMI/AGI-driven fixtures**: several `tests/manager/*` and `tests/pbx/*`
  fixtures that lean on the framework's AMI layer.

Because these import the framework's now-`asyncio` base classes, most fixture
changes are small and follow templates established when the core is converted.

## 7. Third-party dependency impact

`requirements.txt` is dominated by the Twisted ecosystem. Each must be removed,
replaced, or repointed:

| Package | Role today | Disposition |
|---------|-----------|-------------|
| `Twisted==25.5.0` | reactor, protocols, web, names, conch | **Remove** |
| `starpy @ 1.1.1` | AMI client + FastAGI server (Twisted) | **Modify the fork** — convert `/usr/src/phzyx/starpy` to asyncio; repoint `requirements.txt` at the fork; drop its Twisted dependency |
| `autobahn==21.2.1` | ARI/media WebSockets (`autobahn.twisted`) | **Replace or repoint** — `autobahn.asyncio` exists, or move to `websockets`/`aiohttp` |
| `txaio==22.2.1` | Twisted/asyncio compat shim (pulled by autobahn) | **Remove** once autobahn is gone |
| `Automat`, `constantly`, `hyperlink`, `incremental`, `zope.interface` | Twisted transitive deps | **Remove** with Twisted |
| `service_identity`, `pyOpenSSL` | TLS identity (Twisted-adjacent) | **Re-evaluate** — keep only if a replacement lib needs them |
| `scapy==2.6.1` | packet capture (`pcap_listener.py`) | **Keep** — already asyncio-independent |
| `PyYAML`, `lxml`, `requests`, `numpy`, `netifaces`, `construct`, `PyXB-X`, `rawsocket` | non-Twisted | **Keep** |

AMI and FastAGI are no longer a "pick a replacement library" question — the
forked starpy is converted directly (see §7.1). Candidate replacement libraries
to evaluate in the design phase for the remaining gaps (not yet decided):
`asyncssh` (for conch/SSH), `websockets`/`aiohttp`/`autobahn.asyncio`
(WebSockets), `dnslib` + asyncio datagram endpoint or `aiodns`-style serving
(DNS), `aiohttp` or `http.server`-in-executor (static/realtime web). Any package
change is to be made **only** in the suite's virtualenv via `setupVenv.sh` /
`requirements.txt`, per the project constraint.

### 7.1 Converting the starpy fork (`/usr/src/phzyx/starpy`)

Because the fork can be edited, starpy moves from a *replacement* problem to a
*port* problem — and the testsuite's AMI/FastAGI call sites (`ami.py`,
`test_case.py`, `pluggable_modules.py`) can be kept stable if the fork preserves
its public API shape. This is the preferred path: it confines the protocol work
to one library and minimizes churn in the suite.

Fork layout (branch `master-untwisted`, forked from `asterisk/starpy`):

| File | Lines | Role |
|------|------:|------|
| `starpy/manager.py` | 1,143 | AMI client |
| `starpy/fastagi.py` | 998 | FastAGI server |
| `starpy/error.py` | 41 | exception classes — no Twisted, no change |
| `starpy/__init__.py` | 13 | docstring only |

Twisted surface inside the fork and its asyncio targets:

| starpy uses | Where | asyncio target |
|-------------|-------|----------------|
| `basic.LineOnlyReceiver` (line protocol) | `AMIProtocol`, `FastAGIProtocol` | `asyncio.Protocol` with line buffering, or `StreamReader`/`StreamWriter` |
| `protocol.ReconnectingClientFactory` | `AMIFactory` (AMI client + reconnect) | `loop.create_connection` + explicit reconnect/backoff |
| `protocol.Factory` | `FastAGIFactory` (server) | `loop.create_server` |
| `defer.Deferred` (incl. `deferredErrorResp` subclass) | both modules | `asyncio.Future` (+ a small subclass for `registerError`) |
| `defer.maybeDeferred` | `fastagi.InSequence` | coroutine wrapper / `asyncio.ensure_future` |
| `reactor.callLater` | `fastagi.wait`, AMI reconnect | `loop.call_later` |
| `twisted.internet.error` (`tw_error`) | connection errors | `OSError`/`ConnectionError` family |

Favorable factors: **no `inlineCallbacks` generator coroutines** in the fork (the
`returnValue` at `fastagi.py:395` is a local callback, not
`twisted.internet.defer.returnValue`); the only `maybeDeferred` is a single,
contained call site; `error.py` is pure Python and untouched.

Public API to preserve (so the suite's call sites barely change): `manager.AMIFactory(user, secret)`
with `.login(host, port)` returning an awaitable; the `AMIProtocol` action methods
(`sendDeferred`, `collectDeferred`, `registerEvent`/`deregisterEvent`, and the
many `*.addCallback(errorUnlessResponse)` actions); and `fastagi.FastAGIFactory`
with its connect callback. The design phase decides whether the preserved surface
returns `asyncio.Future`s (drop-in for the existing `addCallbacks` chains via a
thin shim) or is exposed as `async def` (cleaner, but requires updating the
suite's chained callbacks). `pyproject.toml` must drop `Twisted >= 24.10.0` from
its `dependencies`, and `requirements.txt` in the suite repoints from
`git+...asterisk/starpy@1.1.1` to the converted fork.

## 8. Twisted → asyncio mapping (preliminary)

A first-pass equivalence table to be refined in the design document:

| Twisted | asyncio / stdlib equivalent |
|---------|------------------------------|
| `reactor.run()` / `reactor.stop()` | `asyncio.run()` / `loop.run_forever()` + `loop.stop()` |
| `reactor.callLater(t, f)` | `loop.call_later(t, f)` (returns cancelable handle) |
| `reactor.callWhenRunning(f)` | `loop.call_soon(f)` / schedule before `run` |
| `reactor.callInThread` / `deferToThread` | `loop.run_in_executor(None, ...)` |
| `task.LoopingCall` | `asyncio.create_task` around a sleep loop, or a helper |
| `defer.Deferred` | `asyncio.Future` / `async def` coroutine |
| `defer.DeferredList` | `asyncio.gather(...)` |
| `addCallback` / `addErrback` | `await` + `try/except`, or `Future.add_done_callback` |
| `DatagramProtocol` + `listenUDP` | `asyncio.DatagramProtocol` + `loop.create_datagram_endpoint` |
| `ProcessProtocol` + `spawnProcess` | `asyncio.SubprocessProtocol` + `loop.subprocess_exec`, or `create_subprocess_exec` |
| `reactor.listenTCP` | `loop.create_server` / `asyncio.start_server` |
| `reactor.connectTCP` | `loop.create_connection` / `asyncio.open_connection` |
| `twisted.web` `Site`/`Resource` | `aiohttp` server (or stdlib `http.server` in executor) |
| `twisted.names` DNS server | `dnslib` responder over an asyncio datagram/stream endpoint |
| `autobahn.twisted.websocket` | `autobahn.asyncio.websocket` or `websockets`/`aiohttp` |
| `starpy.manager` (AMI) | forked starpy converted to asyncio (see §7.1) |
| `starpy.fastagi` (FastAGI) | forked starpy converted to asyncio (see §7.1) |
| `twisted.conch` SSH | `asyncssh` |
| `twisted.python.log.PythonLoggingObserver` | direct stdlib `logging` (no bridge needed) |
| `twisted.python.failure.Failure` | standard exceptions + `traceback` |
| `twisted.python.filepath.FilePath` | `pathlib.Path` |

## 9. Risk and complexity assessment

**Highest risk / highest effort**

- **`test_case.py` + `test_runner.py` event-loop conversion.** Everything else
  depends on the lifecycle contract these define. The `Deferred`-returning
  methods that fixtures override must keep working (or be migrated in lockstep).
- **Converting the `starpy` fork (AMI + FastAGI).** This is a ~2,100-line
  protocol implementation, central (`ami.py`, `test_case.py`,
  `pluggable_modules.py`) and exercised by a large share of tests. Owning the fork
  *lowers* risk versus adopting a different library (no behavioral re-validation
  against an unfamiliar AMI/AGI implementation, and the public API can be held
  stable to shield the suite), but it remains substantial protocol work — line
  framing, the reconnecting AMI client, and the FastAGI server all move off
  Twisted transports. The biggest design decision is whether the converted public
  surface returns `Future`s (drop-in) or `async def` coroutines (cleaner).
- **`asterisk.py` process + SSH control.** Subprocess lifecycle is the backbone
  of every test; the SSH/remote path adds a second, less-exercised code path
  that still must be ported (`asyncssh`).

**Medium**

- ARI/media WebSockets (autobahn): a supported asyncio path exists, but
  reconnect/timeout logic and the factory/protocol structure need rework.
- SIPp process orchestration (`sipp.py`/`sipp_iterator.py`): large but follows
  the same subprocess pattern as `asterisk.py`.
- DNS server (`twisted.names` has no direct asyncio analog — needs a library +
  hand-rolled responder) and the `twisted.web` servers.

**Low / mechanical**

- The `*_test_condition.py` family and most `tests/**` fixtures: `Deferred` →
  `Future`/`gather`, `DatagramProtocol`/`listenUDP` → asyncio datagram endpoints,
  `callLater` → `loop.call_later`. Repetitive, template-able.
- Vestigial imports (e.g. `pcap_listener.py`): delete.
- `log`/`failure`/`filepath` swaps: trivial.

**Cross-cutting risks**

- **Behavioral parity of scheduling.** Twisted and asyncio differ in
  call-ordering, error propagation, and shutdown semantics. Tests that rely on
  subtle reactor timing may surface flakiness.
- **Big-bang vs. incremental.** Because the core is Twisted, there is no clean way
  to run half-migrated: the reactor and the asyncio loop cannot both own the
  process. A bridging strategy (or a hard cutover of the core, then fixtures) must
  be chosen in the design phase.
- **Test coverage of the harness itself.** `lib/python/asterisk/self_test/`
  provides some harness self-tests; their breadth (and whether they can validate
  parity) needs assessment before cutover.

## 10. Recommended migration ordering (for the design/implementation phases)

1. **Establish the asyncio core.** Convert `test_runner.py` and `test_case.py` to
   own an `asyncio` loop; define the new contract for fixture callbacks. Provide a
   thin compatibility layer if a phased cutover is chosen.
2. **Port process control** (`asterisk.py`, then `sipp.py`/`sipp_iterator.py`) to
   asyncio subprocesses; add `asyncssh` for remote instances.
3. **Convert the starpy fork to asyncio** (`/usr/src/phzyx/starpy`:
   `manager.py`, `fastagi.py`), preserving its public API; then adapt the suite's
   AMI/FastAGI call sites (`ami.py`, `test_case.py`, `pluggable_modules.py`) and
   repoint `requirements.txt` at the fork.
4. **Replace WebSocket stacks** (`ari.py`, `media_websocket.py`).
5. **Port the standalone servers** (`dns_server.py`, `http_static_server.py`,
   `realtime_test_module.py`) and UDP modules (`pcap*.py`, `matcher_listener.py`).
6. **Convert the `*_test_condition.py` family** and sweep the 32 `tests/**`
   fixtures using the established templates.
7. **Strip dependencies** from `requirements.txt` and `setupVenv.sh`; verify no
   `twisted`/`txaio`/`autobahn.twisted` import remains anywhere — including in the
   starpy fork (`starpy` itself stays, but converted to asyncio with no Twisted).
8. **Validate** against the self-test harness and a representative cross-section
   of the 1,097 tests.

## 11. Definition of done

- No first-party module imports `twisted`, `txaio`, or `autobahn.twisted`.
- The forked `starpy` imports no `twisted` and declares no Twisted dependency in
  its `pyproject.toml`; the suite imports `starpy` from the converted fork.
- `requirements.txt` contains no Twisted-ecosystem package (the only `starpy`
  entry points at the asyncio fork); the venv built by `setupVenv.sh` installs no
  Twisted.
- `doc/untwist/check_no_twisted.py` — an **AST-based import gate**, not a literal
  grep — finds no `twisted`/`txaio`/`autobahn` *import* in first-party code **or
  the starpy fork** (the `asterisk/aio/` layer is excluded; it may mention Twisted
  only in prose), **and** confirms none of those packages are importable in the
  venv. Paired with `pip check`. A literal grep is unusable here because it
  false-positives on the compat layer's explanatory docstrings.
- The suite runs on the native `asyncio` loop and a representative test set passes
  with behavior equivalent to the Twisted baseline.

## 12. Open questions for the design phase

- AMI/FastAGI (starpy fork): should the converted public API return
  `asyncio.Future`s (drop-in for the suite's existing `addCallbacks` chains) or be
  exposed as `async def` coroutines (cleaner, but requires rewriting the suite's
  callback chains)? This choice sets how much `ami.py`/`test_case.py` change.
- WebSockets: stay on autobahn (asyncio flavor) to minimize churn, or move to
  `websockets`/`aiohttp` for a lighter dependency tree?
- Cutover strategy: hard cutover of the core, or a temporary asyncio/Twisted
  bridge to allow incremental fixture migration?
- DNS: which library best reproduces the authoritative-server behavior currently
  built on `twisted.names`?
- Scope of parity testing required before the Twisted dependency is deleted.
