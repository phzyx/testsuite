# Removing Twisted from the Asterisk Test Suite — Section 5: Phase 2 Implementation (Modernization)

Status: Draft for review (revised after Codex review, 2026-07-04)
Branch: `master-untwisted` (suite) / `master-untwisted` (starpy fork)
Date: 2026-07-04
Companion to: `doc/untwist/01-scope-analysis.md`, `doc/untwist/02-design.md`, `doc/untwist/03-implementation.md`, `doc/untwist/04-potential-issues.md`

## 1. Purpose and how to use this document

This is the executable plan for **Phase B** — the modernization pass described in
design §14. Phase A (Sections 3–12 of the plan, tracked in `03-implementation.md`)
is complete in the sense that matters for starting Phase B: the suite runs on the
asyncio event loop through the `asterisk.aio` shim, there is **no Twisted import or
dependency anywhere** (AST gate + `pip check` green, venv stripped in Step 6).

Phase B removes the *scaffolding* — definitely the `reactor` shim, and (per a B0
scope decision) the `defer` shim — so the codebase reads as idiomatic asyncio
(design §1.1 decision 6). The **`reactor` shim removal is unconditional.** The
**`defer` shim is a B0 scope decision, exactly like starpy** (§4.5): either
**(D1) remove it in this phase** — the default and preferred outcome — or
**(D2) explicitly retain it** as a small, tracked legacy helper deferred to a named
follow-up phase, with the DoD weakened to match. §1's opening scope, §9 (B5), and
§13 (DoD) all read off that single recorded decision; they no longer contradict each
other. The **protocol adapters are likewise a conditional end state**: the
`_TwistedProtocolAdapter` (TCP) and the `Datagram`/`Process` adapters in
`protocols.py` may be *retained in a clearly-named compat module* if their consumers
are not all natively converted (see §11 and the DoD in §13). Every step is a
**behavior-preserving refactor validated against a fixed baseline**; a Phase B step
that changes suite behavior is a bug.

### 1.1 Phase A is NOT "all green" — it has a fixed, accepted baseline

An earlier draft asserted the full suite passes. It does not, and the plan must not
pretend otherwise. The recorded full run (`/usr/src/phzyx/full-ts-run.txt`,
JUnit summary) is:

```
tests=977  failures=30  errors=0  skipped=122
```

These 30 failures are **pre-existing test-quality problems unrelated to the Twisted
removal** (predominantly the `bridge`/attended-transfer family, `hep/pjsip*`,
`sorcery` memory-cache-expire, and a handful of others — see B0 for the full
classified list). They fail the same way on the Twisted baseline and are out of
scope for this migration. Phase B is therefore measured against this **exact
accepted result set**, not against an imaginary all-green suite: the definition of
"no regression" is *no test that passes in the accepted baseline starts failing, and
no new skip appears*. B0 makes this baseline machine-readable so comparison is
mechanical rather than eyeballed.

Ground rules (unchanged from Phase A):

- All package changes go through `requirements.txt` / `setupVenv.sh` and the fork's
  `pyproject.toml` only. If the starpy workstream (§10) is taken, its pinned commit
  in `requirements.txt` is updated; otherwise Phase B adds/removes no dependency.
- Nothing is pushed to GitHub; merging is manual.
- Keep the pristine pre-branch Twisted `master` available for parity comparison.
- Each module conversion is one small, locally-committed, reviewable unit so a
  regression bisects to a single module.

## 2. Current state (grounded inventory)

An earlier draft undercounted the work by scanning only `*.py` files, which **misses
the 146 extensionless `run-test` entrypoint scripts** — each of which imports the
shim and calls `reactor.run()`. Corrected figures (whole tree, `lib/` + `tests/`,
including `run-test` scripts; "production" excludes the shim's own unit tests in
`lib/python/asterisk/aio/test_aio.py`):

**Scale.** ~196 source files import the reactor shim. Of these, **146 are
extensionless `run-test` entrypoint scripts** that each end in `reactor.run()`; they
are not `.py` and must be swept as their own workstream (§9).

**Reactor call-site counts** (whole tree):

| API | count | notes |
|-----|-------|-------|
| `callLater` | ~132 | largest surface; timeouts + deferred kicks |
| `listenTCP` | ~32 | incl. shim tests; fastAGI, WebSocket, misc listeners |
| `listenUDP` | ~16 | RTP/HEP/DNS fixtures + core |
| `spawnProcess` | 11 | Asterisk CLI + SIPp (production: `asterisk.py`, `sipp.py`) |
| `callWhenRunning` | ~29 total | library production: `asterisk.py:167`, `test_case.py:189`; **plus legacy `run-test` callers** that queue their kickoff this way — all handled in the B2 entrypoint sweep, not just the two library sites |
| `callInThread` | 5 total | production: WebSocket media send-file helpers under `tests/channels/websocket/*` (`media_server.py`, `media_client.py`) — the `media_websocket.py` hit is a docstring |
| `connectTCP` | few | production: `tests/.../keep_alive` (reconnect via `ReconnectingClientFactory`) |
| `addStartupBind` / `addAsyncCleanup` | 2 / 2 | aiohttp server bind + `AppRunner.cleanup` |
| `run` | `test_runner.py`, `pcap.py`, **146 `run-test` scripts** | loop-lifecycle owners |
| `stop` | `test_case.py`, `pcap.py`, some fixtures | ends the run |

**Correction to the shim API list.** `addSystemEventTrigger` and `getDelayedCalls`
**do not exist** on the shim (an earlier draft claimed they were defined-but-unused).
`callFromThread` *is* defined but has no production caller. The real methods in use
are `run`, `stop`, `callWhenRunning`, `callLater`, `callInThread`, `listenUDP`,
`listenTCP`, `connectTCP`, `spawnProcess`, `addStartupBind`, `addAsyncCleanup`, the
`reactor.running` flag, and the `_DelayedCall` handle API (`cancel`, `active`,
`reset`, `getTime`). **`_DelayedCall.delay()` (reactor.py:128) has *no* caller —
not in production and not even in `test_aio.py`.** The timer helper in B3.1 should
**drop `delay()`** (the default), or retain it only with an accompanying unit test;
`reset()` and `getTime()` are the ones `test_case.reset_timeout()` actually uses.

### 2.1 Behaviors the shim provides that a naive inline would break

These are the load-bearing details a mechanical `reactor.X → loop.Y` substitution
would drop. Each drives a concrete contract in the steps below.

- **`listenUDP` binds the socket *synchronously*** and installs a usable
  `_SyncDatagramTransport` on `protocol.transport` before returning
  (`reactor.py:618`), because fixtures transmit on the very next line (e.g.
  `tests/rtp/strict_rtp/strict_rtp_yes/strict_rtp.py:88`). A bind failure surfaces
  synchronously (Twisted `CannotListenError` parity). A plain
  `await create_datagram_endpoint(...)` is **not** an equivalent conversion.
- **`listenTCP` wraps the factory in `_TwistedProtocolAdapter`** (`reactor.py:668`):
  the existing factories are Twisted-shaped (`buildProtocol()`), not callable
  asyncio protocol factories. `create_server(factory, ...)` fails against them.
- **`connectTCP` returns a `_Connector`** supporting `ReconnectingClientFactory`
  retry/backoff and `connectionLost` notification. Reconnect is behavior, not
  cosmetics.
- **`spawnProcess`** uses Twisted arg convention: `args[0]` is the program name, so
  it forwards `executable, *args[1:]` (`reactor.py:698`). It installs a
  `_PendingProcessTransport` placeholder so a signal/kill issued *before*
  `connection_made` (a SIPp scenario killed from an AMI event) is buffered and
  replayed. It honors `cwd=path` and `env`, drains pipes, delivers exit
  notification, and its teardown escalates **TERM → wait 1s → KILL → wait 1s** using
  the pidfd non-reaping path that avoids the fabricated-255 double-reap (Phase A task
  #62). A bare `create_subprocess_exec(exe, *args)` duplicates `argv[0]` and loses
  all of the above.
- **`_DelayedCall` is richer than `TimerHandle`**: `getTime()` and `reset()` are both
  used by `test_case.reset_timeout()` (`test_case.py:668`); `TimerHandle` has
  neither. **`getTime()` must return a wall-clock Unix timestamp**, because
  `reset_timeout()` feeds it to `datetime.fromtimestamp()` — a bare monotonic
  `loop.time()` deadline is the wrong domain and would produce garbage dates. The
  helper must therefore track **both** domains: the monotonic deadline used to
  (re)schedule via `loop.call_later`, and the corresponding wall-clock fire time
  returned by `getTime()` (compute one from the other at schedule time, or store the
  `time.time()`-based fire instant alongside). Keyword args need `functools.partial`.
  **Timer-callback exceptions today reach asyncio's default loop exception handler**,
  *not* the shim's `_fatal` path (verify against `reactor.py` before coding). The
  replacement must **preserve that behavior** — let the callback exception escape
  naturally so asyncio dispatches it to the loop's installed handler, or call
  `loop.call_exception_handler({...})` explicitly. (Do **not** write
  `loop.set_exception_handler` for this: that *installs* a handler, it does not
  dispatch an exception.) Making timer-callback failures fatal instead would be an
  intentional behavior change, called out as such with its own replacement test.
- **Ordered `_shutdown()`** (`reactor.py:464`) runs in a specific sequence:
  **(1) cancel timers → (2) await async cleanups (aiohttp `AppRunner.cleanup`, while
  the loop is still healthy) → (3) `stopListening` ports + `disconnect` connectors →
  (4) child processes: TERM, gather-wait 1s, KILL survivors, gather-wait 1s, then
  close transports (non-reaping path) → (5) cancel + gather tracked tasks →
  (6) cancel + gather stray (non-registry) tasks → (7) `sleep(0)` for close
  callbacks.** An earlier draft put async cleanup last and omitted stray-task
  cancellation; both are wrong.

### 2.2 Defer shim surface (~180 sites)

| API | lib | tests |
|-----|-----|-------|
| `addCallback` | 104 | 16 |
| `addErrback` | 28 | 8 |
| `addCallbacks` | 16 | 6 |
| `addBoth` | 6 | 0 |
| `Deferred(...)` construction | 64 | 3 |
| `DeferredList` | 28 | 5 |
| `maybeDeferred` | 15 | 0 |
| `gatherResults` | 6 | 0 |
| `inlineCallbacks` / `returnValue` | 0 | 0 |

No `inlineCallbacks` usage (every chain is explicit). Highest density: `sipp.py`
(14), `test_case.py` (12), `apptest.py` (7), `sip_dialog_test_condition.py` (6),
`pluggable_modules.py` (5), then the `*_test_condition.py` tail.

### 2.3 starpy still has its own parallel shim

The starpy fork is **not** covered by removing `asterisk.aio`. `starpy/starpy/_async.py`
is a self-contained Twisted emulation — `Deferred` (139), `Protocol`/`LineOnlyReceiver`
(425/451), `Factory`/`ClientFactory`/`ReconnectingClientFactory` (500/521/531),
`_TwistedProtocolAdapter` (742), `_Reactor` (802) with `callLater` (923), and
`reactor = _Reactor()` (1016). Both `starpy/starpy/manager.py:26` and
`starpy/starpy/fastagi.py:28` consume it (`from starpy._async import protocol,
reactor, defer, basic`). Phase B must make an explicit decision about this shim
(§10); the end-state DoD depends on that decision.

### 2.4 Adapters

`lib/python/asterisk/aio/protocols.py` provides the `DatagramProtocol`/`ProcessProtocol`
adapters (4 `ProcessProtocol` subclasses: `asterisk.AsteriskProtocol`,
`sipp.SIPpProtocol`, 2 in tests; 12 `DatagramProtocol` subclasses across
`matcher_listener`, `pcap_proxy`, `dns_server`, and RTP/HEP/keep-alive test
fixtures). The TCP path additionally depends on `_TwistedProtocolAdapter`, which
currently lives **inside `reactor.py`** — so deleting `reactor.py` (B1f) and adapter
handling (B3) are coupled, not independent (see §7.3, §11).

## 3. Implementation order (the critical path)

The earlier ordering was doubly wrong. First it inlined `await`/loop calls *before*
introducing native loop ownership, but today test objects and pluggable modules are
constructed **before** `reactor.run()` in `test_runner.py:298`, and those constructors
schedule timers and bind listeners — you cannot `await` or call `get_running_loop()`
there until a loop owns that construction. Second it converted per-family resource
contracts (timers/UDP/TCP/subprocess) *before* the 146 legacy `run-test` entrypoints
had been moved onto the native runtime, which would force every common-library change
to support **two incompatible lifecycle models at once** (legacy synchronous
`reactor.run()` owners and the new native owner). The corrected order puts runtime
ownership first, then migrates **all** entrypoints onto it, and only then touches the
per-resource contracts:

```
Step B0  Baseline + machine-readable manifest + starpy A/B decision (no runtime changes)
Step B1  Single AsyncTestRuntime (one registry set/completion/fatal/shutdown) + TRANSITIONAL BRIDGE (run_async delegates) + async module lifecycle/loader
Step B2  Migrate ALL entrypoints (test_runner + 146 run-test) onto the native runtime helper
Step B3  Per-family conversion contracts (timer, UDP, TCP, subprocess, thread)
Step B4  Delete reactor.py (incl. the transitional bridge); relocate/delete adapter; symbol-aware gate
Step B5  Deferred: classify, then migrate to async/await (module by module)
Step B6  starpy modernization (implements the B0 decision)
Step B7  Adapter retirement (bounded by the B4 relocation decision)
Step B8  Re-run the moved-earlier replacement coverage + end-state gate + full-suite parity + docs
```

Rationale: establish an async runtime that owns object/module construction **and a
transitional `run_async()` bridge** so legacy consumers keep working during the
migration (B1); move every entrypoint — including the 146 `run-test` scripts — onto
that runtime so only **one** lifecycle model exists before any resource contract
changes (B2); define the tricky per-resource contracts once and apply them (B3); only
then delete the shim (and the now-unused bridge) behind a symbol-aware gate (B4).
Deferred (B5) and starpy (B6, deciding in B0) are larger, independently-committable
workstreams that rest on the now-native core. **Replacement tests are written in the
step that changes each behavior (B1–B3), not deferred to B8** (see §12).

## 4. Step B0 — Baseline and machine-readable manifest (no runtime changes)

*("No runtime changes" — not "no code changes": B0 adds a committed manifest/baseline
script. It touches no product/runtime code.)*

1. **Freeze an accepted Phase A baseline.** From `/usr/src/phzyx/full-ts-run.txt`,
   emit a committed JSON/CSV of every testcase and its status (977 rows), and a
   `known_failures` list of the 30 failing cases with a one-line classification each
   (why it is a pre-existing test-quality issue, confirmed against the Twisted
   baseline). Phase B's "no regression" check diffs a fresh run against this file:
   the only allowed differences are the 30 known failures; any *new* failure or skip
   fails the check. The 30 (by `classname :: name`) are the `bridge` atxfer/blindxfer/
   parkcall/automixmon/simple_bridge/transfer_* family (17), `cdr*` nocdr &
   console_dial_sip_transfer (2), `channels.pjsip` dtmf_sdp_recognition /
   non_negotiated_frame_SSRC_change / registration…contact_acl ipv6 (3), `hep`
   pjsip/pjsip_auth/pjsip_ipv6 (3), a `masquerade` case, `redirecting` forwardername,
   `rest_api.channels.redirect` nominal, and `sorcery` memory_cache_expire ×2.
2. **Build an AST-based reactor/defer manifest.** A committed script under
   `doc/untwist/` that parses **both `.py` files and the 146 extensionless `run-test`
   scripts** (read + `ast.parse`, keyed off the `#!` / known filenames, not the
   extension) and emits per-file counts of every `reactor.*` attribute reference and
   every `defer`/`Deferred` construct. This is the single source of truth for scope,
   ordering, and the "zero remaining" end state — replacing the hand counts in §2.
   It must resolve **both the dotted and the re-exported symbol forms**: for the
   reactor, `asterisk.aio.reactor` **and** `from asterisk.aio import reactor`; for
   defer, `asterisk.aio.defer` **and** `from asterisk.aio import defer, Deferred,
   DeferredList, maybeDeferred, gatherResults` (and any other re-exported name — see
   §8/§9), so a `from asterisk.aio import Deferred` is not missed. It must also emit an
   explicit **lifecycle-owner list** — every file (not just `test_runner` and the 146
   `run-test` scripts) that references `reactor.run`/`stop`/`running`/`callWhenRunning`,
   so per-test helper modules that own the loop are surfaced. Two must appear on that
   list up front: `tests/rest_api/applications/stasisstatus/test_case.py` (calls
   `reactor.run()` in `__init__`, §6) and the scripts that call `reactor.stop()`/read
   `reactor.running` directly (§6).
3. **Confirm gate + venv clean** (`check_no_twisted.py`, `pip check`).
4. **Record the starpy A/B decision now** (details in §10). The choice is an
   *architectural input to B2–B5*, not a late add-on: option A (modernize starpy)
   changes the TCP and Deferred conversion strategy and requires re-pinning the fork
   SHA before final parity; option B (scope starpy out) requires **retaining TCP
   compatibility for FastAGI** and narrows the gate scope in §8. Deciding it in B0
   prevents redoing earlier architecture. The decision is recorded here as an exit
   criterion even though the starpy *implementation* (if A) still lands in B6.
5. **Record the `defer`-shim D1/D2 decision now** (§1). Default **D1 — remove the
   `defer` shim in B5**; choose **D2 — retain it as a tracked legacy helper** only if
   the Deferred surface (~180 sites) can't be fully migrated within this phase. This
   is the analogue of the starpy decision and removes the §1↔§9↔§13 contradiction: the
   opening scope, B5's phase-level exit, and the DoD all read this one flag.

**Exit:** committed baseline JSON + `known_failures`; committed AST manifest whose
counts are reproduced by a second run; gate + `pip check` green; **starpy decision
A or B, and `defer` decision D1 or D2, both written down (§10, §1) with their
consequences for B2/B3/B5/B8 noted.**

## 5. Step B1 — Native runtime owner and async module lifecycle

This is the foundational, and most delicate, step. It replaces the shim's
`run()`/`stop()` with a native lifecycle, **moves test-object/module construction
inside it**, and — critically — ships a **transitional bridge** so the migration can
be incremental instead of a single atomic cutover.

0. **One runtime owns everything: `AsyncTestRuntime`.** There must **never** be a
   separate set of "reactor registries" and "test-object registries" — if there were,
   any still-unconverted shim call during B1–B3 (a `callLater`, `listenTCP`,
   `spawnProcess`, `create_task`, `addAsyncCleanup`) would register into a set the
   native shutdown never walks, and its resource would **escape teardown**; likewise a
   fatal error stored in one place would be invisible to the code that must re-raise it.
   B1 therefore defines a single `AsyncTestRuntime` (the concrete owner; the reactor
   shim becomes a thin facade over it) that owns, as the **one** authoritative set:
   - the **completion signal** and the **fatal error**;
   - **every** resource registry — timers/`_delayed_calls`, tasks, ports, connectors,
     process transports, and async cleanups (these are exactly the registries
     `reactor.py:378-383` already holds — the runtime *is* that owner, not a second
     copy);
   - the **ordered shutdown** (the §2.1 seven-phase `_shutdown`).

   During `run_async()`, the shim does **not** keep private registries: every
   registration and every fatal error is **delegated into the same runtime instance**
   that `_main` will shut down. Blocking `run()` (for still-unmigrated scripts, point 5)
   **obtains or creates** and drives the **same** runtime contract (it reuses the
   lazily-created per-run runtime if an earlier pre-run op made one, else creates it —
   point 0's legacy path). This is the invariant the rest
   of §5 builds on; points 2–4 below describe how it is wired.

   **The runtime must exist before *any* constructor runs.** Test-object and module
   constructors already call `reactor.callLater()`/`listenTCP()`/`spawnProcess()` from
   `__init__`, so those registrations must land in a live runtime — creating it *after*
   construction would drop pre-construction resources on the floor. Therefore:
   - **Native path:** `_main` **creates and installs the `AsyncTestRuntime` immediately
     on entry, before any test-object or module construction** — the shim points at it,
     then construction/loading proceeds.
   - **Legacy path:** the shim **lazily creates the per-run runtime on the first pre-run
     shim operation** — *any* of them: `callLater`, `listenTCP`, `spawnProcess`,
     `addStartupBind`, **or `callWhenRunning`** (kickoff-only scripts register nothing
     else, so `callWhenRunning` must be a creation trigger too) — so resources and
     kickoffs registered during construction are captured. Blocking `run()` then
     **drives that already-created runtime** and does **not** replace it; **if no earlier
     operation created one (a script that goes straight to `run()`), `run()` creates the
     runtime itself** before starting. Either way there is exactly one per-run instance.
   - **Reset between runs:** after shutdown, the runtime is **detached/reset** so a
     sequential run in the same process gets fresh, loop-bound state (new completion
     future, empty registries, cleared `_failure`) rather than reusing an instance bound
     to a closed loop.

   **Explicit runtime state machine — registration keys off *state*, not the public
   `reactor.running` boolean.** Today `_register_bind` (`reactor.py:739`) decides
   startup-queue-vs-fire-and-forget by reading `self.running`: when `running` is true a
   bind becomes a background task; when false it enters the awaited `_pending_binds`
   queue. That boolean is too coarse for the transitional bridge — if the run is marked
   "running" before `start()` hooks have finished, a `listenTCP`/`spawnProcess`/
   `addStartupBind` issued from a constructor or a still-transitional `start()` becomes
   **fire-and-forget, and kickoff can fire before its bind completes**. B1 replaces the
   boolean gate with an explicit state:
   1. **`COLLECTING`** — construct the test object and all modules; shim binds enter the
      awaited **startup queue**.
   2. **`STARTING`** — invoke module `start()` hooks; binds they issue **still enter the
      awaited startup queue**.
   3. **drain** the startup queue **until quiescent** — startup work may enqueue more
      startup work (a `start()` that binds a port whose factory schedules another bind),
      so loop until no new startup binds remain.
   4. **`RUNNING`** — mid-run.
   5. **flush `callWhenRunning`** kickoffs.
   6. **await completion.**
   7. **`STOPPING`** — ordered shutdown (point 4).

   **Two independent concerns, deliberately decoupled — do not collapse them onto one
   flag:**
   - **Bind classification is state-based:** `_register_bind` (and the `callWhenRunning`
     gate) consult **`state in (COLLECTING, STARTING)` → startup queue; `state ==
     RUNNING` → mid-run task**. This is what closes the fire-and-forget race — a bind
     from a `start()` hook still queues.
   - **`reactor.running` tracks "is a stop meaningful," and must be TRUE during
     `STARTING` as well as `RUNNING`** (false only in `COLLECTING` and `STOPPING`).
     `test_case.stop_reactor()` checks `reactor.running` before calling `stop()`; a
     timeout, signal, process-exit callback, or startup failure can request shutdown
     **while awaited startup is still running** (`STARTING`). If `running` were false
     during `STARTING`, that stop request would see false and **silently do nothing**,
     hanging the run. So `running` goes true when `STARTING` begins and returns to false
     only at `STOPPING`. (Today's shim already sets `running = True` before the awaited
     startup phase, `reactor.py:406`; this preserves that stop semantics while keeping
     bind classification on the finer-grained state.) A `stop()` observed during
     `STARTING` resolves completion so the drain/`await` unwinds into the ordered
     shutdown.

1. **Introduce `asyncio.run(_main(test_directory))`** in `test_runner.main()` (today
   ending at `reactor.run()`, `test_runner.py:310`). `_main` is an `async def` that
   owns the loop for the whole test, including construction:
   - **First, create and install the single `AsyncTestRuntime`** (point 0) so
     `reactor.*` registrations from constructors land in it.
   - Then load config and **create the test object *inside* `_main`**; **module
     construction/loading is driven by the runtime's `start_all()`** (point 2b), moved
     from the current pre-`run()` position at `test_runner.py:298` — so every constructor
     runs with a running loop **and a live runtime** available.
   - **`_main` runs runtime cleanup in its `finally` even if construction or module
     loading raises** — a constructor that registered a port/timer/process before a
     later constructor threw must still be torn down; the ordered shutdown (point 4)
     runs regardless of where the failure occurred, and the error re-raises after it.

2. **Ship a transitional `reactor.run_async()` bridge — the linchpin of
   incrementality.** Simply switching `test_runner` to `asyncio.run()` **breaks the
   intermediate state**: modules still call the shim, so `callWhenRunning()` queues
   callbacks (because `reactor.running` is still false), `listenTCP`/`spawnProcess`/
   startup binds pile up in `_pending_binds`, and nothing ever calls the shim's
   blocking `reactor.run()` — so those queues never flush and the test stalls. The fix
   is an `async def run_async(self)` on the shim that **adopts the already-running
   loop** instead of starting its own, and **delegates into the single
   `AsyncTestRuntime`** (point 0) rather than keeping its own state. **It does NOT mark
   `running = True` and does NOT call `start_all()`** — startup has exactly one driver,
   `_main` (point 2b). By the time `_main` awaits `run_async()`, the runtime's state
   machine has already run `COLLECTING → STARTING → drain → RUNNING → flush kickoff`, so
   `run_async()` simply **awaits the runtime's completion signal and relays stop**. Every shim
   registration it services — `_delayed_calls`, ports, connectors, process transports,
   tasks, async cleanups — lands in the **runtime's** registries (the same ones `_main`
   will tear down), and any fatal error it hits is stored on the **runtime's**
   `_failure`, not a private field. **`run_async()` does NOT perform shutdown** —
   shutdown has exactly one owner, the runtime's ordered `_shutdown` invoked from
   `_main`'s `finally` (point 4). The bridge only *adopts the loop, awaits the runtime's
   completion signal, and relays the stop signal*; when the completion signal fires it
   returns, and `_main`'s `finally` runs the single ordered teardown over the one
   registry set. (This avoids two shutdown owners **and** two
   registry owners: no duplicate module closes, no double process signals, no double
   transport cleanup, and no resource that escapes teardown because it was filed in the
   "wrong" registry.) `_main` `await`s `reactor.run_async()` inside its `try`.
   This lets B3 migrate **resource consumers** (a `listenTCP`/`callLater`/`spawnProcess`
   call *inside* an already-native entrypoint) off the shim one at a time: their still
   queued startup binds keep working because **`start_all()` drains the startup queue**
   (point 2b) before kickoff, and mid-run binds register directly as tasks — the bridge
   itself only awaits completion and relays stop, it does **not** flush binds.
   (Distinct from un-migrated *entrypoint scripts* in B2, which are driven by the shim's
   still-present **blocking** `run()`, not the bridge — see B2 and point 5.) The bridge
   is **deleted in B4** with the rest of `reactor.py`. (The alternative — converting
   every queued operation atomically in B1 — contradicts the incremental plan and is
   rejected.)

2a. **Completion primitive, startup ordering, and fatal-error propagation** — three
   details the runtime must get exactly right:
   - **Completion signal — thread-safe and per-run.** The runtime's completion primitive
     is **created fresh at the start of each run** (as `run()` already does with
     `_stop_future = loop.create_future()`, `reactor.py:408`), so sequential runs never
     reuse a signal bound to a closed loop. `reactor.stop()`/runtime stop must resolve it
     via **`loop.call_soon_threadsafe(...)`** — it is called from `test_case`/signal
     contexts that may not be on the loop thread, and neither `Future.set_result` nor
     `asyncio.Event.set` is thread-safe. (This matches `stop()` at `reactor.py:446-462`,
     which already wraps the resolve in `call_soon_threadsafe`; the runtime preserves that
     mechanism rather than substituting a bare `Event.set()`.) Resolving is idempotent
     (no-op if already done / not running), matching Phase A's no-op-when-stopped `stop()`.
   - **Startup ordering is fixed: pending binds *before* kickoff.** The runtime must
     reproduce the existing sequence exactly (`reactor.run:410-431`): **(1)** construct
     all local **and** global modules and **retain** every instance; **(2)** finish the
     awaited startup phase — legacy `_pending_binds` **and** native `start()` operations —
     surfacing bind failures; **(3)** *only then* flush the `callWhenRunning` kickoff
     queue. Existing behavior always completes pending binds before kickoff; do not
     reorder these or interleave kickoff with binds.
   - **Fatal errors propagate through `_main`, not merely complete.** A shim fatal error
     (stored on the runtime's `_failure`, `reactor.py:376`/`_fatal:760`) or a
     startup-bind failure **surfaced by `start_all()`** (which now owns the awaited
     startup phase, point 2b — not `run_async()`) must **re-raise out of `_main`
     after** its `finally` shutdown — exactly as blocking `run()` re-raises `_failure`
     after `_shutdown` (`reactor.py:441-444`) and re-raises a startup-bind failure after
     tearing down (`reactor.py:416-424`). Triggering completion alone is insufficient:
     `_main` stores the failure, runs the ordered teardown in `finally`, then re-raises
     so the harness sees a non-zero result. The runtime exposes the stored failure to
     `_main` for this re-raise.

2b. **One owner, one caller, one call sequence: `runtime.start_all()` driven by
   `_main`.** Module `start()` must be driven from exactly **one** place, or a module can
   be started twice. The plan **mandates** (not "prefers") the single caller and the exact
   sequence — there is no second valid implementation:

   - **`_main` is the sole caller of `start_all()`.** `run_async()` does **not** call it
     (point 2 — the bridge only awaits completion + relays stop). Blocking `run()`
     (legacy path) calls the **same** `start_all()` on the same runtime; it is the
     legacy path's sole caller.
   - `start_all()` runs the point-0 state machine in this fixed order, and **the
     runtime state — not `reactor.running` — gates every bind** throughout:
     1. state **`COLLECTING`**: construct + retain all local/global modules (strong
        refs); their constructor binds enter the awaited startup queue.
     2. state **`STARTING`** (**set `reactor.running = True` here** — a stop requested
        during awaited startup must be honored, point 0): invoke each retained module's
        `start()` in registration order (bind failures propagate → startup-failure
        teardown, point 4); binds they issue **still enter the awaited startup queue**
        (classification stays on state, so `running` being true here does NOT make them
        fire-and-forget).
     3. **drain the startup queue until quiescent** (startup work may enqueue more
        startup work — loop until no new binds).
     4. set state **`RUNNING`** (`reactor.running` already true; bind classification now
        flips to mid-run task).
     5. **flush the `callWhenRunning` kickoff** queue.
     6. return to `_main`, which **awaits the completion signal** (via `run_async()`).

   - **A stop requested during `STARTING` must actually abort startup — resolving the
     completion future is not enough on its own.** `start_all()` is the coroutine
     running steps 2–5; simply firing the completion primitive from a stop callback does
     **not** interrupt it, so without explicit checks it would keep invoking `start()`
     hooks, drain binds, transition to `RUNNING`, and flush kickoff — launching the test
     that the stop was meant to prevent — before `_main` ever observes completion. So
     `start_all()` must cooperatively honor stop:
       - check the runtime's `stop_requested` flag **between every `start()` hook and
         between every drained bind**; on observing it, **skip all remaining startup and
         the kickoff flush** and fall straight through to ordered shutdown (point 4)
         **without launching the test**;
       - for a **potentially long** individual startup operation (a bind/connect that
         can block), **race it against the completion signal** (e.g.
         `asyncio.wait({start_task, stop_wait}, return_when=FIRST_COMPLETED)`); if stop
         wins, **cancel and drain the in-flight startup task** (await it so no coroutine
         is left unawaited) before proceeding to shutdown;
       - transition COLLECTING/STARTING → `STOPPING` directly, never through `RUNNING`,
         when stop wins the race.
   - **Never queue bare coroutine *objects* on the startup queue — queue coroutine
     *factories* (or already-scheduled, owned tasks).** If construction fails after a
     bind coroutine was created, or one bind fails while later binds are still queued,
     bare coroutine objects left on the queue are never awaited and emit
     "coroutine was never awaited" `RuntimeWarning`s. The seven resource phases (point 4)
     cover ports/tasks/timers but **not the startup queue itself**, so shutdown must
     **explicitly cancel/close and drain every remaining startup awaitable** (or, with
     factories, simply discard the unused factories — nothing was created). Prefer
     factories so the un-run entries are inert.

   - The mandated `_main` body is therefore exactly:
     `create/obtain runtime (COLLECTING)` → `construct test object` →
     `await runtime.start_all()` (steps 1–5 above) → `await reactor.run_async()`
     (awaits completion, relays stop) → `finally: runtime shutdown (STOPPING)` →
     `re-raise stored failure`.
   - `start_all()` is still **guarded to run its body once per run** (a stray second
     call after `RUNNING` is a no-op; a call observed mid-`STARTING` awaits the in-flight
     startup task rather than racing ahead), but with the single-caller mandate this
     guard is a safety net, not a load-bearing coordination mechanism.

3. **Define an async pluggable-module lifecycle contract *and the loader mechanics*.**
   Because construction now happens in a coroutine, give modules an explicit lifecycle
   instead of "do-work-in-`__init__`":
   - `async def start(self)` — bind listeners, spawn processes, schedule timers
     (replaces work previously done in `__init__` + `addStartupBind`). Optional: a
     module without `start()` is treated as already-started.
   - `async def close(self)` — release resources (replaces `addAsyncCleanup`).
   - a resource-registration API **on the `AsyncTestRuntime`** (timers, ports,
     connectors, process transports, async cleanups, **and background tasks**) so
     shutdown is centrally ordered over the one registry set (point 0).

   `load_test_modules()` today constructs module objects and **discards them**; B1 must
   change it to a real loader with these explicit mechanics:
   - **Retain** the constructed module instances (strong references) for the run's
     lifetime — not garbage after construction.
   - **Detect and `await` an optional `start()`** on each retained module, in
     registration order; a bind failure in `start()` propagates (awaited-startup
     semantics) and triggers startup-failure cleanup (point 4).
   - **`close()`-tracking rule (simple and enforceable).** The loader cannot know
     whether a failed `start()` acquired resources partially, so do not try to guess:
       - a module **without** `start()` is considered started;
       - **once `start()` is invoked — including a failed start — `close()` is always
         called** for that module;
       - therefore `close()` must **tolerate partial initialization and be idempotent**
         (guard each resource release; safe to call after a failed/partial `start()`).
     Modules whose `start()` was never invoked (because an earlier module already
     failed) are not closed.
   - **Preserve close ordering = registration (forward) order.** The existing cleanup
     registry runs **forward** (`reactor._shutdown` iterates `_async_cleanups` in
     registration order); this migration is behavior-preserving, so keep forward order.
     Do **not** switch to LIFO here — that is a separate behavior change and would need
     its own coverage proving every dependency stays valid; it is explicitly out of
     scope for Phase B.
   - **Support resources created *after* startup** — e.g. a listener or timer spun up
     from an AMI/event callback mid-run, not only during `start()`. The registration
     API must accept late registrations so they are still centrally torn down.
   - **Preserve strong references to background tasks** (`asyncio.create_task` results)
     via the registry, so they are neither GC'd early nor leaked at shutdown.

   `_main` drives startup **only** through the runtime's single `start_all()` (point
   2b) — it does not await module `start()` directly — then awaits the completion
   signal, then invokes `runtime._shutdown()` in a `finally`.
4. **Port the ordered shutdown verbatim** (the §2.1 seven-phase sequence) into the
   **runtime's** `_shutdown` — the single owner of the one registry set (point 0) —
   preserving order exactly: timers → async `close()`s →
   ports/connectors → process TERM/wait/KILL/close (non-reaping) → tracked tasks →
   stray tasks → `sleep(0)`. Also port **startup-failure cleanup** (a module that
   fails `start()` must trigger the same ordered teardown of already-started
   resources) and **fatal-error precedence** (a stored mid-run fatal error is
   re-raised after shutdown).
   - **The runtime owns the shutdown *implementation* (`runtime._shutdown()`); it does
     not own the sole *invocation site*.** Two entrypoints call it, one per path, each
     from its **own** `finally`:
       - **Native path:** `_main` (point 1) calls `runtime._shutdown()` in its `finally`
         after awaiting completion.
       - **Legacy path:** the shim's blocking `reactor.run()` — still driving the 146
         unmigrated `run-test` scripts through B1 — calls `runtime._shutdown()` in **its
         own** `finally`. `_main` does not exist on that path, so it cannot be the
         shutdown caller there; if `run()` did not invoke `_shutdown()` itself, legacy
         scripts would tear down nothing. `_shutdown()` is **idempotent** so the two
         sites never double-run for a single execution (only one path drives any given
         process).
   - **Behavior during `STOPPING`/`STOPPED` (and after detachment) is explicitly
     defined — the registries stop accepting normal late registrations once shutdown
     begins.** Mid-run late registration (point 3) is legitimate only while `RUNNING`;
     a callback firing *during* teardown must not slip a resource past the
     snapshot-based cleanup or, worse, queue work that survives into the next sequential
     run. So once state is `STOPPING`/`STOPPED`:
       - a newly-created **task** is immediately cancelled and drained (not left to run);
       - a newly-opened **port/transport** is closed immediately;
       - a newly-scheduled **timer** or **`callWhenRunning`** callback is rejected or
         cancelled, never fired;
       - an **async cleanup** registered this late is either executed inline as part of
         the current teardown or rejected — never deferred;
       - **no callback may queue work for the next run** — the runtime is reset/detached
         between runs (point 0), and shutdown-time registrations must not repopulate the
         fresh registries.
   - **Owned-task exception policy (no silent swallow, no bare orphan).** Every task the
     runtime owns must have its exception *retrieved* (so Python emits no "task exception
     was never retrieved" warning), but retrieval must not erase a real error:
       - a task flagged **fatal** stores the **first** failure on the runtime's
         `_failure` and stops the runtime (drives completion → ordered shutdown →
         re-raise through `_main`, point 2a);
       - a **non-fatal** task's exception is reported to
         **`loop.call_exception_handler({...})`** (dispatched, not installed — cf. §7.1),
         preserving visibility while keeping task hygiene.
5. **One shared kickoff/stop API — do NOT rip reactor wiring out of `test_case` in
   B1.** `test_case` is a *library* consumed by both the new `_main` path **and** the
   146 unmigrated `run-test` scripts, which still call the shim's **blocking**
   `reactor.run()` (not `run_async()`). If B1 rewrites `test_case` to bypass the
   reactor, those scripts lose their kickoff/stop and hang. So B1 introduces **a single
   runtime stop/kickoff API that both `run()` and `run_async()` honor**, and
   `test_case`/`asterisk.py` keep calling it unchanged during B1:
   - `reactor.stop()` and `reactor.running` continue to work, now backed by the
     runtime's **per-run completion primitive** resolved via `call_soon_threadsafe`
     (point 2a; idempotent, matching Phase A's no-op-when-stopped `stop()`); the
     `test_case.py:532` guard reads `reactor.running`. Both blocking `run()` and
     `run_async()` wait on the *same* runtime completion signal and fire the *same*
     kickoff queue. **Shutdown, however, is not symmetric:** blocking `run()` owns its
     own teardown (`runtime._shutdown()` in its `finally`, point 4), whereas
     `run_async()` is a thin bridge over an already-running loop that only awaits
     completion and relays `stop()` — it does **not** call `start_all()` and does
     **not** run shutdown; the native path's shutdown is driven by `_main`'s `finally`.
   - `callWhenRunning` stays functional through B1 (flushed by whichever of
     `run()`/`run_async()` is driving), so `test_case`'s and the scripts' kickoffs both
     fire.
   The reactor references in `test_case`/`asterisk.py` are **removed in B2**, once every
   entrypoint (including the 146 scripts) has been migrated onto the native runtime and
   nothing drives the blocking `run()` any more. **The "no reactor references in
   `test_case`" exit criterion therefore belongs to B2, not B1.**
6. **`pcap.py`** is a standalone tool with its own `reactor.run()`/`stop()`; give it
   an independent `asyncio.run(_pcap_main())` wrapper.

The pseudocode names used in review discussion (`startup_binds`, `kickoff`,
`finished`, `fatal_error`, `async_shutdown`) are **illustrative** — none exist yet;
this step creates the real runtime owner and its API.

**Exit:** `runtests.py` drives a single- and a multi-Asterisk test end-to-end through
`asyncio.run` with `start_all()` draining the startup queue and the `run_async()` bridge
awaiting completion; teardown
clean under `-W error::ResourceWarning`; the Phase A pjsip SIPp set (task #62) still
passes; `test_runner.main()` no longer calls the blocking `reactor.run()`. **A
still-unmigrated `run-test` script (blocking `reactor.run()`) must also still pass**,
proving the shared kickoff/stop API works for both paths. (The `test_case`/`asterisk.py`
reactor references are removed later, in B2 — see point 5.) **Replacement tests for the behaviors this step introduces
are written here** (loader lifecycle: start/close ordering, **close after `start()`
is invoked including failed starts**, late-registered resources, background-task
retention; **single-registry ownership — an unconverted shim `callLater`/`listenTCP`/
`spawnProcess` mid-run is torn down by the runtime shutdown, proving no split
registry**; **fatal-error re-raise through `_main` after teardown**; **sequential
runtime reuse — two runs in one process each get fresh loop-bound state (new
completion future, empty registries, cleared `_failure`) and both pass**; **cross-thread
stop — a `reactor.stop()` issued from a non-loop thread resolves completion via
`call_soon_threadsafe` and unblocks the run**; **startup race — a bind (`listenTCP`/
`spawnProcess`/`addStartupBind`) issued from inside a module's `start()` is drained by
the state machine *before* `callWhenRunning` kickoff fires, i.e. it never becomes
fire-and-forget**; single- and multi-Asterisk ordered teardown; stray-task cancellation
— items 8–9 and the loader cases of §12), not deferred to B8.

## 6. Step B2 — Migrate ALL entrypoints onto the native runtime

Before touching any per-family resource contract, move **every** entrypoint onto the
B1 runtime so only one lifecycle model exists. This is `test_runner` plus the **146
extensionless `run-test` scripts**, each of which imports the shim and calls
`reactor.run()` (and some queue their kickoff via `callWhenRunning`) — **and any
per-test helper module that owns the loop**, such as
`tests/rest_api/applications/stasisstatus/test_case.py`, which calls `reactor.run()`
inside a constructor (points 4–5 below). Doing this after
the per-family conversions would force the common library to support two incompatible
lifecycles at once (legacy synchronous `reactor.run()` owners *and* the native owner).

**The helper takes a factory, not a constructed object.** Because construction must now
happen *inside* the running loop (B1), the entrypoint helper cannot accept an
already-built test object. The minimal contract is:

```python
test = run_test_object(lambda: MyTest(...))   # constructs inside the loop, runs, tears down
if not test.passed or my_extra_check(test):   # returns the object AFTER shutdown
    ...
```

`run_test_object(factory)` wraps `asyncio.run(_main(...))`, constructs the test object
by calling `factory()` under the running loop, drives the B1 lifecycle
(start → run via the `run_async()` bridge → ordered shutdown), and **returns the test
object after shutdown** so scripts can keep custom post-run assertions beyond
`test.passed`.

**The bare-factory form does not fit every script — classify all 146 before assuming
it.** A large share of scripts do work *around* `reactor.run()` that the factory form
drops on the floor. At least **57** explicitly call `test.start_asterisk()` **before**
`reactor.run()` and `test.stop_asterisk()` **after** (e.g. `tests/udptl/run-test:52-57`).
Returning the object after `asyncio.run()` is **insufficient** for these: `asyncio.run()`
has already **closed the loop** before the script's post-run lines execute, so a
`stop_asterisk()` (or any post-run step needing the loop) would run against a dead loop.
So **B0/B2 must classify every one of the 146 scripts** into:

- **constructor-only** (bare `run_test_object(lambda: ...)` fits);
- **pre-run setup** (work before `reactor.run()`, e.g. `start_asterisk()`);
- **post-run cleanup** (work after `reactor.run()`, e.g. `stop_asterisk()`);
- **custom assertions / exit logic** (beyond `test.passed`);
- **other special sequencing.**

For the non-trivial classes the helper must accept **optional in-loop hooks** —
`before_start=` and `after_run=`/cleanup — that run **inside** `_main` (before the loop
closes), so any post-run operation requiring the loop executes **before**
`asyncio.run()` returns. Scripts too idiosyncratic for hooks get an **explicit
conversion** rather than being forced through the helper. The invariant: **no
loop-dependent step may be left to run after `asyncio.run()` has returned.**

Approach:

1. Identify the shared shape (many `run-test` scripts are near-identical boilerplate:
   build a test object, `reactor.run()`, inspect result) and route those through
   `run_test_object(lambda: ...)` so each per-file diff is one line; route the pre/post
   classes through the `before_start`/`after_run` hooks, and hand-convert the rest.
2. Convert in batches by subsystem, re-running a sample from each batch. Un-migrated
   scripts keep working through the **shim's still-present blocking `reactor.run()`**
   (which honors the same shared kickoff/stop API as `run_async()` — B1 point 5); they
   do **not** ride the `run_async()` bridge (that path is only reached via `_main`).
   The blocking `run()` is not deleted until B4.
3. Track remaining `run-test` `reactor.run()`/`callWhenRunning` occurrences in the
   manifest to zero.
4. **A `run-test` is not the only lifecycle owner — a helper `test_case.py` can call
   `reactor.run()` too, and inside its `__init__`.** `tests/rest_api/applications/
   stasisstatus/test_case.py:50` calls `reactor.run()` **from `StasisStatusTestCase.
   __init__`**. Under the factory helper that constructor now runs *inside*
   `asyncio.run()`, so a literal port would attempt a **nested blocking reactor run** and
   deadlock. B2 must therefore, for this file (and any peer the manifest surfaces):
   - **remove loop ownership from the constructor** — construction only builds state; the
     run is driven by the entrypoint;
   - **route its `run-test` through `run_test_object`** like the others;
   - **widen the lifecycle scan to *every* source file** under `tests/` and `lib/`, not
     just `test_runner` and the 146 `run-test` scripts, so a `reactor.run()` hiding in a
     per-test helper module is caught. B0's inventory of lifecycle owners must list this
     file explicitly.
5. **Remove *every* script-level lifecycle call, not just `run()`.** Some scripts drive
   the reactor lifecycle directly rather than only via `reactor.run()`: **four** call
   `reactor.stop()` and **two** read `reactor.running` — e.g.
   `blind-transfer-parkingtimeout/run-test:68` (`reactor.running` + `reactor.stop()`),
   `fastagi/wait-for-digit/run-test:68` (same pair), `manager/mixmonitor/mixmonitor_id/
   run-test:129` (`reactor.stop()`), `funcs/func_presencestate/run-test:39`
   (`reactor.stop()`). These must migrate to the shared stop API (`test.stop_reactor()` /
   the runtime stop) and drop the direct `reactor.running` check. So the B2 lifecycle
   criterion is **zero `run`, `stop`, `running`, *and* `callWhenRunning` across all
   entrypoints and lifecycle helpers** — not "`run()` from scripts + `stop`/`running`
   from `test_case`/`asterisk.py`" only.
6. **Only after the last entrypoint is migrated**, remove the **lifecycle** reactor
   references from `test_case`/`asterisk.py` (the criterion deferred from B1) — nothing
   drives blocking `run()` any more, so this is now safe. Scope this narrowly:
   - **Remove now (B2):** `reactor.run()` from all entrypoints **and lifecycle helpers**;
     `callWhenRunning`, `reactor.stop()`, and `reactor.running` from `test_case`/
     `asterisk.py` **and from every script that calls them directly** (point 5).
   - **Leave for B3 (do NOT touch here):** the resource APIs those files still use —
     `listenTCP` (`test_case.py:383`), `callLater` (`test_case.py:584`), `spawnProcess`
     (`asterisk.py:488`), `callLater` (`asterisk.py:496`), and any peers. These are
     per-family conversions owned by §7 and must not be forced out prematurely.

   The **all-`reactor.*` zero criterion therefore does NOT belong to B2** — it belongs
   **after B3, immediately before B4** (§8), once every resource consumer has been
   converted too.

**Exit:** manifest shows **zero *lifecycle* `reactor.*`** — `reactor.run()` gone from
`test_runner`, all 146 `run-test` scripts, **and every per-test lifecycle helper (incl.
`stasisstatus/test_case.py`)**; and `callWhenRunning`/`stop`/`running` gone from
`test_case`/`asterisk.py` **and from the scripts that called `reactor.stop()`/
`reactor.running` directly** (point 5); a sampled test from each converted batch passes;
the suite matches the accepted baseline. `test_case`/`asterisk.py` **still legitimately
call `listenTCP`/`callLater`/`spawnProcess`** (converted in B3); the shim itself (and
the `run_async()` bridge) still reference the reactor. The complete all-`reactor.*`
zero gate is asserted at the end of B3, just before B4 deletes `reactor.py`.

## 7. Step B3 — Per-family conversion contracts

With a single native runtime owning every entrypoint, convert the resource calls. Each
family gets a **defined contract**, not a one-liner, and each is applied per consumer
with its representative test re-run.

### 7.1 Timers — a small owned `Timeout`/scheduling helper

Do **not** hand back raw `TimerHandle`s. Provide a tiny controller that reproduces
the used `_DelayedCall` surface:

- `call_later(delay, fn, *a, **kw)` → wraps `loop.call_later(delay,
  functools.partial(fn, *a, **kw))` (kwargs require `partial`).
- `getTime()` must return a **wall-clock Unix timestamp** (consumed by
  `datetime.fromtimestamp()` in `reset_timeout()`), and `reset(delay)` — both used by
  `test_case.reset_timeout()`. Implement via cancel + reschedule while tracking **both**
  the monotonic deadline (for `loop.call_later`) and the wall-clock fire time (for
  `getTime()`).
- `active()` vs. fired/cancelled state tracked explicitly.
- **Preserve `_DelayedCall`'s invalid-state exceptions** — `cancel()` on an
  already-fired-or-cancelled call and `reset()` after completion raise the specific
  Twisted errors (`AlreadyCalled`/`AlreadyCancelled`, the same names `error.py`
  re-exports — see §8). Either **reproduce and test those raises**, or **prove by
  manifest audit that no caller depends on them** before dropping them; do not silently
  turn a raising path into a no-op. (This ties to §8: those two exception names must be
  rehomed out of `reactor.py` into the timer module or `error.py`.)
- **omit `delay()`** — it has no caller anywhere (§2); implement it only if a real need
  appears, and only with a test.
- callback exceptions **reach the loop's installed exception handler** — either let the
  exception escape naturally (asyncio dispatches it) or call
  `loop.call_exception_handler({...})`; do **not** use `loop.set_exception_handler`,
  which *installs* a handler rather than dispatching. This matches the shim's current
  behavior (they do *not* go to `_fatal` today); flag any change to fatal semantics as
  intentional with its own test.
- cancellation honored during ordered shutdown.

This is a ~40-line owned utility, far safer than open-coding `TimerHandle` at ~132
sites. It replaces `callLater` everywhere; where the surrounding code is already a
coroutine, prefer `await asyncio.sleep(...)`.

### 7.2 UDP — preserve synchronous pre-bound send

Per consumer, choose one and record it:

- **(preferred for send-immediately fixtures)** keep a synchronous pre-bound socket:
  create + `bind()` the socket synchronously, install a send-capable transport on
  `protocol.transport` immediately, then attach the asyncio receive path from the
  same socket via `create_datagram_endpoint(sock=...)`. This is exactly what the shim
  does today; the "modernization" is relocating it into the module's `start()`/helper,
  not into `reactor.py`. **The endpoint-attachment (`create_datagram_endpoint`) runs as
  a runtime-owned task/registration**, not a fire-and-forget: if attachment fails, the
  handler must **close the pre-bound socket, record a fatal error on the runtime's
  `_failure`, and trigger ordered teardown** — a half-open socket must never be left
  behind, and the failure must propagate through `_main` (point 2a) rather than being
  swallowed.
- **(for receive-only or coroutine-context senders)** convert the fixture's send site
  to `await` endpoint creation before first transmit.

The strict-RTP/HEP/RTP-keepalive fixtures that transmit on the next line
(`strict_rtp.py:88` et al.) require the first form. Bind-failure must stay
synchronous/surfaced.

**Required UDP tests** (the pre-bound-socket split ownership is the risky part):

- **attachment failure** — `create_datagram_endpoint(sock=...)` raising must close the
  pre-bound socket exactly once, record the runtime fatal error, and tear down (no
  half-open socket);
- **`stopListening()` before endpoint attachment** — a consumer that binds then closes
  *before* the receive path attaches must not double-close the fd (no `EBADF` /
  `OSError` from closing an already-closed socket) and must leave clear ownership: once
  the endpoint owns the socket, the transport closes it; before that, the pre-bind code
  owns it. Exactly one owner closes it, once.

### 7.3 TCP — factories are Twisted-shaped

`create_server`/`create_connection` need callable protocol factories; the suite's
factories expose `buildProtocol()`. Per consumer, choose one and record it:

- **Convert** the factory + protocol to native `asyncio.Protocol` (preferred for
  simple listeners), or
- **Relocate and retain** `_TwistedProtocolAdapter` in a clearly-named compat module
  (e.g. `asterisk/aio/_tcp_compat.py`) and wrap the factory there.

Dedicated tasks (not one-liners) for: **fastAGI** listener in `test_case.py:383`;
**WebSocket** server protocols (`ari.py`, `media_websocket.py`); **reconnect**
(`connectTCP` + `ReconnectingClientFactory` retry/backoff + `connectionLost`
notification for the `keep_alive` test); and **factory backlinks** (`factory` ↔
protocol references some code relies on).

### 7.4 Subprocess — full parity contract

Replace `spawnProcess` with `loop.subprocess_exec(protocol_factory, executable,
*args[1:], env=env, cwd=path)` — note **`args[1:]`** (Twisted `args[0]` = program
name; forwarding `*args` duplicates `argv[0]`). Preserve, explicitly:

- immediate `protocol.transport` via a `_PendingProcessTransport` placeholder so a
  pre-`connection_made` signal/kill (early SIPp kill from an AMI event) is buffered
  and replayed. **The `protocol_factory` passed to `subprocess_exec()` must return the
  *exact* already-constructed protocol instance that carries the pending transport —
  normally `lambda: process_protocol`, not a fresh `ProcessProtocol()`.** If the factory
  builds a new instance, asyncio wires `connection_made` to that new object while the
  buffered early signal lives on the original, so the replay never reaches the real
  child — the early-kill path silently breaks;
- pipe draining and true exit-status delivery, including the **`_reliable_returncode`**
  double-reap fix (Phase A task #62);
- `cwd`/`env` propagation;
- teardown escalation **TERM → wait 1s → KILL → wait 1s** via the pidfd non-reaping
  path.

Applies to `asterisk.AsteriskProtocol` and `sipp.SIPpProtocol`.

### 7.5 Threads

`callInThread(fn, *a)` (WebSocket media send-file helpers) →
`loop.run_in_executor(None, functools.partial(fn, *a))`. **Define ownership and
shutdown semantics explicitly**, because a `run_in_executor` future is *not* like a
timer or a network task: **cancelling the asyncio future does not stop the worker
thread** — the blocking `fn` keeps running to completion in the executor. So the
contract is: register the future so shutdown **awaits** it (or an explicit
completion/quit flag the worker checks) rather than cancelling it and moving on;
never leave the default executor with in-flight work at loop close (that reintroduces
the "task/thread was destroyed" hazard). If a call site truly needs to abandon work,
give the worker a cooperative stop signal — do not rely on future cancellation.
**Executor workers must be stopped/joined in their owning module's `close()`, before
that module's network resources are torn down — not in the generic tracked-task phase.**
The seven-phase shutdown (point 4) closes async/network resources (WebSockets, ports)
**before** the tracked-tasks phase; a media send-file worker parked on the executor
would therefore keep writing against an **already-closed WebSocket** if it were only
awaited in the late task phase. So each module that spawns executor work owns the
join: `close()` signals the worker to stop and awaits its future first, so the socket
it writes to is still open while it drains. Do not rely on the generic task phase to
reap executor threads.

**Exit (per family):** the representative test(s) for that family pass, the
manifest count for that API drops to zero outside the shim/compat module, and the
**replacement test for that behavior is landed in this step** (immediate-UDP-send,
subprocess pre-`connection_made` kill + true exit status, TCP reconnect, timer
reset/getTime/active — items 1–6 of §12), before the shim behavior is removed in B4.

**Exit (B3 overall — the full `reactor.*` zero gate lives here, deferred from B2).**
Once **every** family above is converted, including the resource APIs `test_case.py`
and `asterisk.py` were left holding after B2 (`listenTCP:383`, `callLater:584`;
`spawnProcess:488`, `callLater:496`), the manifest shows **zero `reactor.*` anywhere
outside the shim** — lifecycle *and* resource references both gone. **Scope the gate
precisely: the *relocated* TCP compat module (once `_TwistedProtocolAdapter` moves there
in B4) may remain, but it must *not* reference `reactor` — only `reactor.py` itself may
still be reactor-dependent before B4 deletes it.** In other words the compat module is
allowed to exist but is held to the same zero-`reactor.*` standard as everything else;
it is not a second sanctioned home for reactor references. This is the complete gate B4
requires before deleting `reactor.py`.

## 8. Step B4 — Delete `reactor.py`, relocate the adapter, symbol-aware gate

Once the manifest shows zero `reactor.*` outside the shim/compat modules:

1. **Relocate `_TwistedProtocolAdapter`** out of `reactor.py` into the retained TCP
   compat module (§7.3) *or* confirm all TCP consumers were natively converted so it
   can be deleted. This decision gates deletion — see §11.
2. **Relocate the exception classes `error.py` re-exports before deleting `reactor.py`.**
   `aio/error.py:12` imports four names **from `.reactor`** — `ReactorNotRunning`,
   `ReactorAlreadyRunning`, `AlreadyCalled`, `AlreadyCancelled` — and the suite catches
   them by name; deleting `reactor.py` without moving them makes
   `from asterisk.aio import error` raise `ImportError` at collection time. So:
   - **Move the still-needed timer exceptions** `AlreadyCalled`/`AlreadyCancelled` into
     the new timer module (§7.1) or into `error.py` itself — they are the
     invalid-state errors the owned `Timeout` helper raises on `cancel()`-after-fire /
     `reset()`-after-completion (see §7.1), so they outlive the reactor.
   - **Remove the obsolete reactor-lifecycle exceptions** `ReactorNotRunning`/
     `ReactorAlreadyRunning` — once B2 has removed all `reactor.run()`/`stop()` calls,
     nothing raises or catches them, so drop them from `error.py`'s imports and
     `__all__` rather than rehoming dead names.
   - **Verify `from asterisk.aio import error` still imports** (and that every by-name
     catch site still resolves) as an explicit B4 check.
3. Delete `lib/python/asterisk/aio/reactor.py`; drop it from `aio/__init__.py`.
4. **Replace, don't just delete, the shim's lifecycle tests** (§12). `test_aio.py`
   has **58 test methods total** (not "~74"); B4 removes only the subset that exercises
   the deleted `reactor` runtime — the `run`/`stop`/`run_async`, delayed-call,
   listen/connect/spawn, and `_shutdown` cases. B0's manifest step must enumerate that
   exact subset (by method name) and map each to its replacement integration test
   (§12); the timer/defer/protocol tests that survive `reactor.py` deletion stay. No
   `reactor` test is deleted until its named replacement is green.
5. **Symbol-aware AST gate.** Extend `doc/untwist/check_no_twisted.py` to fail on:
   - `import asterisk.aio.reactor` **and** `from asterisk.aio import reactor` (bind of
     the `reactor` symbol) **and** attribute access `aio.reactor`;
   - if the `defer` shim is removed (decision D1, §1), likewise ban
     `asterisk.aio.defer` **and** the re-exported symbols `from asterisk.aio import
     defer, Deferred, DeferredList, maybeDeferred, gatherResults` (and any other name
     `aio/__init__.py` re-exports), not just the dotted path;
   - if starpy is in scope (§10 decision), `from starpy._async import reactor` and
     `starpy._async.reactor`.
   - **intra-package relative imports** — `from .reactor import ...` and
     `from . import reactor` (as `aio/error.py` uses today) — not only the
     fully-qualified `asterisk.aio.reactor` path. A relative import inside `aio/` is the
     exact form that would silently keep a dependency on the deleted module.
   Grepping only the dotted module path misses the common `from asterisk.aio import X`
   form that most consumers use, **and** the relative `from .reactor import X` form used
   within the package itself.

**Exit:** `reactor.py` gone; adapter relocated or deleted per §11; symbol-aware gate
bans reactor imports and passes; suite matches the accepted baseline.

## 9. Step B5 — Deferred: classify first, then migrate

`aio.Deferred` is awaitable and interoperates with coroutines, so migration is
incremental (module by module, highest density first: `sipp`, `test_case`,
`apptest`, the `*_test_condition` family). But the substitutions are **not** generic
— classify each site by behavior before replacing:

- **`addBoth` ≠ `try/finally`.** `addBoth(f)` calls `f` with *either* the result or a
  `Failure` and can **transform or recover**; model it as a function that receives
  `(value_or_exception)` and whose return replaces the value, not a bare `finally`.
- **`DeferredList` ≠ `gather`.** It yields `(success, result)` **tuples**, supports
  `fireOnOneCallback`/`fireOnOneErrback` (early firing) and `consumeErrors`. For the
  plain case, **wrap each awaitable in a small coroutine that returns `(True, value)`
  on success and `(False, exc)` when it catches an exception**, then
  `await asyncio.gather(*wrapped)`. Do **not** `gather(*aws, return_exceptions=True)`
  and then classify results with `isinstance(r, Exception)`: a coroutine may
  *legitimately return* an exception object as its value, which that heuristic would
  misreport as a failure. The per-awaitable wrapper distinguishes "returned an
  exception" from "raised an exception" correctly. **Cancellation has two distinct
  cases — do not collapse them** (the shim's behavior, `defer.py:268-279`, is that
  cancelling a *child* deferred errbacks it with `Failure(CancelledError())`,
  `defer.py:277`, which lands as a normal failure entry and does **not** cancel the
  aggregate):
  - **Child cancellation** (one input operation is cancelled): the wrapper records it as
    a **`(False, cancellation)` failure entry** (and it drives `fireOnOneErrback`/
    `consumeErrors` exactly like any other failure). It must **not** propagate
    `CancelledError` out of the aggregate. So the wrapper catches the child's
    `CancelledError` and converts it to a failure tuple — matching the shim.
  - **Cancellation of the aggregate/wrapper itself** (the `gather`/DeferredList await is
    cancelled from outside, e.g. shutdown/timeout): this **must propagate
    `CancelledError`** and not be swallowed into a tuple, or shutdown/timeout would
    stall. Implement by letting cancellation of the outer `gather`/task raise normally;
    only the *inner per-child* `CancelledError` is converted to `(False, cancellation)`.

  This is a real distinction on modern Python where `CancelledError` derives from
  `BaseException`: the inner wrapper must catch it deliberately (a bare `except
  Exception` would miss it and mislabel a cancelled child), while the outer aggregate
  await must re-raise it.
  **Early firing must reproduce the *current shim's*
  exact resolution shapes** (`defer.py:317-329`), which are asymmetric and **not** the
  Twisted-proper shapes: `fireOnOneCallback` resolves the `DeferredList`'s own deferred
  with a **`(result, index)`** pair identifying which deferred fired first
  (`defer.py:323`), whereas **`fireOnOneErrback` resolves with the *raw* failure —
  `dlist.errback(result)` (`defer.py:326`), no index tuple**. (Twisted proper instead
  raises a `FirstError` carrying the index; the shim does neither of those, and Phase B
  is behavior-preserving, so match the shim's raw-failure behavior and do **not**
  introduce an index on the errback path unless we consciously change it, with its own
  test.) The conversion wraps each awaitable so the callback winner reports its index —
  over index-tagged tasks — then returns `(result, index)` on early success and the
  **bare failure** on early error. **Do not use `return_when=FIRST_EXCEPTION` over the
  `(success, result)`-wrapping tasks:** a wrapper that catches the child error and
  returns `(False, exc)` **completes normally**, so `FIRST_EXCEPTION` never triggers and
  the early errback would never fire. Two correct implementations:
    - **loop over `FIRST_COMPLETED`** and inspect each newly-done wrapper: on the first
      `(True, result)` fire the `fireOnOneCallback` path (`(result, index)`); on the
      first `(False, failure)` fire the `fireOnOneErrback` path (**raw** failure); or
    - run the child tasks **unwrapped** (so a real exception propagates) under
      `FIRST_EXCEPTION` **only** for the errback-only variant — but the `FIRST_COMPLETED`
      loop is the single form that handles both flags together.
  **Combined `fireOnOneCallback=True` *and* `fireOnOneErrback=True` needs explicit
  handling:** whichever completes first (success or failure) wins and fires its
  respective shape; the `FIRST_COMPLETED` loop covers this naturally, `FIRST_EXCEPTION`
  does not. Document the failure-vs-success and index behavior at each converted site. **Early firing does not stop the other
  operations in Twisted** — they keep running and their results are still consumed;
  their **errors are swallowed *only when* `consumeErrors=True`** (`defer.py:330-331`:
  `if (not succeeded) and consumeErrors: return None`). When `consumeErrors=False`, a
  late failure on a not-yet-fired input is **not** swallowed — it stays an unhandled
  error on that operation, so the conversion must not blanket-suppress the remaining
  awaitables' exceptions. The conversion must therefore **keep the pending futures
  owned** (register them for shutdown or explicitly await/drain them), *not* leave them
  as orphaned pending tasks (ResourceWarning / "task was destroyed" leaks) and *not*
  casually cancel them unless the original `fireOnOne` semantics discarded them.
  **Retrieving a pending task's exception (for task hygiene) must not silently discard
  it when `consumeErrors=False`.** These two goals conflict: calling `.exception()` to
  avoid the "task exception was never retrieved" warning also *consumes* the error that
  `consumeErrors=False` says must stay visible. Resolve it by **retrieving and then
  re-reporting**: drain each late failure via `loop.call_exception_handler({...})` (so it
  is still surfaced, matching "not swallowed") rather than dropping it. Only under
  `consumeErrors=True` is the retrieved exception intentionally discarded.
  `gatherResults` (all-success, fail-fast) → `gather(*aws)`.
- **Errbacks receive `Failure`, not exceptions.** A chain that inspects
  `failure.type`/`failure.value`/`.getTraceback()` must be rewritten to `except`
  clauses that reconstruct the equivalent info; do not assume the caught object is a
  bare `Exception` with the same API.
- **Partial conversion only at compatible boundaries.** A callback-style caller
  cannot always consume a newly-`async` callee's return directly; convert a callee to
  `async` only where its callers either already `await` or are converted in the same
  commit. Track this per boundary.
- **Externally-fired `Deferred()`** (created, stored, `.callback()`d later by
  unrelated code) → an explicit `asyncio.Future`/`Event` with a documented
  fire/await recipe, not an inline chain rewrite.

Produce a short **classification pass** (extend the B0 manifest with a per-site
category) before mechanical edits.

**Deleting `defer.py` under D1 requires converting `asterisk.aio`'s own internal
consumers first — this is not just a test-suite migration.** Two package modules import
`Deferred` from `.defer` and would break the moment it is removed:

- **`protocols.py:26` → `LoopingCall`.** `LoopingCall` is built on `Deferred` and has a
  **production consumer** (the strict-RTP fixture). B5 under D1 must **convert that
  `LoopingCall` consumer** (to `asyncio.sleep`-loop task or equivalent) **and then remove
  or modernize `LoopingCall`** so `protocols.py` no longer imports `Deferred`.
- **`utils.py:19` → `getProcessOutputAndValue()`.** It returns a `Deferred`. B5 under D1
  must **convert `getProcessOutputAndValue()` and its callers (asterisk.py) to an async
  API** (`async def` returning `(out, err, code)` / raising on signal) so `utils.py` no
  longer imports `Deferred`.
- **Remove the `Deferred`/`DeferredList`/… re-exports from `aio/__init__.py`** (the names
  the §8 gate bans).
- **Explicit B5 check:** `import asterisk.aio`, `asterisk.aio.protocols`, and
  `asterisk.aio.utils` all still import successfully **after** `defer.py` is deleted.

Under **D2** these conversions are deferred with the shim; the two internal imports are
part of the allow-listed retained surface.

**Exit (per module):** representative test(s) pass; the module's `addCallback`/
`Deferred`/`DeferredList` manifest count is zero. **Phase-level, per the B0 D1/D2
decision (§1, §4.5):** under **D1** (default) the `defer` shim is deleted (after the
internal-consumer conversions above) and the symbol-aware gate (§8) bans
`asterisk.aio.defer` and its re-exported names; under **D2** the shim is explicitly
retained as a tracked legacy helper in a named follow-up phase, the gate keeps an
allow-listed exception for it, and the DoD (§13) is the weakened form. There is no third
"maybe" state — B0 already chose. **If D2 is selected, "small retained helper" must be
made measurable: record an exact allowlist of the files and symbols permitted to keep
using `defer`/`Deferred`, plus the remaining manifest count that allowlist accounts for,
so the gate can assert the count does not grow.** (Moot under the preferred D1 path,
where the count is zero.)

## 10. Step B6 — starpy modernization (implements the B0 decision)

The **decision A/B was made in B0** (§4.4) because it feeds the TCP and Deferred
strategy; this step *implements* it. starpy carries its own full shim (`_async.py`,
§2.3):

- **(A) Modernize starpy** as a parallel workstream: convert `manager.py`/`fastagi.py`
  off `_async` to native asyncio (its own reactor→`asyncio` lifecycle, `Deferred`→
  `async`, `LineOnlyReceiver`/`ReconnectingClientFactory` reconnect), delete
  `_async.py`, cut a new fork commit, and **update the pinned SHA in
  `requirements.txt`** + rebuild the venv. End state: "no reactor/Deferred shim
  anywhere, suite included and starpy included." **(A) requires explicit acceptance
  criteria — the one-paragraph summary is not enough to guide the implementation:**
    - **loop ownership** — starpy consumes the caller's running loop (does not create or
      `asyncio.run()` its own), so it composes with the suite's `AsyncTestRuntime`;
    - **public AMI/AGI API changes** — enumerate every signature that shifts from
      `Deferred`-returning to `async` (login, actions, event registration) and update the
      suite call sites in the same workstream;
    - **reconnect/backoff** — preserve `ReconnectingClientFactory`'s retry/backoff timing
      and `connectionLost` notification semantics on the native transport;
    - **line framing** — preserve `LineOnlyReceiver` delimiter/buffering behavior for
      AMI's `\r\n\r\n` packet framing and AGI line protocol;
    - **standalone use** — starpy must still run outside the suite (its own entrypoint),
      not only under the test runtime;
    - **suite integration** — the fork's AMI/AGI reconnect tests pass against the
      modernized code before the SHA is re-pinned.
- **(B) Scope starpy out of Phase B**: `starpy/_async.py` is a retained, tracked legacy
  shim in a named follow-up phase, and the stated end-state is **weakened** to "the
  suite's `asterisk.aio.reactor` shim is removed; starpy's shim removal is Phase C."
  Under (B), TCP compatibility for FastAGI is deliberately retained (§7.3).

Recommended: (A) if the fork's AMI/AGI reconnect tests are healthy enough to validate
against; otherwise (B) to keep Phase B's DoD honest. The B0 choice is enforced by the
gate scope in §8.

## 11. Step B7 — Adapter retirement (bounded, not "cosmetic")

Adapter retirement cannot be labeled purely optional, because deleting `reactor.py`
also removes `_TwistedProtocolAdapter`, which the TCP path still needs (§7.3, §8).
The binding decision:

- **either** every TCP consumer is converted to native `asyncio.Protocol` (then the
  adapter is deleted with `reactor.py`),
- **or** `_TwistedProtocolAdapter` is relocated to a retained compat module and kept
  until those consumers are converted.

The `DatagramProtocol`/`ProcessProtocol` adapters in `protocols.py` may remain (they
wrap native protocols idiomatically) or be rebased onto `asyncio.DatagramProtocol`/
`asyncio.SubprocessProtocol`; that part is genuinely low-priority. The
`SIPp`/`Asterisk` process rebase, if done, must retain `_reliable_returncode`.

## 12. Step B8 — Re-run replacement coverage, end-state gate, parity, docs

**Replacement tests are written in the step that changes each behavior, not here.**
Scheduling them in B8 (after B4 already deleted `reactor.py` and its `test_aio.py`
coverage) would leave a window with no coverage for the exact behavior being changed.
So each replacement test lands **before** its behavior is removed, alongside the step
that introduces the native equivalent:

| # | Replacement test | Written in |
|---|------------------|-----------|
| 1 | bind-failure propagation (can't-bind fails the test, not hangs) | B3.2/B3.3 |
| 2 | immediate UDP send (transmit on the line after bind) | B3.2 |
| 3 | pre-`connection_made` subprocess kill (buffered/replayed signal) | B3.4 |
| 4 | pipe draining + true exit status (killed-scenario → not-255) | B3.4 |
| 5 | TCP reconnect (`ReconnectingClientFactory` backoff + `connectionLost`) | B3.3 |
| 6 | timer `reset()`/`getTime()`(wall-clock)/active-state semantics | B3.1 |
| 7 | aiohttp `AppRunner` cleanup (no unclosed warnings) | B1 |
| 8 | stray-task cancellation at shutdown | B1 |
| 9 | single- and multi-Asterisk ordered teardown | B1 |

By the time B4 deletes the reactor-runtime subset of `test_aio.py`'s 58 methods (the
`run`/`stop`/`run_async`, delayed-call, listen/connect/spawn, and `_shutdown` cases —
enumerated by name in B0, §8 point 3), this integration coverage already exists and is
green. **B8 does not author new tests** — it re-runs the now-complete replacement suite
as a whole and performs the final gate + parity:

- **Symbol-aware end-state gate** (§8) green: bans `twisted`/`txaio`/`autobahn` and
  `asterisk.aio.reactor` (both import forms + attribute refs) across `lib`, `tests`
  (incl. `run-test`), and — per §10 — starpy; asserts none importable in the venv.
  Under defer decision **D1**, also bans `asterisk.aio.defer` **and** its re-exported
  symbols (`Deferred`/`DeferredList`/`maybeDeferred`/`gatherResults`/…); under **D2**,
  documents the allow-listed retained-helper exception.
- **Full-suite parity** vs. the B0 accepted baseline: the diff must contain only the
  30 known failures — no new failure, no new skip.
- **Docs:** update the checklist below; record the starpy decision (§10) and the
  end state in `02-design.md` §14.

## 13. End-state definition of done

Conditioned on the two B0 decisions (starpy A/B, §10; defer D1/D2, §1):

- No `asterisk.aio.reactor` references remain anywhere; the **entrypoints/runtime
  helpers** (`test_runner._main`, `run_test_object`) own `asyncio.run()` — `test_case`
  **consumes the active runtime**, it does not open its own `asyncio.run()` (a `test_case`
  that owned the loop would risk nested-loop ownership under an entrypoint that already
  did); test-object/module construction happens inside that single loop with an
  `async start()/close()` lifecycle. A **single `AsyncTestRuntime`**
  owns the one authoritative registry set (timers, tasks, ports, connectors, processes,
  cleanups), the per-run completion signal, the fatal error, and the ordered shutdown —
  there is never a separate reactor vs. test-object registry, and shutdown has a
  **single owner**; the transitional `run_async()` bridge is gone.
- New code is `async`/`await`.
- The `defer` shim is removed (**D1**, default), **or** reduced to a small,
  clearly-marked legacy helper with a named follow-up phase (**D2**).
- starpy's `_async.py` is removed and the SHA re-pinned (decision A), **or** explicitly
  declared a tracked Phase C item (decision B).
- The symbol-aware AST gate enforces all of the above; a full run matches the B0
  accepted baseline exactly (only the 30 known failures differ from all-pass).

## 14. Risks specific to Phase B

- **B1 runtime ownership is the real risk**, not the leaf inlines: moving construction
  into the loop and porting the exact seven-phase shutdown (with startup-failure
  cleanup and fatal-error precedence) is where teardown-ordering flakiness and leaked
  transports hide. Guard with the pjsip SIPp set and `-W error::ResourceWarning`.
- **UDP send-immediately** fixtures break under a naive awaited endpoint; the
  synchronous pre-bound socket contract (§7.2) is mandatory for them.
- **TCP factories** are Twisted-shaped; native conversion or a retained adapter is
  required — `create_server(factory,...)` fails outright.
- **Subprocess** argv duplication and lost TERM/KILL/pidfd behavior regress SIPp
  teardown and the 255 fix if the §7.4 contract is not followed.
- **Deferred** semantic mismatches (addBoth/DeferredList/Failure) silently change
  control flow; the classification pass (§9) is the mitigation.
- **The 146 `run-test` scripts** are easy to forget because they are extensionless;
  the AST manifest (B0) must parse them or scope is undercounted again.
- **starpy** left half-converted would reintroduce a Twisted-shaped shim into the
  runtime; the §10 decision must be explicit.

## 15. Testing matrix (parity targets)

Re-use the Phase A matrix (`03-implementation.md` §9) after each step and at the end,
diffing against the B0 accepted baseline:

| Subsystem | Representative area | Phase B relevance |
|-----------|---------------------|-------------------|
| Runtime/core | new `test_runner`/`test_case` integration tests | B1, B4, B8 |
| Entrypoints | sampled `run-test` per subsystem via `run_test_object` | B2 |
| Process control | single- and multi-Asterisk test | B3.4, B1 shutdown |
| SIPp | pjsip scenario incl. killed-at-teardown | B3.4 returncode guard |
| UDP/RTP | `strict_rtp/*`, `pjsip/rtp/*`, HEP | B3.2 immediate-send |
| DNS | `dns_server` test | B3.2/B3.3 |
| HTTP server | static + realtime | B1 `start/close`, B3 |
| ARI / WebSocket | `rest_api/applications/stasisstatus`, media send-file | B3.3, B3.5 |
| AMI / AGI | `tests/manager/*`, a FastAGI test | B5 defer, B6 starpy |
| Reconnect | `keep_alive` | B3.3 connectTCP |

## 16. Tracking checklist

- [ ] B0 — accepted baseline JSON + 30 classified known failures; AST manifest (parses `.py` **and** `run-test`, resolves dotted **and** re-exported reactor/defer symbols); **starpy A/B + defer D1/D2 decisions recorded**; enumerate the `test_aio.py` reactor-runtime subset → replacement map; gate + `pip check` green
- [ ] B1 — `asyncio.run(_main)` owns construction; **single `AsyncTestRuntime` owns the one registry set + per-run completion + fatal error + ordered shutdown — never separate reactor vs test-object registries**; **runtime created/installed BEFORE any constructor runs (native: on `_main` entry; legacy: lazily on first pre-run op incl. `callWhenRunning`, else `run()` creates it — drives, not replaces); reset/detached between sequential runs**; **`_main` runs runtime cleanup even if construction/loading raises**; **explicit runtime state machine COLLECTING→STARTING→drain-until-quiescent→RUNNING→flush kickoff→await→STOPPING; `_register_bind`/`callWhenRunning` gate on STATE, not the `reactor.running` boolean (avoids fire-and-forget binds before kickoff); `reactor.running` decoupled from classification — exposed TRUE during STARTING *and* RUNNING, FALSE during COLLECTING and STOPPING, so a `stop_reactor()`/timeout/signal during awaited startup sees `running=True` and actually stops**; **single owner AND single caller for module startup: `_main` alone calls `runtime.start_all()` (construct+retain → start() → drain pending binds → RUNNING → flush kickoff); `run_async()` does NOT call it and does NOT set running; legacy `run()` is the sole caller on its path; guard = once-per-run safety net (await in-flight if mid-STARTING)**; **`start_all()` cooperatively honors stop during STARTING — checks `stop_requested` between every hook/bind, races long startup ops against completion, cancels+drains the in-flight startup task, and goes straight to shutdown WITHOUT launching the test or transitioning through RUNNING (resolving completion alone does NOT interrupt it)**; **startup queue holds coroutine *factories* (or owned tasks), never bare coroutine objects — abandoned startup awaitables are cancelled/drained/discarded at shutdown so no "coroutine was never awaited" warning**; **STOPPING/STOPPED behavior defined: new tasks cancelled+drained, new ports/transports closed immediately, new timers/`callWhenRunning` rejected/cancelled, late async cleanups run inline or rejected, no callback queues work for the next run**; **owned-task exception policy: every task's exception retrieved (no "never retrieved" warning); fatal → store first failure + stop runtime; non-fatal → `loop.call_exception_handler()` (retrieve-then-report, not silent-discard)**; **runtime owns the shutdown *implementation* (`runtime._shutdown()`, idempotent), invoked per-path from the caller's OWN `finally`: `_main` on the native path, blocking `run()` on the legacy path (`_main` is NOT the sole shutdown caller)**; **transitional `run_async()` bridge** only adopts loop + awaits runtime completion + relays stop (no shutdown, no start_all); **completion primitive per-run, resolved via `call_soon_threadsafe`**; **startup ordering fixed: retain all modules → drain pending binds + native `start()` → then flush `callWhenRunning`**; **shim fatal / startup-bind failure re-raised through `_main` after `finally`**; **shared kickoff/stop API honored by both blocking `run()` and `run_async()` — `test_case`/`asterisk.py` reactor wiring NOT stripped yet**; async module `start()/close()` + loader mechanics (retain / start-optional / **close after `start()` invoked incl. failed starts + idempotent/partial-tolerant** / **registration-order teardown** / late-registration / task-refs); seven-phase ordered shutdown + startup-failure cleanup + fatal precedence; `pcap.py` own runner; **loader + teardown + single-registry + fatal-re-raise + sequential-reuse + cross-thread-stop + startup-race (a bind issued from a module `start()` completes before kickoff fires) + stop-during-STARTING (aborts startup, no test launch) + constructor-failure-after-registering-a-bind + first-bind-fails-with-later-binds-queued + no-"coroutine-never-awaited"-warnings replacement tests landed here**; an unmigrated `run-test` still passes
- [ ] B2 — **all 146 `run-test` scripts classified (constructor-only / pre-run setup / post-run cleanup / custom assertions / other)**; migrated to factory-based `run_test_object(lambda: ...)` **with optional in-loop `before_start`/`after_run` hooks for the ~57 that call `start_asterisk()`/`stop_asterisk()` around `reactor.run()` — no loop-dependent step left to run after `asyncio.run()` returns**; idiosyncratic scripts hand-converted; **lifecycle scan covers EVERY source file, not just `test_runner`+the 146 scripts — the `stasisstatus/test_case.py:50` helper that calls `reactor.run()` in `__init__` loses loop ownership and routes through `run_test_object`**; **scripts calling `reactor.stop()`/`reactor.running` directly (blind-transfer-parkingtimeout, fastagi/wait-for-digit, mixmonitor_id, func_presencestate) migrate to the shared stop API**; **then** remove only **lifecycle** reactor refs (`reactor.run()` from entrypoints+helpers; `callWhenRunning`/`stop`/`running` from `test_case`/`asterisk.py` **and those scripts**) — **leave `listenTCP`/`callLater`/`spawnProcess` for B3**; manifest zero **lifecycle** `reactor.*` — **zero `run`/`stop`/`running`/`callWhenRunning` across all entrypoints and lifecycle helpers**; **the full all-`reactor.*` zero gate is asserted at end of B3, not here**; un-migrated scripts run via the shim's still-present blocking `run()` (not the bridge)
- [ ] B3.1 — owned timer/`Timeout` helper (wall-clock `getTime`/reset/active/partial; exceptions → loop handler; **preserve+test `AlreadyCalled`/`AlreadyCancelled` invalid-state raises or prove unused**; **drop unused `delay()`**) replaces `callLater`; timer test landed
- [ ] B3.2 — UDP per-consumer contract (synchronous pre-bound send preserved where needed); **immediate-send + attachment-failure + `stopListening()`-before-attach (single owner closes socket once, no double-close/EBADF) tests landed**
- [ ] B3.3 — TCP per-consumer contract (native `asyncio.Protocol` or relocated adapter; fastAGI, WebSocket, reconnect tasks); reconnect test landed
- [ ] B3.4 — subprocess parity contract (`args[1:]`, pending-transport, drain, cwd/env, TERM→KILL, pidfd/255 fix; **`protocol_factory` returns the EXACT pending-transport-carrying instance, `lambda: process_protocol`**); pre-`connection_made` kill + exit-status tests landed
- [ ] B3.5 — `callInThread` → `run_in_executor` **with explicit ownership: shutdown awaits the future / cooperative stop flag (cancellation does NOT stop the worker thread); worker stopped/joined in its module `close()` BEFORE that module's network resources close (not in the generic tracked-task phase)**
- [ ] B3-overall — full all-`reactor.*` zero gate; **relocated TCP compat module may remain but must NOT reference `reactor` — only `reactor.py` itself may before B4**
- [ ] B4 — **relocate exception classes `error.py` re-exports (`AlreadyCalled`/`AlreadyCancelled` → timer module/`error.py`; drop obsolete `ReactorNotRunning`/`ReactorAlreadyRunning`) so `from asterisk.aio import error` still imports**; `reactor.py` (incl. `run_async` bridge) deleted; only the enumerated reactor-runtime `test_aio.py` methods removed (replacements green first); `_TwistedProtocolAdapter` relocated/deleted per §11; symbol-aware gate (dotted, re-exported, **and relative `from .reactor import` forms**)
- [ ] B5 — Deferred classified then migrated (addBoth/DeferredList-ownership/Failure; **DeferredList early-fire via `FIRST_COMPLETED` loop not `FIRST_EXCEPTION` over caught-error wrappers; combined `fireOnOneCallback`+`fireOnOneErrback` handled; `consumeErrors=False` late failures retrieved-then-reported, not discarded**); **under D1, convert internal `aio` deps first — strict-RTP `LoopingCall` consumer (then remove/modernize `LoopingCall` in `protocols.py`) + async `getProcessOutputAndValue()` in `utils.py` + drop `__init__.py` `Deferred` re-exports; verify `asterisk.aio`/`.protocols`/`.utils` still import after `defer.py` deleted**; `defer` shim removed (**D1**) or retained as named-deferred helper (**D2**) per B0
- [ ] B6 — starpy: implement the B0 decision (A modernize + re-pin SHA / B scope-out)
- [ ] B7 — adapters retired within the §11 bound
- [ ] B8 — **re-run** the B1–B3 replacement coverage (no new tests owed); end-state gate + baseline-parity green; docs updated
