# Step B0 — recorded decisions and baseline

Authoritative record of the two Phase B architectural decisions that B0 must
lock (plan §4 steps 4–5), plus pointers to the machine-readable baseline and
manifest committed alongside. Recorded **2026-07-04**.

## Baseline artifacts (committed)

- `freeze_baseline.py` → `baseline/baseline_phaseA.{json,csv}` + `baseline/known_failures.json`
  - Source run log: `/usr/src/phzyx/full-ts-run.txt` (run 2026-07-01).
  - 1099 testcases: **947 pass, 30 fail, 122 skip** (executed = pass+fail = 977).
  - The 30 failures are the accepted pre-existing baseline, each classified in
    `known_failures.json`. `freeze_baseline.py` errors if a failure appears that
    is not pre-classified, so a *new* failure cannot be silently absorbed.
  - Phase B "no regression" = a fresh run diffs to this file with the ONLY
    allowed differences being those 30; any new failure **or new skip** fails.
- `reactor_defer_manifest.py` → `manifest/manifest.{json,txt}`
  - Scans all `.py` + every python `run-test` script (393 files); resolves dotted,
    re-exported, and relative import forms; emits per-file `reactor.*` / defer
    counts and an explicit **lifecycle-owner list**.
  - Reproducible: `--check` fails if the committed manifest would change.
  - Totals: `reactor.*` grand = 503 (run=181, callLater=137, stop=59,
    callWhenRunning=35, listenTCP=29, listenUDP=14, spawnProcess=10, connectTCP=5,
    callInThread=5, running=9, …); defer grand = 106.
  - 158 lifecycle owners surfaced, including the two the reviews flagged:
    `tests/rest_api/applications/stasisstatus/test_case.py` (nested `reactor.run`)
    and the four run-test scripts that touch `reactor.stop()`/`reactor.running`
    directly (`blind-transfer-parkingtimeout`, `fastagi/wait-for-digit`,
    `funcs/func_presencestate`, `manager/mixmonitor/mixmonitor_id`).
- Gate: `check_no_twisted.py` green (lib, tests, starpy fork, environment);
  `pip check` reports no broken requirements.

## Decision 1 — `defer` shim: **D1 (remove the shim this phase)**

**Evidence.** Outside the `asterisk.aio` package, the defer caller surface is **61
`Deferred`/`DeferredList`/`maybeDeferred`/`gatherResults`/`AlreadyCalledError`
references across 22 files**, concentrated in a single repeated pattern:

- the `*_test_condition.py` family (channel/fd/lock/sip/pjsip/sip_dialog/thread/
  taskprocessor + `test_conditions.py`) — `Deferred` + `DeferredList` per file;
- `sipp.py` (8), `apptest.py` (6), `asterisk.py` (7), `test_case.py` (3);
- a handful of `tests/**` sites each using a single `DeferredList`/`Deferred`.

This is bounded and patternable within B5.

**Consequences.**
- **B5:** convert all 61 caller references to native asyncio, and convert the two
  internal shim consumers the manifest/inspection identified — `protocols.py`
  `LoopingCall` (`from .defer import Deferred`) and `utils.py`
  `getProcessOutputAndValue` (returns `Deferred`) — then **delete `defer.py`**.
- **B4/B8/DoD (§9, §13):** end-state manifest defer count for callers must reach
  **zero**; the DoD's "zero `Deferred` constructs" clause is in force (no D2
  allowlist / no retained-helper escape hatch).
- **B2/B3:** the `DeferredList` uses in the `*_test_condition.py` family are
  converted as part of their per-family contract work, not left for B8.

## Decision 2 — starpy: **B (scope modernization out to Phase C)**

**Evidence.** The fork is already Twisted-free through its own self-contained
`starpy/_async.py` (the manifest finds **zero** `asterisk.aio.reactor`/`defer`
references in the fork — it does not depend on the suite's shim). Its isolated
smoke test `tests/test_async_smoke.py` passes fully: AMI login+ping, FastAGI
variable-block + ANSWER dialog, AMI auto-reconnect + re-login (the riskiest
behavior), and disconnect cleanup. Fork health *permits* option A, but B keeps
Phase B bounded.

**Consequences.**
- **B3 (§7.3 TCP):** **retain the FastAGI TCP-compat path** — starpy's FastAGI
  server consumes `reactor.listenTCP`-shaped factories via `_async.py`; that
  adapter path stays through Phase B.
- **B4:** deleting `asterisk.aio/reactor.py` does **not** affect the fork (it uses
  its own `_async.reactor`), so no cross-coupling.
- **§8 gate scope:** narrowed — the "zero `asterisk.aio.reactor`/`defer`" end
  state covers the suite only, not the fork's private `_async.py`. The no-Twisted
  gate still applies to the fork and is already green.
- **B6/B8/DoD:** starpy modernization (remove `_async.py`, native asyncio, re-pin
  fork SHA, rebuild venv) is **NOT** part of Phase B DoD; deferred to Phase C.
  Plan §10 option A is retained for reference only.

## Decision 3 — pacing: **pause after B0 for review**

B0 (baseline + manifest + decisions) is committed and paused here for review
before any B1 runtime code (the single `AsyncTestRuntime`, state machine, and
`run_async` bridge — the highest-risk step).
