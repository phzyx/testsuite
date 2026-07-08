# Phase B step B5.0 — Deferred/defer classification & batch plan

Status: For review (classification only — **no code migrated**)
Date: 2026-07-08
Companion to: `doc/untwist/05-phase2-implementation.md` (§B5), `B0-decisions.md` (D1)
Tool: `doc/untwist/defer_classify.py` → `doc/untwist/classify/classify.{json,txt}`

## 1. What this is

B0 Decision **D1** is "remove the defer shim this phase," with a definition-of-
done of **zero `asterisk.aio.defer` Deferred constructs** left in the suite. B5.0
is the read-only inventory that turns that goal into reviewable batches: an
AST classifier walks the suite library (`lib/python`), the test fixtures
(`tests/`), and the starpy fork, and buckets every defer construct by **shape**
(construction vs. chaining) and **role** (where it lives). Nothing is migrated
yet — this document proposes the ordering for your sign-off before any code
changes.

## 2. The landscape

162 files carry a real defer construct, split by role:

| role       | files | what it is                                            |
|------------|-------|-------------------------------------------------------|
| `shim`     | 5     | `lib/python/asterisk/aio/**` — the shim + its own deps |
| `core`     | 23    | `lib/python/asterisk/**` (non-aio) — library consumers |
| `other`    | 2     | harness scripts outside `lib`/`tests`                  |
| `fixture`  | 112   | `tests/**` — leaf test scripts                         |
| `starpy`   | 20    | the fork (**out of B5 scope** — see §5)                |

Construction and chaining totals (suite **and** starpy):

```
construction                         chaining (Deferred-attributed)
  Deferred                  80          addCallback     303
  DeferredList              26          addErrback      182
  maybeDeferred              9          callback         66
  getProcessOutputAndValue   4          addCallbacks     62
  succeed                    3          errback          30
  fail                       5          addBoth           4
  gatherResults              1
  LoopingCall                1
```

Method-name attribution (see §2.1): `addCallback` / `addErrback` / `addBoth` /
`addCallbacks` / `chainDeferred` are counted unconditionally (these names appear
only on Deferreds here). The ambiguous names `callback` / `errback` / `cancel`
/ `pause` are counted as Deferred chaining **only when the receiver is proven to
hold a Deferred**; otherwise they are reported separately as non-migration
sites: `cancel` ×24 (all asyncio Task/TimerHandle cancels), `callback` ×18,
`errback` ×7 (user callbacks like `self.callback(...)` in `ari.py` /
`pluggable_modules.py`). The `cancel` name therefore does **not** appear in the
chaining totals at all — every `.cancel()` in the tree is a task/handle cancel,
not Deferred cancellation.

Two findings that make B5 **simpler than the checklist feared**:

- **No `@inlineCallbacks` / generator style anywhere** (0 decorators). Every
  construct is the explicit `Deferred()` + `addCallback` form — uniform to
  convert, no generator-coroutine rewrites.
- **Exactly one consumer `DeferredList` carries a flag.** The flag uses that
  exist are `consumeErrors` ×4, `fireOnOneErrback` ×2, `fireOnOneCallback` ×1.
  Three of the four `consumeErrors` sites and both `fireOnOneErrback` /
  `fireOnOneCallback` sites are shim/helper/test-internal (`aio/defer.py`,
  starpy `_async.py`, `aio/test_aio.py`) and are deleted wholesale (B5.4 /
  Phase C), not hand-migrated. The **one exception** is a real core consumer:
  `test_case.py:472` `defer.DeferredList(start_defers, consumeErrors=True)`
  (waits for all Asterisk instances to start, then chains
  `__check_success_failure` → `__perform_pre_checks` → `__run_callback`). That
  one is a genuine B5.2b semantic conversion — it maps to
  `asyncio.gather(*, return_exceptions=True)` with explicit inspection of the
  returned results/exceptions, **not** a bare `gather`. Every *other* consumer
  `DeferredList` is a plain "wait for all" that maps cleanly to `asyncio.gather`.
  (Correction to an earlier draft that reported `fireOnOneErrback ×0`: those
  helper-internal bare calls were missed until the classifier registered
  locally-defined defer symbols.)

### 2.1 Classifier accuracy notes

The classifier (`defer_classify.py`) resolves defer through import aliases
(`asterisk.aio.defer`, `asterisk.aio`, `starpy._async`, plus any residual
`twisted` names) **and** through locally-defined symbols, so the shim's own
`class DeferredList` / `def maybeDeferred` bare calls are counted. Ambiguous
chain names are attributed to receivers proven (by same-file assignment from a
Deferred-producing expression) to hold a Deferred; unattributed ones go to the
`ambiguous_totals` bucket and do **not** make a file count as defer usage. As a
result `lib/python/pcap_listener.py` — whose only defer-shaped call was a user
`self.callback(...)` — correctly drops out of the inventory (the `libother`
role is now empty). Both `classify.json` and `classify.txt` are validated by
`--check`.

## 3. The enabling fact (verified)

`asterisk.aio.defer.Deferred` is **Future-backed and awaitable** —
`Deferred.__await__` is defined (`aio/defer.py:254`). Therefore suite code can
`await` any Deferred a producer returns **before** that producer is converted.
This decouples the suite migration (B5) from the starpy fork (B6/Phase C): the
suite can move to `async`/`await` while starpy keeps returning its own
Future-backed Deferreds, which the converted suite simply awaits.

## 4. Proposed batch ordering (for review)

Direction: **root deps → core spine → leaf fixtures → delete shim.** Core is
converted before fixtures so fixture conversions target stable `async`
signatures rather than a moving target.

### B5.1 — Shim-internal deps (the D1 "first deps")

Small, foundational, one consumer each:

- **`aio/utils.py` `getProcessOutputAndValue`** → native `async def`. Sole
  consumer: `asterisk.py` (1 call site).
- **`aio/protocols.py` `LoopingCall`** → asyncio-native periodic task. Sole
  consumer: `tests/rtp/strict_rtp/strict_rtp_seqno/strict_rtp.py` (1 site).
- **`aio/__init__.py`** — drop the `defer` re-export (lands with B5.4 once
  `defer.py` is gone). No `Deferred` symbol is re-exported today, so this is a
  one-line removal, not a fan-out.

### B5.2 — Core library (23 files, `lib/python/asterisk/**` non-aio)

- **B5.2a — leaf helpers/producers:** `ami.py`, `ari.py`, `pluggable_modules.py`,
  `test_conditions.py` + the `*_test_condition.py` family (`sip_dialog`,
  `thread`, `taskprocessor`, `fd`, `sip_channel`, `pjsip_channel`, `channel`,
  `lock`), `voicemail.py`, `confbridge.py`, `bridge_test_case.py`, `originate.py`,
  `linkedid_check.py`, `sipp_iterator.py`, `self_test/harness_shared.py`.
  (`pcap_listener.py` is **not** here — its only defer-shaped call is a user
  callback, so it carries no real Deferred; see §2.1.)
- **B5.2b — the spine (convert last in core):** `sipp.py` (26 sites),
  `asterisk.py` (22), `apptest.py` (19), `test_case.py` (16 — includes the one
  real consumer `DeferredList(start_defers, consumeErrors=True)` at line 472;
  convert to `asyncio.gather(*, return_exceptions=True)` and inspect the result
  tuples for the per-instance failures the callback chain relies on — a
  semantic conversion, not a bare `gather`).

### B5.3 — Test fixtures (112 files), by subsystem family

Mirrors the Step 5 / B2.2 fixture batching. Most are shallow (1 chain each);
`addErrback` ×138 lives here but as one-liners. Suggested sub-batches:

| sub-batch | families                                             | ~files |
|-----------|------------------------------------------------------|--------|
| B5.3a     | `apps/voicemail` (41 files, 32 hits) — templated     | 41     |
| B5.3b     | `channels/pjsip`, `fax/pjsip`, `channels/iax2`       | ~32    |
| B5.3c     | `fastagi/*` (say-*, database, execute, hangup, …)    | ~50*   |
| B5.3d     | `manager/*`, `rest_api/*`, `pbx/*`, remaining `apps/*`, singletons | remainder |

\* fastagi rows overlap other counts; exact membership comes from
`classify.json` `per_file` at batch time.

### B5.4 — Retire the shim

Delete `aio/defer.py`; drop the `defer` re-export from `aio/__init__.py`;
remove/rewrite the defer portions of `aio/test_aio.py`; flip the manifest gate
from "count defer" to a **zero-Deferred hard gate**; verify `asterisk.aio`,
`.protocols`, `.utils`, `.error` still import clean. The two `other`-role defer
users — the shim's own `doc/untwist/test_core_runtime.py` and
`test_ssh_parity.py` parity harnesses — are updated/retired here alongside the
shim, not as a separate batch.

## 5. Out of B5 scope — the starpy fork (B6 / Phase C)

Classified here for completeness only. starpy carries the heaviest single files
(`fastagi.py` `addCallback` ×60, `manager.py` ×51) plus its own `_async.py`
shim. Per B0, starpy's shim removal is **Phase C**; during B5 it keeps returning
awaitable Deferreds that the converted suite awaits (§3). Excluded from the B5
zero-Deferred DoD.

## 6. Open questions for sign-off

1. **Boundary assumption** — OK to rely on `await`-ing starpy Deferreds through
   B5 (verified awaitable) and defer all starpy edits to B6? (Recommended: yes.)
2. **`DeferredList` mapping** — all but one consumer `DeferredList` is a plain
   wait-for-all → `asyncio.gather`. The single exception is `test_case.py:472`
   `DeferredList(start_defers, consumeErrors=True)`, which relies on error
   capture: its callback chain reads the per-instance (success, result) tuples
   to decide start success/failure. That converts to
   `asyncio.gather(*, return_exceptions=True)` with explicit result inspection
   (a B5.2b core-spine item), **not** a bare `gather`. The remaining
   `fireOnOneErrback` / `fireOnOneCallback` / `consumeErrors` flag uses are
   shim/helper/test-internal (`aio/defer.py`, starpy `_async.py`,
   `aio/test_aio.py`) and disappear when those modules are deleted (B5.4 /
   Phase C). Confirm no *other* consumer relies on error-swallowing semantics
   beyond gather's default.
3. **Ordering direction** — core-before-fixtures (proposed) vs. fixtures-first?
4. **`aio/test_aio.py`** — its 24 `Deferred`/8 `maybeDeferred`/etc. are the
   shim's *own* unit tests. Delete wholesale with `defer.py` (B5.4), or preserve
   any as regression coverage against the async replacements?
