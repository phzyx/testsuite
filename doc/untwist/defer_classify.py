#!/usr/bin/env python3
"""AST-based Deferred/defer classification (Phase B step B5.0).

Companion to ``reactor_defer_manifest.py``.  Where that tool answers "how many
defer references remain" for the zero-gate, this tool answers the question B5
actually needs to plan the async/await migration: **what SHAPE is each defer
construct, and where does it live**, so the call sites can be bucketed into
reviewable migration batches.

It walks three trees -- the suite library (``lib/python``), the test fixtures
(``tests``), and (optionally, via ``--starpy DIR``) the starpy fork -- and for
every ``.py`` file plus every extensionless ``run-test`` python script it
classifies defer usage into these buckets:

  construction (resolved through import aliases, like the manifest tool):
    * Deferred()                 -- bare Deferred construction
    * DeferredList(...)          -- with detected keyword flags
                                    (fireOnOneCallback / fireOnOneErrback /
                                     consumeErrors)
    * maybeDeferred / gatherResults / succeed / fail
    * LoopingCall(...)           -- polling primitive
    * getProcessOutputAndValue() -- utils async-subprocess helper

  chaining (method-name calls), split by attribution confidence:
    * high-confidence (counted always): addCallback / addCallbacks /
      addErrback / addBoth / chainDeferred -- these names appear ONLY on
      Deferreds in this codebase.
    * ambiguous (counted as Deferred chaining ONLY when the receiver is proven
      by same-file assignment to hold a Deferred; otherwise bucketed as
      ``ambiguous`` and NOT treated as a migration site): callback / errback /
      cancel / pause / unpause.  This keeps user callbacks
      (``self.callback(...)``) and asyncio Task/TimerHandle ``.cancel()`` out of
      the Deferred totals -- so files whose only defer-shaped call is such a
      false positive (e.g. ``lib/python/pcap_listener.py``) drop out.

  legacy generator style:
    * @inlineCallbacks / returnValue   (should be 0 -- flagged if not)

Import resolution also registers locally-defined defer symbols (the shim's own
``class DeferredList`` / ``def maybeDeferred``), so bare in-module calls -- e.g.
``DeferredList(..., fireOnOneErrback=True)`` inside ``aio/defer.py`` -- are
counted, not missed.

Files are tagged by ROLE so the report can propose an ordering:

    shim      lib/python/asterisk/aio/**         (the shim itself + its deps)
    core      lib/python/asterisk/** (non-aio)   (library consumers)
    libother  lib/python/** (outside asterisk/)
    fixture   tests/**                           (leaf test scripts)
    starpy    the fork (if --starpy given)

Usage:
    python3 doc/untwist/defer_classify.py [--starpy DIR] [--out DIR] [--check]

Reproducible: sorted, byte-identical across runs.  ``--check`` fails (exit 4)
if the committed classification would change.  NO code is modified -- this is a
read-only inventory to drive the B5 batch plan.
"""
from __future__ import annotations

import argparse
import ast
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
DEFAULT_OUT = os.path.join(HERE, "classify")

SKIP_DIRS = {".git", "__pycache__", "venv", ".venv", "env", ".tox",
             "node_modules", "build", "dist", ".eggs"}

# Construction symbols resolved through import aliases (never bare words).
CTOR_SYMBOLS = {
    "Deferred", "DeferredList", "maybeDeferred", "gatherResults",
    "succeed", "fail", "LoopingCall", "getProcessOutputAndValue",
    "inlineCallbacks", "returnValue",
}

# Chain-method names split by how safely they identify a Deferred receiver.
#
#   HIGH_CONFIDENCE -- names that in this codebase appear ONLY on Deferreds
#   (nothing else defines addCallback/addErrback/...).  Counted unconditionally.
#
#   AMBIGUOUS -- names that ALSO appear on non-Deferreds: user callbacks
#   (``self.callback(...)`` in pcap_listener/ari/pluggable_modules), asyncio
#   Tasks / TimerHandles (``task.cancel()`` / ``_handle.cancel()``), etc.  These
#   are counted as Deferred chaining ONLY when the receiver is a local/attribute
#   name proven (by same-file assignment) to hold a Deferred.  Otherwise they go
#   to a separate ``ambiguous`` bucket and do NOT make a file count as defer
#   usage -- so files whose only defer-shaped call is a non-Deferred callback
#   (e.g. pcap_listener.py) correctly drop out of the B5 inventory.
HIGH_CONFIDENCE = {
    "addCallback", "addCallbacks", "addErrback", "addBoth", "chainDeferred",
}
AMBIGUOUS = {"callback", "errback", "cancel", "pause", "unpause"}
CHAIN_METHODS = HIGH_CONFIDENCE | AMBIGUOUS

# Ctor symbols whose *return value* is a Deferred (used for receiver attribution).
DEFERRED_PRODUCING = {
    "Deferred", "DeferredList", "maybeDeferred", "gatherResults",
    "succeed", "fail",
}

DEFERREDLIST_FLAGS = ("fireOnOneCallback", "fireOnOneErrback", "consumeErrors")


def dotted(node):
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
        return ".".join(reversed(parts))
    return None


class Classifier:
    """Multi-phase, receiver-aware defer classifier for one file.

    Phases (over the same AST):
      A. resolve imports              -> ctor aliases + defer-module aliases
      B. register local defs          -> classes/functions the file itself
                                         defines with a defer name (so the shim's
                                         own ``class DeferredList`` counts)
      C. collect deferred-holding names -> targets assigned a Deferred-producing
                                         expression, for receiver attribution
      D. count                        -> ctor constructions, high-confidence
                                         chains (always), ambiguous chains (only
                                         when the receiver is a known Deferred)
    """

    def __init__(self):
        self.ctor_aliases = {}          # local name -> canonical ctor symbol
        self.mod_aliases = set()        # aliases bound to the defer/utils MODULE
        self.mod_dotted = {"asterisk.aio.defer", "asterisk.aio.utils",
                           "starpy._async.defer"}
        self.deferred_names = set()     # dotted names proven to hold a Deferred
        # results
        self.ctor = {}                  # canonical symbol -> count
        self.chain = {}                 # Deferred-attributed chain methods
        self.ambiguous = {}             # defer-shaped calls on non-Deferred recv
        self.dlist_flags = {}
        self.dlist_total = 0
        self.decorated_inline = 0

    def analyze(self, tree):
        self._collect_imports(tree)
        self._collect_local_defs(tree)
        self._collect_deferred_names(tree)
        self._count(tree)

    # ---- phase A: imports ---------------------------------------------
    def _collect_imports(self, tree):
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for a in node.names:
                    if a.name in ("asterisk.aio.defer", "asterisk.aio.utils"):
                        if a.asname:
                            self.mod_aliases.add(a.asname)
                        else:
                            self.mod_dotted.add(a.name)
            elif isinstance(node, ast.ImportFrom):
                self._import_from(node)

    def _import_from(self, node):
        mod = node.module or ""
        level = node.level
        names = node.names
        if level and mod in ("defer", "utils"):
            for a in names:
                if a.name in CTOR_SYMBOLS:
                    self.ctor_aliases[a.asname or a.name] = a.name
            return
        if level and mod == "":
            for a in names:
                if a.name in ("defer", "utils"):
                    self.mod_aliases.add(a.asname or a.name)
            return
        if mod in ("asterisk.aio", "asterisk.aio.defer", "asterisk.aio.utils",
                   "starpy._async", "starpy._async.defer"):
            for a in names:
                if a.name in ("defer", "utils"):
                    self.mod_aliases.add(a.asname or a.name)
                elif a.name in CTOR_SYMBOLS:
                    self.ctor_aliases[a.asname or a.name] = a.name
        elif mod.startswith("twisted"):
            for a in names:
                if a.name in CTOR_SYMBOLS:
                    self.ctor_aliases[a.asname or a.name] = a.name

    # ---- phase B: local defs ------------------------------------------
    def _collect_local_defs(self, tree):
        # A module that DEFINES a defer symbol (the shim's own ``class
        # DeferredList`` / ``def maybeDeferred``) resolves bare calls to it.
        for node in ast.walk(tree):
            if isinstance(node, (ast.ClassDef, ast.FunctionDef,
                                 ast.AsyncFunctionDef)) \
                    and node.name in CTOR_SYMBOLS:
                self.ctor_aliases.setdefault(node.name, node.name)

    # ---- phase C: deferred-holding names ------------------------------
    def _is_deferred_producing(self, value):
        if not isinstance(value, ast.Call):
            return False
        sym = self._resolve_ctor(value.func)
        if sym in DEFERRED_PRODUCING:
            return True
        # a chained call (d.addCallback(...)) returns the Deferred
        if isinstance(value.func, ast.Attribute) \
                and value.func.attr in HIGH_CONFIDENCE:
            return True
        return False

    def _collect_deferred_names(self, tree):
        for node in ast.walk(tree):
            targets = []
            if isinstance(node, ast.Assign) and \
                    self._is_deferred_producing(node.value):
                targets = node.targets
            elif isinstance(node, (ast.AnnAssign, ast.NamedExpr)) and \
                    node.value is not None and \
                    self._is_deferred_producing(node.value):
                targets = [node.target]
            for t in targets:
                d = dotted(t)
                if d:
                    self.deferred_names.add(d)

    # ---- phase D: counting --------------------------------------------
    def _resolve_ctor(self, func):
        if isinstance(func, ast.Name):
            return self.ctor_aliases.get(func.id)
        if isinstance(func, ast.Attribute):
            base = func.value
            if isinstance(base, ast.Name) and base.id in self.mod_aliases \
                    and func.attr in CTOR_SYMBOLS:
                return func.attr
            d = dotted(base)
            if d in self.mod_dotted and func.attr in CTOR_SYMBOLS:
                return func.attr
        return None

    def _count(self, tree):
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                self._count_call(node)
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                for dec in node.decorator_list:
                    name = dec.attr if isinstance(dec, ast.Attribute) else \
                        (dec.id if isinstance(dec, ast.Name) else None)
                    if name == "inlineCallbacks":
                        self.decorated_inline += 1

    def _count_call(self, node):
        sym = self._resolve_ctor(node.func)
        if sym:
            self.ctor[sym] = self.ctor.get(sym, 0) + 1
            if sym == "DeferredList":
                self.dlist_total += 1
                for kw in node.keywords:
                    if kw.arg in DEFERREDLIST_FLAGS:
                        self.dlist_flags[kw.arg] = \
                            self.dlist_flags.get(kw.arg, 0) + 1
        if isinstance(node.func, ast.Attribute) \
                and node.func.attr in CHAIN_METHODS:
            m = node.func.attr
            if m in HIGH_CONFIDENCE:
                self.chain[m] = self.chain.get(m, 0) + 1
            else:  # AMBIGUOUS: attribute to receiver
                recv = dotted(node.func.value)
                if recv in self.deferred_names:
                    self.chain[m] = self.chain.get(m, 0) + 1
                else:
                    self.ambiguous[m] = self.ambiguous.get(m, 0) + 1


def is_python_source(path):
    if path.endswith(".py"):
        return True
    if os.path.basename(path) == "run-test":
        try:
            with open(path, "rb") as fh:
                return b"python" in fh.readline(200)
        except OSError:
            return False
    return False


def iter_targets(root):
    for dp, dn, fns in os.walk(root):
        dn[:] = sorted(d for d in dn if d not in SKIP_DIRS)
        for fn in sorted(fns):
            p = os.path.join(dp, fn)
            if is_python_source(p):
                yield p


def role_for(rel, is_starpy):
    if is_starpy:
        return "starpy"
    if rel.startswith("lib/python/asterisk/aio/"):
        return "shim"
    if rel.startswith("lib/python/asterisk/"):
        return "core"
    if rel.startswith("lib/python/"):
        return "libother"
    if rel.startswith("tests/"):
        return "fixture"
    return "other"


def add_totals(dst, src):
    for k, n in src.items():
        dst[k] = dst.get(k, 0) + n


def classify_tree(root, is_starpy, per_file, role_ctor, role_chain,
                  ctor_totals, chain_totals, ambiguous_totals,
                  dlist_flag_totals, inline_files):
    for path in iter_targets(root):
        rel = os.path.relpath(path, root)
        try:
            tree = ast.parse(open(path, encoding="utf-8",
                                  errors="replace").read(), filename=path)
        except SyntaxError:
            continue
        c = Classifier()
        c.analyze(tree)
        # A file counts as defer usage only on a real construction, a
        # high-confidence/attributed chain, or an inlineCallbacks decorator.
        # Ambiguous-only files (e.g. pcap_listener's user self.callback) drop out.
        if not c.ctor and not c.chain and not c.decorated_inline:
            continue
        role = role_for(rel, is_starpy)
        key = rel if not is_starpy else f"[starpy] {rel}"
        per_file[key] = {
            "role": role,
            "ctor": dict(sorted(c.ctor.items())),
            "chain": dict(sorted(c.chain.items())),
            "ambiguous": dict(sorted(c.ambiguous.items())),
            "deferredlist_flags": dict(sorted(c.dlist_flags.items())),
            "inline_callbacks_decorators": c.decorated_inline,
        }
        role_ctor.setdefault(role, {})
        role_chain.setdefault(role, {})
        add_totals(role_ctor[role], c.ctor)
        add_totals(role_chain[role], c.chain)
        add_totals(ctor_totals, c.ctor)
        add_totals(chain_totals, c.chain)
        add_totals(ambiguous_totals, c.ambiguous)
        add_totals(dlist_flag_totals, c.dlist_flags)
        if c.decorated_inline:
            inline_files.append(key)


def build_manifest(starpy):
    per_file = {}
    role_ctor = {}
    role_chain = {}
    ctor_totals = {}
    chain_totals = {}
    ambiguous_totals = {}
    dlist_flag_totals = {}
    inline_files = []

    classify_tree(REPO, False, per_file, role_ctor, role_chain,
                  ctor_totals, chain_totals, ambiguous_totals,
                  dlist_flag_totals, inline_files)
    starpy_root = None
    if starpy:
        starpy_root = os.path.abspath(starpy)
        classify_tree(starpy_root, True, per_file, role_ctor, role_chain,
                      ctor_totals, chain_totals, ambiguous_totals,
                      dlist_flag_totals, inline_files)

    role_counts = {}
    for e in per_file.values():
        role_counts[e["role"]] = role_counts.get(e["role"], 0) + 1

    return {
        "repo_root": REPO,
        "starpy_root": starpy_root,
        "files_with_defer": len(per_file),
        "files_by_role": dict(sorted(role_counts.items())),
        "ctor_totals": dict(sorted(ctor_totals.items())),
        "chain_totals": dict(sorted(chain_totals.items())),
        "ambiguous_totals": dict(sorted(ambiguous_totals.items())),
        "deferredlist_flag_totals": dict(sorted(dlist_flag_totals.items())),
        "ctor_by_role": {r: dict(sorted(v.items()))
                         for r, v in sorted(role_ctor.items())},
        "chain_by_role": {r: dict(sorted(v.items()))
                          for r, v in sorted(role_chain.items())},
        "inline_callbacks_files": sorted(inline_files),
        "per_file": dict(sorted(per_file.items())),
    }


def render_json(manifest):
    return json.dumps(manifest, indent=2, sort_keys=True) + "\n"


def render_txt(manifest):
    L = []
    L.append("# Deferred/defer classification  (B5.0)")
    L.append(f"files_with_defer={manifest['files_with_defer']}  "
             f"roles={manifest['files_by_role']}")
    L.append("")
    L.append("## construction totals")
    for k, n in manifest["ctor_totals"].items():
        L.append(f"  {k:<26} {n}")
    L.append("")
    L.append("## chaining totals (Deferred-attributed method calls)")
    for k, n in manifest["chain_totals"].items():
        L.append(f"  {k:<26} {n}")
    L.append("")
    L.append("## ambiguous defer-shaped calls  (callback/errback/cancel/pause "
             "on non-Deferred or unresolved receivers -- NOT migration sites)")
    for k, n in (manifest["ambiguous_totals"] or {"(none)": 0}).items():
        L.append(f"  {k:<26} {n}")
    L.append("")
    L.append("## DeferredList flags")
    for k, n in (manifest["deferredlist_flag_totals"] or {"(none)": 0}).items():
        L.append(f"  {k:<26} {n}")
    L.append("")
    L.append("## construction by role")
    for r, v in manifest["ctor_by_role"].items():
        L.append(f"  [{r}] {v}")
    L.append("")
    L.append("## chaining by role")
    for r, v in manifest["chain_by_role"].items():
        L.append(f"  [{r}] {v}")
    L.append("")
    L.append(f"## inlineCallbacks decorators (should be 0): "
             f"{manifest['inline_callbacks_files'] or 'none'}")
    L.append("")
    L.append("## per-file (role | ctor | chain | ambiguous | dlist-flags)")
    for f, e in manifest["per_file"].items():
        parts = []
        if e["ctor"]:
            parts.append("ctor=" + ",".join(f"{k}:{n}"
                                            for k, n in e["ctor"].items()))
        if e["chain"]:
            parts.append("chain=" + ",".join(f"{k}:{n}"
                                             for k, n in e["chain"].items()))
        if e.get("ambiguous"):
            parts.append("amb=" + ",".join(f"{k}:{n}"
                                           for k, n in e["ambiguous"].items()))
        if e["deferredlist_flags"]:
            parts.append("dl=" + ",".join(f"{k}:{n}" for k, n
                                          in e["deferredlist_flags"].items()))
        L.append(f"  [{e['role']:8}] {f}")
        L.append(f"             {'  '.join(parts)}")
    return "\n".join(L) + "\n"


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--starpy", default=None,
                    help="path to the starpy fork to also classify")
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--check", action="store_true",
                    help="fail if committed classify.json/.txt would change")
    args = ap.parse_args()

    manifest = build_manifest(args.starpy)
    json_text = render_json(manifest)
    txt_text = render_txt(manifest)

    if args.check:
        drift = []
        for name, new in (("classify.json", json_text),
                          ("classify.txt", txt_text)):
            cur = os.path.join(args.out, name)
            old = open(cur).read() if os.path.exists(cur) else ""
            if old != new:
                drift.append(name)
        if drift:
            print(f"{', '.join(drift)} would change - re-run without --check",
                  file=sys.stderr)
            return 4
        print("classify.json and classify.txt up to date")
        return 0

    os.makedirs(args.out, exist_ok=True)
    with open(os.path.join(args.out, "classify.json"), "w") as fh:
        fh.write(json_text)
    with open(os.path.join(args.out, "classify.txt"), "w") as fh:
        fh.write(txt_text)

    print("\n".join(txt_text.splitlines()[:42]))
    print(f"\nwrote {args.out}/classify.json and classify.txt")
    return 0


if __name__ == "__main__":
    sys.exit(main())
