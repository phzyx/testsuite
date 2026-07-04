#!/usr/bin/env python3
"""AST-based reactor/defer manifest (Step B0.2).

Single source of truth for Phase B scope, ordering, and the "zero remaining"
end state.  Parses every ``.py`` file AND every extensionless ``run-test``
script in the testsuite tree, and emits per-file counts of:

  * every ``reactor.<attr>`` attribute reference, and
  * every ``defer`` / ``Deferred`` construct.

It resolves ALL the import forms the suite actually uses, so nothing is missed:

  reactor:
    import asterisk.aio.reactor [as R]         -> R.callLater / asterisk.aio.reactor.callLater
    from asterisk.aio import reactor [as r]    -> r.callLater
    from asterisk.aio.reactor import reactor
    from .reactor import reactor               (relative, inside the aio package)
    from . import reactor

  defer:
    import asterisk.aio.defer [as d]           -> d.Deferred
    from asterisk.aio import defer [as d]      -> d.Deferred
    from asterisk.aio import Deferred, DeferredList, maybeDeferred, gatherResults, ...
    from asterisk.aio.defer import Deferred, ...
    from .defer import Deferred                (relative, inside the aio package)
    from . import defer

It also emits an explicit **lifecycle-owner list**: every file (not just
``test_runner`` and the run-test scripts) that references
``reactor.run`` / ``reactor.stop`` / ``reactor.running`` / ``reactor.callWhenRunning``.
Per-test helper modules that own the loop are surfaced there.  The script
asserts that two known owners appear:
  * ``tests/rest_api/applications/stasisstatus/test_case.py`` (reactor.run in __init__)
  * scripts that call ``reactor.stop()`` / read ``reactor.running`` directly.

Usage:
    python3 doc/untwist/reactor_defer_manifest.py [ROOT] [--out DIR] [--check]

Reproducibility: sorted output, byte-identical across runs.  ``--check`` re-runs
and fails (exit 4) if the committed manifest would change.
"""
from __future__ import annotations

import argparse
import ast
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
DEFAULT_OUT = os.path.join(HERE, "manifest")

# Directories never scanned.
SKIP_DIRS = {".git", "__pycache__", "venv", ".venv", "env", ".tox", "node_modules"}

# Public defer symbols (from asterisk/aio/defer.py).  Only counted when tracked
# as an import alias, so these generic words ("succeed", "fail") never produce
# false positives from unrelated code.
DEFER_SYMBOLS = {
    "Deferred", "DeferredList", "gatherResults", "maybeDeferred",
    "succeed", "fail", "TimeoutError", "AlreadyCalledError",
    # Twisted-era names some call sites may still reference if partially migrated
    "inlineCallbacks", "returnValue",
}

LIFECYCLE_ATTRS = ("run", "stop", "running", "callWhenRunning")


def dotted(node):
    """Flatten an attribute/name chain to a dotted string, or None."""
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
        return ".".join(reversed(parts))
    return None


class FileVisitor(ast.NodeVisitor):
    def __init__(self):
        # alias sets resolved from this file's imports
        self.reactor_name_aliases = set()      # Name aliases bound to the reactor object/module
        self.reactor_dotted = {"asterisk.aio.reactor"}  # dotted chains that resolve to reactor
        self.defer_mod_aliases = set()         # Name aliases bound to the defer MODULE
        self.defer_dotted = {"asterisk.aio.defer"}
        self.defer_symbol_aliases = {}         # alias -> canonical defer symbol name
        # results
        self.reactor_attrs = {}                # attr -> count
        self.defer_uses = {}                   # symbol/attr -> count
        self.imports = []

    # ---- imports -------------------------------------------------------
    def visit_Import(self, node):
        for a in node.names:
            if a.name == "asterisk.aio.reactor":
                self.imports.append(a.name)
                if a.asname:
                    self.reactor_name_aliases.add(a.asname)
                else:
                    self.reactor_dotted.add("asterisk.aio.reactor")
            elif a.name == "asterisk.aio.defer":
                self.imports.append(a.name)
                if a.asname:
                    self.defer_mod_aliases.add(a.asname)
                else:
                    self.defer_dotted.add("asterisk.aio.defer")
        self.generic_visit(node)

    def visit_ImportFrom(self, node):
        mod = node.module or ""
        level = node.level
        names = node.names
        # relative imports inside the aio package: `from .reactor import ...`
        if level and mod == "reactor":
            for a in names:
                if a.name == "reactor":
                    self.reactor_name_aliases.add(a.asname or a.name)
            self.imports.append(f"{'.'*level}reactor")
            self.generic_visit(node); return
        if level and mod == "defer":
            for a in names:
                if a.name in DEFER_SYMBOLS:
                    self.defer_symbol_aliases[a.asname or a.name] = a.name
            self.imports.append(f"{'.'*level}defer")
            self.generic_visit(node); return
        if level and mod == "":
            # `from . import reactor` / `from . import defer`
            for a in names:
                if a.name == "reactor":
                    self.reactor_name_aliases.add(a.asname or a.name)
                elif a.name == "defer":
                    self.defer_mod_aliases.add(a.asname or a.name)
            self.generic_visit(node); return
        if mod == "asterisk.aio":
            self.imports.append(mod)
            for a in names:
                if a.name == "reactor":
                    self.reactor_name_aliases.add(a.asname or a.name)
                elif a.name == "defer":
                    self.defer_mod_aliases.add(a.asname or a.name)
                elif a.name in DEFER_SYMBOLS:
                    self.defer_symbol_aliases[a.asname or a.name] = a.name
        elif mod == "asterisk.aio.reactor":
            self.imports.append(mod)
            for a in names:
                if a.name == "reactor":
                    self.reactor_name_aliases.add(a.asname or a.name)
        elif mod == "asterisk.aio.defer":
            self.imports.append(mod)
            for a in names:
                if a.name in DEFER_SYMBOLS:
                    self.defer_symbol_aliases[a.asname or a.name] = a.name
        self.generic_visit(node)

    # ---- references ----------------------------------------------------
    def visit_Attribute(self, node):
        base = node.value
        if isinstance(base, ast.Name):
            if base.id in self.reactor_name_aliases:
                self.reactor_attrs[node.attr] = self.reactor_attrs.get(node.attr, 0) + 1
            elif base.id in self.defer_mod_aliases:
                k = f"defer.{node.attr}"
                self.defer_uses[k] = self.defer_uses.get(k, 0) + 1
        else:
            d = dotted(base)
            if d in self.reactor_dotted:
                self.reactor_attrs[node.attr] = self.reactor_attrs.get(node.attr, 0) + 1
            elif d in self.defer_dotted:
                k = f"defer.{node.attr}"
                self.defer_uses[k] = self.defer_uses.get(k, 0) + 1
        self.generic_visit(node)

    def visit_Name(self, node):
        if isinstance(node.ctx, ast.Load) and node.id in self.defer_symbol_aliases:
            sym = self.defer_symbol_aliases[node.id]
            self.defer_uses[sym] = self.defer_uses.get(sym, 0) + 1
        self.generic_visit(node)


def is_python_source(path):
    if path.endswith(".py"):
        return True
    if os.path.basename(path) == "run-test":
        try:
            with open(path, "rb") as fh:
                first = fh.readline(200)
            return b"python" in first
        except OSError:
            return False
    return False


def iter_targets(root):
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames if d not in SKIP_DIRS)
        for fn in sorted(filenames):
            p = os.path.join(dirpath, fn)
            if is_python_source(p):
                yield p


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("root", nargs="?", default=REPO)
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--check", action="store_true",
                    help="fail if committed manifest would change")
    args = ap.parse_args()
    root = os.path.abspath(args.root)

    per_file = {}
    reactor_totals = {}
    defer_totals = {}
    lifecycle_owners = {}
    parsed = skipped = 0
    scanned = 0

    for path in iter_targets(root):
        scanned += 1
        rel = os.path.relpath(path, root)
        try:
            src = open(path, encoding="utf-8", errors="replace").read()
            tree = ast.parse(src, filename=path)
        except SyntaxError:
            skipped += 1
            continue
        parsed += 1
        v = FileVisitor()
        v.visit(tree)
        if not v.reactor_attrs and not v.defer_uses:
            continue
        entry = {
            "reactor": dict(sorted(v.reactor_attrs.items())),
            "defer": dict(sorted(v.defer_uses.items())),
        }
        per_file[rel] = entry
        for k, n in v.reactor_attrs.items():
            reactor_totals[k] = reactor_totals.get(k, 0) + n
        for k, n in v.defer_uses.items():
            defer_totals[k] = defer_totals.get(k, 0) + n
        lc = {a: v.reactor_attrs[a] for a in LIFECYCLE_ATTRS if a in v.reactor_attrs}
        if lc:
            lifecycle_owners[rel] = {
                "counts": dict(sorted(lc.items())),
                "is_run_test": os.path.basename(path) == "run-test",
            }

    manifest = {
        "root": root,
        "scanned": scanned,
        "parsed": parsed,
        "skipped_non_python": skipped,
        "files_with_reactor_or_defer": len(per_file),
        "reactor_totals": dict(sorted(reactor_totals.items())),
        "defer_totals": dict(sorted(defer_totals.items())),
        "reactor_grand_total": sum(reactor_totals.values()),
        "defer_grand_total": sum(defer_totals.values()),
        "lifecycle_owners": dict(sorted(lifecycle_owners.items())),
        "per_file": dict(sorted(per_file.items())),
    }

    # Sanity: when scanning the testsuite repo, the two known lifecycle owners
    # must be present.  (Skipped for other roots, e.g. the starpy fork.)
    owners = manifest["lifecycle_owners"]
    if root == REPO:
        stasis = "tests/rest_api/applications/stasisstatus/test_case.py"
        assert stasis in owners and "run" in owners[stasis]["counts"], \
            f"expected lifecycle owner missing: {stasis}"
        direct_stop = [f for f, o in owners.items()
                       if o["is_run_test"] and ("stop" in o["counts"] or "running" in o["counts"])]
        assert direct_stop, "expected >=1 run-test script calling reactor.stop()/running directly"

    text = json.dumps(manifest, indent=2, sort_keys=True) + "\n"

    if args.check:
        cur = os.path.join(args.out, "manifest.json")
        old = open(cur).read() if os.path.exists(cur) else ""
        if old != text:
            print("manifest.json would change - re-run without --check", file=sys.stderr)
            return 4
        print("manifest.json up to date")
        return 0

    os.makedirs(args.out, exist_ok=True)
    with open(os.path.join(args.out, "manifest.json"), "w") as fh:
        fh.write(text)

    # concise human summary
    lines = []
    lines.append(f"# reactor/defer manifest  (root={os.path.relpath(root)})")
    lines.append(f"scanned={scanned} parsed={parsed} skipped_non_python={skipped} "
                 f"files_with_usage={len(per_file)}")
    lines.append("")
    lines.append(f"## reactor.* totals  (grand={manifest['reactor_grand_total']})")
    for k, n in manifest["reactor_totals"].items():
        lines.append(f"  reactor.{k:<20} {n}")
    lines.append("")
    lines.append(f"## defer totals  (grand={manifest['defer_grand_total']})")
    for k, n in manifest["defer_totals"].items():
        lines.append(f"  {k:<26} {n}")
    lines.append("")
    lines.append(f"## lifecycle owners ({len(owners)})  "
                 "[run/stop/running/callWhenRunning]")
    for f, o in owners.items():
        tag = "run-test" if o["is_run_test"] else "module"
        cs = " ".join(f"{a}={c}" for a, c in o["counts"].items())
        lines.append(f"  [{tag:8}] {f}   {cs}")
    with open(os.path.join(args.out, "manifest.txt"), "w") as fh:
        fh.write("\n".join(lines) + "\n")

    print("\n".join(lines))
    print(f"\nwrote {args.out}/manifest.json and manifest.txt")
    return 0


if __name__ == "__main__":
    sys.exit(main())
