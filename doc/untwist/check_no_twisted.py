#!/usr/bin/env python3
"""Definition-of-done gate for the Twisted removal (design Section 10).

Unlike a literal ``grep -rE "twisted"``, this checks *imports* via the AST, so it
does not false-positive on the explanatory docstrings/comments inside the
``asterisk.aio`` compatibility layer (review finding 11). Because only imports
are flagged, ``asterisk.aio`` itself is scanned too: the compat layer must not
import Twisted, and the AST gate is what proves it. It also asserts that Twisted
is absent from the active environment, which a source grep cannot prove.

The sibling starpy fork (``../starpy`` relative to this repo) is scanned by
default when present, since it is migrated in lockstep and must also be Twisted
free.

Usage:
    python3 doc/untwist/check_no_twisted.py [root ...]

Exit status is non-zero if any banned import is found in source, or if a banned
package is importable in the current environment.
"""

import ast
import os
import sys
import importlib.util

# Import roots that must be free of Twisted (and its WebSocket stack).
BANNED_PREFIXES = ('twisted', 'txaio', 'autobahn')

# No source directories are excluded. The AST gate flags imports only, so the
# compat layer's prose mentions of Twisted are safe and its actual imports are
# still checked.
EXCLUDE_DIRS = set()


def _default_roots():
    """Source roots scanned when none are given on the command line.

    The in-repo ``lib`` and ``tests`` trees, plus the *shipped* starpy package
    (``../starpy/starpy``) when it exists, so both halves of the migration are
    gated. ``starpy/examples`` is deliberately NOT in the default set: those 17
    scripts are standalone demos, not imported by the suite or the starpy
    package, and are declared out of scope (see 03-implementation.md §4.5). To
    audit them anyway, pass the path explicitly, e.g.
    ``check_no_twisted.py ../starpy``.
    """
    roots = ['lib', 'tests']
    here = os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))))           # repo root (testsuite/)
    starpy_root = os.path.normpath(os.path.join(here, os.pardir, 'starpy'))
    starpy_pkg = os.path.join(starpy_root, 'starpy')
    if os.path.isdir(starpy_pkg):
        roots.append(starpy_pkg)               # shipped package only
    elif os.path.isdir(starpy_root):
        roots.append(starpy_root)
    return tuple(roots)


def _banned(module):
    if module is None:
        return False
    head = module.split('.')[0]
    return head in BANNED_PREFIXES


def scan_file(path):
    """Return a list of (lineno, import-text) for banned imports in ``path``."""
    try:
        with open(path, 'r', encoding='utf-8') as handle:
            tree = ast.parse(handle.read(), filename=path)
    except (SyntaxError, UnicodeDecodeError) as exc:
        return [(0, 'could not parse: %s' % exc)]
    hits = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if _banned(alias.name):
                    hits.append((node.lineno, 'import %s' % alias.name))
        elif isinstance(node, ast.ImportFrom):
            # level>0 is a relative import and never references twisted.
            if node.level == 0 and _banned(node.module):
                hits.append((node.lineno, 'from %s import ...' % node.module))
    return hits


def scan_source(roots):
    findings = []
    for root in roots:
        for dirpath, _dirs, files in os.walk(root):
            if os.path.normpath(dirpath) in EXCLUDE_DIRS:
                continue
            if any(os.path.normpath(dirpath).startswith(ex)
                   for ex in EXCLUDE_DIRS):
                continue
            for name in files:
                if not name.endswith('.py'):
                    continue
                path = os.path.join(dirpath, name)
                for lineno, text in scan_file(path):
                    findings.append((path, lineno, text))
    return findings


def scan_environment():
    present = []
    for pkg in BANNED_PREFIXES:
        try:
            if importlib.util.find_spec(pkg) is not None:
                present.append(pkg)
        except (ImportError, ValueError):
            present.append(pkg)
    return present


def main(argv):
    roots = argv[1:] or list(_default_roots())
    findings = scan_source(roots)
    env = scan_environment()

    if findings:
        print("Banned Twisted/autobahn imports found in source:")
        for path, lineno, text in findings:
            print("  %s:%d: %s" % (path, lineno, text))
    if env:
        print("Banned packages importable in the environment: %s"
              % ", ".join(env))

    if findings or env:
        return 1
    print("OK: no Twisted/txaio/autobahn imports in %s; environment clean."
          % ", ".join(roots))
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
