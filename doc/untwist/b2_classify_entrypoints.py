#!/usr/bin/env python3
"""B2.0 entrypoint classifier.

Parses every extensionless ``run-test`` entrypoint script (and surfaces any
non-``run-test`` source file that *owns the loop* via ``reactor.run()`` inside a
function/constructor) and records, for each one, a set of INDEPENDENT
classification dimensions. B2 then derives a migration strategy from those
dimensions -- deliberately biased toward hand-conversion so no assertion is ever
silently dropped by an automated migration.

Why independent dimensions (not a single precedence chain)
----------------------------------------------------------
An earlier version chose ONE bucket by precedence (direct-lifecycle > pre/post >
custom-exit > constructor-only). That hid real complexity: a script with a
``start_asterisk()`` pre-run step *and* substantial post-run assertions was
reported as merely ``pre-run`` and would have been migrated as a simple hook
conversion, silently dropping the post-run checks. Dimensions are now orthogonal:

  * ``pre_run``            -- side-effecting work before ``reactor.run()``
                             (e.g. ``test.start_asterisk()``) that must move into
                             a ``before_start`` hook to run inside the loop.
  * ``post_run_cleanup``  -- a RECOGNIZED loop-dependent cleanup call after the
                             run (``stop_asterisk`` / ``stop_reactor`` / ``stop``)
                             that must move into an ``after_run`` hook.
  * ``custom_assertions`` -- any exit/post-run logic beyond the canonical
                             ``if not test.passed: return 1`` / ``return 0``: a
                             guard whose condition checks anything other than
                             ``<obj>.passed`` (e.g. ``overall_result``,
                             ``notified``), a mutation of ``.passed``, extra
                             conditionals/loops, or non-cleanup/non-logging calls
                             (``check_test_conditions``, ``check_voicemail_exists``).
  * ``multiple_objects``  -- more than one test-object construction (a CapWords
                             class instantiated by bare name). Dotted utility
                             calls (``os.path.join``) and lowercase factories are
                             NOT counted, so this reflects test objects, not any
                             assigned call.
  * ``direct_lifecycle``  -- drives the reactor lifecycle directly anywhere in the
                             file: ``reactor.stop()`` / ``reactor.running`` /
                             ``reactor.callWhenRunning`` (often in a non-TestCase
                             helper).
  * ``helper_owned``      -- a Python entrypoint whose ``main`` has no
                             ``reactor.run()`` AND does not call ``run_test_object``:
                             the loop is driven by a bespoke helper module (e.g. a
                             constructor that owns a blocking reactor -- see
                             ``non_run_test_loop_owners``). Handled in B2.3.
  * ``migrated``          -- ``main`` already calls ``run_test_object(...)``: the
                             script has been converted off the reactor onto the
                             async helper. A distinct dimension from ``helper_owned``
                             so migration progress is not conflated with the
                             still-legacy constructor-owns-reactor scripts.

Derived migration strategy (``requires_hand_convert`` wins)
-----------------------------------------------------------
  * ``shell``            -- non-Python harness (out of B2's Python-migration scope).
  * ``parse-error``      -- a Python shebang that failed to parse (a real problem).
  * ``migrated``         -- already converted: ``main`` calls ``run_test_object``.
  * ``helper-owned``     -- loop owned by a bespoke helper module, not yet
                            converted (B2.3).
  * ``hand-convert``     -- ``custom_assertions`` OR ``multiple_objects`` OR
                            ``direct_lifecycle``. Takes PRECEDENCE over hook
                            conversion: if a script needs hand attention for any
                            reason, that dominates the fact that it also has a
                            pre/post hook step.
  * ``hook-convert``     -- ``pre_run`` and/or ``post_run_cleanup`` only, with a
                            canonical tail and a single object: mechanically
                            movable into ``before_start`` / ``after_run`` hooks.
  * ``constructor-only`` -- the bare ``run_test_object(lambda: Test())`` form fits.

Usage:
    python3 doc/untwist/b2_classify_entrypoints.py [ROOT] [--out DIR] [--check]

Reproducible: sorted output, byte-identical across runs. ``--check`` re-runs and
exits non-zero if the committed artifact would change.
"""

import argparse
import ast
import json
import os
import sys


# Recognized loop-dependent cleanup methods that map cleanly onto an after_run
# hook. Anything else after the run is treated as a custom assertion so it gets
# hand attention rather than being silently assumed mechanical.
CLEANUP_METHODS = {'stop_asterisk', 'stop_reactor', 'stop'}

# Pure-logging methods are side-effect-free for classification: a bare logging
# call after the run is not itself a custom assertion (the surrounding
# conditional/mutation, if any, is what gets flagged).
LOG_METHODS = {'warn', 'warning', 'error', 'info', 'debug', 'critical',
               'exception', 'log'}


# ---------------------------------------------------------------------------
# reactor.<attr> detection
# ---------------------------------------------------------------------------
def _reactor_attr(node):
    """If ``node`` is ``reactor.<attr>`` (Attribute), return ``<attr>``; else None."""
    if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) \
            and node.value.id == 'reactor':
        return node.attr
    return None


def _is_reactor_run_call(node):
    """True if ``node`` is an ``ast.Call`` of ``reactor.run(...)``."""
    return (isinstance(node, ast.Call)
            and _reactor_attr(node.func) == 'run')


def _calls_run_test_object(scope):
    """True if ``run_test_object(...)`` is called anywhere in ``scope`` (a
    ``main`` FunctionDef, or None). This is the structural signature of a script
    already migrated onto the async helper: the loop is owned by
    ``run_test_object`` inside ``main``, not a raw ``reactor.run()``. Scoped to
    ``main`` (not the whole module) so an unrelated helper reference cannot
    mislabel a script. Matches only the bare-name call, never an import alias."""
    if scope is None:
        return False
    for node in ast.walk(scope):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id == 'run_test_object'):
            return True
    return False


def _method_name(call):
    """For ``x.method(...)`` return ``method``; for a bare ``func(...)`` return
    ``None`` (only attribute/method calls carry a method name)."""
    if isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute):
        return call.func.attr
    return None


def _all_call_names(stmt):
    """Every method/function name reached anywhere inside ``stmt`` (for the
    human-readable report only -- decisions use the structural helpers below)."""
    names = []
    for n in ast.walk(stmt):
        if isinstance(n, ast.Call):
            if isinstance(n.func, ast.Attribute):
                names.append(n.func.attr)
            elif isinstance(n.func, ast.Name):
                names.append(n.func.id)
    return names


def _call_assign_target(node):
    """If ``node`` is ``name = <Call>(...)`` (single bare-name target, call RHS),
    return the target name; else None. This is the shape of an object
    construction OR a side-effecting utility assignment (``proc = Popen(...)``);
    the two are told apart by position (primary) and CapWords (secondary)."""
    if (isinstance(node, ast.Assign) and isinstance(node.value, ast.Call)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)):
        return node.targets[0].id
    return None


def _is_capwords_ctor(node):
    """``name = CapWordsClass(...)`` with a bare CapWords callee -- the
    conservative signal for a SECONDARY object construction. Dotted utility
    calls (``subprocess.Popen``) and lowercase factories are excluded, so a
    second object is only counted when it clearly looks like a class."""
    if _call_assign_target(node) is not None:
        func = node.value.func
        return isinstance(func, ast.Name) and func.id[:1].isupper()
    return False


def _contains_call(stmt):
    return any(isinstance(n, ast.Call) for n in ast.walk(stmt))


def _is_control_flow(stmt):
    return isinstance(stmt, (ast.If, ast.For, ast.While, ast.Try, ast.With,
                             ast.AsyncFor, ast.AsyncWith))


def _is_state_mutation(stmt):
    """Assignment to an existing object's attribute/subscript, or an augmented
    assignment -- e.g. ``test.reactor_timeout = 100``. New local-name bindings
    (``path = a + b``) are not mutations."""
    if isinstance(stmt, ast.AugAssign):
        return True
    if isinstance(stmt, ast.Assign):
        return any(isinstance(t, (ast.Attribute, ast.Subscript))
                   for t in stmt.targets)
    if isinstance(stmt, ast.AnnAssign):
        return isinstance(stmt.target, (ast.Attribute, ast.Subscript))
    return False


def _condition_passed_only(test_node):
    """True iff a guard condition references ONLY ``<obj>.passed`` -- the
    canonical ``if not test.passed`` shape. Any other attribute (``notified``,
    ``overall_result``), any bare-name comparison, or any call makes it a custom
    assertion. This is the fix for guards that check more than ``passed``."""
    attrs = set()
    for n in ast.walk(test_node):
        if isinstance(n, ast.Call):
            return False
        if isinstance(n, ast.Attribute):
            attrs.add(n.attr)
    return attrs == {'passed'}


def _is_canonical_guard(node):
    """True if ``node`` is an ``if <passed-only>: return ...`` whose body/orelse
    contain only returns/pass."""
    if not isinstance(node, ast.If):
        return False
    if not _condition_passed_only(node.test):
        return False
    for inner in node.body + node.orelse:
        if not isinstance(inner, (ast.Return, ast.Pass)):
            return False
    return True


def _is_docstring_expr(stmt):
    """A bare string-constant statement (docstring / inline comment block)."""
    return (isinstance(stmt, ast.Expr)
            and isinstance(stmt.value, ast.Constant)
            and isinstance(stmt.value.value, str))


def _assigns_passed(stmt):
    """True if ``stmt`` assigns to ``<obj>.passed`` (a pass/fail mutation)."""
    if isinstance(stmt, ast.Assign):
        for tgt in stmt.targets:
            if isinstance(tgt, ast.Attribute) and tgt.attr == 'passed':
                return True
    return False


def classify_main(main_fn):
    """Classify the body of a ``main`` FunctionDef into independent dimensions."""
    body = main_fn.body

    run_idx = None
    for i, stmt in enumerate(body):
        if isinstance(stmt, ast.Expr) and _is_reactor_run_call(stmt.value):
            run_idx = i
            break

    info = {
        'has_reactor_run': run_idx is not None,
        'pre_run': False,
        'pre_run_calls': [],
        'post_run_cleanup': False,
        'post_run_calls': [],
        'custom_assertions': False,
        'multiple_objects': False,
        'ctor_count': 0,
    }
    if run_idx is None:
        return info

    before = body[:run_idx]
    after = body[run_idx + 1:]

    # --- construction / multiple objects -----------------------------------
    # Identify the primary test object WITHOUT assuming the first call-assignment
    # is it: utility/resource setup (``TEST_DIR = os.path.dirname(...)``,
    # ``tmp = NamedTemporaryFile()``, ``path = os.path.join(...)``) frequently
    # precedes construction. Prefer the conventional ``test = <call>()`` target
    # (every direct script uses it except blind-transfer-parkingtimeout, which is
    # already hand-convert); else fall back to the first CapWords construction.
    # Secondary objects are later CapWords constructions only -- the conservative
    # signal -- so ``func_srv_test()`` still registers as the (lowercase) primary
    # while ``os.path.join`` / ``Popen`` never inflate the object count.
    call_assign = [(i, _call_assign_target(s)) for i, s in enumerate(before)
                   if _call_assign_target(s) is not None]
    primary_idx = next((i for i, name in call_assign if name == 'test'), None)
    if primary_idx is None:
        primary_idx = next((i for i, _name in call_assign
                            if _is_capwords_ctor(before[i])), None)
    secondary_obj_idx = [i for i, _name in call_assign
                         if primary_idx is not None and i > primary_idx
                         and _is_capwords_ctor(before[i])]
    info['ctor_count'] = (1 if primary_idx is not None else 0) \
        + len(secondary_obj_idx)
    info['multiple_objects'] = len(secondary_obj_idx) >= 1

    # --- pre-run work ------------------------------------------------------
    # Every pre-run statement other than a docstring or a recognized object
    # construction (the primary object + CapWords secondaries) is pre-run work
    # when it carries a call, control flow, or state mutation. This catches
    # ``proc = subprocess.Popen(...)``, ``test.reactor_timeout = 100``, and
    # setup inside if/for/try -- not just top-level ``x.method()`` calls.
    construction_idx = set(secondary_obj_idx)
    if primary_idx is not None:
        construction_idx.add(primary_idx)
    for i, s in enumerate(before):
        if i in construction_idx or _is_docstring_expr(s):
            continue
        if _contains_call(s) or _is_control_flow(s) or _is_state_mutation(s):
            info['pre_run'] = True
            info['pre_run_calls'].extend(_all_call_names(s))

    # --- post-run classification ------------------------------------------
    for s in after:
        info['post_run_calls'].extend(_all_call_names(s))

        if isinstance(s, (ast.Return, ast.Pass)) or _is_docstring_expr(s):
            continue
        if _is_canonical_guard(s):
            continue
        if isinstance(s, ast.Expr) and isinstance(s.value, ast.Call):
            m = _method_name(s.value)
            if m in CLEANUP_METHODS:
                info['post_run_cleanup'] = True
                continue
            if m in LOG_METHODS:
                continue
            # Any other bare call after the run (check_test_conditions, etc.)
            # is treated as a custom assertion.
            info['custom_assertions'] = True
            continue
        if _assigns_passed(s):
            info['custom_assertions'] = True
            continue
        # Non-canonical if, for/while/try/with, other assignments: custom.
        info['custom_assertions'] = True

    return info


def scan_file_reactor_lifecycle(tree):
    """Set of direct lifecycle attrs (``stop`` / ``running`` / ``callWhenRunning``)
    referenced as ``reactor.<attr>`` anywhere in ``tree``."""
    found = set()
    for node in ast.walk(tree):
        attr = _reactor_attr(node)
        if attr in ('stop', 'running', 'callWhenRunning'):
            found.add(attr)
    return found


def _find_main(tree):
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == 'main':
            return node
    return None


def _reactor_run_in_function(tree):
    """List of function/method qualnames containing ``reactor.run()``."""
    owners = []

    class _Visitor(ast.NodeVisitor):
        def __init__(self):
            self.stack = []

        def _visit_scope(self, node):
            self.stack.append(node.name)
            has_run = any(_is_reactor_run_call(n)
                          for n in ast.walk(node)
                          if isinstance(n, ast.Call))
            if has_run:
                owners.append('.'.join(self.stack))
            self.generic_visit(node)
            self.stack.pop()

        def visit_FunctionDef(self, node):
            self._visit_scope(node)

        def visit_AsyncFunctionDef(self, node):
            self._visit_scope(node)

        def visit_ClassDef(self, node):
            self.stack.append(node.name)
            self.generic_visit(node)
            self.stack.pop()

    _Visitor().visit(tree)
    return owners


def migration_strategy(rec):
    """Derive the single migration strategy from the independent dimensions.
    ``hand-convert`` takes precedence over hook conversion."""
    if rec.get('language') == 'shell':
        return 'shell'
    if rec.get('primary_bucket') == 'parse-error' or rec.get('parse_error'):
        return 'parse-error'
    if rec.get('migrated'):
        return 'migrated'
    if rec.get('helper_owned'):
        return 'helper-owned'
    if (rec['custom_assertions'] or rec['multiple_objects']
            or rec['direct_lifecycle']):
        return 'hand-convert'
    if rec['pre_run'] or rec['post_run_cleanup']:
        return 'hook-convert'
    return 'constructor-only'


def collect(root):
    runtests = []
    for dirpath, _dirs, files in os.walk(os.path.join(root, 'tests')):
        for name in files:
            if name == 'run-test':
                runtests.append(os.path.join(dirpath, name))
    runtests.sort()

    records = []
    for path in runtests:
        rel = os.path.relpath(path, root)
        with open(path, 'r') as fh:
            src = fh.read()
        try:
            tree = ast.parse(src)
        except SyntaxError as exc:
            # run-test is extensionless; several are bash/lua harness scripts.
            # A non-python shebang means "out of B2 scope"; a python shebang that
            # fails to parse is a real problem to surface.
            first = src.splitlines()[0] if src.splitlines() else ''
            is_python = 'python' in first
            rec = {
                'file': rel,
                'language': 'python' if is_python else 'shell',
                'parse_error': str(exc),
            }
            rec['primary_bucket'] = 'parse-error' if is_python else 'shell'
            rec['migration_strategy'] = migration_strategy(rec)
            records.append(rec)
            continue

        main_fn = _find_main(tree)
        main_info = classify_main(main_fn) if main_fn else {
            'has_reactor_run': False}
        lifecycle = scan_file_reactor_lifecycle(tree)

        has_run = main_info.get('has_reactor_run', False)
        migrated = _calls_run_test_object(main_fn)
        # Invariant: a migrated main drives the loop via run_test_object and must
        # NOT also carry a raw reactor.run(). The two are mutually exclusive; a
        # file exhibiting both signals a half-finished edit worth surfacing loudly.
        assert not (migrated and has_run), (
            "%s: main() calls run_test_object AND reactor.run()" % rel)
        rec = {
            'file': rel,
            'language': 'python',
            'has_main': main_fn is not None,
            'has_reactor_run': has_run,
            'migrated': migrated,
            # A migrated script has no raw reactor.run(), but it is NOT a
            # bespoke-helper loop owner: keep the two disjoint so migration
            # progress is not conflated with the constructor-owns-reactor case.
            'helper_owned': (not has_run) and not migrated,
            'ctor_count': main_info.get('ctor_count', 0),
            'pre_run': main_info.get('pre_run', False),
            'pre_run_calls': sorted(set(main_info.get('pre_run_calls', []))),
            'post_run_cleanup': main_info.get('post_run_cleanup', False),
            'post_run_calls': sorted(set(main_info.get('post_run_calls', []))),
            'custom_assertions': main_info.get('custom_assertions', False),
            'multiple_objects': main_info.get('multiple_objects', False),
            'direct_lifecycle': bool(lifecycle),
            'direct_lifecycle_attrs': sorted(lifecycle),
        }
        rec['migration_strategy'] = migration_strategy(rec)
        rec['primary_bucket'] = rec['migration_strategy']
        records.append(rec)

    # Surface non-run-test loop owners (reactor.run inside a def/ctor) under
    # tests/ and lib/, so a hidden lifecycle helper is not missed (point 4).
    helper_owners = []
    for base in ('tests', 'lib'):
        for dirpath, _dirs, files in os.walk(os.path.join(root, base)):
            # Skip the shim's own aio package: reactor.run() there is the shim
            # exercising itself in its unit tests, not a production entrypoint.
            if os.sep + os.path.join('asterisk', 'aio') in dirpath:
                continue
            for name in files:
                if not name.endswith('.py'):
                    continue
                path = os.path.join(dirpath, name)
                with open(path, 'r') as fh:
                    src = fh.read()
                if 'reactor' not in src:
                    continue
                try:
                    tree = ast.parse(src)
                except SyntaxError:
                    continue
                owners = _reactor_run_in_function(tree)
                if owners:
                    helper_owners.append({
                        'file': os.path.relpath(path, root),
                        'owners': sorted(owners),
                    })
    helper_owners.sort(key=lambda d: d['file'])

    return records, helper_owners


def build_report(records, helper_owners):
    strategies = {}
    dimension_totals = {k: 0 for k in (
        'pre_run', 'post_run_cleanup', 'custom_assertions',
        'multiple_objects', 'direct_lifecycle', 'helper_owned', 'migrated')}
    python = shell = with_run = 0
    for rec in records:
        key = rec.get('migration_strategy', 'parse-error')
        strategies[key] = strategies.get(key, 0) + 1
        if rec.get('language') == 'shell':
            shell += 1
        else:
            python += 1
        if rec.get('has_reactor_run'):
            with_run += 1
        for dim in dimension_totals:
            if rec.get(dim):
                dimension_totals[dim] += 1
    summary = {
        'total_run_tests': len(records),
        'python_entrypoints': python,
        'shell_harnesses': shell,
        'with_reactor_run': with_run,
        'strategies': dict(sorted(strategies.items())),
        'dimension_totals': dict(sorted(dimension_totals.items())),
        'helper_loop_owners': len(helper_owners),
    }
    return {
        'summary': summary,
        'run_tests': records,
        'non_run_test_loop_owners': helper_owners,
    }


def render_txt(report):
    lines = []
    s = report['summary']
    lines.append('# B2.0 entrypoint classification')
    lines.append('run-test files: %d   (python: %d, shell: %d, '
                 'with reactor.run: %d)'
                 % (s['total_run_tests'], s['python_entrypoints'],
                    s['shell_harnesses'], s['with_reactor_run']))
    lines.append('')
    lines.append('## migration strategies')
    for k, v in s['strategies'].items():
        lines.append('  %-18s %d' % (k, v))
    lines.append('')
    lines.append('## dimension totals (independent; a file may set several)')
    for k, v in s['dimension_totals'].items():
        lines.append('  %-18s %d' % (k, v))
    lines.append('')
    lines.append('## non-run-test loop owners (reactor.run inside a def/ctor)')
    if report['non_run_test_loop_owners']:
        for h in report['non_run_test_loop_owners']:
            lines.append('  %s  [%s]' % (h['file'], ', '.join(h['owners'])))
    else:
        lines.append('  (none)')
    lines.append('')
    for strat in ('migrated', 'hand-convert', 'helper-owned', 'hook-convert',
                  'constructor-only', 'parse-error', 'shell'):
        members = [r for r in report['run_tests']
                   if r.get('migration_strategy') == strat]
        if not members:
            continue
        lines.append('## %s (%d)' % (strat, len(members)))
        for r in members:
            flags = []
            if r.get('pre_run'):
                flags.append('pre-run')
            if r.get('post_run_cleanup'):
                flags.append('post-cleanup')
            if r.get('custom_assertions'):
                flags.append('custom-assert')
            if r.get('multiple_objects'):
                flags.append('multi-object')
            if r.get('direct_lifecycle'):
                flags.append('direct-lifecycle=%s'
                             % ','.join(r['direct_lifecycle_attrs']))
            suffix = ('   ' + ' '.join(flags)) if flags else ''
            lines.append('  %s%s' % (r['file'], suffix))
        lines.append('')
    return '\n'.join(lines).rstrip() + '\n'


def main():
    parser = argparse.ArgumentParser(description='B2.0 entrypoint classifier')
    parser.add_argument('root', nargs='?', default='.')
    parser.add_argument('--out', default='doc/untwist/manifest')
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()

    records, helper_owners = collect(args.root)
    report = build_report(records, helper_owners)

    json_text = json.dumps(report, indent=1, sort_keys=True) + '\n'
    txt_text = render_txt(report)

    json_path = os.path.join(args.out, 'b2_classification.json')
    txt_path = os.path.join(args.out, 'b2_classification.txt')

    if args.check:
        ok = True
        for path, text in ((json_path, json_text), (txt_path, txt_text)):
            try:
                with open(path) as fh:
                    current = fh.read()
            except FileNotFoundError:
                current = None
            if current != text:
                print('%s would change - re-run without --check' % path)
                ok = False
        if ok:
            print('b2_classification up to date')
            return 0
        return 4

    os.makedirs(args.out, exist_ok=True)
    with open(json_path, 'w') as fh:
        fh.write(json_text)
    with open(txt_path, 'w') as fh:
        fh.write(txt_text)
    print(render_txt(report))
    print('wrote %s and %s' % (json_path, txt_path))
    return 0


if __name__ == '__main__':
    sys.exit(main())
