"""Module that spawns and manages running a test

This module provides an entry point, loading, and teardown of test
runs for the Test Suite

Copyright (C) 2014, Digium, Inc.
Matt Jordan <mjordan@digium.com>

This program is free software, distributed under the terms of
the GNU General Public License Version 2.
"""

import asyncio
import inspect
import sys
import logging
import logging.config
import os
import yaml
from importlib.machinery import SourceFileLoader

try:
    from yaml import CSafeLoader as MyLoader
except ImportError:
    from yaml import SafeLoader as MyLoader

from asterisk.aio.runtime import new_runtime, detach_runtime, get_current_runtime

LOGGER = logging.getLogger('test_runner')
logging.basicConfig()


class TestModuleFinder(object):
    """Determines if a module is a test module that can be loaded"""

    supported_paths = []

    def __init__(self, path_entry):
        """Constructor

        path_entry The path to look for test modules in
        """
        if not path_entry in TestModuleFinder.supported_paths:
            raise ImportError()
        LOGGER.debug("TestModuleFinder supports path %s" % path_entry)
        return

    def find_module(self, fullname, suggested_path=None):
        """Attempts to find the specified module

        Keyword Arguments:
        fullname       The full name of the module to load
        suggested_path Optional path to find the module at
        """
        search_paths = TestModuleFinder.supported_paths
        if suggested_path:
            search_paths.append(suggested_path)
        for path in search_paths:
            if os.path.exists('%s/%s.py' % (path, fullname)):
                return TestModuleLoader(path)
        LOGGER.debug("Unable to find module '%s'" % fullname)
        return None


class TestModuleLoader(object):
    """Loads modules defined in the tests"""

    def __init__(self, path_entry):
        """Constructor

        Keyword Arguments:
        path_entry The path the module is located at
        """
        self._path_entry = path_entry

    def _get_filename(self, fullname):
        """Get the full path to the specified python file"""
        return '%s/%s.py' % (self._path_entry, fullname)

    def load_module(self, fullname):
        """Load the module into memory

        Keyword Arguments:
        fullname The full name of the module to load
        """
        if fullname in sys.modules:
            mod = sys.modules[fullname]
        else:
            mod = sys.modules.setdefault(
                fullname,
                SourceFileLoader(fullname, self._get_filename(fullname)).load_module())

        return mod


sys.path_hooks.append(TestModuleFinder)


def load_test_modules(test_config, test_object):
    """Load the pluggable modules for a test

    Keyword Arguments:
    test_config The test configuration object
    test_object The test object that the modules will attach to
    """

    if not test_object:
        return
    if not 'test-modules' in test_config:
        LOGGER.error("No test-modules block in configuration")
        return
    if 'modules' not in test_config['test-modules']:
        # Not an error - just no pluggable modules specified
        return

    # Retain constructed modules on the runtime so they (a) survive past
    # construction rather than being GC'd, and (b) are enrolled in the async
    # start()/close() lifecycle driven by start_all/_shutdown.
    runtime = get_current_runtime()

    for module_spec in test_config['test-modules']['modules']:
        # If there's a specific portion of the config for this module,
        # use it
        if ('config-section' in module_spec
                and module_spec['config-section'] in test_config):
            module_config = test_config[module_spec['config-section']]
        else:
            module_config = test_config

        module_type = load_and_parse_module(module_spec['typename'])
        # Modules take in two parameters: the module configuration object,
        # and the test object that they attach to
        module = module_type(module_config, test_object)
        if runtime is not None and module is not None:
            runtime.register_module(module)


def load_and_parse_module(type_name):
    """Take a qualified module/object name, load the module, and return
    a typename specifying the object

    Keyword Arguments:
    type_name A fully qualified module/object to load into memory

    Returns:
    An object type that to be instantiated
    None on error
    """

    LOGGER.debug("Importing %s" % type_name)

    # Split the object typename into its constituent parts - the module name
    # and the actual type of the object in that module
    parts = type_name.split('.')
    module_name = ".".join(parts[:-1])

    if not len(module_name):
        LOGGER.error("No module specified: %s" % typename)
        return None

    if os.path.exists('lib/python/asterisk/%s.py' % module_name):
        # This is convoluted but required.  lib/python/asterisk packages
        # must be loaded using absolute package names and 'asterisk' must
        # be included in the list of parts.  We cannot simply prepend
        # type_name from the start because this blocks load of modules
        # that are local to the test (add-test-to-search-path: 'True').
        module_name = 'asterisk.' + module_name
        parts = ['asterisk'] + parts

    module = __import__(module_name)
    for comp in parts[1:]:
        module = getattr(module, comp)
    return module


def create_test_object(test_path, test_config):
    """Create the specified test object from the test configuration

    Parameters:
    test_path   The path to the test directory
    test_config The test configuration object, read from the yaml file

    Returns:
    A test object that has at least the following:
        - __init__(test_path) - constructor that takes in the location of the
            test directory
        - evaluate_results() - True if the test passed, False otherwise
    Or None if the object couldn't be created.
    """
    def get_test_object():
        objs = test_config['test-modules']['test-object']
        if not isinstance(objs, list):
            objs = [objs]
        return next((obj for obj in objs), None)

    if not 'test-modules' in test_config:
        LOGGER.error("No test-modules block in configuration")
        return None
    if not 'test-object' in test_config['test-modules']:
        LOGGER.error("No test-object specified for this test")
        return None

    test_object_spec = get_test_object()
    if not test_object_spec:
        LOGGER.error("No test-object found for version range(s)")
        return None

    module_obj = load_and_parse_module(test_object_spec['typename'])
    if module_obj is None:
        return None

    test_object_config = None
    if ('config-section' in test_object_spec
            and test_object_spec['config-section'] in test_config):
        test_object_config = test_config[test_object_spec['config-section']]
    else:
        test_object_config = test_config

    # The test object must support injection of its location as a parameter
    # to the constructor, and its test-configuration object (or the full test
    # config object, if none is specified)
    test_obj = module_obj(test_path, test_object_config)
    return test_obj


def load_test_config(test_directory):
    """Load and parse the yaml test config specified by the test_directory

    Note: this will throw exceptions if an error occurs while parsing the yaml
    file. This is expected: if you provide an invalid configuration, it's far
    easier to let this crash and fix the yaml then try and 'handle' a completely
    invalid configuration gracefully.

    Parameters:
    test_directory The directory containing this test run's information

    Returns:
    An object containing the yaml configuration, or None on error
    """

    test_config = None

    # Load and parse the test configuration
    test_config_path = ('%s/test-config.yaml' % test_directory)
    if not os.path.exists(test_config_path):
        LOGGER.error("No test-config.yaml file found in %s" % test_directory)
        return test_config

    with open(test_config_path) as file_stream:
        test_config = yaml.load(file_stream, Loader=MyLoader)

    return test_config


def read_module_paths(test_config, test_path):
    """Read additional paths required for loading modules for the test

    Parameters:
    test_config The test configuration object
    """

    if not 'test-modules' in test_config:
        # Don't log anything. The test will complain later when
        # attempting to load modules
        return

    if ('add-test-to-search-path' in test_config['test-modules'] and
            test_config['test-modules']['add-test-to-search-path']):
        TestModuleFinder.supported_paths.append(test_path)
        sys.path.append(test_path)

    if 'add-to-search-path' in test_config['test-modules']:
        for path in test_config['test-modules']['add-to-search-path']:
            TestModuleFinder.supported_paths.append(path)
            sys.path.append(path)

    if 'add-relative-to-search-path' in test_config['test-modules']:
        for path in test_config['test-modules']['add-relative-to-search-path']:
            TestModuleFinder.supported_paths.append(os.path.join(test_path, path))
            sys.path.append(os.path.join(test_path, path))


async def _main(test_directory, test_config, result):
    """Native asyncio entrypoint for a single test run.

    Driven by ``asyncio.run()``, so ``_main`` owns the event loop and the single
    per-run ``AsyncTestRuntime`` for the *whole* run, construction included. The
    runtime is installed *first* so ``reactor.*`` registrations issued from test
    object / module constructors land in it; the test object and its modules are
    then built (state COLLECTING -> constructor binds enter the awaited startup
    queue); ``start_all`` drives the awaited startup phase (STARTING -> drain
    -> RUNNING -> flush kickoff); ``run_async`` awaits the completion signal.

    Cleanup has one owner and always runs: ``finally`` invokes the runtime's
    ordered ``_shutdown`` (via ``_finish``) even if construction, module loading,
    or startup raised -- so a port/timer/process registered by an early
    constructor is torn down before the exception propagates -- then detaches the
    runtime. A fatal error (startup-bind failure or a stored mid-run ``_failure``)
    re-raises out of ``_main`` *after* that teardown. The loaded test object is
    handed back through ``result`` so the caller can evaluate it.
    """
    runtime = new_runtime(asyncio.get_running_loop())
    try:
        test_object = create_test_object(test_directory, test_config)
        if test_object is None:
            return

        # Retain the test object for result evaluation even if a later module
        # load / startup step raises (teardown still runs in finally).
        result['test_object'] = test_object

        # Load other modules that may be specified
        load_test_modules(test_config, test_object)

        # Load global modules as well
        if test_object.global_config.config:
            load_test_modules(test_object.global_config.config, test_object)

        # Drive the shared startup state machine, then await completion (bridge).
        await runtime.start_all()
        await runtime.run_async()
    finally:
        # Single ordered teardown, run on every exit path, then detach. Whether
        # a teardown failure is suppressed depends on whether setup/run already
        # failed (an exception is unwinding through this finally):
        #   - setup/run failed: log the teardown error and preserve the
        #     original exception (a broken teardown must not mask it).
        #   - clean run: a teardown failure is itself a real failure and must
        #     propagate, so a test with broken teardown cannot report success.
        # Detachment is guaranteed in either case via the inner finally.
        # A fatal mid-run task stores runtime._failure and stops the run
        # *normally* (stop() resolves completion), so no exception is unwinding
        # through this finally. sys.exc_info() alone would therefore miss that
        # case and let a teardown error escape and mask the real fatal error
        # (which is only re-raised below). Fold _failure into the snapshot so a
        # teardown failure is logged-and-suppressed whenever setup/run already
        # failed, and the original fatal error is the one that propagates.
        setup_failed = (sys.exc_info()[0] is not None
                        or runtime._failure is not None)
        try:
            await runtime._finish()
        except Exception:
            if setup_failed:
                LOGGER.exception("error during runtime shutdown")
            else:
                raise
        finally:
            detach_runtime(runtime)

    # Re-raise a fatal error (startup or mid-run) after the teardown above.
    if runtime._failure is not None:
        failure = runtime._failure
        runtime._failure = None
        raise failure


async def _maybe_await(value):
    """Await ``value`` if it is awaitable, otherwise return it unchanged.

    In-loop hooks (``before_start``/``after_run``) may be written as plain
    callables (returning ``None``), coroutine functions, or functions that
    return a chainable awaitable. All three shapes are awaitable-or-not; this
    normalizes them so the hook driver can ``await`` uniformly.
    """
    if inspect.isawaitable(value):
        return await value
    return value


async def _run_object_async(factory, before_start, after_run, result):
    """Async core of ``run_test_object``: the ``run-test`` analogue of ``_main``.

    Where ``_main`` builds the test object from a parsed test-config, this builds
    it from a caller-supplied ``factory`` -- the shape a ``run-test`` script needs
    now that construction must happen *inside* the running loop. The
    lifecycle is otherwise identical to ``_main``: install a fresh per-run
    runtime first (so ``reactor.*`` registrations from the constructor land in
    it), construct via ``factory()``, drive the shared startup state machine
    (``start_all``) and await completion (``run_async``), then run the single
    ordered teardown in ``finally`` on every exit path and detach.

    The optional hooks replace work a legacy script did *around* ``reactor.run()``
    but that ``asyncio.run()`` would otherwise strand on a closed loop:

      * ``before_start(test)`` runs *before* ``start_all`` -- the slot a script
        used for pre-run work (e.g. ``start_asterisk()``) issued before
        ``reactor.run()``.
      * ``after_run(test)`` runs *after* ``run_async`` returns but *before* the
        ``finally`` teardown and, crucially, before ``asyncio.run()`` closes the
        loop -- the slot for post-run work (e.g. ``stop_asterisk()``) a script
        issued after ``reactor.run()``. Running it here upholds the invariant
        that no loop-dependent step is left for after the loop is gone.

    Either hook may be sync or async (see ``_maybe_await``). The constructed test
    object is handed back through ``result`` so the caller can evaluate it after
    shutdown, exactly as ``main`` does with ``_main``.

    Failure precedence (single most important guarantee): a construction / hook /
    startup error is *captured*, not allowed to propagate straight out, so ordered
    teardown always runs and, afterwards, exactly one failure is raised in strict
    precedence order --

      1. the stored runtime fatal (the real mid-run/startup test failure),
      2. the hook / setup error,
      3. the teardown error.

    Capturing is what upholds the fatal-error guarantee: without it a raising
    ``after_run`` would unwind past the fatal re-raise and mask the original
    runtime failure. The winner propagates; every lower-precedence error is
    surfaced in the log rather than silently lost.
    """
    runtime = new_runtime(asyncio.get_running_loop())
    hook_error = None       # construction / before_start / startup / after_run
    teardown_error = None   # ordered _shutdown failure
    try:
        test_object = factory()
        if test_object is not None:
            # Retain for result evaluation even if a later hook / startup raises
            # (teardown still runs in finally).
            result['test_object'] = test_object

            # Pre-run hook, inside the loop, before the startup state machine.
            if before_start is not None:
                await _maybe_await(before_start(test_object))

            # Drive the shared startup state machine, then await completion.
            await runtime.start_all()
            await runtime.run_async()

            # Post-run hook: runs while the loop is still open, before teardown
            # and before asyncio.run() closes the loop. A fatal mid-run error
            # resolves completion via stop() (run_async returns normally), so
            # this still runs -- matching a legacy script's post-run line
            # executing after reactor.run() returned.
            if after_run is not None:
                await _maybe_await(after_run(test_object))
    except Exception as exc:
        # Capture rather than propagate: teardown must still run, and a stored
        # runtime fatal must take precedence over this hook/setup error below.
        hook_error = exc
    finally:
        # Single ordered teardown, run on every exit path, then detach.
        try:
            await runtime._finish()
        except Exception as exc:
            teardown_error = exc
        finally:
            detach_runtime(runtime)

    # Apply explicit precedence over the three independent failure channels. The
    # highest-precedence one propagates; the rest are logged so nothing is lost.
    fatal = runtime._failure
    runtime._failure = None

    primary = None
    superseded = []
    for candidate in (fatal, hook_error, teardown_error):
        if candidate is None:
            continue
        if primary is None:
            primary = candidate
        else:
            superseded.append(candidate)

    for exc in superseded:
        LOGGER.error("error superseded by a higher-precedence failure",
                     exc_info=exc)
    if primary is not None:
        raise primary


def run_test_object(factory, before_start=None, after_run=None):
    """Construct, run, and tear down a ``run-test`` test object under asyncio.

    The shared entrypoint helper for migrated ``run-test`` scripts. It wraps
    ``asyncio.run()`` (which owns the loop end-to-end: creates it, and on exit
    cancels stragglers, shuts down async generators and the default executor,
    then closes it) around ``_run_object_async``. Because construction must now
    happen inside the running loop, it takes a **factory**, not an already-built
    object::

        test = run_test_object(YourTest)                    # constructor-only
        if not test.passed:
            return 1

    Scripts that did work around ``reactor.run()`` pass the corresponding hook,
    which runs *inside* the loop so nothing loop-dependent is left for after
    ``asyncio.run()`` returns::

        test = run_test_object(UdptlTest,
                               before_start=lambda t: t.start_asterisk(),
                               after_run=lambda t: t.stop_asterisk())

    Returns the constructed test object after shutdown (or ``None`` if the
    factory produced nothing) so callers keep their custom post-run assertions
    beyond ``test.passed``.
    """
    result = {}
    asyncio.run(_run_object_async(factory, before_start, after_run, result))
    return result.get('test_object')


def main(argv=None):
    """Main entry point for the test run

    Returns:
    0 on successful test run
    1 on any error
    """

    if argv is None:
        args = sys.argv

    if (len(args) < 2):
        LOGGER.error("test_runner requires the full path to the test "
                     "directory to execute")
        return 1
    test_directory = args[1]

    LOGGER.info("Starting test run for %s" % test_directory)
    test_config = load_test_config(test_directory)
    if test_config is None:
        return 1

    read_module_paths(test_config, test_directory)

    # Native asyncio entrypoint: asyncio.run() creates, drives, and tears the
    # loop down (cancels stragglers, shuts down async gens *and* the default
    # executor used by callInThread, then closes the loop).
    result = {}
    asyncio.run(_main(test_directory, test_config, result))

    test_object = result.get('test_object')
    if test_object is None:
        return 1

    LOGGER.info("Test run for %s completed with result %s" %
                (test_directory, str(test_object.passed)))
    if test_object.evaluate_results():
        return 0

    return 1


if __name__ == '__main__':
    sys.exit(main() or 0)
