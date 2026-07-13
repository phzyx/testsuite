"""Module for SIPp based tests.

This module provides classes that manipulate SIPp.

Copyright (C) 2010, Digium, Inc.
Russell Bryant <russell@digium.com>

This program is free software, distributed under the terms of
the GNU General Public License Version 2.
"""

import asyncio
import logging
from . import test_suite_utils

from abc import ABCMeta, abstractmethod
from asterisk.aio import error
from asterisk.aio.runtime import current_runtime
from asterisk.aio import ProcessProtocol
from .test_case import TestCase
from .utils_socket import get_available_port
from .test_runner import load_and_parse_module
from .pluggable_registry import PLUGGABLE_EVENT_REGISTRY,\
    PLUGGABLE_ACTION_REGISTRY


LOGGER = logging.getLogger(__name__)


class ScenarioGenerator(object):
    """Scenario Generators provide a generator function for creating scenario
    sets for use by SIPpTestCase"""
    __metaclass__ = ABCMeta

    @abstractmethod
    def generator(self):
        pass


class ConfigScenarioGenerator(ScenarioGenerator):
    """Default SIPpTestCase Scenario Generator. The generator is initialized
    with a list of scenarios provided by a list of dictionary objects.
    """

    def __init__(self, test_iterations):
        self.test_iterations = test_iterations

    def generator(self):
        """Generatess scenarios sets by pulling them from the full scenario
        set list one at a time.
        """
        for defined_scenario_set in self.test_iterations:
            yield defined_scenario_set
        return


class SIPpTestCase(TestCase):
    """A SIPp test object for the pluggable framework.

    In addition to the 'normal' entry points that TestCase defines for
    modules to attach themselves, this Test Object defines the following:

    register_scenario_started_observer - add a callback function that will be
        called every time a SIPp Scenario starts

    register_scenario_stopped_observer - add a callback function that will be
        called every time a SIPp Scenario stops

    register_intermediate_observer - add a callback function that will be called
        between sets of SIPp Scenarios run in parallel

    register_final_observer - add a callback function that will be called when
        all SIPp Scenarios have finished, but before the reactor is stopped
    """

    def __init__(self, test_path, test_config):
        """Constructor

        Keyword Arguments:
        test_path path to the location of the test directory
        test_config yaml loaded object containing config information
        """
        super(SIPpTestCase, self).__init__(test_path, test_config=test_config)

        if not test_config:
            raise ValueError("SIPpTestObject requires a test config")

        self._scenario_started_observers = []
        self._scenario_stopped_observers = []
        self._intermediate_observers = []
        self._final_observers = []
        self._test_config = test_config
        self._current_test = 0
        self._connected_amis = 0
        self.scenarios = []
        # These call backs are designed to give the sipp module a callback method
        # similar to the ami connect method. This is for tests that have sipp
        # scenarios driven by events generated from the test that also require
        # custom modules.
        self.start_CB_invoked = False
        self.start_callback_module = test_config.get('start_callback_module')
        self.start_callback_method = test_config.get('start_callback_method')
        self.stop_CB_invoked = False
        self.stop_callback_module = test_config.get('stop_callback_module')
        self.stop_callback_method = test_config.get('stop_callback_method')


        self.asterisk_instances = test_config.get('asterisk-instances') or 1
        self.connect_ami = test_config.get('connect-ami') or False

        test_iterations = test_config.get('test-iterations')
        if test_iterations:
            self.scenario_generator = ConfigScenarioGenerator(test_iterations)
        else:
            # Scenario Generator should be provided by a pluggable module.
            self.scenario_generator = None

        if 'fail-on-any' in self._test_config:
            self._fail_on_any = self._test_config['fail-on-any']
        else:
            self._fail_on_any = True

        if 'stop-after-scenarios' in self._test_config:
            self._stop_after_scenarios = self._test_config['stop-after-scenarios']
        else:
            self._stop_after_scenarios = True

        self.register_intermediate_obverver(self._handle_scenario_finished)
        self.create_asterisk(count=self.asterisk_instances, test_config=test_config)

    def on_reactor_timeout(self):
        """Create a failure token when the test times out"""
        self.create_fail_token("Reactor timed out. Test Failed.")

    def register_scenario_generator(self, generator):
        """Register a scenario generator object instead of using
        ConfigScenarioGenerator

        generator should be a concrete implementation of ScenarioGenerator
        """
        self.scenario_generator = generator

    def run(self):
        """Override of the run method.

        Create an AMI factory in case anyone wants it
        """
        super(SIPpTestCase, self).run()

        for a in range(1, self.asterisk_instances):
            self.ast[a-1].cli_exec('sip set debug on')
            self.ast[a-1].cli_exec('pjsip set logger on')

        LOGGER.info("creating ami factory")
        if not isinstance(self.connect_ami, dict):
            self.create_ami_factory(count=self.asterisk_instances)
        else:
            self.create_ami_factory(**self.connect_ami)

    def stop_asterisk(self):
        """Kill any remaining SIPp scenarios"""
        for scenario in self.scenarios:
            for instance in scenario:
                if not instance.exited:
                    LOGGER.info("Forcibly killing %s" % instance.name)
                    instance.kill()

    def ami_connect(self, ami):
        """Handler for the AMI connect event"""
        super(SIPpTestCase, self).ami_connect(ami)

        # keep track of number of connected amis and only start once they
        # are all connected
        self._connected_amis +=1
        if self._connected_amis == self.asterisk_instances:
            self._execute_test()

    def ami_reconnect(self, ami):
        """Handler for the AMI reconnect event"""
        super(SIPpTestCase, self).ami_reconnect(ami)

        for a in range(1, self.asterisk_instances):
            self.ast[a-1].cli_exec('sip set debug on')
            self.ast[a-1].cli_exec('pjsip set logger on')

    def register_scenario_started_observer(self, observer):
        """Register a function to be called when a SIPp scenario starts.

        Keyword Arguments:
        observer   The function to be called. The function should take in a
                   single parameter, the SIPpScenario object that started.
        """
        self._scenario_started_observers.append(observer)

    def register_scenario_stopped_observer(self, observer):
        """Register a function to be called when a SIPp scenario stops.

        Keyword Arguments:
        observer    The function to be called. The function should take in a
                    single parameter, the SIPpScenario object that stopped.
        """
        self._scenario_stopped_observers.append(observer)

    def register_intermediate_obverver(self, observer):
        """Register a function to be called in between SIPp scenarios.

        Keywords:
        observer    The function to be called.  The function should take in a
                    single parameter, a list of tuples.  Each tuple will be
                    (success, result), where success is the pass/fail status of
                    the SIPpScenario, and result is the SIPpScenario object.
        """
        self._intermediate_observers.append(observer)

    def register_final_observer(self, observer):
        """Register a final observer, called when all SIPp scenarios are done
        in a given sequence

        Keyword Arguments:
        observer    The function to be called. The function should take in two
                    parameters: an integer value indicating which test iteration
                    is being executed, and this object.
        """
        self._final_observers.append(observer)

    def _handle_scenario_finished(self, result):
        """Handle whether or not a scenario finished successfully"""
        for (success, scenario) in result:
            if (success and scenario.passed):
                LOGGER.info("Scenario %s passed" % (scenario.name))
                self.set_passed(True)
            else:
                # Note: the SIPpSequence/Scenario objects will auto fail this
                # test if configured to do so.  Merely report pass/fail here.
                LOGGER.warning("Scenario %s failed" % (scenario.name))
                self.set_passed(False)

    def _execute_test(self):
        """Execute the next test"""

        def _final_callback(sequence):
            """Call the final observers, then optionally stop the reactor"""
            for observer in self._final_observers:
                observer(self._current_test, self)
            if self._stop_after_scenarios:
                LOGGER.info("All SIPp Scenarios executed; stopping reactor")
                self.stop_reactor()

        for defined_scenario_set in self.scenario_generator.generator():
            scenario_set = defined_scenario_set['scenarios']
            # Build a list of SIPpScenario objects to run in parallel from
            # each set of scenarios in the YAML config
            sipp_scenarios = []
            for scenario in scenario_set:
                if ("coordinated-sender" in scenario and
                        "coordinated-receiver" in scenario):
                    sipp_scenarios.append(CoordinatedScenario(self.test_name,
                                                              scenario))
                else:
                    ordered_args = scenario.get('ordered-args') or []
                    target = scenario.get('target') or '127.0.0.1'
                    sipp_scenarios.append(SIPpScenario(self.test_name,
                                                       scenario['key-args'],
                                                       ordered_args,
                                                       target=target))
            self.scenarios.append(sipp_scenarios)

        if len(self.scenarios) == 0:
            LOGGER.error("No scenarios registered. Test Failed.")
            self.set_passed(False)
            self.stop_reactor()
            return

        sipp_sequence = SIPpScenarioSequence(self,
                                             self.scenarios,
                                             self._fail_on_any,
                                             self._intermediate_callback_fn,
                                             _final_callback,
                                             self._stop_after_scenarios)
        sipp_sequence.register_scenario_start_callback(self._scenario_start_callback_fn)
        sipp_sequence.register_scenario_stop_callback(self._scenario_stop_callback_fn)
        current_runtime().create_task(sipp_sequence.execute())

    def _intermediate_callback_fn(self, result):
        """Notify observers between SIPp iterations"""
        for observer in self._intermediate_observers:
            observer(result)
        return result

    def _scenario_start_callback_fn(self, result):
        """Notify observers that the scenario has started"""

        def __run_callback(result):
            """Do the notification"""
            for observer in self._scenario_started_observers:
                observer(result)
            return result

        # Allow some time for the SIPp process to come up
        current_runtime().callLater(.25, __run_callback, result)
        current_runtime().callLater(1, self.do_start_callback)

    def _scenario_stop_callback_fn(self, result):
        """Notify observers that the scenario has stopped"""
        for observer in self._scenario_stopped_observers:
            observer(result)
        self.do_stop_callback()
        return result

    # The start call back should be called with a delay as individual sipp scenarios need time
    # to be ready to receive messages and this callback executes immediately.
    def do_start_callback(self):
        """Call the configured callback module/method"""
        if self.start_callback_module is None or self.start_callback_method is None or self.start_CB_invoked is True:
            return
        self.start_CB_invoked = True
        callback_method = load_and_parse_module(self.start_callback_module + '.' + self.start_callback_method)
        callback_method(self, None)

    def do_stop_callback(self):
        """Call the configured callback module/method"""
        if self.stop_callback_module is None or self.stop_callback_method is None or self.stop_CB_invoked is True:
            return
        self.stop_CB_invoked = True
        callback_method = load_and_parse_module(self.stop_callback_module + '.' + self.stop_callback_method)
        callback_method(self, None)

class SIPpAMIActionTestCase(SIPpTestCase):
    """SIPpTestCase that also executes an AMI action"""
    def __init__(self, test_path, test_config):
        """Constructor

        Keyword Arguments:
        test_path path to the location of the test directory
        test_config yaml loaded object containing config information
        """

        super(SIPpAMIActionTestCase, self).__init__(test_path,
                                                    test_config=test_config)

        self.ami_token = self.create_fail_token("Remove token when AMI "
                                                "command is successful")

        self.ami_args = test_config['ami-action']['args']
        self.ami_delay = test_config['ami-action'].get('delay', 0)
        self.run_after_scenarios = test_config['ami-action'].get(
            'run-after-scenarios', False
        )
        if self.run_after_scenarios:
            self.register_final_observer(self.scenarios_complete)

    def on_reactor_timeout(self):
        """Create a failure token when the test times out"""
        self.create_fail_token("Reactor timed out. Test Failed.")

    def remove_token_on_success(self, message, expected='Success'):
        """Remove the failure token for AMI message if it didn't fail"""
        if type(message) is dict and message['response'] != expected:
            return
        self.remove_fail_token(self.ami_token)

    def run_ami_action(self):
        def _ami_action():
            """Send the AMI action"""
            LOGGER.info("Sending Action: %s" % self.ami_args)
            ami_out = self.ami.sendDeferred(self.ami_args)
            ami_out.addCallback(self.ami.errorUnlessResponse)
            ami_out.addCallback(self.remove_token_on_success)

        current_runtime().callLater(self.ami_delay, _ami_action)

    def ami_connect(self, ami):
        """Handle the AMI connect event"""
        super(SIPpAMIActionTestCase, self).ami_connect(ami)
        self.ami = ami

        if not self.run_after_scenarios:
            self.run_ami_action()

    def scenarios_complete(self, test, test_object):
        LOGGER.info("Scenarios complete, running AMI action")
        self.run_ami_action()


class SIPpScenarioSequence(object):
    """Execute a sequence of SIPp Scenarios in sequence.

    This class manages the execution of multiple SIPpScenarios in sequence.
    """

    def __init__(self, test_case, sipp_scenarios=None,
                 fail_on_any=False,
                 intermediate_cb_fn=None,
                 final_callback=None,
                 stop_on_done=True):
        """Create a new sequence of scenarios

        Keyword Arguments:
        test_case           The TestCase derived object to pass to the
                            SIPpScenario objects
        sipp_scenarios      A list of SIPpScenario objects to execute, or a list
                            of lists of SIPpScenario objects to execute in
                            parallel
        fail_on_any         If any scenario fails, stop the reactor and kill the
                            test.
        intermediate_cb_fn  A callback function invoked with a list of
                            (success, result) tuples for each scenario that was
                            executed, where the result object is the
                            SIPpScenario. This will be called for each set of
                            SIPpScenario objects.
        final_callback      A callable invoked with this object when all
                            scenarios have executed, but before the reactor is
                            stopped.
        stop_on_done        Stop the test_case object when all scenarios have
                            executed. Defaults to True.
        """
        self._sipp_scenarios = sipp_scenarios or []
        self._test_case = test_case
        self._fail_on_any = fail_on_any
        self._test_counter = 0
        self._intermediate_cb_fn = intermediate_cb_fn
        self._final_callback = final_callback
        self._scenario_start_fn = None
        self._scenario_stop_fn = None
        self._stop_on_done = stop_on_done

    def register_scenario_start_callback(self, callback_fn):
        """Register a callback function that will be called on the start of
        every SIPpScenario

        Keyword Arguments:
        callback_fn A function that takes in a single parameter. That parameter
                    will be the SIPpScenario object that was just started
        """
        self._scenario_start_fn = callback_fn

    def register_scenario_stop_callback(self, callback_fn):
        """Register a callback function that will be called at the end of
        every SIPpScenario

        Keyword Arguments:
        callback_fn A function that acts as a deferred callback.  It takes in
        a single parameter that will be the SIPpScenario object.
        """
        self._scenario_stop_fn = callback_fn

    def register_scenario(self, sipp_scenario):
        """Register a new scenario with the sequence

        Registers a SIPpScenario object with the sequence of scenarios to execute

        Keyword Arguments:
        sipp_scenario The SIPpScenario object to execute
        """
        self._sipp_scenarios.append(sipp_scenario)

    async def __run_scenario(self, coro):
        """Await a single scenario, then apply the stop callback to its result.

        This preserves the original ordering where the stop callback was added
        to each scenario's Deferred (firing when the scenario stops).
        """
        result = await coro
        if self._scenario_stop_fn:
            result = self._scenario_stop_fn(result)
        return result

    async def execute(self):
        """Execute the tests in sequence"""

        while self._test_counter < len(self._sipp_scenarios):
            scenarios = self._sipp_scenarios[self._test_counter]
            # Turn the scenario into a list if all we got was a single scenario
            # to execute
            if type(scenarios) is not list:
                scenarios = [scenarios]

            awaitables = []
            for scenario in scenarios:
                # If we fail on any, let the SIPp scenario handle it by passing
                # it the TestCase object
                if self._fail_on_any:
                    coro = scenario.run(self._test_case)
                else:
                    coro = scenario.run(None)
                # Order here is slightly important.  Wire up the stop callback
                # (fires when the scenario stops) before notifying start
                # observers that we're started.
                awaitables.append(self.__run_scenario(coro))
                if self._scenario_start_fn:
                    self._scenario_start_fn(scenario)

            # DeferredList in the original waited for every child regardless of
            # errors; return_exceptions=True preserves that (no fail-fast).
            results = await asyncio.gather(*awaitables, return_exceptions=True)
            result = [(not isinstance(r, BaseException), r) for r in results]

            if self._intermediate_cb_fn:
                self._intermediate_cb_fn(result)

            # Only evaluate for failure if we're responsible for failing the
            # test case - otherwise the SIPpScenario will do it for us
            for (success, scenario_or_exc) in result:
                if self._fail_on_any:
                    continue
                # On the error path (return_exceptions=True) scenario_or_exc is
                # the raised exception, not a SIPpScenario -- do not touch its
                # attributes.
                if not success:
                    LOGGER.warning("SIPp Scenario failed: %s" % scenario_or_exc)
                    self._test_case.set_passed(False)
                elif not scenario_or_exc.passed:
                    LOGGER.warning("SIPp Scenario %s Failed" %
                                   scenario_or_exc.name)
                    self._test_case.set_passed(False)
            self._test_counter += 1

        if self._final_callback:
            self._final_callback(self)
        if self._stop_on_done:
            self._test_case.stop_reactor()


class SIPpProtocol(ProcessProtocol):
    """Class that manages a single SIPp instance"""

    def __init__(self, name, stop_future, start_future=None):
        """Create a SIPp process

        Keyword Arguments:
        name            The name of the scenario
        stop_future     An asyncio Future that will be resolved when the
                        process has exited
        start_future    An asyncio Future that will be resolved when the
                        process connection is made
        """
        self._name = name
        self.output = ""
        self.exitcode = 0
        self.exited = False
        self.stderr = []
        self._stop_future = stop_future
        self._start_future = start_future

    def kill(self):
        """Kill the SIPp scenario"""
        if not self.exited:
            LOGGER.warn("Killing SIPp Scenario %s" % self._name)
            try:
                self.transport.signalProcess('KILL')
            except error.ProcessExitedAlready:
                LOGGER.warn("Process for scenario %s exited" % self._name)

    def outReceived(self, data):
        """Override of ProcessProtocol.outReceived"""
        LOGGER.debug("Received from SIPp scenario %s:\n %s" % (self._name,
                        data.decode('utf-8', 'ignore')))
        self.output += data.decode('utf-8', 'ignore')

    def connectionMade(self):
        """Override of ProcessProtocol.connectionMade"""
        LOGGER.debug("Connection made to SIPp scenario %s" % (self._name))
        if self._start_future and not self._start_future.done():
            self._start_future.set_result(self)

    def errReceived(self, data):
        """Override of ProcessProtocol.errReceived"""
        # SIPp will send some 'normal' messages to stderr. Buffer them so we
        # can output them later if we want
        self.stderr.append(data)

    def processEnded(self, reason):
        """Override of ProcessProtocol.processEnded"""
        if self.exited:
            return reason

        self.exited = True
        message = ""
        if reason.value and reason.value.exitCode:
            message = ("SIPp scenario %s ended with code %d" %
                       (self._name, reason.value.exitCode,))
            self.exitcode = reason.value.exitCode
            for msg in self.stderr:
                LOGGER.warn(msg)
        else:
            message = "SIPp scenario %s ended" % self._name
        if not self._stop_future.done():
            self._stop_future.set_result(self)
        LOGGER.info(message)
        return reason


class SIPpScenario(object):
    """A SIPp based scenario for the Asterisk testsuite.

    Unlike SIPpTest, SIPpScenario does not attempt to manage the Asterisk
    instance. Instead, it will launch a SIPp scenario, assuming that there is an
    instance of Asterisk already in existence to handle the SIP messages. This
    is useful when a SIPp scenario must be integrated with a more complex test
    (using the TestCase class, for example)
    """
    def __init__(self, test_dir, scenario, positional_args=(),
                 target='127.0.0.1'):
        """
        Keyword Arguments:
        test_dir        The path to the directory containing the run-test file.

        scenario        A SIPp scenario to execute. The scenario should
                        be a dictionary with the key 'scenario' being the
                        filename of the SIPp scenario. Any other key-value pairs
                        are treated as arguments to SIPp. For example, specify
                        '-timeout' : '60s' to set the timeout option to SIPp to
                        60 seconds. If a parameter specified is also one
                        specified by default, the value provided will be used.

                        The default SIPp parameters include:
            -p <port>    - Unless otherwise specified, the port number will
                           be 5060 + <scenario list index, starting at 1>.
                           So, the first SIPp sceario will use port 5061.
            -m 1         - Stop the test after 1 'call' is processed.
            -i 127.0.0.1 - Use this as the local IP address for the Contact
                           headers, Via headers, etc.
            -timeout 20s - Set a global test timeout of 20 seconds.

        positional_args Certain SIPp parameters can be specified multiple
                        times, or take multiple arguments. Supply those through
                        this iterable.

                        The canonical example being -key:
                            ('-key', 'extra_via_param', ';rport',
                             '-key', 'user_addr', 'sip:myname@myhost')
        target          Overrides the default target address (127.0.0.1) of the
                        SIPp scenario. Be sure to specify IPv6 addresses in
                        brackets ([::1])
        """
        self.scenario = scenario
        self.name = scenario['scenario']
        # don't allow caller to mangle his own list
        self.positional_args = tuple(positional_args)
        self.test_dir = test_dir
        self.default_port = 5061
        self.sipp = test_suite_utils.which("sipp")
        self.passed = False
        self.exited = False
        self.result = None
        self._process = None
        self.target = target
        self._test_case = None
        if not self.sipp:
            raise ValueError("SIPpTestObject requires that sipp is installed")

    def kill(self):
        """Kill the executing SIPp scenario"""
        if self._process:
            self._process.kill()
        return

    async def run(self, test_case=None, start_future=None):
        """Execute a SIPp scenario

        Execute the SIPp scenario that was passed to this object

        Keyword Arguments:
        test_case   If not None, the scenario will automatically evaluate its
                    pass/fail status at the end of the run. In the event of a
                    failure, it will fail the test case scenario and call
                    stop_reactor.
        start_future An optional asyncio Future resolved when the SIPp process
                    connection is made.

        Returns:
        This SIPpScenario, once the SIPp process has exited.
        """

        self.result = None
        sipp_args = [
            self.sipp, self.target,
            '-sf',
            '%s/sipp/%s' % (self.test_dir, self.scenario['scenario']),
            '-nostdin',
            '-skip_rlimit',
        ]

        default_args = {
            '-p': str(self.default_port),
            '-m': '1',
            '-i': '127.0.0.1',
            '-timeout': '20s'
        }

        # Override and extend defaults
        default_args.update(self.scenario)
        del default_args['scenario']

        # correct file paths to be relative to the test's sipp directory
        correctable_paths = { "-slave_cfg", "-inf", "-oocsf", "-tls_cert", "-tls_key", "-tls_ca", "-tls_crl" }
        for defarg in default_args:
            if defarg in correctable_paths:
                default_args[defarg] = ('%s/sipp/%s' % (
                    self.test_dir, default_args[defarg]))

        if '-mp' not in default_args:
            # Current SIPp correctly chooses an available port for audio, but
            # unfortunately it then attempts to bind to the audio port + n for
            # things like rtcp and video without first checking if those other
            # ports are unused (https://github.com/SIPp/sipp/issues/276).
            #
            # So as a work around, if not given, we'll specify the media port
            # ourselves, and make sure all associated ports are available.
            #
            # num = 4 = ports for audio rtp/rtcp and video rtp/rtcp
            default_args['-mp'] = str(get_available_port(
                default_args.get('-i'), num=4))

        for (key, val) in default_args.items():
            sipp_args.extend([key, val])

        # The majority of tests do no need re-transmissions enabled. As a
        # matter of fact most tests will fail if sipp re-transmits a message.
        # By default disable all re-transmissions in a scenario unless
        # explicitly told to allow them.
        if '-enable-retrans' in self.positional_args:
            sipp_args.extend(
                [i for i in self.positional_args if i != '-enable-retrans'])
        else:
            sipp_args.append('-nr')
            sipp_args.extend(self.positional_args)

        LOGGER.info("Executing SIPp scenario: %s" % self.scenario['scenario'])
        LOGGER.debug(sipp_args)

        stop_future = asyncio.get_event_loop().create_future()

        self._process = SIPpProtocol(self.scenario['scenario'], stop_future,
                                     start_future)
        current_runtime().spawnProcess(self._process,
                             sipp_args[0],
                             sipp_args,
                             {"TERM": "vt100", },
                             None,
                             None)

        # Wait for the process to exit
        result = await stop_future

        # Bookkeeping formerly done in __scenario_callback
        self.exited = True
        self.result = result
        if (result.exitcode == 0):
            self.passed = True
            LOGGER.info("SIPp Scenario %s Exited" %
                        (self.scenario['scenario']))
        else:
            LOGGER.warning("SIPp Scenario %s Failed [%d]" %
                           (self.scenario['scenario'], result.exitcode))

        # If a test case was injected, auto-fail it on scenario failure
        # (formerly __evaluate_scenario_results)
        if test_case:
            self._test_case = test_case
            if not self.passed:
                LOGGER.warning("SIPp Scenario %s Failed" %
                               self.scenario['scenario'])
                self._test_case.passed = False
                self._test_case.stop_reactor()

        return self


class CoordinatedScenario(object):
    """A SIPp based scenario for the Asterisk testsuite that handles basic 3PCC
    coordination.

    CoordinatedScenario wraps and builds on the capabilities of SIPpScenario to
    allow a pair of 3PCC scenarios to launch at the appropriate times to
    function correctly. The sender scenario will not be started until the
    receiver scenario comes up and opens its 3PCC port.
    """

    next_3pcc_port = 5080

    def __init__(self, test_dir, coordinated_config):
        """
        Keyword Arguments:
        test_dir            The path to the directory containing the run-test
                            file.

        coordinated_config  The configuration to use for the two coordinated
                            SIPp scenarios. The SIPpScenario configurations are
                            stored under keys "coordinated-sender" and
                            "coordinated-receiver". These configs should not
                            explicitly set 3PCC configurations as it will be
                            handled automatically.
        """

        self.coordination_port = CoordinatedScenario.next_3pcc_port
        CoordinatedScenario.next_3pcc_port += 1
        coordination_address = '127.0.0.1:%s' % (self.coordination_port)

        receiver_config = coordinated_config["coordinated-receiver"]
        receiver_config['key-args']['-3pcc'] = coordination_address
        target = receiver_config.get('target', '127.0.0.1')
        self.receiver = SIPpScenario(test_dir,
                                     receiver_config['key-args'],
                                     receiver_config.get('ordered-args', []),
                                     target=target)

        sender_config = coordinated_config["coordinated-sender"]
        sender_config['key-args']['-3pcc'] = coordination_address
        target = sender_config.get('target', '127.0.0.1')
        self.sender = SIPpScenario(test_dir,
                                   sender_config['key-args'],
                                   sender_config.get('ordered-args', []),
                                   target=target)

        self.exited = False
        self.passed = False
        self.name = "Coordinated Scenario %d" % self.coordination_port

    def kill(self):
        """Kill the executing SIPp scenario"""
        self.sender.kill()
        self.receiver.kill()
        return

    async def run(self, test_case=None):
        """Execute a coordinated SIPp scenario

        Execute the set of SIPp scenarios passed to this object

        Keyword Arguments:
        test_case  If not None, the scenario will automatically evaluate its
                   pass/fail status at the end of the run. In the event of a
                   failure, it will fail the test case scenario and call
                   stop_reactor.

        Returns:
        This CoordinatedScenario, once both scenarios have exited.
        """

        LOGGER.info("Executing coordinated SIPp scenario %d" %
                    (self.coordination_port))

        # The sender must not be started until the receiver has come up and
        # opened its 3PCC port. Start the receiver, wait for its connection to
        # be made, then start the sender and wait for both to finish.
        receiver_start_future = asyncio.get_event_loop().create_future()
        receiver_task = asyncio.ensure_future(
            self.receiver.run(test_case, receiver_start_future))
        await receiver_start_future

        sender_task = asyncio.ensure_future(self.sender.run(test_case))
        await asyncio.gather(receiver_task, sender_task)

        # Bookkeeping formerly done in __scenario_callback
        if self.sender.exited and self.receiver.exited:
            self.exited = True
            if self.sender.passed and self.receiver.passed:
                self.passed = True

            if self.passed:
                LOGGER.info("Coordinated SIPp Scenario %d Exited" %
                            (self.coordination_port))
            else:
                LOGGER.warning("Coordinated SIPp Scenario %d Failed" %
                               (self.coordination_port))
        return self


class SIPpTest(TestCase):
    """A SIPp based test for the Asterisk testsuite.

    This is a common implementation of a test that uses 1 or more SIPp
    scenarios.  The result code of each SIPp instance is used to determine
    whether or not the test passed.

    This class currently uses a single Asterisk instance and runs all of the
    scenarios against it.  If any configuration needs to be provided to this
    Asterisk instance, it is expected to be in the configs/ast1/ direcotry
    under the test_dir provided to the constructor.  This directory was
    chosen based on the convention that has been established in the testsuite
    for the location of configuration for a test.
    """

    def __init__(self, working_dir, test_dir, scenarios, test_config=None):
        """Constructor

        Keyword Arguments:
        working_dir Deprecated. No longer used.
        test_dir    The path to the directory containing the run-test file.
        scenarios   A list of SIPp scenarios. This class expects these
                    to exist in the sipp directory under test_dir. The list
                    must be constructed as a list of dictionaries. Each
                    dictionary must have the key 'scenario' with the value being
                    the filename of the SIPp scenario. Any other key-value pairs
                    are treated as arguments to SIPp. For example, specity
                    '-timeout' : '60s' to set the timeout option to SIPp to 60
                    seconds. If a parameter specified is also one specified by
                    default, the value provided will be used.

                    The default SIPp parameters include:
                -p <port>    - Unless otherwise specified, the port number will
                               be 5060 + <scenario list index, starting at 1>.
                               So, the first SIPp sceario will use port 5061.
                -m 1         - Stop the test after 1 'call' is processed.
                -i 127.0.0.1 - Use this as the local IP address for the Contact
                               headers, Via headers, etc.
                -timeout 20s - Set a global test timeout of 20 seconds.
        """
        super(SIPpTest, self).__init__()
        self.test_dir = test_dir
        self.scenarios = scenarios
        self.result = []
        self.create_asterisk(test_config=test_config)
        self._scenario_objects = []

    def stop_asterisk(self):
        """Kill any scenarios still in existence"""
        for scenario in self._scenario_objects:
            if not scenario.exited:
                LOGGER.warn("SIPp Scenario %s has not exited; killing" %
                            scenario.name)
                scenario.kill()

    def run(self):
        """Run the test.

        Returns 0 for success, 1 for failure.
        """
        super(SIPpTest, self).run()

        i = 0
        scenarios = []
        for scenario_def in self.scenarios:
            default_port = 5060 + i + 1
            i += 1
            if '-p' not in scenario_def:
                scenario_def['-p'] = str(default_port)
            scenario = SIPpScenario(self.test_dir, scenario_def)
            self._scenario_objects.append(scenario)
            scenarios.append(scenario)

        current_runtime().create_task(self.__evaluate_scenarios(scenarios))

    async def __evaluate_scenarios(self, scenarios):
        """Run all scenarios and set the aggregate pass/fail status"""
        # DeferredList in the original waited for every child regardless of
        # errors; return_exceptions=True preserves that (no fail-fast).
        results = await asyncio.gather(
            *[scenario.run(self) for scenario in scenarios],
            return_exceptions=True)
        for result in results:
            if not isinstance(result, BaseException):
                self.result.append(result.passed)
        self.passed = (self.result.count(False) == 0)
        self.stop_reactor()


class SIPpStartEventModule(object):
    """An event module that triggers when SIPp scenario(s) starts.

    Optional options:
      count - trigger event after 'count' scenarios
      name - trigger event after matching 'name' to a scenario's name

    If no options are specified then this event is triggered once the first
    scenario has been started. If both options are set then the event is
    triggered when 'count' scenarios named 'name' have started.
    """

    def __init__(self, test_object, triggered_callback, config):
        """Setup the test start observer"""

        if not isinstance(test_object, SIPpTestCase):
            raise TypeError("Test case must be of type SIPpTestCase")

        self.test_object = test_object
        self.triggered_callback = triggered_callback

        self.count = config and config.get('count', 0) or 0
        self.name = config and config.get('name') or None

        test_object.register_scenario_started_observer(self.handle_start)

    def handle_start(self, scenario):
        """Notify the event-action mapper that the test has started."""

        if self.count > 0:
            self.count -= 1

        if self.count == 0 and (not self.name or self.name == scenario.name):
            self.triggered_callback(self, scenario)


PLUGGABLE_EVENT_REGISTRY.register("sipp-start", SIPpStartEventModule)


class SIPpActionModule(object):
    """An action module that initiates SIPp scenarios."""

    def __init__(self, test_object, config):
        """Initialize SIPp action module"""

        scenarios = config.get('scenarios')
        if not scenarios:
            # This is more of a fix the test error. Either this action
            # is not needed, or a scenario needs to be properly added
            LOGGER.error("No registered SIPp scenarios (action does nothing).")
            test_object.set_passed(False)
            test_object.stop_reactor()
            return

        self.sequence = SIPpScenarioSequence(test_object,
            fail_on_any=config.get('fail-on-any', True),
            stop_on_done=config.get('stop-after-scenarios', True))

        for s in scenarios:
            if ('coordinated-sender' in s and 'coordinated-receiver' in s):
                self.sequence.register_scenario(CoordinatedScenario(
                    test_object.test_name, s))
            else:
                self.sequence.register_scenario(SIPpScenario(
                    test_object.test_name, s['key-args'],
                    s.get('ordered-args') or [],
                    s.get('target') or '127.0.0.1'))

    def run(self, triggered_by, source, extra):
        """Execute specified SIPp scenarios"""

        current_runtime().create_task(self.sequence.execute())


PLUGGABLE_ACTION_REGISTRY.register("sipp", SIPpActionModule)
