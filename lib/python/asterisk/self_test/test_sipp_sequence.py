#!/usr/bin/env python
"""Tests for SIPpScenarioSequence error handling

Copyright (C) 2026, Digium, Inc.

This program is free software, distributed under the terms of
the GNU General Public License Version 2.
"""

from harness_shared import main, run_coroutine
import unittest
from asterisk.sipp import SIPpScenarioSequence


class FakeTestCase(object):
    """Minimal stand-in for the TestCase the sequence drives"""

    def __init__(self):
        self.passed = True
        self.stopped = False
        self.set_passed_calls = []

    def set_passed(self, value):
        self.set_passed_calls.append(value)

    def stop_reactor(self):
        self.stopped = True


class RaisingScenario(object):
    """A scenario whose run() raises"""

    def __init__(self):
        self.name = "RaisingScenario"
        self.passed = False

    async def run(self, test_case=None):
        raise RuntimeError("scenario boom")


class PassingScenario(object):
    """A scenario that runs to completion successfully"""

    def __init__(self):
        self.name = "PassingScenario"
        self.passed = True

    async def run(self, test_case=None):
        return self


class SIPpScenarioSequenceErrorTest(unittest.TestCase):
    """Regression: when a scenario's run() raises, execute() must treat the
    gathered exception as a failure -- never call attributes on it.
    """

    def test_execute_scenario_raises_no_attributeerror(self):
        tc = FakeTestCase()
        seq = SIPpScenarioSequence(tc, [[RaisingScenario()]],
                                   fail_on_any=False, stop_on_done=True)
        # Must not raise AttributeError
        run_coroutine(seq.execute())
        # The raising scenario is reported as a failure
        self.assertIn(False, tc.set_passed_calls)
        # The sequence still completes and stops the reactor
        self.assertTrue(tc.stopped)

    def test_execute_mixed_pass_and_raise(self):
        tc = FakeTestCase()
        seq = SIPpScenarioSequence(
            tc, [[PassingScenario(), RaisingScenario()]],
            fail_on_any=False, stop_on_done=True)
        # Must not raise AttributeError even with a passing scenario alongside
        run_coroutine(seq.execute())
        # The raising scenario still fails the test case
        self.assertIn(False, tc.set_passed_calls)
        self.assertTrue(tc.stopped)


if __name__ == "__main__":
    main()
