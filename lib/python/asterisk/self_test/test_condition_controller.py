#!/usr/bin/env python
"""Tests for the TestConditionController evaluation dispatch

Copyright (C) 2026, Digium, Inc.

This program is free software, distributed under the terms of
the GNU General Public License Version 2.
"""

from harness_shared import main, run_coroutine
import unittest
from asterisk.test_conditions import TestConditionController


class FailingCondition(object):
    """A minimal condition whose evaluate() raises.

    It implements just the surface the controller touches, plus the
    TestCondition-like methods observers rely on (get_status/pass_expected).
    """

    def __init__(self):
        self._status = 'Inconclusive'
        self.pass_expected = True
        self.failed_reason = None

    def check_build_options(self):
        return True

    def register_asterisk_instance(self, ast):
        pass

    def get_enabled(self):
        return True

    def get_name(self):
        return "FailingCondition"

    def get_status(self):
        return self._status

    def fail_check(self, reason=""):
        self._status = 'Failed'
        self.failed_reason = reason

    async def evaluate(self, related_test_condition=None):
        raise RuntimeError("boom")

    def __str__(self):
        return "FailingCondition"


class ControllerErrorPathTest(unittest.TestCase):
    """Regression: a condition that raises must be reported as a failed
    TestCondition, never as a raw exception handed to observers.
    """

    def test_evaluate_raises_marks_condition_failed(self):
        captured = []
        controller = TestConditionController(test_config=object())
        controller.register_observer(lambda cond: captured.append(cond), "")

        cond = FailingCondition()
        controller.register_post_test_condition(cond)

        # Must not raise: previously a raw exception was passed to observers,
        # which then blew up calling get_status() on it.
        run_coroutine(controller.evaluate_post_checks())

        # Observers received the TestCondition object (not the exception) and
        # the condition was marked Failed with the exception text as reason.
        self.assertTrue(captured)
        self.assertIs(captured[-1], cond)
        self.assertEqual(cond.get_status(), 'Failed')
        self.assertIn("boom", cond.failed_reason)


if __name__ == "__main__":
    main()
