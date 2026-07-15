#!/usr/bin/env python
"""AppTest action-sequencing regression tests

Copyright (C) 2026, Digium, Inc.

This program is free software, distributed under the terms of
the GNU General Public License Version 2.

These tests pin the contract of
``ApplicationEventInstance.execute_next_action``: actions are driven in
order, and a failed action must NOT advance to later actions.
"""

import sys
import asyncio
import logging
import unittest

sys.path.append('lib/python')

from harness_shared import main
from asterisk.aio.runtime import new_runtime, detach_runtime
from asterisk import apptest


class _RecordingTestObject(object):
    """Captures the completion callbacks execute_next_action fires."""

    def __init__(self):
        self.completed = False
        self.ended = False

    def event_instance_ran_actions(self):
        self.completed = True

    def end_scenario(self):
        self.ended = True


def _make_instance(test_object):
    """Build an ApplicationEventInstance without its AMI-bound constructor."""
    inst = apptest.ApplicationEventInstance.__new__(
        apptest.ApplicationEventInstance)
    inst.ran_actions = False
    inst.unexpected = False
    inst._ApplicationEventInstance__current_action = 0
    inst.channel_obj = object()
    inst.test_object = test_object
    return inst


class ActionSequencingTests(unittest.TestCase):
    """Regression coverage for execute_next_action sequencing."""

    def _drive(self, actions):
        """Drive execute_next_action to quiescence on a fresh runtime."""
        test_object = _RecordingTestObject()
        inst = _make_instance(test_object)
        rt = new_runtime()

        async def scenario():
            inst.execute_next_action(actions=list(actions))
            # Let any scheduled action tasks drain.
            for _ in range(50):
                await asyncio.sleep(0.01)
            rt.stop()

        rt.create_task(scenario())
        try:
            rt.run()
        finally:
            detach_runtime(rt)
        return test_object

    def test_all_passing_actions_complete(self):
        """A clean sequence runs every action and reports completion."""
        log = []

        async def act(channel):
            log.append('act')
            return channel

        test_object = self._drive([
            lambda c: act(c),   # coroutine action
            lambda c: None,     # synchronous (no-op) action
            lambda c: act(c),   # coroutine action
        ])

        self.assertEqual(log, ['act', 'act'])
        self.assertTrue(test_object.completed)
        self.assertFalse(test_object.ended)

    def test_failed_action_halts_sequence(self):
        """A failed action stops the sequence; later actions never run."""
        log = []

        async def good(channel):
            log.append('good')
            return channel

        async def boom(channel):
            log.append('boom')
            raise ValueError('action failed')

        async def after(channel):
            log.append('after')
            return channel

        # execute_next_action logs the failure via LOGGER.exception; silence
        # it so the expected traceback does not clutter the test output.
        logging.disable(logging.CRITICAL)
        try:
            test_object = self._drive([
                lambda c: good(c),
                lambda c: boom(c),
                lambda c: after(c),
            ])
        finally:
            logging.disable(logging.NOTSET)

        # The failing action ran, but nothing after it did.
        self.assertEqual(log, ['good', 'boom'])
        self.assertNotIn('after', log)
        # A halted sequence must not be reported as completed - otherwise a
        # failed action could masquerade as a passing scenario.
        self.assertFalse(test_object.completed)
        self.assertFalse(test_object.ended)


if __name__ == "__main__":
    main()
