#!/usr/bin/env python
"""Test condition for verifying SIP dialogs

Copyright (C) 2011-2012, Digium, Inc.
Matt Jordan <mjordan@digium.com>

This program is free software, distributed under the terms of
the GNU General Public License Version 2.
"""

import asyncio
import logging
import logging.config

from .test_conditions import TestCondition

LOGGER = logging.getLogger(__name__)


class SipDialogTestCondition(TestCondition):
    """This class is a base class for the pre- and post-test condition
    classes that check for the existence of SIP dialogs in Asterisk. It provides
    common functionality for parsing out the results of the 'sip show objects'
    and 'sip show history' Asterisk commands
    """

    def __init__(self, test_config):
        """Constructor"""
        super(SipDialogTestCondition, self).__init__(test_config)
        # a dictionary of ast objects to a dictionary of SIP dialogs and a list
        # of their history
        self.dialogs_history = {}
        self._finished_deferred = None
        self.ast = None

    def _get_dialog_names(self, objects):
        """Get the names of dialogs"""
        in_objects = False
        object_list = objects.split('\n')
        dialog_names = []
        for obj in object_list:
            if "Dialog objects" in obj:
                in_objects = True
            if "name:" in obj and in_objects:
                dialog_names.append(obj[obj.find(":") + 1:].strip())
        return dialog_names

    async def get_sip_dialogs(self, ast):
        """Build the dialog history and objects for a particular Asterisk
        instance
        """
        def __store_history(result):
            """Store the results of sip show history"""
            # Get the Call ID from the result
            call_id = result.cli_cmd.replace("sip show history", "").strip()
            raw_history = result.output
            LOGGER.debug(result.output)
            if 'No such SIP Call ID' not in raw_history:
                # dialog got disposed before we could get its history; ignore
                lines = raw_history.split('\n')
                self.dialogs_history[self.ast.host][call_id] = lines

        self.dialogs_history[ast.host] = {}
        objects_result = await ast.cli_exec("sip show objects")
        LOGGER.debug(objects_result.output)
        dialog_names = self._get_dialog_names(objects_result.output)
        LOGGER.debug(dialog_names)
        if not dialog_names:
            LOGGER.debug("No SIP history found for Asterisk instance %s" %
                         ast.host)
            return ast

        async def __history(name):
            """Gather the history for a single SIP dialog"""
            LOGGER.debug("Retrieving history for SIP dialog %s" % name)
            __store_history(await ast.cli_exec("sip show history %s" % name))

        # Wait for every child regardless of errors; do not fail fast.
        await asyncio.gather(*[__history(name) for name in dialog_names],
                             return_exceptions=True)
        return ast


class SipDialogPreTestCondition(SipDialogTestCondition):
    """Check the pre-test conditions for SIP dialogs. This test simply
    checks that there are no SIP dialogs present before test execution.
    """

    def __init__(self, test_config):
        """Constructor"""
        super(SipDialogPreTestCondition, self).__init__(test_config)
        self._counter = 0
        self._finished_deferred = None

    async def evaluate(self, related_test_condition=None):
        """Evaluate the condition"""

        async def __process(instance):
            """Turn on history for an instance, then inspect its dialogs"""
            history_result = await instance.cli_exec("sip set history on")
            # Find the Asterisk instance that ran the command
            target = None
            for candidate in self.ast:
                if candidate.host == history_result.host:
                    target = candidate
                    break
            if target is None:
                LOGGER.warning("Unable to determine Asterisk instance from CLI "
                               "command run on host %s" % history_result.host)
                return

            await super(SipDialogPreTestCondition,
                        self).get_sip_dialogs(target)
            dialog_history = self.dialogs_history[target.host]
            if len(dialog_history) > 0:
                # If any dialogs are present before test execution, something
                # funny is going on
                super(SipDialogPreTestCondition, self).fail_check(
                    "%d dialogs were detected in Asterisk %s before test execution" %
                    (len(dialog_history), target.host))
            else:
                super(SipDialogPreTestCondition, self).pass_check()

        self._counter = 0
        # Turn on history and check for dialogs. The original fired each
        # instance's Deferred independently (no cross-instance fail-fast);
        # return_exceptions=True preserves that.
        await asyncio.gather(*[__process(ast) for ast in self.ast],
                             return_exceptions=True)
        return self


class SipDialogPostTestCondition(SipDialogTestCondition):
    """Check the post-test conditions for SIP dialogs.

    This test looks for any SIP dialogs still in existence. If it does not
    detect any, the test passes. If it does detect dialogs, it checks to make
    sure that the dialogs have been hungup and are scheduled for destruction.
    If those two conditions are met, the test passes; otherwise it fails.

    Note: as a future enhancement, implement an AMI command that will force
    garbage collection on the SIP dialogs.  We can then also check that the
    scheduler properly collects SIP dialogs as part of this test.
    """

    def __init__(self, test_config):
        """Constructor"""
        super(SipDialogPostTestCondition, self).__init__(test_config)

        self._counter = 0
        self._finished_deferred = None
        self.history_sequence = []
        if 'history_requirements' in test_config.config:
            self.history_sequence = test_config.config['history_requirements']

    async def evaluate(self, related_test_condition=None):
        """Evaluate the condition"""

        # Walk the instances one at a time, gathering and inspecting dialogs
        for counter in range(len(self.ast)):
            self._counter = counter
            await super(SipDialogPostTestCondition, self).get_sip_dialogs(
                self.ast[counter])

            history_requirements = {}
            dialogs_history = self.dialogs_history[self.ast[counter].host]
            if not dialogs_history:
                continue

            # Set up the history statements to look for in each dialog history
            for dialog_name in dialogs_history.keys():
                history_check = {}
                for h_seq in self.history_sequence:
                    history_check[h_seq] = False
                history_requirements[dialog_name] = history_check

            # Assume we pass the check.  This will be overriden if any history
            # check fails
            super(SipDialogPostTestCondition, self).pass_check()
            for dialog, history in dialogs_history.items():
                scheduled = False
                for h_seq in history:
                    if "SchedDestroy" in h_seq:
                        scheduled = True
                    for req in history_requirements[dialog].keys():
                        if req in h_seq:
                            history_requirements[dialog][req] = True
                if not scheduled:
                    super(SipDialogPostTestCondition, self).fail_check(
                        "Dialog %s in Asterisk instance %s not scheduled for "
                        "destruction" % (dialog, self.ast[counter].host))
                for req in history_requirements[dialog].keys():
                    if history_requirements[dialog][req] is False:
                        super(SipDialogPostTestCondition, self).fail_check(
                            "Dialog %s in Asterisk instance %s did not have "
                            "required step in history: %s" % (
                                dialog,
                                self.ast[counter].host, req))
        return self
