#!/usr/bin/env python

import asyncio
import logging

from .test_conditions import TestCondition

LOGGER = logging.getLogger(__name__)

# a list of task processors that should be ignored by this test
IGNORED_TASKPROCESSORS = [
    # the outsess traskprocessor will exist until 30 seconds after the call has ended
    'pjsip/outsess/'
]

class Taskprocessor(object):
    """A small object that tracks a task processor.

    The object parses a line from the Asterisk CLI command core show taskprocessors to
    populate its values
    """
    processor = ""

    def __init__(self, line):
        """Constructor

        Keyword Arguments:
        line A raw line received from Asterisk that describes a task processor
        """
        line = line.strip()
        tokens = line.split()

        if len(tokens) == 6:
            self.processor = tokens[0].strip()
            self.processed = int(tokens[1])
            self.in_queue = int(tokens[2])
            self.max_depth = int(tokens[3])
            self.low_water = int(tokens[4])
            self.high_water = int(tokens[5])


class TaskprocessorTestCondition(TestCondition):
    """Base class for the task processor pre- and post-test
    checks. This class provides a common mechanism to get the task
    processors from Asterisk and populate them in a dictionary for
    tracking
    """

    def __init__(self, test_config):
        """Constructor

        Keyword Arguments:
        test_config The test config object
        """
        super(TaskprocessorTestCondition, self).__init__(test_config)
        self.task_processors = {}

    def is_taskprocessor_ignored(self, name):
        for ignored_taskprocessor in IGNORED_TASKPROCESSORS:
            if name.startswith(ignored_taskprocessor):
                return True

        return False

    async def get_task_processors(self, ast):
        """Get the task processors from some instance of Asterisk"""

        result = await ast.cli_exec("core show taskprocessors")

        lines = result.output
        if 'No such command' in lines:
            return
        if 'Unable to connect to remote asterisk' in lines:
            return

        line_tokens = lines.split('\n')
        task_processors = []

        for line in line_tokens:
            task_processor = Taskprocessor(line)
            if task_processor.processor != '' and not self.is_taskprocessor_ignored(task_processor.processor):
                LOGGER.debug("Tracking %s", task_processor.processor)
                task_processors.append(task_processor)

        self.task_processors[result.host] = task_processors


class TaskprocessorPreTestCondition(TaskprocessorTestCondition):
    """The Task Processor Pre-TestCondition object"""

    async def evaluate(self, related_test_condition=None):
        """Evaluate the test condition"""

        # Automatically pass the pre-test condition - whatever task processors
        # are currently open are needed by Asterisk and merely expected to exist
        # when the test is finished
        super(TaskprocessorPreTestCondition, self).pass_check()

        # Wait for every child regardless of errors; do not fail fast.
        await asyncio.gather(*[
            super(TaskprocessorPreTestCondition, self).get_task_processors(ast)
            for ast in self.ast], return_exceptions=True)

        return self


class TaskprocessorPostTestCondition(TaskprocessorTestCondition):
    """The Task Processor Post-TestCondition object"""

    async def evaluate(self, related_test_condition=None):
        """Evaluate the test condition"""

        if related_test_condition is None:
            msg = "No pre-test condition object provided"
            super(TaskprocessorPostTestCondition, self).fail_check(msg)
            return

        # Wait for every child regardless of errors; do not fail fast.
        await asyncio.gather(*[
            super(TaskprocessorPostTestCondition, self).get_task_processors(ast)
            for ast in self.ast], return_exceptions=True)

        for ast_host in related_test_condition.task_processors.keys():
            if not ast_host in self.task_processors:
                super(TaskprocessorPostTestCondition, self).fail_check(
                    "Asterisk host in pre-test check [%s]"
                    " not found in post-test check" % ast_host)
            else:
                # Find all task processors in post-check not in pre-check
                for task_processor in self.task_processors[ast_host]:
                    if (len([
                            t for t
                            in related_test_condition.task_processors[ast_host]
                            if task_processor.processor == t.processor]) == 0):
                        super(TaskprocessorPostTestCondition, self).fail_check(
                            "Failed to find task processor %s in "
                            "pre-test check" % (task_processor.processor))
        super(TaskprocessorPostTestCondition, self).pass_check()
        return self
