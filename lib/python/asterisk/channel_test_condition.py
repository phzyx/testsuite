"""Test condition for channels

Copyright (C) 2011-2012, Digium, Inc.
Matt Jordan <mjordan@digium.com>

This program is free software, distributed under the terms of
the GNU General Public License Version 2.
"""

import asyncio

from .test_conditions import TestCondition
import re


class ChannelTestCondition(TestCondition):
    """Test condition that checks for the existence of channels.  If channels
    are detected and the number of active channels is greater than the
    configured amount, an error is raised.

    By default, the number of allowed active channels is 0.
    """

    def __init__(self, test_config):
        """Constructor

        Keyword Arguments:
        test_config The TestConfig object for the test
        """
        super(ChannelTestCondition, self).__init__(test_config)

        self.allowed_channels = 0
        if ('allowedchannels' in test_config.config):
            self.allowed_channels = test_config.config['allowedchannels']

    async def evaluate(self, related_test_condition=None):
        """Evaluate this test condition

        Keyword Argument:
        related_test_condition The test condition that this condition is
                                related to

        Returns:
        This test condition once evaluation is complete
        """
        def __channel_callback(result):
            """Callback called from core show channels"""

            channel_expression = re.compile('^[A-Za-z0-9]+/')
            channel_tokens = result.output.strip().split('\n')
            active_channels = 0
            referenced_channels = 0
            for token in channel_tokens:
                if channel_expression.match(token):
                    referenced_channels += 1
                if 'active channels' in token:
                    active_channel_tokens = token.partition(' ')
                    active_channels = int(active_channel_tokens[0].strip())
            if active_channels > self.allowed_channels:
                msg = ("Detected number of active channels %d is greater than "
                       "the allowed %d on Asterisk %s" %
                       (active_channels, self.allowed_channels, result.host))
                super(ChannelTestCondition, self).fail_check(msg)
            elif referenced_channels > self.allowed_channels:
                msg = ("Channel leak detected - "
                       "number of referenced channels %d is greater than "
                       "the allowed %d on Asterisk %s" %
                       (referenced_channels, self.allowed_channels,
                        result.host))
                super(ChannelTestCondition, self).fail_check(msg)
            return result

        # Set to pass and let a failure override
        super(ChannelTestCondition, self).pass_check()

        async def __check(ast):
            """Run 'core show channels' on an instance and inspect it"""
            __channel_callback(await ast.cli_exec('core show channels'))

        # DeferredList in the original waited for every child regardless of
        # errors; return_exceptions=True preserves that (no fail-fast).
        await asyncio.gather(*[__check(ast) for ast in self.ast],
                             return_exceptions=True)
        return self
