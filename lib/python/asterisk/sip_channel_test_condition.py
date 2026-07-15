#!/usr/bin/env python
"""Test condition that verifies SIP channels

Copyright (C) 2013, Digium, Inc.
Nitesh Bansal <nitesh.bansal@gmail.com>
Matt Jordan <mjordan@digium.com>

This program is free software, distributed under the terms of
the GNU General Public License Version 2.
"""

import asyncio

from .test_conditions import TestCondition


class SipChannelTestCondition(TestCondition):
    """Test condition that checks for the existence of SIP channels.

    If channels are detected and the number of active channels is greater than
    the configured amount, an error is raised.

    By default, the number of allowed active channels is 0.
    """

    def __init__(self, test_config):
        """Constructor"""
        super(SipChannelTestCondition, self).__init__(test_config)

        self.allowed_channels = 0
        if ('allowedchannels' in test_config.config):
            self.allowed_channels = test_config.config['allowedchannels']

    async def evaluate(self, related_test_condition=None):
        """Evaluate the test condition"""

        def __channel_callback(result):
            """Callback for the CLI command"""

            channel_tokens = result.output.strip().split('\n')
            active_channels = 0
            for token in channel_tokens:
                if 'active SIP channel' in token:
                    active_channel_tokens = token.partition(' ')
                    active_channels = int(active_channel_tokens[0].strip())
            if active_channels > self.allowed_channels:
                super(SipChannelTestCondition, self).fail_check(
                    ("Detected number of active SIP channels %d is greater "
                     "than the allowed %d on Asterisk %s" %
                     (active_channels, self.allowed_channels, result.host)))
            return result

        # Set to pass and let a failure override
        super(SipChannelTestCondition, self).pass_check()

        async def __check(ast):
            """Run 'sip show channels' on an instance and inspect it"""
            __channel_callback(await ast.cli_exec('sip show channels'))

        # Wait for every child regardless of errors; do not fail fast.
        await asyncio.gather(*[__check(ast) for ast in self.ast],
                             return_exceptions=True)
        return self
