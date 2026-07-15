'''
Copyright (C) 2018, Digium, Inc.
Torrey Searle  <tsearle@gmail.com>
Nitesh Bansal <nitesh.bansal@gmail.com>
Matt Jordan <mjordan@digium.com>

This program is free software, distributed under the terms of
the GNU General Public License Version 2.
'''

import asyncio

from test_conditions import TestCondition


class PJSipChannelTestCondition(TestCondition):
    """Test condition that checks for the existence of PJSIP channels.

    If channels are detected and the number of active channels is greater than
    the configured amount, an error is raised.

    By default, the number of allowed active channels is 0.
    """

    def __init__(self, test_config):
        """Constructor"""
        super(PJSipChannelTestCondition, self).__init__(test_config)

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
                if 'Objects found: ' in token:
                    active_channels = int(token[token.find(":")+1:].strip())
            if active_channels > self.allowed_channels:
                super(PJSipChannelTestCondition, self).fail_check(
                    ("Detected number of active PJSIP channels %d is greater "
                     "than the allowed %d on Asterisk %s" %
                     (active_channels, self.allowed_channels, result.host)))
            return result

        # Set to pass and let a failure override
        super(PJSipChannelTestCondition, self).pass_check()

        async def __check(ast):
            """Run 'pjsip show channels' on an instance and inspect it"""
            __channel_callback(await ast.cli_exec('pjsip show channels'))

        # Wait for every child regardless of errors; do not fail fast.
        await asyncio.gather(*[__check(ast) for ast in self.ast],
                             return_exceptions=True)
        return self
