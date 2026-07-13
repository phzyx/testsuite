"""Unit test harness

This module provides the entry-point for tests

Copyright (C) 2018, CFWare, LLC.
Corey Farrell <git@cfware.com>

This program is free software, distributed under the terms of
the GNU General Public License Version 2.
"""

import asyncio
import logging
import os
import sys
import unittest

# Add directory where the modules to test can be found
sys.path.append('lib/python')


def ReadTestFile(filename, basepath="lib/python/asterisk/self_test"):
    fd = open(os.path.join(basepath, filename), "r")
    output = fd.read()
    fd.close()
    return output


def run_coroutine(coro):
    """Drive an async condition ``evaluate`` coroutine to completion.

    The production condition modules now expose ``evaluate`` as a coroutine.
    The unit tests still call it synchronously, so this helper runs it on a
    throwaway event loop and returns its result.
    """
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class AstMockOutput(object):
    """mock cli output base class"""

    def __init__(self, host="127.0.0.1"):
        """Constructor"""
        self.host = host

    def MockDeferFile(self, filename):
        return self.MockDefer(ReadTestFile(filename))

    async def MockDefer(self, output):
        """use a native coroutine to mock deferred CLI output"""
        self.output = output
        return self


def main():
    """Run the unit tests"""

    logging.basicConfig()
    unittest.main()


__all__ = ["main", "AstMockOutput", "ReadTestFile"]
