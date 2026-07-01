#!/usr/bin/env python
"""Pluggable module for running an HTTP server that hosts static content

Copyright (C) 2016, Digium, Inc.
Matt Jordan <mjordan@digium.com>

This program is free software, distributed under the terms of
the GNU General Public License Version 2.

asyncio port (design doc Section 6.2): the ``twisted.web`` ``static.File`` +
``server.Site`` listener is reimplemented as an ``aiohttp`` static route served
on the ``asterisk.aio`` reactor loop. aiohttp's static handler provides the
path-traversal protection the parity contract (Section 6.5) requires.
"""

import logging
import os

from aiohttp import web

from asterisk.aio import reactor

LOGGER = logging.getLogger(__name__)


class HTTPStaticServer(object):
    """A pluggable module that creates an HTTP server hosting static content."""

    def __init__(self, module_config, test_object):
        """Constructor

        Keyword Arguments:
        module_config The pluggable module's configuration
        test_object   The one and only test object
        """
        self._root = os.path.join(os.getcwd(), module_config['root-directory'])
        self._port = module_config.get('port', 8090)
        self._runner = None
        # Bind through the reactor's awaited startup path so a failure to claim
        # the port surfaces out of run() (matching twisted's synchronous
        # listenTCP), and register async cleanup of the AppRunner at shutdown.
        reactor.addStartupBind(self._start(),
                               label='http-static:%d' % self._port)

    async def _start(self):
        """Start the aiohttp static server on the reactor's event loop."""
        app = web.Application()
        # add_static resolves requests against the root and rejects attempts to
        # escape it (path traversal), returning 403/404 rather than the file.
        #
        # follow_symlinks=True restores Twisted's static.File behaviour: some
        # webroots (e.g. the STIR/SHAKEN tests) publish their certificate files
        # as symlinks into a sibling keys/ directory. aiohttp refuses to serve
        # symlinked targets by default (404), which broke those tests; Twisted
        # followed the links. The root is a fixed, test-controlled directory, so
        # following links here does not widen the traversal surface in practice.
        app.router.add_static('/', self._root, show_index=False,
                              follow_symlinks=True)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, '0.0.0.0', self._port)
        await site.start()
        reactor.addAsyncCleanup(self._runner.cleanup)
        LOGGER.info("Started static HTTP server on port %d serving %s",
                    self._port, self._root)
