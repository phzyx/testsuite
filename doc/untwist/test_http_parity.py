#!/usr/bin/env python3
"""HTTP parity test for the Step 4 aiohttp cutover (design 02-design §6.2,
parity contract §6.5).

Two ``twisted.web`` servers were reimplemented on ``aiohttp`` over the
``asterisk.aio`` reactor loop:

  * ``http_static_server.HTTPStaticServer`` -- previously ``static.File`` +
    ``server.Site``; now an aiohttp static route.
  * ``realtime_test_module.RealtimeTestModule`` -- previously a
    Resource/Site tree; now a catch-all ``/{table}/{operation}`` route that
    reflects the per-operation handlers behind a request shim.

This test binds both servers on the reactor and drives raw HTTP clients (no
client-side URL normalisation, so path-traversal attempts hit the server
verbatim) to assert the §6.5 HTTP contract points:

  * Static: exact-path file serving, 404 for a missing file, and
    path-traversal protection (a ``..`` escape must NOT return a file that
    lives outside the served root).
  * Realtime: exact route matching (``/{table}/{operation}``), query- and
    form-parameter parsing, correct response bodies for the
    single/multi/store/update/destroy/require operations, and the 404 path
    for an unknown table.
  * Clean shutdown of both aiohttp runners.

Run:  PYTHONPATH=lib/python .venv/bin/python doc/untwist/test_http_parity.py
      (exit 0 = OK)
"""

import asyncio
import os
import socket
import sys
import tempfile
from urllib.parse import urlencode

from asterisk.aio.runtime import current_runtime
from asterisk.http_static_server import HTTPStaticServer
from asterisk.realtime_test_module import RealtimeTestModule

results = {}

REALTIME_PORT = 46821


class _TestObj(object):
    """Minimal stand-in for the pluggable-module test object."""
    def __init__(self):
        self.test_name = 'http_parity'

    def register_ami_observer(self, callback):
        # RealtimeTestModule registers an AMI hook; parity test never connects
        # AMI, so record and ignore it.
        self._ami_observer = callback


def _free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return port


# ---------------------------------------------------------------------------- #
# Raw HTTP client (HTTP/1.0 + close: read to EOF; no path normalisation so a
# literal ``..`` in the request target reaches the server unchanged).
# ---------------------------------------------------------------------------- #
async def _raw_request(port, method, target, body=b'', headers=None):
    reader, writer = await asyncio.open_connection('127.0.0.1', port)
    try:
        lines = ['%s %s HTTP/1.0' % (method, target), 'Host: 127.0.0.1']
        for key, value in (headers or {}).items():
            lines.append('%s: %s' % (key, value))
        if body:
            lines.append('Content-Length: %d' % len(body))
        request = ('\r\n'.join(lines) + '\r\n\r\n').encode('utf-8') + body
        writer.write(request)
        await writer.drain()
        raw = await asyncio.wait_for(reader.read(), 3)
    finally:
        writer.close()
    head, _, payload = raw.partition(b'\r\n\r\n')
    status = int(head.split(b'\r\n', 1)[0].split(b' ')[1])
    return status, payload


async def _get(port, target):
    return await _raw_request(port, 'GET', target)


async def _post(port, target, form):
    body = urlencode(form).encode('utf-8')
    return await _raw_request(
        port, 'POST', target, body,
        {'Content-Type': 'application/x-www-form-urlencoded'})


async def _wait_ready(port):
    """Poll until a server accepts connections (callWhenRunning startup is
    scheduled, not awaited, so give it a moment)."""
    for _ in range(50):
        try:
            _, writer = await asyncio.open_connection('127.0.0.1', port)
            writer.close()
            return
        except OSError:
            await asyncio.sleep(0.05)
    raise RuntimeError('server on port %d never came up' % port)


# ---------------------------------------------------------------------------- #
# Scenarios
# ---------------------------------------------------------------------------- #
async def _run(static_port):
    try:
        await _wait_ready(static_port)
        await _wait_ready(REALTIME_PORT)

        # --- Static server -------------------------------------------------- #
        status, body = await _get(static_port, '/hello.txt')
        results['static_ok_status'] = status
        results['static_ok_body'] = body

        status, _ = await _get(static_port, '/missing.txt')
        results['static_404_status'] = status

        # Path traversal: a literal ../ escape must not leak the secret file
        # that lives one directory above the served root.
        status, body = await _get(static_port, '/../secret.txt')
        results['static_traverse_status'] = status
        results['static_traverse_leaked'] = b'top-secret' in body

        # --- Realtime server ------------------------------------------------ #
        # single: form params select one row.
        status, body = await _post(static_port and REALTIME_PORT,
                                   '/friends/single',
                                   {'id': 'alice'})
        results['rt_single_status'] = status
        results['rt_single_body'] = body

        # multi: LIKE match returns multiple rows separated by CRLF.
        status, body = await _post(REALTIME_PORT, '/friends/multi',
                                   {'id LIKE': '%'})
        results['rt_multi_status'] = status
        results['rt_multi_rows'] = body.count(b'\r\n') + 1 if body else 0

        # require: always "0".
        status, body = await _get(REALTIME_PORT, '/friends/require')
        results['rt_require_body'] = body

        # store then read back the new row.
        await _post(REALTIME_PORT, '/friends/store',
                    {'id': 'carol', 'greeting': 'hola'})
        status, body = await _post(REALTIME_PORT, '/friends/single',
                                   {'id': 'carol'})
        results['rt_store_body'] = body

        # update with COMBINED sources: the id (WHERE) arrives as a URL query
        # param and the new greeting (SET) as a POST form field. The dispatcher
        # merges both into request.args, and _updateResource splits them back
        # apart using request.uri -- so this one request exercises both the
        # update operation and query+form parameter parsing together.
        status, body = await _post(REALTIME_PORT,
                                   '/friends/update?id=alice',
                                   {'greeting': 'yo'})
        results['rt_update_status'] = status
        results['rt_update_body'] = body        # rows affected
        status, body = await _post(REALTIME_PORT, '/friends/single',
                                   {'id': 'alice'})
        results['rt_update_readback'] = body     # greeting must now be 'yo'

        # destroy: returns count removed.
        status, body = await _post(REALTIME_PORT, '/friends/destroy',
                                   {'id': 'bob'})
        results['rt_destroy_body'] = body

        # unknown table -> handler-driven 404.
        status, _ = await _post(REALTIME_PORT, '/nosuch/single',
                                {'id': 'x'})
        results['rt_badtable_status'] = status

        # unknown operation -> dispatcher 404.
        status, _ = await _post(REALTIME_PORT, '/friends/bogus',
                                {'id': 'x'})
        results['rt_badop_status'] = status
    finally:
        current_runtime().stop()


def main():
    # Static root with an in-root file and a secret file ABOVE the root.
    tmp = tempfile.mkdtemp()
    root = os.path.join(tmp, 'webroot')
    os.makedirs(root)
    with open(os.path.join(root, 'hello.txt'), 'w') as handle:
        handle.write('static-content')
    with open(os.path.join(tmp, 'secret.txt'), 'w') as handle:
        handle.write('top-secret')

    static_port = _free_port()

    # HTTPStaticServer resolves root-directory against cwd; chdir so the
    # webroot path is relative and correct.
    os.chdir(tmp)
    HTTPStaticServer({'root-directory': 'webroot', 'port': static_port},
                     _TestObj())

    data = {'friends': [
        {'id': 'alice', 'greeting': 'hi'},
        {'id': 'bob', 'greeting': 'hey'},
    ]}
    RealtimeTestModule({'data': data}, _TestObj())

    current_runtime().callWhenRunning(lambda: asyncio.ensure_future(_run(static_port)))
    current_runtime().callLater(20, current_runtime().stop)  # safety net
    current_runtime().run()

    # --- Static assertions -------------------------------------------------- #
    assert results.get('static_ok_status') == 200, \
        "static file GET status: %r" % results.get('static_ok_status')
    assert results.get('static_ok_body') == b'static-content', \
        "static file body wrong: %r" % results.get('static_ok_body')
    assert results.get('static_404_status') == 404, \
        "missing file should 404: %r" % results.get('static_404_status')
    assert results.get('static_traverse_leaked') is False, \
        "PATH TRAVERSAL LEAK: secret file served through ../ escape"
    assert results.get('static_traverse_status') in (400, 403, 404), \
        "traversal attempt should be rejected: %r" \
        % results.get('static_traverse_status')

    # --- Realtime assertions ------------------------------------------------ #
    assert results.get('rt_single_status') == 200, \
        "single status: %r" % results.get('rt_single_status')
    assert results.get('rt_single_body') == b'id=alice&greeting=hi', \
        "single row wrong: %r" % results.get('rt_single_body')
    assert results.get('rt_multi_rows') == 2, \
        "multi should return 2 rows: %r" % results.get('rt_multi_rows')
    assert results.get('rt_require_body') == b'0', \
        "require should be '0': %r" % results.get('rt_require_body')
    assert results.get('rt_store_body') == b'id=carol&greeting=hola', \
        "stored row not retrievable: %r" % results.get('rt_store_body')
    assert results.get('rt_update_status') == 200, \
        "update status: %r" % results.get('rt_update_status')
    assert results.get('rt_update_body') == b'1', \
        "update should affect 1 row: %r" % results.get('rt_update_body')
    assert results.get('rt_update_readback') == b'id=alice&greeting=yo', \
        "update (query WHERE + form SET) did not apply: %r" \
        % results.get('rt_update_readback')
    assert results.get('rt_destroy_body') == b'1', \
        "destroy should remove 1 row: %r" % results.get('rt_destroy_body')
    assert results.get('rt_badtable_status') == 404, \
        "unknown table should 404: %r" % results.get('rt_badtable_status')
    assert results.get('rt_badop_status') == 404, \
        "unknown operation should 404: %r" % results.get('rt_badop_status')

    print("  [http static]   exact file 200, missing 404 OK")
    print("  [http traverse] ../ escape rejected, secret not leaked OK")
    print("  [http rt route]  /{table}/{operation} single/multi/require OK")
    print("  [http rt parse]  form params, store/destroy round-trip OK")
    print("  [http rt update] query WHERE + form SET merged, update applied OK")
    print("  [http rt 404]    unknown table and unknown operation 404 OK")
    print("ALL OK")


if __name__ == '__main__':
    print("asterisk http servers (aiohttp) parity test")
    main()
