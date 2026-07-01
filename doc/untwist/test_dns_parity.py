#!/usr/bin/env python3
"""DNS parity test for the Step 4 dns_server.py cutover (design 02-design §6.3,
parity contract §6.5).

`dns_server.py` used to serve zones with `twisted.names`; it now serves them with
`dnslib` over the `asterisk.aio` reactor. This test loads a real twisted-style
Python zone file through the converted loader, binds the server *before*
`reactor.run()` (so the reactor's awaited startup guarantees it is answering
before any query), and drives async UDP and TCP DNS clients on the reactor loop
to assert the contract points:

  * Zone-file parsing + record types: SOA/A/AAAA/SRV/NAPTR round-trip.
  * Authoritative-answer (AA) flag is set on answers from our zones.
  * SRV additional-section glue (A/AAAA for the target) is included.
  * NXDOMAIN (name absent) vs NODATA (name present, type absent) are
    distinguished, each carrying the SOA in the authority section.
  * Non-authoritative names are REFUSED.
  * UDP responses over 512 bytes are truncated (TC bit, no answers); the same
    query over TCP returns the full, untruncated answer set.

Run:  PYTHONPATH=lib/python .venv/bin/python doc/untwist/test_dns_parity.py
      (exit 0 = OK)
"""

import asyncio
import os
import socket
import sys
import tempfile

from dnslib import DNSRecord, QTYPE, RCODE

from asterisk.aio import reactor
from asterisk.dns_server import DNSServer

results = {}

ZONE = """
zone = [
    SOA('example.com', mname="ns1.example.com", rname="root.example.com",
        serial=2003010601, refresh="1H", retry="1H", expire="1H", minimum="1H"),
    NAPTR('example.com', 50, 50, b"S", b"SIP+D2T", b"", '_sip._tcp.example.com'),
    SRV('_sip._udp.example.com', 0, 1, 5061, 'pbx.example.com'),
    A('pbx.example.com', '127.0.0.1'),
    AAAA('pbx.example.com', '::1'),
] + [
    SRV('_big._udp.example.com', 0, 1, 5060 + i, 'h%d.example.com' % i)
    for i in range(40)
] + [
    A('h%d.example.com' % i, '127.0.0.%d' % (i % 254 + 1)) for i in range(40)
]
"""


class _TestObj(object):
    """Minimal stand-in for the pluggable-module test object."""
    def __init__(self, test_name):
        self.test_name = test_name


def _free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return port


# ---------------------------------------------------------------------------- #
# Async DNS clients (run on the reactor loop; no blocking socket I/O)
# ---------------------------------------------------------------------------- #
async def _udp_query(port, qname, qtype):
    loop = asyncio.get_event_loop()
    fut = loop.create_future()

    class _Client(asyncio.DatagramProtocol):
        def datagram_received(self, data, addr):
            if not fut.done():
                fut.set_result(data)

    transport, _ = await loop.create_datagram_endpoint(
        _Client, remote_addr=('127.0.0.1', port))
    try:
        transport.sendto(DNSRecord.question(qname, qtype).pack())
        data = await asyncio.wait_for(fut, 3)
    finally:
        transport.close()
    return DNSRecord.parse(data)


async def _tcp_query(port, qname, qtype):
    reader, writer = await asyncio.open_connection('127.0.0.1', port)
    try:
        packed = DNSRecord.question(qname, qtype).pack()
        writer.write(bytes([(len(packed) >> 8) & 0xff, len(packed) & 0xff])
                     + packed)
        await writer.drain()
        header = await asyncio.wait_for(reader.readexactly(2), 3)
        length = (header[0] << 8) | header[1]
        body = await asyncio.wait_for(reader.readexactly(length), 3)
    finally:
        writer.close()
    return DNSRecord.parse(body)


# ---------------------------------------------------------------------------- #
# Scenarios
# ---------------------------------------------------------------------------- #
async def _run(port):
    try:
        # SRV lookup: answer + AA flag + A/AAAA glue in additional.
        srv = await _udp_query(port, '_sip._udp.example.com', 'SRV')
        results['srv_aa'] = srv.header.aa
        results['srv_answers'] = [r.rdata.target for r in srv.rr
                                  if r.rtype == QTYPE.SRV]
        glue = {r.rtype for r in srv.ar}
        results['srv_glue_a'] = QTYPE.A in glue
        results['srv_glue_aaaa'] = QTYPE.AAAA in glue

        # NAPTR + A + AAAA record types parse and serve.
        naptr = await _udp_query(port, 'example.com', 'NAPTR')
        results['naptr_count'] = sum(1 for r in naptr.rr
                                     if r.rtype == QTYPE.NAPTR)
        a = await _udp_query(port, 'pbx.example.com', 'A')
        results['a_data'] = [str(r.rdata) for r in a.rr if r.rtype == QTYPE.A]
        aaaa = await _udp_query(port, 'pbx.example.com', 'AAAA')
        results['aaaa_count'] = sum(1 for r in aaaa.rr
                                    if r.rtype == QTYPE.AAAA)

        # NODATA: name exists (pbx has A/AAAA) but no SRV -> NOERROR, SOA auth.
        nodata = await _udp_query(port, 'pbx.example.com', 'SRV')
        results['nodata_rcode'] = nodata.header.rcode
        results['nodata_answers'] = len(nodata.rr)
        results['nodata_soa'] = any(r.rtype == QTYPE.SOA for r in nodata.auth)

        # NXDOMAIN: name absent -> NXDOMAIN, SOA auth.
        nx = await _udp_query(port, 'nope.example.com', 'A')
        results['nx_rcode'] = nx.header.rcode
        results['nx_soa'] = any(r.rtype == QTYPE.SOA for r in nx.auth)

        # Non-authoritative -> REFUSED.
        refused = await _udp_query(port, 'somewhere.org', 'A')
        results['refused_rcode'] = refused.header.rcode

        # Truncation: big response over UDP sets TC and drops answers; TCP
        # returns the full set.
        big_udp = await _udp_query(port, '_big._udp.example.com', 'SRV')
        results['tc_bit'] = big_udp.header.tc
        results['tc_udp_answers'] = len(big_udp.rr)
        big_tcp = await _tcp_query(port, '_big._udp.example.com', 'SRV')
        results['tcp_tc_bit'] = big_tcp.header.tc
        results['tcp_answers'] = sum(1 for r in big_tcp.rr
                                     if r.rtype == QTYPE.SRV)
    finally:
        reactor.stop()


def main():
    tmp = tempfile.mkdtemp()
    os.makedirs(os.path.join(tmp, 'dns_zones'))
    with open(os.path.join(tmp, 'dns_zones', 'example.com'), 'w') as handle:
        handle.write(ZONE)

    port = _free_port()
    # Bind BEFORE run() so the reactor's awaited startup makes the server ready
    # before the first query (the contract's startup-readiness point).
    DNSServer({'port': port, 'python-zones': ['example.com']}, _TestObj(tmp))

    reactor.callWhenRunning(lambda: asyncio.ensure_future(_run(port)))
    reactor.callLater(15, reactor.stop)  # safety net
    reactor.run()

    assert results.get('srv_aa') == 1, "AA flag not set on SRV answer"
    assert results.get('srv_answers') and \
        'pbx.example.com' in str(results['srv_answers'][0]), \
        "SRV answer missing: %r" % results.get('srv_answers')
    assert results.get('srv_glue_a') and results.get('srv_glue_aaaa'), \
        "SRV additional-section A/AAAA glue missing"
    assert results.get('naptr_count') == 1, \
        "NAPTR not served: %r" % results.get('naptr_count')
    assert results.get('a_data') == ['127.0.0.1'], \
        "A record wrong: %r" % results.get('a_data')
    assert results.get('aaaa_count') == 1, "AAAA record not served"

    assert results.get('nodata_rcode') == RCODE.NOERROR, \
        "NODATA should be NOERROR: %r" % results.get('nodata_rcode')
    assert results.get('nodata_answers') == 0, "NODATA should have no answers"
    assert results.get('nodata_soa') is True, "NODATA missing SOA in authority"

    assert results.get('nx_rcode') == RCODE.NXDOMAIN, \
        "absent name should be NXDOMAIN: %r" % results.get('nx_rcode')
    assert results.get('nx_soa') is True, "NXDOMAIN missing SOA in authority"

    assert results.get('refused_rcode') == RCODE.REFUSED, \
        "non-authoritative name should be REFUSED: %r" \
        % results.get('refused_rcode')

    assert results.get('tc_bit') == 1, "large UDP response not truncated (TC)"
    assert results.get('tc_udp_answers') == 0, \
        "truncated UDP response should carry no answers"
    assert results.get('tcp_tc_bit') == 0, "TCP response should not be truncated"
    assert results.get('tcp_answers') == 40, \
        "TCP response lost answers: %r" % results.get('tcp_answers')

    print("  [dns types]   SOA/A/AAAA/SRV/NAPTR parsed and served OK")
    print("  [dns aa+glue] AA flag set, SRV A/AAAA glue present OK")
    print("  [dns nodata]  NOERROR+SOA (type absent) vs NXDOMAIN+SOA OK")
    print("  [dns refused] non-authoritative name REFUSED OK")
    print("  [dns tc->tcp] UDP TC=1 no-answers, TCP full 40 answers OK")
    print("ALL OK")


if __name__ == '__main__':
    print("asterisk dns_server (dnslib) parity test")
    main()
