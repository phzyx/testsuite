#!/usr/bin/env python
""" Pluggable module for running an isolated configured DNS server

Copyright (C) 2015, Digium, Inc.
Joshua Colp <jcolp@digium.com>

This program is free software, distributed under the terms of
the GNU General Public License Version 2.

asyncio port (design doc Section 6.3): the authoritative DNS server that was
built on ``twisted.names`` (``server``/``authority``/``dns``) is reimplemented on
``dnslib`` served over the ``asterisk.aio`` reactor -- a UDP datagram protocol
and a length-prefixed TCP protocol, both bound through the reactor's awaited
startup so the server is answering before Asterisk queries it.

The Python zone-file format is preserved exactly: files are still Python source
defining ``zone = [...]`` with record helpers (``SOA``/``A``/``AAAA``/``SRV``/
``NAPTR``/...), matching twisted's ``PySourceAuthority`` DSL where the first
positional argument is the owner name. BIND-syntax zone files remain supported
via ``dnslib``'s zone parser.
"""

import logging

import dnslib
from dnslib import DNSRecord, DNSHeader, RR, QTYPE, RCODE

from asterisk.aio import reactor, DatagramProtocol

LOGGER = logging.getLogger(__name__)

# The classic 512-byte UDP DNS payload limit. Responses larger than this are
# returned truncated (TC bit set) so the resolver retries over TCP -- the
# behavior the twisted.names server exhibited and the parity contract requires.
UDP_MAX = 512


# ---------------------------------------------------------------------------- #
# Time-string parsing (twisted.names.dns.str2time parity)
# ---------------------------------------------------------------------------- #
_TIME_UNITS = {'S': 1, 'M': 60, 'H': 3600, 'D': 86400, 'W': 604800}


def _str2time(value):
    """Parse an SOA time field: an int, a plain seconds string, or ``<n><unit>``
    where unit is S/M/H/D/W (e.g. ``"1H"`` -> 3600), matching twisted."""
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    if not text:
        return 0
    unit = text[-1].upper()
    if unit in _TIME_UNITS:
        return int(float(text[:-1]) * _TIME_UNITS[unit])
    return int(text)


def _name(value):
    """Normalize a DNS name to a lowercase, dot-stripped key."""
    return str(value).rstrip('.').lower()


def _to_bytes(value):
    """NAPTR flags/service/regexp are bytes in the zone DSL; accept str too."""
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    return str(value).encode()


# ---------------------------------------------------------------------------- #
# Zone loading
# ---------------------------------------------------------------------------- #
class _ZoneRecord(object):
    """A single parsed record: owner name, rtype, dnslib rdata, optional ttl."""

    __slots__ = ('owner', 'rtype', 'rdata', 'ttl', 'soa_minimum')

    def __init__(self, owner, rtype, rdata, ttl, soa_minimum=None):
        self.owner = owner
        self.rtype = rtype
        self.rdata = rdata
        self.ttl = ttl
        self.soa_minimum = soa_minimum


def _make_builders():
    """Return the record-helper namespace exec'd against a Python zone file.

    Mirrors twisted's ``PySourceAuthority.wrapRecord``: each helper takes the
    owner name as its first positional argument, the remaining arguments matching
    the corresponding ``twisted.names.dns.Record_*`` constructor, and yields a
    ``_ZoneRecord`` carrying the equivalent ``dnslib`` rdata.
    """

    def SOA(name, mname='', rname='', serial=0, refresh=0, retry=0,
            expire=0, minimum=0, ttl=None):
        minimum_s = _str2time(minimum)
        times = (int(serial), _str2time(refresh), _str2time(retry),
                 _str2time(expire), minimum_s)
        rdata = dnslib.SOA(str(mname), str(rname), times)
        return _ZoneRecord(_name(name), QTYPE.SOA, rdata, ttl,
                           soa_minimum=minimum_s)

    def A(name, address='0.0.0.0', ttl=None):
        return _ZoneRecord(_name(name), QTYPE.A, dnslib.A(str(address)), ttl)

    def AAAA(name, address='::', ttl=None):
        return _ZoneRecord(_name(name), QTYPE.AAAA, dnslib.AAAA(str(address)),
                           ttl)

    def SRV(name, priority=0, weight=0, port=0, target='', ttl=None):
        rdata = dnslib.SRV(int(priority), int(weight), int(port), str(target))
        return _ZoneRecord(_name(name), QTYPE.SRV, rdata, ttl)

    def NAPTR(name, order=0, preference=0, flags=b'', service=b'',
              regexp=b'', replacement='', ttl=None):
        rdata = dnslib.NAPTR(int(order), int(preference), _to_bytes(flags),
                             _to_bytes(service), _to_bytes(regexp),
                             str(replacement))
        return _ZoneRecord(_name(name), QTYPE.NAPTR, rdata, ttl)

    def NS(name, nsname='', ttl=None):
        return _ZoneRecord(_name(name), QTYPE.NS, dnslib.NS(str(nsname)), ttl)

    def CNAME(name, cname='', ttl=None):
        return _ZoneRecord(_name(name), QTYPE.CNAME, dnslib.CNAME(str(cname)),
                           ttl)

    def PTR(name, ptrname='', ttl=None):
        return _ZoneRecord(_name(name), QTYPE.PTR, dnslib.PTR(str(ptrname)),
                           ttl)

    def MX(name, preference=0, exchange='', ttl=None):
        rdata = dnslib.MX(str(exchange), int(preference))
        return _ZoneRecord(_name(name), QTYPE.MX, rdata, ttl)

    def TXT(name, *data, **kw):
        ttl = kw.get('ttl')
        joined = b''.join(_to_bytes(d) for d in data)
        return _ZoneRecord(_name(name), QTYPE.TXT, dnslib.TXT(joined), ttl)

    return {'SOA': SOA, 'A': A, 'AAAA': AAAA, 'SRV': SRV, 'NAPTR': NAPTR,
            'NS': NS, 'CNAME': CNAME, 'PTR': PTR, 'MX': MX, 'TXT': TXT}


class Zone(object):
    """An authoritative zone: owner->rtype->[RR] plus the SOA and origin."""

    def __init__(self):
        self.origin = None            # lowercase zone apex (from the SOA)
        self.soa_rr = None            # the SOA as a dnslib RR
        self.soa_minimum = 3600       # default TTL for records without one
        self.records = {}             # {owner_lower: {rtype_int: [RR]}}

    @classmethod
    def from_python(cls, filename):
        """Load a twisted-style Python zone file."""
        namespace = {}
        with open(filename) as handle:
            code = compile(handle.read(), filename, 'exec')
        exec(code, _make_builders(), namespace)
        if 'zone' not in namespace:
            raise ValueError("No zone defined in " + filename)
        return cls._from_records(namespace['zone'])

    @classmethod
    def from_bind(cls, filename):
        """Load a BIND-syntax zone file via dnslib's parser."""
        zone = cls()
        with open(filename) as handle:
            rrs = RR.fromZone(handle.read())
        # Establish origin/SOA first, then default TTLs.
        for rr in rrs:
            if rr.rtype == QTYPE.SOA:
                zone.origin = _name(rr.rname)
                zone.soa_rr = rr
                zone.soa_minimum = rr.rdata.times[4] if rr.rdata.times else 3600
        for rr in rrs:
            if rr.ttl in (0, None):
                rr.ttl = zone.soa_minimum
            zone._index(rr)
        return zone

    @classmethod
    def _from_records(cls, zone_records):
        zone = cls()
        soa = next((r for r in zone_records if r.rtype == QTYPE.SOA), None)
        if soa is not None:
            zone.origin = soa.owner
            zone.soa_minimum = soa.soa_minimum or 3600
        for rec in zone_records:
            ttl = rec.ttl if rec.ttl is not None else zone.soa_minimum
            rr = RR(rec.owner, rec.rtype, ttl=ttl, rdata=rec.rdata)
            if rec.rtype == QTYPE.SOA:
                zone.soa_rr = rr
            zone._index(rr)
        return zone

    def _index(self, rr):
        owner = _name(rr.rname)
        self.records.setdefault(owner, {}).setdefault(rr.rtype, []).append(rr)

    def owns(self, qname):
        """True if ``qname`` falls within this zone's origin."""
        if self.origin is None:
            return False
        return qname == self.origin or qname.endswith('.' + self.origin)


# ---------------------------------------------------------------------------- #
# Resolver: turn a query into an authoritative reply
# ---------------------------------------------------------------------------- #
class Resolver(object):
    """Answer queries authoritatively across the configured zones."""

    def __init__(self, zones):
        self._zones = zones

    def _zone_for(self, qname):
        # Most specific origin wins if zones nest.
        best = None
        for zone in self._zones:
            if zone.owns(qname) and (best is None or
                                     len(zone.origin) > len(best.origin)):
                best = zone
        return best

    def resolve(self, request):
        """Build a dnslib reply DNSRecord for ``request``."""
        reply = request.reply()
        question = request.q
        qname = _name(question.qname)
        qtype = question.qtype

        zone = self._zone_for(qname)
        if zone is None:
            # Not authoritative for this name.
            reply.header.rcode = RCODE.REFUSED
            return reply

        reply.header.aa = 1
        if self._add_answers(reply, zone, qname, qtype):
            return reply

        # No answer records. Distinguish NODATA (name exists, wrong type) from
        # NXDOMAIN (name does not exist), attaching the SOA in the authority
        # section either way, as an authoritative server does.
        if zone.soa_rr is not None:
            reply.add_auth(zone.soa_rr)
        if qname not in zone.records:
            reply.header.rcode = RCODE.NXDOMAIN
        return reply

    def _add_answers(self, reply, zone, qname, qtype, depth=0):
        recs = zone.records.get(qname)
        if not recs or depth > 8:
            return False

        if qtype in recs:
            targets = []
            for rr in recs[qtype]:
                reply.add_answer(rr)
                if qtype == QTYPE.SRV:
                    targets.append(_name(rr.rdata.target))
            self._add_glue(reply, zone, targets)
            return True

        # Follow a CNAME when the requested type is not directly present.
        if qtype != QTYPE.CNAME and QTYPE.CNAME in recs:
            for rr in recs[QTYPE.CNAME]:
                reply.add_answer(rr)
                target = _name(rr.rdata.label)
                self._add_answers(reply, zone, target, qtype, depth + 1)
            return True

        return False

    def _add_glue(self, reply, zone, targets):
        """Add A/AAAA glue for SRV targets that live in this zone."""
        for target in targets:
            recs = zone.records.get(target)
            if not recs:
                continue
            for gtype in (QTYPE.A, QTYPE.AAAA):
                for rr in recs.get(gtype, []):
                    reply.add_ar(rr)


def _pack_udp(reply):
    """Pack a reply for UDP, truncating (TC bit) if it exceeds 512 bytes."""
    data = reply.pack()
    if len(data) <= UDP_MAX:
        return data
    # Return an answer-less response with TC set so the client retries via TCP.
    truncated = DNSRecord(DNSHeader(id=reply.header.id, qr=1, aa=1, tc=1,
                                    ra=0, rcode=reply.header.rcode),
                          q=reply.q)
    return truncated.pack()


# ---------------------------------------------------------------------------- #
# Transport protocols
# ---------------------------------------------------------------------------- #
class _DNSDatagramProtocol(DatagramProtocol):
    """UDP DNS server protocol (aio.DatagramProtocol)."""

    def __init__(self, resolver):
        self._resolver = resolver

    def datagramReceived(self, data, addr):
        try:
            request = DNSRecord.parse(data)
            reply = self._resolver.resolve(request)
            self.transport.write(_pack_udp(reply), addr)
        except Exception:
            LOGGER.exception("Failed to handle UDP DNS query from %s", addr)


class _DNSTCPProtocol(object):
    """TCP DNS server protocol: 2-byte length-prefixed messages.

    Built for the reactor's listenTCP adapter, which calls ``buildProtocol`` on
    the factory and then ``makeConnection(transport)`` on the protocol.
    """

    def __init__(self, resolver):
        self._resolver = resolver
        self._buffer = b''
        self.transport = None

    def makeConnection(self, transport):
        self.transport = transport

    def dataReceived(self, data):
        self._buffer += data
        # A TCP DNS message is a 2-byte big-endian length followed by that many
        # bytes; more than one may arrive per read.
        while len(self._buffer) >= 2:
            length = (self._buffer[0] << 8) | self._buffer[1]
            if len(self._buffer) < length + 2:
                break
            message = self._buffer[2:length + 2]
            self._buffer = self._buffer[length + 2:]
            try:
                request = DNSRecord.parse(message)
                reply = self._resolver.resolve(request)
                packed = reply.pack()          # no truncation over TCP
                header = bytes([(len(packed) >> 8) & 0xff, len(packed) & 0xff])
                self.transport.write(header + packed)
            except Exception:
                LOGGER.exception("Failed to handle TCP DNS query")

    def connectionLost(self, reason):
        pass


class _DNSTCPFactory(object):
    """Factory the reactor's listenTCP uses to build per-connection protocols."""

    def __init__(self, resolver):
        self._resolver = resolver

    def buildProtocol(self, addr):
        return _DNSTCPProtocol(self._resolver)


# ---------------------------------------------------------------------------- #
# Pluggable module entry point
# ---------------------------------------------------------------------------- #
class DNSServer(object):
    """Start a local authoritative DNS server (UDP + TCP) from zone files.

    Configuration options:
        port: The port to listen for DNS requests on (default 10053).
        python-zones: An array of Python zone files (twisted PySourceAuthority
            syntax; the record helpers take the owner name as first argument).
        bind-zones: An array of BIND-syntax zone files.
    """

    def __init__(self, config, test_obj):
        """Initialize, load zones, and bind the UDP and TCP listeners."""
        port = config.get('port', 10053)
        pyzones = config.get('python-zones', [])
        bindzones = config.get('bind-zones', [])

        zones = []
        for pyzone in pyzones:
            path = '%s/dns_zones/%s' % (test_obj.test_name, pyzone)
            zones.append(Zone.from_python(path))
            LOGGER.info("Added Python zone file %s", pyzone)

        for bindzone in bindzones:
            path = '%s/dns_zones/%s' % (test_obj.test_name, bindzone)
            zones.append(Zone.from_bind(path))
            LOGGER.info("Added BIND zone file %s", bindzone)

        resolver = Resolver(zones)

        # Bind through the reactor so startup is awaited: the server is listening
        # (UDP and TCP) before Asterisk begins resolving.
        reactor.listenUDP(port, _DNSDatagramProtocol(resolver))
        reactor.listenTCP(port, _DNSTCPFactory(resolver))

        LOGGER.info("Started DNS server (UDP and TCP) on port %d", port)
