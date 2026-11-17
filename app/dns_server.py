"""Authoritative UDP DNS service for the fixed intranet zone.

Every answer is built from one immutable version snapshot fetched in a
single storage read, so a reply can never mix records of two versions.
While a transfer is in progress the old version stays active and keeps
being answered.

Host isolation: while an isolation is effective for the active serial,
A queries for the isolated name are answered SERVFAIL (the name is
"blocked" without touching the zone data itself); every other name and
query type is answered exactly as before. The snapshot pairs the zone
version with the isolation state that belongs to it, so an answer never
mixes a new zone with an old isolation or vice versa.
"""
from __future__ import annotations

import logging
from typing import List

from dnslib import A, DNSRecord, RR, QTYPE, RCODE
from dnslib.server import BaseResolver, DNSServer

from .config import settings
from .storage import isolation_effective, storage

log = logging.getLogger("dns")


def _normalise(qname: str) -> str:
    return qname.rstrip(".").lower()


class ZoneResolver(BaseResolver):
    def resolve(self, request: DNSRecord, handler) -> DNSRecord:
        reply = request.reply()
        q = request.q
        qname = _normalise(str(q.qname))
        zone = settings.dns_zone.rstrip(".").lower()

        in_zone = qname == zone or qname.endswith("." + zone)
        if not in_zone:
            # This service is authoritative only for the fixed zone.
            reply.header.rcode = RCODE.REFUSED
            return reply

        if QTYPE[q.qtype] != "A":
            # In-zone but not an A query: authoritative NOERROR/NODATA.
            reply.header.rcode = RCODE.NOERROR
            return reply

        # One consistent snapshot -> one version per answer, paired with
        # the isolation state that belongs to that same version.
        serial, records, _, isolation = storage.get_active_state()

        if (
            isolation_effective(isolation, serial)
            and qname == isolation["name"]
        ):
            # Isolated host: block only this name's A answers.
            reply.header.rcode = RCODE.SERVFAIL
            log.info(
                "SERVFAIL %s isolated until %s (serial=%s, request=%s)",
                qname, isolation["expires_at"], serial,
                isolation["request_id"],
            )
            return reply

        name_map = {}
        for rec in records:
            name_map.setdefault(rec["name"], []).append(rec["ip"])

        ips: List[str] = name_map.get(qname, [])
        if not ips:
            reply.header.rcode = RCODE.NXDOMAIN
            log.debug("NXDOMAIN %s (serial=%s)", qname, serial)
            return reply

        for ip in ips:
            reply.add_answer(
                RR(
                    q.qname,
                    QTYPE.A,
                    rdata=A(ip),
                    ttl=30,
                )
            )
        log.debug("ANSWER %s -> %s (serial=%s)", qname, ips, serial)
        return reply


class DnsService:
    def __init__(self) -> None:
        self._server: DNSServer | None = None

    def start(self) -> None:
        resolver = ZoneResolver()
        # DNSServer is itself a daemon thread; start_thread() runs the
        # UDP serve loop without blocking the FastAPI startup.
        self._server = DNSServer(
            resolver,
            port=settings.dns_port,
            address=settings.dns_host,
            tcp=False,
        )
        self._server.start_thread()
        log.info(
            "UDP DNS listener on %s:%s (zone=%s, role=%s)",
            settings.dns_host,
            settings.dns_port,
            settings.dns_zone,
            settings.role,
        )

    def stop(self) -> None:
        if self._server is not None:
            try:
                self._server.stop()
            except Exception:  # pragma: no cover
                pass


dns_service = DnsService()
