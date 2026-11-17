"""End-to-end test for the host-isolation API (real primary + backup).

Run with the project venv:  .venv/bin/python -I tests/isolation_test.py

Scenarios exercised:
 1. isolation submit on the primary -> SERVFAIL for that name's A query
    on the primary, other names/record types unaffected, serial unchanged
 2. isolation replicates to the backup via normal sync (no serial bump)
 3. idempotency: same request_id retry returns the stored deadline and
    never extends it; request_id reuse with other params conflicts
 4. single-slot rule, stale serial, unknown name, duration bounds
 5. backup restart during isolation: state persists, deadline unchanged
 6. expiry: both roles lift the block at the original absolute deadline
 7. partition: backup cut off from the primary keeps serving SERVFAIL
    until the original deadline, then lifts on its own
 8. publish invalidates the isolation immediately on both roles; a late
    retry of the old request cannot re-cover the new zone
"""
from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
from dnslib import DNSRecord, RCODE

ROOT = Path(__file__).resolve().parent.parent
PY = str(ROOT / ".venv" / "bin" / "python")
DATA = ROOT / "data"
DATA.mkdir(exist_ok=True)

ZONE = "internal.local"
PRIMARY_API = "http://127.0.0.1:18020"
BACKUP_API = "http://127.0.0.1:18021"
PRIMARY_DNS = ("127.0.0.1", 15060)
BACKUP_DNS = ("127.0.0.1", 15061)
BLACKHOLE = "http://127.0.0.1:19999"  # nothing listens -> refused
SYNC_INTERVAL = "1"

PASS, FAIL = 0, 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name} {detail}")


def wait_http(url: str, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            r = httpx.get(url, timeout=1.5)
            if r.status_code == 200:
                return
        except Exception as exc:
            last = exc
        time.sleep(0.3)
    raise RuntimeError(f"service not ready: {url} ({last})")


def dns_query(server, name: str, qtype: str = "A", timeout: float = 3.0):
    q = DNSRecord.question(name, qtype)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout)
    sock.sendto(q.pack(), server)
    data, _ = sock.recvfrom(65535)
    sock.close()
    return DNSRecord.parse(data)


def rcode(server, name: str, qtype: str = "A") -> int:
    return dns_query(server, name, qtype).header.rcode


def wait_rcode(server, name: str, want: int, timeout: float = 8.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if rcode(server, name) == want:
                return True
        except OSError:
            pass
        time.sleep(0.4)
    return False


def start_service(role: str, api_port: int, dns_port: int,
                  db: str, primary_url: str | None) -> subprocess.Popen:
    env = os.environ.copy()
    env.update(
        {
            "DNS_ROLE": role,
            "DNS_ZONE": ZONE,
            "DB_PATH": db,
            "DNS_HOST": "127.0.0.1",
            "DNS_PORT": str(dns_port),
            "API_HOST": "127.0.0.1",
            "API_PORT": str(api_port),
            "SYNC_INTERVAL": SYNC_INTERVAL,
            "HTTP_TIMEOUT": "3",
            "PYTHONUNBUFFERED": "1",
        }
    )
    if primary_url:
        env["PRIMARY_API_URL"] = primary_url
    log = open(DATA / f"{role}-iso.log", "w")
    return subprocess.Popen(
        [PY, "-I", "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", str(api_port)],
        cwd=str(ROOT), env=env, stdout=log, stderr=subprocess.STDOUT,
    )


def stop(proc: subprocess.Popen) -> None:
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def publish(api: str, records) -> httpx.Response:
    return httpx.post(f"{api}/api/zone", json={"records": records},
                      timeout=5)


def isolate(api: str, serial: int, name: str, request_id: str,
            duration: int) -> httpx.Response:
    return httpx.post(
        f"{api}/api/isolation",
        json={"serial": serial, "name": name,
              "request_id": request_id, "duration": duration},
        timeout=5,
    )


def version(api: str) -> dict:
    return httpx.get(f"{api}/api/version", timeout=3).json()


def isolation_view(api: str) -> dict:
    return httpx.get(f"{api}/api/isolation", timeout=3).json()


def parse_iso(ts: str) -> float:
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=timezone.utc
    ).timestamp()


def main() -> int:
    for f in ("primary-iso.db", "backup-iso.db"):
        for suffix in ("", "-wal", "-shm"):
            p = DATA / (f + suffix)
            if p.exists():
                p.unlink()

    primary = start_service(
        "primary", 18020, 15060, str(DATA / "primary-iso.db"), None
    )
    backup = start_service(
        "backup", 18021, 15061, str(DATA / "backup-iso.db"), PRIMARY_API
    )
    try:
        wait_http(f"{PRIMARY_API}/health")
        wait_http(f"{BACKUP_API}/health")
        resp = publish(PRIMARY_API, [
            {"name": f"db.{ZONE}", "ip": "10.0.0.11"},
            {"name": f"web.{ZONE}", "ip": "10.0.0.21"},
        ])
        assert resp.status_code == 201 and resp.json()["serial"] == 1
        check("backup synced serial 1",
              wait_rcode(BACKUP_DNS, f"db.{ZONE}", RCODE.NOERROR))

        print("[1] submit isolation req-1 for db (30s) on the primary")
        t0 = time.time()
        r = isolate(PRIMARY_API, 1, f"db.{ZONE}", "req-1", 30)
        check("submit -> 201 created",
              r.status_code == 201 and r.json()["status"] == "created",
              r.text)
        iso1 = r.json()["isolation"]
        expires_epoch = parse_iso(iso1["expires_at"])
        check("deadline ~30s ahead, serial untouched",
              28 <= expires_epoch - t0 <= 32
              and iso1["serial"] == 1
              and version(PRIMARY_API)["serial"] == 1,
              f"iso={iso1} serial={version(PRIMARY_API)['serial']}")

        check("primary SERVFAIL for isolated name",
              rcode(PRIMARY_DNS, f"db.{ZONE}") == RCODE.SERVFAIL)
        check("primary still answers other name",
              rcode(PRIMARY_DNS, f"web.{ZONE}") == RCODE.NOERROR)
        check("unknown name still NXDOMAIN",
              rcode(PRIMARY_DNS, f"missing.{ZONE}") == RCODE.NXDOMAIN)
        check("AAAA for isolated name stays NOERROR/NODATA",
              rcode(PRIMARY_DNS, f"db.{ZONE}", "AAAA") == RCODE.NOERROR)

        print("[2] isolation replicates to the backup without a serial bump")
        check("backup SERVFAIL after sync",
              wait_rcode(BACKUP_DNS, f"db.{ZONE}", RCODE.SERVFAIL))
        check("backup serial still 1",
              version(BACKUP_API)["serial"] == 1)
        check("backup still answers other name",
              rcode(BACKUP_DNS, f"web.{ZONE}") == RCODE.NOERROR)
        bview = isolation_view(BACKUP_API)
        check("backup /api/isolation shows req-1",
              bview["isolation"] is not None
              and bview["isolation"]["request_id"] == "req-1"
              and bview["isolation"]["expires_at"]
              == iso1["expires_at"],
              str(bview))
        check("both /api/version expose the isolation",
              version(PRIMARY_API)["isolation"]["request_id"] == "req-1"
              and version(BACKUP_API)["isolation"]["request_id"] == "req-1")
        check("both /api/zone carry the isolation block",
              httpx.get(f"{PRIMARY_API}/api/zone").json()["isolation"]
              ["request_id"] == "req-1"
              and httpx.get(f"{BACKUP_API}/api/zone").json()["isolation"]
              ["request_id"] == "req-1")

        print("[3] idempotency and validation")
        r = isolate(PRIMARY_API, 1, f"db.{ZONE}", "req-1", 30)
        check("same request retry -> 200 duplicate, deadline unchanged",
              r.status_code == 200
              and r.json()["status"] == "duplicate"
              and r.json()["isolation"]["expires_at"]
              == iso1["expires_at"],
              r.text)
        r = isolate(PRIMARY_API, 1, f"db.{ZONE}", "req-1", 60)
        check("request_id reuse with other params -> 409",
              r.status_code == 409, r.text)
        r = isolate(PRIMARY_API, 1, f"web.{ZONE}", "req-2", 30)
        check("second name while busy -> 409 (single slot)",
              r.status_code == 409, r.text)
        r = isolate(PRIMARY_API, 99, f"db.{ZONE}", "req-3", 30)
        check("stale observed serial -> 409", r.status_code == 409, r.text)
        r = isolate(PRIMARY_API, 1, f"ghost.{ZONE}", "req-4", 30)
        check("unpublished name -> 404", r.status_code == 404, r.text)
        r = isolate(PRIMARY_API, 1, f"web.{ZONE}", "req-5", 10)
        check("duration < 30 -> 422", r.status_code == 422, r.text)
        r = isolate(PRIMARY_API, 1, f"web.{ZONE}", "req-5", 301)
        check("duration > 300 -> 422", r.status_code == 422, r.text)
        r = isolate(BACKUP_API, 1, f"web.{ZONE}", "req-6", 30)
        check("submit on backup -> 403", r.status_code == 403, r.text)

        print("[4] backup restart keeps the isolation (same deadline)")
        stop(backup)
        backup = start_service(
            "backup", 18021, 15061, str(DATA / "backup-iso.db"), PRIMARY_API
        )
        wait_http(f"{BACKUP_API}/health")
        check("backup still SERVFAIL after restart",
              wait_rcode(BACKUP_DNS, f"db.{ZONE}", RCODE.SERVFAIL))
        bview = isolation_view(BACKUP_API)
        check("deadline unchanged across restart",
              bview["isolation"] is not None
              and bview["isolation"]["expires_at"] == iso1["expires_at"],
              str(bview))

        print("[5] both roles lift the block at the original deadline")
        while time.time() < expires_epoch - 2:
            time.sleep(0.5)
        check("still SERVFAIL just before the deadline",
              rcode(PRIMARY_DNS, f"db.{ZONE}") == RCODE.SERVFAIL
              and rcode(BACKUP_DNS, f"db.{ZONE}") == RCODE.SERVFAIL)
        lifted_p = wait_rcode(PRIMARY_DNS, f"db.{ZONE}", RCODE.NOERROR,
                              timeout=10)
        lifted_b = wait_rcode(BACKUP_DNS, f"db.{ZONE}", RCODE.NOERROR,
                              timeout=10)
        check("primary lifts at the deadline", lifted_p)
        check("backup lifts at the deadline", lifted_b)
        check("lift happened at/after the stored deadline",
              time.time() >= expires_epoch - 1,
              f"now={time.time()} expires={expires_epoch}")
        check("isolation gone from both status endpoints",
              isolation_view(PRIMARY_API)["isolation"] is None
              and isolation_view(BACKUP_API)["isolation"] is None)

        print("[6] partition: backup keeps SERVFAIL until the deadline")
        r = isolate(PRIMARY_API, 1, f"db.{ZONE}", "req-p1", 30)
        check("second isolation accepted after expiry",
              r.status_code == 201, r.text)
        iso_p = r.json()["isolation"]
        expires_p = parse_iso(iso_p["expires_at"])
        check("backup picked up req-p1",
              wait_rcode(BACKUP_DNS, f"db.{ZONE}", RCODE.SERVFAIL))
        # Cut the backup off from the primary entirely.
        stop(backup)
        backup = start_service(
            "backup", 18021, 15061, str(DATA / "backup-iso.db"), BLACKHOLE
        )
        wait_http(f"{BACKUP_API}/health")
        time.sleep(2.0)
        check("partitioned backup still SERVFAIL (local state)",
              rcode(BACKUP_DNS, f"db.{ZONE}") == RCODE.SERVFAIL)
        check("primary unaffected by the partition",
              rcode(PRIMARY_DNS, f"db.{ZONE}") == RCODE.SERVFAIL)
        info = version(BACKUP_API)
        check("backup sync state records the failure",
              info["last_sync_status"] == "failed", str(info))
        while time.time() < expires_p - 1:
            time.sleep(0.5)
        check("partitioned backup lifts at the original deadline",
              wait_rcode(BACKUP_DNS, f"db.{ZONE}", RCODE.NOERROR,
                         timeout=10))
        # Reconnect: backup converges back to the primary's state.
        stop(backup)
        backup = start_service(
            "backup", 18021, 15061, str(DATA / "backup-iso.db"), PRIMARY_API
        )
        wait_http(f"{BACKUP_API}/health")
        check("reconnected backup converges (no isolation)",
              isolation_view(BACKUP_API)["isolation"] is None)

        print("[7] publish invalidates the isolation immediately")
        r = isolate(PRIMARY_API, 1, f"db.{ZONE}", "req-v1", 300)
        check("long isolation accepted", r.status_code == 201, r.text)
        check("backup SERVFAIL for req-v1",
              wait_rcode(BACKUP_DNS, f"db.{ZONE}", RCODE.SERVFAIL))
        resp = publish(PRIMARY_API, [
            {"name": f"db.{ZONE}", "ip": "10.0.0.12"},
            {"name": f"web.{ZONE}", "ip": "10.0.0.22"},
        ])
        check("publish -> serial 2",
              resp.status_code == 201 and resp.json()["serial"] == 2,
              resp.text)
        r = dns_query(PRIMARY_DNS, f"db.{ZONE}")
        check("primary answers new IP at once (isolation dropped)",
              r.header.rcode == RCODE.NOERROR
              and [str(x.rdata) for x in r.rr] == ["10.0.0.12"], str(r))
        check("primary isolation view cleared",
              isolation_view(PRIMARY_API)["isolation"] is None)
        check("backup follows to serial 2 without the isolation",
              wait_rcode(BACKUP_DNS, f"db.{ZONE}", RCODE.NOERROR)
              and [str(x.rdata) for x in
                   dns_query(BACKUP_DNS, f"db.{ZONE}").rr]
              == ["10.0.0.12"])
        check("backup isolation view cleared",
              isolation_view(BACKUP_API)["isolation"] is None)

        print("[8] a late retry of the old request cannot re-cover v2")
        r = isolate(PRIMARY_API, 1, f"db.{ZONE}", "req-v1", 300)
        check("old request retry -> duplicate, no new isolation",
              r.status_code == 200 and r.json()["status"] == "duplicate",
              r.text)
        r = dns_query(PRIMARY_DNS, f"db.{ZONE}")
        check("v2 still answers normally after the stale retry",
              r.header.rcode == RCODE.NOERROR
              and [str(x.rdata) for x in r.rr] == ["10.0.0.12"], str(r))
        check("isolation view still clear",
              isolation_view(PRIMARY_API)["isolation"] is None)
        r = isolate(PRIMARY_API, 2, f"db.{ZONE}", "req-v2", 30)
        check("fresh isolation against serial 2 works",
              r.status_code == 201, r.text)
        check("primary SERVFAIL again for the new isolation",
              rcode(PRIMARY_DNS, f"db.{ZONE}") == RCODE.SERVFAIL)
    finally:
        stop(primary)
        if backup.poll() is None:
            stop(backup)

    print(f"\n==== {PASS} passed, {FAIL} failed ====")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
