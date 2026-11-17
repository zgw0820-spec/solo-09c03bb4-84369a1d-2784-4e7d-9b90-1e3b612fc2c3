"""SQLite persistence.

Each role has its own database file. A publish/sync is committed as one
transaction: the new immutable version row and the pointer to the active
serial move together, so an answer can never be assembled from two
versions. A stale transfer (serial <= active serial) is rejected, which
guarantees a late/old transfer can never roll back an already active
version.

Host isolation state lives next to the versions:

* ``isolations`` keeps every accepted isolation request (keyed by
  ``request_id``) so a retry of the same request is idempotent and can
  never extend the original absolute deadline;
* the ``current_isolation`` state key points at the isolation that holds
  the single slot. An isolation is *effective* only while its stored
  serial equals the active serial and its deadline has not passed, so an
  old isolation can never re-cover a newer zone version;
* publishing (primary) or committing a synced version (backup) clears
  the pointer inside the same transaction, which invalidates the old
  isolation immediately.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .config import settings

_UTC = timezone.utc
_init_lock = threading.Lock()

# Isolation submission outcomes (Storage.submit_isolation).
ISO_CREATED = "created"
ISO_DUPLICATE = "duplicate"  # same request_id + same params: no extension
ISO_CONFLICT_REQUEST_ID = "conflict_request_id"  # id reused, other params
ISO_STALE_SERIAL = "stale_serial"  # observed serial != active serial
ISO_NAME_NOT_FOUND = "name_not_found"  # name not published in active zone
ISO_BUSY = "busy"  # another isolation currently holds the slot


def utcnow_iso() -> str:
    return datetime.now(_UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _iso_from_epoch(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, _UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _isolation_row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "request_id": row["request_id"],
        "name": row["name"],
        "serial": row["serial"],
        "duration": row["duration"],
        "created_at": row["created_at"],
        "expires_at": row["expires_at"],
        "expires_epoch": row["expires_epoch"],
    }


def isolation_effective(
    isolation: Optional[Dict[str, Any]],
    serial: int,
    now: Optional[float] = None,
) -> bool:
    """An isolation only ever applies to the zone serial it was filed
    against, and only until its absolute deadline. The serial match is
    what stops an old isolation from re-covering a newer zone, no matter
    in which order publishes and sync responses interleave."""
    if isolation is None or isolation["serial"] != serial:
        return False
    return (time.time() if now is None else now) < isolation["expires_epoch"]


def _connect(db_path: str) -> sqlite3.Connection:
    directory = os.path.dirname(os.path.abspath(db_path))
    os.makedirs(directory, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


class Storage:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._lock = threading.Lock()
        with _init_lock:
            self._init_schema()

    # -- schema ---------------------------------------------------------
    def _init_schema(self) -> None:
        conn = self._connect()
        try:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS zone_versions (
                    serial       INTEGER PRIMARY KEY,
                    records_json TEXT    NOT NULL,
                    record_count INTEGER NOT NULL,
                    published_at TEXT    NOT NULL
                );
                CREATE TABLE IF NOT EXISTS state (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS isolations (
                    request_id    TEXT PRIMARY KEY,
                    name          TEXT NOT NULL,
                    serial        INTEGER NOT NULL,
                    duration      INTEGER NOT NULL,
                    created_at    TEXT NOT NULL,
                    expires_at    TEXT NOT NULL,
                    expires_epoch REAL NOT NULL
                );
                """
            )
            row = conn.execute(
                "SELECT value FROM state WHERE key='active_serial'"
            ).fetchone()
            if row is None:
                # Empty seed version: the service answers NXDOMAIN but is
                # internally consistent (one active version) before publish.
                conn.execute(
                    "INSERT OR IGNORE INTO zone_versions "
                    "(serial, records_json, record_count, published_at) "
                    "VALUES (0, '[]', 0, ?)",
                    (utcnow_iso(),),
                )
                conn.execute(
                    "INSERT OR IGNORE INTO state(key, value) "
                    "VALUES ('active_serial', '0')"
                )
        finally:
            conn.close()

    def _connect(self) -> sqlite3.Connection:
        return _connect(self.db_path)

    # -- reads ----------------------------------------------------------
    @staticmethod
    def _get_state(conn: sqlite3.Connection, key: str) -> Optional[str]:
        row = conn.execute(
            "SELECT value FROM state WHERE key=?", (key,)
        ).fetchone()
        return None if row is None else row["value"]

    @staticmethod
    def _get_isolation_by_id(
        conn: sqlite3.Connection, request_id: str
    ) -> Optional[Dict[str, Any]]:
        row = conn.execute(
            "SELECT * FROM isolations WHERE request_id=?", (request_id,)
        ).fetchone()
        return _isolation_row_to_dict(row) if row else None

    def _get_current_isolation(
        self, conn: sqlite3.Connection
    ) -> Optional[Dict[str, Any]]:
        request_id = self._get_state(conn, "current_isolation")
        if not request_id:
            return None
        return self._get_isolation_by_id(conn, request_id)

    def get_active_state(
        self,
    ) -> Tuple[int, List[Dict[str, str]], Optional[str], Optional[Dict[str, Any]]]:
        """One consistent snapshot: (serial, records, published_at, isolation).

        All four values are read inside a single read transaction, so a
        DNS answer or an API response always pairs the zone version with
        the isolation state that belongs to it. ``isolation`` is the raw
        slot row (or None); callers decide effectiveness via
        :func:`isolation_effective`.
        """
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            serial = int(self._get_state(conn, "active_serial") or "0")
            row = conn.execute(
                "SELECT records_json, published_at FROM zone_versions "
                "WHERE serial=?",
                (serial,),
            ).fetchone()
            records = json.loads(row["records_json"]) if row else []
            isolation = self._get_current_isolation(conn)
            conn.execute("COMMIT")
            return serial, records, (row["published_at"] if row else None), isolation
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def get_active_version(
        self,
    ) -> Tuple[int, List[Dict[str, str]], Optional[str]]:
        """Return (serial, records, published_at) of one active version."""
        serial, records, published_at, _ = self.get_active_state()
        return serial, records, published_at

    def active_serial(self) -> int:
        serial, _, _, _ = self.get_active_state()
        return serial

    def get_sync_state(self) -> Dict[str, Any]:
        conn = self._connect()
        try:
            def g(key: str) -> Optional[str]:
                return self._get_state(conn, key)

            return {
                "active_serial": int(g("active_serial") or "0"),
                "last_sync_at": g("last_sync_at"),
                "last_sync_status": g("last_sync_status") or "never",
                "last_sync_error": g("last_sync_error"),
                "upstream_serial": (
                    int(g("upstream_serial"))
                    if g("upstream_serial") is not None
                    else None
                ),
            }
        finally:
            conn.close()

    def version_exists(self, serial: int) -> bool:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT 1 FROM zone_versions WHERE serial=?", (serial,)
            ).fetchone()
            return row is not None
        finally:
            conn.close()

    # -- writes ---------------------------------------------------------
    @staticmethod
    def _clear_isolation_slot(conn: sqlite3.Connection) -> None:
        conn.execute("DELETE FROM state WHERE key='current_isolation'")

    def _set_isolation_slot(
        self, conn: sqlite3.Connection, isolation: Dict[str, Any]
    ) -> None:
        conn.execute(
            "INSERT INTO isolations "
            "(request_id, name, serial, duration, created_at, expires_at, "
            " expires_epoch) VALUES (?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(request_id) DO UPDATE SET "
            "name=excluded.name, serial=excluded.serial, "
            "duration=excluded.duration, created_at=excluded.created_at, "
            "expires_at=excluded.expires_at, "
            "expires_epoch=excluded.expires_epoch",
            (
                isolation["request_id"],
                isolation["name"],
                isolation["serial"],
                isolation["duration"],
                isolation["created_at"],
                isolation["expires_at"],
                isolation["expires_epoch"],
            ),
        )
        conn.execute(
            "INSERT INTO state(key, value) VALUES('current_isolation', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (isolation["request_id"],),
        )

    def commit_version(
        self,
        serial: int,
        records: List[Dict[str, str]],
        published_at: str,
        isolation: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Atomically store a version and make it the active one.

        The isolation slot moves in the same transaction: it is replaced
        by the isolation that arrived with this version (only when that
        isolation belongs to the new serial), or cleared — so accepting
        a new zone invalidates the old isolation immediately and an old
        isolation can never settle on the new zone.

        Returns True when applied, False when rejected as a stale rollback.
        """
        if serial < 1:
            raise ValueError("serial must be a positive integer")
        payload = json.dumps(records, separators=(",", ":"), sort_keys=True)
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                active = int(
                    self._get_state(conn, "active_serial") or "0"
                )
                if serial <= active:
                    # Late/old transfer or duplicate: never roll back.
                    conn.execute("COMMIT")
                    return False
                conn.execute(
                    "INSERT OR REPLACE INTO zone_versions "
                    "(serial, records_json, record_count, published_at) "
                    "VALUES (?, ?, ?, ?)",
                    (serial, payload, len(records), published_at),
                )
                conn.execute(
                    "UPDATE state SET value=? WHERE key='active_serial'",
                    (str(serial),),
                )
                if isolation is not None and isolation.get("serial") == serial:
                    self._set_isolation_slot(conn, isolation)
                else:
                    self._clear_isolation_slot(conn)
                conn.execute("COMMIT")
                return True
            except Exception:
                conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()

    def next_publish(
        self, records: List[Dict[str, str]]
    ) -> Tuple[int, str]:
        """Primary only: increment serial only after a valid publish.

        The isolation slot is cleared in the same transaction: a publish
        invalidates the old isolation immediately.
        """
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                next_serial = int(
                    self._get_state(conn, "active_serial") or "0"
                ) + 1
                published_at = utcnow_iso()
                conn.execute(
                    "INSERT INTO zone_versions "
                    "(serial, records_json, record_count, published_at) "
                    "VALUES (?, ?, ?, ?)",
                    (
                        next_serial,
                        json.dumps(
                            records, separators=(",", ":"), sort_keys=True
                        ),
                        len(records),
                        published_at,
                    ),
                )
                conn.execute(
                    "UPDATE state SET value=? WHERE key='active_serial'",
                    (str(next_serial),),
                )
                self._clear_isolation_slot(conn)
                conn.execute("COMMIT")
                return next_serial, published_at
            except Exception:
                conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()

    def submit_isolation(
        self,
        request_id: str,
        name: str,
        serial: int,
        duration: int,
    ) -> Tuple[str, Optional[Dict[str, Any]], int]:
        """Primary only: file one host-isolation request atomically.

        Returns (status, isolation_or_None, active_serial). All checks
        and the insert happen in one IMMEDIATE transaction, so a
        concurrent publish or a concurrent isolation request cannot
        interleave with the decision:

        * same ``request_id`` + same parameters -> ``duplicate`` with the
          stored row (a retry never extends the original deadline);
        * ``request_id`` reused with different parameters -> conflict;
        * ``serial`` must equal the active serial (the isolation targets
          exactly the zone version the operator observed);
        * ``name`` must be a published A name of the active version;
        * only one name may be isolated at a time: while the slot holds
          an effective isolation the request is rejected as busy.
        """
        now = time.time()
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                active = int(self._get_state(conn, "active_serial") or "0")

                existing = self._get_isolation_by_id(conn, request_id)
                if existing is not None:
                    if (
                        existing["name"] == name
                        and existing["serial"] == serial
                        and existing["duration"] == duration
                    ):
                        conn.execute("COMMIT")
                        return ISO_DUPLICATE, existing, active
                    conn.execute("COMMIT")
                    return ISO_CONFLICT_REQUEST_ID, existing, active

                if serial != active:
                    conn.execute("COMMIT")
                    return ISO_STALE_SERIAL, None, active

                row = conn.execute(
                    "SELECT records_json FROM zone_versions WHERE serial=?",
                    (active,),
                ).fetchone()
                records = json.loads(row["records_json"]) if row else []
                if name not in {r["name"] for r in records}:
                    conn.execute("COMMIT")
                    return ISO_NAME_NOT_FOUND, None, active

                current = self._get_current_isolation(conn)
                if isolation_effective(current, active, now):
                    conn.execute("COMMIT")
                    return ISO_BUSY, current, active

                expires_epoch = now + duration
                isolation = {
                    "request_id": request_id,
                    "name": name,
                    "serial": serial,
                    "duration": duration,
                    "created_at": _iso_from_epoch(now),
                    "expires_at": _iso_from_epoch(expires_epoch),
                    "expires_epoch": expires_epoch,
                }
                self._set_isolation_slot(conn, isolation)
                conn.execute("COMMIT")
                return ISO_CREATED, isolation, active
            except Exception:
                conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()

    def apply_isolation(
        self, serial: int, isolation: Optional[Dict[str, Any]]
    ) -> str:
        """Backup only: reconcile the isolation slot for an *unchanged*
        zone serial with the state just pulled from the primary.

        Returns "updated", "unchanged" or "ignored" (when the local
        active serial no longer matches, e.g. a stale sync response
        arriving late — it must never re-cover a newer zone).
        """
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                active = int(self._get_state(conn, "active_serial") or "0")
                if serial != active:
                    conn.execute("COMMIT")
                    return "ignored"
                current = self._get_current_isolation(conn)
                if isolation is None:
                    if current is None:
                        conn.execute("COMMIT")
                        return "unchanged"
                    self._clear_isolation_slot(conn)
                    conn.execute("COMMIT")
                    return "updated"
                if isolation.get("serial") != active:
                    # Isolation belongs to another zone version: never
                    # let it settle on this one.
                    conn.execute("COMMIT")
                    return "ignored"
                if current == isolation:
                    conn.execute("COMMIT")
                    return "unchanged"
                self._set_isolation_slot(conn, isolation)
                conn.execute("COMMIT")
                return "updated"
            except Exception:
                conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()

    def update_sync_state(
        self,
        status: str,
        error: Optional[str],
        upstream_serial: Optional[int],
    ) -> None:
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "INSERT INTO state(key, value) VALUES('last_sync_at', ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (utcnow_iso(),),
                )
                conn.execute(
                    "INSERT INTO state(key, value) "
                    "VALUES('last_sync_status', ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (status,),
                )
                conn.execute(
                    "INSERT INTO state(key, value) "
                    "VALUES('last_sync_error', ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (error or "",),
                )
                if upstream_serial is not None:
                    conn.execute(
                        "INSERT INTO state(key, value) "
                        "VALUES('upstream_serial', ?) "
                        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                        (str(upstream_serial),),
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()


storage = Storage(settings.db_path)
