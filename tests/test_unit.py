"""Unit tests for validation and the SQLite version/rollback guarantees."""
import os
import tempfile
import time

import pytest


@pytest.fixture()
def storage(tmp_path, monkeypatch):
    db = str(tmp_path / "test.db")
    monkeypatch.setenv("DB_PATH", db)
    monkeypatch.setenv("DNS_ZONE", "internal.local")
    monkeypatch.setenv("MAX_RECORDS", "8")
    # Import fresh so settings + storage bind to the temp env.
    import importlib

    from app import config, storage as storage_mod

    importlib.reload(config)
    importlib.reload(storage_mod)
    yield storage_mod
    storage_mod.storage._connect().close()


def rec(name, ip):
    return {"name": name, "ip": ip}


def test_seed_starts_at_serial_zero(storage):
    assert storage.storage.active_serial() == 0
    serial, records, _ = storage.storage.get_active_version()
    assert serial == 0 and records == []


def test_publish_increments_serial_only_on_success(storage):
    serial, _ = storage.storage.next_publish(
        [rec("db.internal.local", "10.0.0.1")]
    )
    assert serial == 1
    serial, _ = storage.storage.next_publish(
        [rec("db.internal.local", "10.0.0.2")]
    )
    assert serial == 2
    # Active version is exactly one immutable snapshot.
    active, records, _ = storage.storage.get_active_version()
    assert active == 2 and records == [rec("db.internal.local", "10.0.0.2")]


def test_stale_transfer_cannot_roll_back(storage):
    storage.storage.next_publish([rec("a.internal.local", "10.0.0.1")])
    storage.storage.commit_version(
        2, [rec("b.internal.local", "10.0.0.2")], "t2"
    )
    assert storage.storage.active_serial() == 2
    # Late arrival of an old complete version.
    applied = storage.storage.commit_version(
        1, [rec("old.internal.local", "10.9.9.9")], "t1-late"
    )
    assert applied is False
    assert storage.storage.active_serial() == 2
    _, records, _ = storage.storage.get_active_version()
    assert records == [rec("b.internal.local", "10.0.0.2")]
    # Re-delivery of the same serial is idempotent and rejected too.
    assert (
        storage.storage.commit_version(
            2, [rec("b.internal.local", "10.0.0.2")], "t2-dup"
        )
        is False
    )


def test_failed_commit_leaves_old_version(storage):
    storage.storage.next_publish([rec("a.internal.local", "10.0.0.1")])
    with pytest.raises(Exception):
        # serial < 1 is invalid; nothing should have changed.
        storage.storage.commit_version(
            0, [rec("x.internal.local", "10.0.0.9")], "bad"
        )
    assert storage.storage.active_serial() == 1


def test_zone_validation_rules():
    from app.models import ZonePayload

    def payload(rows):
        return ZonePayload.model_validate(
            {"records": [{"name": n, "ip": ip} for n, ip in rows]}
        )

    ok = payload(
        [
            ("db.internal.local", "10.0.0.1"),
            ("web.internal.local", "10.0.0.2"),
        ]
    )
    assert len(ok.records) == 2

    with pytest.raises(Exception):
        payload([])  # empty zone
    with pytest.raises(Exception):
        payload([("db.external.example", "10.0.0.1")])  # outside zone
    with pytest.raises(Exception):
        payload([("db.internal.local", "not-an-ip")])
    with pytest.raises(Exception):
        payload([("db.internal.local", "fe80::1")])  # IPv6 not allowed
    with pytest.raises(Exception):
        payload(
            [
                ("db.internal.local", "10.0.0.1"),
                ("db.internal.local", "10.0.0.2"),
            ]
        )  # duplicate name
    with pytest.raises(Exception):
        payload(
            [(f"h{i}.internal.local", f"10.0.0.{i}") for i in range(9)]
        )  # > 8 records

    # Bare apex and unqualified relative names inside the zone are fine.
    apex = payload([("internal.local", "10.0.0.1")])
    assert apex.records[0].name == "internal.local"
    trailing = payload([("svc.internal.local.", "10.0.0.5")])
    assert trailing.records[0].name == "svc.internal.local"


# ----------------------------------------------------------- isolation
def _publish_two(storage):
    storage.storage.next_publish(
        [rec("db.internal.local", "10.0.0.1"),
         rec("web.internal.local", "10.0.0.2")]
    )


def test_isolation_create_and_idempotent_retry(storage):
    _publish_two(storage)
    st, iso, active = storage.storage.submit_isolation(
        "req-1", "db.internal.local", 1, 60
    )
    assert st == storage.ISO_CREATED and active == 1
    assert iso["name"] == "db.internal.local"
    assert iso["expires_epoch"] - time.time() == pytest.approx(60, abs=3)
    # Effective for the serial it was filed against.
    serial, _, _, cur = storage.storage.get_active_state()
    assert cur == iso
    assert storage.isolation_effective(cur, serial)
    assert not storage.isolation_effective(cur, serial + 1)
    assert not storage.isolation_effective(
        cur, serial, now=cur["expires_epoch"] + 0.1
    )
    # Same request retried: identical row, deadline NOT extended.
    time.sleep(0.05)
    st, iso2, _ = storage.storage.submit_isolation(
        "req-1", "db.internal.local", 1, 60
    )
    assert st == storage.ISO_DUPLICATE
    assert iso2["expires_epoch"] == iso["expires_epoch"]
    assert iso2["created_at"] == iso["created_at"]


def test_isolation_request_id_reuse_with_other_params_conflicts(storage):
    _publish_two(storage)
    storage.storage.submit_isolation("req-1", "db.internal.local", 1, 60)
    st, _, _ = storage.storage.submit_isolation(
        "req-1", "web.internal.local", 1, 60
    )
    assert st == storage.ISO_CONFLICT_REQUEST_ID
    st, _, _ = storage.storage.submit_isolation(
        "req-1", "db.internal.local", 1, 120
    )
    assert st == storage.ISO_CONFLICT_REQUEST_ID


def test_isolation_requires_matching_serial_and_published_name(storage):
    _publish_two(storage)
    st, _, active = storage.storage.submit_isolation(
        "req-a", "db.internal.local", 99, 60
    )
    assert st == storage.ISO_STALE_SERIAL and active == 1
    st, _, _ = storage.storage.submit_isolation(
        "req-b", "ghost.internal.local", 1, 60
    )
    assert st == storage.ISO_NAME_NOT_FOUND
    # Nothing was stored: the slot is still free.
    st, _, _ = storage.storage.submit_isolation(
        "req-c", "db.internal.local", 1, 60
    )
    assert st == storage.ISO_CREATED


def test_isolation_single_slot_busy_until_expiry(storage):
    _publish_two(storage)
    st, _, _ = storage.storage.submit_isolation(
        "req-1", "db.internal.local", 1, 1
    )
    assert st == storage.ISO_CREATED
    st, cur, _ = storage.storage.submit_isolation(
        "req-2", "web.internal.local", 1, 60
    )
    assert st == storage.ISO_BUSY and cur["request_id"] == "req-1"
    # Same name under a new request_id is still "busy" (no extension
    # through the back door).
    st, _, _ = storage.storage.submit_isolation(
        "req-3", "db.internal.local", 1, 60
    )
    assert st == storage.ISO_BUSY
    # After the deadline passes the slot frees up.
    time.sleep(1.05)
    st, iso, _ = storage.storage.submit_isolation(
        "req-4", "web.internal.local", 1, 60
    )
    assert st == storage.ISO_CREATED and iso["name"] == "web.internal.local"


def test_publish_invalidates_isolation_immediately(storage):
    _publish_two(storage)
    storage.storage.submit_isolation("req-1", "db.internal.local", 1, 300)
    storage.storage.next_publish([rec("db.internal.local", "10.0.0.9")])
    serial, _, _, cur = storage.storage.get_active_state()
    assert serial == 2 and cur is None
    # A retry of the old request stays a no-op duplicate, it does not
    # re-isolate the name in the new zone.
    st, iso, _ = storage.storage.submit_isolation(
        "req-1", "db.internal.local", 1, 300
    )
    assert st == storage.ISO_DUPLICATE and iso["serial"] == 1
    _, _, _, cur = storage.storage.get_active_state()
    assert cur is None


def test_commit_version_carries_isolation_atomically(storage):
    _publish_two(storage)
    iso = {
        "request_id": "req-x", "name": "db.internal.local", "serial": 2,
        "duration": 60, "created_at": "t", "expires_at": "t2",
        "expires_epoch": time.time() + 60,
    }
    applied = storage.storage.commit_version(
        2, [rec("db.internal.local", "10.0.0.2")], "t2", isolation=iso
    )
    assert applied is True
    serial, _, _, cur = storage.storage.get_active_state()
    assert serial == 2 and cur == iso
    # A newer version without isolation clears the slot.
    applied = storage.storage.commit_version(
        3, [rec("db.internal.local", "10.0.0.3")], "t3"
    )
    assert applied is True
    _, _, _, cur = storage.storage.get_active_state()
    assert cur is None
    # A stale commit is rejected and must not resurrect the old
    # isolation over the new zone.
    applied = storage.storage.commit_version(
        2, [rec("db.internal.local", "10.0.0.2")], "t2-late", isolation=iso
    )
    assert applied is False
    serial, _, _, cur = storage.storage.get_active_state()
    assert serial == 3 and cur is None
    # An isolation whose serial does not match the committed serial is
    # dropped defensively.
    applied = storage.storage.commit_version(
        4, [rec("db.internal.local", "10.0.0.4")], "t4", isolation=iso
    )
    assert applied is True
    _, _, _, cur = storage.storage.get_active_state()
    assert cur is None


def test_apply_isolation_reconciles_same_serial_only(storage):
    _publish_two(storage)
    iso = {
        "request_id": "req-s", "name": "db.internal.local", "serial": 1,
        "duration": 60, "created_at": "t", "expires_at": "t2",
        "expires_epoch": time.time() + 60,
    }
    assert storage.storage.apply_isolation(1, iso) == "updated"
    _, _, _, cur = storage.storage.get_active_state()
    assert cur == iso
    # Identical re-delivery is a no-op.
    assert storage.storage.apply_isolation(1, iso) == "unchanged"
    # A serial mismatch (late response for an old zone) is ignored.
    assert storage.storage.apply_isolation(99, iso) == "ignored"
    old = dict(iso, request_id="req-old", serial=0)
    assert storage.storage.apply_isolation(1, old) == "ignored"
    # Clearing works and is idempotent.
    assert storage.storage.apply_isolation(1, None) == "updated"
    assert storage.storage.apply_isolation(1, None) == "unchanged"
    _, _, _, cur = storage.storage.get_active_state()
    assert cur is None


def test_get_active_state_pairs_zone_and_isolation(storage):
    _publish_two(storage)
    storage.storage.submit_isolation("req-1", "db.internal.local", 1, 60)
    serial, records, published_at, iso = storage.storage.get_active_state()
    assert serial == 1
    assert {r["name"] for r in records} == {
        "db.internal.local", "web.internal.local"
    }
    assert published_at and iso["request_id"] == "req-1"
    # Legacy reader still works and ignores isolation.
    s2, r2, p2 = storage.storage.get_active_version()
    assert (s2, r2, p2) == (serial, records, published_at)
