"""FastAPI entry point: REST API (with Swagger UI) + in-process UDP DNS."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Response

from .config import settings
from .dns_server import dns_service
from .models import (
    IsolationOut,
    IsolationRequest,
    IsolationStatus,
    IsolationSubmitResult,
    IsolationView,
    PublishResult,
    RecordOut,
    VersionOut,
    ZoneOut,
    ZonePayload,
)
from .storage import (
    ISO_BUSY,
    ISO_CONFLICT_REQUEST_ID,
    ISO_CREATED,
    ISO_NAME_NOT_FOUND,
    ISO_STALE_SERIAL,
    isolation_effective,
    storage,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("main")


def _effective_isolation(
    serial: int, isolation: dict | None
) -> dict | None:
    """The isolation only counts when it matches this exact serial and
    its absolute deadline has not passed."""
    return isolation if isolation_effective(isolation, serial) else None


def _zone_out() -> ZoneOut:
    serial, records, published_at, isolation = storage.get_active_state()
    isolation = _effective_isolation(serial, isolation)
    return ZoneOut(
        zone=settings.dns_zone,
        serial=serial,
        records=[RecordOut(**r) for r in records],
        published_at=published_at,
        isolation=IsolationOut.from_row(isolation) if isolation else None,
    )


def _version_out() -> VersionOut:
    serial, records, published_at, isolation = storage.get_active_state()
    isolation = _effective_isolation(serial, isolation)
    sync_state = storage.get_sync_state()
    return VersionOut(
        role=settings.role,
        zone=settings.dns_zone,
        serial=serial,
        record_count=len(records),
        published_at=published_at,
        last_sync_at=sync_state["last_sync_at"],
        last_sync_status=sync_state["last_sync_status"],
        last_sync_error=sync_state["last_sync_error"] or None,
        upstream_serial=sync_state["upstream_serial"],
        isolation=(
            IsolationStatus.from_row(isolation) if isolation else None
        ),
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info(
        "starting role=%s zone=%s db=%s dns_udp=%s api=%s",
        settings.role,
        settings.dns_zone,
        settings.db_path,
        settings.dns_port,
        settings.api_port,
    )
    dns_service.start()
    if not settings.is_primary:
        from .sync import sync_loop

        await sync_loop.start()
    try:
        yield
    finally:
        if not settings.is_primary:
            from .sync import sync_loop

            await sync_loop.stop()
        dns_service.stop()


app = FastAPI(
    title="Primary/Backup DNS for fixed intranet zone",
    version="1.1.0",
    description=(
        "Primary accepts a complete zone (up to 8 A records); the backup "
        "pulls whole versions over HTTP and answers A queries over UDP. "
        "Every answer uses one immutable version. A host-isolation API "
        "temporarily blocks one published A name (SERVFAIL) on both "
        "servers without touching the zone serial."
    ),
    lifespan=lifespan,
)


@app.get("/health", tags=["ops"])
def health() -> dict:
    return {"status": "ok", "role": settings.role, "zone": settings.dns_zone}


@app.get("/api/version", response_model=VersionOut, tags=["zone"])
def get_version() -> VersionOut:
    return _version_out()


@app.get("/api/zone", response_model=ZoneOut, tags=["zone"])
def get_zone() -> ZoneOut:
    return _zone_out()


# ---------------------------------------------------------------- primary
@app.post(
    "/api/zone",
    response_model=PublishResult,
    status_code=201,
    tags=["primary"],
)
def publish_zone(payload: ZonePayload) -> PublishResult:
    """Replace the whole zone. The serial increments only on success."""
    if not settings.is_primary:
        raise HTTPException(
            status_code=403,
            detail="zone publishing is only allowed on the primary",
        )
    records = [r.model_dump() for r in payload.sorted_records()]
    serial, published_at = storage.next_publish(records)
    log.info("published new version serial=%s records=%s",
             serial, len(records))
    return PublishResult(
        ok=True,
        serial=serial,
        record_count=len(records),
        published_at=published_at,
    )


# -------------------------------------------------------------- isolation
@app.post(
    "/api/isolation",
    response_model=IsolationSubmitResult,
    tags=["isolation"],
)
def submit_isolation(
    payload: IsolationRequest, response: Response
) -> IsolationSubmitResult:
    """Isolate one published A name for 30-300 seconds (primary only).

    The request carries the zone serial the operator observed, the
    published A name, an idempotency key (``request_id``) and the
    duration. Only one name can be isolated at a time. The isolation
    does not change the zone serial; it replicates to the backup with
    the normal zone sync. A retry of the same ``request_id`` returns the
    stored isolation unchanged and never extends its deadline.
    """
    if not settings.is_primary:
        raise HTTPException(
            status_code=403,
            detail="isolation is only accepted on the primary",
        )
    status, isolation, active_serial = storage.submit_isolation(
        request_id=payload.request_id,
        name=payload.name,
        serial=payload.serial,
        duration=payload.duration,
    )
    if status == ISO_STALE_SERIAL:
        raise HTTPException(
            status_code=409,
            detail=(
                f"observed serial {payload.serial} does not match the "
                f"active serial {active_serial}; re-read /api/zone and "
                "file the isolation against the current serial"
            ),
        )
    if status == ISO_NAME_NOT_FOUND:
        raise HTTPException(
            status_code=404,
            detail=(
                f"no published A record named {payload.name!r} in the "
                f"active zone (serial {active_serial})"
            ),
        )
    if status == ISO_CONFLICT_REQUEST_ID:
        raise HTTPException(
            status_code=409,
            detail=(
                f"request_id {payload.request_id!r} was already used with "
                "different parameters; choose a new request_id"
            ),
        )
    if status == ISO_BUSY:
        raise HTTPException(
            status_code=409,
            detail=(
                f"only one name can be isolated at a time; "
                f"{isolation['name']!r} is isolated until "
                f"{isolation['expires_at']} "
                f"(request_id={isolation['request_id']!r})"
            ),
        )
    # created | duplicate: the stored row is authoritative; a duplicate
    # retry returns it unchanged and never extends the deadline.
    log.info(
        "isolation %s request=%s name=%s serial=%s expires=%s",
        status, isolation["request_id"], isolation["name"],
        isolation["serial"], isolation["expires_at"],
    )
    if status == ISO_CREATED:
        response.status_code = 201
    return IsolationSubmitResult(
        ok=True,
        status=status,
        isolation=IsolationStatus.from_row(isolation),
    )


@app.get("/api/isolation", response_model=IsolationView, tags=["isolation"])
def get_isolation() -> IsolationView:
    """Current effective isolation on this machine (both roles)."""
    serial, _, _, isolation = storage.get_active_state()
    isolation = _effective_isolation(serial, isolation)
    return IsolationView(
        zone=settings.dns_zone,
        serial=serial,
        isolation=(
            IsolationStatus.from_row(isolation) if isolation else None
        ),
    )


# ----------------------------------------------------------------- backup
@app.post("/api/sync", tags=["backup"])
async def manual_sync() -> dict:
    """Pull the complete current zone from the primary once."""
    if settings.is_primary:
        raise HTTPException(
            status_code=403,
            detail="sync is only available on the backup",
        )
    from .sync import sync_loop

    outcome = await sync_loop.trigger()
    result = outcome.as_dict()
    result["zone"] = _zone_out().model_dump()
    return result
