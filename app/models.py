"""Pydantic models and zone validation rules shared by both roles."""
from __future__ import annotations

import ipaddress
import re
import time
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from .config import settings

_LABEL = re.compile(r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?$")
_REQUEST_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")

# Host-isolation duration bounds, in seconds (operator-facing contract).
MIN_ISOLATION_DURATION = 30
MAX_ISOLATION_DURATION = 300


def _normalise_name(v: str) -> str:
    if not isinstance(v, str):
        raise ValueError("name must be a string")
    return v.strip().rstrip(".").lower()


class RecordInput(BaseModel):
    name: str = Field(..., examples=["db.internal.local"])
    ip: str = Field(..., examples=["10.0.0.11"])

    @field_validator("name")
    @classmethod
    def _strip_name(cls, v: str) -> str:
        return _normalise_name(v)

    @field_validator("ip")
    @classmethod
    def _valid_ip(cls, v: str) -> str:
        try:
            ip = ipaddress.ip_address(v.strip())
        except ValueError as exc:
            raise ValueError(f"invalid IP address: {v}") from exc
        if ip.version != 4:
            raise ValueError("only IPv4 A records are supported")
        return str(ip)


class ZonePayload(BaseModel):
    """A complete replacement of the fixed zone (max 8 A records)."""

    records: List[RecordInput]

    @model_validator(mode="after")
    def _validate_zone(self) -> "ZonePayload":
        if not self.records:
            raise ValueError("zone must contain at least one A record")
        if len(self.records) > settings.max_records:
            raise ValueError(
                f"a zone may contain at most {settings.max_records} A records"
            )
        zone = settings.dns_zone.rstrip(".").lower()
        suffix = "." + zone
        seen: set[str] = set()
        for rec in self.records:
            name = rec.name
            if name != zone and not name.endswith(suffix):
                raise ValueError(
                    f"record {name!r} is outside the fixed zone {zone!r}"
                )
            owner = name[: -len(suffix)] if name.endswith(suffix) else ""
            labels = owner.split(".") if owner else []
            if any(not _LABEL.match(label) for label in labels):
                raise ValueError(f"invalid owner name: {name!r}")
            if name in seen:
                raise ValueError(f"duplicate A record name: {name!r}")
            seen.add(name)
        return self

    def sorted_records(self) -> List[RecordInput]:
        return sorted(self.records, key=lambda r: r.name)


class RecordOut(BaseModel):
    name: str
    ip: str


class IsolationOut(BaseModel):
    """The stored isolation slot as replicated between the two roles.

    The field set is deterministic for a given slot row so the primary's
    and the backup's ``/api/zone`` payloads stay comparable.
    """

    request_id: str
    name: str
    serial: int
    duration: int
    created_at: str
    expires_at: str
    expires_epoch: float

    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "IsolationOut":
        return cls(**row)


class IsolationStatus(IsolationOut):
    """Isolation as observed right now, with the time left."""

    remaining_seconds: float

    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "IsolationStatus":
        return cls(
            **row,
            remaining_seconds=round(
                max(0.0, row["expires_epoch"] - time.time()), 3
            ),
        )


class IsolationRequest(BaseModel):
    """Operator request to isolate one published A name temporarily."""

    serial: int = Field(
        ..., ge=0, description="zone serial the operator observed"
    )
    name: str = Field(..., examples=["db.internal.local"])
    request_id: str = Field(
        ...,
        min_length=1,
        max_length=128,
        description="idempotency key; a retry never extends the deadline",
    )
    duration: int = Field(
        ...,
        ge=MIN_ISOLATION_DURATION,
        le=MAX_ISOLATION_DURATION,
        description="isolation lifetime in seconds (30-300)",
    )

    @field_validator("name")
    @classmethod
    def _strip_name(cls, v: str) -> str:
        return _normalise_name(v)

    @field_validator("request_id")
    @classmethod
    def _valid_request_id(cls, v: str) -> str:
        v = v.strip()
        if not _REQUEST_ID.match(v):
            raise ValueError(
                "request_id must be 1-128 chars of [A-Za-z0-9._:-] "
                "and start with a letter or digit"
            )
        return v


class IsolationSubmitResult(BaseModel):
    ok: bool
    status: str  # "created" or "duplicate" (idempotent retry)
    isolation: IsolationStatus


class IsolationView(BaseModel):
    zone: str
    serial: int
    isolation: Optional[IsolationStatus] = None


class ZoneOut(BaseModel):
    zone: str
    serial: int
    records: List[RecordOut]
    published_at: Optional[str] = None
    isolation: Optional[IsolationOut] = None


class VersionOut(BaseModel):
    role: str
    zone: str
    serial: int
    record_count: int
    published_at: Optional[str] = None
    last_sync_at: Optional[str] = None
    last_sync_status: str = "never"
    last_sync_error: Optional[str] = None
    upstream_serial: Optional[int] = None
    isolation: Optional[IsolationStatus] = None


class PublishResult(BaseModel):
    ok: bool
    serial: int
    record_count: int
    published_at: str
