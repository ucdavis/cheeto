"""Dirty tracking for incremental LDAP sync.

`LDAPSyncable` documents carry an embedded `ldap: LDAPInfo` maintained by a
beanie before-event hook: the hook fingerprints only the LDAP-projected
fields and bumps `modified_at` when the fingerprint changes, so bookkeeping
saves (e.g. the nightly IAM sync advancing `iam.iam_synced_at` on every
user) do NOT re-dirty the record.

Sites sync independently (one daemon beat entry per site), so "synced" is a
per-site watermark map rather than a flag: `needs_sync(site)` compares
`modified_at` against `synced[site]`. After a successful per-record sync the
LDAP ops write `ldap.synced.<site> = <the modified_at they projected>` via a
query-builder update — which bypasses document action events, so the
watermark write neither re-fires this hook nor bumps `updated_at`. Writing
the *snapshotted* `modified_at` (not now()) means a record mutated mid-sync
stays dirty.

Changes that affect a record's LDAP projection but live on OTHER documents
(SshKey, UserSiteInfo, GroupMembership, StorageVolume, AutomountMap) are
covered by propagation hooks on those documents that set
`ldap.modified_at` on the affected records directly — see `touch_ldap`.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any

from beanie import (
    Insert,
    Replace,
    Save,
    SaveChanges,
    Update,
    before_event,
)
from beanie.operators import Set
from pydantic import BaseModel, Field, field_validator


def ldap_touch() -> Set:
    """Update operator that marks a record LDAP-dirty directly. Used by the
    propagation hooks (and only them): applied through query-builder
    updates, it bypasses the target's own action events by design."""
    return Set({'ldap.modified_at': utcnow_naive()})


def utcnow_naive() -> datetime:
    """Naive-UTC now. The mongo client is tz-naive (see the note in
    cheeto/operations/iam.py), so every datetime stored in LDAPInfo must be
    naive UTC or `modified_at > synced[site]` comparisons raise TypeError."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def as_naive_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def stable_fingerprint(payload: dict[str, Any]) -> str:
    return sha256(
        json.dumps(payload, sort_keys=True, default=str).encode()
    ).hexdigest()


class LDAPInfo(BaseModel):
    """LDAP sync bookkeeping. Embedded model: no Link/DocRef fields."""

    # cheeto never projects/updates this record in LDAP (and never clears
    # its watermarks). Prune is unaffected: it keeps anything with a beanie
    # record, so an ignored record already in LDAP is left alone.
    ignore: bool = False
    fingerprint: str | None = None
    modified_at: datetime = Field(default_factory=utcnow_naive)
    synced: dict[str, datetime] = Field(default_factory=dict)

    @field_validator('modified_at')
    @classmethod
    def _naive_modified_at(cls, v: datetime) -> datetime:
        return as_naive_utc(v)

    @field_validator('synced')
    @classmethod
    def _naive_synced(cls, v: dict[str, datetime]) -> dict[str, datetime]:
        return {k: as_naive_utc(dt) for k, dt in v.items()}

    def needs_sync(self, sitename: str) -> bool:
        return self.modified_at > self.synced.get(sitename, datetime.min)


class LDAPSyncable(BaseModel):
    """Mixin for Documents projected to LDAP. Subclasses implement
    `ldap_fingerprint()` over exactly the fields their LDAP projection
    reads; the hook below is inherited and registered by beanie on each
    concrete Document class (init_actions walks dir(cls))."""

    ldap: LDAPInfo = Field(default_factory=LDAPInfo)

    def ldap_fingerprint(self) -> str:
        raise NotImplementedError

    @before_event(Insert, Replace, Save, SaveChanges, Update)
    def refresh_ldap_fingerprint(self) -> None:
        # save() pre-serializes, then fires the inner Update actions — so
        # this runs twice per save(); the compare short-circuits run two.
        fp = self.ldap_fingerprint()
        if fp != self.ldap.fingerprint:
            self.ldap.fingerprint = fp
            self.ldap.modified_at = utcnow_naive()
