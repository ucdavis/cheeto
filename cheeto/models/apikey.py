from datetime import datetime, timezone
from typing import Annotated, Literal

import pymongo
from pydantic import Field, field_validator
from pymongo import IndexModel

from ..apikey import normalize_scopes
from ..constants import API_SCOPE_ALL
from .base import BaseDocument

ApiKeyState = Literal['active', 'expired', 'revoked']


def _utc(when: datetime) -> datetime:
    """Datetimes come back from MongoDB naive, in UTC."""
    return when if when.tzinfo is not None else when.replace(
        tzinfo=timezone.utc,
    )


class ApiKey(BaseDocument):
    """A key for the REST API (`cheeto/daemon/`). See `cheeto/apikey.py`
    for the format. The secret itself is never stored.

    `name` labels the client and is NOT unique: two keys of one client can
    overlap during a rotation. `key_id` is the identity. A revoked key
    keeps its record (`revoked_at`), for the audit trail.
    """

    key_id: Annotated[str, Field(pattern=r'^[0-9a-f]{12}$')]
    name: Annotated[str, Field(min_length=1, max_length=64)]
    secret_hash: str
    scopes: list[str]
    comment: str = ''

    expires_at: datetime | None = None
    revoked_at: datetime | None = None
    # Written by the API itself, at most once per
    # `daemon.deps.KEY_TOUCH_INTERVAL` for each key.
    last_used_at: datetime | None = None

    @field_validator('scopes')
    @classmethod
    def validate_scopes(cls, v):
        return normalize_scopes(v)

    def state(self, now: datetime | None = None) -> ApiKeyState:
        if self.revoked_at is not None:
            return 'revoked'
        now = now or datetime.now(timezone.utc)
        if self.expires_at is not None and _utc(self.expires_at) <= now:
            return 'expired'
        return 'active'

    def allows(self, scope: str) -> bool:
        return API_SCOPE_ALL in self.scopes or scope in self.scopes

    class Settings:
        name = 'api_keys'
        indexes = [
            IndexModel([('key_id', pymongo.ASCENDING)], unique=True),
        ]
