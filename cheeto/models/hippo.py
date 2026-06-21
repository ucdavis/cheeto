from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pymongo
from beanie import Link
from pymongo import IndexModel
from pydantic import Field, field_validator

from ..constants import HIPPO_EVENT_ACTIONS, HIPPO_EVENT_STATUSES
from .base import BaseDocument
from .group import Group
from .site import Site
from .user import User


class HippoEvent(BaseDocument):
    """Rich, queryable record of a HiPPO API event we've ingested."""

    # Identity (from hippo)
    hippo_id: int
    hippo_endpoint: str
    action: str
    status: str = 'Pending'
    n_tries: int = 0
    last_error: str | None = None

    # Resolved context
    cluster: str = ''
    site: Link[Site] | None = None

    target_username: str | None = None
    target_user: Link[User] | None = None

    target_groupnames: list[str] = Field(default_factory=list)
    target_groups: list[Link[Group]] = Field(default_factory=list)

    sponsor_username: str | None = None

    # Original payload (for debugging + idempotency)
    raw: dict[str, Any] = Field(default_factory=dict)

    # Timestamps specific to the event lifecycle
    queued_at: datetime | None = None
    first_seen_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    completed_at: datetime | None = None
    # When the terminal status was successfully posted back to the HiPPO
    # API; None = not yet (post_back disabled or the postback call failed).
    # Processing is gated on `status`, postback healing on this field.
    posted_back_at: datetime | None = None

    @field_validator('action')
    @classmethod
    def validate_action(cls, v):
        if v not in HIPPO_EVENT_ACTIONS:
            raise ValueError(f'Invalid HiPPO event action: {v}')
        return v

    @field_validator('status')
    @classmethod
    def validate_status(cls, v):
        if v not in HIPPO_EVENT_STATUSES:
            raise ValueError(f'Invalid HiPPO event status: {v}')
        return v

    class Settings:
        name = 'hippo_events'
        indexes = [
            IndexModel(
                [('hippo_id', pymongo.ASCENDING),
                 ('hippo_endpoint', pymongo.ASCENDING)],
                unique=True,
            ),
            IndexModel([('status', pymongo.ASCENDING)]),
            IndexModel([('action', pymongo.ASCENDING)]),
            IndexModel([('site', pymongo.ASCENDING)]),
            IndexModel([('target_user', pymongo.ASCENDING)]),
            IndexModel([('target_groups', pymongo.ASCENDING)]),
            IndexModel([('first_seen_at', pymongo.DESCENDING)]),
        ]
