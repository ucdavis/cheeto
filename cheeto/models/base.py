from datetime import datetime, timezone

from beanie import Document, Insert, PydanticObjectId, Replace, Save, SaveChanges, Update, before_event
from pydantic import BaseModel, Field


def link_target_id(link) -> PydanticObjectId | None:
    """Target id of a `Link[X]` regardless of whether it's been fetched.
    Returns None for None. Use anywhere a Link[X] needs to be compared by
    identity without a round-trip to materialize it."""
    if link is None:
        return None
    ref = getattr(link, 'ref', None)
    if ref is not None:
        return ref.id
    return link.id


class BaseDocument(Document):
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @before_event(Replace, Save, SaveChanges, Update)
    def update_timestamp(self):
        self.updated_at = datetime.now(timezone.utc)

    class Settings:
        use_state_management = True
        is_root = False


class Expirable(BaseModel):
    """Mixin providing optional expires_at and provisioned_at timestamps."""

    expires_at: datetime | None = None
    provisioned_at: datetime | None = None
