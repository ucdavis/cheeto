from datetime import datetime, timezone

from beanie import Document, Insert, Replace, Save, SaveChanges, Update, before_event
from pydantic import Field


class BaseDocument(Document):
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @before_event(Replace, Save, SaveChanges, Update)
    def update_timestamp(self):
        self.updated_at = datetime.now(timezone.utc)

    class Settings:
        use_state_management = True
        is_root = False
