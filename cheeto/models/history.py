from datetime import datetime, timezone
from typing import Any

import pymongo
from beanie import Document, Link
from pymongo import IndexModel
from pydantic import Field

from .user import User


class History(Document):
    op: str
    author: Link[User] | None = None
    changes: dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    class Settings:
        name = 'history'
        indexes = [
            IndexModel([('timestamp', pymongo.DESCENDING)]),
            IndexModel([('op', pymongo.ASCENDING)]),
            IndexModel([('author', pymongo.ASCENDING)]),
        ]
