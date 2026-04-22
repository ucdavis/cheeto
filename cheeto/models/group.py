from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Optional

import pymongo
from beanie import BackLink, Link
from pymongo import IndexModel
from pydantic import Field, field_validator

from ..constants import GROUP_TYPES, UINT_MAX
from .base import BaseDocument
from .user import User

if TYPE_CHECKING:
    from .slurm import SlurmAccount


class Group(BaseDocument):
    name: Annotated[str, Field(min_length=1, max_length=32)]
    gid: Annotated[int, Field(ge=0, le=UINT_MAX)]
    type: str = 'group'

    members: list[Link[User]] = Field(default_factory=list)
    sponsors: list[Link[User]] = Field(default_factory=list)
    sudoers: list[Link[User]] = Field(default_factory=list)
    slurmers: list[Link[User]] = Field(default_factory=list)

    slurm: Optional[BackLink['SlurmAccount']] = Field(
        default=None,
        json_schema_extra={'original_field': 'group'},
    )

    @field_validator('type')
    @classmethod
    def validate_type(cls, v):
        if v not in GROUP_TYPES:
            raise ValueError(f'Invalid group type: {v}')
        return v

    class Settings:
        name = 'groups'
        indexes = [
            IndexModel([('name', pymongo.ASCENDING)], unique=True),
            IndexModel([('gid', pymongo.ASCENDING)], unique=True),
        ]
