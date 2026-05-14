from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Optional

import pymongo
from beanie import BackLink, Link
from pymongo import IndexModel
from pydantic import Field, field_validator, model_validator

from ..constants import GROUP_TYPES, UINT_MAX
from .base import BaseDocument
from .user import User

if TYPE_CHECKING:
    from .slurm import SlurmAccount


class Group(BaseDocument):
    """The base group document.

    Polymorphic root via `is_root=True` — `AccessGroup` and `StatusGroup`
    subclasses share this collection and are distinguished by beanie's
    `_class_id` discriminator. Plain `Group` instances are for posix-style
    groups (lab groups, system groups, sponsor-led groups).
    """

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
        is_root = True
        indexes = [
            IndexModel([('name', pymongo.ASCENDING)], unique=True),
            IndexModel([('gid', pymongo.ASCENDING)], unique=True),
        ]


class AccessGroup(Group):
    """A login/sudo/compute-ssh/etc. access bucket.

    The inherited `name` field IS the LDAP groupname (e.g. `'sudo-users'`).
    `access_name` is the short shorthand referenced by `User.access` (e.g.
    `'sudo'`). Members are computed via BackLink from `User.access`; the
    inherited members/sponsors/sudoers/slurmers fields are unused on this
    subclass and stay as their empty defaults.
    """

    access_name: Annotated[str, Field(min_length=1)]

    @model_validator(mode='after')
    def _enforce_type(self) -> 'AccessGroup':
        self.type = 'access'
        return self


class StatusGroup(Group):
    """An active/inactive/disabled/offboarding status bucket.

    The inherited `name` is the LDAP groupname (e.g. `'active-users'`).
    `status_name` is the `User.status` shorthand (e.g. `'active'`). Status
    and access groups never share an LDAP cn under the new naming
    convention (see plan note about decoupling status from login-ssh).
    """

    status_name: Annotated[str, Field(min_length=1)]

    @model_validator(mode='after')
    def _enforce_type(self) -> 'StatusGroup':
        self.type = 'status'
        return self
