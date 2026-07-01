from __future__ import annotations

from typing import Annotated

import pymongo
from pymongo import IndexModel
from pydantic import Field, field_validator, model_validator

from ..constants import GROUP_TYPES, UINT_MAX
from .base import BaseDocument
from .ldap_sync import LDAPSyncable, stable_fingerprint


class Group(LDAPSyncable, BaseDocument):
    """The base group document.

    Polymorphic root via `is_root=True` — `AccessGroup` and `StatusGroup`
    subclasses share this collection and are distinguished by beanie's
    `_class_id` discriminator. Plain `Group` instances are for posix-style
    groups (lab groups, system groups, sponsor-led groups).
    """

    name: Annotated[str, Field(min_length=1, max_length=32)]
    gid: Annotated[int, Field(ge=0, le=UINT_MAX)]
    type: str = 'group'

    # Membership is per-site and lives on GroupMembership edges, not here.
    # See cheeto/models/group_membership.py. Slurm accounts are per-site and
    # keyed on (group, site); they are looked up via SlurmAccount, not a
    # backlink here.

    @field_validator('type')
    @classmethod
    def validate_type(cls, v):
        if v not in GROUP_TYPES:
            raise ValueError(f'Invalid group type: {v}')
        return v

    def ldap_fingerprint(self) -> str:
        # The LDAP group entry is (cn=name, gidNumber=gid); membership
        # changes arrive via GroupMembership propagation.
        return stable_fingerprint({'name': self.name, 'gid': self.gid})

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
    `'sudo'`). Members are computed via the `User.access` links, not via
    GroupMembership edges — access/status groups are not posix groups.
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
