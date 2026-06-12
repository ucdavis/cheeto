from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Annotated

import pymongo
from beanie import BackLink, Delete, Insert, Link, Replace, Update, after_event
from pydantic import BaseModel, Field, field_validator

from ..constants import (
    DEFAULT_SHELL,
    EMAIL_REGEX,
    IAM_STATUSES,
    IAM_USER_TYPES,
    SHELLS,
    UINT_MAX,
    USER_TYPES,
)
from .base import BaseDocument, Expirable, link_target_id
from .ldap_sync import LDAPSyncable, ldap_touch, stable_fingerprint

if TYPE_CHECKING:
    from .group import AccessGroup, StatusGroup
    from .group_membership import GroupMembership
    from .user_site_info import UserSiteInfo


class UCDIAMAssociation(BaseModel):
    org_id: str # bouOrgOId: business org unit
    org_name: str # official name of the business org unit; broadly, the "college"
    # IAM dept codes are zero-padded ('072067', '050'); keep as strings so we
    # don't lose leading zeros via int coercion.
    org_code: str # deptCode of the BOU
    dept_name: str # apptDeptOfficialName from the association itself
    dept_code: str # apptDeptCode from the association itself
    title: str # titleOfficialName from the association itself
    title_type: str # emplClassDesc from the association itself


class UCDIAMPerson(BaseModel):
    """Snapshot of an IAM person record. Only populated after a successful
    `get_person_using_iam_id` lookup; left intact during missing streaks so
    operators can see what the user looked like before they vanished."""

    iam_id: Annotated[int, Field(ge=0, le=UINT_MAX)]
    mothra_id: Annotated[int, Field(ge=0, le=UINT_MAX)]
    user_types: list[str] = Field(default_factory=list)
    associations: list[UCDIAMAssociation] = Field(default_factory=list)

    @field_validator('user_types')
    @classmethod
    def validate_user_types(cls, v):
        for user_type in v:
            if user_type not in IAM_USER_TYPES:
                raise ValueError(f'Invalid IAM user type: {user_type}')
        return v


class UCDIAMInfo(BaseModel):
    """IAM sync bookkeeping for a User.

    The `person` snapshot is the most recent successful `get_person` payload;
    it stays around during missing streaks. `iam_status` is the authoritative
    current state of the IAM record. The timestamps drive the offboarding
    state machine in `cheeto.operations.iam.SyncUserIAM`.
    """

    iam_status: str = 'present'
    person: UCDIAMPerson | None = None
    # Last sync attempt that produced a definitive answer (200-with-data,
    # 200-empty, or 404). Transient errors (5xx, transport, timeout) do NOT
    # advance this.
    iam_synced_at: datetime.datetime | None = None
    # Last sync that returned a populated payload.
    last_seen_at: datetime.datetime | None = None
    # First sync (in the current missing streak) that found the user gone.
    # Cleared on the next hit. Drives the offboarding grace window.
    first_missing_at: datetime.datetime | None = None

    @field_validator('iam_status')
    @classmethod
    def validate_iam_status(cls, v):
        if v not in IAM_STATUSES:
            raise ValueError(f'Invalid IAM status: {v}')
        return v


class User(LDAPSyncable, BaseDocument, Expirable):
    name: Annotated[str, Field(min_length=1, max_length=32)]
    email: Annotated[str, Field(pattern=EMAIL_REGEX)]
    uid: Annotated[int, Field(ge=0, le=UINT_MAX)]
    gid: Annotated[int, Field(ge=0, le=UINT_MAX)]
    fullname: str
    surname: str | None = None

    shell: str = DEFAULT_SHELL
    type: str = 'user'
    # Link to a StatusGroup record (`active`, `inactive`, `disabled`,
    # `offboarding`). Optional during construction so existing operations
    # that build users without setting status keep working; production code
    # is expected to assign one before downstream effects (LDAP sync,
    # offboarding) can fire.
    status: Link['StatusGroup'] | None = None
    password: str | None = None
    # Links to AccessGroup records (`login-ssh`, `sudo`, etc.).
    access: list[Link['AccessGroup']] = Field(default_factory=list)
    comments: list[str] = Field(default_factory=list)

    home_directory: str
    iam: UCDIAMInfo | None = None

    memberships: list[BackLink['GroupMembership']] = Field(
        default_factory=list,
        json_schema_extra={'original_field': 'user'},
    )

    sites: list[BackLink['UserSiteInfo']] = Field(
        default_factory=list,
        json_schema_extra={'original_field': 'site'},
    )

    ssh_keys: list[BackLink['SshKey']] = Field(
        default_factory=list,
        json_schema_extra={'original_field': 'user'},
    )

    @field_validator('shell')
    @classmethod
    def validate_shell(cls, v):
        if v not in SHELLS:
            raise ValueError(f'Invalid shell: {v}')
        return v

    @field_validator('type')
    @classmethod
    def validate_type(cls, v):
        if v not in USER_TYPES:
            raise ValueError(f'Invalid user type: {v}')
        return v

    def ldap_fingerprint(self) -> str:
        # Exactly the fields LDAPUserRecord projects (operations/ldap.py)
        # plus the access/status links that drive special-group membership.
        # SshKey / UserSiteInfo / GroupMembership changes are covered by
        # propagation hooks on those documents, not here.
        return stable_fingerprint({
            'name': self.name,
            'email': self.email,
            'uid': self.uid,
            'gid': self.gid,
            'fullname': self.fullname,
            'surname': self.surname,
            'home_directory': self.home_directory,
            'shell': self.shell,
            'password': self.password,
            'expires_at': self.expires_at,
            'status': str(link_target_id(self.status)),
            'access': sorted(str(link_target_id(a)) for a in self.access),
        })

    class Settings:
        name = 'users'
        indexes = [
            [('name', pymongo.ASCENDING)],
            [('uid', pymongo.ASCENDING)],
            [('gid', pymongo.ASCENDING)],
        ]


class SshKey(BaseDocument, Expirable):
    key: str
    user: Link[User]

    @after_event(Insert, Replace, Update, Delete)
    async def mark_user_ldap_dirty(self) -> None:
        # save()/save_changes() route through self.update(), so Update
        # covers them; subscribing Save too would double-fire. Runs without
        # the caller's session — a spurious dirty on txn abort is harmless.
        await User.find_one(
            User.id == link_target_id(self.user),
        ).update(ldap_touch())

    class Settings:
        name = 'ssh_keys'
        indexes = [
            [('user', pymongo.ASCENDING)],
        ]
