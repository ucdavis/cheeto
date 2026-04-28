from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Annotated

import pymongo
from beanie import BackLink, Link
from pydantic import BaseModel, Field, field_validator

from ..constants import (
    ACCESS_TYPES,
    DEFAULT_SHELL,
    EMAIL_REGEX,
    IAM_USER_TYPES,
    SHELLS,
    UINT_MAX,
    USER_STATUSES,
    USER_TYPES,
)
from .base import BaseDocument, Expirable

if TYPE_CHECKING:
    from .group import Group
    from .user_site_info import UserSiteInfo


class UCDIAMAssociation(BaseModel):
    org_id: str # bouOrgOId: business org unit 
    org_name: str # official name of the business org unit; broadly, the "college"
    org_code: int # deptCode of the BOU
    dept_name: str # apptDeptOfficialName from the association itself 
    dept_code: int # apptDeptCode from the association itself
    title: str # titleOfficialName from the association itself
    title_type: str # emplClassDesc from the association itself


class UCDIAMInfo(BaseModel):
    iam_id: Annotated[int, Field(ge=0, le=UINT_MAX)]
    mothra_id: Annotated[int, Field(ge=0, le=UINT_MAX)]
    user_types: list[str] = Field(default_factory=list)
    associations: list[UCDIAMAssociation] = Field(default_factory=list)
    iam_synced_at: datetime.datetime | None = None
    # Last sync that returned a populated payload (vs. iam_synced_at, which
    # advances on any definitive answer including a 200-empty/404 miss).
    last_seen_at: datetime.datetime | None = None
    # First sync (in the current missing streak) that found the user gone.
    # Cleared on the next hit. Drives the offboarding grace window.
    first_missing_at: datetime.datetime | None = None

    @field_validator('user_types')
    @classmethod
    def validate_user_types(cls, v):
        for user_type in v:
            if user_type not in IAM_USER_TYPES:
                raise ValueError(f'Invalid IAM user type: {user_type}')
        return v


class User(BaseDocument, Expirable):
    name: Annotated[str, Field(min_length=1, max_length=32)]
    email: Annotated[str, Field(pattern=EMAIL_REGEX)]
    uid: Annotated[int, Field(ge=0, le=UINT_MAX)]
    gid: Annotated[int, Field(ge=0, le=UINT_MAX)]
    fullname: str

    shell: str = DEFAULT_SHELL
    type: str = 'user'
    status: str = 'active'
    password: str | None = None
    access: list[str] = Field(default_factory=lambda: ['login-ssh'])
    comments: list[str] = Field(default_factory=list)

    home_directory: str
    iam: UCDIAMInfo | None = None

    groups: list[BackLink['Group']] = Field(
        default_factory=list,
        json_schema_extra={'original_field': 'members'},
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

    @field_validator('status')
    @classmethod
    def validate_status(cls, v):
        if v not in USER_STATUSES:
            raise ValueError(f'Invalid user status: {v}')
        return v

    @field_validator('access')
    @classmethod
    def validate_access(cls, v):
        for access in v:
            if access not in ACCESS_TYPES:
                raise ValueError(f'Invalid access type: {access}')
        return v

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

    class Settings:
        name = 'ssh_keys'
        indexes = [
            [('user', pymongo.ASCENDING)],
        ]
