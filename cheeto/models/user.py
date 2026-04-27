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
    SHELLS,
    UINT_MAX,
    USER_STATUSES,
    USER_TYPES,
)
from .base import BaseDocument, Expirable

if TYPE_CHECKING:
    from .group import Group
    from .user_site_info import UserSiteInfo


class UCDIAMInfo(BaseModel):
    iam_id: Annotated[int, Field(ge=0, le=UINT_MAX)]
    mothra_id: Annotated[int, Field(ge=0, le=UINT_MAX)]
    colleges: list[str] = Field(default_factory=list)
    iam_synced_at: datetime.datetime | None = None


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
