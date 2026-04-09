from beanie import Document
from pydantic import BaseModel, Field, field_validator
from typing import Annotated, Literal
import datetime
from ..constants import DEFAULT_SHELL, SHELLS, UINT_MAX, USER_TYPES, USER_STATUSES, ACCESS_TYPES, EMAIL_REGEX
from ..types import AccessType

class SshKey(BaseModel):
    key: str
    registered_at: datetime.datetime = Field(default_factory=datetime.datetime.now)
    expires_at: datetime.datetime | None = None


class UCDIAMInfo(BaseModel):
    iam_id: int = Field(ge=0, le=UINT_MAX)
    mothra_id: int = Field(ge=0, le=UINT_MAX)
    colleges: list[str] = Field(default_factory=list)
    iam_synced_at: datetime.datetime | None = None


class User(Document):
    username: str = Field(min_length=1, max_length=32)
    email: str = Field(pattern=EMAIL_REGEX)
    uid: int = Field(ge=0, le=UINT_MAX)
    gid: int = Field(ge=0, le=UINT_MAX)
    fullname: str

    shell: str = DEFAULT_SHELL
    type: str = 'user'
    status: str = 'active'
    ssh_key: list[SshKey]
    access: list[str] = Field(default_factory=lambda: ['login-ssh'])

    home_directory: str
    iam: UCDIAMInfo | None = None

    @field_validator('shell')
    def validate_shell(cls, v):
        if v not in SHELLS:
            raise ValueError(f'Invalid shell: {v}')
        return v
    
    @field_validator('type')
    def validate_type(cls, v):
        if v not in USER_TYPES:
            raise ValueError(f'Invalid user type: {v}')
        return v
    
    @field_validator('status')
    def validate_status(cls, v):
        if v not in USER_STATUSES:
            raise ValueError(f'Invalid user status: {v}')
        return v

    @field_validator('access')
    def validate_access(cls, v):
        for access in v:
            if access not in ACCESS_TYPES:
                raise ValueError(f'Invalid access type: {access}')
        return v

    