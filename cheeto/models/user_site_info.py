import pymongo
from beanie import Link
from pymongo import IndexModel
from pydantic import Field, field_validator

from ..constants import ACCESS_TYPES, USER_STATUSES
from .base import BaseDocument, Expirable
from .site import Site
from .user import User


class UserSiteInfo(BaseDocument, Expirable):
    user: Link[User]
    site: Link[Site]

    status: str = 'active'
    access: list[str] = Field(default_factory=lambda: ['login-ssh'])

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
        name = 'user_site_info'
        indexes = [
            IndexModel(
                [('user', pymongo.ASCENDING), ('site', pymongo.ASCENDING)],
                unique=True,
            ),
        ]
