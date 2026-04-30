from __future__ import annotations

from typing import TYPE_CHECKING

import pymongo
from beanie import Link
from pymongo import IndexModel
from pydantic import Field

from .base import BaseDocument, Expirable
from .site import Site
from .user import User

if TYPE_CHECKING:
    from .group import AccessGroup, StatusGroup


class UserSiteInfo(BaseDocument, Expirable):
    user: Link[User]
    site: Link[Site]

    # Per-site status override; if None, downstream code falls back to
    # User.status. Same semantics as User.status — a Link to a StatusGroup
    # record.
    status: Link['StatusGroup'] | None = None
    # Per-site access override. Effective access for a (user, site) is the
    # union of User.access and UserSiteInfo.access (matches v1 SiteUser
    # semantics in cheeto/database/user.py).
    access: list[Link['AccessGroup']] = Field(default_factory=list)

    class Settings:
        name = 'user_site_info'
        indexes = [
            IndexModel(
                [('user', pymongo.ASCENDING), ('site', pymongo.ASCENDING)],
                unique=True,
            ),
        ]
