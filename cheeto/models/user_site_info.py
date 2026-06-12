from __future__ import annotations

from typing import TYPE_CHECKING

import pymongo
from beanie import Delete, Insert, Link, Replace, Update, after_event
from pymongo import IndexModel
from pydantic import Field

from .base import BaseDocument, Expirable, link_target_id
from .ldap_sync import ldap_touch
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
    # Per-site access override. Override semantics: a non-empty list
    # replaces User.access entirely at this site; an empty list (the
    # default) falls through to User.access. Use
    # `cheeto.queries.user.effective_access_links(user, usi)` to compute
    # the effective list rather than reading this field directly.
    access: list[Link['AccessGroup']] = Field(default_factory=list)

    @after_event(Insert, Replace, Update, Delete)
    async def mark_user_ldap_dirty(self) -> None:
        # Per-site status/access overrides feed the user's LDAP projection.
        # Update covers save()/save_changes(); runs without the caller's
        # session (spurious dirty on txn abort is harmless).
        await User.find_one(
            User.id == link_target_id(self.user),
        ).update(ldap_touch())

    class Settings:
        name = 'user_site_info'
        indexes = [
            IndexModel(
                [('user', pymongo.ASCENDING), ('site', pymongo.ASCENDING)],
                unique=True,
            ),
        ]
