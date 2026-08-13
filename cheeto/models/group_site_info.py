from __future__ import annotations

import pymongo
from beanie import Delete, Insert, Link, after_event
from pymongo import IndexModel

from .base import link_target_id
from .group import Group
from .ldap_sync import ldap_touch, queue_ldap_touch
from .site_association import SiteAssociation


class GroupSiteInfo(SiteAssociation):
    """Explicit group presence at a site — the group analog of UserSiteInfo.

    One document per `(group, site)`. Presence is *authoritative*: readers
    (LDAP site sync, puppet export, `find_groups --site`) project exactly the
    groups with a GroupSiteInfo, while `GroupMembership` edges continue to
    govern membership and roles *within* a present group. Auto-created by
    every attach path via `operations.group_site.ensure_group_site`; removed
    only explicitly (`RemoveSiteGroup`, `user remove site` for personal
    groups) or by the DeleteGroup/DeleteUser/DeleteSite cascades.
    """

    group: Link[Group]

    @after_event(Insert, Delete)
    async def mark_group_ldap_dirty(self) -> None:
        # Presence drives whether the group's entry exists in the site's
        # LDAP tree. Sessionless write — deferred past any active Operation
        # transaction (see ldap_sync docstring). with_children=True so a
        # stray reference to a polymorphic subclass row still resolves.
        group_id = link_target_id(self.group)
        await queue_ldap_touch(
            lambda: Group.find_one(
                Group.id == group_id, with_children=True,
            ).update(ldap_touch())
        )

    class Settings:
        name = 'group_site_info'
        indexes = [
            IndexModel(
                [('group', pymongo.ASCENDING), ('site', pymongo.ASCENDING)],
                unique=True,
            ),
            IndexModel([('site', pymongo.ASCENDING)]),
        ]
