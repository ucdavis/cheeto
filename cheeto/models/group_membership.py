from __future__ import annotations

from typing import Literal

import pymongo
from beanie import Delete, Insert, Link, Replace, Update, after_event
from pymongo import IndexModel
from pydantic import Field, field_validator

from .base import link_target_id
from .group import Group
from .ldap_sync import ldap_touch
from .site_association import SiteAssociation
from .user import User


MembershipRole = Literal['member', 'sponsor', 'sudoer', 'slurmer']


class GroupMembership(SiteAssociation):
    """A user's membership in a group *at a site*, with one or more roles.

    Replaces v1's per-site `SiteGroup._members/_sponsors/_sudoers/_slurmers`
    buckets and the global `Group.members/...` link lists from the first v2
    cut. One document per `(user, group, site)`; the `roles` array carries
    every capacity in which the user participates (a user can be both a
    `member` and a `sponsor` of the same group at the same site).

    The role buckets map directly to v1:
      - `member`  — appears in the group's member list (posix membership)
      - `sponsor` — a PI/sponsor of the group
      - `sudoer`  — granted sudo via the group
      - `slurmer` — granted the group's Slurm account
    """

    user: Link[User]
    group: Link[Group]
    roles: list[MembershipRole] = Field(default_factory=list)

    @field_validator('roles')
    @classmethod
    def _dedupe_and_sort_roles(cls, v: list[str]) -> list[str]:
        # Element validity is already guaranteed by the MembershipRole
        # Literal; de-dupe and sort so the stored array reads consistently
        # regardless of insertion order.
        return sorted(set(v))

    @after_event(Insert, Replace, Update, Delete)
    async def mark_edges_ldap_dirty(self) -> None:
        # Membership edges affect BOTH sides of the LDAP projection: the
        # user's memberOf reconcile and the group's member list. Update
        # covers save()/save_changes() (they route through self.update());
        # runs without the caller's session (spurious dirty is harmless).
        await User.find_one(
            User.id == link_target_id(self.user),
        ).update(ldap_touch())
        await Group.find_one(
            Group.id == link_target_id(self.group),
            with_children=True,
        ).update(ldap_touch())

    class Settings:
        name = 'group_membership'
        indexes = [
            IndexModel(
                [
                    ('user', pymongo.ASCENDING),
                    ('group', pymongo.ASCENDING),
                    ('site', pymongo.ASCENDING),
                ],
                unique=True,
            ),
            IndexModel([('group', pymongo.ASCENDING), ('site', pymongo.ASCENDING)]),
            IndexModel([('site', pymongo.ASCENDING)]),
            IndexModel([('user', pymongo.ASCENDING)]),
            IndexModel([('roles', pymongo.ASCENDING)]),
        ]
