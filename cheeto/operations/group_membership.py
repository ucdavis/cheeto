from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession

from ..models.group import Group
from ..models.group_membership import (
    SITE_USER_ROLES, GroupMembership, MembershipRole,
)
from ..models.site import Site
from ..models.user import User
from .base import Operation
from .group_site import ensure_group_site
from .user_site import ensure_user_site


async def ensure_group_membership(
    user: User, group: Group, site: Site,
    roles: Iterable[MembershipRole],
    session: AsyncClientSession | None,
) -> tuple[GroupMembership, bool]:
    """Idempotently ensure `user` holds every role in `roles` on `group` at `site`.

    Ensures the group's site presence first (any role edge implies presence),
    and the user's site presence when any role is in `SITE_USER_ROLES` --
    without a `UserSiteInfo` the LDAP/puppet/Slurm readers silently drop the
    member. Then finds-or-inserts the `(user, group, site)` edge and merges
    in any missing roles. Returns `(edge, added_to_site)`.

    The find_one passes `session` for the same reason as
    `ensure_group_site`: inside a transaction, sessionless reads cannot see
    the transaction's own uncommitted inserts. Do NOT catch DuplicateKeyError
    here -- a write error inside a Mongo transaction is unrecoverable
    in-transaction; the unique (user, group, site) index backstops races.
    """
    await ensure_group_site(group, site, session)
    added_to_site = False
    if SITE_USER_ROLES & set(roles):
        _, added_to_site = await ensure_user_site(user, site, session)
    wanted = set(roles)
    edge = await GroupMembership.find_one(
        GroupMembership.user.id == user.id,
        GroupMembership.group.id == group.id,
        GroupMembership.site.id == site.id,
        session=session,
    )
    if edge is None:
        edge = GroupMembership(
            user=user, group=group, site=site, roles=sorted(wanted),
        )
        await edge.insert(session=session)
    elif not wanted <= set(edge.roles):
        edge.roles = sorted(set(edge.roles) | wanted)
        await edge.save(session=session)
    return edge, added_to_site


class _GroupMembershipOp(Operation):
    """Base for add/remove member/sponsor/sudoer/slurmer operations.

    Membership is per-site: each operation targets the `(user, group, site)`
    edge and adds or removes a single `role` from it. Adding to a
    non-existent edge creates it; removing the last role deletes it.
    """

    role: MembershipRole  # set by subclass

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        group_name: str,
        user_name: str,
        site_name: str,
    ) -> None:
        super().__init__(client, author)
        self.group_name = group_name
        self.user_name = user_name
        self.site_name = site_name

    async def _resolve(self) -> tuple[Group, User, Site]:
        group = await Group.find_one(Group.name == self.group_name)
        if group is None:
            raise ValueError(f'Group {self.group_name} does not exist')
        user = await User.find_one(User.name == self.user_name)
        if user is None:
            raise ValueError(f'User {self.user_name} does not exist')
        site = await Site.find_one(Site.name == self.site_name)
        if site is None:
            raise ValueError(f'Site {self.site_name} does not exist')
        return group, user, site

    @staticmethod
    async def _find_edge(
        user: User, group: Group, site: Site,
    ) -> GroupMembership | None:
        return await GroupMembership.find_one(
            GroupMembership.user.id == user.id,
            GroupMembership.group.id == group.id,
            GroupMembership.site.id == site.id,
        )

    def describe(self) -> dict[str, Any]:
        return {
            'group': self.group_name,
            'user': self.user_name,
            'site': self.site_name,
            'role': self.role,
        }


class _AddToGroup(_GroupMembershipOp):
    """Returns True when the add also put the user on the site (see
    `ensure_group_membership`)."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._added_to_site = False

    async def execute(self, session: AsyncClientSession) -> bool:
        group, user, site = await self._resolve()
        _, self._added_to_site = await ensure_group_membership(
            user, group, site, (self.role,), session,
        )
        return self._added_to_site

    def describe(self) -> dict[str, Any]:
        return {**super().describe(), 'added_to_site': self._added_to_site}


class _RemoveFromGroup(_GroupMembershipOp):

    async def execute(self, session: AsyncClientSession) -> None:
        group, user, site = await self._resolve()
        edge = await self._find_edge(user, group, site)
        if edge is None or self.role not in edge.roles:
            return
        remaining = [r for r in edge.roles if r != self.role]
        if remaining:
            edge.roles = remaining
            await edge.save(session=session)
        else:
            await edge.delete(session=session)


class AddGroupMember(_AddToGroup):
    op_name = 'add_group_member'
    role = 'member'


class RemoveGroupMember(_RemoveFromGroup):
    op_name = 'remove_group_member'
    role = 'member'


class AddGroupSponsor(_AddToGroup):
    op_name = 'add_group_sponsor'
    role = 'sponsor'


class RemoveGroupSponsor(_RemoveFromGroup):
    op_name = 'remove_group_sponsor'
    role = 'sponsor'


class AddGroupSudoer(_AddToGroup):
    op_name = 'add_group_sudoer'
    role = 'sudoer'


class RemoveGroupSudoer(_RemoveFromGroup):
    op_name = 'remove_group_sudoer'
    role = 'sudoer'


class AddGroupSlurmer(_AddToGroup):
    op_name = 'add_group_slurmer'
    role = 'slurmer'


class RemoveGroupSlurmer(_RemoveFromGroup):
    op_name = 'remove_group_slurmer'
    role = 'slurmer'
