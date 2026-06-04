from __future__ import annotations

from typing import Any

from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession

from ..models.group import Group
from ..models.group_membership import GroupMembership, MembershipRole
from ..models.site import Site
from ..models.user import User
from .base import Operation


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

    async def execute(self, session: AsyncClientSession) -> None:
        group, user, site = await self._resolve()
        edge = await self._find_edge(user, group, site)
        if edge is None:
            edge = GroupMembership(
                user=user, group=group, site=site, roles=[self.role],
            )
            await edge.insert(session=session)
        elif self.role not in edge.roles:
            edge.roles = [*edge.roles, self.role]
            await edge.save(session=session)


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
