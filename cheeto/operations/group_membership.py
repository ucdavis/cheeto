from __future__ import annotations

from typing import Any

from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession

from ..models.group import Group
from ..models.user import User
from .base import Operation


class _GroupMembershipOp(Operation):
    """Base for add/remove member/sponsor/sudoer operations."""

    field: str  # 'members', 'sponsors', 'sudoers' — set by subclass

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        group_name: str,
        user_name: str,
    ) -> None:
        super().__init__(client, author)
        self.group_name = group_name
        self.user_name = user_name

    def describe(self) -> dict[str, Any]:
        return {
            'group': self.group_name,
            'user': self.user_name,
            'field': self.field,
        }


class _AddToGroup(_GroupMembershipOp):

    async def execute(self, session: AsyncClientSession) -> None:
        group = await Group.find_one(
            Group.name == self.group_name, fetch_links=True,
        )
        if group is None:
            raise ValueError(f'Group {self.group_name} does not exist')

        user = await User.find_one(User.name == self.user_name)
        if user is None:
            raise ValueError(f'User {self.user_name} does not exist')

        member_list: list = getattr(group, self.field)
        existing_ids = {m.id for m in member_list}
        if user.id not in existing_ids:
            member_list.append(user)
            await group.save(session=session)


class _RemoveFromGroup(_GroupMembershipOp):

    async def execute(self, session: AsyncClientSession) -> None:
        group = await Group.find_one(
            Group.name == self.group_name, fetch_links=True,
        )
        if group is None:
            raise ValueError(f'Group {self.group_name} does not exist')

        user = await User.find_one(User.name == self.user_name)
        if user is None:
            raise ValueError(f'User {self.user_name} does not exist')

        member_list: list = getattr(group, self.field)
        setattr(
            group, self.field,
            [m for m in member_list if m.id != user.id],
        )
        await group.save(session=session)


class AddGroupMember(_AddToGroup):
    op_name = 'add_group_member'
    field = 'members'


class RemoveGroupMember(_RemoveFromGroup):
    op_name = 'remove_group_member'
    field = 'members'


class AddGroupSponsor(_AddToGroup):
    op_name = 'add_group_sponsor'
    field = 'sponsors'


class RemoveGroupSponsor(_RemoveFromGroup):
    op_name = 'remove_group_sponsor'
    field = 'sponsors'


class AddGroupSudoer(_AddToGroup):
    op_name = 'add_group_sudoer'
    field = 'sudoers'


class RemoveGroupSudoer(_RemoveFromGroup):
    op_name = 'remove_group_sudoer'
    field = 'sudoers'
