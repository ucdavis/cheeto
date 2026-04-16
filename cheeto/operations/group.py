from __future__ import annotations

from typing import Any

from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession

from ..constants import MIN_CLASS_ID, MIN_LABGROUP_ID, MAX_LABGROUP_ID, MIN_PIGROUP_GID, MIN_SYSTEM_UID
from ..models.group import Group
from ..models.user import User
from .base import Operation


async def _get_next_gid(min_id: int, max_id: int) -> int:
    result = await Group.find(
        Group.gid >= min_id,
        Group.gid < max_id,
    ).sort('-gid').limit(1).to_list()
    if result:
        return result[0].gid + 1
    return min_id


class CreateGroup(Operation):
    op_name = 'create_group'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        name: str,
        gid: int,
        type: str = 'group',
    ) -> None:
        super().__init__(client, author)
        self.name = name
        self.gid = gid
        self.type = type

    async def execute(self, session: AsyncClientSession) -> Group:
        existing = await Group.find_one(Group.name == self.name)
        if existing is not None:
            raise ValueError(f'Group {self.name} already exists')

        group = Group(name=self.name, gid=self.gid, type=self.type)
        await group.insert(session=session)
        self._group = group
        return group

    def describe(self) -> dict[str, Any]:
        return {'groupname': self.name, 'gid': self.gid, 'type': self.type}


class CreateSystemGroup(Operation):
    op_name = 'create_system_group'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        name: str,
    ) -> None:
        super().__init__(client, author)
        self.name = name

    async def execute(self, session: AsyncClientSession) -> Group:
        gid = await _get_next_gid(MIN_SYSTEM_UID, MIN_SYSTEM_UID + 1_000_000)
        op = CreateGroup(
            self.client, self.author,
            name=self.name, gid=gid, type='system',
        )
        group = await op.execute(session)
        self._group = group
        return group

    def describe(self) -> dict[str, Any]:
        return {'groupname': self.name, 'type': 'system'}


class CreateClassGroup(Operation):
    op_name = 'create_class_group'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        name: str,
    ) -> None:
        super().__init__(client, author)
        self.name = name

    async def execute(self, session: AsyncClientSession) -> Group:
        gid = await _get_next_gid(MIN_CLASS_ID, MIN_CLASS_ID + 1_000_000)
        op = CreateGroup(
            self.client, self.author,
            name=self.name, gid=gid, type='class',
        )
        group = await op.execute(session)
        self._group = group
        return group

    def describe(self) -> dict[str, Any]:
        return {'groupname': self.name, 'type': 'class'}


class CreateLabGroup(Operation):
    op_name = 'create_lab_group'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        name: str,
    ) -> None:
        super().__init__(client, author)
        self.name = name

    async def execute(self, session: AsyncClientSession) -> Group:
        gid = await _get_next_gid(MIN_LABGROUP_ID, MAX_LABGROUP_ID)
        op = CreateGroup(
            self.client, self.author,
            name=self.name, gid=gid, type='group',
        )
        group = await op.execute(session)
        self._group = group
        return group

    def describe(self) -> dict[str, Any]:
        return {'groupname': self.name, 'type': 'lab'}


class CreateGroupFromSponsor(Operation):
    op_name = 'create_group_from_sponsor'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        sponsor_name: str,
    ) -> None:
        super().__init__(client, author)
        self.sponsor_name = sponsor_name
        self.group_name = f'{sponsor_name}grp'

    async def execute(self, session: AsyncClientSession) -> Group:
        sponsor = await User.find_one(User.name == self.sponsor_name)
        if sponsor is None:
            raise ValueError(f'Sponsor user {self.sponsor_name} does not exist')

        existing = await Group.find_one(Group.name == self.group_name)
        if existing is not None:
            raise ValueError(f'Group {self.group_name} already exists')

        gid = MIN_PIGROUP_GID + sponsor.uid
        group = Group(
            name=self.group_name,
            gid=gid,
            type='group',
            sponsors=[sponsor],
            members=[sponsor],
        )
        await group.insert(session=session)
        self._group = group
        return group

    def describe(self) -> dict[str, Any]:
        return {
            'groupname': self.group_name,
            'sponsor': self.sponsor_name,
        }
