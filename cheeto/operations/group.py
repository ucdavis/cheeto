from __future__ import annotations

from typing import Any

from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession

from ..constants import (
    MAX_LABGROUP_ID,
    MIN_CLASS_ID,
    MIN_LABGROUP_ID,
    MIN_PIGROUP_GID,
    MIN_SPECIAL_GID,
    MIN_SYSTEM_UID,
)
from ..models.group import AccessGroup, Group, StatusGroup
from ..models.user import User
from .base import Operation


# Standard set of access types and their LDAP groupnames. Mirrors what v1
# carried in `LDAPConfig.user_access_groups`; moved here so the mapping is
# data-driven from the new `AccessGroup` records and config no longer needs
# to know.
DEFAULT_ACCESS_GROUPS: tuple[tuple[str, str], ...] = (
    # (access_name, ldap_groupname stored as Group.name)
    ('login-ssh', 'login-ssh-users'),
    ('ondemand', 'ondemand-users'),
    ('compute-ssh', 'compute-ssh-users'),
    ('root-ssh', 'root-ssh-users'),
    ('sudo', 'sudo-users'),
    ('slurm', 'slurm-users'),
)

# Standard status groups. The new design DECOUPLES `status='active'` from
# `access='login-ssh'` — see plan note. Active users go to `active-users`
# rather than `login-ssh-users` (PAM/SSH must intersect).
DEFAULT_STATUS_GROUPS: tuple[tuple[str, str], ...] = (
    ('active', 'active-users'),
    ('inactive', 'inactive-users'),
    ('disabled', 'disabled-users'),
    ('offboarding', 'offboarding-users'),
)

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


class SeedAccessStatusGroups(Operation):
    """Idempotently seed the standard `AccessGroup` and `StatusGroup` records.

    Required before users can be created with non-empty `access` / `status`
    Links and before `BootstrapLDAPSite` can run (it reads these records to
    know which special groups to create in LDAP).

    Idempotent: any record whose `access_name` / `status_name` already exists
    is skipped. Newly-created records are assigned monotonically-increasing
    gids starting from `gid_start`, skipping any gid that's already in use
    so re-runs after manual gid adjustments don't collide.

    Optional `access_groups` / `status_groups` constructor args override the
    DEFAULT_*_GROUPS tuples (e.g. for migration from a custom v1 config).
    """

    op_name = 'seed_access_status_groups'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        gid_start: int = MIN_SPECIAL_GID,
        access_groups: list[tuple[str, str]] | None = None,
        status_groups: list[tuple[str, str]] | None = None,
    ) -> None:
        super().__init__(client, author)
        self.gid_start = gid_start
        self.access_groups = (
            list(access_groups) if access_groups is not None
            else list(DEFAULT_ACCESS_GROUPS)
        )
        self.status_groups = (
            list(status_groups) if status_groups is not None
            else list(DEFAULT_STATUS_GROUPS)
        )
        self._created: dict[str, str] = {}

    async def _next_gid(self, used: set[int]) -> int:
        gid = self.gid_start + len(used)
        while gid in used:
            gid += 1
        used.add(gid)
        return gid

    async def _seed_access(
        self, access_name: str, ldap_name: str,
        used_gids: set[int], session: AsyncClientSession,
    ) -> None:
        existing = await AccessGroup.find_one(
            AccessGroup.access_name == access_name,
        )
        if existing is not None:
            self._created[ldap_name] = 'already_exists'
            used_gids.add(existing.gid)
            return
        gid = await self._next_gid(used_gids)
        record = AccessGroup(
            name=ldap_name, gid=gid, access_name=access_name, type='access',
        )
        await record.insert(session=session)
        self._created[ldap_name] = 'created'

    async def _seed_status(
        self, status_name: str, ldap_name: str,
        used_gids: set[int], session: AsyncClientSession,
    ) -> None:
        existing = await StatusGroup.find_one(
            StatusGroup.status_name == status_name,
        )
        if existing is not None:
            self._created[ldap_name] = 'already_exists'
            used_gids.add(existing.gid)
            return
        gid = await self._next_gid(used_gids)
        record = StatusGroup(
            name=ldap_name, gid=gid, status_name=status_name, type='status',
        )
        await record.insert(session=session)
        self._created[ldap_name] = 'created'

    async def execute(self, session: AsyncClientSession) -> dict[str, str]:
        # Pre-load all gids in use across the polymorphic groups collection
        # (regular Groups, AccessGroups, StatusGroups). The unique index on
        # gid will reject duplicates, but eagerly tracking lets us skip them
        # without round-tripping per attempt.
        all_groups = await Group.find_all().to_list()
        used_gids: set[int] = {g.gid for g in all_groups}

        for access_name, ldap_name in self.access_groups:
            await self._seed_access(access_name, ldap_name, used_gids, session)
        for status_name, ldap_name in self.status_groups:
            await self._seed_status(status_name, ldap_name, used_gids, session)
        return dict(self._created)

    def describe(self) -> dict[str, Any]:
        return {
            'gid_start': self.gid_start,
            'access_count': len(self.access_groups),
            'status_count': len(self.status_groups),
            'created': dict(self._created),
        }
