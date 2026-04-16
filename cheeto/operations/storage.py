from __future__ import annotations

from typing import Any

from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession

from ..models.group import Group
from ..models.site import Site
from ..models.storage import AutomountMap, Storage, StorageAllocation
from ..models.user import User
from .base import Operation


class CreateHomeStorage(Operation):
    op_name = 'create_home_storage'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        user_name: str,
        site_name: str,
        host: str,
        host_path: str | None = None,
        quota: str | None = None,
    ) -> None:
        super().__init__(client, author)
        self.user_name = user_name
        self.site_name = site_name
        self.host = host
        self.host_path = host_path
        self.quota = quota

    async def execute(self, session: AsyncClientSession) -> Storage:
        user = await User.find_one(User.name == self.user_name)
        if user is None:
            raise ValueError(f'User {self.user_name} does not exist')

        group = await Group.find_one(Group.name == self.user_name)
        if group is None:
            raise ValueError(f'Group {self.user_name} does not exist')

        site = await Site.find_one(Site.name == self.site_name)
        if site is None:
            raise ValueError(f'Site {self.site_name} does not exist')

        automount_map = await AutomountMap.find_one(
            AutomountMap.name == 'home',
            AutomountMap.site.id == site.id,
        )

        allocations = []
        if self.quota is not None:
            allocations.append(
                StorageAllocation(quota=self.quota, comment='initial allocation')
            )

        storage = Storage(
            name=self.user_name,
            site=site,
            type='zfs',
            category='home',
            owner=user,
            group=group,
            host=self.host,
            host_path=self.host_path or f'/home/{self.user_name}',
            allocations=allocations,
            automount_map=automount_map,
            mount_name=self.user_name,
        )
        await storage.insert(session=session)
        self._storage = storage
        return storage

    def describe(self) -> dict[str, Any]:
        return {
            'user': self.user_name,
            'site': self.site_name,
            'host': self.host,
            'quota': self.quota,
        }
