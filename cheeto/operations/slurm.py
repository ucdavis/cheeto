from __future__ import annotations

from typing import Any

from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession

from ..models.group import Group
from ..models.site import Site
from ..models.slurm import (
    SlurmAccount,
    SlurmAccountLimits,
    SlurmAllocation,
    SlurmAssociation,
    SlurmPartition,
    SlurmQOS,
)
from ..models.user import User
from .base import Operation


class CreateSlurmPartition(Operation):
    op_name = 'create_slurm_partition'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        name: str,
        site_name: str,
    ) -> None:
        super().__init__(client, author)
        self.name = name
        self.site_name = site_name

    async def execute(self, session: AsyncClientSession) -> SlurmPartition:
        site = await Site.find_one(Site.name == self.site_name)
        if site is None:
            raise ValueError(f'Site {self.site_name} does not exist')

        partition = SlurmPartition(name=self.name, site=site)
        await partition.insert(session=session)
        self._partition = partition
        return partition

    def describe(self) -> dict[str, Any]:
        return {'name': self.name, 'site': self.site_name}


class CreateSlurmQOS(Operation):
    op_name = 'create_slurm_qos'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        name: str,
        site_name: str,
        group_limits: list[SlurmAllocation] | None = None,
        user_limits: list[SlurmAllocation] | None = None,
        job_limits: list[SlurmAllocation] | None = None,
        priority: int = 0,
        flags: list[str] | None = None,
    ) -> None:
        super().__init__(client, author)
        self.name = name
        self.site_name = site_name
        self.group_limits = group_limits or []
        self.user_limits = user_limits or []
        self.job_limits = job_limits or []
        self.priority = priority
        self.flags = flags or ['DenyOnLimit']

    async def execute(self, session: AsyncClientSession) -> SlurmQOS:
        site = await Site.find_one(Site.name == self.site_name)
        if site is None:
            raise ValueError(f'Site {self.site_name} does not exist')

        qos = SlurmQOS(
            name=self.name,
            site=site,
            group_limits=self.group_limits,
            user_limits=self.user_limits,
            job_limits=self.job_limits,
            priority=self.priority,
            flags=self.flags,
        )
        await qos.insert(session=session)
        self._qos = qos
        return qos

    def describe(self) -> dict[str, Any]:
        return {
            'name': self.name,
            'site': self.site_name,
            'priority': self.priority,
            'flags': self.flags,
        }


class CreateSlurmAssociation(Operation):
    op_name = 'create_slurm_association'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        site_name: str,
        account_group_name: str,
        partition_name: str,
        qos_name: str,
    ) -> None:
        super().__init__(client, author)
        self.site_name = site_name
        self.account_group_name = account_group_name
        self.partition_name = partition_name
        self.qos_name = qos_name

    async def execute(self, session: AsyncClientSession) -> SlurmAssociation:
        site = await Site.find_one(Site.name == self.site_name)
        if site is None:
            raise ValueError(f'Site {self.site_name} does not exist')

        account = await SlurmAccount.find_one(
            SlurmAccount.site.id == site.id,
            SlurmAccount.group.id == (
                await Group.find_one(Group.name == self.account_group_name)
            ).id,
        )
        if account is None:
            raise ValueError(
                f'SlurmAccount for group {self.account_group_name} '
                f'on site {self.site_name} does not exist'
            )

        partition = await SlurmPartition.find_one(
            SlurmPartition.name == self.partition_name,
            SlurmPartition.site.id == site.id,
        )
        if partition is None:
            raise ValueError(
                f'SlurmPartition {self.partition_name} '
                f'on site {self.site_name} does not exist'
            )

        qos = await SlurmQOS.find_one(
            SlurmQOS.name == self.qos_name,
            SlurmQOS.site.id == site.id,
        )
        if qos is None:
            raise ValueError(
                f'SlurmQOS {self.qos_name} '
                f'on site {self.site_name} does not exist'
            )

        assoc = SlurmAssociation(
            site=site,
            account=account,
            partition=partition,
            qos=qos,
        )
        await assoc.insert(session=session)
        self._assoc = assoc
        return assoc

    def describe(self) -> dict[str, Any]:
        return {
            'site': self.site_name,
            'account_group': self.account_group_name,
            'partition': self.partition_name,
            'qos': self.qos_name,
        }
