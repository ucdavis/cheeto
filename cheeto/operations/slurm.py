from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path
from typing import Any

import sh
from beanie import PydanticObjectId
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
    SlurmTRES,
)
from ..models.user import User
from ..queries.slurm import build_desired_slurm_state
from ..slurm_sync import (
    AsyncSAcctMgr,
    CommandBatch,
    DumpSAcctMgr,
    SlurmSyncAborted,
    count_deletions,
    reconcile,
)
from .base import UNSET, Operation


_QOS_ALLOC_FIELDS = ('group_limits', 'user_limits', 'job_limits')


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

        async def _insert_all(allocs: list[SlurmAllocation]) -> list[SlurmAllocation]:
            for a in allocs:
                await a.insert(session=session)
            return allocs

        group_limits = await _insert_all(self.group_limits)
        user_limits = await _insert_all(self.user_limits)
        job_limits = await _insert_all(self.job_limits)

        qos = SlurmQOS(
            name=self.name,
            site=site,
            group_limits=group_limits,
            user_limits=user_limits,
            job_limits=job_limits,
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


class AddQOSAllocation(Operation):
    op_name = 'add_qos_allocation'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        qos_name: str,
        site_name: str,
        field: str,
        tres: SlurmTRES,
        comment: str = '',
        expires_at: datetime | None | Any = UNSET,
        provisioned_at: datetime | None | Any = UNSET,
    ) -> None:
        super().__init__(client, author)
        if field not in _QOS_ALLOC_FIELDS:
            raise ValueError(
                f'Invalid allocation field: {field} '
                f'(expected one of {_QOS_ALLOC_FIELDS})'
            )
        self.qos_name = qos_name
        self.site_name = site_name
        self.field = field
        self.tres = tres
        self.comment = comment
        self.expires_at = expires_at
        self.provisioned_at = provisioned_at

    async def execute(self, session: AsyncClientSession) -> SlurmAllocation:
        site = await Site.find_one(Site.name == self.site_name)
        if site is None:
            raise ValueError(f'Site {self.site_name} does not exist')

        qos = await SlurmQOS.find_one(
            SlurmQOS.name == self.qos_name,
            SlurmQOS.site.id == site.id,
            fetch_links=True,
        )
        if qos is None:
            raise ValueError(
                f'SlurmQOS {self.qos_name} on site {self.site_name} does not exist'
            )
        kwargs = {
            'tres': self.tres,
            'comment': self.comment,
        }
        if self.expires_at is not UNSET:
            kwargs['expires_at'] = self.expires_at
        if self.provisioned_at is not UNSET:
            kwargs['provisioned_at'] = self.provisioned_at

        alloc = SlurmAllocation(**kwargs)
        await alloc.insert(session=session)

        getattr(qos, self.field).append(alloc)
        await qos.save(session=session)

        self._alloc = alloc
        return alloc

    def describe(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            'qos': self.qos_name,
            'site': self.site_name,
            'field': self.field,
            'tres': self.tres.model_dump(),
            'comment': self.comment,
        }
        if self.expires_at not in [UNSET, None]:
            data['expires_at'] = self.expires_at.isoformat()
        if self.provisioned_at not in [UNSET, None]:
            data['provisioned_at'] = self.provisioned_at.isoformat()
        return data


class EditSlurmAllocation(Operation):
    op_name = 'edit_slurm_allocation'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        allocation_id: str,
        tres: SlurmTRES | None = None,
        comment: str | None = None,
        expires_at: datetime | None | Any = UNSET,
        provisioned_at: datetime | None | Any = UNSET,
    ) -> None:
        super().__init__(client, author)
        self.allocation_id = allocation_id
        self.tres = tres
        self.comment = comment
        self.expires_at = expires_at
        self.provisioned_at = provisioned_at

    async def execute(self, session: AsyncClientSession) -> SlurmAllocation:
        alloc = await SlurmAllocation.get(PydanticObjectId(self.allocation_id))
        if alloc is None:
            raise ValueError(f'Allocation {self.allocation_id} does not exist')

        if self.tres is not None:
            alloc.tres = self.tres
        if self.comment is not None:
            alloc.comment = self.comment
        if self.expires_at is not UNSET:
            alloc.expires_at = self.expires_at
        if self.provisioned_at is not UNSET:
            alloc.provisioned_at = self.provisioned_at

        await alloc.save(session=session)
        self._alloc = alloc
        return alloc

    def describe(self) -> dict[str, Any]:
        data: dict[str, Any] = {'allocation_id': self.allocation_id}
        if self.tres is not None:
            data['tres'] = self.tres.model_dump()
        if self.comment is not None:
            data['comment'] = self.comment
        if self.expires_at is not UNSET:
            data['expires_at'] = (
                self.expires_at.isoformat() if self.expires_at else None
            )
        if self.provisioned_at is not UNSET:
            data['provisioned_at'] = (
                self.provisioned_at.isoformat() if self.provisioned_at else None
            )
        return data


class EditSlurmQOS(Operation):
    """Edit top-level QOS fields (priority/flags). Allocations are managed via
    AddQOSAllocation / EditSlurmAllocation."""

    op_name = 'edit_slurm_qos'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        name: str,
        site_name: str,
        priority: int | None = None,
        flags: list[str] | None = None,
    ) -> None:
        super().__init__(client, author)
        self.name = name
        self.site_name = site_name
        self.priority = priority
        self.flags = flags

    async def execute(self, session: AsyncClientSession) -> SlurmQOS:
        site = await Site.find_one(Site.name == self.site_name)
        if site is None:
            raise ValueError(f'Site {self.site_name} does not exist')
        qos = await SlurmQOS.find_one(
            SlurmQOS.name == self.name,
            SlurmQOS.site.id == site.id,
        )
        if qos is None:
            raise ValueError(
                f'SlurmQOS {self.name} on site {self.site_name} does not exist'
            )
        if self.priority is not None:
            qos.priority = self.priority
        if self.flags is not None:
            qos.flags = self.flags
        await qos.save(session=session)
        self._qos = qos
        return qos

    def describe(self) -> dict[str, Any]:
        data: dict[str, Any] = {'name': self.name, 'site': self.site_name}
        if self.priority is not None:
            data['priority'] = self.priority
        if self.flags is not None:
            data['flags'] = self.flags
        return data


class RemoveSlurmPartition(Operation):
    op_name = 'remove_slurm_partition'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        name: str,
        site_name: str,
        force: bool = False,
    ) -> None:
        super().__init__(client, author)
        self.name = name
        self.site_name = site_name
        self.force = force
        self.cascaded_associations = 0

    async def execute(self, session: AsyncClientSession) -> None:
        site = await Site.find_one(Site.name == self.site_name)
        if site is None:
            raise ValueError(f'Site {self.site_name} does not exist')
        partition = await SlurmPartition.find_one(
            SlurmPartition.name == self.name,
            SlurmPartition.site.id == site.id,
        )
        if partition is None:
            raise ValueError(
                f'SlurmPartition {self.name} on site {self.site_name} does not exist'
            )

        assocs = await SlurmAssociation.find(
            SlurmAssociation.partition.id == partition.id,
        ).to_list()
        if assocs:
            if not self.force:
                raise ValueError(
                    f'{len(assocs)} association(s) still reference partition '
                    f'{self.name}; re-run with force=True to cascade.'
                )
            for a in assocs:
                await a.delete(session=session)
            self.cascaded_associations = len(assocs)

        await partition.delete(session=session)

    def describe(self) -> dict[str, Any]:
        return {
            'name': self.name,
            'site': self.site_name,
            'force': self.force,
            'cascaded_associations': self.cascaded_associations,
        }


class RemoveSlurmQOS(Operation):
    """Remove a QOS. Also deletes its owned SlurmAllocation docs (they're
    single-owned, referenced only from the QOS's limit lists)."""

    op_name = 'remove_slurm_qos'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        name: str,
        site_name: str,
        force: bool = False,
    ) -> None:
        super().__init__(client, author)
        self.name = name
        self.site_name = site_name
        self.force = force
        self.cascaded_associations = 0
        self.removed_allocations = 0

    async def execute(self, session: AsyncClientSession) -> None:
        site = await Site.find_one(Site.name == self.site_name)
        if site is None:
            raise ValueError(f'Site {self.site_name} does not exist')
        qos = await SlurmQOS.find_one(
            SlurmQOS.name == self.name,
            SlurmQOS.site.id == site.id,
            fetch_links=True,
            nesting_depth=1,
        )
        if qos is None:
            raise ValueError(
                f'SlurmQOS {self.name} on site {self.site_name} does not exist'
            )

        assocs = await SlurmAssociation.find(
            SlurmAssociation.qos.id == qos.id,
        ).to_list()
        if assocs:
            if not self.force:
                raise ValueError(
                    f'{len(assocs)} association(s) still reference QOS '
                    f'{self.name}; re-run with force=True to cascade.'
                )
            for a in assocs:
                await a.delete(session=session)
            self.cascaded_associations = len(assocs)

        # Delete the QOS's owned allocations (they're not shared with other QOSes).
        for alloc in (qos.group_limits + qos.user_limits + qos.job_limits):
            await alloc.delete(session=session)
            self.removed_allocations += 1

        await qos.delete(session=session)

    def describe(self) -> dict[str, Any]:
        return {
            'name': self.name,
            'site': self.site_name,
            'force': self.force,
            'cascaded_associations': self.cascaded_associations,
            'removed_allocations': self.removed_allocations,
        }


class RemoveSlurmAssociation(Operation):
    """Remove one or more SlurmAssociations. Site + account group are required;
    partition and qos are optional filters — when omitted, every association
    matching the remaining criteria is removed."""

    op_name = 'remove_slurm_association'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        site_name: str,
        account_group_name: str,
        partition_name: str | None = None,
        qos_name: str | None = None,
    ) -> None:
        super().__init__(client, author)
        self.site_name = site_name
        self.account_group_name = account_group_name
        self.partition_name = partition_name
        self.qos_name = qos_name
        self.removed_count = 0

    async def execute(self, session: AsyncClientSession) -> int:
        site = await Site.find_one(Site.name == self.site_name)
        if site is None:
            raise ValueError(f'Site {self.site_name} does not exist')
        group = await Group.find_one(Group.name == self.account_group_name)
        if group is None:
            raise ValueError(
                f'Group {self.account_group_name} does not exist'
            )
        account = await SlurmAccount.find_one(
            SlurmAccount.group.id == group.id,
            SlurmAccount.site.id == site.id,
        )
        if account is None:
            raise ValueError(
                f'No SlurmAccount for {self.account_group_name} on {self.site_name}'
            )

        filters = [
            SlurmAssociation.site.id == site.id,
            SlurmAssociation.account.id == account.id,
        ]
        if self.partition_name is not None:
            partition = await SlurmPartition.find_one(
                SlurmPartition.name == self.partition_name,
                SlurmPartition.site.id == site.id,
            )
            if partition is None:
                raise ValueError(
                    f'SlurmPartition {self.partition_name} on {self.site_name} does not exist'
                )
            filters.append(SlurmAssociation.partition.id == partition.id)
        if self.qos_name is not None:
            qos = await SlurmQOS.find_one(
                SlurmQOS.name == self.qos_name,
                SlurmQOS.site.id == site.id,
            )
            if qos is None:
                raise ValueError(
                    f'SlurmQOS {self.qos_name} on {self.site_name} does not exist'
                )
            filters.append(SlurmAssociation.qos.id == qos.id)

        assocs = await SlurmAssociation.find(*filters).to_list()
        if not assocs:
            raise ValueError('No matching associations found')
        for a in assocs:
            await a.delete(session=session)
        self.removed_count = len(assocs)
        return self.removed_count

    def describe(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            'site': self.site_name,
            'account_group': self.account_group_name,
            'removed_count': self.removed_count,
        }
        if self.partition_name is not None:
            data['partition'] = self.partition_name
        if self.qos_name is not None:
            data['qos'] = self.qos_name
        return data


class ProvisionSlurmAllocation(Operation):
    """Composite: create a QOS (if absent) with one initial allocation, then
    create an association linking the group's SlurmAccount to it.

    Replaces the old `cheeto db slurm new alloc`. If qos_name is None, it
    derives `{group}-{partition}-qos`.
    """

    op_name = 'provision_slurm_allocation'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        site_name: str,
        account_group_name: str,
        partition_name: str,
        qos_name: str | None = None,
        group_limits_tres: SlurmTRES | None = None,
        comment: str = '',
        priority: int = 0,
        flags: list[str] | None = None,
    ) -> None:
        super().__init__(client, author)
        self.site_name = site_name
        self.account_group_name = account_group_name
        self.partition_name = partition_name
        self.qos_name = qos_name or f'{account_group_name}-{partition_name}-qos'
        self.group_limits_tres = group_limits_tres
        self.comment = comment
        self.priority = priority
        self.flags = flags or ['DenyOnLimit']
        self.created_qos = False

    async def execute(self, session: AsyncClientSession) -> SlurmAssociation:
        site = await Site.find_one(Site.name == self.site_name)
        if site is None:
            raise ValueError(f'Site {self.site_name} does not exist')
        group = await Group.find_one(Group.name == self.account_group_name)
        if group is None:
            raise ValueError(
                f'Group {self.account_group_name} does not exist'
            )
        account = await SlurmAccount.find_one(
            SlurmAccount.group.id == group.id,
            SlurmAccount.site.id == site.id,
        )
        if account is None:
            raise ValueError(
                f'No SlurmAccount for {self.account_group_name} on {self.site_name}'
            )
        partition = await SlurmPartition.find_one(
            SlurmPartition.name == self.partition_name,
            SlurmPartition.site.id == site.id,
        )
        if partition is None:
            raise ValueError(
                f'SlurmPartition {self.partition_name} on {self.site_name} does not exist'
            )

        qos = await SlurmQOS.find_one(
            SlurmQOS.name == self.qos_name,
            SlurmQOS.site.id == site.id,
        )
        if qos is None:
            group_limits: list[SlurmAllocation] = []
            if self.group_limits_tres is not None:
                alloc = SlurmAllocation(
                    tres=self.group_limits_tres, comment=self.comment,
                )
                await alloc.insert(session=session)
                group_limits = [alloc]
            qos = SlurmQOS(
                name=self.qos_name,
                site=site,
                group_limits=group_limits,
                priority=self.priority,
                flags=self.flags,
            )
            await qos.insert(session=session)
            self.created_qos = True

        existing = await SlurmAssociation.find_one(
            SlurmAssociation.site.id == site.id,
            SlurmAssociation.account.id == account.id,
            SlurmAssociation.partition.id == partition.id,
            SlurmAssociation.qos.id == qos.id,
        )
        if existing is not None:
            self._assoc = existing
            return existing

        assoc = SlurmAssociation(
            site=site, account=account, partition=partition, qos=qos,
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
            'created_qos': self.created_qos,
        }


class SyncSlurm(Operation):
    """Reconcile a site's desired Slurm state (from beanie) onto a live
    controller via `sacctmgr`.

    Reads desired state with `build_desired_slurm_state`, reads current state
    from `sacctmgr`, diffs them, and emits ordered command batches. Dry-run by
    default (returns the planned command strings); `apply=True` dispatches each
    ordered batch concurrently (bounded by `concurrency`), tolerating per-command
    failures. A `max_deletions` cap aborts before any mutation if the plan would
    delete more than the cap (pass None to disable).

    Not transactional: the side effects are external `sacctmgr` calls that can't
    be rolled back, and a full sync can outlast the Mongo transaction lifetime.
    """

    op_name = 'sync_slurm'
    transactional = False

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        sitename: str,
        sacctmgr: AsyncSAcctMgr | DumpSAcctMgr | None = None,
        sudo: bool = False,
        apply: bool = False,
        concurrency: int = 8,
        max_deletions: int | None = 50,
        dump_commands: Path | None = None,
    ) -> None:
        super().__init__(client, author)
        self.sitename = sitename
        self.sacctmgr = sacctmgr
        self.sudo = sudo
        self.apply = apply
        self.concurrency = max(1, concurrency)
        self.max_deletions = max_deletions
        self.dump_commands = dump_commands
        self._plan: dict[str, list[str]] = {}
        self._tally: dict[str, dict[str, int]] = {}

    async def execute(self, session: AsyncClientSession) -> dict[str, Any]:
        site = await Site.find_one(Site.name == self.sitename)
        if site is None:
            raise ValueError(f'Site {self.sitename!r} does not exist')

        sacctmgr = self.sacctmgr or AsyncSAcctMgr(sudo=self.sudo)

        desired = await build_desired_slurm_state(site)
        current = await sacctmgr.read_current_state()
        batches = reconcile(desired, current)

        self._plan = {
            b.label: [str(spec) for spec in b.specs] for b in batches if b.specs
        }

        # Blast-radius guard. Only enforced on apply — a dry-run preview
        # should always show the full plan (deletions included).
        deletions = count_deletions(batches)
        if (
            self.apply
            and self.max_deletions is not None
            and deletions > self.max_deletions
        ):
            would_delete = [
                str(spec) for b in batches if b.is_deletion for spec in b.specs
            ]
            raise SlurmSyncAborted(
                f'Sync would delete {deletions} Slurm entities, exceeding the '
                f'cap of {self.max_deletions}. Pass --max-deletions=-1 to '
                f'disable the cap.',
                would_delete=would_delete,
            )

        if self.dump_commands is not None:
            with self.dump_commands.open('w') as fp:
                for b in batches:
                    for spec in b.specs:
                        print(str(spec), file=fp)

        if self.apply:
            await self._apply(sacctmgr, batches)

        return {
            'site': self.sitename,
            'apply': self.apply,
            'plan': dict(self._plan),
            'tally': dict(self._tally),
        }

    async def _apply(
        self, sacctmgr: AsyncSAcctMgr, batches: list[CommandBatch],
    ) -> None:
        """Dispatch each batch in order; within a batch run all commands
        concurrently (they're order-independent). One command failing never
        aborts the batch."""
        sem = asyncio.Semaphore(self.concurrency)

        for batch in batches:
            if not batch.specs:
                continue
            tally = {'ok': 0, 'failed': 0, 'total': len(batch.specs)}

            async def _one(spec, tally=tally):
                async with sem:
                    try:
                        await sacctmgr.dispatch(spec)
                    except sh.ErrorReturnCode as e:
                        self.logger.warning(
                            'slurm sync command failed: %s\n%s', spec, e,
                        )
                        tally['failed'] += 1
                    else:
                        tally['ok'] += 1

            await asyncio.gather(*(_one(s) for s in batch.specs))
            self._tally[batch.label] = tally

    def describe(self) -> dict[str, Any]:
        return {
            'site': self.sitename,
            'apply': self.apply,
            'sudo': self.sudo,
            'concurrency': self.concurrency,
            'planned': {label: len(cmds) for label, cmds in self._plan.items()},
            'tally': dict(self._tally),
        }
