"""Slurm-related query functions.

These return typed dataclasses holding raw beanie Documents. Dict formatting for
Rich/YAML display happens in the caller (e.g. cheeto/cmds/ng/_slurm_show.py).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from beanie import PydanticObjectId
from beanie.operators import In

from ..models.group import Group
from ..models.site import Site
from ..models.slurm import (
    SlurmAccount,
    SlurmAllocation,
    SlurmAssociation,
    SlurmPartition,
    SlurmQOS,
    SlurmTRES,
)
from ..models.user import User
from ..utils import size_to_megs


def total_tres(allocations: list[SlurmAllocation]) -> SlurmTRES:
    """Sum cpus/gpus/mem across a list of SlurmAllocations into a single SlurmTRES.

    Default-unlimited values (cpus=-1, gpus=-1, mem=None) are treated as
    'not contributing' and excluded from the sum. If no allocation sets a
    value for a given field, it stays at its default.
    """
    cpus = -1
    gpus = -1
    mem_megs: int | None = None

    for alloc in allocations:
        t = alloc.tres
        if t.cpus != -1:
            cpus = (0 if cpus == -1 else cpus) + t.cpus
        if t.gpus != -1:
            gpus = (0 if gpus == -1 else gpus) + t.gpus
        if t.mem is not None:
            mem_megs = (0 if mem_megs is None else mem_megs) + size_to_megs(t.mem)

    mem_str: str | None = None
    if mem_megs is not None:
        if mem_megs >= 1024 * 1024:
            mem_str = f'{mem_megs / (1024 * 1024):.10g}T'
        elif mem_megs >= 1024:
            mem_str = f'{mem_megs / 1024:.10g}G'
        else:
            mem_str = f'{mem_megs}M'

    return SlurmTRES(cpus=cpus, gpus=gpus, mem=mem_str)


@dataclass
class GroupSlurm:
    """The SlurmAccount and SlurmAssociations for a (group, site) pair."""
    account: SlurmAccount
    associations: list[SlurmAssociation] = field(default_factory=list)


@dataclass
class UserGroupSlurm:
    """One entry per (user's group, role) -> Slurm info accessible via that group."""
    group: Group
    role: str              # 'member' | 'slurmer'
    slurm: GroupSlurm


async def group_slurm_at_site(group: Group, site: Site) -> GroupSlurm | None:
    """Fetch the SlurmAccount and SlurmAssociations for (group, site), or None."""
    account = await SlurmAccount.find_one(
        SlurmAccount.group.id == group.id,
        SlurmAccount.site.id == site.id,
        fetch_links=True,
        nesting_depth=1,
    )
    if account is None:
        return None
    # nesting_depth=2 so qos.group_limits allocations resolve for total_tres().
    associations = await SlurmAssociation.find(
        SlurmAssociation.account.id == account.id,
        fetch_links=True,
        nesting_depth=2,
    ).to_list()
    return GroupSlurm(account=account, associations=associations)


async def user_slurm_at_site(user: User, site: Site) -> list[UserGroupSlurm]:
    """Find all Slurm resources the user has access to at `site` via their groups.

    Batched: issues ~4 queries total regardless of how many groups the user
    belongs to.
    """
    member_groups = await Group.find(Group.members.id == user.id).to_list()
    slurmer_groups = await Group.find(Group.slurmers.id == user.id).to_list()

    # Dedupe by group id; collect every role the user has in each group.
    group_roles: dict[PydanticObjectId, tuple[Group, list[str]]] = {}
    for g in member_groups:
        group_roles.setdefault(g.id, (g, []))[1].append('member')
    for g in slurmer_groups:
        if g.id in group_roles:
            group_roles[g.id][1].append('slurmer')
        else:
            group_roles[g.id] = (g, ['slurmer'])

    if not group_roles:
        return []

    # One query for all SlurmAccounts across these groups at this site.
    accounts = await SlurmAccount.find(
        In(SlurmAccount.group.id, list(group_roles.keys())),
        SlurmAccount.site.id == site.id,
        fetch_links=True,
        nesting_depth=1,
    ).to_list()
    if not accounts:
        return []

    account_by_group: dict[PydanticObjectId, SlurmAccount] = {
        a.group.id: a for a in accounts
    }

    # One query for all SlurmAssociations across these accounts.
    # nesting_depth=2 so qos.group_limits allocations resolve for total_tres().
    associations = await SlurmAssociation.find(
        In(SlurmAssociation.account.id, [a.id for a in accounts]),
        fetch_links=True,
        nesting_depth=2,
    ).to_list()

    assocs_by_account: dict[PydanticObjectId, list[SlurmAssociation]] = {}
    for a in associations:
        assocs_by_account.setdefault(a.account.id, []).append(a)

    results: list[UserGroupSlurm] = []
    for gid, (group, roles) in group_roles.items():
        account = account_by_group.get(gid)
        if account is None:
            continue
        gs = GroupSlurm(
            account=account,
            associations=assocs_by_account.get(account.id, []),
        )
        for role in roles:
            results.append(UserGroupSlurm(group=group, role=role, slurm=gs))
    return results


# ---------------------------------------------------------------------------
# QOS lookups
# ---------------------------------------------------------------------------


async def qos_at_site(site: Site, name: str) -> SlurmQOS | None:
    """Fetch a single SlurmQOS at a site, with allocations resolved."""
    return await SlurmQOS.find_one(
        SlurmQOS.name == name,
        SlurmQOS.site.id == site.id,
        fetch_links=True,
        nesting_depth=1,
    )


async def list_qos_at_site(site: Site) -> list[SlurmQOS]:
    """List every SlurmQOS at a site, with allocations resolved."""
    return await SlurmQOS.find(
        SlurmQOS.site.id == site.id,
        fetch_links=True,
        nesting_depth=1,
    ).sort('+name').to_list()


# ---------------------------------------------------------------------------
# Association lookups
# ---------------------------------------------------------------------------


async def association_at(
    site: Site,
    group: Group,
    partition: SlurmPartition,
    qos: SlurmQOS,
) -> SlurmAssociation | None:
    """Fetch one SlurmAssociation by its (site, account.group, partition, qos) tuple."""
    account = await SlurmAccount.find_one(
        SlurmAccount.group.id == group.id,
        SlurmAccount.site.id == site.id,
    )
    if account is None:
        return None
    return await SlurmAssociation.find_one(
        SlurmAssociation.site.id == site.id,
        SlurmAssociation.account.id == account.id,
        SlurmAssociation.partition.id == partition.id,
        SlurmAssociation.qos.id == qos.id,
        fetch_links=True,
        nesting_depth=2,
    )


async def list_associations_at_site(
    site: Site,
    *,
    group: Group | None = None,
    partition: SlurmPartition | None = None,
    qos: SlurmQOS | None = None,
) -> list[SlurmAssociation]:
    """List SlurmAssociations at a site, optionally filtered by group/partition/qos."""
    filters = [SlurmAssociation.site.id == site.id]
    if group is not None:
        account = await SlurmAccount.find_one(
            SlurmAccount.group.id == group.id,
            SlurmAccount.site.id == site.id,
        )
        if account is None:
            return []
        filters.append(SlurmAssociation.account.id == account.id)
    if partition is not None:
        filters.append(SlurmAssociation.partition.id == partition.id)
    if qos is not None:
        filters.append(SlurmAssociation.qos.id == qos.id)
    return await SlurmAssociation.find(
        *filters, fetch_links=True, nesting_depth=2,
    ).to_list()
