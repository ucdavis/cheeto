"""Slurm-related query functions.

These return typed dataclasses holding raw beanie Documents. Dict formatting for
Rich/YAML display happens in the caller (e.g. cheeto/cmds/ng/_slurm_show.py).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from beanie import PydanticObjectId
from beanie.operators import In


SlurmRole = Literal['member', 'slurmer', 'sticky-member', 'sticky']

from ..models.group import Group
from ..models.base import link_target_id
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
from ..models.user_site_info import UserSiteInfo
from ..utils import size_to_megs
from .group import group_members_at_site, user_groups_at_site
from .user import find_users


def total_tres(allocations: list[SlurmAllocation]) -> SlurmTRES:
    """Sum cpus/gpus/mem across a list of SlurmAllocations into a single SlurmTRES.

    Default-unlimited values (cpus=None, gpus=None, mem=None) are treated as
    'not contributing' and excluded from the sum. If no allocation sets a
    value for a given field, it stays at None (unlimited).
    """
    cpus: int | None = None
    gpus: int | None = None
    mem_megs: int | None = None

    for alloc in allocations:
        t = alloc.tres
        if t.cpus is not None:
            cpus = (0 if cpus is None else cpus) + t.cpus
        if t.gpus is not None:
            gpus = (0 if gpus is None else gpus) + t.gpus
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
    role: SlurmRole
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
    """Find all Slurm resources the user has access to at `site`.

    Accounts come from two sources:
      1. The user's group memberships at the site (`member` / `slurmer` /
         `sticky-member` roles). `user_groups_at_site` handles the
         direct + `site.group.sticky` union.
      2. `site.slurm.sticky` — accounts every user at the site implicitly
         has access to (role `sticky`). Only applies when the user has a
         UserSiteInfo at `site`.

    Batched: ~5 queries total regardless of group count.
    """
    group_rows = await user_groups_at_site(user, site)
    group_roles: dict[PydanticObjectId, tuple[Group, list[SlurmRole]]] = {
        row.group.id: (row.group, list(row.roles)) for row in group_rows
    }

    # Sticky accounts apply only to users present at the site. Skip the USI
    # round-trip when the sticky list is empty (the common case). When
    # `group_roles` is non-empty the user already has a USI (or a direct
    # membership) at the site, but for sticky accounts we still require an
    # explicit USI.
    sticky_account_ids: set[PydanticObjectId] = set()
    if site.slurm.sticky:
        usi = await UserSiteInfo.find_one(
            UserSiteInfo.user.id == user.id,
            UserSiteInfo.site.id == site.id,
        )
        if usi is not None:
            sticky_account_ids = {
                link_target_id(link) for link in site.slurm.sticky
            }

    if not group_roles and not sticky_account_ids:
        return []

    # One query for all SlurmAccounts across these groups at this site,
    # plus any sticky accounts not already covered by a group-derived
    # account.
    accounts: list[SlurmAccount] = []
    if group_roles:
        accounts.extend(await SlurmAccount.find(
            In(SlurmAccount.group.id, list(group_roles.keys())),
            SlurmAccount.site.id == site.id,
            fetch_links=True,
            nesting_depth=1,
        ).to_list())
    fetched_ids = {a.id for a in accounts}
    missing_sticky = sticky_account_ids - fetched_ids
    if missing_sticky:
        accounts.extend(await SlurmAccount.find(
            In(SlurmAccount.id, list(missing_sticky)),
            fetch_links=True,
            nesting_depth=1,
        ).to_list())
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

    # Emit sticky-only entries for accounts the user didn't reach via a
    # group membership at the site. `account.group` is a fetched Group at
    # nesting_depth=1 so we can use it as the row's group label.
    reached_group_ids = {gid for gid in group_roles}
    for account in accounts:
        if account.id not in sticky_account_ids:
            continue
        if account.group.id in reached_group_ids:
            continue
        gs = GroupSlurm(
            account=account,
            associations=assocs_by_account.get(account.id, []),
        )
        results.append(UserGroupSlurm(
            group=account.group, role='sticky', slurm=gs,
        ))
    return results


async def resolve_slurm_account_label(link) -> str | None:
    """Render a `Link[SlurmAccount]` as the owning group's name (the
    `SlurmAccount` itself has no name — its identity is `(group, site)`).
    Two lightweight fetches: one for the account, one for the group."""
    if link is None:
        return None
    if isinstance(link, SlurmAccount):
        account = link
    else:
        account = await SlurmAccount.get(link_target_id(link))
    if account is None:
        return None
    group_id = link_target_id(account.group)
    if group_id is None:
        return None
    if isinstance(account.group, Group):
        return account.group.name
    group = await Group.get(group_id, with_children=True)
    return group.name if group is not None else None


async def resolve_slurm_account_labels(links) -> list[str]:
    """Batched: one `In()` query for accounts, one for their groups."""
    if not links:
        return []
    account_ids: list[PydanticObjectId] = []
    accounts: list[SlurmAccount] = []
    for link in links:
        if isinstance(link, SlurmAccount):
            accounts.append(link)
            continue
        target_id = link_target_id(link)
        if target_id is not None:
            account_ids.append(target_id)
    if account_ids:
        accounts.extend(
            await SlurmAccount.find(In(SlurmAccount.id, account_ids)).to_list()
        )
    if not accounts:
        return []

    group_ids = [link_target_id(a.group) for a in accounts]
    group_ids = [gid for gid in group_ids if gid is not None]
    groups = await Group.find(
        In(Group.id, group_ids), with_children=True,
    ).to_list()
    name_by_id = {g.id: g.name for g in groups}
    return sorted(
        n for a in accounts
        if (n := name_by_id.get(link_target_id(a.group))) is not None
    )


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


# ---------------------------------------------------------------------------
# Partition lookups
# ---------------------------------------------------------------------------


async def partition_at_site(site: Site, name: str) -> SlurmPartition | None:
    return await SlurmPartition.find_one(
        SlurmPartition.name == name,
        SlurmPartition.site.id == site.id,
    )


async def list_partitions_at_site(
    site: Site,
    *,
    group: Group | None = None,
) -> list[SlurmPartition]:
    """List partitions at a site. With `group`, restrict to partitions the
    group has at least one association on."""
    if group is None:
        return await SlurmPartition.find(
            SlurmPartition.site.id == site.id,
        ).sort('+name').to_list()

    assocs = await list_associations_at_site(site, group=group)
    seen: dict[PydanticObjectId, SlurmPartition] = {}
    for a in assocs:
        # a.partition is a resolved SlurmPartition Document at nesting_depth=2.
        seen[a.partition.id] = a.partition
    return sorted(seen.values(), key=lambda p: p.name)


# ---------------------------------------------------------------------------
# Allocation lookups
# ---------------------------------------------------------------------------


@dataclass
class QOSAllocation:
    """An allocation contextualized by which QOS owns it and in which limit list."""
    qos: SlurmQOS
    field: str          # 'group_limits' | 'user_limits' | 'job_limits'
    allocation: SlurmAllocation


def explode_qos_allocations(
    qos: SlurmQOS,
    *,
    field: str | None = None,
) -> list[QOSAllocation]:
    """Flatten a QOS's three limit lists into (qos, field, alloc) tuples.

    Synchronous — assumes `qos` was fetched with allocations resolved
    (e.g. `fetch_links=True, nesting_depth=1` on the SlurmQOS query, or
    `nesting_depth=2` when reaching it through a SlurmAssociation).
    """
    out: list[QOSAllocation] = []
    field_lists = (
        ('group_limits', qos.group_limits),
        ('user_limits', qos.user_limits),
        ('job_limits', qos.job_limits),
    )
    for fname, allocs in field_lists:
        if field is not None and field != fname:
            continue
        for a in allocs:
            out.append(QOSAllocation(qos=qos, field=fname, allocation=a))
    return out


async def list_allocations_at_site(
    site: Site,
    *,
    group: Group | None = None,
    partition: SlurmPartition | None = None,
    qos: SlurmQOS | None = None,
    field: str | None = None,
) -> list[QOSAllocation]:
    """Find allocations across QOSes at a site, optionally narrowed.

    - With `qos`: only that QOS's allocations.
    - With `group` and/or `partition`: only QOSes referenced by matching
      associations.
    - `field` further restricts to a single limit list.
    """
    if qos is not None:
        full = await SlurmQOS.find_one(
            SlurmQOS.id == qos.id,
            fetch_links=True,
            nesting_depth=1,
        )
        if full is None:
            return []
        return explode_qos_allocations(full, field=field)

    qos_ids: set[PydanticObjectId] | None = None
    if group is not None or partition is not None:
        assocs = await list_associations_at_site(
            site, group=group, partition=partition,
        )
        if not assocs:
            return []
        qos_ids = {a.qos.id for a in assocs}

    qoses = await list_qos_at_site(site)
    if qos_ids is not None:
        qoses = [q for q in qoses if q.id in qos_ids]

    out: list[QOSAllocation] = []
    for q in qoses:
        out.extend(explode_qos_allocations(q, field=field))
    return out


# ---------------------------------------------------------------------------
# Desired-state extraction for `ng slurm sync`
# ---------------------------------------------------------------------------


def _tres_limit(tres: SlurmTRES):
    """Collapse a SlurmTRES into a normalized TRESLimit (mem → megabytes)."""
    from ..slurm_sync import TRESLimit
    return TRESLimit(
        cpus=tres.cpus,
        gpus=tres.gpus,
        mem_megs=size_to_megs(tres.mem) if tres.mem else None,
    )


async def build_desired_slurm_state(site: Site):
    """Assemble the site's desired Slurm state (QOS, accounts, associations)
    from beanie into a `SlurmSyncState` for reconciliation against a
    controller. Associations are the cross product of each group's
    SlurmAssociations with its slurm-eligible members/slurmers at the site
    (users whose effective access at the site includes `slurm`). Only
    accounts that end up with at least one association are included.
    """
    from ..slurm_sync import (
        AccountState,
        PROTECTED_QOS,
        QOSState,
        SlurmSyncState,
    )

    state = SlurmSyncState()

    # QOS: collapse each limit list into a single normalized TRES.
    for qos in await list_qos_at_site(site):
        if qos.name in PROTECTED_QOS:
            continue
        state.qos[qos.name] = QOSState(
            group=_tres_limit(total_tres(qos.group_limits)),
            user=_tres_limit(total_tres(qos.user_limits)),
            job=_tres_limit(total_tres(qos.job_limits)),
            priority=qos.priority,
            flags=frozenset(qos.flags),
        )

    # slurm-eligible users at the site (effective access includes 'slurm').
    eligible = {u.name for u in await find_users(access='slurm', site=site.name)}

    # All accounts at the site, keyed by owning group name.
    accounts = await SlurmAccount.find(
        SlurmAccount.site.id == site.id, fetch_links=True, nesting_depth=1,
    ).to_list()
    account_state_by_group: dict[str, AccountState] = {}
    for acct in accounts:
        account_state_by_group[acct.group.name] = AccountState(
            max_user_jobs=acct.limits.max_user_jobs,
            max_group_jobs=acct.limits.max_group_jobs,
            max_submit_jobs=acct.limits.max_submit_jobs,
            max_job_length=acct.limits.max_job_length,
        )

    # Associations: each (group, partition, qos) × eligible members/slurmers.
    roster_cache: dict[PydanticObjectId, set[str]] = {}
    used_groups: set[str] = set()
    for assoc in await list_associations_at_site(site):
        group = assoc.account.group          # resolved at nesting_depth=2
        partition_name = assoc.partition.name
        qos_name = assoc.qos.name
        if group.id not in roster_cache:
            roster = await group_members_at_site(group, site)
            roster_cache[group.id] = (
                set(roster['members']) | set(roster['slurmers'])
            )
        members = roster_cache[group.id] & eligible
        if members:
            used_groups.add(group.name)
        for username in members:
            state.associations[(username, group.name, partition_name)] = qos_name

    state.accounts = {
        gname: acct for gname, acct in account_state_by_group.items()
        if gname in used_groups
    }

    # Per-user default account. Today every at-site user resolves to the
    # site's configured default; `_desired_default_account` is the seam for a
    # future per-user override. Only users with at least one association are
    # eligible (the default account is sticky, so they're associated with it).
    site_default = await resolve_slurm_account_label(site.slurm.default_account)
    if site_default is not None:
        for (username, _account, _partition) in state.associations:
            state.default_accounts[username] = _desired_default_account(
                username, site_default,
            )

    return state


def _desired_default_account(username: str, site_default: str) -> str:
    """The default account a user should have at the site. Returns the site
    default for now; the signature leaves room for a future per-user
    override (e.g. a UserSiteInfo-level setting)."""
    return site_default
