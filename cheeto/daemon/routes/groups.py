"""Groups: the global record, and the per-site views under
`/groups/{groupname}/sites/{site}`."""

from dataclasses import dataclass
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException

from ...constants import GROUP_TYPES
from ...models.group import Group
from ...models.site import Site
from ...queries.group import (
    find_group_site_info,
    find_groups,
    group_members_at_site,
    group_site_names,
    is_sticky_group,
)
from ...queries.site import find_site
from ...queries.slurm import group_slurm_at_site, slurm_account_at_site
from ...queries.storage import list_site_storages
from ..cache import CachedRoute
from ..convert import account_out, storages_out
from ..deps import requires, resolved_group, resolved_site
from ..schemas import (
    GroupOut,
    GroupRoster,
    GroupSiteOut,
    GroupSlurmOut,
    GroupSummary,
    PartitionQOS,
    StorageOut,
)

router = APIRouter(prefix='/groups', tags=['groups'],
                   route_class=CachedRoute,
                   dependencies=requires('groups'))

GroupType = Literal[GROUP_TYPES]          # type: ignore[valid-type]


@dataclass
class GroupAtSite:
    group: Group
    site: Site


async def resolved_group_site(
    group: Group = Depends(resolved_group),
    site: Site = Depends(resolved_site),
) -> GroupAtSite:
    """A group that is present at the site (has a `GroupSiteInfo`)."""
    if await find_group_site_info(group, site) is None:
        raise HTTPException(
            status_code=404,
            detail=f'group {group.name!r} is not at site {site.name!r}',
        )
    return GroupAtSite(group=group, site=site)


@router.get('', response_model=list[GroupSummary])
async def list_groups(
    type: GroupType | None = None,
    site: str | None = None,
    user: str | None = None,
):
    """Groups that match every given filter. Without `type`, the per-user
    groups and the access, status and user-type groups are left out."""
    sitename = None
    if site is not None:
        resolved = await find_site(site)
        if resolved is None:
            raise HTTPException(status_code=404,
                                detail=f'unknown site {site!r}')
        sitename = resolved.name
    groups = await find_groups(type=type, site=sitename, user=user)
    return [GroupSummary(name=g.name, gid=g.gid, type=g.type) for g in groups]


@router.get('/{groupname}', response_model=GroupOut)
async def get_group(group: Group = Depends(resolved_group)):
    return GroupOut(
        name=group.name, gid=group.gid, type=group.type,
        sites=await group_site_names(group),
        created_at=group.created_at, updated_at=group.updated_at,
    )


@router.get('/{groupname}/sites/{site}', response_model=GroupSiteOut)
async def group_site(at: GroupAtSite = Depends(resolved_group_site)):
    roster = await group_members_at_site(at.group, at.site)
    account = await slurm_account_at_site(at.group, at.site)
    return GroupSiteOut(
        name=at.group.name, gid=at.group.gid, type=at.group.type,
        site=at.site.name,
        sticky=is_sticky_group(at.group, at.site),
        slurm_account=account is not None,
        **{role: len(names) for role, names in roster.items()},
    )


@router.get('/{groupname}/sites/{site}/members', response_model=GroupRoster)
async def group_site_members(at: GroupAtSite = Depends(resolved_group_site)):
    """Usernames by role. `members` of a sticky group is every user at the
    site."""
    return GroupRoster(**await group_members_at_site(at.group, at.site))


@router.get('/{groupname}/sites/{site}/slurm', response_model=GroupSlurmOut)
async def group_site_slurm(at: GroupAtSite = Depends(resolved_group_site)):
    info = await group_slurm_at_site(at.group, at.site)
    if info is None:
        raise HTTPException(
            status_code=404,
            detail=f'group {at.group.name!r} has no Slurm account at site '
                   f'{at.site.name!r}',
        )
    return GroupSlurmOut(
        account=account_out(info.account),
        associations=sorted(
            (
                PartitionQOS(partition=a.partition.name, qos=a.qos.name)
                for a in info.associations
            ),
            key=lambda pq: (pq.partition, pq.qos),
        ),
    )


@router.get('/{groupname}/sites/{site}/storage',
            response_model=list[StorageOut])
async def group_site_storage(at: GroupAtSite = Depends(resolved_group_site)):
    return storages_out(
        await list_site_storages(at.site, group_id=at.group.id),
    )
