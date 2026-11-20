"""Slurm state of a site.

Two layers, kept apart:
- CONFIGURED: accounts, partitions, QOS and the account-level
  (account, partition, qos) associations, as stored.
- EFFECTIVE: `/user-associations`, the per-user rows the Slurm sync
  creates (`build_desired_slurm_state`), after the access and status gates.
"""

from typing import Literal

from fastapi import APIRouter, Depends, HTTPException

from ...models.site import Site
from ...queries.group import find_group_by_name
from ...queries.slurm import (
    build_desired_slurm_state,
    list_accounts_at_site,
    list_allocations_at_site,
    list_associations_at_site,
    list_partitions_at_site,
    list_qos_at_site,
    partition_at_site,
    qos_at_site,
)
from ..cache import CachedRoute
from ..convert import account_out, allocation_out, qos_out
from ..deps import resolved_site
from ..schemas import (
    AssociationOut,
    QOSAllocationOut,
    QOSOut,
    SiteSlurmUsers,
    SlurmAccountOut,
    UserAssociationOut,
)

router = APIRouter(prefix='/sites/{site}/slurm', tags=['slurm'],
                   route_class=CachedRoute)


async def _filters(site: Site, group, partition, qos) -> dict:
    """Resolve the optional name filters; an unknown name is a 404."""
    out: dict = {}
    if group is not None:
        out['group'] = await find_group_by_name(group)
        if out['group'] is None:
            raise HTTPException(status_code=404,
                                detail=f'unknown group {group!r}')
    if partition is not None:
        out['partition'] = await partition_at_site(site, partition)
        if out['partition'] is None:
            raise HTTPException(status_code=404,
                                detail=f'unknown partition {partition!r}')
    if qos is not None:
        out['qos'] = await qos_at_site(site, qos)
        if out['qos'] is None:
            raise HTTPException(status_code=404,
                                detail=f'unknown QOS {qos!r}')
    return out


@router.get('/accounts', response_model=list[SlurmAccountOut])
async def slurm_accounts(site: Site = Depends(resolved_site)):
    return [account_out(a) for a in await list_accounts_at_site(site)]


@router.get('/partitions', response_model=list[str])
async def slurm_partitions(site: Site = Depends(resolved_site)):
    return [p.name for p in await list_partitions_at_site(site)]


@router.get('/qos', response_model=list[QOSOut])
async def slurm_qos_list(site: Site = Depends(resolved_site)):
    return [qos_out(q) for q in await list_qos_at_site(site)]


@router.get('/qos/{name}', response_model=QOSOut)
async def slurm_qos(name: str, site: Site = Depends(resolved_site)):
    qos = await qos_at_site(site, name)
    if qos is None:
        raise HTTPException(status_code=404, detail=f'unknown QOS {name!r}')
    return qos_out(qos)


@router.get('/associations', response_model=list[AssociationOut])
async def slurm_associations(
    site: Site = Depends(resolved_site),
    group: str | None = None,
    partition: str | None = None,
    qos: str | None = None,
):
    """The configured (account, partition, qos) rows."""
    filters = await _filters(site, group, partition, qos)
    rows = [
        AssociationOut(account=a.account.group.name,
                       partition=a.partition.name, qos=a.qos.name)
        for a in await list_associations_at_site(site, **filters)
    ]
    return sorted(rows, key=lambda r: (r.account, r.partition, r.qos))


@router.get('/allocations', response_model=list[QOSAllocationOut])
async def slurm_allocations(
    site: Site = Depends(resolved_site),
    group: str | None = None,
    partition: str | None = None,
    qos: str | None = None,
    limit: Literal['group_limits', 'user_limits', 'job_limits'] | None = None,
):
    """The allocations of every QOS at the site, one row per allocation."""
    filters = await _filters(site, group, partition, qos)
    rows = await list_allocations_at_site(site, field=limit, **filters)
    return [
        QOSAllocationOut(
            qos=r.qos.name, limit=r.field,
            **allocation_out(r.allocation).model_dump(),
        )
        for r in rows
    ]


@router.get('/user-associations', response_model=SiteSlurmUsers)
async def slurm_user_associations(site: Site = Depends(resolved_site)):
    """The effective per-user associations: exactly the state the Slurm
    sync reconciles the controller to."""
    state = await build_desired_slurm_state(site)
    coordinators: dict[str, list[str]] = {}
    for account, user in sorted(state.coordinators):
        coordinators.setdefault(account, []).append(user)
    return SiteSlurmUsers(
        associations=[
            UserAssociationOut(
                user=user, account=account, partition=partition, qos=qos,
                max_jobs=state.association_max_jobs[(user, account, partition)],
            )
            for (user, account, partition), qos
            in sorted(state.associations.items())
        ],
        default_accounts=dict(sorted(state.default_accounts.items())),
        coordinators=coordinators,
    )
