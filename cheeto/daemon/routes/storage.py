"""Storage state of a site: the user-facing storages, the backing volumes
and the automount maps."""

from typing import Literal

from fastapi import APIRouter, Depends, HTTPException

from ...constants import STORAGE_CATEGORIES
from ...models.site import Site
from ...queries.group import find_group_by_name
from ...queries.storage import (
    list_site_automount_maps,
    list_site_storages,
    list_site_volumes,
)
from ...queries.user import find_user_by_name
from ..cache import CachedRoute
from ..convert import storages_out, volume_out
from ..deps import resolved_site
from ..schemas import AutomountMapOut, StorageOut, VolumeOut

router = APIRouter(prefix='/sites/{site}/storage', tags=['storage'],
                   route_class=CachedRoute)

StorageCategory = Literal[tuple(STORAGE_CATEGORIES)]  # type: ignore[valid-type]


@router.get('/storages', response_model=list[StorageOut])
async def storages(
    site: Site = Depends(resolved_site),
    category: StorageCategory | None = None,
    owner: str | None = None,
    group: str | None = None,
):
    """Every storage at the site; `category=share` gives the shares."""
    owner_id = group_id = None
    if owner is not None:
        user = await find_user_by_name(owner)
        if user is None:
            raise HTTPException(status_code=404,
                                detail=f'unknown user {owner!r}')
        owner_id = user.id
    if group is not None:
        found = await find_group_by_name(group)
        if found is None:
            raise HTTPException(status_code=404,
                                detail=f'unknown group {group!r}')
        group_id = found.id
    return storages_out(await list_site_storages(
        site, category, owner_id=owner_id, group_id=group_id,
    ))


@router.get('/volumes', response_model=list[VolumeOut])
async def volumes(site: Site = Depends(resolved_site)):
    return [volume_out(v) for v in await list_site_volumes(site)]


@router.get('/automount-maps', response_model=list[AutomountMapOut])
async def automount_maps(site: Site = Depends(resolved_site)):
    return [
        AutomountMapOut(name=m.name, prefix=m.prefix,
                        options=list(m.options))
        for m in await list_site_automount_maps(site)
    ]
