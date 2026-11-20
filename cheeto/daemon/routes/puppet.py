"""Puppet exports: the routes the puppet server pulls."""

from fastapi import APIRouter, Depends, Request

from ...models.site import Site
from ...operations.site import ExportRootSSHKeys
from ...operations.storage import ExportPuppetStorage
from ...queries.site import find_site
from ...queries.user import root_ssh_keys
from ..deps import resolved_site

router = APIRouter(prefix='/puppet', tags=['puppet'])


@router.get('/root-keys/{site}')
async def root_keys(site: str, request: Request) -> dict:
    # Unlike storage, root keys fall back to the global set (every admin
    # with root-ssh) when the site is unknown rather than 404ing.
    resolved = await find_site(site)
    sitename = resolved.name if resolved is not None else None
    blocks = await ExportRootSSHKeys.run(request.app.state.client,
                                         None,
                                         sitename=sitename,
                                         skip_history=True)
    return {'root-ssh-public-keys': root_ssh_keys(blocks)}


@router.get('/storage/{site}')
async def puppet_storage(request: Request,
                         site: Site = Depends(resolved_site)) -> dict:
    return await ExportPuppetStorage.run(request.app.state.client,
                                         None,
                                         sitename=site.name,
                                         skip_history=True)
