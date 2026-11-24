"""Puppet exports: the routes the puppet server pulls, and the legacy
account map (`all.yaml`) of a site as JSON.

The scopes are declared per route, not on the router: the account map
needs `legacy`, the other two need `puppet`.
"""

import asyncio
import json

from fastapi import APIRouter, Depends, Request, Response

from ...models.site import Site
from ...operations.site import ExportRootSSHKeys
from ...operations.storage import ExportPuppetStorage
from ...puppet import PuppetAccountMap
from ...queries.puppet_legacy import (
    LegacyExportData,
    build_puppet_legacy,
    load_legacy_export_data,
)
from ...queries.site import find_site
from ...queries.user import root_ssh_keys
from ..cache import CachedRoute
from ..deps import requires, resolved_site

router = APIRouter(prefix='/puppet', tags=['puppet'],
                   route_class=CachedRoute)


@router.get('/root-keys/{site}', dependencies=requires('puppet'))
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


@router.get('/storage/{site}', dependencies=requires('puppet'))
async def puppet_storage(request: Request,
                         site: Site = Depends(resolved_site)) -> dict:
    return await ExportPuppetStorage.run(request.app.state.client,
                                         None,
                                         sitename=site.name,
                                         skip_history=True)


def _accounts_json(data: LegacyExportData) -> bytes:
    """The response body of `puppet_accounts`. CPU work only (the record
    validation, the dump and the JSON encoding), so it can run in a thread."""
    puppet_map = build_puppet_legacy(data, passwords=False)
    return json.dumps(PuppetAccountMap.Schema().dump(puppet_map)).encode()


@router.get(
    '/accounts/{site}',
    dependencies=requires('legacy'),
    response_class=Response,
    responses={200: {
        'content': {'application/json': {}},
        'description': 'The `user`, `group` and `share` maps of `all.yaml`.',
    }},
)
async def puppet_accounts(site: Site = Depends(resolved_site)) -> Response:
    """The users, groups and shares of a site, in the shape of
    `domains/<fqdn>/merged/all.yaml` of the puppet.hpc repo, without the
    `password` key of the user records.

    The database reads run on the event loop (the client belongs to it).
    The rest is about half of the time and pure CPU work, so it runs in a
    thread: with the GIL that makes nothing faster, but the event loop gets
    a turn every few milliseconds and the other requests do not wait. The
    body is returned as bytes, so FastAPI does not encode the map a second
    time."""
    data = await load_legacy_export_data(site)
    body = await asyncio.to_thread(_accounts_json, data)
    return Response(content=body, media_type='application/json')
