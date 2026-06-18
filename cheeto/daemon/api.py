#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2026
# (c) The Regents of the University of California, Davis, 2026
# File   : daemon/api.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 10.06.2026

import secrets
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Request, Security
from fastapi.responses import PlainTextResponse
from fastapi.security import APIKeyHeader

from ..config import Config
from ..database.base import connect_beanie
from ..operations.site import ExportRootSSHKeys
from ..operations.storage import ExportPuppetStorage
from ..queries.site import find_site_by_name_or_fqdn

_api_key_header = APIKeyHeader(name='X-API-Key', auto_error=False)


def create_api(config: Config, client=None) -> FastAPI:
    """Build the cheeto REST API.

    `client` injects an already-initialized beanie client (tests pass the
    session-loop client; httpx's ASGITransport does not run the lifespan).
    Without it, the lifespan owns a connect_beanie/close pair."""

    @asynccontextmanager
    async def lifespan(api: FastAPI):
        own_client = client is None
        api.state.client = client or await connect_beanie(config.mongo,
                                                          quiet=True)
        yield
        if own_client:
            await api.state.client.close()

    api = FastAPI(title='cheeto', lifespan=lifespan,
                  root_path=config.api.root_path if config.api else '')
    if client is not None:
        api.state.client = client

    expected_key = config.api.api_key if config.api else None

    async def require_api_key(key: str | None = Security(_api_key_header)):
        if expected_key is None:
            return
        if key is None or not secrets.compare_digest(key, expected_key):
            raise HTTPException(status_code=401,
                                detail='invalid or missing API key')

    async def resolved_sitename(site: str) -> str:
        """Map the `{site}` path segment (a site name OR an fqdn) to the
        canonical site name the export operations key on; 404 if unknown."""
        resolved = await find_site_by_name_or_fqdn(site)
        if resolved is None:
            raise HTTPException(status_code=404,
                                detail=f'unknown site {site!r}')
        return resolved.name

    @api.get('/puppet/root-keys/{site}',
             response_class=PlainTextResponse,
             dependencies=[Depends(require_api_key)])
    async def root_keys(request: Request,
                        sitename: str = Depends(resolved_sitename)) -> str:
        return await ExportRootSSHKeys.run(request.app.state.client,
                                           None,
                                           sitename=sitename)

    @api.get('/puppet/storage/{site}',
             dependencies=[Depends(require_api_key)])
    async def puppet_storage(request: Request,
                            sitename: str = Depends(resolved_sitename)) -> dict:
        return await ExportPuppetStorage.run(request.app.state.client,
                                             None,
                                             sitename=sitename)

    return api
