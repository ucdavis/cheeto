#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2026
# (c) The Regents of the University of California, Davis, 2026
# File   : daemon/api.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 10.06.2026

from contextlib import asynccontextmanager

from fastapi import APIRouter, Depends, FastAPI

from ..config import Config
from ..db import connect_beanie
from .deps import require_api_key
from .routes import groups, puppet, sites, slurm, storage, users


def create_api(config: Config, client=None) -> FastAPI:
    """Build the cheeto REST API. Every route is a read (`GET`).

    Resources (`cheeto/daemon/routes/`): `/sites`, `/users`, `/groups`,
    `/sites/{site}/slurm`, `/sites/{site}/storage`, and the `/puppet`
    exports. The response models are in `cheeto/daemon/schemas.py`.

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
    # Read by `deps.require_api_key`; None disables the check.
    api.state.api_key = config.api.api_key if config.api else None

    # All routes live on one router so they can be mounted under a
    # configurable path prefix; the API key is enforced router-wide.
    router = APIRouter(dependencies=[Depends(require_api_key)])
    for module in (puppet, sites, users, groups, slurm, storage):
        router.include_router(module.router)

    api.include_router(router, prefix=_router_prefix(config))
    return api


def _router_prefix(config: Config) -> str:
    """Normalize the configured API prefix to FastAPI's router form: empty,
    or a leading-slash path with no trailing slash (e.g. 'cheeto/' -> '/cheeto')."""
    prefix = (config.api.prefix if config.api else '').strip().rstrip('/')
    if prefix and not prefix.startswith('/'):
        prefix = '/' + prefix
    return prefix
