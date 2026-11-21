#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2026
# (c) The Regents of the University of California, Davis, 2026
# File   : daemon/api.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 10.06.2026

from contextlib import asynccontextmanager

from fastapi import APIRouter, FastAPI

from ..config import Config
from ..db import connect_beanie
from .cache import ResponseCache
from .deps import KeyTouches
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
    # Read by `deps.authorize`. `api_key` is the static key of the config
    # (every scope); issued keys are `ApiKey` documents. Without
    # `allow_anonymous`, a request needs one or the other.
    api.state.api_key = config.api.api_key if config.api else None
    api.state.allow_anonymous = (
        config.api.allow_anonymous if config.api else False
    )
    api.state.key_touches = KeyTouches()
    # Read by `cache.CachedRoute`; None (no `api.cache` block) means every
    # request runs its handler. One cache per app instance.
    cache_config = config.api.cache if config.api else None
    api.state.cache = (
        ResponseCache(ttl=cache_config.ttl, stale=cache_config.stale,
                      max_entries=cache_config.max_entries)
        if cache_config is not None else None
    )

    # All routes live on one router so they can be mounted under a
    # configurable path prefix. Each resource router declares its own
    # scope (`deps.requires`); there is no router-wide key check.
    router = APIRouter()
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
