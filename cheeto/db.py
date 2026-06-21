#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) The Regents of the University of California, Davis
# License: Modified BSD
#
# The v2 (beanie/async) MongoDB connection. Kept mongoengine-free so the whole
# v2 path can run without the optional `legacy` extra installed. The v1
# mongoengine connection lives in cheeto/legacy/database/base.py.

from .config import MongoConfig
from .log import Console


def _print_config(config: MongoConfig) -> None:
    console = Console(stderr=True)
    console.print('mongo config:')
    console.print(f'  uri: [green]{config.uri}:{config.port}')
    console.print(f'  user: [green]{config.user}')
    console.print(f'  db: [green]{config.database}')
    console.print(f'  tls: {config.tls}')


async def connect_beanie(config: MongoConfig, quiet: bool = False):
    from beanie import init_beanie
    from pymongo import AsyncMongoClient

    from .models import ALL_MODELS

    if not quiet:
        _print_config(config)
    kwargs = dict(
        tls=config.tls,
        tlsCAFile=config.tls_ca_file,
    )
    if config.user:
        kwargs.update(username=config.user, password=config.password)
    client = AsyncMongoClient(f'{config.uri}:{config.port}', **kwargs)
    await init_beanie(database=client[config.database], document_models=ALL_MODELS)
    return client
