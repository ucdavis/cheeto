#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2026
# (c) The Regents of the University of California, Davis, 2026
# File   : daemon/tasks.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 10.06.2026

import asyncio
import logging
import os
from pathlib import Path

from pymongo import AsyncMongoClient

from .app import app, daemon_config
from ..config import Config
from ..database.base import connect_beanie
from ..iam_async import AsyncIAMAPI
from ..ldap_async import AsyncLDAPManager
from ..models import User
from ..operations.hippo import HippoEventProcessor
from ..operations.iam import ReapOffboardedUsers, SyncAllUsersIAM
from ..operations.ldap import SyncSiteLDAP
from ..operations.site import ExportSympaEmails
from ..operations.slurm import SyncSlurm

logger = logging.getLogger(__name__)


def run_op(coro_fn, *args):
    """Bridge an async op body into a sync celery task: run it in a fresh
    event loop with a fresh loop-bound beanie client (AsyncMongoClient must
    be created on the loop it is used from — never share one across task
    runs), resolving the daemon author user first."""
    async def _runner():
        config = daemon_config()
        client = await connect_beanie(config.mongo, quiet=True)
        try:
            author = await User.find_one(User.name == config.daemon.author)
            if author is None:
                logger.warning(
                    f'daemon author {config.daemon.author!r} not found; '
                    'History entries will have author=None'
                )
            return await coro_fn(config, client, author, *args)
        finally:
            await client.close()
    return asyncio.run(_runner())


async def _hippo_process(config: Config,
                         client: AsyncMongoClient,
                         author: User | None) -> None:
    tcfg = config.daemon.tasks.hippo
    processor = HippoEventProcessor(client, config.hippo, author=author)
    await processor.process(post_back=tcfg.post_back)


async def _iam_sync(config: Config,
                    client: AsyncMongoClient,
                    author: User | None) -> dict[str, int]:
    tcfg = config.daemon.tasks.iam_sync
    async with AsyncIAMAPI(config.ucdiam) as iam_api:
        return await SyncAllUsersIAM.run(
            client, author,
            iam_api=iam_api,
            grace_days=config.ucdiam.grace_days,
            expiry_offset_days=config.ucdiam.expiry_offset_days,
            types=tcfg.types,
            max_users=tcfg.max_users,
            concurrency=tcfg.concurrency,
        )


async def _reap(config: Config,
                client: AsyncMongoClient,
                author: User | None) -> list[str]:
    return await ReapOffboardedUsers.run(client, author)


async def _ldap_sync(config: Config,
                     client: AsyncMongoClient,
                     author: User | None,
                     sitename: str) -> dict:
    tcfg = config.daemon.tasks.ldap_sync
    async with AsyncLDAPManager(config.ldap, sitename=sitename) as ldap:
        return await SyncSiteLDAP.run(
            client, author,
            sitename=sitename,
            ldap=ldap,
            force=tcfg.force,
            concurrency=tcfg.concurrency,
            prune=tcfg.prune,
            max_deletions=tcfg.max_deletions,
        )


async def _slurm_sync(config: Config,
                      client: AsyncMongoClient,
                      author: User | None,
                      sitename: str) -> dict:
    tcfg = config.daemon.tasks.slurm_sync
    return await SyncSlurm.run(
        client, author,
        sitename=sitename,
        sudo=tcfg.sudo,
        apply=tcfg.apply,
        concurrency=tcfg.concurrency,
        max_deletions=tcfg.max_deletions,
    )


async def _sympa_export(config: Config,
                        client: AsyncMongoClient,
                        author: User | None,
                        sitename: str) -> dict:
    tcfg = config.daemon.tasks.sympa
    text = await ExportSympaEmails.run(client, author,
                                       sitename=sitename,
                                       ignore=tcfg.ignore)
    outdir = Path(tcfg.output_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    target = outdir / f'{sitename}.txt'
    tmp = outdir / f'.{sitename}.txt.tmp'
    tmp.write_text(text)
    os.replace(tmp, target)  # atomic: consumers never see a partial list
    return {'path': str(target),
            'emails': len(text.splitlines())}


# Explicit names so beat entries and queue routing are stable strings.
# SlurmSyncAborted / prune-abort exceptions propagate to a task FAILURE in
# the mongodb result backend — that is the alerting signal for a sync whose
# deletion count exceeded max_deletions.

@app.task(name='cheeto.hippo_process')
def hippo_process():
    return run_op(_hippo_process)


@app.task(name='cheeto.iam_sync')
def iam_sync():
    return run_op(_iam_sync)


@app.task(name='cheeto.reap')
def reap():
    return run_op(_reap)


@app.task(name='cheeto.ldap_sync')
def ldap_sync(sitename: str):
    return run_op(_ldap_sync, sitename)


@app.task(name='cheeto.slurm_sync')
def slurm_sync(sitename: str):
    return run_op(_slurm_sync, sitename)


@app.task(name='cheeto.sympa_export')
def sympa_export(sitename: str):
    return run_op(_sympa_export, sitename)
