#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2026
# (c) The Regents of the University of California, Davis, 2026
# File   : daemon/app.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 10.06.2026

import os
import ssl
from functools import lru_cache
from pathlib import Path
from urllib.parse import quote_plus

from celery import Celery

from ..config import (
    BrokerSSLConfig,
    Config,
    DEFAULT_CONFIG_PATH,
    MongoConfig,
    get_config,
)

app = Celery('cheeto')


@lru_cache(maxsize=1)
def daemon_config() -> Config:
    """Load the cheeto Config from the CHEETO_CONFIG / CHEETO_PROFILE env
    vars (set by `cheeto daemon ...` before celery starts). Called at task
    runtime — post-fork in prefork workers — and cached per-process. Plain
    data, so fork-safe."""
    path = os.environ.get('CHEETO_CONFIG', str(DEFAULT_CONFIG_PATH))
    profile = os.environ.get('CHEETO_PROFILE', 'default')
    config = get_config(config_path=Path(path), profile=profile)
    if config is None:
        raise RuntimeError(f'could not load config from {path}')
    if config.daemon is None:
        raise RuntimeError(
            f'no daemon config block in {path} (profile {profile!r})'
        )
    return config


def mongo_result_backend(mongo: MongoConfig) -> tuple[str, dict]:
    """Build the celery mongodb result-backend URL and the
    `mongodb_backend_settings` dict from our MongoConfig. Task results land
    in the app database, in their own `celery_taskmeta` collection."""
    auth = ''
    if mongo.user:
        auth = f'{quote_plus(mongo.user)}:{quote_plus(mongo.password)}@'
    query = []
    if mongo.tls:
        query.append('tls=true')
        if mongo.tls_ca_file:
            query.append(f'tlsCAFile={quote_plus(mongo.tls_ca_file)}')
    qs = ('?' + '&'.join(query)) if query else ''
    url = f'mongodb://{auth}{mongo.uri}:{mongo.port}/{qs}'
    settings = {'database': mongo.database,
                'taskmeta_collection': 'celery_taskmeta'}
    return url, settings


_CERT_REQS = {
    'none': ssl.CERT_NONE,
    'optional': ssl.CERT_OPTIONAL,
    'required': ssl.CERT_REQUIRED,
}


def broker_use_ssl(cfg: BrokerSSLConfig) -> dict:
    """Translate our BrokerSSLConfig into celery's `broker_use_ssl` dict (the
    ssl options handed to the pyamqp/RabbitMQ socket): verify the broker
    against `ca_file`, optionally present a client cert for mutual TLS."""
    try:
        cert_reqs = _CERT_REQS[cfg.cert_reqs]
    except KeyError:
        raise ValueError(
            f'broker_use_ssl.cert_reqs must be one of '
            f'{sorted(_CERT_REQS)}; got {cfg.cert_reqs!r}'
        )
    opts: dict = {'cert_reqs': cert_reqs}
    if cfg.ca_file:
        opts['ca_certs'] = cfg.ca_file
    if cfg.cert_file:
        opts['certfile'] = cfg.cert_file
    if cfg.key_file:
        opts['keyfile'] = cfg.key_file
    return opts


def configure_celery_app(config: Config, celery_app: Celery = app) -> Celery:
    if config.daemon is None:
        raise RuntimeError('config has no daemon block')
    from .schedule import build_beat_schedule
    backend_url, backend_settings = mongo_result_backend(config.mongo)
    celery_app.conf.update(
        broker_url=config.daemon.broker_url,
        result_backend=backend_url,
        mongodb_backend_settings=backend_settings,
        task_default_queue='cheeto',
        task_serializer='json',
        result_serializer='json',
        accept_content=['json'],
        timezone=config.daemon.timezone,
        enable_utc=True,
        # The syncs are idempotent reconciliations, but they must never run
        # concurrently against the same target: one task at a time per
        # worker, no prefetched backlog, ack on receipt (beat re-enqueues on
        # the next tick anyway; interval entries also carry `expires`).
        # Scale by adding site queues, not hub worker replicas.
        task_acks_late=False,
        worker_prefetch_multiplier=1,
        worker_concurrency=1,
        task_time_limit=config.daemon.task_time_limit,
        task_soft_time_limit=max(60, config.daemon.task_time_limit - 300),
        result_expires=86400,  # celery.backend_cleanup prunes celery_taskmeta
        worker_hijack_root_logger=False,  # keep cheeto's RichHandler
        beat_schedule_filename=config.daemon.beat_schedule_filename,
        beat_schedule=build_beat_schedule(config),
    )
    # TLS for the broker (amqps). Only set when configured so the default
    # plaintext path is untouched.
    if config.daemon.broker_use_ssl is not None:
        celery_app.conf.broker_use_ssl = broker_use_ssl(
            config.daemon.broker_use_ssl
        )
    return celery_app


# Support `celery -A cheeto.daemon.app:app ...` (e.g. `celery inspect`) when
# the env vars are set; the `cheeto daemon` commands configure explicitly, so
# config errors surface loudly there rather than at import.
if os.environ.get('CHEETO_CONFIG'):
    try:
        configure_celery_app(daemon_config())
    except Exception:
        pass

from . import tasks  # noqa: E402, F401  — register tasks on `app` at import
