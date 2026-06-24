#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2026
# (c) The Regents of the University of California, Davis, 2026
# File   : cmds/daemon.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 10.06.2026

import argparse
import logging
import os
from argparse import Namespace
from pathlib import Path

from ponderosa import ArgParser

from . import commands


@commands.register('daemon',
                   help='Persistent services: celery worker/beat and REST API')
def daemon_cmd(args: Namespace):
    pass


@daemon_cmd.args(common=True)
def daemon_args(parser: ArgParser):
    pass


@daemon_args.postprocessor(priority=150)
def export_config_env(args: Namespace):
    # Runs BEFORE the priority-100 parse_config postprocessor replaces
    # args.config (a Path) with the parsed Config object. The env vars are
    # how the config location reaches celery prefork children and
    # daemon_config() at task runtime.
    os.environ['CHEETO_CONFIG'] = str(args.config)
    os.environ['CHEETO_PROFILE'] = args.profile


@commands.register('daemon', 'worker',
                   help='Run a celery worker (hub by default; --site for a '
                        'cluster head-node slurm worker)')
def daemon_worker(args: Namespace):
    from ..daemon.app import app, configure_celery_app
    configure_celery_app(args.config)
    if args.site:
        queues = f'slurm.{args.site}'
        name = f'slurm-{args.site}@%h'
    else:
        queues = 'cheeto'
        name = 'cheeto-hub@%h'
    app.worker_main(['worker',
                     '-Q', queues,
                     '-n', name,
                     '--concurrency', str(args.concurrency),
                     '--loglevel', args.worker_loglevel])


@daemon_worker.args()
def _(parser: ArgParser):
    parser.add_argument('--site', default=None,
                        help='Consume only this site\'s slurm.<site> queue '
                             '(run on the cluster head node)')
    parser.add_argument('--concurrency', type=int, default=1,
                        help='Worker process pool size (default 1: syncs '
                             'must not overlap)')
    parser.add_argument('--worker-loglevel', default='INFO',
                        help='Celery worker log level')


@commands.register('daemon', 'beat',
                   help='Run the celery beat scheduler (exactly one instance '
                        'globally)')
def daemon_beat(args: Namespace):
    from ..daemon.app import app, configure_celery_app
    configure_celery_app(args.config)
    argv = ['beat', '--loglevel', args.worker_loglevel]
    if args.schedule_file:
        argv += ['-s', str(args.schedule_file)]
    if args.pidfile:
        argv += ['--pidfile', str(args.pidfile)]
    app.start(argv)


@daemon_beat.args()
def _(parser: ArgParser):
    parser.add_argument('--schedule-file', type=Path, default=None,
                        help='Beat schedule database path (default: '
                             'daemon.beat_schedule_filename from config)')
    parser.add_argument('--pidfile', type=Path, default=None,
                        help='Pidfile guarding against duplicate beats')
    parser.add_argument('--worker-loglevel', default='INFO',
                        help='Celery beat log level')


@commands.register('daemon', 'enqueue',
                   help='Enqueue a task for immediate execution on the '
                        'relevant worker')
def daemon_enqueue(args: Namespace):
    from ..daemon.app import app, configure_celery_app
    from ..daemon.schedule import build_enqueue_entries
    from ..log import Console

    console = Console()
    configure_celery_app(args.config)
    try:
        entries = build_enqueue_entries(args.config, args.task,
                                        sites=args.site)
    except ValueError as e:
        console.print(f'[red]{e}[/]')
        return 1

    results = []
    for entry in entries:
        # apply_async on the registered task (rather than send_task by
        # name) validates registration and honors task_always_eager.
        result = app.tasks[entry['task']].apply_async(
            args=entry['args'], **entry['options'],
        )
        results.append((entry, result))
        target = f' {entry["args"][0]}' if entry['args'] else ''
        console.print(
            f'[bold]{args.task}{target}[/] -> queue '
            f'[cyan]{entry["options"]["queue"]}[/] (id {result.id})'
        )

    if not args.wait:
        return 0

    failed = 0
    for entry, result in results:
        target = f' {entry["args"][0]}' if entry['args'] else ''
        try:
            value = result.get(timeout=args.timeout)
        except Exception as e:
            console.print(f'[red]{args.task}{target} FAILED:[/] {e}')
            failed += 1
        else:
            console.print(f'[green]{args.task}{target} done:[/] {value}')
    return 1 if failed else 0


@daemon_enqueue.args()
def _(parser: ArgParser):
    from ..daemon.schedule import TASK_SPECS
    parser.add_argument('task', choices=sorted(TASK_SPECS),
                        help='Task to enqueue')
    parser.add_argument('--site', action='append', default=None,
                        help='Target site for per-site tasks (repeatable; '
                             'default: all configured sites)')
    parser.add_argument('--wait', action='store_true', default=False,
                        help='Block until the task(s) finish and report '
                             'results (requires the result backend)')
    parser.add_argument('--timeout', type=float, default=3600,
                        help='Per-task wait timeout in seconds with --wait '
                             '(default 3600)')


@commands.register('daemon', 'flower',
                   help='Run Flower to monitor celery workers/tasks')
def daemon_flower(args: Namespace):
    from ..daemon.app import app, configure_celery_app
    # Launch flower on the configured app so it inherits broker_url,
    # broker_use_ssl (TLS), and the mongodb result backend.
    configure_celery_app(args.config)
    logger = logging.getLogger(__name__)

    # Flower is sensitive to broker url format; have to convert amqps to amqp and add ssl=true
    broker_url = app.conf.broker_url
    if broker_url.startswith('amqps://'):
        broker_url = broker_url.replace('amqps://', 'amqp://')
        broker_url = f'{broker_url}?ssl=true'

    app.conf.update(broker_url=broker_url)

    # now derive to rabbitmq API url
    _, _, broker_base_url = broker_url.partition('//')
    broker_base_url, _, _ = broker_base_url.rpartition(':')
    broker_api_url = f'https://{broker_base_url}:15671/api/'

    argv = ['flower',
            f'--address={args.address}',
            f'--port={args.port}',
            f'--broker-api={broker_api_url}']
    if args.basic_auth:
        argv.append(f'--basic_auth={args.basic_auth}')
    if args.url_prefix:
        argv.append(f'--url_prefix={args.url_prefix}')
    # Trailing args after `--` pass straight through to flower
    # (e.g. --persistent, --broker_api=…, --max_tasks=…).
    extra = args.flower_args[1:] if args.flower_args[:1] == ['--'] \
        else args.flower_args
    argv.extend(extra)
    logger.info(f'Starting flower with argv: {argv}')
    app.start(argv)


@daemon_flower.args()
def _(parser: ArgParser):
    parser.add_argument('--address', default='127.0.0.1',
                        help='Bind address (default 127.0.0.1; use 0.0.0.0 '
                             'in a container)')
    parser.add_argument('--port', type=int, default=5555,
                        help='Bind port (default 5555)')
    parser.add_argument('--basic-auth', default=None,
                        help='Restrict access: user:pass[,user:pass…]')
    parser.add_argument('--url-prefix', default=None,
                        help='Mount Flower under a path prefix (reverse-proxy)')
    parser.add_argument('flower_args', nargs=argparse.REMAINDER,
                        help='Extra flower flags, after `--`')


@commands.register('daemon', 'api',
                   help='Run the REST API (uvicorn)')
def daemon_api(args: Namespace):
    import uvicorn
    from ..daemon.api import create_api
    api_config = args.config.api
    host = args.host or (api_config.host if api_config else '127.0.0.1')
    port = args.port or (api_config.port if api_config else 8000)
    uvicorn.run(create_api(args.config), host=host, port=port)


@daemon_api.args()
def _(parser: ArgParser):
    parser.add_argument('--host', default=None,
                        help='Bind address (default: api.host from config)')
    parser.add_argument('--port', type=int, default=None,
                        help='Bind port (default: api.port from config)')
