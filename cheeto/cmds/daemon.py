#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2026
# (c) The Regents of the University of California, Davis, 2026
# File   : cmds/daemon.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 10.06.2026

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
