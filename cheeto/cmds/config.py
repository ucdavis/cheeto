#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2024
# (c) The Regents of the University of California, Davis, 2023-2024
# File   : cmds/config.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 04.11.2024

from argparse import Namespace
import atexit
import logging
import os
from pathlib import Path
import sys

from ponderosa import ArgParser, arggroup

from . import commands
from .. import log, __version__
from ..config import (Config,
                      DEFAULT_CONFIG_PATH,
                      get_config,
                      LDAPConfig)
from ..errors import ExitCode


@commands.root.args('common config', common=True)
def common_args(parser: ArgParser):
    parser.add_argument('--log',
                       type=Path,
                       default=Path(os.devnull),
                       help='Log to file.')
    parser.add_argument('--quiet', '-q',
                       default=False,
                       action='store_true')
    parser.add_argument('--config',
                       type=Path,
                       default=DEFAULT_CONFIG_PATH,
                       help='Path to alternate config file')
    parser.add_argument('--profile', '-p',
                       default='default',
                       help='Config profile to use')


@common_args.postprocessor(priority=1000)
def print_version(args: Namespace):
    console = log.Console(stderr=True)
    if not args.quiet:
        console.print(f'cheeto [green]v{__version__}[/green]')


@common_args.postprocessor(priority=100)
def parse_config(args: Namespace):
    args.config = get_config(config_path=args.config, profile=args.profile)
    if 'accounts.hpc' in args.config.mongo.uri:
        pass
        #print("Testing right now, don't use prod", file=sys.stderr)
        #sys.exit(1)


@common_args.postprocessor(priority=200)
def setup_log(args: Namespace):
    if args.log:
        log_file = args.log.open('a')
        log.setup(log_file, quiet=args.quiet)
        
        def close():
            if log_file and not log_file.closed:
                log_file.close()
        atexit.register(close)


@commands.register('config', 'show',
                   help='Parse and show the config file')
def show(args: Namespace):
    logger = logging.getLogger(__name__)

    if args.config is None:
        sys.exit(ExitCode.VALIDATION_ERROR)
    else:
        print(Config.Schema().dumps(args.config))


@commands.register('config', 'write',
                   help='Write a skeleton config file')
def write(args: Namespace):
    logger = logging.getLogger(__name__)

    config = Config(ldap = dict(
                        hpccf = LDAPConfig(servers=['ldaps://ldap1.hpc.ucdavis.edu', 'ldaps://ldap2.hpc.ucdavis.edu'],
                                           searchbase='dc=hpc,dc=ucdavis,dc=edu',
                                           login_dn='uid=cheeto,ou=Services,dc=hpc,dc=ucdavis,dc=edu',
                                           password='password',
                                           user_classes=['inetOrgPerson', 'posixAccount']),

                    ))

    config_path = get_config_path()
    if not config_path.exists():
        logger.info(f'Config file not found, writing basic config to {config_path}')
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(Config.Schema().dumps(config))
    else:
        logger.warn(f'Config file already exists at {config_path}, exiting.')
        sys.exit(0)
