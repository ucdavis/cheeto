#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : __main__.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 17.02.2023

import argparse

from . import __version__
from . import config
from . import database
from . import hippo
from . import monitor
from . import log
from . import nocloud
from . import puppet
from . import slurm

from .args import add_common_args
from .config import get_config


def main():
    parser = argparse.ArgumentParser()
    parser.set_defaults(func = lambda _: parser.print_help())
    parser.add_argument('--version', action='version', version=f'cheeto {__version__}')
    commands = parser.add_subparsers()

    config_parser = commands.add_parser('config')
    config_commands = config_parser.add_subparsers()
    config.show(config_commands)
    config.write(config_commands)

    hippo_parser = commands.add_parser('hippo')
    hippo_commands = hippo_parser.add_subparsers()
    hippo.convert(hippo_commands)
    hippo.sync(hippo_commands)
    hippo.sanitize(hippo_commands)
    hippo.validate(hippo_commands)

    puppet_parser = commands.add_parser('puppet')
    puppet_commands = puppet_parser.add_subparsers()
    puppet.validate_yamls(puppet_commands)
    puppet.create_nologin_user(puppet_commands)
    puppet.sync_ldap(puppet_commands)

    nocloud_parser = commands.add_parser('nocloud')
    nocloud_commands = nocloud_parser.add_subparsers()
    nocloud.render(nocloud_commands)

    slurm_parser = commands.add_parser('slurm')
    slurm_commands = slurm_parser.add_subparsers()
    slurm.sync(slurm_commands)
    slurm.show_qos(slurm_commands)

    monitor_parser = commands.add_parser('monitor')
    monitor_commands = monitor_parser.add_subparsers()
    monitor.power(monitor_commands)

    database_parser = commands.add_parser('database')
    database_commands = database_parser.add_subparsers()
    database.load(database_commands)

    query_parser = database_commands.add_parser('query')
    query_commands = query_parser.add_subparsers()
    database.query_users(query_commands)

    args = parser.parse_args()
    with args.log.open('a') as log_fp:
        log.setup(log_fp, quiet=args.quiet)
        args.func(args)


if __name__ == '__main__':
    main()
