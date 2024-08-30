#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : __main__.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 17.02.2023

import argparse
import sys

from cheeto import hippoapi

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
    database_parser.set_defaults(func = lambda _: database_parser.print_help())
    database_commands = database_parser.add_subparsers()
    database.load(database_commands)

    hippoapi_parser = database_commands.add_parser('hippoapi')
    hippoapi_parser.set_defaults(func = lambda _: hippoapi_parser.print_help())
    hippoapi_commands = hippoapi_parser.add_subparsers()
    database.hippoapi_process(hippoapi_commands)
    database.hippoapi_events(hippoapi_commands)


    user_parser = database_commands.add_parser('user')
    user_parser.set_defaults(func = lambda _: user_parser.print_help())
    user_commands = user_parser.add_subparsers()
    database.query_users(user_commands)
    database.user_set_status(user_commands)
    database.user_set_type(user_commands)
    database.user_groups(user_commands)
    database.user_show(user_commands)

    group_parser = database_commands.add_parser('group')
    group_parser.set_defaults(func = lambda _: group_parser.print_help())
    group_commands = group_parser.add_subparsers()
    database.show_group(group_commands)
    database.query_groups(group_commands)
    database.group_add_user(group_commands)
    database.group_add_sponsor(group_commands)
    database.group_new_system(group_commands)

    args = parser.parse_args()
    if not hasattr(args, 'log'):
        args.func(args)
    else:
        with args.log.open('a') as log_fp:
            log.setup(log_fp, quiet=args.quiet)
            return args.func(args) or 0


if __name__ == '__main__':
    sys.exit(main())
