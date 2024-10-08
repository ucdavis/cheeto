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

from . import __version__
from . import config
from . import database
from . import hippo
from . import monitor
from . import log
from . import nocloud
from . import puppet
from . import slurm

from .config import get_config


def main():
    parser = argparse.ArgumentParser()
    parser.set_defaults(func = lambda _: parser.print_help())
    parser.add_argument('--version', action='version', version=f'cheeto {__version__}')
    parser.add_argument('--config', type=get_config, default=get_config())
    commands = parser.add_subparsers()

    config_parser = commands.add_parser('config')
    config_commands = config_parser.add_subparsers()
    config.show(config_commands)
    config.write(config_commands)

    hippo_parser = commands.add_parser('hippo')
    hippo_parser.set_defaults(func = lambda _: hippo_parser.print_help())
    hippo_commands = hippo_parser.add_subparsers()
    hippo.cmd_hippoapi_process(hippo_commands)
    hippo.cmd_hippoapi_events(hippo_commands)

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

    hippoapi_parser = database_commands.add_parser('hippoapi')




    site_parser = database_commands.add_parser('site')
    site_parser.set_defaults(func = lambda _: site_parser.print_help())
    site_commands = site_parser.add_subparsers()
    database.cmd_site_add_global_slurm(site_commands)
    database.cmd_site_write_to_puppet(site_commands)
    database.cmd_site_sync_to_ldap(site_commands)
    database.cmd_site_write_sympa(site_commands)
    database.cmd_site_write_root_key(site_commands)
    database.cmd_site_list(site_commands)
    database.cmd_site_sync_new_puppet(site_commands)
    database.cmd_site_sync_old_puppet(site_commands)
    database.cmd_site_from_puppet(site_commands)

    user_parser = database_commands.add_parser('user')
    user_parser.set_defaults(func = lambda _: user_parser.print_help())
    user_commands = user_parser.add_subparsers()
    database.cmd_user_query(user_commands)
    database.cmd_user_set_status(user_commands)
    database.cmd_user_set_type(user_commands)
    database.cmd_user_add_access(user_commands)
    database.cmd_user_remove_access(user_commands)
    database.cmd_user_groups(user_commands)
    database.cmd_user_show(user_commands)

    group_parser = database_commands.add_parser('group')
    group_parser.set_defaults(func = lambda _: group_parser.print_help())
    group_commands = group_parser.add_subparsers()
    database.cmd_group_show(group_commands)
    database.cmd_group_query(group_commands)
    database.cmd_group_add_member(group_commands)
    database.cmd_group_remove_member(group_commands)
    database.cmd_group_add_sponsor(group_commands)
    database.cmd_group_remove_sponsor(group_commands)
    database.cmd_group_add_sudoer(group_commands)
    database.cmd_group_remove_sudoer(group_commands)
    database.cmd_group_add_slurmer(group_commands)
    database.cmd_group_remove_slurmer(group_commands)
    database.cmd_group_new_system(group_commands)
    database.cmd_group_new_class(group_commands)

    storage_parser = database_commands.add_parser('storage')
    storage_parser.set_defaults(func = lambda _: storage_parser.print_help())
    storage_commands = storage_parser.add_subparsers()
    database.cmd_storage_add(storage_commands)
    database.cmd_storage_to_puppet(storage_commands)
    database.cmd_storage_show(storage_commands)

    args = parser.parse_args()
    
    console = log.Console(stderr=True)
    console.print(f'cheeto [green]v{__version__}[/green]')

    if not hasattr(args, 'log'):
        args.func(args)
    else:
        with args.log.open('a') as log_fp:
            log.setup(log_fp, quiet=args.quiet)
            return args.func(args) or 0


if __name__ == '__main__':
    sys.exit(main())
