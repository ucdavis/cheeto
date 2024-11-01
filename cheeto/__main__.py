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


def main():
    parser.add_argument('--version', action='version', version=f'cheeto {__version__}')

    database_parser = commands.add_parser('database', aliases=['db'])
    database_parser.set_defaults(func = lambda _: database_parser.print_help())
    database_commands = database_parser.add_subparsers()

    site_parser = database_commands.add_parser('site', aliases=['s'])

    user_parser = database_commands.add_parser('user', aliases=['u'])
    user_parser.set_defaults(func = lambda _: user_parser.print_help())
    user_commands = user_parser.add_subparsers()
    database.cmd_user_set_status(user_commands)
    database.cmd_user_set_type(user_commands)
    database.cmd_user_set_password(user_commands)
    database.cmd_user_generate_passwords(user_commands)
    database.cmd_user_add_access(user_commands)
    database.cmd_user_remove_access(user_commands)
    database.cmd_user_groups(user_commands)
    database.cmd_user_show(user_commands)
    database.cmd_user_new_system(user_commands)

    group_parser = database_commands.add_parser('group', aliases=['g'])
    group_parser.set_defaults(func = lambda _: group_parser.print_help())
    group_commands = group_parser.add_subparsers()
    database.cmd_group_show(group_commands)
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
    database.cmd_group_new_lab(group_commands)

    storage_parser = database_commands.add_parser('storage', aliases=['st', 'store'])
    storage_parser.set_defaults(func = lambda _: storage_parser.print_help())
    storage_commands = storage_parser.add_subparsers()
    database.cmd_storage_to_puppet(storage_commands)
    database.cmd_storage_show(storage_commands)

    new_storage_parser = storage_commands.add_parser('new', aliases=['n'])
    new_storage_commands = new_storage_parser.add_subparsers()
    database.cmd_storage_new_storage(new_storage_commands)
    database.cmd_storage_new_collection(new_storage_commands)

    slurm_parser = database_commands.add_parser('slurm', aliases=['sl'])
    slurm_parser.set_defaults(func = lambda _: slurm_parser.print_help())
    slurm_commands = slurm_parser.add_subparsers()

    slurm_new_parser = slurm_commands.add_parser('new', aliases=['n'])
    slurm_new_commands = slurm_new_parser.add_subparsers()
    database.cmd_slurm_new_qos(slurm_new_commands)
    database.cmd_slurm_new_assoc(slurm_new_commands)

    
    console = log.Console(stderr=True)
    if not args.quiet:
        console.print(f'cheeto [green]v{__version__}[/green]')




if __name__ == '__main__':
    sys.exit(main())
