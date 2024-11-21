#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2024
# (c) The Regents of the University of California, Davis, 2023
# File   : cmds/slurm.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 04.11.2024

from argparse import FileType, Namespace
from pathlib import Path

import json
from ponderosa import ArgParser
from rich.console import Console
from rich.progress import track
import sh

from . import commands
from .database import (connect_to_database,
                       slurm_association_state as build_db_association_state,
                       slurm_qos_state as build_db_qos_state,
                       site_args)
from ..errors import ExitCode
from ..puppet import PuppetAccountMap
from ..slurm import (build_puppet_association_state,
                     build_puppet_qos_state,
                     build_slurm_association_state,
                     build_slurm_qos_state,
                     generate_commands,
                     SAcctMgr)

from ..types import validate_yaml_forest
from ..yaml import (MergeStrategy,
                    parse_yaml_forest)


@site_args.apply()
@commands.register('slurm', 'sync',
                   help='Sync Slurm associations from database or YAMLs to controller')
def sync(args: Namespace):
    console = Console(stderr=True)

    if args.source == 'yaml':
        if args.yaml_files is None:
            console.print('Must provide YAML files (-i) when source is yaml!')
            return ExitCode.BAD_CMDLINE_ARGS
        console.rule('Load association data.')
        console.print('Loading Puppet YAML data...')
        yaml_forest = parse_yaml_forest(args.yaml_files,
                                        merge_on=MergeStrategy.ALL)
        # Generator only yields one item with MergeStrategy.ALL
        _, puppet_data = next(validate_yaml_forest(yaml_forest,
                                                   PuppetAccountMap,
                                                   strict=True))

        console.print('Building Puppet associations table...')
        cheeto_associations = build_puppet_association_state(puppet_data)
        console.print('Building Puppet QoSes...')
        cheeto_qos_map = build_puppet_qos_state(puppet_data)
    else:
        if args.site is None:
            console.print('Must provide --site when source is db!')
            return ExitCode.BAD_CMDLINE_ARGS
        connect_to_database(args.config.mongo)
        console.print(f'Sync slurm for site {args.site}')
        console.rule('Load association data')
        console.print('Load from mongodb...')
        cheeto_associations = build_db_association_state(args.site)
        cheeto_qos_map = build_db_qos_state(args.site)

    sacctmgr = SAcctMgr(sudo=args.sudo)
    console.print('Getting current associations...')
    if args.slurm_associations:
        slurm_associations = build_slurm_association_state(args.slurm_associations)
    else:
        slurm_associations = sacctmgr.get_slurm_association_state()
    console.print('Getting current QoSes...')
    if args.slurm_qoses:
        slurm_qos_map, _ = build_slurm_qos_state(args.slurm_qoses)
    else:
        slurm_qos_map, _ = sacctmgr.get_slurm_qos_state()

    console.rule('Reconcile Puppet and Slurm.')
    console.print('Generating reconciliation commands...')
    command_queue = generate_commands(slurm_associations,
                                      slurm_qos_map,
                                      cheeto_associations,
                                      cheeto_qos_map,
                                      sacctmgr=sacctmgr)

    if args.dump_commands and args.dump_commands.exists():
        args.dump_commands.unlink()
    report = {}
    for command_group_name, slurm_op, command_group in command_queue:
        group_report = {'successes': 0, 'failures': 0, 'commands': len(command_group)}
        report[slurm_op.name] = group_report

        if not command_group:
            continue

        console.rule(f'Commands: {command_group_name}', style='blue')
        if args.apply:
            for command in track(command_group, console=console):
                try:
                    #console.out(f'Run: {command}', highlight=False)
                    command()
                except sh.ErrorReturnCode_1 as e: #type: ignore
                    console.print(f'\nCommand Error: {e}', highlight=False)
                    group_report['failures'] += 1
                else:
                    group_report['successes'] += 1
        else:
            for command in command_group:
                if args.dump_commands:
                    with args.dump_commands.open('a') as f:
                        print(str(command), file=f)
                else:
                    console.out(str(command), highlight=False)


    print(json.dumps(report, indent=2)) 


@sync.args('Sync')
def sync_args(parser: ArgParser):
    parser.add_argument('--sudo', action='store_true', default=False,
                        help='Run sacctmgr commands with sudo.')
    parser.add_argument('--apply', action='store_true', default=False,
                        help='Execute and apply the Slurm changes.')
    parser.add_argument('--slurm-associations', type=FileType('r'),
                        help='Read slurm associations from the specified file '
                             'instead of parsing from a `sacctmgr show -P assoc` call.')
    parser.add_argument('--slurm-qoses', type=FileType('r'),
                        help='Read slurm QoSes from the specified file '
                             'instead of parsing from a `sacctmgr show -P qos` call.')
    parser.add_argument('--source', choices=['db', 'yaml'], default='yaml')
    parser.add_argument('-i', dest='yaml_files', nargs='+',
                        help='YAML inputs for when source is yaml')
    parser.add_argument('--dump-commands', type=Path, default=None)

