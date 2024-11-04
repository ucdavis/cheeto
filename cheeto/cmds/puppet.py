#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2024
# (c) The Regents of the University of California, Davis, 2023
# File   : cmds/puppet.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 04.11.2024

from argparse import Namespace
from pathlib import Path

from ponderosa import ArgParser, arggroup
from rich import print as rprint
from rich.console import Console
from rich.syntax import Syntax

from . import commands
from ..args import EnumAction
from ..puppet import (_postload_validators,
                      PuppetAccountMap,
                      validate_yaml_forest)
from ..yaml import MergeStrategy, parse_yaml_forest


@arggroup('YAML Validation')
def validate_args(parser: ArgParser):
    parser.add_argument('--dump', default='/dev/stdout',
                        help='Dump the validated YAML to the given file')
    parser.add_argument('--echo', action='store_true', default=False)
    parser.add_argument('files', nargs='+',
                        help='YAML files to validate.')
    parser.add_argument('--merge',
                        default=MergeStrategy.NONE,
                        type=MergeStrategy,
                        action=EnumAction,
                        help='Merge the given YAML files before validation.')
    parser.add_argument('--strict', action='store_true', default=False,
                        help='Terminate on validation errors.')
    parser.add_argument('--partial',
                        default=False,
                        action='store_true',
                        help='Allow partial loading (ie missing keys).')
    parser.add_argument('--postload-validate', default=False,
                       action='store_true')
    for func_name in _postload_validators.keys():
        parser.add_argument(f'--{func_name}', action='store_true', default=False)



@validate_args.apply()
@commands.register('puppet', 'validate',
                   help='Validate a puppet.hpc-formatted YAML file')
def validate_yamls(args: Namespace):

    console = Console(stderr=True)

    yaml_forest = parse_yaml_forest(args.files,
                                    merge_on=args.merge)
    
    for source_file, puppet_data in validate_yaml_forest(yaml_forest,
                                                         PuppetAccountMap,
                                                         args.strict):
        if not args.quiet: 
            console.rule(source_file, style='blue')

        if args.postload_validate:
            for postload_func in _postload_validators.values():
                postload_func(source_file, puppet_data, args.strict)
        else:
            for func_name in _postload_validators.keys():
                if getattr(args, func_name):
                    _postload_validators[func_name](source_file, puppet_data, args.strict)

        output_yaml = PuppetAccountMap.Schema().dumps(puppet_data) 
        hl_yaml = Syntax(output_yaml,
                         'yaml',
                         theme='github-dark',
                         background_color='default')

        if args.dump:
            with open(args.dump, 'w') as fp:
                if args.dump == '/dev/stdout':
                    rprint(hl_yaml, file=fp)
                else:
                    print(output_yaml, file=fp)

        if args.dump != '/dev/stdout' and args.echo and not args.quiet:
            console.print(hl_yaml)


@arggroup('YAML Repo Args')
def repo_args(parser: ArgParser):
    parser.add_argument('--site-dir', 
                        type=Path,
                        required=True,
                        help='Site-specific puppet accounts directory.')
    parser.add_argument('--global-dir',
                        type=Path,
                        required=True,
                        help='Global puppet accounts directory.')
    parser.add_argument('--key-dir',
                        type=Path,
                        required=True,
                        help='Puppet SSH keys directory.')
    parser.add_argument('--base-branch',
                        default='main',
                        help='Branch to make PR against.')
    parser.add_argument('--timeout',
                        type=int,
                        default=30)
