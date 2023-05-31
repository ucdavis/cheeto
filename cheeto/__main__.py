#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : __main__.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 17.02.2023

import argparse
import os
from pathlib import Path

from . import hippo
from . import log
from . import nocloud
from . import puppet
from . import slurm


def add_common_args(parser):
    parser.add_argument('--log', type=Path, default=Path(os.devnull),
                        help='Log to file.')
    parser.add_argument('--quiet', default=False, action='store_true')


def main():
    parser = argparse.ArgumentParser()
    parser.set_defaults(func = lambda _: parser.print_help())
    commands = parser.add_subparsers()


    hippo_parser = commands.add_parser('hippo')
    hippo_commands = hippo_parser.add_subparsers()
    hippo_convert_parser = hippo_commands.add_parser('convert')
    add_common_args(hippo_convert_parser)
    hippo_convert_parser.set_defaults(func=hippo.convert_to_puppet)
    hippo.add_convert_args(hippo_convert_parser)

    hippo_sanitize_parser = hippo_commands.add_parser('sanitize')
    add_common_args(hippo_sanitize_parser)
    hippo.add_sanitize_args(hippo_sanitize_parser)
    hippo_sanitize_parser.set_defaults(func=hippo.sanitize)


    validate_puppet_parser = commands.add_parser('validate-puppet')
    validate_puppet_parser.set_defaults(func=puppet.validate_yamls)
    add_common_args(validate_puppet_parser)
    puppet.add_validate_args(validate_puppet_parser)

    nocloud_parser = commands.add_parser('nocloud-render')
    nocloud_parser.set_defaults(func=nocloud.render)
    add_common_args(nocloud_parser)
    nocloud.add_render_args(nocloud_parser)


    slurm_parser = commands.add_parser('slurm')
    slurm_commands = slurm_parser.add_subparsers()
    slurm_show_qos_parser = slurm_commands.add_parser('show-qos')
    add_common_args(slurm_show_qos_parser)
    slurm_show_qos_parser.set_defaults(func=lambda args: print('QOS'))

    slurm_sync_parser = slurm_commands.add_parser('sync')
    add_common_args(slurm_sync_parser)
    slurm.add_sync_args(slurm_sync_parser)
    slurm_sync_parser.set_defaults(func = slurm.sync)
    

    args = parser.parse_args()
    with args.log.open('a') as log_fp:
        log.setup(log_fp, quiet=args.quiet)
        args.func(args)


if __name__ == '__main__':
    main()
