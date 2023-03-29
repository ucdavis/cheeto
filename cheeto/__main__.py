#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : __main__.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 17.02.2023

import argparse

from . import hippo
from . import nocloud
from . import puppet


def add_common_args(parser):
    pass


def main():
    parser = argparse.ArgumentParser()
    parser.set_defaults(func = lambda _: parser.print_help())
    commands = parser.add_subparsers()

    hippo_convert_parser = commands.add_parser('hippo-convert',
                                               description='Convert HIPPO yaml to puppet.hpc format.')
    add_common_args(hippo_convert_parser)
    hippo_convert_parser.set_defaults(func=hippo.convert_to_puppet)
    hippo.add_convert_args(hippo_convert_parser)

    validate_puppet_parser = commands.add_parser('validate-puppet')
    validate_puppet_parser.set_defaults(func=puppet.validate_yamls)
    add_common_args(validate_puppet_parser)
    puppet.add_validate_args(validate_puppet_parser)

    nocloud_parser = commands.add_parser('nocloud-render')
    nocloud_parser.set_defaults(func=nocloud.render)
    add_common_args(nocloud_parser)
    nocloud.add_render_args(nocloud_parser)

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
