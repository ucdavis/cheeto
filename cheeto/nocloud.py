#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott & Omen Wild, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : __main__.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Author : Omen Wild <omen@ucdavis.edu>
# Date   : 29.03.2023

import os
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from .utils import __pkg_dir__


PKG_TEMPLATES = __pkg_dir__ / 'templates'


def add_render_args(parser):
    parser.add_argument('--templates-dir', '-t',
                        default='./templates',
                        type=Path)
    parser.add_argument('--authorized-keys', '-k',
                        default='/etc/ssh/users/root.pub',
                        type=Path)
    parser.add_argument('--output-dir', '-o',
                        default='nocloud-net',
                        type=Path)
    parser.add_argument('--cobbler-ip', '-c',
                        default='10.17.12.90')
    parser.add_argument('--puppet-environment', '-e',
                        default='production')
    parser.add_argument('--puppet-ip',
                        default='169.237.253.18')
    parser.add_argument('--puppet-fqdn',
                        default='puppet.hpc.ucdavis.edu')


def render(args):
    hosts_base = args.templates_dir / "hosts"
    host_paths = list(hosts_base.glob('*.j2'))

    environment = Environment(loader=FileSystemLoader(
                                        [str(args.templates_dir),
                                         str(args.templates_dir / 'layouts'),
                                         str(PKG_TEMPLATES),
                                         str(PKG_TEMPLATES / 'layouts')]
                                      )
                              )

    ssh_authorized_keys = args.authorized_keys.read_text().splitlines()

    for host_path in host_paths: 
        hostname = host_path.stem
        print(f"Processing: {hostname}")

        host_j2 = environment.get_template(f"hosts/{hostname}.j2")
        
        nocloud_host_dir = args.output_dir / hostname
        nocloud_host_dir.mkdir(mode=0o755, parents=False, exist_ok=True)

        meta_data_f = nocloud_host_dir / "meta-data"
        meta_data_f.touch(mode=0o644, exist_ok=True)

        vendor_data_f = nocloud_host_dir / "vendor-data"
        vendor_data_f.touch(mode=0o644, exist_ok=True)

        contents = host_j2.render(
            hostname=hostname,
            ssh_authorized_keys=ssh_authorized_keys,
            cobbler_ip=args.cobbler_ip,
            puppet_ip=args.puppet_ip,
            puppet_fqdn=args.puppet_fqdn,
            puppet_environment=args.puppet_environment
        )

        user_data_f = nocloud_host_dir / "user-data"
        user_data_f.write_text(contents)
