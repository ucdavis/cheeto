#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott & Omen Wild, 2023-2024
# (c) The Regents of the University of California, Davis, 2023-2024
# File   : __main__.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Author : Omen Wild <omen@ucdavis.edu>
# Date   : 29.03.2023

from argparse import Namespace
from pathlib import Path

from jinja2 import Environment, FileSystemLoader
from ponderosa import ArgParser

from . import commands
from ..yaml import parse_yaml, puppet_merge


@commands.register('nocloud',
                   help='Operations on nocloud cloud-init files')
def _(*args):
    pass


@commands.register('nocloud', 'render',
                   help='Render nocloud templates for cobbler')
def render(args: Namespace):
    site_dir = args.site_dir
    common_dir = site_dir.parent

    hosts_base = args.site_dir / "hosts"
    host_paths = list(hosts_base.glob('*.j2'))

    environment = Environment(loader=FileSystemLoader(
                                        [str(site_dir),
                                         str(common_dir),
                                         str(common_dir/ 'partitions')]
                                      ),
                              trim_blocks=True,
                              lstrip_blocks=True
                              )

    config = puppet_merge(*[parse_yaml(f) for f in \
                            [common_dir / args.config_basename, site_dir / args.config_basename]])

    ak = args.authorized_keys
    ssh_authorized_keys = [line for line in ak.read_text().splitlines() \
                           if not line.strip().startswith('#')]

    for host_path in host_paths: 
        hostname = host_path.stem
        print(f"Rendering: {hostname}")

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
            **config
        )

        user_data_f = nocloud_host_dir / "user-data"
        user_data_f.write_text(contents)


@render.args()
def add_render_args(parser: ArgParser):
    parser.add_argument('--site-dir',
                        type=Path)
    parser.add_argument('--authorized-keys', '-k',
                        default='/etc/ssh/users/root.pub',
                        type=Path)
    parser.add_argument('--output-dir', '-o',
                        default='nocloud-net',
                        type=Path)
    parser.add_argument('--config-basename',
                        default='config.yaml')
