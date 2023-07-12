#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : hippo.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 17.02.2023

from dataclasses import asdict
import os
from pathlib import Path
import sys
from typing import Mapping, Optional

import marshmallow_dataclass
from marshmallow_dataclass import dataclass

from .errors import ExitCode
from .puppet import (PuppetAccountMap,
                     PuppetUserRecord,
                     PuppetUserMap,
                     parse_yaml_forest,
                     validate_yaml_forest,
                     MergeStrategy)
from .utils import require_kwargs, get_relative_path
from .types import *
from .utils import parse_yaml, puppet_merge


@require_kwargs
@dataclass(frozen=True)
class HippoSponsor(BaseModel):
    accountname: str
    name: str
    email: Email
    kerb: KerberosID
    iam: IAMID
    mothra: MothraID
    cluster: Optional[str]


@require_kwargs
@dataclass(frozen=True)
class HippoAccount(BaseModel):
    name: str
    email: Email
    kerb: KerberosID
    iam: IAMID
    mothra: MothraID
    key: str


@require_kwargs
@dataclass(frozen=True)
class HippoRecord(BaseModel):
    sponsor: HippoSponsor
    account: HippoAccount


@require_kwargs
@dataclass(frozen=True)
class HippoSponsorGroupMapping(BaseModel):
    mapping: Mapping[KerberosID, KerberosID]


def add_convert_args(parser):
    parser.add_argument('-i', '--hippo-file',
                        type=Path,
                        required=True)
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
    parser.add_argument('--cluster-yaml',
                        type=Path,
                        required=True,
                        help='Path to merged cluster YAML.')
    parser.add_argument('--group-map',
                        type=Path,
                        help='Sponsor KerberosID to group name mapping.')


def add_sanitize_args(parser):
    parser.add_argument('--site-file',
                        type=Path,
                        required=True,
                        help='Site-specific puppet YAML.')
    parser.add_argument('--global-file',
                        type=Path,
                        required=True,
                        help='Global puppet YAML.')


def sanitize(args):
    yaml_files = [args.global_file, args.site_file] if args.site_file.exists() \
                 else [args.global_file]
    merged_yaml = parse_yaml_forest(yaml_files,
                                    merge_on=MergeStrategy.ALL)
    # Generator only yields one item with MergeStrategy.ALL
    _, user_record = next(validate_yaml_forest(merged_yaml,
                                               PuppetUserMap,
                                               strict=True))
    if len(user_record.user) == 0:
        print(f'ERROR: No users!', file=sys.stderr)
        sys.exit(ExitCode.BAD_MERGE)
    if len(user_record.user) > 1:
        print(f'ERROR: merge resulted in multiple users.', file=sys.stderr)
        sys.exit(ExitCode.BAD_MERGE)

    site_dumper = PuppetUserMap.site_dumper()
    global_dumper = PuppetUserMap.global_dumper()

    with args.global_file.open('w') as fp:
        print(global_dumper.dumps(user_record), file=fp)

    with args.site_file.open('w') as fp:
        print(site_dumper.dumps(user_record), file=fp)


def convert_to_puppet(args):
    current_state = PuppetAccountMap.Schema().load(parse_yaml(args.cluster_yaml))
    
    if args.group_map:
        group_map = HippoSponsorGroupMapping.Schema().load(parse_yaml(args.group_map))
    else:
        group_map = HippoSponsorGroupMapping.Schema().load({'mapping': {}})

    hippo_yaml = parse_yaml(args.hippo_file)
    hippo_record = HippoRecord.Schema().load(hippo_yaml)

    user_name = hippo_record.account.kerb
    site_dumper = PuppetUserMap.site_dumper()
    site_filename = args.site_dir / f'{user_name}.site.yaml'

    global_dumper = PuppetUserMap.global_dumper()
    global_filename = args.global_dir / f'{user_name}.yaml'

    if hippo_record.sponsor.kerb in group_map.mapping:
        sponsor = group_map.mapping[hippo_record.sponsor.kerb]
    else:
        sponsor = f'{hippo_record.sponsor.kerb}grp'

    if sponsor not in current_state.group:
        print(f'ERROR: {args.hippo_file}: {sponsor} is not a valid group.', file=sys.stderr)
        sys.exit(ExitCode.INVALID_SPONSOR)

    user_record = PuppetUserRecord(
        fullname = hippo_record.account.name,
        email = hippo_record.account.email,
        uid = hippo_record.account.mothra,
        gid = hippo_record.account.mothra,
        groups = [sponsor]
    )

    user_map = PuppetUserMap(
        user = {user_name: user_record}
    )

    if os.path.exists(site_filename):
        current_site_yaml = parse_yaml(site_filename)
        merged_yaml = puppet_merge(asdict(user_map), current_site_yaml)
        user_map = PuppetUserMap.Schema().load(
            merged_yaml
        )

    if os.path.exists(global_filename):
        print(f'NOTE: {args.hippo_file}: Global YAML {global_filename} exists, skipping.',
              file=sys.stderr)
    else:
        with global_filename.open('w') as fp:
            print(global_dumper.dumps(user_map), file=fp)

    with site_filename.open('w') as fp:
        print(site_dumper.dumps(user_map), file=fp)

    try:
        relative = get_relative_path(args.site_dir, args.global_dir)
        link_src = relative / global_filename.name
        args.site_dir.joinpath(global_filename.name).symlink_to(link_src)
    except FileExistsError as e:
        print(f'NOTE: {global_filename} already linked to {args.site_dir}', file=sys.stderr)

    if args.key_dir:
        key_dest = args.key_dir / f'{hippo_record.account.kerb}.pub'
        with key_dest.open('w') as fp:
            print(hippo_record.account.key, file=fp)

