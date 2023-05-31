#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : hippo.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 17.02.2023

from dataclasses import asdict
import logging
import os
from pathlib import Path
import sys
from typing import Mapping, Union

import marshmallow
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
    logger = logging.getLogger(__name__)

    yaml_files = [args.global_file, args.site_file] if args.site_file.exists() \
                 else [args.global_file]
    merged_yaml = parse_yaml_forest(yaml_files,
                                    merge_on=MergeStrategy.ALL)
    # Generator only yields one item with MergeStrategy.ALL
    _, user_record = next(validate_yaml_forest(merged_yaml,
                                               PuppetUserMap,
                                               strict=True))
    if len(user_record.user) == 0:
        logger.error(f'No users!')
        sys.exit(ExitCode.BAD_MERGE)
    if len(user_record.user) > 1:
        logger.error(f'Merge resulted in multiple users.')
        sys.exit(ExitCode.BAD_MERGE)

    site_dumper = PuppetUserMap.site_dumper()
    global_dumper = PuppetUserMap.global_dumper()

    with args.global_file.open('w') as fp:
        print(global_dumper.dumps(user_record), file=fp)

    with args.site_file.open('w') as fp:
        print(site_dumper.dumps(user_record), file=fp)


def hippo_to_puppet(hippo_file: Path,
                    global_dir: Path,
                    site_dir: Path,
                    key_dir: Path,
                    current_state: PuppetAccountMap,
                    group_map: HippoSponsorGroupMapping):

    logger = logging.getLogger(__name__)

    hippo_yaml = parse_yaml(str(hippo_file))
    try:
        hippo_record = HippoRecord.Schema().load(hippo_yaml) #type: ignore
    except marshmallow.exceptions.ValidationError as e: #type: ignore
        logger.error(f'[red]ValidationError: {hippo_file}[/]')
        logger.error(e.messages)
        sys.exit(ExitCode.VALIDATION_ERROR)

    user_name = hippo_record.account.kerb
    site_dumper = PuppetUserMap.site_dumper()
    site_filename = site_dir / f'{user_name}.site.yaml'

    global_dumper = PuppetUserMap.global_dumper()
    global_filename = global_dir / f'{user_name}.yaml'

    if hippo_record.sponsor.kerb in group_map.mapping:
        sponsor = group_map.mapping[hippo_record.sponsor.kerb]
    else:
        sponsor = f'{hippo_record.sponsor.kerb}grp'

    if sponsor not in current_state.group: #type: ignore
        logger.error(f'{hippo_file}: {sponsor} is not a valid group.')
        sys.exit(ExitCode.INVALID_SPONSOR)

    user_record = PuppetUserRecord( #type: ignore
        fullname = hippo_record.account.name,
        email = hippo_record.account.email,
        uid = hippo_record.account.mothra,
        gid = hippo_record.account.mothra,
        groups = [sponsor] #type: ignore
    )

    user_map = PuppetUserMap( #type: ignore
        user = {user_name: user_record}
    )

    if os.path.exists(site_filename):
        current_site_yaml = parse_yaml(str(site_filename))
        merged_yaml = puppet_merge(asdict(user_map), current_site_yaml)
        user_map = PuppetUserMap.Schema().load( #type: ignore
            merged_yaml
        )

    if os.path.exists(global_filename):
        logger.info(f'{hippo_file}: Global YAML {global_filename} exists, skipping.')
    else:
        with global_filename.open('w') as fp:
            print(global_dumper.dumps(user_map), file=fp)

    with site_filename.open('w') as fp:
        print(site_dumper.dumps(user_map), file=fp)

    try:
        relative = get_relative_path(site_dir, global_dir)
        link_src = relative / global_filename.name
        site_dir.joinpath(global_filename.name).symlink_to(link_src)
    except FileExistsError:
        logger.info(f'{global_filename} already linked to {site_dir}.')

    if key_dir:
        key_dest = key_dir / f'{hippo_record.account.kerb}.pub'
        with key_dest.open('w') as fp:
            print(hippo_record.account.key, file=fp)


def add_convert_args(parser):
    parser.add_argument('-i', '--hippo-file',
                        type=Path,
                        nargs='+',
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


def convert(args):
    logger = logging.getLogger(__name__)

    current_state = PuppetAccountMap.Schema().load(parse_yaml(args.cluster_yaml)) #type: ignore
    
    if args.group_map:
        group_map = HippoSponsorGroupMapping.Schema().load(parse_yaml(args.group_map)) #type: ignore
    else:
        group_map = HippoSponsorGroupMapping.Schema().load({'mapping': {}}) #type: ignore

    for hippo_file in args.hippo_file:
        logger.info(f'Processing {hippo_file}...')
        hippo_to_puppet(hippo_file,
                        args.global_dir,
                        args.site_dir,
                        args.key_dir,
                        current_state,
                        group_map)

