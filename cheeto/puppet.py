#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : puppet.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 21.02.2023

from dataclasses import asdict
from enum import Enum
import os
from typing import Optional, List, Mapping, Union
import sys

import marshmallow
import marshmallow_dataclass
from marshmallow_dataclass import dataclass
from rich import print as rprint
from rich.console import Console
from rich.syntax import Syntax

from .types import *
from .utils import (require_kwargs,
                    parse_yaml,
                    puppet_merge,
                    EnumAction)
from . import _yaml

@require_kwargs
@dataclass(frozen=True)
class PuppetAutofs(BaseModel):
    nas: str
    path: str # TODO: path-like


@require_kwargs
@dataclass(frozen=True)
class PuppetZFS(BaseModel):
    quota: DataQuota


@require_kwargs
@dataclass(frozen=True)
class PuppetUserStorage(BaseModel):
    zfs: Union[PuppetZFS, bool]
    autofs: Optional[PuppetAutofs] = None


@require_kwargs
@dataclass(frozen=True)
class SlurmQOSTRES(BaseModel):
    cpus: Optional[UInt32] = None
    gpus: Optional[UInt32] = None
    mem: Optional[DataQuota] = None


@require_kwargs
@dataclass(frozen=True)
class SlurmQOS(BaseModel):
    group: SlurmQOSTRES = None
    job: Optional[SlurmQOSTRES] = None
    priority: Optional[int] = None

    def to_slurm(self):
        tokens = []
        for res, val in asdict(self.group).items():
            if val is not None:
                tokens.append(f'Grp{res.capitalize()}={val}')
        if self.job is not None:
            for res, val in asdict(self.job).items():
                if val is not None:
                    tokens.append(f'Max{res.capitalize()}={val}')
        return tokens


@require_kwargs
@dataclass(frozen=True)
class SlurmPartition(BaseModel):
    qos: Optional[SlurmQOS] = None


@require_kwargs
@dataclass(frozen=True)
class SlurmRecord(BaseModel):
    account: Optional[Union[KerberosID, List[KerberosID]]] = None
    partitions: Optional[Mapping[str, SlurmPartition]] = None
    max_jobs: Optional[UInt32] = None


@require_kwargs
@dataclass(frozen=True)
class SlurmRecordMap(BaseModel):
    pass


@require_kwargs
@dataclass(frozen=True)
class PuppetUserRecord(BaseModel):
    fullname: str
    email: Email
    uid: LinuxUID
    gid: LinuxGID
    groups: Optional[List[str]] = None
    password: Optional[LinuxPassword] = None
    shell: Optional[Shell] = None
    tag: Optional[List[str]] = None
    home: Optional[str] = None

    ensure: Optional[PuppetEnsure] = None
    membership: Optional[PuppetMembership] = None

    storage: Optional[PuppetUserStorage] = None


@require_kwargs
@dataclass(frozen=True)
class PuppetUserMap(BaseModel):
    user: Mapping[KerberosID, PuppetUserRecord]


@require_kwargs
@dataclass(frozen=True)
class PuppetGroupStorage(BaseModel):
    name: str
    owner: KerberosID
    group: Optional[KerberosID] = None
    autofs: Optional[PuppetAutofs] = None
    zfs: Optional[Union[PuppetZFS, bool]] = None


@require_kwargs
@dataclass(frozen=True)
class PuppetGroupRecord(BaseModel):
    gid: LinuxGID
    ensure: Optional[PuppetEnsure] = None
    tag: Optional[List[str]] = None

    storage: Optional[List[PuppetGroupStorage]] = None
    slurm: Optional[SlurmRecord] = None
    

@require_kwargs
@dataclass(frozen=True)
class PuppetGroupMap(BaseModel):
    group: Mapping[KerberosID, PuppetGroupRecord]


@require_kwargs
@dataclass(frozen=True)
class PuppetShareStorage(BaseModel):
    owner: KerberosID
    group: Optional[KerberosID]
    zfs: Union[PuppetZFS, bool]
    autofs: Optional[PuppetAutofs]


@require_kwargs
@dataclass(frozen=True)
class PuppetShareRecord(BaseModel):
    storage: PuppetShareStorage


@require_kwargs
@dataclass(frozen=True)
class PuppetShareMap(BaseModel):
    share: Mapping[str, PuppetShareRecord]


@require_kwargs
@dataclass(frozen=True)
class PuppetAccountMap(BaseModel):
    group: Optional[Mapping[KerberosID, PuppetGroupRecord]] = None
    user: Optional[Mapping[KerberosID, PuppetUserRecord]] = None
    share: Optional[Mapping[str, PuppetShareRecord]] = None


class MergeStrategy(Enum):
    ALL = 'all'
    PREFIX = 'prefix'
    NONE = 'none'


def parse_yaml_tree(yaml_files,
                   merge_on=MergeStrategy.NONE,
                   strict=True):

    yaml_tree = {}
    if merge_on is MergeStrategy.ALL:
        parsed_yamls = (parse_yaml(f) for f in yaml_files)
        yaml_tree = {'merged-all': puppet_merge(*parsed_yamls)}
    elif merge_on is MergeStrategy.NONE:
        yaml_tree = {f: parse_yaml(f) for f in yaml_files}
    elif merge_on is MergeStrategy.PREFIX:
        file_groups = {}
        for filename in yaml_files:
            prefix, _, _ = os.path.basename(filename).partition('.')
            if prefix in file_groups:
                file_groups[prefix].append(parse_yaml(filename))
            else:
                file_groups[prefix] = [parse_yaml(filename)]
        yaml_tree = {prefix: puppet_merge(*yamls) for prefix, yamls in file_groups.items()}

    return yaml_tree


def add_validate_args(parser):
    parser.add_argument('--merge',
                        default=MergeStrategy.NONE,
                        type=MergeStrategy,
                        action=EnumAction,
                        help='Merge the given YAML files before validation.')
    parser.add_argument('--partial',
                        default=False,
                        action='store_true',
                        help='Allow partial loading (ie missing keys).')
    parser.add_argument('--dump', default='/dev/stdout',
                        help='Dump the validated YAML to the given file')
    parser.add_argument('files', nargs='+',
                        help='YAML files to validate.')
    parser.add_argument('--strict', action='store_true', default=False,
                        help='Terminate on validation errors.')
    parser.add_argument('--quiet', default=False, action='store_true')


def validate_yamls(args):

    console = Console(stderr=True)

    yaml_tree = parse_yaml_tree(args.files,
                                merge_on=args.merge,
                                strict=args.strict)
    
    for source_file, yaml_obj in yaml_tree.items():
        if not args.quiet:
            console.rule(source_file, style='blue')

        try:
            puppet_data = PuppetAccountMap.Schema().load(yaml_obj,
                                                         partial=args.partial)
        except marshmallow.exceptions.ValidationError as e:
            rprint(f'[red]ValidationError:[/]', file=sys.stderr)
            rprint(e.messages, file=sys.stderr)
            if args.strict:
                sys.exit(1)
            continue

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

        if args.dump != '/dev/stdout' and not args.quiet:
            console.print()

