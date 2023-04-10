#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : puppet.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 21.02.2023

from dataclasses import dataclass
import os
from typing import Optional, List, Mapping, Union
import sys

import marshmallow
import marshmallow_dataclass
from rich import print as rprint
from rich.console import Console
from rich.syntax import Syntax

from .types import *
from .utils import require_kwargs
from .yaml import parse_yaml, puppet_merge


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
    autofs: Optional[PuppetAutofs]


@require_kwargs
@dataclass(frozen=True)
class PuppetSlurmQOSTRES(BaseModel):
    cpus: Optional[UInt32] = None
    gpus: Optional[UInt32] = None
    mem: Optional[DataQuota] = None


@require_kwargs
@dataclass(frozen=True)
class PuppetSlurmQOS(BaseModel):
    group: PuppetSlurmQOSTRES
    job: Optional[PuppetSlurmQOSTRES] = None


@require_kwargs
@dataclass(frozen=True)
class PuppetSlurmPartition(BaseModel):
    name: str
    qos: Optional[PuppetSlurmQOS] = None


@require_kwargs
@dataclass(frozen=True)
class PuppetSlurmRecord(BaseModel):
    partitions: List[PuppetSlurmPartition]
    account: Optional[KerberosID] = None


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

    class Meta:
        ordered = True


@require_kwargs
@dataclass(frozen=True)
class PuppetUserMap(BaseModel):
    user: Mapping[KerberosID, PuppetUserRecord]


@require_kwargs
@dataclass(frozen=True)
class PuppetGroupStorage(BaseModel):
    name: str
    owner: KerberosID
    autofs: Optional[PuppetAutofs]
    zfs: Optional[Union[PuppetZFS, bool]] = None


@require_kwargs
@dataclass(frozen=True)
class PuppetGroupRecord(BaseModel):
    gid: LinuxGID
    ensure: Optional[PuppetEnsure] = None
    tag: Optional[List[str]] = None

    storage: Optional[List[PuppetGroupStorage]] = None
    slurm: Optional[PuppetSlurmRecord] = None


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


PuppetUserRecordSchema  = marshmallow_dataclass.class_schema(PuppetUserRecord)
PuppetUserMapSchema     = marshmallow_dataclass.class_schema(PuppetUserMap)
PuppetGroupRecordSchema = marshmallow_dataclass.class_schema(PuppetGroupRecord)
PuppetGroupMapSchema    = marshmallow_dataclass.class_schema(PuppetUserMap)
PuppetAccountMapSchema  = marshmallow_dataclass.class_schema(PuppetAccountMap)


def add_validate_args(parser):
    parser.add_argument('--merge', action='store_true', default=False,
                        help='Merge the given YAML files before validation.')
    parser.add_argument('--dump', default='/dev/stdout',
                        help='Dump the validated YAML to the given file')
    parser.add_argument('files', nargs='+',
                        help='YAML files to validate.')
    parser.add_argument('--strict', action='store_true', default=False,
                        help='Terminate on validation errors.')
    parser.add_argument('--quiet', default=False, action='store_true')


def validate_yamls(args):

    console = Console(stderr=True)

    parsed_yamls = []
    for filename in args.files:
        parsed_yamls.append(parse_yaml(filename))

    if args.merge:
        parsed_yamls = [('merged', puppet_merge(*parsed_yamls))]
    else:
        parsed_yamls = zip(args.files, parsed_yamls)
    
    for source_file, yaml_obj in parsed_yamls:
        if not args.quiet:
            console.rule(source_file, style='blue')

        try:
            puppet_data = PuppetAccountMapSchema().load(yaml_obj)
        except marshmallow.exceptions.ValidationError as e:
            rprint(f'[red]ValidationError:[/] {e}', file=sys.stderr)
            if args.strict:
                sys.exit(1)
            continue

        output_yaml = puppet_data.to_yaml(omit_none=True)
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

