#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : puppet.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 21.02.2023

import argparse
from dataclasses import asdict
from enum import Enum
import logging
import os
from typing import Optional, List, Mapping, Union, Set
import sys

import marshmallow
from marshmallow import post_dump
from marshmallow_dataclass import dataclass
from rich import print as rprint
from rich.console import Console
from rich.syntax import Syntax

from .errors import ExitCode
from .types import *
from .utils import (require_kwargs,
                    parse_yaml,
                    puppet_merge,
                    EnumAction,
                    size_to_megs)


MIN_PIGROUP_GID = 100_000_000
MIN_SYSTEM_UID = 4_000_000_000


@require_kwargs
@dataclass(frozen=True)
class PuppetAutofs(BaseModel):
    nas: str
    path: str # TODO: path-like
    options: Optional[str] = None


@require_kwargs
@dataclass(frozen=True)
class PuppetZFS(BaseModel):
    quota: DataQuota #type: ignore


@require_kwargs
@dataclass(frozen=True)
class PuppetUserStorage(BaseModel):
    zfs: Union[PuppetZFS, bool]
    autofs: Optional[PuppetAutofs] = None


@require_kwargs
@dataclass(frozen=True)
class SlurmQOSTRES(BaseModel):
    cpus: Optional[UInt32] = None #type: ignore
    gpus: Optional[UInt32] = None #type: ignore
    mem: Optional[DataQuota] = None #type: ignore

    @marshmallow.post_load
    def convert_mem(self, in_data, **kwargs):
        if in_data['mem'] is not None:
            in_data['mem'] = f'{size_to_megs(in_data["mem"])}M'
        return in_data

    def to_slurm(self) -> str:
        tokens = [f'cpu={self.cpus if self.cpus is not None else -1}',
                  f'mem={size_to_megs(self.mem) if self.mem is not None else -1}',
                  f'gres/gpu={self.gpus if self.gpus is not None else -1}']
        return ','.join(tokens)

    @staticmethod
    def negate() -> str:
        return 'cpu=-1,mem=-1,gres/gpu=-1'


@require_kwargs
@dataclass(frozen=True)
class SlurmQOS(BaseModel):
    group: Optional[SlurmQOSTRES] = None #type: ignore
    user: Optional[SlurmQOSTRES] = None #type: ignore
    job: Optional[SlurmQOSTRES] = None #type: ignore
    priority: Optional[int] = 0
    flags: Optional[Set[SlurmQOSFlag]] = None #type: ignore

    def to_slurm(self) -> List[str]:
        tokens = []
        grptres = self.group.to_slurm() if self.group is not None else SlurmQOSTRES.negate()
        usertres = self.user.to_slurm() if self.user is not None else SlurmQOSTRES.negate()
        jobtres = self.job.to_slurm() if self.job is not None else SlurmQOSTRES.negate()
        flags = ','.join(self.flags) if self.flags is not None else '-1'
        
        tokens.append(f'GrpTres={grptres}')
        tokens.append(f'MaxTRESPerUser={usertres}')
        tokens.append(f'MaxTresPerJob={jobtres}')
        tokens.append(f'Flags={flags}')
        tokens.append(f'Priority={self.priority}')

        return tokens


@require_kwargs
@dataclass(frozen=True)
class SlurmPartition(BaseModel):
    qos: Union[SlurmQOS, str]


@require_kwargs
@dataclass(frozen=True)
class SlurmRecord(BaseModel):
    account: Optional[Union[KerberosID, List[KerberosID]]] = None #type: ignore
    partitions: Optional[Mapping[str, SlurmPartition]] = None
    max_jobs: Optional[UInt32] = None #type: ignore


@require_kwargs
@dataclass(frozen=True)
class SlurmRecordMap(BaseModel):
    pass


@require_kwargs
@dataclass(frozen=True)
class PuppetUserRecord(BaseModel):
    fullname: str
    email: Email #type: ignore
    uid: LinuxUID #type: ignore
    gid: LinuxGID #type: ignore
    groups: Optional[Set[str]] = None
    group_sudo: Optional[List[KerberosID]] = None #type: ignore
    password: Optional[LinuxPassword] = None #type: ignore
    shell: Optional[Shell] = None #type: ignore
    tag: Optional[Set[str]] = None
    home: Optional[str] = None

    ensure: Optional[PuppetEnsure] = None #type: ignore
    membership: Optional[PuppetMembership] = None #type: ignore

    storage: Optional[PuppetUserStorage] = None
    slurm: Optional[SlurmRecord] = None

    @post_dump
    def sort(self, item, **kwargs):
        if 'groups' in item:
            item['groups'] = sorted(item['groups'])
        if 'tag' in item:
            item['tag'] = sorted(item['tag'])
        return item


@require_kwargs
@dataclass(frozen=True)
class PuppetUserMap(BaseModel):
    user: Mapping[KerberosID, PuppetUserRecord] #type: ignore

    @staticmethod
    def global_dumper():
        return PuppetUserMap.Schema(only=['user.fullname', #type: ignore
                                          'user.email',
                                          'user.uid',
                                          'user.gid',
                                          'user.password',
                                          'user.shell'])

    @staticmethod
    def site_dumper():
        return PuppetUserMap.Schema(only=['user.groups', #type: ignore
                                          'user.group_sudo',
                                          'user.tag',
                                          'user.home',
                                          'user.ensure',
                                          'user.membership',
                                          'user.storage',
                                          'user.slurm'])


@require_kwargs
@dataclass(frozen=True)
class PuppetGroupStorage(BaseModel):
    name: str
    owner: KerberosID #type: ignore
    group: Optional[KerberosID] = None #type: ignore
    autofs: Optional[PuppetAutofs] = None
    zfs: Optional[Union[PuppetZFS, bool]] = None
    globus: Optional[bool] = False


@require_kwargs
@dataclass(frozen=True)
class PuppetGroupRecord(BaseModel):
    gid: LinuxGID #type: ignore
    ensure: Optional[PuppetEnsure] = None #type: ignore
    tag: Optional[Set[str]] = None

    storage: Optional[List[PuppetGroupStorage]] = None
    slurm: Optional[SlurmRecord] = None

    @post_dump
    def sort(self, item, **kwargs):
        if 'tag' in item:
            item['tag'] = sorted(item['tag'])
        return item
    

@require_kwargs
@dataclass(frozen=True)
class PuppetGroupMap(BaseModel):
    group: Mapping[KerberosID, PuppetGroupRecord] #type: ignore


@require_kwargs
@dataclass(frozen=True)
class PuppetShareStorage(BaseModel):
    owner: KerberosID #type: ignore
    group: Optional[KerberosID] #type: ignore
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
    group: Optional[Mapping[KerberosID, PuppetGroupRecord]] = None #type: ignore
    user: Optional[Mapping[KerberosID, PuppetUserRecord]] = None #type: ignore
    share: Optional[Mapping[str, PuppetShareRecord]] = None


class MergeStrategy(Enum):
    ALL = 'all'
    PREFIX = 'prefix'
    NONE = 'none'


def parse_yaml_forest(yaml_files: list,
                      merge_on: Optional[MergeStrategy] = MergeStrategy.NONE) -> dict:
    yaml_forest = {}
    if merge_on is MergeStrategy.ALL:
        parsed_yamls = [parse_yaml(f) for f in yaml_files]
        yaml_forest = {'merged-all': puppet_merge(*parsed_yamls)}

    elif merge_on is MergeStrategy.NONE:
        yaml_forest = {f: parse_yaml(f) for f in yaml_files}

    elif merge_on is MergeStrategy.PREFIX:
        file_groups = {}
        for filename in yaml_files:
            prefix, _, _ = os.path.basename(filename).partition('.')
            if prefix in file_groups:
                file_groups[prefix].append(parse_yaml(filename))
            else:
                file_groups[prefix] = [parse_yaml(filename)]
        yaml_forest = {prefix: puppet_merge(*yamls) for prefix, yamls in file_groups.items()}

    return yaml_forest


def validate_yaml_forest(yaml_forest: dict,
                         MapSchema: Union[type[PuppetAccountMap],
                                          type[PuppetGroupMap],
                                          type[PuppetUserMap],
                                          type[PuppetShareMap]],
                         strict: Optional[bool] = False,
                         partial: Optional[bool] = False): 
    logger = logging.getLogger(__name__)

    for source_root, yaml_obj in yaml_forest.items():

        try:
            puppet_data = MapSchema.Schema().load(yaml_obj, #type: ignore
                                                  partial=partial)
        except marshmallow.exceptions.ValidationError as e: #type: ignore
            logger.error(f'[red]ValidationError: {source_root}[/]')
            logger.error(e.messages)
            if strict:
                sys.exit(ExitCode.VALIDATION_ERROR)
            continue
        else:
            yield source_root, puppet_data


def add_yaml_load_args(parser: argparse.ArgumentParser):
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


def add_validate_args(parser: argparse.ArgumentParser):
    add_yaml_load_args(parser)
    parser.add_argument('--dump', default='/dev/stdout',
                        help='Dump the validated YAML to the given file')
    parser.add_argument('files', nargs='+',
                        help='YAML files to validate.')


def validate_yamls(args: argparse.Namespace):

    console = Console(stderr=True)

    yaml_forest = parse_yaml_forest(args.files,
                                    merge_on=args.merge)
    
    for source_file, puppet_data in validate_yaml_forest(yaml_forest,
                                                         PuppetAccountMap,
                                                         args.strict):
        if not args.quiet:
            console.rule(source_file, style='blue')

        output_yaml = PuppetAccountMap.Schema().dumps(puppet_data) #type: ignore
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
            console.print(hl_yaml)

