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

import ldap
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
                    size_to_megs,
                    link_relative)


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
    groups: Optional[Set[KerberosID]] = None #type: ignore
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
    sponsors: Optional[List[KerberosID]] = None #type: ignore
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



def validate_sponsors(source_root: str, 
                      puppet_data: PuppetAccountMap,
                      args: argparse.Namespace,
                      **kwargs) -> None:
    logger = logging.getLogger(__name__)
    for group_name, group in puppet_data.group.items():
        if group.sponsors is not None:
            for sponsor_name in group.sponsors:
                if sponsor_name not in puppet_data.user:
                    logger.error(f'[red]ValidationError: {source_root}[/]')
                    logger.error(f'group.{group_name}.sponsors: {sponsor_name} not a valid user.')
                    if args.strict:
                        sys.exit(ExitCode.VALIDATION_ERROR)


def validate_user_groups(source_root: str, 
                         puppet_data: PuppetAccountMap,
                         args: argparse.Namespace,
                         **kwargs) -> None:
    logger = logging.getLogger(__name__)
    for user_name, user in puppet_data.user.items():
        if user.groups is not None:
            for group_name in user.groups:
                if not (group_name in puppet_data.group or group_name in puppet_data.user):
                    logger.error(f'[red]ValidationError: {source_root}[/]')
                    logger.error(f'user.{user_name}.groups: {group_name} not a valid group.')
                    if args.strict:
                        sys.exit(ExitCode.VALIDATION_ERROR)


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
    parser.add_argument('--echo', action='store_true', default=False)
    parser.add_argument('--postload-validate', default=False,
                        action='store_true')
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

        if args.postload_validate:
            validate_sponsors(source_file, puppet_data, args)
            validate_user_groups(source_file, puppet_data, args)

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

        if args.dump != '/dev/stdout' and args.echo and not args.quiet:
            console.print(hl_yaml)


class LDAPQueryParams(Enum):
    username = 'uid'
    uuid = 'ucdPersonUUID'
    email = 'mail'
    displayname = 'displayName'


def add_create_nologin_user_args(parser: argparse.ArgumentParser):
    qgroup = parser.add_argument_group('LDAP Query Parameters')
    xgroup = qgroup.add_mutually_exclusive_group(required=True)
    for param in LDAPQueryParams:
        xgroup.add_argument(f'--{param.name}', metavar=param.value)
    
    lgroup = parser.add_argument_group('LDAP Server Arguments')
    lgroup.add_argument('--ldap-uri', default='ldap://ldap.ucdavis.edu')

    parser.add_argument('--site-dir', 
                        type=Path,
                        required=True,
                        help='Site-specific puppet accounts directory.')
    parser.add_argument('--global-dir',
                        type=Path,
                        required=True,
                        help='Global puppet accounts directory.')
    parser.add_argument('--force',
                        default=False,
                        action='store_true',
                        help='Overwrite existing global YAML file.')


def flatten_and_decode(data: dict) -> dict:
    return {key: value[0].decode() for key, value in data.items()} #type: ignore


def create_nologin_user(args: argparse.Namespace):
    console = Console(stderr=True)
    logger = logging.getLogger(__name__)
    conn = ldap.initialize(args.ldap_uri)
    
    query = ','.join((f'{param.value}={getattr(args, param.name)}' for param in LDAPQueryParams \
                      if getattr(args, param.name) is not None))

    console.print(f'Server: {args.ldap_uri}')
    console.print(f'Query: {query}')

    try:
        result = conn.search_s('ou=People,dc=ucdavis,dc=edu',
                               ldap.SCOPE_SUBTREE, #type: ignore
                               query,
                               [param.value for param in LDAPQueryParams])
    except Exception as e:
        console.print(e)
    else:
        if not (0 < len(result) < 2): #type: ignore
            logger.error(f'Should get exactly one result (got {len(result)})') #type: ignore
            logger.error(result)
            sys.exit(ExitCode.BAD_LDAP_QUERY)
        
        data = flatten_and_decode(result[0][1]) #type: ignore
        console.print(f'Result: {data}')

        user_name = data['uid']
        yaml_dumper = PuppetUserMap.global_dumper()
        yaml_filename = args.global_dir / f'{user_name}.yaml'

        if not args.force and os.path.exists(yaml_filename):
            logger.error(f'{yaml_filename} already exists! Exiting.')
            sys.exit(ExitCode.FILE_EXISTS)

        user_record = PuppetUserMap(
            user = {
                user_name: PuppetUserRecord(
                    fullname = data['displayName'],
                    email = data.get('mail', ['donotreply@ucdavis.edu']),
                    uid = data['ucdPersonUUID'],
                    gid = data['ucdPersonUUID'],
                )
            }
        )

        output_yaml = yaml_dumper.dumps(user_record) #type: ignore
        hl_yaml = Syntax(output_yaml,
                         'yaml',
                         theme='github-dark',
                         background_color='default')

        if not args.quiet:
            console.print(hl_yaml)

        with yaml_filename.open('w') as fp:
            print(output_yaml, file=fp)

        try:
            link_relative(args.site_dir, yaml_filename)
        except FileExistsError:
            logger.info(f'{yaml_filename} already linked to {args.site_dir}.')

