#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : puppet.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 21.02.2023

import argparse
from dataclasses import asdict, field
from enum import Enum
import logging
import os
from typing import Callable, Optional, List, Mapping, Union, Set, Tuple, Type
from typing_extensions import Concatenate
import sys

from filelock import FileLock
import ldap
import marshmallow
from marshmallow import post_dump
from marshmallow_dataclass import dataclass
from rich import print as rprint
from rich.console import Console
from rich.syntax import Syntax

from .args import subcommand
from .errors import ExitCode
from .parsing import (MergeStrategy,
                      parse_yaml_forest,
                      puppet_merge,
                      validate_yaml_forest)
from .types import *
from .utils import (require_kwargs,
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
    def common_schema():
        return PuppetUserMap.Schema(only=['user.fullname', #type: ignore
                                          'user.email',
                                          'user.uid',
                                          'user.gid',
                                          'user.password',
                                          'user.shell'])

    @staticmethod
    def site_schema():
        return PuppetUserMap.Schema(only=['user.groups', #type: ignore
                                          'user.group_sudo',
                                          'user.tag',
                                          'user.home',
                                          'user.shell',
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

    @staticmethod
    def common_schema():
        return PuppetGroupMap.Schema(only=['group.gid', #type: ignore
                                           'group.tag',
                                           'group.ensure'])

    @staticmethod
    def site_schema():
        return PuppetGroupMap.Schema(only=['group.tag', 
                                           'group.ensure',
                                           'group.sponsors',
                                           'group.storage',
                                           'group.slurm'])


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
class PuppetMeta(BaseModel):
    admin_sponsors: List[KerberosID] #type: ignore


@require_kwargs
@dataclass(frozen=True)
class PuppetAccountMap(BaseModel):
    group: Mapping[KerberosID, PuppetGroupRecord] = field(default_factory=dict) #type: ignore
    user: Mapping[KerberosID, PuppetUserRecord] = field(default_factory=dict) #type: ignore
    share: Mapping[str, PuppetShareRecord] = field(default_factory=dict)
    meta: Optional[PuppetMeta] = None


def get_group_storage_paths(group: str, puppet_data: PuppetAccountMap):
    try:
        storage = puppet_data.group[group].storage #type: ignore
    except (KeyError, AttributeError):
        return None
    else:
        if storage is None:
            return None
        storages = []
        for storage in storage:
            if storage.owner == 'root' or 'root' in storage.name: # or storage.zfs is None or type(storage.zfs) is bool:
                continue
            path = Path('/group') / storage.name
            if storage.zfs not in (None, True, False):
                quota = storage.zfs.quota
            else:
                quota = None
            storages.append(path)
        return storages


def get_group_slurm_partitions(group: str, puppet_data: PuppetAccountMap):
    try:
        slurm = puppet_data.group[group].slurm
        partitions = slurm.partitions
    except (KeyError, AttributeError):
        return None, None
    else:
        return slurm.account, list(partitions.keys())


postload_validator_t = Callable[Concatenate[str, PuppetAccountMap, bool, ...], None]
_postload_validators : Mapping[str, postload_validator_t] = {}


def postload_validator(func: postload_validator_t) -> postload_validator_t:
    _postload_validators[func.__name__] = func
    return func


@postload_validator
def validate_sponsors(source_root: str, 
                      puppet_data: PuppetAccountMap,
                      strict: Optional[bool] = True,
                      **kwargs) -> None:
    logger = logging.getLogger(__name__)
    for group_name, group in puppet_data.group.items():
        if group.sponsors is not None:
            for sponsor_name in group.sponsors:
                if sponsor_name not in puppet_data.user:
                    logger.error(f'[red]ValidationError: {source_root}[/]')
                    logger.error(f'group.{group_name}.sponsors: {sponsor_name} not a valid user.')
                    if strict:
                        sys.exit(ExitCode.VALIDATION_ERROR)


@postload_validator
def validate_user_groups(source_root: str, 
                         puppet_data: PuppetAccountMap,
                         strict: Optional[bool] = True,
                         **kwargs) -> None:
    logger = logging.getLogger(__name__)
    for user_name, user in puppet_data.user.items():
        if user.groups is not None:
            for group_name in user.groups:
                if not (group_name in puppet_data.group or group_name in puppet_data.user):
                    logger.error(f'[red]ValidationError: {source_root}[/]')
                    logger.error(f'user.{user_name}.groups: {group_name} not a valid group.')
                    if strict:
                        sys.exit(ExitCode.VALIDATION_ERROR)


class YamlRepo:

    def __init__(self, root: Path,
                       max_depth: int = 1,
                       strict: bool = True,
                       load: bool = False,
                       model: Type[BaseModel] = PuppetAccountMap):

        self.root : Path = root
        self.max_depth : int = max_depth
        self.strict : bool = strict
        self.model : Type[BaseModel] = model
        self.data : Optional[BaseModel] = None
        self.postload_validators : List[postload_validator_t] = []
        if load:
            self.load()

    def find_yamls(self) -> List[Path]:
        yamls = []
        for root_dir, _, filenames in self.root.walk():
            if len(root_dir.relative_to(self.root).parents) >= self.max_depth:
                continue
            for filename in filenames:
                if filename.endswith('.yaml'):
                    yamls.append(root_dir / filename)
        return sorted(yamls, reverse=True)

    def parse_yamls(self, yaml_paths: List[Path]) -> BaseModel:
        yaml_forest = parse_yaml_forest(yaml_paths,
                                        merge_on=MergeStrategy.ALL)
        _, forest = next(validate_yaml_forest(yaml_forest,
                                              self.model,
                                              strict=self.strict))
        return forest

    def load(self):
        yaml_paths = self.find_yamls()
        logger = logging.getLogger(__name__)
        logger.info(f'Loading {len(yaml_paths)} YAML files from {self.root}')
        self.data = self.parse_yamls(yaml_paths)
        self.postload_validate()

    def _raise_notloaded(self):
        if self.data is None:
            raise RuntimeError('YamlRepo must have load() called.')

    def register_validator(self, func: Callable[Concatenate[str, PuppetAccountMap, bool, ...], None]):
        self.postload_validators.append(func)
    
    def postload_validate(self):
        self._raise_notloaded()
        for func in self.postload_validators:
            func(str(self.root), self.data, self.strict) # type: ignore


class CommonData(YamlRepo):

    def __init__(self, root: Path,
                       key_dir: Optional[Path] = None,
                       **kwargs):

        if key_dir is None:
            self.key_dir : Path = root / 'keys'
        else:
            self.key_dir : Path = key_dir

        self.user_schema = PuppetUserMap.common_schema()
        self.group_schema = PuppetGroupMap.common_schema()

        super().__init__(root, **kwargs)

    def lock(self, timeout: int):
        lock_path = self.root / '.cheeto.lock'
        return FileLock(lock_path, timeout=timeout)

    def write_key(self, user_name: str,
                        key: str):
        with (self.key_dir / f'{user_name}.pub').open('w') as fp:
            print(key, file=fp)

    def create_user(self, user_name: str,
                          user: PuppetUserRecord):

        logger = logging.getLogger(__name__)
        file_path = self.root / f'{user_name}.yaml'
        if file_path.exists():
            logger.info(f'Common YAML {file_path} exists, skipping.')
        else:
            record = PuppetUserMap(user = {user_name: user})
            with file_path.open('w') as fp:
                print(self.user_schema.dumps(record), file=fp)
        
        return file_path

    def create_group(self,
                     group_name: str,
                     gid: int,
                     sponsors: Optional[List[str]] = None) -> PuppetGroupMap:

        logger = logging.getLogger(__name__)
        file_path = self.root / f'{group_name}.yaml'
        group = PuppetGroupRecord(gid=gid, sponsors=sponsors)
        record = PuppetGroupMap(group = {group_name: group})
        if file_path.exists():
            logger.info(f'Common YAML {file_path} exists, skipping.')
        else:
            with file_path.open('w') as fp:
                print(self.group_schema.dumps(record), file=fp)

        return record

    def create_group_from_sponsor(self,
                                  user_name: str,
                                  uid: int) -> Tuple[str, PuppetGroupMap]:

        group_name = f'{user_name}grp'
        gid = MIN_PIGROUP_GID + uid
        sponsors = [user_name]
        return group_name, self.create_group(group_name, gid, sponsors=sponsors)
            

class SiteData(YamlRepo):

    def __init__(self,
                 root: Path,
                 common_root: Optional[Path] = None,
                 key_dir: Optional[Path] = None,
                 load: bool = True,
                 **kwargs):
        
        if common_root is None:
            self.common = CommonData(root.parent.parent)
        else:
            self.common = CommonData(common_root,
                                     key_dir=key_dir,
                                     **kwargs)

        self.user_schema = PuppetUserMap.site_schema()
        self.group_schema = PuppetGroupMap.site_schema()

        super().__init__(root, **kwargs)

        if load:
            self.load()

    def lock(self, timeout: int):
        return self.common.lock(timeout)

    def write_key(self,
                  user_name: str,
                  key: str):
        self.common.write_key(user_name, key)

    def update_user(self,
                    user_name: str,
                    user: PuppetUserRecord,
                    enable: bool = False):

        logger = logging.getLogger(__name__)
        logger.info(f'update_user: {user_name}')

        site_path, common_path = self.get_entity_paths(user_name)
        if not site_path.exists():
            self.create_user(user_name, user)
        else:
            current_user = self.data.user[user_name]
            merged = puppet_merge(current_user.to_raw_yaml(),
                                  user.to_raw_yaml())
            if enable and merged.get('shell', None) in DISABLED_SHELLS:
                merged['shell'] = DEFAULT_SHELL
            user = PuppetUserRecord.Schema().load(merged)
            record = PuppetUserMap(user = {user_name: user})
            with site_path.open('w') as fp:
                print(self.user_schema.dumps(record), file=fp)

    def enable_user(self,
                    user_name: str):
        # TODO: implement this :)
        pass

    def create_user(self,
                    user_name: str,
                    user: PuppetUserRecord):

        site_path, common_path = self.get_entity_paths(user_name)
        # Create the top-level common user.yaml file
        self.common.create_user(user_name, user)
        # Create the user.site.yaml file
        record = PuppetUserMap(user = {user_name: user})
        with site_path.open('w') as fp:
            print(self.user_schema.dumps(record), file=fp)
        # Link to the top-level user.yaml into the site directory
        self.link_entity(common_path)

    def create_group_from_sponsor(self,
                                  user_name: str,
                                  uid: int) -> str:
        
        # Build group name and create top level group.yaml
        group_name, group_record = self.common.create_group_from_sponsor(user_name, uid)
        site_path, common_path = self.get_entity_paths(group_name)
        # Write out site-specific group.site.yaml
        with site_path.open('w') as fp:
            print(self.group_schema.dumps(group_record), file=fp)
        # Link the top-level group.yaml into the site
        self.link_entity(common_path)
        return group_name
    
    def link_entity(self, common_path: Path):
        logger = logging.getLogger(__name__)
        try:
            link_relative(self.root, common_path)
        except FileExistsError:
            logger.info(f'{common_path} already linked to {self.root}.')

    def get_entity_paths(self, entity_basename: str) -> Tuple[Path, Path]:
        return self.root / f'{entity_basename}.site.yaml', \
               self.common.root / f'{entity_basename}.yaml'

    def get_group_storage_paths(self, group_name: str):
        self._raise_notloaded()
        return get_group_storage_paths(group_name, self.data) # type: ignore

    def get_group_slurm_partitions(self, group_name: str):
        self._raise_notloaded()
        return get_group_slurm_partitions(group_name, self.data) # type: ignore


def add_validate_args(parser: argparse.ArgumentParser):
    group = parser.add_argument_group('YAML Validation')
    group.add_argument('--dump', default='/dev/stdout',
                  help='Dump the validated YAML to the given file')
    group.add_argument('--echo', action='store_true', default=False)
    group.add_argument('files', nargs='+',
                        help='YAML files to validate.')
    group.add_argument('--merge',
                        default=MergeStrategy.NONE,
                        type=MergeStrategy,
                        action=EnumAction,
                        help='Merge the given YAML files before validation.')
    group.add_argument('--strict', action='store_true', default=False,
                        help='Terminate on validation errors.')
    group.add_argument('--partial',
                        default=False,
                        action='store_true',
                        help='Allow partial loading (ie missing keys).')
    group.add_argument('--postload-validate', default=False,
                       action='store_true')
    for func_name in _postload_validators.keys():
        group.add_argument(f'--{func_name}', action='store_true', default=False)



@subcommand('validate', add_validate_args)
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
            for postload_func in _postload_validators.values():
                postload_func(source_file, puppet_data, args.strict)
        else:
            for func_name in _postload_validators.keys():
                if getattr(args, func_name):
                    _postload_validators[func_name](source_file, puppet_data, args.strict)

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
    
    lgroup = parser.add_argument_group('LDAP Server')
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


@subcommand('create-nologin-user', 
            add_create_nologin_user_args)
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

