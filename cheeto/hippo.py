#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : hippo.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 17.02.2023

from dataclasses import asdict
from datetime import datetime
import logging
import os
from pathlib import Path
import shutil
import socket
import sys
import time
import traceback
from typing import Mapping, Optional, Tuple, Union, List

from filelock import FileLock
from jinja2 import Environment, FileSystemLoader
import marshmallow
from marshmallow_dataclass import add_schema
from marshmallow_dataclass import dataclass
import sh

from .errors import ExitCode
from .git import Git, Gh, CIStatus
from .mail import Mail
from .puppet import (MIN_PIGROUP_GID, PuppetAccountMap, PuppetGroupMap, PuppetGroupRecord,
                     PuppetUserRecord,
                     PuppetUserMap,
                     parse_yaml_forest,
                     validate_yaml_forest,
                     MergeStrategy)
from .templating import PKG_TEMPLATES
from .types import *
from .utils import link_relative, require_kwargs, get_relative_path
from .utils import parse_yaml, puppet_merge
from .slurm import get_group_slurm_partitions


@require_kwargs
@dataclass(frozen=True)
class HippoSponsor(BaseModel):
    accountname: str
    name: str
    email: Email #type: ignore
    kerb: KerberosID #type: ignore
    iam: IAMID #type: ignore
    mothra: MothraID #type: ignore


@require_kwargs
@dataclass(frozen=True)
class HippoAccount(BaseModel):
    name: str
    email: Email #type: ignore
    kerb: KerberosID #type: ignore
    iam: IAMID #type: ignore
    mothra: MothraID #type: ignore
    key: str


@require_kwargs
@dataclass(frozen=True)
class HippoMeta(BaseModel):
    cluster: str


@require_kwargs
@dataclass(frozen=True)
class HippoRecord(BaseModel):
    sponsor: HippoSponsor
    account: HippoAccount
    meta: HippoMeta


@require_kwargs
@dataclass(frozen=True)
class HippoSponsorGroupMapping(BaseModel):
    mapping: Mapping[KerberosID, KerberosID] #type: ignore


@require_kwargs
@dataclass(frozen=True)
class HippoAdminSponsorList(BaseModel):
    sponsors: List[KerberosID] #type: ignore


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


def add_validate_args(parser):
    parser.add_argument('-i', '--hippo-file',
                        type=Path,
                        nargs='+',
                        required=True)


def validate(args) -> None:
    for hippo_file in args.hippo_file:
        record = load_hippo(hippo_file, quiet=args.quiet)
        if record is None:
            sys.exit(ExitCode.VALIDATION_ERROR)


def load_hippo(hippo_file: Path,
               quiet: Optional[bool] = True) -> Union[None, HippoRecord]:

    logger = logging.getLogger(__name__)
    
    hippo_yaml = parse_yaml(str(hippo_file))
    try:
        hippo_record = HippoRecord.Schema().load(hippo_yaml) #type: ignore
    except marshmallow.exceptions.ValidationError as e: #type: ignore
        logger.error(f'[red]ValidationError: {hippo_file}[/]')
        logger.error(e.messages)
        return None
    else:
        if not quiet:
            logger.info(hippo_record)
        return hippo_record


def hippo_to_puppet(hippo_file: Path,
                    global_dir: Path,
                    site_dir: Path,
                    key_dir: Path,
                    current_state: PuppetAccountMap,
                    group_map: HippoSponsorGroupMapping,
                    admin_sponsors: HippoAdminSponsorList) -> Tuple[Optional[str],
                                                                    Optional[str],
                                                                    Optional[PuppetUserRecord]]:
    logger = logging.getLogger(__name__)

    hippo_record = load_hippo(hippo_file)
    if hippo_record is None:
        logger.error(f'{hippo_file}: validation error!')
        return None, None, None, None

    user_name = hippo_record.account.kerb
    site_dumper = PuppetUserMap.site_dumper()
    site_filename = site_dir / f'{user_name}.site.yaml'

    global_dumper = PuppetUserMap.global_dumper()
    global_filename = global_dir / f'{user_name}.yaml'

    sponsor_group = f'{hippo_record.sponsor.kerb}grp'
    if hippo_record.sponsor.kerb in admin_sponsors.sponsors:
        logger.info(f'{user_name} appears to be a sponsor account.')
        sponsor_group = create_sponsor_group(user_name,
                                             hippo_record.account.mothra,
                                             global_dir,
                                             site_dir)
    else:
        logger.info(f'{user_name} appears to be a user account.')

        if hippo_record.sponsor.kerb in group_map.mapping:
            sponsor_group = group_map.mapping[hippo_record.sponsor.kerb]
            
        if sponsor_group not in current_state.group: #type: ignore
            logger.error(f'{hippo_file}: {sponsor_group} is not a valid group.')
            return None, None, None, None

    user_record = PuppetUserRecord( #type: ignore
        fullname = hippo_record.account.name,
        email = hippo_record.account.email,
        uid = hippo_record.account.mothra,
        gid = hippo_record.account.mothra,
        groups = [sponsor_group] #type: ignore
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
        link_relative(site_dir, global_filename)
    except FileExistsError:
        logger.info(f'{global_filename} already linked to {site_dir}.')

    if key_dir:
        key_dest = key_dir / f'{hippo_record.account.kerb}.pub'
        with key_dest.open('w') as fp:
            print(hippo_record.account.key, file=fp)
    
    return user_name, sponsor_group, user_record, hippo_record


def create_sponsor_group(sponsor_username: str,
                         sponsor_uid: int,
                         global_dir: Path,
                         site_dir: Path):
    logger = logging.getLogger(__name__)
    group_name = f'{sponsor_username}grp'
    group_record = PuppetGroupRecord(
        gid = MIN_PIGROUP_GID + sponsor_uid
    )
    group_map = PuppetGroupMap(
        group = {group_name: group_record}
    )
    group_filename = global_dir / f'{group_name}.yaml'
    if os.path.exists(group_filename):
        logger.warning(f'{group_filename} already exists!')
    else:
        logger.info(f'Writing out {group_filename}')
        group_map.save_yaml(group_filename)
        link_relative(site_dir, group_filename)

    return group_name


def get_group_storage_paths(group: str, puppet_data: PuppetAccountMap):
    try:
        storage = puppet_data.group[group].storage
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
    parser.add_argument('--admin-sponsors',
                        type=Path,
                        required=True,
                        help='Accounts that sponsor the sponsors.')


def convert(args):
    logger = logging.getLogger(__name__)

    current_state = PuppetAccountMap.Schema().load(parse_yaml(args.cluster_yaml)) #type: ignore
    
    if args.group_map:
        group_map = HippoSponsorGroupMapping.load_yaml(args.group_map) #type: ignore
    else:
        group_map = HippoSponsorGroupMapping.Schema().load({'mapping': {}}) #type: ignore

    admin_sponsors_map = HippoAdminSponsorList.load_yaml(args.admin_sponsors) #type: ignore

    for hippo_file in args.hippo_file:
        logger.info(f'Processing {hippo_file}...')
        hippo_to_puppet(hippo_file,
                        args.global_dir,
                        args.site_dir,
                        args.key_dir,
                        current_state,
                        group_map,
                        admin_sponsors_map)


def add_sync_args(parser):
    add_convert_args(parser)
    parser.add_argument('--timeout', type=int, default=30)
    parser.add_argument('--processed-dir', required=True, type=Path,
                        help='Directory to move completed user.txt files to.')
    parser.add_argument('--base-branch', default='main')


def create_branch_name(prefix: Optional[str] = 'cheeto-hippo-sync') -> str:
    timestamp = datetime.now().strftime('%Y-%m-%d.%H-%M-%S')
    return f'{prefix}.{timestamp}'


def sync(args):
    templates_dir = PKG_TEMPLATES / 'emails'
    jinja_env = Environment(loader=FileSystemLoader(searchpath=templates_dir))

    try:
        _sync(args, jinja_env)
    except:
        logger = logging.getLogger(__name__)
        logger.critical(f'Exception in sync, sending ticket to hpc-help.')
        logger.critical(traceback.format_exc())
        template = jinja_env.get_template('sync-error.txt.j2')
        subject = f'cheeto sync exception on {socket.getfqdn()}'
        contents = template.render(hostname=socket.getfqdn(),
                                   stacktrace=traceback.format_exc(),
                                   logfile=args.log)
        Mail().send('cswel@ucdavis.edu', contents, subject=subject)()
        

def _sync(args, jinja_env: Environment):
    logger = logging.getLogger(__name__)
    lock_path = args.global_dir / '.cheeto.lock'
    lock = FileLock(lock_path, timeout=args.timeout)

    with lock:

        current_state = PuppetAccountMap.load_yaml(args.cluster_yaml) #type: ignore
        
        if args.group_map:
            group_map = HippoSponsorGroupMapping.load_yaml(args.group_map) #type: ignore
        else:
            group_map = HippoSponsorGroupMapping.Schema().load({'mapping': {}}) #type: ignore

        admin_sponsors_map = HippoAdminSponsorList.load_yaml(args.admin_sponsors) #type: ignore


        gh = Gh(working_dir=args.global_dir)
        git = Git(working_dir=args.global_dir)
        git.checkout(branch=args.base_branch)()
        git.clean(force=True)()
        git.pull()()
        working_branch = create_branch_name()
        git.checkout(branch=working_branch, create=True)()

        fqdn = socket.getfqdn()
        
        created_users = []
        for hippo_file in args.hippo_file:
            logger.info(f'Processing {hippo_file}...')
            user, sponsor, user_record, hippo_record = hippo_to_puppet(hippo_file,
                                                                       args.global_dir,
                                                                       args.site_dir,
                                                                       args.key_dir,
                                                                       current_state,
                                                                       group_map,
                                                                       admin_sponsors_map)
            created_users.append((user, sponsor, user_record, hippo_record))
            if user is None:
                continue
            message = f'[{fqdn}] user: {user}, sponsor: {sponsor}'
            git.add(Path('.'))()
            try:
                git.commit(message)()
            except sh.ErrorReturnCode_1: #type: ignore
                logger.info(f'Nothing to commit.')
        
        logger.info(f'Pushing and creating branch: {working_branch}.')
        git.push(remote_create=working_branch)()

        logger.info('Creating pull request.')
        gh.pr_create(args.base_branch)()

        wait_on_pull_request(gh, working_branch, args.timeout, jinja_env)
       
        # TODO: check if merge has already occurred in case admin does so manually.
        logger.info('Merging pull request.')
        gh.pr_merge(working_branch)()
        
        git.checkout(branch=args.base_branch)()
        git.pull()()

        notify_template = jinja_env.get_template('account-ready.txt.j2')
        mail = Mail()
        logger.info(f'Moving processed files to {args.processed_dir}')
        for hippo_file, (user, sponsor, puppet_record, hippo_record) in zip(args.hippo_file, created_users): #type: ignore
            if user is not None:
                shutil.move(hippo_file, args.processed_dir / hippo_file.name)
                slurm_account, slurm_partitions = get_group_slurm_partitions(sponsor, current_state)
                storages = get_group_storage_paths(sponsor, current_state)

                email_contents = notify_template.render(
                                    cluster=hippo_record.meta.cluster,
                                    username=user,
                                    domain=args.site_dir.name,
                                    slurm_account=slurm_account,
                                    slurm_partitions=slurm_partitions,
                                    storages=storages
                                 )
                logger.info(f'Sending email: {email_contents}')
                email_subject = f'Account Information: {hippo_record.meta.cluster}'
                mail.send(puppet_record.email,
                          email_contents,
                          reply_to='hpc-help@ucdavis.edu',
                          subject=email_subject)()


def wait_on_pull_request(gh: Gh,
                         branch: str,
                         poll_interval: int,
                         jinja_env: Environment):

    logger = logging.getLogger(__name__)
    logger.info(f'Waiting on CI for pull request.')
    #logger.info(gh.pr_view(branch)())
    pr_url = gh.pr_view_url(branch)().strip() #type: ignore
    mail = Mail()

    ticket_created = False
    while True:
        status = gh.get_last_run_status(branch)
        if status is CIStatus.INCOMPLETE:
            logger.info(f'CI incomplete; waiting {poll_interval}s: {pr_url}')
        elif status is CIStatus.SUCCESS:
            logger.info(f'CI success: {pr_url}')
            break
        else:
            if not ticket_created:
                subject = f'cheeto account sync CI failed for branch {{branch}}'
                logger.warning(f'CI failed, creating ticket: {pr_url}')
                template = jinja_env.get_template('ci-error.txt.j2')
                contents = template.render(pr_url=pr_url)
                mail.send('cswel@ucdavis.edu', contents, subject=subject)()
                ticket_created = True
            else:
                logger.info(f'CI failed, waiting resolution: {pr_url}')
        time.sleep(poll_interval)
