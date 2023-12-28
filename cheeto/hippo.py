#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : hippo.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 17.02.2023

import argparse
from datetime import datetime
from enum import auto, Enum
import glob
import logging
import os
from pathlib import Path
import shutil
import socket
import sys
import time
import traceback
from typing import Optional, Tuple, Union, List, Set

from filelock import FileLock
from jinja2 import Environment, FileSystemLoader
import marshmallow
from marshmallow_dataclass import dataclass
import sh

from .args import subcommand
from .errors import ExitCode
from .git import Git, Gh, CIStatus
from .mail import Mail
from .parsing import (parse_yaml,
                      puppet_merge,
                      parse_yaml_forest,
                      validate_yaml_forest)
from .puppet import (PuppetAccountMap, 
                     SiteData,
                     PuppetUserRecord,
                     PuppetUserMap,
                     MergeStrategy)
from .templating import PKG_TEMPLATES
from .types import *
from .utils import require_kwargs


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
    groups: List[KerberosID] #type: ignore
    account: HippoAccount
    meta: HippoMeta


def add_sanitize_args(parser):
    parser.add_argument('--site-file',
                        type=Path,
                        required=True,
                        help='Site-specific puppet YAML.')
    parser.add_argument('--global-file',
                        type=Path,
                        required=True,
                        help='Global puppet YAML.')


@subcommand('sanitize', add_sanitize_args)
def sanitize(args: argparse.Namespace):
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

    site_dumper = PuppetUserMap.site_schema()
    global_dumper = PuppetUserMap.common_schema()

    with args.global_file.open('w') as fp:
        print(global_dumper.dumps(user_record), file=fp)

    with args.site_file.open('w') as fp:
        print(site_dumper.dumps(user_record), file=fp)


def add_validate_args(parser):
    parser.add_argument('-i', '--hippo-file',
                        type=Path,
                        nargs='+',
                        required=True)


@subcommand('validate', add_validate_args)
def validate(args: argparse.Namespace) -> None:
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


class PostConvert:

    def __init__(self,
                 hippo_path: Path,
                 hippo_record: Optional[HippoRecord] = None,
                 user_name: Optional[str] = None,
                 user_record: Optional[PuppetUserRecord] = None):

        self.hippo_path = hippo_path
        self.hippo_record = hippo_record
        self.user_name = user_name
        self.user_record = user_record

    @property
    def commit_message(self):
        groups = ', '.join(self.user_record.groups) #type: ignore
        fqdn = socket.getfqdn()
        return f'[{fqdn}] user: {self.user_name}, groups: {groups}'

    



def hippo_to_puppet(hippo_record: HippoRecord,
                    site: SiteData,
                    sponsors_group: str = 'sponsors') -> Tuple[str, PuppetUserRecord]:

    logger = logging.getLogger(__name__)

    user_name = hippo_record.account.kerb
    uid = hippo_record.account.mothra
    groups = set(hippo_record.groups)

    # Check if the group for sponsors is in the group request:
    # if so, this is a new PI group and their group YAML should be created
    if sponsors_group in groups: # type: ignore
        logger.info(f'{user_name} appears to be a sponsor account, creating group.')
        group_name = site.create_group_from_sponsor(user_name, uid)
        # The PI's themselves needs to be in their new group
        groups.add(group_name)
    else:
        logger.info(f'{user_name} appears to be a user account.')

    user = PuppetUserRecord( #type: ignore
        fullname = hippo_record.account.name,
        email = hippo_record.account.email,
        uid = uid,
        gid = uid,
        groups = groups
    )

    site.update_user(user_name, user)

    return user_name, user


class HippoConverter:

    def __init__(self,
                 root: Path,
                 processed_dir: Optional[Path] = None,
                 invalid_dir: Optional[Path] = None):
        self.root = root
        if processed_dir is None:
            self.processed_dir = root / '.processed'
        else:
            self.processed_dir = processed_dir
        if invalid_dir is None:
            self.processed_dir = root / '.invalid'
        else:
            self.invalid_dir = invalid_dir

    def find_yamls(self):
        return self.root.glob('*.yaml')

    def convert_yamls(self,
                      site: SiteData,
                      sponsors_group: str = 'sponsors'):

        logger = logging.getLogger(__name__)

        for hippo_file in self.find_yamls():
            hippo_record = load_hippo(hippo_file)
            if hippo_record is None:
                logger.error(f'{hippo_file}: validation error!')
                shutil.move(hippo_file, self.invalid_dir)
                continue

            for group in set(hippo_record.groups):
                if group not in site.data.group: #type: ignore
                    logger.error(f'{hippo_file}: {group} is not a valid group.')
                    shutil.move(hippo_file, self.invalid_dir)
                    continue

            user_name, user_record = hippo_to_puppet(hippo_record,
                                                     site,
                                                     sponsors_group=sponsors_group)
            




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


@subcommand('convert', add_convert_args)
def convert(args: argparse.Namespace):
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


@subcommand('sync', add_sync_args)
def sync(args: argparse.Namespace):
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

        site = SiteData(args.site_dir)
        
        gh = Gh(working_dir=args.global_dir)
        git = Git(working_dir=args.global_dir)
        git.checkout(branch=args.base_branch)()
        git.clean(force=True)()
        git.pull()()
        working_branch = create_branch_name()
        git.checkout(branch=working_branch, create=True)()

        fqdn = socket.getfqdn()
        
        get_commit = git.rev_parse()
        start_commit = get_commit().strip()
        created_users = []
        for hippo_file in args.hippo_file:
            logger.info(f'Processing {hippo_file}...')
            user_name, groups, user_record, hippo_record = hippo_to_puppet(hippo_file, site)
            created_users.append((user_name, groups, user_record, hippo_record))
            if user_name is None:
                continue
            message = f'[{fqdn}] user: {user_name}, groups: {groups}'
            git.add(Path('.'))()
            try:
                git.commit(message)()
            except sh.ErrorReturnCode_1: #type: ignore
                logger.info(f'Nothing to commit.')
        end_commit = get_commit().strip()

        if start_commit == end_commit:
            logger.info('No commits made, skipping PR flow.')
            for hippo_file, (user, sponsor, puppet_record, hippo_record) in zip(args.hippo_file, created_users): #type: ignore
                if user is not None:
                    os.remove(hippo_file)
            return
        
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
