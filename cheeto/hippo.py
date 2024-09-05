#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : hippo.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 17.02.2023

import argparse
from enum import IntEnum, auto, Enum
import logging
import os
from pathlib import Path
import shlex
import shutil
import socket
import sys
import time
import traceback
from typing import Optional, Tuple, Union, List

from jinja2 import Environment, FileSystemLoader
import marshmallow
from marshmallow_dataclass import dataclass
import sh

from .args import subcommand
from .errors import ExitCode
from .git import Git, Gh, CIStatus, branch_name_title
from .mail import Mail
from .yaml import (parse_yaml,
                   parse_yaml_forest)
from .puppet import (PuppetAccountMap, 
                     SiteData,
                     PuppetUserRecord,
                     PuppetUserMap,
                     MergeStrategy)
from .templating import PKG_TEMPLATES
from .types import *
from .utils import (human_timestamp,
                    require_kwargs,
                    TIMESTAMP_NOW,
                    sanitize_timestamp)


@require_kwargs
@dataclass(frozen=True)
class HippoAccount(BaseModel):
    name: str
    email: Email #type: ignore
    kerb: KerberosID #type: ignore
    iam: IAMID #type: ignore
    mothra: MothraID #type: ignore
    key: Optional[str] = None


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


class ConversionState(IntEnum):
    UNINIT = auto()
    INVALID = auto()
    ERROR = auto()
    PREPROC = auto()
    PROC = auto()
    COMPLETE = auto()

    
class ConversionOp(Enum):
    USER = 'USER'
    SPONSOR = 'SPONSOR'
    KEY = 'KEY'
    NOOP = 'NOOP'


def hippo_to_puppet(hippo_record: HippoRecord,
                    site: SiteData,
                    sponsors_group: str) -> Tuple[str, PuppetUserRecord, ConversionOp]:

    logger = logging.getLogger(__name__)

    user_name = hippo_record.account.kerb
    uid = hippo_record.account.mothra
    groups = set(hippo_record.groups)
    dataop = ConversionOp.USER

    # Check if the group for sponsors is in the group request:
    # if so, this is a new PI group and their group YAML should be created
    if sponsors_group in groups: # type: ignore
        logger.info(f'{user_name} appears to be a sponsor account, creating group.')
        group_name = site.create_group_from_sponsor(user_name, uid)
        # Make the sponsor a member of their own new group
        groups.add(group_name)
        dataop = ConversionOp.SPONSOR
    else:
        logger.info(f'{user_name} appears to be a user account.')

    if not groups and dataop != ConversionOp.SPONSOR:
        dataop = ConversionOp.KEY

    user = PuppetUserRecord( #type: ignore
        fullname = hippo_record.account.name,
        email = hippo_record.account.email,
        uid = uid,
        gid = uid,
        groups = groups
    )
    
    # don't re-enable nologin accounts on key update
    enable = dataop in (ConversionOp.USER, ConversionOp.SPONSOR)
    site.update_user(user_name, user, enable=enable)
    
    if hippo_record.account.key is not None:
        site.write_key(user_name, hippo_record.account.key)

    logger.info(f'hippo_to_puppet: did {dataop.name} for {user_name} with groups {groups}') 

    return user_name, user, dataop


class HippoConverter:

    def __init__(self,
                 input_file: Path,
                 site: SiteData,
                 jinja_env: Environment,
                 processed_dir: Path,
                 invalid_dir: Path,
                 noop_dir: Path,
                 sponsors_group: str = 'sponsors'):

        self.logger = logging.getLogger(__name__)
        self.input_file = input_file
        self.site = site
        self.jinja_env = jinja_env
        self.processed_dir = processed_dir
        self.invalid_dir = invalid_dir
        self.noop_dir = noop_dir
        self.sponsors_group = sponsors_group
        self.hippo_record : Optional[HippoRecord] = None
        self.user_name : Optional[str] = None
        self.user_record : Optional[PuppetUserRecord] = None
        self.state : ConversionState = ConversionState.UNINIT
        self.op : ConversionOp = ConversionOp.NOOP

        for d in (self.invalid_dir, self.noop_dir, self.processed_dir):
            d.mkdir(parents=True, exist_ok=True)

    def __str__(self):
        return f'HippoConversion: {self.input_file}\n'\
               f'         status: {self.state.name}\n'\
               f'             op: {self.op.name}\n'\
               f'           user: {self.user_name}\n'\
               f'                 {self.user_record}'

    def set_state(self, state: ConversionState):
        self.logger.info(f'{self.input_file}: set state to {state.name}')
        self.state = state

    def set_op(self, op: ConversionOp):
        self.logger.info(f'{self.input_file}: set op to {op.name}')
        self.op = op

    def summary(self):
        if self.op == ConversionOp.NOOP:
            return f'op: no-op, file: {self.input_file}'
        msg = f'op: {self.op}, user: {self.user_name}'
        if self.op == ConversionOp.KEY:
            return msg
        else:
            return f'{msg}, {self.user_record.groups}' #type: ignore

    def set_invalid(self):
        if self.state == ConversionState.INVALID:
            self.logger.warning(f'{self.input_file} set Invalid twice.')
            return
        self.set_state(ConversionState.INVALID)
        self.logger.info(f'Moving {self.input_file} to invalid dir ({self.invalid_dir})')
        shutil.move(self.input_file, self.invalid_dir / processed_filename(self.input_file))

    def set_noop(self):
        if self.state != ConversionState.PROC:
            self.logger.warning(f'{self.input_file}: tried to set_noop with state not PROC.')
            return
        self.set_state(ConversionState.COMPLETE)
        self.set_op(ConversionOp.NOOP)
        self.logger.info(f'Moving {self.input_file} to noop dir ({self.noop_dir})')
        shutil.move(self.input_file, self.noop_dir / processed_filename(self.input_file))

    def set_complete(self):
        if self.state == ConversionState.INVALID:
            self.logger.warning(f'{self.input_file}: tried to set_complete on invalid conversion.')
            return
        if self.state == ConversionState.COMPLETE:
            self.logger.warning(f'{self.input_file} tried to set_complete on completed conversion.')
            return
        self.logger.info(f'Moving {self.input_file} to processed dir ({self.processed_dir})')
        shutil.move(self.input_file, self.processed_dir / processed_filename(self.input_file))
        self.set_state(ConversionState.COMPLETE)

    def preprocess(self):
        self.logger.info(f'Loading {self.input_file}')
        self.hippo_record = load_hippo(self.input_file)
        if self.hippo_record is None:
            self.logger.error(f'{self.input_file}: validation error!')
            self.set_invalid()
            return

        for group in set(self.hippo_record.groups):
            if group not in self.site.data.group: #type: ignore
                self.logger.error(f'{self.input_file}: {group} is not a valid group.')
                self.set_invalid()
                return

        self.set_state(ConversionState.PREPROC)

    def process(self):
        if self.state == ConversionState.PREPROC and self.hippo_record is not None:
            self.user_name, self.user_record, self.op = hippo_to_puppet(self.hippo_record,
                                                                        self.site,
                                                                        sponsors_group=self.sponsors_group)
            self.set_state(ConversionState.PROC)

    def postprocess(self):
        if self.state != ConversionState.PROC or self.user_record is None:
            return

        mail = Mail()

        if self.op == ConversionOp.USER:
            group = list(self.hippo_record.groups)[0]
            slurm_account, slurm_partitions = self.site.get_group_slurm_partitions(group)
            storages = self.site.get_group_storage_paths(group)
            template = self.jinja_env.get_template('account-ready.txt.j2')
            email_contents = template.render(
                                cluster=self.hippo_record.meta.cluster,
                                username=self.user_name,
                                domain=self.site.root.name,
                                slurm_account=slurm_account,
                                slurm_partitions=slurm_partitions,
                                storages=storages
                             )
            email_subject = f'Account Information: {self.hippo_record.meta.cluster}'
            email_cmd = mail.send(self.user_record.email,
                                  email_contents,
                                  reply_to='hpc-help@ucdavis.edu',
                                  subject=email_subject)
        elif self.op == ConversionOp.KEY:
            template = self.jinja_env.get_template('key-updated.txt.j2')
            email_contents = template.render(
                                cluster=self.hippo_record.meta.cluster,
                                username=self.user_name,
                                domain=self.site.root.name
                             )
            email_subject = f'Key Updated: {self.hippo_record.meta.cluster}'
            email_cmd = mail.send(self.user_record.email,
                                  email_contents,
                                  reply_to='hpc-help@ucdavis.edu',
                                  subject=email_subject)
        else:
            # okay this is ugly but if its a sponsor group, its new, so the only
            # two elements in the groups will be "sponsors" and the new group
            group = list(self.user_record.groups.copy() - {self.sponsors_group})[0]
            template = self.jinja_env.get_template('new-sponsor.txt.j2')
            email_contents = template.render(
                                cluster=self.hippo_record.meta.cluster,
                                username=self.user_name,
                                domain=self.site.root.name,
                                group=group
                             )
            email_subject = f'New Sponsor Account: {self.hippo_record.meta.cluster}'
            email_cmd = mail.send(self.user_record.email,
                                  email_contents,
                                  reply_to='hpc-help@ucdavis.edu',
                                  subject=email_subject)

        self.logger.info(f'Sending email: \nSubject: {email_subject}\nBody: {email_contents}')
        email_cmd()
        self.set_complete()


class HippoData:

    def __init__(self,
                 root: Path,
                 processed_dir: Optional[Path] = None,
                 invalid_dir: Optional[Path] = None,
                 noop_dir: Optional[Path] = None):
        self.root = root
        if processed_dir is None:
            self.processed_dir = root / '.processed'
        else:
            self.processed_dir = processed_dir
        if invalid_dir is None:
            self.invalid_dir = root / '.invalid'
        else:
            self.invalid_dir = invalid_dir
        if noop_dir is None:
            self.noop_dir = root / '.noop'
        else:
            self.noop_dir = noop_dir

    def find_yamls(self):
        return self.root.glob('*.yaml')

    def convert_yamls(self,
                      site: SiteData,
                      jinja_env: Environment,
                      sponsors_group: str):

        for hippo_file in self.find_yamls():
            converter = HippoConverter(hippo_file,
                                       site,
                                       jinja_env,
                                       self.processed_dir,
                                       self.invalid_dir,
                                       self.noop_dir,
                                       sponsors_group=sponsors_group)
            yield converter


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
    parser.add_argument('--sponsor-group',
                        required=True,
                        help='Group that sponsors belong to.')


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
    parser.add_argument('-i', '--hippo-dir',
                        type=Path,
                        required=True,
                        help='Incoming hippo directory.')
    parser.add_argument('--site-dir', 
                        type=Path,
                        required=True,
                        help='Site-specific puppet accounts directory.')
    parser.add_argument('--sponsor-group',
                        default='sponsors',
                        help='Group that sponsors belong to.')
    parser.add_argument('--global-dir',
                        type=Path,
                        help='Global puppet accounts directory.')
    parser.add_argument('--key-dir',
                        type=Path,
                        help='Puppet SSH keys directory.')
    parser.add_argument('--processed-dir', type=Path,
                        help='Directory to move completed user.txt files to.')
    parser.add_argument('--invalid-dir', type=Path,
                        help='Directory to move invalid user.txt files to.')
    parser.add_argument('--base-branch',
                        default='main',
                        help='Branch to make PR against.')
    parser.add_argument('--timeout',
                        type=int,
                        default=30)


def processed_filename(filename: Path) -> str:
    date_suffix = f'.{sanitize_timestamp(TIMESTAMP_NOW)}'
    return filename.with_suffix(''.join((date_suffix, *filename.suffixes))).name


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
                                   logfile=args.log,
                                   timestamp=human_timestamp(TIMESTAMP_NOW),
                                   pyexe=sys.executable,
                                   exeargs=shlex.join(sys.argv))
        Mail().send('cswel@ucdavis.edu', contents, subject=subject)()
        

def _sync(args, jinja_env: Environment):
    logger = logging.getLogger(__name__)

    site_data = SiteData(args.site_dir,
                         common_root=args.global_dir,
                         key_dir=args.key_dir,
                         load=False)
    hippo_data = HippoData(args.hippo_dir,
                           processed_dir=args.processed_dir,
                           invalid_dir=args.invalid_dir)

    lock = site_data.lock(args.timeout)
    with lock:
        
        gh = Gh(working_dir=site_data.common.root)
        git = Git(working_dir=site_data.common.root)
        git.checkout(branch=args.base_branch)()
        git.clean(force=True, exclude=os.path.basename(lock.lock_file))()
        git.pull()()

        site_data.load()

        working_branch, pr_title = branch_name_title()
        git.checkout(branch=working_branch, create=True)()

        fqdn = socket.getfqdn()
        
        get_commit = git.rev_parse()
        start_commit = get_commit().strip()
        converters = list(hippo_data.convert_yamls(site_data, 
                                                   jinja_env, 
                                                   args.sponsor_group))
        for converter in converters:
            converter.preprocess()
            if converter.state == ConversionState.PREPROC:
                converter.process()
                message = f'[{fqdn}] {converter.summary()}'
                git.add(Path('.'))()
                try:
                    git.commit(message)()
                except sh.ErrorReturnCode_1: #type: ignore
                    logger.info(f'Nothing to commit.')
                    converter.set_noop()
        end_commit = get_commit().strip()

        if start_commit == end_commit:
            logger.info('No commits made, skipping PR flow.')
            for converter in converters:
                #converter.set_noop()
                logger.info(converter)
            return
        
        logger.info(f'Pushing and creating branch: {working_branch}.')
        git.push(remote_create=working_branch)()

        logger.info('Creating pull request.')
        pr_body = '\n'.join((f'- {c.summary()}' for c in converters))
        gh.pr_create(args.base_branch, title=pr_title, body=pr_body)()

        wait_on_pull_request(gh, working_branch, args.timeout, jinja_env)
       
        # TODO: check if merge has already occurred in case admin does so manually.
        logger.info('Merging pull request.')
        gh.pr_merge(working_branch)()
        
        git.checkout(branch=args.base_branch)()
        git.pull()()

        logger.info(f'Moving processed files to {hippo_data.processed_dir}')
        for converter in converters:
            converter.postprocess()
                

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
