#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : slurm.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 10.04.2023

import argparse
import csv
from enum import Enum, auto
from io import StringIO
import json
import os
import sys
from typing import Any, Generator, Tuple, Optional, TextIO

from rich.console import Console
from rich.progress import track
import sh

from .args import subcommand
from .types import *
from .puppet import (parse_yaml_forest,
                     validate_yaml_forest,
                     MergeStrategy,
                     PuppetAccountMap,
                     SlurmQOS)
from .utils import (check_filter,
                    filter_nulls)

class SControl:

    def __init__(self, scontrol_path: Optional[str] = None,
                       sudo: bool = False):
        if scontrol_path is None:
            self.scontrol_path = sh.which('scontrol').strip() #type: ignore
        else:
            self.scontrol_path = scontrol_path

        if not os.path.exists(self.scontrol_path):
            raise RuntimeError(f'{self.scontrol_path} does not exist!')

        if sudo:
            self.cmd = sh.sudo.bake(self.scontrol_path, '-oQ')
        else:
            self.cmd = sh.Command(self.scontrol_path).bake('-oQ')

        self.show = self.cmd.bake('show')

    def show_partitions(self) -> sh.Command:
        return self.show.bake('partitions')

    @staticmethod
    def get_scontrol_parser(fp: TextIO) -> Generator[dict[str, str], Any, None]:
        for line in fp:
            tokens = line.strip().split()
            tuples = (t.partition('=') for t in tokens)
            yield {k: v for k, _, v in tuples}


class SAcctMgr:

    def __init__(self, sacctmgr_path: Optional[str] = None,
                       sudo: bool = False):
        if sacctmgr_path is None:
            self.sacctmgr_path = sh.which('sacctmgr').strip() #type: ignore
        else:
            self.sacctmgr_path = sacctmgr_path

        if not os.path.exists(self.sacctmgr_path):
            raise RuntimeError(f'{self.sacctmgr_path} does not exist!')

        if sudo:
            self.cmd = sh.sudo.bake(self.sacctmgr_path, '-iQ')
        else:
            self.cmd = sh.Command(self.sacctmgr_path).bake('-iQ')
        self.add = self.cmd.bake('add')
        self.modify = self.cmd.bake('modify')
        self.remove = self.cmd.bake('remove')
        self.show = self.cmd.bake('show', '-P')

    def add_account(self, account_name: str,
                          max_jobs: Optional[int] = None) -> sh.Command:
        args = ['account', account_name]
        if max_jobs is not None:
            args.append(f'MaxJobs={max_jobs}')
        return self.add.bake(*args)

    def modify_account(self, account_name: str,
                             max_jobs: int) -> sh.Command:
        if max_jobs is None:
            max_jobs = -1
        return self.modify.bake('account',
                                account_name,
                                'set',
                                f'MaxJobs={max_jobs}')

    def remove_account(self, account_name: str) -> sh.Command:
        return self.remove.bake('account', account_name)

    def add_qos(self, qos_name: str, 
                      qos: SlurmQOS) -> sh.Command:
        return self.add.bake('qos',
                             qos_name,
                             *qos.to_slurm())

    def modify_qos(self, qos_name: str,
                         qos: SlurmQOS) -> sh.Command:
        return self.modify.bake('qos',
                                qos_name,
                                'set',
                                *qos.to_slurm())

    def remove_qos(self, qos_name: str) -> sh.Command:
        return self.remove.bake('qos', qos_name)

    def add_user(self, user_name: str,
                       account_name: str,
                       partition_name: str,
                       qos_name: str) -> sh.Command:
        return self.add.bake('user',
                             f'user={user_name}',
                             f'account={account_name}',
                             f'partition={partition_name}',
                             f'qos={qos_name}')

    def remove_user(self, user_name: str,
                          account_name: Optional[str] = None,
                          partition_name: Optional[str] = None) -> sh.Command:

        args = ['user', f'user={user_name}']
        if account_name is not None:
            args.append(f'account={account_name}')
        if partition_name is not None:
            args.append(f'partition={partition_name}')

        return self.remove.bake(*args)

    def modify_user_qos(self, user_name: str,
                              account_name: str,
                              partition_name: str,
                              qos_name: str) -> sh.Command:
        return self.modify.bake('user',
                                'set',
                                f'qos={qos_name}',
                                f'defaultqos=-1',
                                'where',
                                f'user={user_name}',
                                f'account={account_name}',
                                f'partition={partition_name}')
    
    def show_associations(self, query: Optional[dict] = None) -> sh.Command:
        cmd = self.show.bake('associations')
        if query is not None:
            query = [f'{k}={v}' for k, v in query.items()] #type: ignore
            cmd = cmd.bake('where', *query) #type: ignore
        return cmd

    def show_qos(self) -> sh.Command:
        return self.show.bake('qos')

    @staticmethod
    def get_show_parser(fp: TextIO) -> csv.DictReader:
        return csv.DictReader(fp, delimiter='|')

    def get_slurm_qos_state(self) -> Tuple[dict, dict]:
        buf = StringIO()
        cmd = self.show_qos()
        cmd(_out=buf)
        buf.seek(0)
        return build_slurm_qos_state(buf)

    def get_slurm_association_state(self, query: Optional[dict] = None) -> dict:
        buf = StringIO()
        cmd = self.show_associations(query=query)
        cmd(_out=buf)
        buf.seek(0)
        return build_slurm_association_state(buf)


def get_qos_name(account_name: str, partition_name: str) -> str:
    return f'{account_name}-{partition_name}-qos'


def sanitize_tres(tres_string: str) -> dict:
    tres_string = tres_string.strip()

    if not tres_string:
        return {}

    tokens = tres_string.split(',')
    tres = {}
    for token in tokens:
        resource, _, value = token.partition('=')
        resource = resource.removeprefix('gres/')
        resource, _, _ = resource.partition(':') # for now we discard the type from resource:type
        tres[resource] = value

    return tres


def build_puppet_tres(tres_string: str) -> Optional[dict]:
    slurm_tres = sanitize_tres(tres_string)
    if any((item is not None for item in slurm_tres.values())):
        puppet_tres = dict(cpus=slurm_tres.get('cpu', None),
                           mem=slurm_tres.get('mem', None),
                           gpus=slurm_tres.get('gpu', None))
    else:
        puppet_tres = None

    return puppet_tres


def build_slurm_qos_state(qos_file_pointer: TextIO,
                          filter_on: dict = {'Name': 'normal'}) -> Tuple[dict, dict]:
    qos_map = {}
    filtered_map = {}
    for row in SAcctMgr.get_show_parser(qos_file_pointer):
        
        filter_row = check_filter(row, filter_on)
    
        puppet_grp_tres = build_puppet_tres(row['GrpTRES'])
        puppet_job_tres = build_puppet_tres(row['MaxTRES'])
        puppet_user_tres = build_puppet_tres(row['MaxTRESPU'])

        puppet_qos = SlurmQOS.Schema().load(dict(group=puppet_grp_tres, #type: ignore
                                                 job=puppet_job_tres,
                                                 user=puppet_user_tres,
                                                 priority=row['Priority'])) 
        
        if filter_row:
            filtered_map[row['Name']] = puppet_qos
        else:
            qos_map[row['Name']] = puppet_qos

    return qos_map, filtered_map


def build_slurm_association_state(associations_file_pointer: TextIO,
                                  filter_accounts_on: dict = {'Account': ['root']},
                                  filter_users_on: dict = {}) -> dict:

    associations = dict(users={}, accounts={})
    for row in SAcctMgr.get_show_parser(associations_file_pointer):
        row = filter_nulls(row)
        
        if 'Partition' not in row:
            # This is a parent Account definition, add it to the map.
            # Set partitions empty and fill in as we encounter them.
            filter_row = check_filter(row, filter_accounts_on)
            if not filter_row:
                extras = int(row['MaxJobs']) if 'MaxJobs' in row else None
                associations['accounts'][row['Account']] = extras
        elif 'User' in row:
            filter_row = check_filter(row, filter_users_on)
            if not filter_row:
                user_name = row['User']
                account_name = row['Account']
                partition_name = row['Partition']
                qos_name = row['QOS']
                
                user_assocs = associations['users'].get(user_name, {})
                assoc_key = (user_name, account_name, partition_name)
                if assoc_key in associations['users']:
                    print(f'Overwriting {assoc_key}:', associations['users'][assoc_key], 'with', qos_name)
                associations['users'][assoc_key] = qos_name
        else:
            print(row, file=sys.stderr)

    return associations


def build_puppet_qos_state(puppet_mapping: PuppetAccountMap) -> dict:
    # Mapping of name-of-QOS => SlurmQOS
    qos_map = {}

    qos_references = []
    for group_name, group in puppet_mapping.group.items():
        if group.slurm is None or group.slurm.partitions is None:
            continue
        for partition_name, partition in group.slurm.partitions.items():
            # If it is a reference to another QOS, it will be put in the map
            # where it is defined
            if type(partition.qos) is str:
                qos_references.append((group_name, partition_name, partition.qos))
                continue
            qos_name = get_qos_name(group_name, partition_name)
            qos_map[qos_name] = partition.qos

    # Validate that all QOS references actually exist
    for group_name, partition_name, qos_name in qos_references:
        if qos_name not in qos_map:
            raise ValueError(f'{group_name} has invalid QoS for {partition_name}: {qos_name}')

    return qos_map


def build_puppet_association_state(puppet_mapping: PuppetAccountMap) -> dict:
    
    puppet_associations = dict(users={}, accounts={})

    for group_name, group in puppet_mapping.group.items():
        if group.slurm is not None:
            puppet_associations['accounts'][group_name] = group.slurm.max_jobs

    for user_name, user in puppet_mapping.user.items():

        inherited_partitions = []

        # Get groups first...
        if user.groups is not None:
            for group_name in user.groups:
                if group_name in puppet_mapping.group: #type: ignore
                    group = puppet_mapping.group[group_name] #type: ignore
                    if group.slurm is not None and group.slurm.partitions is not None:
                        inherited_partitions.append((group_name, group.slurm.partitions))

        # Now via account associations
        if user.slurm is not None and user.slurm.account is not None:
            for account in user.slurm.account:
                group = puppet_mapping.group[account] #type: ignore
                inherited_partitions.append((account, group.slurm.partitions))

        for account_name, partitions in inherited_partitions:
            for partition_name, partition in partitions.items():
                if type(partition.qos) is str:
                    qos_name = partition.qos
                else:
                    qos_name = get_qos_name(account_name, partition_name)
                puppet_associations['users'][(user_name, account_name, partition_name)] = qos_name

    return puppet_associations


def reconcile_qoses(old_qoses: dict, new_qoses: dict) -> Tuple[list, list, list]:
    deletions = []
    updates = []
    additions = []
    
    for qos_name, old_qos in old_qoses.items():
        if qos_name not in new_qoses:
            deletions.append(qos_name)
        else:
            new_qos = new_qoses[qos_name]
            if old_qos != new_qos:
                updates.append((qos_name, new_qos))
    
    for qos_name, new_qos in new_qoses.items():
        if qos_name not in old_qoses:
            additions.append((qos_name, new_qos))
   
    return deletions, updates, additions


def reconcile_users(old_assocs: dict, new_assocs: dict) -> Tuple[list, list, list]:
    deletions = []
    updates = []
    additions = []
    
    # Deletions and updates: check old associations against new
    for assoc_key in old_assocs:
        if assoc_key not in new_assocs:
            deletions.append(assoc_key)
        else:
            if old_assocs[assoc_key] != new_assocs[assoc_key]:
                updates.append(assoc_key + (new_assocs[assoc_key],))
    
    # Additions: new user associations checked against old
    for assoc_key in new_assocs:
        if assoc_key not in old_assocs:
            additions.append(assoc_key + (new_assocs[assoc_key],))
    
    return deletions, updates, additions


def reconcile_accounts(old_accounts: dict, new_accounts: dict) -> Tuple[list, list, list]:
    deletions = []
    updates = []
    additions = []
    
    for account_name, old_extra in old_accounts.items():
        if account_name not in new_accounts:
            deletions.append(account_name)
        else:
            if old_extra != new_accounts[account_name]:
                updates.append((account_name, new_accounts[account_name]))

    for account_name, new_extra in new_accounts.items():
        if account_name not in old_accounts:
            additions.append((account_name, new_extra))
    
    return deletions, updates, additions


class SlurmOp(Enum):
    ADD_QOS = auto()
    ADD_USER = auto()
    ADD_ACCOUNT = auto()
    MODIFY_QOS = auto()
    MODIFY_USER = auto()
    MODIFY_ACCOUNT = auto()
    DELETE_QOS = auto()
    DELETE_USER = auto()
    DELETE_ACCOUNT = auto()


def generate_commands(slurm_associations: dict,
                      slurm_qoses: dict, 
                      puppet_associations: dict,
                      puppet_qoses: dict,
                      sacctmgr: Optional[SAcctMgr] = None,
                      **sacctmgr_kwargs) -> list:

    if sacctmgr is None:
        sacctmgr = SAcctMgr(**sacctmgr_kwargs)

    user_deletions, user_updates, user_additions = reconcile_users(slurm_associations['users'], 
                                                                   puppet_associations['users'])
    account_deletions, account_updates, account_additions = reconcile_accounts(slurm_associations['accounts'], 
                                                                               puppet_associations['accounts'])
    qos_deletions, qos_updates, qos_additions = reconcile_qoses(slurm_qoses, 
                                                                puppet_qoses)
    
    command_queue = []
    
    command_queue.append(('Add New QOSes', SlurmOp.ADD_QOS,
                          [sacctmgr.add_qos(*addition) for addition in qos_additions]))
    command_queue.append(('Modify QOSes', SlurmOp.MODIFY_QOS,
                          [sacctmgr.modify_qos(*update) for update in qos_updates]))
    command_queue.append(('Modify Users', SlurmOp.MODIFY_USER,
                          [sacctmgr.modify_user_qos(*update) for update in user_updates]))
    command_queue.append(('Delete Users', SlurmOp.DELETE_USER,
                          [sacctmgr.remove_user(*deletion) for deletion in user_deletions]))
    command_queue.append(('Delete QOSes', SlurmOp.DELETE_QOS,
                          [sacctmgr.remove_qos(deletion) for deletion in qos_deletions]))
    command_queue.append(('Add New Accounts', SlurmOp.ADD_ACCOUNT,
                          [sacctmgr.add_account(*addition) for addition in account_additions]))
    command_queue.append(('Modify Accounts', SlurmOp.MODIFY_ACCOUNT,
                          [sacctmgr.modify_account(*update) for update in account_updates]))
    command_queue.append(('Add New Users', SlurmOp.ADD_USER,
                          [sacctmgr.add_user(*addition) for addition in user_additions]))
    command_queue.append(('Delete Accounts', SlurmOp.DELETE_ACCOUNT,
                          [sacctmgr.remove_account(deletion) for deletion in account_deletions]))

    return command_queue


def add_sync_args(parser):
    parser.add_argument('--sudo', action='store_true', default=False,
                        help='Run sacctmgr commands with sudo.')
    parser.add_argument('--apply', action='store_true', default=False,
                        help='Execute and apply the Slurm changes.')
    parser.add_argument('--slurm-associations', type=argparse.FileType('r'),
                        help='Read slurm associations from the specified file '
                             'instead of parsing from a `sacctmgr show -P assoc` call.')
    parser.add_argument('--slurm-qoses', type=argparse.FileType('r'),
                        help='Read slurm QoSes from the specified file '
                             'instead of parsing from a `sacctmgr show -P qos` call.')
    parser.add_argument('yaml_files', nargs='+',
                        help='Source YAML files.')


@subcommand('show-qos')
def show_qos(args: argparse.Namespace):
    pass


@subcommand('sync', add_sync_args)
def sync(args: argparse.Namespace):
    console = Console(stderr=True)

    console.rule('Load association data.')
    console.print('Loading Puppet YAML data...')
    yaml_forest = parse_yaml_forest(args.yaml_files,
                                    merge_on=MergeStrategy.ALL)
    # Generator only yields one item with MergeStrategy.ALL
    _, puppet_data = next(validate_yaml_forest(yaml_forest,
                                               PuppetAccountMap,
                                               strict=True))

    console.print('Building Puppet associations table...')
    puppet_associations = build_puppet_association_state(puppet_data)
    console.print('Building Puppet QoSes...')
    puppet_qos_map = build_puppet_qos_state(puppet_data)

    sacctmgr = SAcctMgr(sudo=args.sudo)
    console.print('Getting current associations...')
    if args.slurm_associations:
        slurm_associations = build_slurm_association_state(args.slurm_associations)
    else:
        slurm_associations = sacctmgr.get_slurm_association_state()
    console.print('Getting current QoSes...')
    if args.slurm_qoses:
        slurm_qos_map, _ = build_slurm_qos_state(args.slurm_qoses)
    else:
        slurm_qos_map, _ = sacctmgr.get_slurm_qos_state()

    console.rule('Reconcile Puppet and Slurm.')
    console.print('Generating reconciliation commands...')
    command_queue = generate_commands(slurm_associations,
                                      slurm_qos_map,
                                      puppet_associations,
                                      puppet_qos_map,
                                      sacctmgr=sacctmgr)

    report = {}
    for command_group_name, slurm_op, command_group in command_queue:
        group_report = {'successes': 0, 'failures': 0, 'commands': len(command_group)}
        report[slurm_op.name] = group_report

        if not command_group:
            continue

        console.rule(f'Commands: {command_group_name}', style='blue')
        if args.apply:
            for command in track(command_group, console=console):
                try:
                    #console.out(f'Run: {command}', highlight=False)
                    command()
                except sh.ErrorReturnCode_1 as e: #type: ignore
                    console.print(f'\nCommand Error: {e}', highlight=False)
                    group_report['failures'] += 1
                else:
                    group_report['successes'] += 1
        else:
            for command in command_group:
                console.out(str(command), highlight=False)

    print(json.dumps(report, indent=2)) 





def audit_partitions(args):
    console = Console(stderr=True)

    yaml_forest = parse_yaml_forest(args.yaml_files,
                                    merge_on=MergeStrategy.ALL)
    _, puppet_data = next(validate_yaml_forest(yaml_forest,
                                               PuppetAccountMap,
                                               strict=True))


def clone(args):
    pass
