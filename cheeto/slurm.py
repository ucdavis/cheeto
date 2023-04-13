#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : slurm.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 10.04.2023

from dataclasses import dataclass
from io import StringIO
import os
import sys
from typing import Tuple

from rich import print as rprint
from rich.console import Console
from rich.syntax import Syntax
import sh

from .types import *
from .puppet import (SlurmQOSTRES,
                     SlurmQOS,
                     SlurmPartition,
                     SlurmRecord)
from .utils import parse_yaml, puppet_merge



class SAcctMgrCmd:

    def __init__(self, sacctmgr_path: str = None):
        if sacctmgr_path is None:
            self.sacctmgr_path = sh.which('sacctmgr').strip()
        else:
            self.sacctmgr_path = sacctmgr_path

        if not os.path.exists(self.sacctmgr_path):
            raise RuntimeError(f'{self.sacctmgr_path} does not exist!')

        self.cmd = sh.Command(self.sacctmgr_path).bake('-i')
        self.add = self.cmd.bake('add')
        self.modify = self.cmd.bake('modify')
        self.remove = self.cmd.bake('remove')
        self.show = self.cmd.bake('show', '-P')

    def add_account(self, account_name: str) -> sh.Command:
        return self.add.bake('account', account_name)

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

    def add_user(self, user_name: str,
                       account_name: str,
                       partition_name: str,
                       qos_name: str) -> sh.Command:
        return self.add.bake('user',
                             user_name,
                             f'account={account_name}',
                             f'partition={partition_name}',
                             f'qos={qos_name}')

    def remove_user(self, user_name: str,
                          account_name: str = None,
                          partition_name: str = None) -> sh.Command:

        args = ['user', user_name]
        if account_name is not None:
            args.append(f'account={account_name}')
        if partition_name is not None:
            args.append(f'partition={partition_name}')

        return self.remove.bake(*args)
    
    def show_users(self) -> sh.Command:
        return self.show.bake('users')

    def show_qos(self) -> sh.Command:
        return self.show.bake('qos')

    @staticmethod
    def get_show_parser(fp: TextIO) -> csv.DictReader:
        return csv.DictReader(fp, delimiter='|')

    def get_current_qos_map(self) -> Tuple[dict, dict]:
        buf = StringIO()
        cmd = self.show_qos()
        cmd(_out=buf)
        return build_qos_map(buf)


def build_qos_map(qos_file_pointer: TextIO,
                  filter_on: dict = {'Name': 'normal'}):
    qos_map = {}
    filtered_map = {}
    for row in SAcctMgrCmd.get_show_parser(qos_file_pointer):
        
        filter_row = check_filter(row, filter_on)
    
        slurm_tres = sanitize_tres(row['GrpTRES'])
        puppet_tres = SlurmQOSTRES(cpus=slurm_tres.get('cpu', None),
                                   mem=slurm_tres.get('mem', None),
                                   gpus=slurm_tres.get('gpu', None))
        puppet_qos = SlurmQOS(group=puppet_tres)
        
        if filter_row:
            filtered_map[row['Name']] = puppet_qos
        else:
            qos_map[row['Name']] = puppet_qos

    return qos_map, filtered_map

