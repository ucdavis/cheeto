#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : slurm.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 10.04.2023

from dataclasses import dataclass
import os
import sys

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



class SAcctMgr:

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

    def add_account(self, account_name : str):
        return self.add.bake('account', account_name)

    def add_qos(self, qos_name : str, 
                      qos : SlurmQOS):
        return self.add.bake('qos',
                             qos_name,
                             *qos.to_slurm())

    def modify_qos(self, qos_name : str,
                         qos : SlurmQOS):
        return self.modify.bake('qos',
                                qos_name,
                                'set',
                                *qos.to_slurm())

    def add_user(self, user_name : str,
                       account_name : str,
                       partition_name : str,
                       qos_name : str):
        return self.add.bake('user',
                             user_name,
                             f'account={account_name}',
                             f'partition={partition_name}',
                             f'qos={qos_name}')

    def remove_user(self, user_name : str,
                          account_name : str = None,
                          partition_name : str = None):

        args = ['user', user_name]
        if account_name is not None:
            args.append(f'account={account_name}')
        if partition_name is not None:
            args.append(f'partition={partition_name}')

        return self.remove.bake(*args)
    



                        


