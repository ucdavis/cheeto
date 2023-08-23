#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : git.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 31.05.2023

from enum import Enum, auto
import logging
import os
from pathlib import Path
from typing import Optional

import sh


class Git:

    def __init__(self, working_dir: Optional[Path] = None):
        logger = logging.getLogger(__name__)
        self.working_dir = Path('.') if working_dir is None \
                                     else working_dir
        self.cmd = sh.Command('git').bake(_cwd=self.working_dir)
        logger.info(f'`git` working in: {self.working_dir}')

    def checkout(self, branch: Optional[str] = '',
                       create: Optional[bool] = False) -> sh.Command:
        checkout = self.cmd.bake('checkout')
        if create:
            checkout = checkout.bake('-b')
        return checkout.bake(branch)

    def pull(self) -> sh.Command:
        return self.cmd.bake('pull')

    def add(self, *files: Path) -> sh.Command:
        return self.cmd.bake('add', *files)

    def commit(self, message: str) -> sh.Command:
        return self.cmd.bake('commit', '-m', message)

    def push(self, remote_create: Optional[str] = None) -> sh.Command:
        if remote_create is None:
            return self.cmd.bake('push')
        return self.cmd.bake('push', '--set-upstream', 'origin', remote_create)

    def clean(self, force: Optional[bool] = True) -> sh.Command:
        clean = self.cmd.bake('clean')
        if force:
            clean = clean.bake('-f')
        return clean

    def rev_parse(self, commit: Optional[str] = 'HEAD') -> sh.Command:
        return self.cmd.bake('rev-parse', commit)


class CIStatus(Enum):
    SUCCESS = auto()
    FAILURE = auto()
    INCOMPLETE = auto()


class Gh:

    def __init__(self, working_dir: Optional[Path] = None):
        logger = logging.getLogger(__name__)
        self.working_dir = Path('.') if working_dir is None \
                                     else working_dir
        _env = dict(**os.environ)
        _env['GH_PAGER'] = ''
        #GH_TOKEN=os.getenv('GH_TOKEN'),
        #PATH='/Users/camille/.nix-profile/bin/')
        self.cmd = sh.Command('gh').bake(_cwd=self.working_dir,
                                         _env=_env)
        logger.info(f'`gh` working in: {self.working_dir}')

    def run_list(self, branch: str,
                       json: Optional[str] = None,
                       jq: Optional[str] = None) -> sh.Command:
        
        if json is None and jq is not None:
            raise ValueError('Must supply json to use jq.')

        run_list = self.cmd.bake('run', 'list', '-b', branch)
        if json is not None:
            run_list = run_list.bake('--json', json)
        if jq is not None:
            run_list = run_list.bake('--jq', jq)

        return run_list

    def pr_create(self, base: str = 'main') -> sh.Command:
        return self.cmd.bake('pr', 'create', '--fill', '--base', base)

    def pr_view(self, branch: str) -> sh.Command:
        return self.cmd.bake('pr', 'view', branch)

    def pr_view_url(self, branch: str) -> sh.Command:
        view = self.pr_view(branch)
        return view.bake('--json', 'url', '--jq', '.url')

    def pr_merge(self, branch: str,
                       delete_branch: Optional[bool] = False) -> sh.Command:
        merge = self.cmd.bake('pr', 'merge', '--merge', branch)
        if delete_branch:
            merge = merge.bake('--delete-branch')
        return merge

    def get_last_run_status(self, branch: str) -> CIStatus:
        cmd = self.run_list(branch, json='conclusion', jq='last | .conclusion')
        status = cmd().strip() #type: ignore
        if status == 'success':
            return CIStatus.SUCCESS
        elif status == 'failure':
            return CIStatus.FAILURE
        elif status == '':
            return CIStatus.INCOMPLETE
        else:
            raise ValueError(f'Unknown CI Status: "{status}"')
