#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : git.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 31.05.2023

from contextlib import contextmanager
from enum import Enum, auto
import logging
import os
from pathlib import Path
import socket
from typing import Optional, Tuple

from filelock import FileLock
import sh

from .utils import human_timestamp, TIMESTAMP_NOW, sanitize_timestamp


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

    def merge(self, branch: str):
        return self.cmd.bake('merge', branch)

    def clean(self, force: Optional[bool] = True,
                    exclude: Optional[str] = None) -> sh.Command:
        clean = self.cmd.bake('clean')
        if force:
            clean = clean.bake('-f')
        if exclude is not None:
            clean = clean.bake('--exclude', exclude)
        return clean

    def rev_parse(self, commit: Optional[str] = 'HEAD') -> sh.Command:
        return self.cmd.bake('rev-parse', commit)


class CIStatus(Enum):
    SUCCESS = auto()
    FAILURE = auto()
    INCOMPLETE = auto()
    CANCELLED = auto()
    UNKNOWN = auto()


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

    def pr_create(self, base: str = 'main',
                        title: Optional[str] = None,
                        body: Optional[str] = None) -> sh.Command:
        if None in (title, body):
            return self.cmd.bake('pr', 'create', '--fill', '--base', base)
        else:
            return self.cmd.bake('pr', 'create', '-t', title, 
                                 '-b', body, '--base', base)

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
        elif status == 'cancelled':
            return CIStatus.CANCELLED
        elif status == '':
            return CIStatus.INCOMPLETE
        else:
            return CIStatus.UNKNOWN
            #raise ValueError(f'Unknown CI Status: "{status}"')


def branch_name_title(prefix: Optional[str] = 'cheeto-puppet-sync') -> Tuple[str, str]:
    branch_name = f"{prefix}.{sanitize_timestamp(TIMESTAMP_NOW)}"
    title = f"[{socket.getfqdn()}] {prefix}: {human_timestamp(TIMESTAMP_NOW)}"
    return branch_name, title


class GitRepo:

    def __init__(self,
                 root: Path,
                 base_branch: str = 'main'):
        self.root = root
        self.lock_file = self.root / '.cheeto.lock'
        self.base_branch = base_branch
        self.cmd = Git(working_dir=self.root.absolute())
        self.logger = logging.getLogger(self.__class__.__name__)

    def lock(self, timeout: int):
        return FileLock(self.lock_file, timeout=timeout)

    @contextmanager
    def commit(self,
               message: str,
               working_branch: Optional[str] = None,
               clean: bool = False,
               timeout: int = 30,
               push_merge: bool = True):
        
        if working_branch is None:
            working_branch, _ = branch_name_title()

        with self.lock(timeout):
            self.cmd.checkout(self.base_branch)()
            if clean:
                self.cmd.clean(force=True, exclude=self.lock_file.name)()
            self.cmd.pull()()
            self.cmd.checkout(branch=working_branch, create=True)()

            yield self.cmd.add()
            
            try:
                self.cmd.commit(message)()
            except sh.ErrorReturnCode_1: #type: ignore
                self.logger.info(f'Nothing to commit.')
                self.cmd.checkout(self.base_branch)()
                return

            if push_merge:
                self.logger.info(f'Pushing and creating branch: {working_branch}.')
                self.cmd.push(remote_create=working_branch)()

                self.logger.info(f'merge {working_branch} into {self.base_branch}')
                self.cmd.checkout(self.base_branch)()
                self.cmd.merge(working_branch)()

                self.cmd.push()()
            else:
                self.cmd.checkout(self.base_branch)()
