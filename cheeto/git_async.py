#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2026
# (c) The Regents of the University of California, Davis, 2026
# File   : git_async.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 12.06.2026

"""Async-friendly git wrapper (the v2 counterpart of cheeto/git.py, which
lingers for the v1 stack until cutover).

Builders return BAKED `sh` commands; execution is the caller's job (see
.claude/rules/sh.md). The base command bakes `_async=True` (the
slurm_sync.py convention — sh 2.2.x needs the explicit flag), so every
call returns an awaitable: `await cmd()` — the subprocess never blocks
the loop.

Fixes ported-in from the v1 implementation:
- branch names are minted per call (v1 froze a module-import timestamp, so
  a long-lived daemon reused the same branch forever) and carry a
  microsecond suffix (second resolution collides under repeated syncs);
- empty roundtrips delete their working branch instead of accumulating;
- failures restore the base branch (the branch is kept for forensics);
- emptiness is probed explicitly (`git diff --cached --name-only`) instead
  of treating any `git commit` exit-1 as "nothing to commit";
- pushed working branches are also deleted on the remote;
- an unpushed working branch is never deleted (it holds the only copy of
  the commit);
- the file lock is acquired off-loop.
"""

from __future__ import annotations

import asyncio
import logging
import socket
from contextlib import asynccontextmanager, nullcontext
from datetime import datetime
from pathlib import Path
from typing import AsyncIterator

import sh
from filelock import FileLock

from .utils import human_timestamp, sanitize_timestamp


class GitEmptyCommitError(Exception):
    """Raised by AsyncGitRepo.commit/roundtrip when the body staged no
    changes. When it propagates out of roundtrip the repo is already back
    on the base branch and the working branch deleted."""


def branch_name_title(prefix: str = 'cheeto-puppet-sync') -> tuple[str, str]:
    # Minted per call — never cache the timestamp (a daemon process lives
    # for weeks). Microseconds disambiguate same-second roundtrips.
    now = datetime.now()
    branch = f'{prefix}.{sanitize_timestamp(now)}.{now.microsecond:06d}'
    title = f'[{socket.getfqdn()}] {prefix}: {human_timestamp(now)}'
    return branch, title


class Git:
    """Baked git command builders bound to a working directory."""

    def __init__(self, working_dir: Path | None = None):
        self.working_dir = Path('.') if working_dir is None else working_dir
        # _tty_out=False: sh defaults to a pty, which makes git colorize
        # and page — corrupting parsed output (and the emptiness probe).
        self.cmd = sh.Command('git').bake(
            _cwd=self.working_dir, _async=True, _tty_out=False,
        )
        logging.getLogger(__name__).info(
            '`git` working in: %s', self.working_dir,
        )

    def init(self, branch: str = 'main') -> sh.Command:
        return self.cmd.bake('init', '-b', branch)

    def checkout(self, branch: str, create: bool = False) -> sh.Command:
        checkout = self.cmd.bake('checkout')
        if create:
            checkout = checkout.bake('-b')
        return checkout.bake(branch)

    def pull(self) -> sh.Command:
        return self.cmd.bake('pull')

    def add(self, *files: Path) -> sh.Command:
        return self.cmd.bake('add', *files)

    def commit(self, message: str, allow_empty: bool = False) -> sh.Command:
        commit = self.cmd.bake('commit')
        if allow_empty:
            commit = commit.bake('--allow-empty')
        return commit.bake('-m', message)

    def diff_cached_names(self) -> sh.Command:
        # Always exits 0; empty output means nothing staged. The reliable
        # emptiness probe (exit codes from `git commit` are ambiguous).
        return self.cmd.bake('diff', '--cached', '--name-only')

    def push(self, remote_create: str | None = None) -> sh.Command:
        if remote_create is None:
            return self.cmd.bake('push')
        return self.cmd.bake('push', '--set-upstream', 'origin', remote_create)

    def push_delete(self, branch: str) -> sh.Command:
        return self.cmd.bake('push', 'origin', '--delete', branch)

    def merge(self, branch: str) -> sh.Command:
        return self.cmd.bake('merge', branch)

    def remove_branch(self, branch: str) -> sh.Command:
        return self.cmd.bake('branch', '-D', branch)

    def clean(self, force: bool = True,
              exclude: str | None = None) -> sh.Command:
        clean = self.cmd.bake('clean')
        if force:
            clean = clean.bake('-f')
        if exclude is not None:
            clean = clean.bake('--exclude', exclude)
        return clean

    def rev_parse(self, commit: str = 'HEAD') -> sh.Command:
        return self.cmd.bake('rev-parse', commit)

    def current_branch(self) -> sh.Command:
        return self.cmd.bake('rev-parse', '--abbrev-ref', 'HEAD')


class AsyncGitRepo:

    def __init__(self, root: Path, base_branch: str = 'main'):
        self.root = Path(root)
        self.lock_file = self.root / '.cheeto.lock'
        self.base_branch = base_branch
        self.git = Git(working_dir=self.root.absolute())
        self.logger = logging.getLogger(self.__class__.__name__)

    @asynccontextmanager
    async def _locked(self, timeout: int) -> AsyncIterator[None]:
        # acquire() blocks, so it runs off-loop. thread_local=False is
        # REQUIRED: acquisition happens in a to_thread pool thread and
        # release on the loop thread — the default thread-local lock would
        # silently fail to release.
        lock = FileLock(self.lock_file, timeout=timeout, thread_local=False)
        await asyncio.to_thread(lock.acquire)
        try:
            yield
        finally:
            lock.release()

    async def create(self) -> None:
        # Both init and the root commit run through the same _cwd-baked
        # command, so they cannot target different directories.
        self.root.mkdir(parents=True, exist_ok=True)
        await self.git.init(branch=self.base_branch)()
        await self.git.commit('Root commit', allow_empty=True)()

    async def _staged_changes(self) -> bool:
        return bool(str(await self.git.diff_cached_names()()).strip())

    @asynccontextmanager
    async def commit(self, message: str, *, lock: bool = True,
                     timeout: int = 30) -> AsyncIterator[sh.Command]:
        """Yield the baked `git add` command; the body stages files with
        `await add(path)` and the staged changes are committed on exit.
        Raises GitEmptyCommitError when nothing was staged."""
        async with self._locked(timeout) if lock else nullcontext():
            yield self.git.add()
            if not await self._staged_changes():
                raise GitEmptyCommitError('Nothing to commit.')
            await self.git.commit(message)()

    @asynccontextmanager
    async def roundtrip(self, message: str, *,
                        working_branch: str | None = None,
                        clean: bool = False,
                        timeout: int = 30,
                        push_merge: bool = True,
                        delete_branch: bool = True) -> AsyncIterator[sh.Command]:
        """Full sync cycle: branch off base, stage (body), commit, then
        push + merge to base + push. The working branch is deleted locally
        AND on the remote when `delete_branch` — but never when
        `push_merge=False` (it holds the only copy of the commit).

        GitEmptyCommitError propagates (callers report no-change); any
        other failure restores the base branch and keeps the working
        branch for forensics.
        """
        if working_branch is None:
            working_branch, _ = branch_name_title()

        async with self._locked(timeout):
            await self.git.checkout(self.base_branch)()
            if clean:
                await self.git.clean(force=True, exclude=self.lock_file.name)()
            await self.git.pull()()
            await self.git.checkout(working_branch, create=True)()

            pushed = False
            try:
                yield self.git.add()
                if not await self._staged_changes():
                    raise GitEmptyCommitError(
                        f'Nothing to commit on {working_branch}.'
                    )
                await self.git.commit(message)()

                if push_merge:
                    self.logger.info(
                        'Pushing and creating branch: %s', working_branch,
                    )
                    await self.git.push(remote_create=working_branch)()
                    pushed = True
                    self.logger.info(
                        'merge %s into %s', working_branch, self.base_branch,
                    )
                    await self.git.checkout(self.base_branch)()
                    await self.git.merge(working_branch)()
                    await self.git.push()()
            except GitEmptyCommitError:
                await self.git.checkout(self.base_branch)()
                await self.git.remove_branch(working_branch)()
                raise
            except BaseException:
                # BaseException so task cancellation also restores; the
                # working branch is deliberately kept for forensics.
                try:
                    await self.git.checkout(self.base_branch)()
                except sh.ErrorReturnCode:
                    self.logger.exception(
                        'failed to restore base branch %s', self.base_branch,
                    )
                raise
            else:
                if not push_merge:
                    await self.git.checkout(self.base_branch)()
                elif delete_branch:
                    self.logger.info('Deleting branch: %s', working_branch)
                    await self.git.remove_branch(working_branch)()
                    if pushed:
                        await self.git.push_delete(working_branch)()
