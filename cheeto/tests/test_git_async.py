"""Tests for the async git layer (cheeto/git_async.py).

Run against a real git binary: a bare repo under tmp_path acts as
`origin`, with a working clone the AsyncGitRepo operates in. Fixtures are
synchronous (no loop running), so plain sh calls execute normally there;
inside the async tests every git command is awaited.
"""

import os
from pathlib import Path

import pytest
import sh
from filelock import FileLock, Timeout

from ..git_async import AsyncGitRepo, GitEmptyCommitError, branch_name_title


@pytest.fixture
def cloned_repo(tmp_path):
    if os.getenv('GITHUB_ACTIONS') == 'true':
        sh.git('config', '--global', 'user.name', 'Test User')
        sh.git('config', '--global', 'user.email', 'test@example.com')
    origin = tmp_path / 'origin.git'
    sh.git('init', '--bare', '-b', 'main', str(origin))
    clone = tmp_path / 'repo'
    sh.git('clone', str(origin), str(clone))
    sh.git('commit', '--allow-empty', '-m', 'Root commit', _cwd=str(clone))
    # Upstream tracking so the roundtrip's unpatched `git pull` works.
    sh.git('push', '-u', 'origin', 'main', _cwd=str(clone))
    return origin, AsyncGitRepo(clone)


async def _origin_files(origin: Path) -> list[str]:
    out = await sh.git('ls-tree', '-r', '--name-only', 'main',
                       _cwd=str(origin), _async=True, _tty_out=False)
    return [line.strip() for line in str(out).splitlines() if line.strip()]


async def _origin_branches(origin: Path) -> list[str]:
    out = await sh.git('branch', '--format=%(refname:short)',
                       _cwd=str(origin), _async=True, _tty_out=False)
    return [line.strip() for line in str(out).splitlines() if line.strip()]


async def _origin_rev(origin: Path) -> str:
    return str(await sh.git('rev-parse', 'main',
                            _cwd=str(origin), _async=True, _tty_out=False)).strip()


async def _local_branches(repo: AsyncGitRepo) -> list[str]:
    out = await repo.git.cmd('branch', '--format=%(refname:short)')
    return [line.strip() for line in str(out).splitlines() if line.strip()]


async def _current_branch(repo: AsyncGitRepo) -> str:
    return str(await repo.git.current_branch()()).strip()


class TestBranchNames:

    def test_distinct_branch_names_per_call(self):
        # v1 froze the timestamp at module import — a daemon process
        # reused the same branch name forever.
        assert branch_name_title()[0] != branch_name_title()[0]

    def test_checkout_requires_branch(self):
        from ..git_async import Git
        with pytest.raises(TypeError):
            Git().checkout()


class TestAsyncGitRepo:

    async def test_create(self, tmp_path):
        if os.getenv('GITHUB_ACTIONS') == 'true':
            await sh.git('config', '--global', 'user.name',
                         'Test User', _async=True)
            await sh.git('config', '--global', 'user.email',
                         'test@example.com', _async=True)
        repo = AsyncGitRepo(tmp_path / 'fresh')
        await repo.create()
        assert await _current_branch(repo) == 'main'
        log = str(await repo.git.cmd('log', '--oneline'))
        assert 'Root commit' in log

    async def test_roundtrip_pushes_to_origin(self, cloned_repo):
        origin, repo = cloned_repo
        async with repo.roundtrip('add file') as add:
            (repo.root / 'file.txt').write_text('hello\n')
            await add(repo.root.absolute())

        assert 'file.txt' in await _origin_files(origin)
        assert await _current_branch(repo) == 'main'
        # delete_branch defaults True: the working branch is gone locally
        # AND on the remote (v1 accumulated remote branches forever).
        assert await _local_branches(repo) == ['main']
        assert await _origin_branches(origin) == ['main']

    async def test_roundtrip_keep_branch(self, cloned_repo):
        origin, repo = cloned_repo
        async with repo.roundtrip(
            'add file', working_branch='keepme', delete_branch=False,
        ) as add:
            (repo.root / 'file.txt').write_text('hello\n')
            await add(repo.root.absolute())

        assert 'keepme' in await _local_branches(repo)
        assert 'keepme' in await _origin_branches(origin)

    async def test_empty_roundtrip_raises_and_leaves_nothing(self, cloned_repo):
        origin, repo = cloned_repo
        rev_before = await _origin_rev(origin)

        with pytest.raises(GitEmptyCommitError):
            async with repo.roundtrip('nothing to see'):
                pass  # stage nothing

        assert await _origin_rev(origin) == rev_before
        assert await _local_branches(repo) == ['main']
        assert await _current_branch(repo) == 'main'

    async def test_two_roundtrips_one_process(self, cloned_repo):
        # The v1 daemon-killer: the second auto-named roundtrip reused the
        # first one's branch name and died on `checkout -b`.
        origin, repo = cloned_repo
        for name in ('one.txt', 'two.txt'):
            async with repo.roundtrip(f'add {name}') as add:
                (repo.root / name).write_text(f'{name}\n')
                await add(repo.root.absolute())

        files = await _origin_files(origin)
        assert 'one.txt' in files and 'two.txt' in files

    async def test_failure_restores_base_branch(self, cloned_repo):
        origin, repo = cloned_repo
        with pytest.raises(RuntimeError, match='boom'):
            async with repo.roundtrip('doomed', working_branch='doomed') as add:
                (repo.root / 'file.txt').write_text('hello\n')
                await add(repo.root.absolute())
                raise RuntimeError('boom')

        assert await _current_branch(repo) == 'main'
        # The working branch survives for forensics.
        assert 'doomed' in await _local_branches(repo)
        assert 'doomed' not in await _origin_branches(origin)

    async def test_real_commit_failure_not_swallowed(self, cloned_repo):
        # v1 treated ANY `git commit` exit-1 as "nothing to commit"; a
        # failing hook must propagate as ErrorReturnCode instead.
        origin, repo = cloned_repo
        hook = repo.root / '.git' / 'hooks' / 'pre-commit'
        hook.write_text('#!/bin/sh\nexit 1\n')
        hook.chmod(0o755)

        with pytest.raises(sh.ErrorReturnCode):
            async with repo.roundtrip('hooked', working_branch='hooked') as add:
                (repo.root / 'file.txt').write_text('hello\n')
                await add(repo.root.absolute())

        assert await _current_branch(repo) == 'main'

    async def test_no_push_retains_branch(self, cloned_repo):
        # An unpushed working branch holds the only copy of the commit;
        # delete_branch must not apply.
        origin, repo = cloned_repo
        rev_before = await _origin_rev(origin)
        async with repo.roundtrip(
            'local only', working_branch='localwork',
            push_merge=False, delete_branch=True,
        ) as add:
            (repo.root / 'file.txt').write_text('hello\n')
            await add(repo.root.absolute())

        assert await _current_branch(repo) == 'main'
        assert 'localwork' in await _local_branches(repo)
        committed = str(await repo.git.cmd(
            'ls-tree', '-r', '--name-only', 'localwork',
        ))
        assert 'file.txt' in committed
        assert await _origin_rev(origin) == rev_before

    async def test_lock_timeout(self, cloned_repo):
        origin, repo = cloned_repo
        held = FileLock(repo.lock_file, thread_local=False)
        held.acquire()
        try:
            with pytest.raises(Timeout):
                async with repo.roundtrip('blocked', timeout=0):
                    pass
        finally:
            held.release()
