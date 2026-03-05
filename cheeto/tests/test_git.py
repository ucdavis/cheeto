from pathlib import Path
import pytest
import sh

from ..git import Git, GitRepo

class TestGitRepo:

    def test_create_repo(self, tmpdir):
        root = Path(tmpdir.join('repo'))
        repo = GitRepo(root)
        repo.create()
        assert (root / '.git').exists()
        assert (root / '.git').is_dir()

    def test_commit(self, tmpdir):
        root = Path(tmpdir.join('repo'))
        repo = GitRepo(root)
        repo.create()

        test_file = root / 'file.txt'
        with repo.commit('Initial commit') as add:
            sh.touch(test_file) #type: ignore
            add(test_file)

        files = [Path(f.strip()).name for f in list(repo.cmd.cmd('ls-tree', '-r', '--name-only', 'HEAD', _iter=True))] #type: ignore
        assert files == [test_file.name]

    def test_roundtrip(self, tmpdir, monkeypatch):
        monkeypatch.setattr(Git, 'pull', lambda self: lambda: None)

        root = Path(tmpdir.join('repo'))
        repo = GitRepo(root)
        repo.create()

        test_file = root / 'file.txt'
        branch_name = 'test-branch'
        with repo.roundtrip('Initial commit',
                            working_branch=branch_name,
                            push_merge=False) as add:
            sh.touch(test_file) #type: ignore
            add(test_file)

        files = [Path(f.strip()).name for f in list(repo.cmd.cmd('ls-tree', '-r', '--name-only', branch_name, _iter=True))] #type: ignore
        assert files == [test_file.name]

    def test_roundtrip_delete_branch(self, tmpdir, monkeypatch):
        monkeypatch.setattr(Git, 'pull', lambda self: lambda: None)

        root = Path(tmpdir.join('repo'))
        repo = GitRepo(root)
        repo.create()

        test_file = root / 'file.txt'
        branch_name = 'test-branch'
        with repo.roundtrip('Initial commit',
                            working_branch=branch_name,
                            push_merge=False,
                            delete_branch=True) as add:
            sh.touch(test_file) #type: ignore
            add(test_file)

        branches = [b.strip() for b in list(repo.cmd.cmd('branch', _iter=True))] #type: ignore
        assert branch_name not in branches