from pathlib import Path
import pytest
import sh

from ..git import Git, GitRepo


@pytest.fixture
def test_repo(tmpdir):
    root = Path(tmpdir.join('repo'))
    repo = GitRepo(root)
    repo.create()
    repo.git.cmd('config', 'user.name', 'Test User')
    repo.git.cmd('config', 'user.email', 'test@example.com')
    return repo

class TestGitRepo:

    def test_commit(self, test_repo):
        test_file = test_repo.root / 'file.txt'
        with test_repo.commit('Initial commit') as add:
            sh.touch(test_file) #type: ignore
            add(test_file)

        files = [Path(f.strip()).name for f in \
                 list(test_repo.git.cmd('ls-tree', '-r', '--name-only', 'HEAD', _iter=True))] #type: ignore
        assert files == [test_file.name]

    def test_roundtrip(self, test_repo, monkeypatch):
        monkeypatch.setattr(Git, 'pull', lambda self: lambda: None)

        test_file = test_repo.root / 'file.txt'
        branch_name = 'test-branch'
        with test_repo.roundtrip('Initial commit',
                            working_branch=branch_name,
                            push_merge=False) as add:
            sh.touch(test_file) #type: ignore
            add(test_file)

        files = [Path(f.strip()).name for f in \
            list(test_repo.git.cmd('ls-tree', '-r', '--name-only', branch_name, _iter=True))] #type: ignore
        assert files == [test_file.name]

    def test_roundtrip_delete_branch(self, test_repo, monkeypatch):
        monkeypatch.setattr(Git, 'pull', lambda self: lambda: None)

        test_file = test_repo.root / 'file.txt'
        branch_name = 'test-branch'
        with test_repo.roundtrip('Initial commit',
                            working_branch=branch_name,
                            push_merge=False,
                            delete_branch=True) as add:
            sh.touch(test_file) #type: ignore
            add(test_file)

        branches = [b.strip() for b in \
                    list(test_repo.git.cmd('branch', _iter=True))] #type: ignore
        assert branch_name not in branches