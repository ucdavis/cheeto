"""Sync a site's legacy puppet.hpc YAML into a git repo.

The v2 port of `cheeto db site sync-old-puppet`: export the site via
`site_to_puppet_legacy`, write `domains/<fqdn>/merged/all.yaml` (and
optionally `keys/<username>.pub`) inside an AsyncGitRepo roundtrip —
commit on a timestamped branch, push, merge to base, push.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from beanie.operators import In
from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession

from ..git_async import AsyncGitRepo, GitEmptyCommitError, branch_name_title
from ..models.base import link_target_id
from ..models.site import Site
from ..models.user import SshKey, User
from ..models.user_site_info import UserSiteInfo
from ..queries.puppet_legacy import site_to_puppet_legacy
from .base import Operation


class SyncOldPuppet(Operation):
    """Export a site to the legacy puppet.hpc YAML repo and round-trip it
    through git. The repo clone must already exist (deploy step). With
    `push=False` the commit stays on the retained working branch for
    inspection. A git failure propagates (daemon task FAILURE is the
    alerting signal); `GitEmptyCommitError` is reported as
    `changed=False`.
    """

    op_name = 'sync_old_puppet'
    # External side effects (git, filesystem) — no transaction.
    transactional = False

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        sitename: str,
        repo: Path,
        base_branch: str = 'main',
        push: bool = True,
        write_keys: bool = True,
        delete_branch: bool = True,
        lock_timeout: int = 30,
    ) -> None:
        super().__init__(client, author)
        self.sitename = sitename
        self.repo = Path(repo)
        self.base_branch = base_branch
        self.push = push
        self.write_keys = write_keys
        self.delete_branch = delete_branch
        self.lock_timeout = lock_timeout
        self._branch = ''
        self._changed = False
        self._pushed = False
        self._users_with_keys = 0

    async def execute(self, session: AsyncClientSession) -> dict[str, Any]:
        site = await Site.find_one(Site.name == self.sitename)
        if site is None:
            raise ValueError(f'Site {self.sitename!r} does not exist')
        if not (self.repo / '.git').exists():
            raise ValueError(
                f'{self.repo} is not a git repository clone; the puppet '
                'repo must be cloned before syncing'
            )

        # DB reads happen before the repo lock is taken.
        puppet_map = await site_to_puppet_legacy(site)
        keys_by_name = (
            await self._site_keys(site) if self.write_keys else {}
        )
        self._users_with_keys = len(keys_by_name)

        self._branch, _ = branch_name_title()
        repo = AsyncGitRepo(self.repo, base_branch=self.base_branch)
        yaml_path = (
            self.repo / 'domains' / site.fqdn / 'merged' / 'all.yaml'
        ).absolute()

        try:
            async with repo.roundtrip(
                f'Update merged yaml for {self.sitename}',
                working_branch=self._branch,
                clean=True,
                timeout=self.lock_timeout,
                push_merge=self.push,
                delete_branch=self.delete_branch,
            ) as add:
                yaml_path.parent.mkdir(parents=True, exist_ok=True)
                puppet_map.save_yaml(yaml_path)
                if self.write_keys:
                    keys_dir = (self.repo / 'keys').absolute()
                    keys_dir.mkdir(parents=True, exist_ok=True)
                    for name, keys in keys_by_name.items():
                        (keys_dir / f'{name}.pub').write_text(
                            '\n'.join(keys) + '\n'
                        )
                await add(self.repo.absolute())
        except GitEmptyCommitError:
            self.logger.info(
                'sync_old_puppet(%s): no changes', self.sitename,
            )
        else:
            self._changed = True
            self._pushed = self.push

        return self.describe()

    async def _site_keys(self, site: Site) -> dict[str, list[str]]:
        """username -> ssh keys for every user at the site, one query per
        collection (the ExportRootSSHKeys join shape, but bulk)."""
        usis = await UserSiteInfo.find(
            UserSiteInfo.site.id == site.id,
        ).to_list()
        user_ids = [usi.user.ref.id for usi in usis]
        if not user_ids:
            return {}
        users = await User.find(In(User.id, user_ids)).to_list()
        name_by_id = {u.id: u.name for u in users}
        keys_by_name: dict[str, list[str]] = {}
        for key in await SshKey.find(In(SshKey.user.id, user_ids)).to_list():
            name = name_by_id.get(link_target_id(key.user))
            if name is not None:
                keys_by_name.setdefault(name, []).append(key.key)
        return keys_by_name

    def describe(self) -> dict[str, Any]:
        return {
            'sitename': self.sitename,
            'repo': str(self.repo),
            'branch': self._branch,
            'changed': self._changed,
            'pushed': self._pushed,
            'users_with_keys': self._users_with_keys,
            'delete_branch': self.delete_branch,
        }
