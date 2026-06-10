from __future__ import annotations

from typing import Any

from beanie.operators import In
from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession

from ..models.group import AccessGroup, Group, StatusGroup
from ..models.base import link_target_id
from ..models.site import Site
from ..models.slurm import SlurmAccount, SlurmAllocation
from ..models.user import User
from ..models.user_site_info import UserSiteInfo
from .base import Operation


class CreateSite(Operation):
    op_name = 'create_site'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        name: str,
        fqdn: str,
    ) -> None:
        super().__init__(client, author)
        self.name = name
        self.fqdn = fqdn

    async def execute(self, session: AsyncClientSession) -> Site:
        existing = await Site.find_one(Site.name == self.name)
        if existing is not None:
            raise ValueError(f'Site {self.name} already exists')

        site = Site(name=self.name, fqdn=self.fqdn)
        await site.insert(session=session)
        self._site = site
        return site

    def describe(self) -> dict[str, Any]:
        return {
            'name': self.name,
            'fqdn': self.fqdn,
        }


# ---------------------------------------------------------------------------
# Sticky group / slurm-account management
# ---------------------------------------------------------------------------


async def _load_site_and_group(
    sitename: str, groupname: str,
) -> tuple[Site, Group]:
    site = await Site.find_one(Site.name == sitename)
    if site is None:
        raise ValueError(f'Site {sitename!r} does not exist')
    group = await Group.find_one(
        Group.name == groupname, with_children=True,
    )
    if group is None:
        raise ValueError(f'Group {groupname!r} does not exist')
    return site, group


class AddStickyGroup(Operation):
    """Add `groupname` to `site.group.sticky`. Idempotent.

    Refuses access/status groups — those are managed via SyncUserToLDAP
    and have no per-site sticky semantics.
    """

    op_name = 'add_sticky_group'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        sitename: str,
        groupname: str,
    ) -> None:
        super().__init__(client, author)
        self.sitename = sitename
        self.groupname = groupname

    async def execute(self, session: AsyncClientSession) -> None:
        site, group = await _load_site_and_group(self.sitename, self.groupname)
        if isinstance(group, (AccessGroup, StatusGroup)):
            raise ValueError(
                f'Group {self.groupname!r} is an access/status group; not '
                f'eligible for site.group.sticky'
            )
        if group.id in set(site.group.sticky):
            return  # idempotent
        site.group.sticky.append(group.id)
        await site.save(session=session)

    def describe(self) -> dict[str, Any]:
        return {'sitename': self.sitename, 'groupname': self.groupname}


class RemoveStickyGroup(Operation):
    """Remove `groupname` from `site.group.sticky`. Idempotent."""

    op_name = 'remove_sticky_group'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        sitename: str,
        groupname: str,
    ) -> None:
        super().__init__(client, author)
        self.sitename = sitename
        self.groupname = groupname

    async def execute(self, session: AsyncClientSession) -> None:
        site, group = await _load_site_and_group(self.sitename, self.groupname)
        before = len(site.group.sticky)
        site.group.sticky = [
            ref for ref in site.group.sticky if ref != group.id
        ]
        if len(site.group.sticky) == before:
            return  # nothing to remove
        await site.save(session=session)

    def describe(self) -> dict[str, Any]:
        return {'sitename': self.sitename, 'groupname': self.groupname}


async def _load_slurm_account_for(
    site: Site, group: Group,
) -> SlurmAccount:
    account = await SlurmAccount.find_one(
        SlurmAccount.group.id == group.id,
        SlurmAccount.site.id == site.id,
    )
    if account is None:
        raise ValueError(
            f'No SlurmAccount for group={group.name!r} at site={site.name!r}'
        )
    return account


class AddStickySlurmAccount(Operation):
    """Add the SlurmAccount for `(groupname, sitename)` to
    `site.slurm.sticky`. With `default=True`, also set it as
    `site.slurm.default_account` (the only way to set a default that
    satisfies the membership invariant)."""

    op_name = 'add_sticky_slurm_account'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        sitename: str,
        groupname: str,
        default: bool = False,
    ) -> None:
        super().__init__(client, author)
        self.sitename = sitename
        self.groupname = groupname
        self.default = default

    async def execute(self, session: AsyncClientSession) -> None:
        site, group = await _load_site_and_group(self.sitename, self.groupname)
        account = await _load_slurm_account_for(site, group)

        if account.id not in set(site.slurm.sticky):
            site.slurm.sticky.append(account.id)
        if self.default:
            site.slurm.default_account = account.id
        await site.save(session=session)

    def describe(self) -> dict[str, Any]:
        return {
            'sitename': self.sitename,
            'groupname': self.groupname,
            'default': self.default,
        }


class RemoveStickySlurmAccount(Operation):
    """Remove the SlurmAccount for `(groupname, sitename)` from
    `site.slurm.sticky`. Idempotent.

    Refuses to remove the account currently set as `default_account`
    unless `clear_default=True` — otherwise the @before_event validator
    would reject the save.
    """

    op_name = 'remove_sticky_slurm_account'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        sitename: str,
        groupname: str,
        clear_default: bool = False,
    ) -> None:
        super().__init__(client, author)
        self.sitename = sitename
        self.groupname = groupname
        self.clear_default = clear_default

    async def execute(self, session: AsyncClientSession) -> None:
        site, group = await _load_site_and_group(self.sitename, self.groupname)
        account = await _load_slurm_account_for(site, group)

        if site.slurm.default_account == account.id:
            if not self.clear_default:
                raise ValueError(
                    f'SlurmAccount for {self.groupname!r} is set as '
                    f'default_account; pass clear_default=True (or '
                    f'--clear-default on the CLI) to remove it'
                )
            site.slurm.default_account = None

        before = len(site.slurm.sticky)
        site.slurm.sticky = [
            ref for ref in site.slurm.sticky if ref != account.id
        ]
        if len(site.slurm.sticky) == before and site.slurm.default_account is not None:
            return  # nothing changed
        await site.save(session=session)

    def describe(self) -> dict[str, Any]:
        return {
            'sitename': self.sitename,
            'groupname': self.groupname,
            'clear_default': self.clear_default,
        }


class SetSiteDefaultSlurmAccount(Operation):
    """Set the SlurmAccount for `(groupname, sitename)` as the site's
    `default_account`. The account is also ensured to be in `site.slurm.sticky`
    (the @before_event validator requires the default to be a sticky account),
    so this works whether or not the account was already sticky."""

    op_name = 'set_site_default_slurm_account'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        sitename: str,
        groupname: str,
    ) -> None:
        super().__init__(client, author)
        self.sitename = sitename
        self.groupname = groupname

    async def execute(self, session: AsyncClientSession) -> None:
        site, group = await _load_site_and_group(self.sitename, self.groupname)
        account = await _load_slurm_account_for(site, group)

        if account.id not in set(site.slurm.sticky):
            site.slurm.sticky.append(account.id)
        site.slurm.default_account = account.id
        await site.save(session=session)

    def describe(self) -> dict[str, Any]:
        return {'sitename': self.sitename, 'groupname': self.groupname}


class ClearSiteDefaultSlurmAccount(Operation):
    """Clear the site's `default_account` (leaving the account in
    `site.slurm.sticky`). Idempotent."""

    op_name = 'clear_site_default_slurm_account'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        sitename: str,
    ) -> None:
        super().__init__(client, author)
        self.sitename = sitename

    async def execute(self, session: AsyncClientSession) -> None:
        site = await Site.find_one(Site.name == self.sitename)
        if site is None:
            raise ValueError(f'Site {self.sitename!r} does not exist')
        if site.slurm.default_account is None:
            return  # idempotent
        site.slurm.default_account = None
        await site.save(session=session)

    def describe(self) -> dict[str, Any]:
        return {'sitename': self.sitename}


class SetSiteStorageDefaults(Operation):
    """Set the site's storage defaults (`SiteStorageSettings`): the parent
    volume new homes are provisioned under, the default home quota, and the
    home mount mechanism. Only the kwargs passed are changed; the embedded
    validator enforces automount/static exclusivity (clear one by setting
    the other)."""

    op_name = 'set_site_storage_defaults'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        sitename: str,
        home_volume: str | None = None,
        home_quota: str | None = None,
        home_automount_map: str | None = None,
        home_static_mount: str | None = None,
    ) -> None:
        super().__init__(client, author)
        self.sitename = sitename
        self.home_volume = home_volume
        self.home_quota = home_quota
        self.home_automount_map = home_automount_map
        self.home_static_mount = home_static_mount

    async def execute(self, session: AsyncClientSession) -> None:
        from ..models.storage import AutomountMap, StaticMount, StorageVolume

        site = await Site.find_one(Site.name == self.sitename)
        if site is None:
            raise ValueError(f'Site {self.sitename!r} does not exist')

        if self.home_volume is not None:
            volume = await StorageVolume.find_one(
                StorageVolume.name == self.home_volume,
                StorageVolume.site.id == site.id,
            )
            if volume is None:
                raise ValueError(
                    f'Volume {self.home_volume!r} does not exist on '
                    f'{self.sitename!r}'
                )
            site.storage.default_home_volume = volume.id

        if self.home_quota is not None:
            site.storage.default_home_quota = self.home_quota

        if self.home_automount_map is not None:
            amap = await AutomountMap.find_one(
                AutomountMap.name == self.home_automount_map,
                AutomountMap.site.id == site.id,
            )
            if amap is None:
                raise ValueError(
                    f'AutomountMap {self.home_automount_map!r} does not '
                    f'exist on {self.sitename!r}'
                )
            site.storage.home_automount_map = amap.id
            site.storage.home_static_mount = None

        if self.home_static_mount is not None:
            smount = await StaticMount.find_one(
                StaticMount.name == self.home_static_mount,
                StaticMount.site.id == site.id,
            )
            if smount is None:
                raise ValueError(
                    f'StaticMount {self.home_static_mount!r} does not '
                    f'exist on {self.sitename!r}'
                )
            site.storage.home_static_mount = smount.id
            site.storage.home_automount_map = None

        await site.save(session=session)

    def describe(self) -> dict[str, Any]:
        return {
            'sitename': self.sitename,
            'home_volume': self.home_volume,
            'home_quota': self.home_quota,
            'home_automount_map': self.home_automount_map,
            'home_static_mount': self.home_static_mount,
        }


# ---------------------------------------------------------------------------
# Root SSH key export
# ---------------------------------------------------------------------------


class ExportRootSSHKeys(Operation):
    """Render the root `authorized_keys` content for a site: the SSH keys of
    every `admin` user at the site whose effective access includes
    `root-ssh`, grouped per user.

    Format (matches v1's `db site root-key`, plus an `environment=` option so
    sshd records which admin authenticated as root):

        # alice <alice@ucdavis.edu>
        environment="REMOTE_SSH_USER=alice" ssh-ed25519 AAAA... alice@laptop

    Read-only; recorded in History as an audit trail of root-key exports.
    """

    op_name = 'export_root_ssh_keys'
    transactional = False

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        sitename: str,
    ) -> None:
        super().__init__(client, author)
        self.sitename = sitename
        self._user_count = 0
        self._key_count = 0

    async def execute(self, session: AsyncClientSession) -> str:
        # Imported here to avoid any import-time coupling to the queries layer.
        from ..queries.user import find_users, list_user_ssh_keys

        site = await Site.find_one(Site.name == self.sitename)
        if site is None:
            raise ValueError(f'Site {self.sitename!r} does not exist')

        # Admins at the site whose effective (override-aware) access includes
        # root-ssh, sorted by name.
        users = await find_users(
            type='admin', access='root-ssh', site=self.sitename,
        )

        lines: list[str] = []
        for user in users:
            keys = await list_user_ssh_keys(user)
            if not keys:
                continue
            self._user_count += 1
            lines.append(f'# {user.name} <{user.email}>')
            for k in keys:
                lines.append(f'environment="REMOTE_SSH_USER={user.name}" {k.key}')
                self._key_count += 1

        return '\n'.join(lines) + '\n' if lines else ''

    def describe(self) -> dict[str, Any]:
        return {
            'sitename': self.sitename,
            'users': self._user_count,
            'keys': self._key_count,
        }


# ---------------------------------------------------------------------------
# Sympa mailing-list email export
# ---------------------------------------------------------------------------


_DEFAULT_SYMPA_IGNORE = {'hpc-help@ucdavis.edu'}


class ExportSympaEmails(Operation):
    """Render a site's Sympa mailing-list feed: the emails of every `user`/
    `admin` at the site whose effective per-site status is not `inactive`
    (`disabled`/`offboarding` are included, matching v1), minus an ignore set.
    One sorted, de-duplicated email per line.

    Read-only; recorded in History.
    """

    op_name = 'export_sympa_emails'
    transactional = False

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        sitename: str,
        ignore: list[str] | None = None,
    ) -> None:
        super().__init__(client, author)
        self.sitename = sitename
        self.ignore = (
            set(ignore) if ignore is not None else set(_DEFAULT_SYMPA_IGNORE)
        )
        self._count = 0

    async def execute(self, session: AsyncClientSession) -> str:
        site = await Site.find_one(Site.name == self.sitename)
        if site is None:
            raise ValueError(f'Site {self.sitename!r} does not exist')

        usis = await UserSiteInfo.find(
            UserSiteInfo.site.id == site.id,
        ).to_list()
        if not usis:
            return ''
        usi_by_user = {usi.user.ref.id: usi for usi in usis}
        users = await User.find(In(User.id, list(usi_by_user))).to_list()

        # Preload status-name lookup so effective status resolves without a
        # query per user.
        status_name_by_id = {
            sg.id: sg.status_name
            for sg in await StatusGroup.find_all().to_list()
        }

        def _status_name(link) -> str | None:
            if link is None:
                return None
            return status_name_by_id.get(link_target_id(link))

        emails: set[str] = set()
        for user in users:
            if user.type not in ('user', 'admin'):
                continue
            usi = usi_by_user[user.id]
            # USI status overrides the global status when set.
            status = _status_name(usi.status or user.status)
            if status == 'inactive':
                continue
            if user.email in self.ignore:
                continue
            emails.add(user.email)

        self._count = len(emails)
        if not emails:
            return ''
        return '\n'.join(sorted(emails)) + '\n'

    def describe(self) -> dict[str, Any]:
        return {
            'sitename': self.sitename,
            'emails': self._count,
            'ignored': len(self.ignore),
        }


# ---------------------------------------------------------------------------
# Site removal (cascade)
# ---------------------------------------------------------------------------


class RemoveSite(Operation):
    """Remove a site and every per-site record that links to it.

    Beanie has no reverse cascade, so each linking collection
    (`SITE_LINKED_MODELS`) is deleted explicitly, plus the `SlurmAllocation`
    records owned by the site's QOSes (they carry no `site` field). Global
    records (users, groups) are untouched. Transactional: the whole cascade
    commits atomically.
    """

    op_name = 'remove_site'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        sitename: str,
    ) -> None:
        super().__init__(client, author)
        self.sitename = sitename
        self._deleted: dict[str, int] = {}

    async def execute(self, session: AsyncClientSession) -> dict[str, int]:
        from ..queries.site import SITE_LINKED_MODELS, site_alloc_ids

        site = await Site.find_one(Site.name == self.sitename)
        if site is None:
            raise ValueError(f'Site {self.sitename!r} does not exist')

        deleted: dict[str, int] = {}

        # Allocations first: they're found via the site's QOSes, which are
        # deleted in the loop below.
        alloc_ids = list(set(await site_alloc_ids(site)))
        if alloc_ids:
            res = await SlurmAllocation.find(
                In(SlurmAllocation.id, alloc_ids),
            ).delete(session=session)
            deleted['slurm_allocations'] = getattr(res, 'deleted_count', 0)

        for label, model in SITE_LINKED_MODELS:
            res = await model.find(
                model.site.id == site.id,
            ).delete(session=session)
            deleted[label] = getattr(res, 'deleted_count', 0)

        await site.delete(session=session)
        deleted['site'] = 1

        self._deleted = deleted
        return deleted

    def describe(self) -> dict[str, Any]:
        return {'sitename': self.sitename, **self._deleted}
