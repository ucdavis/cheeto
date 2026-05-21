from __future__ import annotations

from typing import Any

from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession

from ..models.group import AccessGroup, Group, StatusGroup
from ..models.base import link_target_id
from ..models.site import Site
from ..models.slurm import SlurmAccount
from ..models.user import User
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
        sticky_ids = {link_target_id(link) for link in site.group.sticky}
        if group.id in sticky_ids:
            return  # idempotent
        site.group.sticky.append(group)
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
            link for link in site.group.sticky
            if link_target_id(link) != group.id
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

        sticky_ids = {link_target_id(link) for link in site.slurm.sticky}
        if account.id not in sticky_ids:
            site.slurm.sticky.append(account)
        if self.default:
            site.slurm.default_account = account
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

        if link_target_id(site.slurm.default_account) == account.id:
            if not self.clear_default:
                raise ValueError(
                    f'SlurmAccount for {self.groupname!r} is set as '
                    f'default_account; pass clear_default=True (or '
                    f'--clear-default on the CLI) to remove it'
                )
            site.slurm.default_account = None

        before = len(site.slurm.sticky)
        site.slurm.sticky = [
            link for link in site.slurm.sticky
            if link_target_id(link) != account.id
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
