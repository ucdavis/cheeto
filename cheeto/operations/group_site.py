"""Operations for explicit group site-presence (`GroupSiteInfo`).

`ensure_group_site` is the shared idempotent attach helper called from every
code path that ties a group to a site (membership adds, sponsor-group
creation, sticky adds, slurm accounts, storage provisioning, user site
adds). `AddSiteGroup`/`RemoveSiteGroup` are the explicit CLI-facing ops, and
`BackfillGroupSiteInfo` materializes presence records for pre-existing data
from the imputed sources.
"""

from __future__ import annotations

from typing import Any

from beanie.operators import In
from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession

from ..models.base import link_target_id
from ..models.group import AccessGroup, Group, StatusGroup
from ..models.group_membership import GroupMembership
from ..models.group_site_info import GroupSiteInfo
from ..models.site import Site
from ..models.slurm import SlurmAccount
from ..models.storage import Storage
from ..models.user import User
from ..queries.group import (
    find_group_by_name,
    imputed_group_ids_at_site,
    resolve_group_names,
)
from .base import Operation


async def ensure_group_site(
    group: Group, site: Site, session: AsyncClientSession | None,
) -> GroupSiteInfo:
    """Idempotent find-or-insert of the `(group, site)` presence record.

    The find_one must pass `session`: inside a transaction, sessionless
    reads cannot see the transaction's own uncommitted inserts, and a
    duplicate insert would abort the whole transaction (see the
    CreateClassUsers docstring). Cross-transaction races are backstopped by
    the unique (group, site) index — the losing transaction aborts and a
    re-run succeeds; do NOT catch DuplicateKeyError here, a write error
    inside a Mongo transaction is unrecoverable in-transaction.
    """
    existing = await GroupSiteInfo.find_one(
        GroupSiteInfo.group.id == group.id,
        GroupSiteInfo.site.id == site.id,
        session=session,
    )
    if existing is not None:
        return existing
    gsi = GroupSiteInfo(group=group, site=site)
    await gsi.insert(session=session)
    return gsi


class _SiteGroupOp(Operation):
    """Shared resolution/refusals for the explicit add/remove-site ops."""

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        group_name: str,
        site_name: str,
    ) -> None:
        super().__init__(client, author)
        self.group_name = group_name
        self.site_name = site_name

    async def _resolve(self) -> tuple[Group, Site]:
        group = await find_group_by_name(self.group_name)
        if group is None:
            raise ValueError(f'Group {self.group_name} does not exist')
        if isinstance(group, (AccessGroup, StatusGroup)):
            raise ValueError(
                f'Group {self.group_name} is a seeded access/status group; '
                f'those exist at every site and have no per-site presence '
                f'record'
            )
        if group.type == 'user':
            raise ValueError(
                f'Group {self.group_name} is a personal group; its site '
                f'presence follows its owner (`user add site` / '
                f'`user remove site`)'
            )
        site = await Site.find_one(Site.name == self.site_name)
        if site is None:
            raise ValueError(f'Site {self.site_name} does not exist')
        return group, site

    def describe(self) -> dict[str, Any]:
        return {'group': self.group_name, 'site': self.site_name}


class AddSiteGroup(_SiteGroupOp):
    op_name = 'add_site_group'

    async def execute(self, session: AsyncClientSession) -> GroupSiteInfo:
        group, site = await self._resolve()
        existing = await GroupSiteInfo.find_one(
            GroupSiteInfo.group.id == group.id,
            GroupSiteInfo.site.id == site.id,
        )
        if existing is not None:
            raise ValueError(
                f'Group {self.group_name} already on site {self.site_name}'
            )
        gsi = GroupSiteInfo(group=group, site=site)
        await gsi.insert(session=session)
        return gsi


class RemoveSiteGroup(_SiteGroupOp):
    """Detach a group from a site.

    Slurm accounts, storage records, and sticky references block removal
    even with `force` — each has its own decommission path, and a lingering
    sticky ref would make the next backfill recreate the presence record.
    Membership edges block unless `force`, which deletes them per-document
    so the LDAP dirty hooks fire. The group's LDAP entry at the site is
    removed by the next prune, not immediately.
    """

    op_name = 'remove_site_group'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        group_name: str,
        site_name: str,
        force: bool = False,
    ) -> None:
        super().__init__(
            client, author, group_name=group_name, site_name=site_name,
        )
        self.force = force
        self._memberships_deleted = 0

    async def execute(self, session: AsyncClientSession) -> dict[str, int]:
        group, site = await self._resolve()
        gsi = await GroupSiteInfo.find_one(
            GroupSiteInfo.group.id == group.id,
            GroupSiteInfo.site.id == site.id,
        )
        if gsi is None:
            raise ValueError(
                f'Group {self.group_name} not on site {self.site_name}'
            )

        blockers: list[str] = []
        if await SlurmAccount.find_one(
            SlurmAccount.group.id == group.id,
            SlurmAccount.site.id == site.id,
        ):
            blockers.append('a Slurm account (remove it first)')
        if await Storage.find_one(
            Storage.group.id == group.id,
            Storage.site.id == site.id,
        ):
            blockers.append('storage records (delete group storage first)')
        if group.id in set(site.group.sticky):
            blockers.append(
                'a sticky-group entry (`site remove sticky-group` first)'
            )
        if blockers:
            raise ValueError(
                f'Group {self.group_name} still has {"; ".join(blockers)} '
                f'at {self.site_name}'
            )

        edges = await GroupMembership.find(
            GroupMembership.group.id == group.id,
            GroupMembership.site.id == site.id,
        ).to_list()
        if edges and not self.force:
            raise ValueError(
                f'Group {self.group_name} has {len(edges)} membership '
                f'edge(s) at {self.site_name}; pass --force to delete them'
            )
        for edge in edges:
            await edge.delete(session=session)
        await gsi.delete(session=session)
        self._memberships_deleted = len(edges)
        return {'memberships_deleted': self._memberships_deleted}

    def describe(self) -> dict[str, Any]:
        return {
            'group': self.group_name,
            'site': self.site_name,
            'force': self.force,
            'memberships_deleted': self._memberships_deleted,
        }


class BackfillGroupSiteInfo(Operation):
    """Create `GroupSiteInfo` records for groups whose site presence is
    currently only imputed (membership edges, sticky refs, slurm accounts,
    storage, personal groups of site users). Must run once per deployment
    before the GSI-based readers (LDAP site sync/prune, puppet export) see
    real data; safe to re-run any time.
    """

    op_name = 'backfill_group_site_info'
    # Whole-collection scan across sites; each insert is independently
    # idempotent under the unique (group, site) index, so a bare session is
    # fine and avoids transactionLifetimeLimit on large backfills.
    transactional = False

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        site_name: str | None = None,
        dry_run: bool = False,
    ) -> None:
        super().__init__(client, author)
        self.site_name = site_name
        self.dry_run = dry_run
        self._report: dict[str, dict[str, Any]] = {}

    async def execute(
        self, session: AsyncClientSession,
    ) -> dict[str, dict[str, Any]]:
        if self.site_name is not None:
            site = await Site.find_one(Site.name == self.site_name)
            if site is None:
                raise ValueError(f'Site {self.site_name} does not exist')
            sites = [site]
        else:
            sites = await Site.find_all().to_list()

        report: dict[str, dict[str, Any]] = {}
        for site in sites:
            imputed = await imputed_group_ids_at_site(site)
            existing_gsis = await GroupSiteInfo.at_site(site).to_list()
            existing_ids = {
                link_target_id(g.group) for g in existing_gsis
            }
            existing_ids.discard(None)
            missing = imputed - existing_ids
            # Explicit presence with no imputed source — informational;
            # legitimate for groups attached ahead of any members/resources.
            extras = existing_ids - imputed

            groups = (
                await Group.find(
                    In(Group.id, list(missing)), with_children=True,
                ).to_list()
                if missing else []
            )
            created: list[str] = []
            for group in sorted(groups, key=lambda g: g.name):
                if not self.dry_run:
                    await GroupSiteInfo(group=group, site=site).insert(
                        session=session,
                    )
                created.append(group.name)

            report[site.name] = {
                'created': created,
                'existing': len(existing_ids),
                'extras': await resolve_group_names(list(extras)),
            }

        self._report = report
        return report

    def describe(self) -> dict[str, Any]:
        # Counts only — keep History rows bounded on big backfills.
        return {
            'dry_run': self.dry_run,
            'site': self.site_name,
            'sites': {
                name: {
                    'created': len(r['created']),
                    'existing': r['existing'],
                    'extras': len(r['extras']),
                }
                for name, r in self._report.items()
            },
        }
